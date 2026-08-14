//! Reference composition root for the Portunus crates.
//!
//! The daemon deliberately remains thin: it translates network contracts into
//! engine calls and translates engine results back into gRPC status/messages.
//! Business and protocol logic belongs in reusable library crates.
//!
//! ```text
//! grpcurl/client ──HTTP/2 + Protobuf──> Control ──commands──> Engine
//!                <──server stream───── Control <──watch────── Engine
//! ```

use portunus_daemon::{
    admission::{AdmissionConfig, ControlInterceptor},
    auth::AuthConfig,
    errors::engine_status,
    logging::{init_global_logging, LoggingConfig},
    operations::{mark_draining, mark_serving},
};
use portunus_engine::torrent::{Config, Engine};
use portunus_proto::{
    portunus_control_server::{PortunusControl, PortunusControlServer},
    ConfigResponse, ConfigUpdate, Empty, MetricsResponse, StopTransferRequest, TransferRequest,
    TransferResponse, FILE_DESCRIPTOR_SET,
};
use std::{net::SocketAddr, path::PathBuf, pin::Pin};
use tokio_stream::Stream;
use tonic::{transport::Server, Request, Response, Status};

#[derive(Clone)]
struct Control {
    engine: Engine,
}
#[tonic::async_trait]
impl PortunusControl for Control {
    // Inputs:
    // - A gRPC request containing source and destination strings.
    // Outputs:
    // - An accepted transfer response, or `InvalidArgument` status.
    // Logic:
    // - Remove the protobuf envelope, convert the destination into a path, submit
    //   through the engine's bounded actor API, and map domain errors to gRPC.
    async fn add_transfer(
        &self,
        request: Request<TransferRequest>,
    ) -> Result<Response<TransferResponse>, Status> {
        let r = request.into_inner();
        let id = self
            .engine
            .add_transfer(r.source, PathBuf::from(r.destination))
            .await
            .map_err(engine_status)?;
        Ok(Response::new(TransferResponse {
            transfer_id: id,
            accepted: true,
            message: "queued".into(),
        }))
    }
    // Inputs:
    // - A gRPC request containing the transfer ID to stop.
    // Outputs:
    // - A stopped response, or `NotFound` status for an unknown transfer.
    // Logic:
    // - Forward the identifier through the engine command queue and preserve it
    //   in the success response for easy client-side correlation.
    async fn stop_transfer(
        &self,
        request: Request<StopTransferRequest>,
    ) -> Result<Response<TransferResponse>, Status> {
        let id = request.into_inner().transfer_id;
        self.engine
            .stop_transfer(id.clone())
            .await
            .map_err(engine_status)?;
        Ok(Response::new(TransferResponse {
            transfer_id: id,
            accepted: true,
            message: "stopped".into(),
        }))
    }
    type StreamMetricsStream =
        Pin<Box<dyn Stream<Item = Result<MetricsResponse, Status>> + Send + 'static>>;
    // Inputs:
    // - Empty gRPC request; subscription state comes from the engine handle.
    // Outputs:
    // - A server-streaming response yielding metrics until the engine closes.
    // Logic:
    // - Read the latest watch value, convert it to protobuf, yield it, then await
    //   a change. Slow clients naturally keep only the latest watch snapshot.
    async fn stream_metrics(
        &self,
        _: Request<Empty>,
    ) -> Result<Response<Self::StreamMetricsStream>, Status> {
        let mut rx = self.engine.subscribe_metrics();
        let output = async_stream::try_stream! {loop{let m=rx.borrow_and_update().clone();yield MetricsResponse{download_speed:m.download_speed,upload_speed:m.upload_speed,connected_peers:m.connected_peers,progress:m.progress,active_transfers:m.active_transfers};if rx.changed().await.is_err(){break;}}};
        Ok(Response::new(Box::pin(output)))
    }
    // Inputs:
    // - A protobuf patch whose fields are optional.
    // Outputs:
    // - The complete effective configuration after applying present values.
    // Logic:
    // - Mutate only supplied fields under the engine's config write lock, then
    //   obtain a fresh snapshot and translate it back to the wire contract.
    async fn update_config(
        &self,
        request: Request<ConfigUpdate>,
    ) -> Result<Response<ConfigResponse>, Status> {
        let update = request.into_inner();
        self.engine
            .update_config(|c| {
                if let Some(v) = update.download_limit_bytes_per_second {
                    c.download_limit = v;
                }
                if let Some(v) = update.upload_limit_bytes_per_second {
                    c.upload_limit = v;
                }
                if let Some(v) = update.max_peers {
                    c.max_peers = v;
                }
                if let Some(v) = update.command_buffer {
                    c.command_buffer = v;
                }
            })
            .await;
        let c = self.engine.config().await;
        Ok(Response::new(ConfigResponse {
            download_limit_bytes_per_second: c.download_limit,
            upload_limit_bytes_per_second: c.upload_limit,
            max_peers: c.max_peers,
            command_buffer: c.command_buffer,
        }))
    }
}

#[tokio::main]
// Inputs:
// - Optional `PORTUNUS_ADDR`, `PORTUNUS_LOG`, `RUST_LOG`, and
//   `PORTUNUS_BEARER_TOKEN`, and `PORTUNUS_MAX_IN_FLIGHT` environment variables.
// Outputs:
// - A running gRPC server until Ctrl-C, or a boxed startup/runtime error.
// Logic:
// - Initialize structured logging, parse the listen address, start the bounded
//   engine, register its gRPC adapter, and drain gracefully on the OS signal.
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let logging = LoggingConfig::from_env()?;
    init_global_logging(&logging)?;
    let addr: SocketAddr = std::env::var("PORTUNUS_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:50051".into())
        .parse()?;
    let bearer = std::env::var("PORTUNUS_BEARER_TOKEN").ok();
    let auth = AuthConfig::from_source(bearer.as_deref())?;
    let service = Control {
        engine: Engine::start(Config::default()),
    };
    tracing::info!(%addr, log_filter = logging.filter(), "Portunus control plane listening");
    let in_flight = std::env::var("PORTUNUS_MAX_IN_FLIGHT").ok();
    let admission = AdmissionConfig::from_source(in_flight.as_deref())?;
    let control =
        PortunusControlServer::with_interceptor(service, ControlInterceptor::new(auth, admission));
    let (mut health_reporter, health_service) = tonic_health::server::health_reporter();
    mark_serving(&mut health_reporter).await;
    let reflection = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(FILE_DESCRIPTOR_SET)
        .build_v1()?;
    Server::builder()
        .add_service(health_service)
        .add_service(reflection)
        .add_service(control)
        .serve_with_shutdown(addr, async move {
            let _ = tokio::signal::ctrl_c().await;
            mark_draining(&mut health_reporter).await;
            tracing::info!("Portunus control plane draining");
        })
        .await?;
    Ok(())
}
