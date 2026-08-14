use portunus_engine::{Config, Engine};
use portunus_proto::{
    portunus_control_server::{PortunusControl, PortunusControlServer},
    *,
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
    async fn add_transfer(
        &self,
        request: Request<TransferRequest>,
    ) -> Result<Response<TransferResponse>, Status> {
        let r = request.into_inner();
        let id = self
            .engine
            .add_transfer(r.source, PathBuf::from(r.destination))
            .await
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        Ok(Response::new(TransferResponse {
            transfer_id: id,
            accepted: true,
            message: "queued".into(),
        }))
    }
    async fn stop_transfer(
        &self,
        request: Request<StopTransferRequest>,
    ) -> Result<Response<TransferResponse>, Status> {
        let id = request.into_inner().transfer_id;
        self.engine
            .stop_transfer(id.clone())
            .await
            .map_err(|e| Status::not_found(e.to_string()))?;
        Ok(Response::new(TransferResponse {
            transfer_id: id,
            accepted: true,
            message: "stopped".into(),
        }))
    }
    type StreamMetricsStream =
        Pin<Box<dyn Stream<Item = Result<MetricsResponse, Status>> + Send + 'static>>;
    async fn stream_metrics(
        &self,
        _: Request<Empty>,
    ) -> Result<Response<Self::StreamMetricsStream>, Status> {
        let mut rx = self.engine.subscribe_metrics();
        let output = async_stream::try_stream! {loop{let m=rx.borrow_and_update().clone();yield MetricsResponse{download_speed:m.download_speed,upload_speed:m.upload_speed,connected_peers:m.connected_peers,progress:m.progress,active_transfers:m.active_transfers};if rx.changed().await.is_err(){break;}}};
        Ok(Response::new(Box::pin(output)))
    }
    async fn update_config(
        &self,
        request: Request<ConfigUpdate>,
    ) -> Result<Response<ConfigResponse>, Status> {
        let update = request.into_inner();
        self.engine
            .update_config(|c| {
                if let Some(v) = update.download_limit_bytes_per_second {
                    c.download_limit = v
                }
                if let Some(v) = update.upload_limit_bytes_per_second {
                    c.upload_limit = v
                }
                if let Some(v) = update.max_peers {
                    c.max_peers = v
                }
                if let Some(v) = update.command_buffer {
                    c.command_buffer = v
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
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let addr: SocketAddr = std::env::var("PORTUNUS_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:50051".into())
        .parse()?;
    let service = Control {
        engine: Engine::start(Config::default()),
    };
    tracing::info!(%addr,"Portunus control plane listening");
    Server::builder()
        .add_service(PortunusControlServer::new(service))
        .serve_with_shutdown(addr, async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    Ok(())
}
