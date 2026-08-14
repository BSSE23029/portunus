//! Transactional content-addressed chunk storage infrastructure.
//!
//! Generic modules separate integrity, sparse assembly, atomic publication,
//! recovery, resource admission, range mapping, and concurrency policy. The
//! [`torrent`] module is a compatibility workload, not the crate's abstraction.

pub mod access;
pub mod assembly;
pub mod content;
pub mod integrity;
pub mod journal;
pub mod layout;
pub mod quota;
pub mod torrent;
