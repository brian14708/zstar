//! Optional immutable packed-object storage. Catalogs are separate from v1 archives.
mod catalog;
pub use catalog::{Catalog, CatalogLimits, ChunkRecord, PackRecord, Snapshot};
#[cfg(feature = "async")]
mod storage;
#[cfg(feature = "async")]
pub use storage::{
    PackStore, PackWriter, PackWriterOptions, PackedObjectResolver, ResolverOptions,
};
#[cfg(feature = "async")]
mod generation;
#[cfg(feature = "async")]
pub use generation::{Generation, PublicationFailure, publish_generation};
#[cfg(feature = "fs")]
mod local;
#[cfg(feature = "fs")]
pub use local::LocalPackStore;
#[cfg(feature = "s3")]
mod s3;
#[cfg(feature = "s3")]
pub use s3::S3PackStore;

#[cfg(feature = "async")]
mod workflow;
#[cfg(feature = "async")]
pub use workflow::{export_objects, externalize_archive, materialize_archive};
