//! Standard-I/O archives and writers, available without the `async` feature.
//!
//! Opening eagerly validates all metadata. Content is fetched and validated
//! when accessed. A member handle exclusively borrows its archive until dropped;
//! use the root asynchronous API for independently concurrent owned handles.
//!
//! ```
//! use zstar::blocking::{Archive, Builder};
//! let mut writer = Builder::try_new(Vec::new())?;
//! writer.append_bytes("hello", b"world")?;
//! let mut archive = Archive::from_bytes(writer.finish()?)?;
//! assert_eq!(archive.read("hello")?, b"world");
//! # Ok::<(), zstar::Error>(())
//! ```
mod archive;
pub(crate) mod engine;
mod file;
mod io;

pub use crate::writer_engine::Builder;
pub use archive::{Archive, Entries, ReaderOptions};
pub use file::File;
pub use io::{MemorySource, ObjectResolver, RangeReader, SeekSource};
