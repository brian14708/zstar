use std::ops::Deref;
#[cfg(not(feature = "async"))]
use std::sync::Arc;

/// Immutable decoded storage with no spare vector capacity retained.
#[derive(Clone, Debug)]
pub struct DecodedBody {
    #[cfg(feature = "async")]
    raw: bytes::Bytes,
    #[cfg(not(feature = "async"))]
    raw: Arc<[u8]>,
}
impl DecodedBody {
    pub(crate) fn compact(raw: Vec<u8>) -> Self {
        Self {
            raw: raw.into_boxed_slice().into(),
        }
    }
    #[cfg(feature = "async")]
    pub(crate) fn into_bytes(self) -> bytes::Bytes {
        self.raw
    }
}
impl Deref for DecodedBody {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        &self.raw
    }
}
