use std::ops::Deref;
use std::sync::Arc;

#[derive(Clone, Debug)]
pub enum DecodedBody {
    Shared(Arc<[u8]>),
    #[cfg(feature = "async")]
    Async {
        raw: bytes::Bytes,
    },
}
impl DecodedBody {
    #[cfg(feature = "async")]
    pub(crate) fn from_vec(raw: Vec<u8>) -> Self {
        Self::Async { raw: raw.into() }
    }
    pub(crate) fn compact(raw: Vec<u8>) -> Self {
        Self::Shared(raw.into())
    }
    #[cfg(feature = "async")]
    pub(crate) fn into_bytes(self) -> bytes::Bytes {
        match self {
            Self::Shared(raw) => bytes::Bytes::from_owner(raw),
            Self::Async { raw, .. } => raw,
        }
    }
}
impl Deref for DecodedBody {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        match self {
            Self::Shared(raw) => raw,
            #[cfg(feature = "async")]
            Self::Async { raw, .. } => raw,
        }
    }
}
