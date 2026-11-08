//! Machine-readable resource-limit failures.
use std::fmt;

/// Resource whose configured budget or address-space bound was exceeded.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum BudgetResource {
    EncodedFrameBytes,
    DecodedFrameBytes,
    WindowBytes,
    EntryMetadataBytes,
    Extents,
    PointerBytes,
    ReadBytes,
    MetadataPageBytes,
    MetadataBufferBytes,
    MetadataRetainedBytes,
    MetadataEntries,
    DirectoryHandles,
    BatchRequests,
    BatchCopies,
    RangeBytes,
    AllocationBytes,
    ChunkBytes,
    EncodedObjectBytes,
    ExternalObjectBytes,
    ExternalFileBytes,
    UploadBytes,
    UploadParts,
    ResponseBytes,
    IndexBytes,
    CborItems,
}

/// Details of a resource rejection. Numeric bounds are present when known.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct BudgetError {
    pub resource: BudgetResource,
    pub limit: Option<u64>,
    pub requested: Option<u64>,
    /// Human-readable operation context; match `resource` in program logic.
    pub context: &'static str,
}
impl BudgetError {
    /// Describes a resource rejection without numeric accounting.
    #[must_use]
    pub const fn new(resource: BudgetResource, context: &'static str) -> Self {
        Self {
            resource,
            limit: None,
            requested: None,
            context,
        }
    }
    /// Describes an attempted value exceeding a known bound.
    #[must_use]
    pub const fn exceeded(
        resource: BudgetResource,
        context: &'static str,
        limit: u64,
        requested: u64,
    ) -> Self {
        Self {
            resource,
            limit: Some(limit),
            requested: Some(requested),
            context,
        }
    }
}
impl fmt::Display for BudgetError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.context)?;
        if let (Some(limit), Some(requested)) = (self.limit, self.requested) {
            write!(f, " (requested {requested}, limit {limit})")?;
        }
        Ok(())
    }
}
impl std::error::Error for BudgetError {}
