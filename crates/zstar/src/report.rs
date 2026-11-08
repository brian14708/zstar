//! Provenance of selected content validation.
/// Checks completed by a selective read; does not certify the whole archive.
#[derive(Clone, Debug, Default)]
pub struct ReadReport {
    /// Whether directory, selected entry pages, and ancestor validation completed.
    /// This does not claim validation of unrelated metadata pages.
    pub metadata_validated: bool,
    /// Encoded offsets of the data frames checked.
    pub data_frames_checked: Vec<u64>,
    /// Decoded object hashes checked against their addresses.
    pub external_objects_sha256_checked: Vec<[u8; 32]>,
    /// Object addresses read with external hash verification disabled.
    pub external_objects_unchecked: Vec<[u8; 32]>,
}
