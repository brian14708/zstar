//! Focused metadata configuration for the ordinary reader and writer interfaces.
use crate::{ManifestOptions, SpillFactory, SpoolBudget};
use std::sync::Arc;

/// Temporary metadata storage, independent of retained index and content memory.
#[derive(Clone, Debug)]
pub struct SpoolOptions {
    pub memory_bytes: u64,
    pub allow_disk: bool,
    pub budget: Arc<SpoolBudget>,
    pub create_store: Option<SpillFactory>,
}
impl Default for SpoolOptions {
    fn default() -> Self {
        let options = ManifestOptions::default();
        Self {
            memory_bytes: options.memory_bytes,
            allow_disk: options.allow_disk,
            budget: options.spool_budget,
            create_store: options.create_spill_store,
        }
    }
}
#[cfg(feature = "async")]
impl SpoolOptions {
    fn apply(self, options: &mut ManifestOptions) {
        options.memory_bytes = self.memory_bytes;
        options.allow_disk = self.allow_disk;
        options.spool_budget = self.budget;
        options.create_spill_store = self.create_store;
    }
}

/// Reader metadata and index budgets. Content limits belong to the reader policy.
#[derive(Clone, Debug)]
pub struct MetadataReaderOptions {
    pub max_encoded_bytes: u64,
    pub max_decoded_bytes: u64,
    pub max_retained_entry_bytes: u64,
    pub max_entries: u64,
    pub spool: SpoolOptions,
}
impl Default for MetadataReaderOptions {
    fn default() -> Self {
        let options = ManifestOptions::default();
        Self {
            max_encoded_bytes: options.max_encoded_bytes,
            max_decoded_bytes: options.max_decoded_bytes,
            max_retained_entry_bytes: options.max_retained_entry_bytes,
            max_entries: options.max_entries,
            spool: SpoolOptions::default(),
        }
    }
}
#[cfg(feature = "async")]
impl MetadataReaderOptions {
    pub(crate) fn apply(self, options: &mut ManifestOptions) {
        options.max_encoded_bytes = self.max_encoded_bytes;
        options.max_decoded_bytes = self.max_decoded_bytes;
        options.max_retained_entry_bytes = self.max_retained_entry_bytes;
        options.max_entries = self.max_entries;
        self.spool.apply(options);
    }
}

/// Writer metadata encoding and spool budgets, without reader-only settings.
#[derive(Clone, Debug)]
pub struct MetadataWriterOptions {
    pub max_encoded_bytes: u64,
    pub max_decoded_bytes: u64,
    pub block_target: usize,
    pub spool: SpoolOptions,
}
impl Default for MetadataWriterOptions {
    fn default() -> Self {
        let options = ManifestOptions::default();
        Self {
            max_encoded_bytes: options.max_encoded_bytes,
            max_decoded_bytes: options.max_decoded_bytes,
            block_target: options.block_target,
            spool: SpoolOptions::default(),
        }
    }
}
#[cfg(feature = "async")]
impl MetadataWriterOptions {
    pub(crate) fn apply(self, options: &mut ManifestOptions) {
        options.max_encoded_bytes = self.max_encoded_bytes;
        options.max_decoded_bytes = self.max_decoded_bytes;
        options.block_target = self.block_target;
        self.spool.apply(options);
    }
}
