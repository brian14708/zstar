//! Bounded append-only storage, shared quotas, and external record sorting.
use super::{ManifestOptions, invalid};
use crate::manifest::BLOCK_TARGET;
use crate::{Error, Result};
use ciborium::Value;
use std::io::{Cursor, Read, Seek, SeekFrom, Write};
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

pub trait SpillStore: Read + Write + Seek + Send {}
impl<T: Read + Write + Seek + Send> SpillStore for T {}

#[derive(Debug)]
pub struct SpoolBudget {
    limit: u64,
    used: AtomicU64,
}
impl SpoolBudget {
    #[must_use]
    pub fn new(limit: u64) -> Self {
        Self {
            limit,
            used: AtomicU64::new(0),
        }
    }
    #[must_use]
    pub fn used(&self) -> u64 {
        self.used.load(Ordering::Acquire)
    }
    #[must_use]
    pub fn limit(&self) -> u64 {
        self.limit
    }
    fn reserve(&self, size: u64) -> Result<()> {
        if size == 0 {
            return Ok(());
        }
        let mut used = self.used.load(Ordering::Acquire);
        loop {
            let next = used
                .checked_add(size)
                .filter(|sum| *sum <= self.limit)
                .ok_or(Error::Budget("aggregate spool storage"))?;
            match self
                .used
                .compare_exchange_weak(used, next, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => return Ok(()),
                Err(current) => used = current,
            }
        }
    }
    fn release(&self, size: u64) {
        if size == 0 {
            return;
        }
        let previous = self.used.fetch_sub(size, Ordering::AcqRel);
        debug_assert!(previous >= size);
    }
}
struct Reservation<'a> {
    budget: &'a SpoolBudget,
    size: u64,
}
impl Drop for Reservation<'_> {
    fn drop(&mut self) {
        self.budget.release(self.size);
    }
}

#[derive(Clone)]
pub struct SpillFactory(Arc<dyn Fn() -> Result<Box<dyn SpillStore>> + Send + Sync>);
impl std::fmt::Debug for SpillFactory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SpillFactory(..)")
    }
}
impl SpillFactory {
    pub fn new(factory: impl Fn() -> Result<Box<dyn SpillStore>> + Send + Sync + 'static) -> Self {
        Self(Arc::new(factory))
    }
}

/// An append-only spool. Read positions never change the append position.
pub struct Spool {
    pub(super) store: Box<dyn SpillStore>,
    len: u64,
    pub(super) options: ManifestOptions,
    spilled: bool,
    charged: u64,
}
impl Spool {
    #[must_use]
    pub fn new(options: ManifestOptions) -> Self {
        Self {
            store: Box::new(Cursor::new(Vec::<u8>::new())),
            len: 0,
            options,
            spilled: false,
            charged: 0,
        }
    }
    pub fn with_store(options: ManifestOptions, store: Box<dyn SpillStore>) -> Result<Self> {
        let mut spool = Self {
            store,
            len: 0,
            options,
            spilled: true,
            charged: 0,
        };
        if spool.store.seek(SeekFrom::End(0))? != 0 {
            return Err(Error::Unsupported("spill store must be empty"));
        }
        Ok(spool)
    }
    #[must_use]
    pub fn len(&self) -> u64 {
        self.len
    }
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
    #[must_use]
    pub fn spilled(&self) -> bool {
        self.spilled
    }
    pub fn append(&mut self, bytes: &[u8]) -> Result<u64> {
        self.append_parts(bytes, &[])
    }
    fn append_parts(&mut self, first: &[u8], second: &[u8]) -> Result<u64> {
        let end = self
            .len
            .checked_add(first.len() as u64)
            .and_then(|end| end.checked_add(second.len() as u64))
            .ok_or(Error::Budget("spool size"))?;
        if end > self.options.max_encoded_bytes {
            return Err(Error::Budget("spool size"));
        }
        let previous = self.charged;
        let added = end.saturating_sub(previous);
        self.options.spool_budget.reserve(added)?;
        let mut reservation = Reservation {
            budget: &self.options.spool_budget,
            size: added,
        };
        if !self.spilled && end > self.options.memory_bytes {
            let mut store: Box<dyn SpillStore> =
                if let Some(factory) = &self.options.create_spill_store {
                    (factory.0)()?
                } else {
                    if !self.options.allow_disk {
                        return Err(Error::Budget("manifest memory; supply a spill store"));
                    }
                    #[cfg(feature = "disk")]
                    {
                        Box::new(tempfile::tempfile()?)
                    }
                    #[cfg(not(feature = "disk"))]
                    {
                        return Err(Error::Budget("disk feature is disabled"));
                    }
                };
            if store.seek(SeekFrom::End(0))? != 0 {
                return Err(Error::Unsupported("spill store must be empty"));
            }
            self.store.seek(SeekFrom::Start(0))?;
            std::io::copy(&mut self.store, &mut store)?;
            self.store = store;
            self.spilled = true;
        }
        self.store.seek(SeekFrom::Start(self.len))?;
        // Retain the reservation after a partial write: the store may have
        // acquired bytes beyond the logical end. Drop releases that charge.
        self.charged = previous + added;
        reservation.size = 0;
        self.store.write_all(first)?;
        self.store.write_all(second)?;
        let start = self.len;
        self.len = end;
        Ok(start)
    }
    pub fn read(&mut self, offset: u64, size: u64) -> Result<Vec<u8>> {
        crate::checked_range(offset, size, self.len)?;
        let mut out = vec![0; usize::try_from(size).map_err(|_| Error::Budget("spool range"))?];
        self.store.seek(SeekFrom::Start(offset))?;
        self.store.read_exact(&mut out)?;
        Ok(out)
    }
    pub(crate) fn append_record(&mut self, value: &Value) -> Result<()> {
        let raw = crate::cbor::encode_checked(value)?;
        self.append_encoded_record(&raw)
    }
    pub(crate) fn append_encoded_record(&mut self, raw: &[u8]) -> Result<()> {
        let len = u32::try_from(raw.len()).map_err(|_| invalid("spool record maximum"))?;
        // Reserve and seek once for the length prefix and body, without
        // allocating a combined record buffer.
        self.append_parts(&len.to_le_bytes(), raw)?;
        Ok(())
    }
    pub(crate) fn read_record(&mut self, at: &mut u64) -> Result<Option<Value>> {
        if *at == self.len {
            return Ok(None);
        }
        let size = u64::from(u32::from_le_bytes(self.read(*at, 4)?.try_into().unwrap()));
        *at += 4;
        // Records are entry tuples; reject before allocating the read buffer.
        if size > crate::manifest::ENTRY_MAX as u64 {
            return Err(Error::MalformedToc(
                "spooled record exceeds entry maximum".into(),
            ));
        }
        let raw = self.read(*at, size)?;
        *at += size;
        Ok(Some(crate::cbor::decode(&raw)?))
    }
}
impl Drop for Spool {
    fn drop(&mut self) {
        self.options.spool_budget.release(self.charged);
    }
}

/// Eight-way external merge sort. Both the records and run directory spill;
/// no vector proportional to the complete input is retained.
pub(crate) fn sort_records(
    mut input: Spool,
    options: ManifestOptions,
    key: fn(&Value) -> Result<String>,
) -> Result<Spool> {
    use std::cmp::Reverse;
    use std::collections::BinaryHeap;
    let mut runs = Spool::new(options.clone());
    let mut directory = Spool::new(options.clone());
    let mut at = 0;
    let mut count = 0_u64;
    while at < input.len() {
        let mut records = Vec::new();
        let begin = at;
        while at < input.len() && (at - begin < BLOCK_TARGET as u64 || records.is_empty()) {
            let value = input.read_record(&mut at)?.expect("record before end");
            records.push((key(&value)?, value));
        }
        records.sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
        let start = runs.len();
        for (_, value) in records {
            runs.append_record(&value)?;
        }
        directory.append(&start.to_le_bytes())?;
        directory.append(&runs.len().to_le_bytes())?;
        count += 1;
    }
    drop(input);
    while count > 1 {
        let mut next = Spool::new(options.clone());
        let mut next_directory = Spool::new(options.clone());
        let mut next_count = 0;
        let mut position = 0_u64;
        while position < count {
            let n = (count - position).min(8) as usize;
            let mut cursors = Vec::new();
            let mut values = Vec::new();
            let mut heap = BinaryHeap::new();
            for i in 0..n {
                let bytes = directory.read((position + i as u64) * 16, 16)?;
                let mut at = u64::from_le_bytes(bytes[..8].try_into().unwrap());
                let end = u64::from_le_bytes(bytes[8..].try_into().unwrap());
                let value = runs
                    .read_record(&mut at)?
                    .ok_or_else(|| invalid("empty sort run"))?;
                heap.push(Reverse((key(&value)?, i)));
                cursors.push((at, end));
                values.push(Some(value));
            }
            let start = next.len();
            while let Some(Reverse((_, i))) = heap.pop() {
                next.append_record(&values[i].take().expect("queued sort value"))?;
                let (at, end) = &mut cursors[i];
                if *at < *end {
                    let value = runs
                        .read_record(at)?
                        .ok_or_else(|| invalid("truncated sort run"))?;
                    if *at > *end {
                        return Err(invalid("sort run boundary"));
                    }
                    heap.push(Reverse((key(&value)?, i)));
                    values[i] = Some(value);
                }
            }
            next_directory.append(&start.to_le_bytes())?;
            next_directory.append(&next.len().to_le_bytes())?;
            next_count += 1;
            position += n as u64;
        }
        runs = next;
        directory = next_directory;
        count = next_count;
    }
    Ok(runs)
}
