//! Bounded in-memory metadata records and sorting.
use super::invalid;
use crate::{BudgetResource, Error, Result};
use ciborium::Value;

pub struct MetadataBuffer {
    bytes: Vec<u8>,
    max_bytes: u64,
}
impl MetadataBuffer {
    pub const fn new(max_bytes: u64) -> Self {
        Self {
            bytes: Vec::new(),
            max_bytes,
        }
    }
    pub const fn len(&self) -> u64 {
        self.bytes.len() as u64
    }
    pub fn append(&mut self, bytes: &[u8]) -> Result<u64> {
        self.append_parts(bytes, &[])
    }
    fn append_parts(&mut self, first: &[u8], second: &[u8]) -> Result<u64> {
        let size = first.len().checked_add(second.len()).ok_or(Error::Range)?;
        let end = self.bytes.len().checked_add(size).ok_or(Error::Range)?;
        if end as u64 > self.max_bytes {
            return Err(Error::budget_exceeded(
                BudgetResource::MetadataBufferBytes,
                "metadata buffer size",
                self.max_bytes,
                end as u64,
            ));
        }
        self.bytes.try_reserve_exact(size).map_err(|_| {
            Error::budget(
                BudgetResource::AllocationBytes,
                "metadata buffer allocation",
            )
        })?;
        let start = self.len();
        self.bytes.extend_from_slice(first);
        self.bytes.extend_from_slice(second);
        Ok(start)
    }
    pub fn read(&self, offset: u64, size: u64) -> Result<&[u8]> {
        let range = crate::checked_range(offset, size, self.len())?;
        Ok(
            &self.bytes[usize::try_from(range.start).map_err(|_| Error::Range)?
                ..usize::try_from(range.end).map_err(|_| Error::Range)?],
        )
    }
    #[cfg(any(feature = "fs", test))]
    pub(crate) fn append_record(&mut self, value: &Value) -> Result<()> {
        self.append_encoded_record(&crate::cbor::encode_checked(value)?)
    }
    pub(crate) fn append_encoded_record(&mut self, raw: &[u8]) -> Result<()> {
        let len = u32::try_from(raw.len()).map_err(|_| invalid("metadata record maximum"))?;
        self.append_parts(&len.to_le_bytes(), raw)?;
        Ok(())
    }
    pub(crate) fn read_record(&self, at: &mut u64) -> Result<Option<Value>> {
        if *at == self.len() {
            return Ok(None);
        }
        let size = u64::from(u32::from_le_bytes(self.read(*at, 4)?.try_into().unwrap()));
        *at += 4;
        let raw = self.read(*at, size)?;
        *at += size;
        Ok(Some(crate::cbor::decode(raw)?))
    }
}

/// Sort record offsets in memory, then copy their canonical bytes in path order.
#[expect(
    clippy::needless_pass_by_value,
    reason = "Sorting replaces the buffer and releases the unsorted bytes before returning."
)]
pub fn sort_records(
    input: MetadataBuffer,
    key: fn(&Value) -> Result<String>,
) -> Result<MetadataBuffer> {
    let mut records = Vec::new();
    let mut at = 0;
    let mut retained = 0_u64;
    while at < input.len() {
        let start = at;
        let value = input
            .read_record(&mut at)?
            .ok_or_else(|| invalid("metadata record"))?;
        let key = key(&value)?;
        retained = retained
            .saturating_add(key.capacity() as u64)
            .saturating_add(std::mem::size_of::<(String, u64, u64)>() as u64);
        if retained > input.max_bytes {
            return Err(Error::budget_exceeded(
                BudgetResource::MetadataBufferBytes,
                "metadata sorting index",
                input.max_bytes,
                retained,
            ));
        }
        records.try_reserve_exact(1).map_err(|_| {
            Error::budget(BudgetResource::AllocationBytes, "metadata sorting index")
        })?;
        records.push((key, start, at - start));
    }
    records.sort_unstable_by(|a, b| a.0.cmp(&b.0));
    let mut output = MetadataBuffer::new(input.max_bytes);
    for (_, start, size) in records {
        output.append(input.read(start, size)?)?;
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejected_record_leaves_buffer_unchanged() {
        let mut buffer = MetadataBuffer::new(6);
        buffer.append(b"a").unwrap();
        assert!(matches!(
            buffer.append_encoded_record(b"bc"),
            Err(Error::Budget(_))
        ));
        assert_eq!(buffer.len(), 1);
        assert_eq!(buffer.read(0, 1).unwrap(), b"a");
        buffer.append_encoded_record(b"b").unwrap();
        assert_eq!(buffer.read(1, 5).unwrap(), b"\x01\x00\x00\x00b");
    }
}
