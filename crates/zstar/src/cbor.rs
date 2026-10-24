//! Restricted deterministic CBOR, validated before serde can discard duplicates.
use crate::{Error, Result};
use ciborium::Value;

fn invalid(message: &str) -> Error {
    Error::MalformedToc(message.into())
}

pub(crate) fn decode(bytes: &[u8]) -> Result<Value> {
    let mut parser = SequenceDecoder::<_, true>::new(bytes, bytes.len() as u64);
    let value = parser.next()?.ok_or_else(|| invalid("empty CBOR"))?;
    if parser.next()?.is_some() {
        return Err(invalid("trailing CBOR bytes"));
    }
    Ok(value)
}

// Complete blocks benefit from bounded preallocation. Streaming documents
// retain Vec's growth policy; exact reservations regress large archive opens.
pub(crate) struct SequenceDecoder<R, const PREALLOCATE: bool = false> {
    reader: R,
    remaining: u64,
    items: usize,
    // Per-top-level-item byte caps, enforced before any allocation. The
    // first item (manifest header) and later items (entries) differ.
    first_max: u64,
    item_max: u64,
    index: u64,
    consumed: u64,
}
impl<R: std::io::Read, const PREALLOCATE: bool> SequenceDecoder<R, PREALLOCATE> {
    pub(crate) fn new(reader: R, remaining: u64) -> Self {
        Self {
            reader,
            remaining,
            items: 0,
            first_max: u64::MAX,
            item_max: u64::MAX,
            index: 0,
            consumed: 0,
        }
    }
    /// Cap the encoded bytes of the first top-level item and of every later one.
    pub(crate) fn with_item_limits(mut self, first_max: u64, item_max: u64) -> Self {
        self.first_max = first_max;
        self.item_max = item_max;
        self
    }
    fn charge(&mut self, len: u64) -> Result<()> {
        let cap = if self.index == 0 {
            self.first_max
        } else {
            self.item_max
        };
        self.consumed = self.consumed.saturating_add(len);
        if self.consumed > cap {
            // Entries have a normative format bound (malformed); the header's
            // bound is a local budget.
            return Err(if self.index == 0 {
                Error::Budget("manifest header size")
            } else {
                invalid("CBOR item exceeds size maximum")
            });
        }
        Ok(())
    }
    pub(crate) fn next(&mut self) -> Result<Option<Value>> {
        if self.remaining == 0 {
            return Ok(None);
        }
        self.items = 0;
        self.consumed = 0;
        let value = self.item(0)?;
        self.index += 1;
        Ok(Some(value))
    }
    fn take(&mut self, len: usize) -> Result<Vec<u8>> {
        self.charge(len as u64)?;
        if len as u64 > self.remaining {
            return Err(invalid("truncated CBOR"));
        }
        let mut value = vec![0; len];
        self.reader.read_exact(&mut value)?;
        self.remaining -= len as u64;
        Ok(value)
    }
    // Bytes still permitted for the current top-level item.
    fn item_room(&self) -> u64 {
        let cap = if self.index == 0 {
            self.first_max
        } else {
            self.item_max
        };
        cap.saturating_sub(self.consumed)
    }
    fn fixed<const N: usize>(&mut self) -> Result<[u8; N]> {
        self.charge(N as u64)?;
        if N as u64 > self.remaining {
            return Err(invalid("truncated CBOR"));
        }
        let mut bytes = [0; N];
        self.reader.read_exact(&mut bytes)?;
        self.remaining -= N as u64;
        Ok(bytes)
    }
    fn item(&mut self, depth: usize) -> Result<Value> {
        if depth > 64 || self.items >= 16_000_000 {
            return Err(Error::Budget("CBOR nesting or item budget"));
        }
        self.items += 1;
        let head = self.fixed::<1>()?[0];
        let major = head >> 5;
        let ai = head & 31;
        if major == 7 {
            return match ai {
                20 => Ok(Value::Bool(false)),
                21 => Ok(Value::Bool(true)),
                _ => Err(invalid("unsupported CBOR simple value")),
            };
        }
        let n = match ai {
            0..=23 => u64::from(ai),
            24 => {
                let n = u64::from(self.fixed::<1>()?[0]);
                if n < 24 {
                    return Err(invalid("nonpreferred CBOR width"));
                }
                n
            }
            25 => {
                let n = u64::from(u16::from_be_bytes(self.fixed::<2>()?));
                if n <= 255 {
                    return Err(invalid("nonpreferred CBOR width"));
                }
                n
            }
            26 => {
                let n = u64::from(u32::from_be_bytes(self.fixed::<4>()?));
                if n <= 65535 {
                    return Err(invalid("nonpreferred CBOR width"));
                }
                n
            }
            27 => {
                let n = u64::from_be_bytes(self.fixed::<8>()?);
                if u32::try_from(n).is_ok() {
                    return Err(invalid("nonpreferred CBOR width"));
                }
                n
            }
            _ => return Err(invalid("indefinite or reserved CBOR encoding")),
        };
        match major {
            0 => Ok(Value::Integer(n.into())),
            1 if i64::try_from(n).is_ok() => Ok(Value::Integer(
                (-1_i128 - i128::from(n)).try_into().expect("CBOR int64"),
            )),
            2 | 3 => {
                let len = usize::try_from(n).map_err(|_| invalid("CBOR length overflow"))?;
                let raw = self.take(len)?;
                if major == 2 {
                    Ok(Value::Bytes(raw))
                } else {
                    Ok(Value::Text(
                        String::from_utf8(raw).map_err(|_| invalid("invalid UTF-8"))?,
                    ))
                }
            }
            4 => {
                if n > self.remaining || n > self.item_room() {
                    return Err(invalid("CBOR array length"));
                }
                // Avoid repeated growth for ordinary tuples without trusting
                // an untrusted length to request an arbitrarily large buffer.
                let mut values = if PREALLOCATE {
                    Vec::with_capacity(n.min(1024) as usize)
                } else {
                    Vec::new()
                };
                for _ in 0..n {
                    values.push(self.item(depth + 1)?);
                }
                Ok(Value::Array(values))
            }
            5 => {
                if n > self.remaining / 2 || n > self.item_room() / 2 {
                    return Err(invalid("CBOR map length"));
                }
                let mut values: Vec<(Value, Value)> = if PREALLOCATE {
                    Vec::with_capacity(n.min(1024) as usize)
                } else {
                    Vec::new()
                };
                for _ in 0..n {
                    let key = self.item(depth + 1)?;
                    let Value::Text(text) = &key else {
                        return Err(invalid("CBOR map key is not text"));
                    };
                    // Preferred CBOR text heads sort by UTF-8 byte length,
                    // then by bytes. Borrow the previous key instead of
                    // allocating and re-encoding both keys for every pair.
                    if values.last().is_some_and(|(prev, _)| {
                        let Value::Text(prev) = prev else {
                            unreachable!()
                        };
                        text_order(prev, text).is_ge()
                    }) {
                        return Err(invalid("duplicate or unordered CBOR key"));
                    }
                    values.push((key, self.item(depth + 1)?));
                }
                Ok(Value::Map(values))
            }
            _ => Err(invalid("unsupported CBOR type or integer")),
        }
    }
}

#[cfg(test)]
pub(crate) fn encode(value: &Value) -> Result<Vec<u8>> {
    match value {
        Value::Array(items) => {
            let mut out = head(4, items.len() as u64);
            for item in items {
                out.extend(encode(item)?);
            }
            Ok(out)
        }
        Value::Map(items) => {
            let mut encoded = Vec::new();
            for (key, value) in items {
                encoded.push((encode(key)?, encode(value)?));
            }
            encoded.sort_by(|a, b| a.0.cmp(&b.0));
            let mut out = head(5, encoded.len() as u64);
            for (key, value) in encoded {
                out.extend(key);
                out.extend(value);
            }
            Ok(out)
        }
        _ => {
            let mut out = Vec::new();
            ciborium::ser::into_writer(value, &mut out).map_err(|e| invalid(&e.to_string()))?;
            Ok(out)
        }
    }
}

// Validate producer values directly. This mirrors the wire parser's restricted
// types and resource bounds without allocating an encoded/decoded value tree.
pub(crate) fn encoded_len(value: &Value) -> Result<usize> {
    encoded_len_at(value, 0)
}
pub(crate) fn encoded_len_at(value: &Value, depth: usize) -> Result<usize> {
    fn visit(value: &Value, depth: usize, items: &mut usize) -> Result<usize> {
        if depth > 64 || *items >= 16_000_000 {
            return Err(Error::Budget("CBOR nesting or item budget"));
        }
        *items += 1;
        Ok(match value {
            Value::Integer(n) if i128::from(*n) >= 0 => {
                head_len(u64::try_from(i128::from(*n)).map_err(|_| invalid("uint64"))?)
            }
            Value::Integer(n) => head_len(
                u64::try_from(-1 - i128::from(*n))
                    .ok()
                    .filter(|v| i64::try_from(*v).is_ok())
                    .ok_or_else(|| invalid("int64"))?,
            ),
            Value::Bool(_) => 1,
            Value::Text(s) => head_len(s.len() as u64) + s.len(),
            Value::Bytes(b) => head_len(b.len() as u64) + b.len(),
            Value::Array(values) => {
                let mut len = head_len(values.len() as u64);
                for value in values {
                    len += visit(value, depth + 1, items)?;
                }
                len
            }
            Value::Map(values) => {
                let mut len = head_len(values.len() as u64);
                // Most attributes/root maps have only a few keys. A bounded
                // linear check avoids a hash-table allocation in those cases.
                let mut keys = (values.len() > 8)
                    .then(|| std::collections::HashSet::with_capacity(values.len()));
                for (i, (key, value)) in values.iter().enumerate() {
                    let Value::Text(key_text) = key else {
                        return Err(invalid("CBOR map key must be text"));
                    };
                    let duplicate = if let Some(keys) = &mut keys {
                        !keys.insert(key_text.as_str())
                    } else {
                        values[..i].iter().any(|(previous, _)| previous == key)
                    };
                    if duplicate {
                        return Err(invalid("duplicate CBOR map key"));
                    }
                    len += visit(key, depth + 1, items)? + visit(value, depth + 1, items)?;
                }
                len
            }
            _ => return Err(invalid("unsupported CBOR type or integer")),
        })
    }
    visit(value, depth, &mut 0)
}
pub(crate) fn head_len(n: u64) -> usize {
    if n < 24 {
        1
    } else if n <= 255 {
        2
    } else if n <= 65535 {
        3
    } else if u32::try_from(n).is_ok() {
        5
    } else {
        9
    }
}

pub(crate) fn append_head(out: &mut Vec<u8>, major: u8, n: u64) {
    match head_len(n) {
        1 => out.push((major << 5) | n as u8),
        2 => out.extend([(major << 5) | 24, n as u8]),
        3 => {
            out.push((major << 5) | 25);
            out.extend((n as u16).to_be_bytes());
        }
        5 => {
            out.push((major << 5) | 26);
            out.extend((n as u32).to_be_bytes());
        }
        _ => {
            out.push((major << 5) | 27);
            out.extend(n.to_be_bytes());
        }
    }
}
pub(crate) fn append_text(out: &mut Vec<u8>, text: &str) {
    append_head(out, 3, text.len() as u64);
    out.extend_from_slice(text.as_bytes());
}
fn text_order(a: &str, b: &str) -> std::cmp::Ordering {
    a.len().cmp(&b.len()).then_with(|| a.cmp(b))
}
// Only values accepted by encoded_len or the restricted wire parser may be
// passed here. Map ordering includes the encoded text-key length heads.
pub(crate) fn append_validated(out: &mut Vec<u8>, value: &Value) {
    match value {
        Value::Integer(n) if i128::from(*n) >= 0 => {
            append_head(
                out,
                0,
                u64::try_from(i128::from(*n)).expect("validated uint64"),
            );
        }
        Value::Integer(n) => {
            append_head(
                out,
                1,
                u64::try_from(-1 - i128::from(*n)).expect("validated int64"),
            );
        }
        Value::Bool(b) => out.push(if *b { 0xf5 } else { 0xf4 }),
        Value::Text(s) => append_text(out, s),
        Value::Bytes(b) => {
            append_head(out, 2, b.len() as u64);
            out.extend_from_slice(b);
        }
        Value::Array(values) => {
            append_head(out, 4, values.len() as u64);
            for value in values {
                append_validated(out, value);
            }
        }
        Value::Map(values) => {
            fn key_text(key: &Value) -> &str {
                match key {
                    Value::Text(text) => text.as_str(),
                    _ => unreachable!(),
                }
            }
            append_head(out, 5, values.len() as u64);
            if values
                .windows(2)
                .all(|pair| text_order(key_text(&pair[0].0), key_text(&pair[1].0)).is_lt())
            {
                for (key, value) in values {
                    append_validated(out, key);
                    append_validated(out, value);
                }
                return;
            }
            let mut pairs: Vec<_> = values.iter().collect();
            pairs.sort_unstable_by(|(a, _), (b, _)| {
                let (Value::Text(a), Value::Text(b)) = (a, b) else {
                    unreachable!()
                };
                text_order(a, b)
            });
            for (key, value) in pairs {
                append_validated(out, key);
                append_validated(out, value);
            }
        }
        _ => unreachable!("unvalidated CBOR value"),
    }
}

pub(crate) fn encode_checked(value: &Value) -> Result<Vec<u8>> {
    let mut out = Vec::with_capacity(encoded_len(value)?);
    append_validated(&mut out, value);
    Ok(out)
}
#[cfg(test)]
fn head(major: u8, n: u64) -> Vec<u8> {
    let mut out = Vec::new();
    if n < 24 {
        out.push((major << 5) | n as u8);
    } else if n <= 255 {
        out.extend([(major << 5) | 24, n as u8]);
    } else if n <= 65535 {
        out.push((major << 5) | 25);
        out.extend((n as u16).to_be_bytes());
    } else if u32::try_from(n).is_ok() {
        out.push((major << 5) | 26);
        out.extend((n as u32).to_be_bytes());
    } else {
        out.push((major << 5) | 27);
        out.extend(n.to_be_bytes());
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn oversized_item_is_rejected_before_allocation() {
        // Claims a 4 GiB byte string but supplies nothing: must fail on the cap
        // (not on read_exact after allocating).
        let mut raw = vec![0x5b];
        raw.extend((4u64 << 30).to_be_bytes());
        let mut d = SequenceDecoder::<_>::new(raw.as_slice(), u64::MAX).with_item_limits(16, 16);
        assert!(matches!(d.next(), Err(Error::Budget(_))));
        let mut d = SequenceDecoder::<_>::new(raw.as_slice(), u64::MAX).with_item_limits(99, 16);
        d.index = 1;
        assert!(matches!(d.next(), Err(Error::MalformedToc(m)) if m.contains("size maximum")));
        // Per-item, not cumulative: many small items pass.
        let many = vec![0x01; 100];
        let mut d = SequenceDecoder::<_>::new(many.as_slice(), 100).with_item_limits(16, 1);
        while d.next().unwrap().is_some() {}
        // Array/map counts are bounded by the item cap.
        let arr = [0x9a, 0x00, 0x10, 0x00, 0x00];
        let mut d = SequenceDecoder::<_>::new(arr.as_slice(), u64::MAX).with_item_limits(16, 16);
        assert!(d.next().is_err());
    }
    #[test]
    fn text_key_order_matches_encoded_bytes_at_width_and_utf8_boundaries() {
        let keys: Vec<_> = ["", "a", "z", "é", "aa", "雪"]
            .into_iter()
            .map(str::to_owned)
            .chain([23, 24, 255, 256, 65535, 65536].map(|len| "x".repeat(len)))
            .collect();
        for a in &keys {
            for b in &keys {
                let a_wire = encode(&Value::Text(a.clone())).unwrap();
                let b_wire = encode(&Value::Text(b.clone())).unwrap();
                assert_eq!(text_order(a, b), a_wire.cmp(&b_wire));
                let mut raw = vec![0xa2];
                raw.extend(a_wire);
                raw.push(0xf5);
                raw.extend(b_wire);
                raw.push(0xf4);
                assert_eq!(decode(&raw).is_ok(), text_order(a, b).is_lt());
                let mut streamed = SequenceDecoder::<_>::new(raw.as_slice(), raw.len() as u64);
                assert_eq!(streamed.next().is_ok(), text_order(a, b).is_lt());
            }
        }
    }
    #[test]
    fn maps_of_all_sizes_reject_duplicates_and_preserve_canonical_encoding() {
        for len in [0, 1, 2, 8, 9, 32] {
            let mut pairs: Vec<_> = (0..len)
                .map(|i| (Value::Text(format!("key-{i:02}")), Value::Bool(true)))
                .collect();
            for reverse in [false, true] {
                if reverse {
                    pairs.reverse();
                }
                let value = Value::Map(pairs.clone());
                let raw = encode_checked(&value).unwrap();
                assert_eq!(raw, encode(&value).unwrap());
                assert_eq!(encode_checked(&decode(&raw).unwrap()).unwrap(), raw);
            }
            if let Some(pair) = pairs.first().cloned() {
                pairs.push(pair);
                assert!(encode_checked(&Value::Map(pairs)).is_err());
            }
        }
    }
    #[test]
    fn rejects_noncanonical_encodings() {
        for bytes in [
            &[0x18, 0x01][..],
            &[0xbf, 0xff],
            &[0xa2, 0x61, b'a', 0x01, 0x61, b'a', 0x02],
            &[0xf6],
            &[0xc0, 0x01],
        ] {
            assert!(decode(bytes).is_err(), "{bytes:?}");
        }
    }
    #[test]
    fn validates_extension_values_recursively() {
        assert!(decode(&[0xa1, 0x61, b'a', 0x81, 0xf5]).is_ok());
        assert!(decode(&[0xa1, 0x61, b'a', 0x81, 0xf6]).is_err());
    }
    #[test]
    fn direct_encoding_matches_wire_parser_and_fallback_encoder() {
        let values = Value::Array(vec![
            Value::Integer(u64::MAX.into()),
            Value::Integer(i64::MIN.into()),
            Value::Integer(23.into()),
            Value::Integer(24.into()),
            Value::Integer(256.into()),
            Value::Integer(65536.into()),
            Value::Integer((1_i128 << 32).try_into().unwrap()),
            Value::Bool(true),
            Value::Bytes(vec![1, 2]),
            Value::Map(vec![
                (Value::Text("z".into()), Value::Text("é".into())),
                (
                    Value::Text("aa".into()),
                    Value::Array(vec![Value::Bool(false)]),
                ),
            ]),
        ]);
        let bytes = encode_checked(&values).unwrap();
        assert_eq!(bytes, encode(&values).unwrap());
        assert_eq!(bytes.len(), encoded_len(&values).unwrap());
        assert_eq!(decode(&bytes).unwrap(), values);
        for value in [
            Value::Null,
            Value::Float(1.0),
            Value::Integer((i128::from(i64::MIN) - 1).try_into().unwrap()),
            Value::Map(vec![(Value::Integer(0.into()), Value::Bool(true))]),
            Value::Map(vec![
                (Value::Text("duplicate".into()), Value::Bool(true)),
                (Value::Text("duplicate".into()), Value::Bool(false)),
            ]),
        ] {
            assert!(encode_checked(&value).is_err());
        }
        let mut nested = Value::Bool(true);
        for _ in 0..64 {
            nested = Value::Array(vec![nested]);
        }
        assert_eq!(decode(&encode_checked(&nested).unwrap()).unwrap(), nested);
        assert!(encode_checked(&Value::Array(vec![nested])).is_err());
    }
}
