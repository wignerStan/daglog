//! Deterministic atoms (FNV-1a digest, JSONL line framing). No business types.
//!
//! Two atoms: a stable content digest (FNV-1a 64-bit, the same scheme the
//! source project uses — fast, dependency-free, deterministic across machines)
//! and JSONL line framing (one compact JSON object per `\n`-terminated line).

/// Compute the FNV-1a 64-bit digest of `data`, returned as the canonical
/// prefixed hex string (`fnv1a64:0123...`).
///
/// Deterministic and portable: the same bytes always produce the same hash on
/// every machine, which is what a content-addressed event log needs. Not a
/// cryptographic hash — it is a tamper-*detection* chain over a trusted-append
/// store, not a security boundary.
#[must_use]
pub fn stable_digest(bytes: impl AsRef<[u8]>) -> String {
    // FNV-1a 64-bit: offset basis 0xcbf29ce484222325, prime 0x100000001b3.
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in bytes.as_ref() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    format!("fnv1a64:{hash:016x}")
}

/// Frame one JSON-serializable value as a JSONL line: compact serialization
/// plus a trailing newline. Returns the raw bytes ready to append.
///
/// # Errors
/// Propagates `serde_json::Error` if `value` cannot be serialized.
pub fn frame_line<T: serde::Serialize>(value: &T) -> Result<Vec<u8>, serde_json::Error> {
    let mut line = serde_json::to_vec(value)?;
    line.push(b'\n');
    Ok(line)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stable_digest_is_deterministic_and_prefixed() {
        let a = stable_digest(b"hello");
        let b = stable_digest(b"hello");
        assert_eq!(a, b);
        assert!(a.starts_with("fnv1a64:"));
        assert_ne!(stable_digest(b"hello"), stable_digest(b"hellp"));
    }

    #[test]
    fn frame_line_ends_with_newline() {
        let v = serde_json::json!({"a": 1});
        let line = frame_line(&v).unwrap();
        assert!(line.ends_with(b"\n"));
        assert_eq!(line, b"{\"a\":1}\n");
    }
}
