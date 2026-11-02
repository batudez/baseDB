//! A C ABI over redb, so the Go benchmark can load it as a DLL.
//!
//! Every write function is one redb write transaction, committed with redb's
//! default durability (Immediate: fsynced before commit returns). Return
//! values: 0 = ok, 1 = found (get), negative = error (printed to stderr).

use redb::{Database, ReadableDatabase, TableDefinition};
use std::slice;

const TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("kv");
const VERSION: &[u8] = b"4.4.0";

pub struct Db {
    db: Database,
}

fn fail<E: std::fmt::Display>(what: &str, e: E) -> i32 {
    eprintln!("redb {what}: {e}");
    -1
}

unsafe fn bytes<'a>(p: *const u8, n: usize) -> &'a [u8] {
    if n == 0 {
        &[]
    } else {
        slice::from_raw_parts(p, n)
    }
}

/// Copies the redb version (no NUL) into out; returns its length.
#[no_mangle]
pub unsafe extern "C" fn baseredb_version(out: *mut u8, cap: usize) -> usize {
    let n = VERSION.len().min(cap);
    std::ptr::copy_nonoverlapping(VERSION.as_ptr(), out, n);
    n
}

/// Opens (or creates) the database file at path (UTF-8, not NUL-terminated).
#[no_mangle]
pub unsafe extern "C" fn baseredb_open(path: *const u8, path_len: usize, cache_bytes: usize) -> *mut Db {
    let path = match std::str::from_utf8(bytes(path, path_len)) {
        Ok(p) => p,
        Err(e) => {
            fail("open", e);
            return std::ptr::null_mut();
        }
    };
    let db = match Database::builder().set_cache_size(cache_bytes).create(path) {
        Ok(db) => db,
        Err(e) => {
            fail("open", e);
            return std::ptr::null_mut();
        }
    };
    // Create the table, so that readers find it from the start.
    let r = db.begin_write().map_err(|e| e.to_string()).and_then(|tx| {
        tx.open_table(TABLE).map_err(|e| e.to_string())?;
        tx.commit().map_err(|e| e.to_string())
    });
    if let Err(e) = r {
        fail("open", e);
        return std::ptr::null_mut();
    }
    Box::into_raw(Box::new(Db { db }))
}

#[no_mangle]
pub unsafe extern "C" fn baseredb_close(db: *mut Db) {
    if !db.is_null() {
        drop(Box::from_raw(db));
    }
}

/// One key, one durable transaction.
#[no_mangle]
pub unsafe extern "C" fn baseredb_put(db: *mut Db, k: *const u8, kl: usize, v: *const u8, vl: usize) -> i32 {
    let db = &(*db).db;
    let r = db.begin_write().map_err(|e| e.to_string()).and_then(|tx| {
        {
            let mut t = tx.open_table(TABLE).map_err(|e| e.to_string())?;
            t.insert(bytes(k, kl), bytes(v, vl)).map_err(|e| e.to_string())?;
        }
        tx.commit().map_err(|e| e.to_string())
    });
    match r {
        Ok(()) => 0,
        Err(e) => fail("put", e),
    }
}

/// Many operations, one durable transaction. ops is a sequence of
/// [kind: u8 (0 put, 1 delete)][key len: u32 LE][value len: u32 LE][key][value].
#[no_mangle]
pub unsafe extern "C" fn baseredb_batch(db: *mut Db, ops: *const u8, len: usize) -> i32 {
    let db = &(*db).db;
    let buf = bytes(ops, len);
    let r = db.begin_write().map_err(|e| e.to_string()).and_then(|tx| {
        {
            let mut t = tx.open_table(TABLE).map_err(|e| e.to_string())?;
            let mut i = 0;
            while i < buf.len() {
                let kind = buf[i];
                let kl = u32::from_le_bytes(buf[i + 1..i + 5].try_into().unwrap()) as usize;
                let vl = u32::from_le_bytes(buf[i + 5..i + 9].try_into().unwrap()) as usize;
                let k = &buf[i + 9..i + 9 + kl];
                let v = &buf[i + 9 + kl..i + 9 + kl + vl];
                i += 9 + kl + vl;
                if kind == 1 {
                    t.remove(k).map_err(|e| e.to_string())?;
                } else {
                    t.insert(k, v).map_err(|e| e.to_string())?;
                }
            }
        }
        tx.commit().map_err(|e| e.to_string())
    });
    match r {
        Ok(()) => 0,
        Err(e) => fail("batch", e),
    }
}

/// Copies the value of k into out (capacity cap) and its length into *out_len.
/// Returns 1 if found, 0 if not, -2 if out is too small (*out_len = needed).
#[no_mangle]
pub unsafe extern "C" fn baseredb_get(
    db: *mut Db,
    k: *const u8,
    kl: usize,
    out: *mut u8,
    cap: usize,
    out_len: *mut usize,
) -> i32 {
    let db = &(*db).db;
    let r = db.begin_read().map_err(|e| e.to_string()).and_then(|tx| {
        let t = tx.open_table(TABLE).map_err(|e| e.to_string())?;
        match t.get(bytes(k, kl)).map_err(|e| e.to_string())? {
            None => Ok(0),
            Some(g) => {
                let v = g.value();
                *out_len = v.len();
                if v.len() > cap {
                    return Ok(-2);
                }
                std::ptr::copy_nonoverlapping(v.as_ptr(), out, v.len());
                Ok(1)
            }
        }
    });
    match r {
        Ok(n) => n,
        Err(e) => fail("get", e),
    }
}

/// Reads up to n keys from start (inclusive) in order; returns how many.
#[no_mangle]
pub unsafe extern "C" fn baseredb_scan(db: *mut Db, start: *const u8, sl: usize, n: usize) -> i64 {
    let db = &(*db).db;
    let r = db.begin_read().map_err(|e| e.to_string()).and_then(|tx| {
        let t = tx.open_table(TABLE).map_err(|e| e.to_string())?;
        let mut count = 0usize;
        for item in t.range(bytes(start, sl)..).map_err(|e| e.to_string())? {
            if count == n {
                break;
            }
            let (k, v) = item.map_err(|e| e.to_string())?;
            let _ = (k.value(), v.value());
            count += 1;
        }
        Ok(count as i64)
    });
    match r {
        Ok(n) => n,
        Err(e) => fail("scan", e) as i64,
    }
}
