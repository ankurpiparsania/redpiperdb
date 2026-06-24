#![deny(clippy::all)]

use napi::bindgen_prelude::*;
use napi_derive::napi;
use redb::{Database, ReadableTable, TableDefinition};
use std::sync::Arc;

const UNCOMPRESSED_FLAG: u8 = 0;
const COMPRESSED_FLAG: u8 = 1;

fn encode_value(data: &[u8], use_compression: bool) -> Vec<u8> {
    if use_compression && data.len() > 64 {
        if let Ok(compressed) = zstd::stream::encode_all(data, 3) {
            let mut out = Vec::with_capacity(compressed.len() + 1);
            out.push(COMPRESSED_FLAG);
            out.extend_from_slice(&compressed);
            return out;
        }
    }
    let mut out = Vec::with_capacity(data.len() + 1);
    out.push(UNCOMPRESSED_FLAG);
    out.extend_from_slice(data);
    out
}

fn decode_value(data: &[u8]) -> Result<Vec<u8>> {
    if data.is_empty() { return Ok(Vec::new()); }
    match data[0] {
        COMPRESSED_FLAG => zstd::stream::decode_all(&data[1..])
            .map_err(|e| Error::from_reason(format!("Decompression failed: {}", e))),
        UNCOMPRESSED_FLAG => Ok(data[1..].to_vec()),
        _ => Ok(data.to_vec()), 
    }
}

#[napi(object)]
pub struct RedbConfig { pub use_zstd: bool }

#[derive(Clone)]
#[napi(object)]
pub struct BatchOp {
    pub op_type: String,
    pub table: String,
    pub key: String,
    pub value: Option<Buffer>,
}

#[derive(Clone)]
#[napi(object)]
pub struct RangeEntry {
    pub key: String,
    pub value: Option<Buffer>,
}

#[derive(Clone)]
#[napi(object)]
pub struct RangeOptions {
    pub start: Option<String>,
    pub end: Option<String>,
    pub limit: Option<u32>,
    pub reverse: Option<bool>,
    pub keys_only: Option<bool>,
}

#[napi]
pub struct RedbDatabase {
    db: Arc<Database>,
    use_zstd: bool,
}

#[napi]
impl RedbDatabase {
    #[napi(constructor)]
    pub fn new(path: String, config: Option<RedbConfig>) -> Result<Self> {
        let db = Database::create(&path).map_err(|e| Error::from_reason(e.to_string()))?;
        let use_zstd = config.map(|c| c.use_zstd).unwrap_or(true);
        Ok(Self { db: Arc::new(db), use_zstd })
    }

    #[napi]
    pub fn create_table(&self, table_name: String) -> Result<()> {
        let table_def: TableDefinition<&str, &[u8]> = TableDefinition::new(&table_name);
        let write_txn = self.db.begin_write().map_err(|e| Error::from_reason(e.to_string()))?;
        { write_txn.open_table(table_def).map_err(|e| Error::from_reason(e.to_string()))?; }
        write_txn.commit().map_err(|e| Error::from_reason(e.to_string()))?;
        Ok(())
    }

    #[napi]
    pub async fn get(&self, table_name: String, key: String) -> Result<Option<Buffer>> {
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || {
            let table_def: TableDefinition<&str, &[u8]> = TableDefinition::new(&table_name);
            let read_txn = db.begin_read().map_err(|e| Error::from_reason(e.to_string()))?;
            let table = read_txn.open_table(table_def).map_err(|e| Error::from_reason(e.to_string()))?;
            
            if let Some(access) = table.get(key.as_str()).map_err(|e| Error::from_reason(e.to_string()))? {
                let decoded = decode_value(access.value())?;
                Ok(Some(Buffer::from(decoded)))
            } else { Ok(None) }
        }).await.unwrap()
    }

    #[napi]
    pub async fn put(&self, table_name: String, key: String, value: Buffer) -> Result<()> {
        let db = self.db.clone();
        let use_zstd = self.use_zstd;
        tokio::task::spawn_blocking(move || {
            let table_def: TableDefinition<&str, &[u8]> = TableDefinition::new(&table_name);
            let write_txn = db.begin_write().map_err(|e| Error::from_reason(e.to_string()))?;
            {
                let mut table = write_txn.open_table(table_def).map_err(|e| Error::from_reason(e.to_string()))?;
                let encoded = encode_value(value.as_ref(), use_zstd);
                table.insert(key.as_str(), encoded.as_slice()).map_err(|e| Error::from_reason(e.to_string()))?;
            }
            write_txn.commit().map_err(|e| Error::from_reason(e.to_string()))?;
            Ok(())
        }).await.unwrap()
    }

    #[napi]
    pub async fn remove(&self, table_name: String, key: String) -> Result<()> {
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || {
            let table_def: TableDefinition<&str, &[u8]> = TableDefinition::new(&table_name);
            let write_txn = db.begin_write().map_err(|e| Error::from_reason(e.to_string()))?;
            {
                let mut table = write_txn.open_table(table_def).map_err(|e| Error::from_reason(e.to_string()))?;
                table.remove(key.as_str()).map_err(|e| Error::from_reason(e.to_string()))?;
            }
            write_txn.commit().map_err(|e| Error::from_reason(e.to_string()))?;
            Ok(())
        }).await.unwrap()
    }

    #[napi]
    pub async fn batch(&self, ops: Vec<BatchOp>) -> Result<()> {
        let db = self.db.clone();
        let use_zstd = self.use_zstd;
        tokio::task::spawn_blocking(move || {
            let write_txn = db.begin_write().map_err(|e| Error::from_reason(e.to_string()))?;
            {
                for op in ops {
                    let table_def: TableDefinition<&str, &[u8]> = TableDefinition::new(&op.table);
                    let mut table = write_txn.open_table(table_def).map_err(|e| Error::from_reason(e.to_string()))?;
                    if op.op_type == "put" {
                        if let Some(val) = &op.value {
                            let encoded = encode_value(val.as_ref(), use_zstd);
                            table.insert(op.key.as_str(), encoded.as_slice()).map_err(|e| Error::from_reason(e.to_string()))?;
                        }
                    } else if op.op_type == "del" {
                        table.remove(op.key.as_str()).map_err(|e| Error::from_reason(e.to_string()))?;
                    }
                }
            }
            write_txn.commit().map_err(|e| Error::from_reason(e.to_string()))?;
            Ok(())
        }).await.unwrap()
    }

    #[napi]
    pub async fn get_range(&self, table_name: String, opts: RangeOptions) -> Result<Vec<RangeEntry>> {
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || {
            let table_def: TableDefinition<&str, &[u8]> = TableDefinition::new(&table_name);
            let read_txn = db.begin_read().map_err(|e| Error::from_reason(e.to_string()))?;
            let table = read_txn.open_table(table_def).map_err(|e| Error::from_reason(e.to_string()))?;
            
            let start = opts.start.as_deref();
            let end = opts.end.as_deref();
            
            let mut iter = match (start, end) {
                (Some(s), Some(e)) => table.range(s..=e).map_err(|e| Error::from_reason(e.to_string()))?,
                (Some(s), None) => table.range(s..).map_err(|e| Error::from_reason(e.to_string()))?,
                (None, Some(e)) => table.range(..=e).map_err(|e| Error::from_reason(e.to_string()))?,
                (None, None) => table.iter().map_err(|e| Error::from_reason(e.to_string()))?,
            };

            let mut results = Vec::new();
            let limit = opts.limit.unwrap_or(u32::MAX) as usize;
            let keys_only = opts.keys_only.unwrap_or(false);

            while let Some(Ok((k_access, v_access))) = iter.next() {
                let key_str = k_access.value().to_string();
                let value_buf = if keys_only { None } else {
                    let decoded = decode_value(v_access.value())?;
                    Some(Buffer::from(decoded))
                };
                results.push(RangeEntry { key: key_str, value: value_buf });
            }

            if opts.reverse.unwrap_or(false) { results.reverse(); }
            if results.len() > limit { results.truncate(limit); }

            Ok(results)
        }).await.unwrap()
    }

    #[napi]
    pub fn close(&self) -> Result<()> { Ok(()) }
}