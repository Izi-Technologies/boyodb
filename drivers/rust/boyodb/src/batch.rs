//! Batch insert utilities for BoyoDB.
//!
//! Provides efficient bulk data insertion using Arrow RecordBatches.

use crate::client::Client;
use crate::error::Error;

use arrow_array::builder::{
    BooleanBuilder, Float64Builder, Int64Builder, StringBuilder, TimestampMicrosecondBuilder,
};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema};
use std::io::Cursor;
use std::sync::Arc;

/// A simple batch inserter that builds Arrow data and sends it to the server.
pub struct BatchInserter<'a> {
    client: &'a Client,
    database: String,
    table: String,
    schema: Arc<Schema>,
    batch_size: usize,
    current_batch: Vec<Vec<Value>>,
}

/// Values that can be inserted into a batch.
#[derive(Debug, Clone)]
pub enum Value {
    Null,
    Bool(bool),
    Int64(i64),
    Float64(f64),
    String(String),
    TimestampMicros(i64),
    Bytes(Vec<u8>),
}

impl<'a> BatchInserter<'a> {
    /// Create a new batch inserter.
    pub fn new(
        client: &'a Client,
        database: &str,
        table: &str,
        schema: Arc<Schema>,
        batch_size: usize,
    ) -> Self {
        Self {
            client,
            database: database.to_string(),
            table: table.to_string(),
            schema,
            batch_size,
            current_batch: Vec::with_capacity(batch_size),
        }
    }

    /// Add a row to the batch.
    pub fn add_row(&mut self, values: Vec<Value>) -> Result<(), Error> {
        if values.len() != self.schema.fields().len() {
            return Err(Error::Batch(format!(
                "Expected {} values, got {}",
                self.schema.fields().len(),
                values.len()
            )));
        }
        self.current_batch.push(values);
        Ok(())
    }

    /// Flush the current batch to the server.
    pub async fn flush(&mut self) -> Result<usize, Error> {
        if self.current_batch.is_empty() {
            return Ok(0);
        }

        let batch = self.build_record_batch()?;
        let row_count = batch.num_rows();

        // Convert to Arrow IPC
        let ipc_data = record_batch_to_ipc(&batch)?;

        // Send to server
        self.client
            .ingest_ipc(&self.database, &self.table, &ipc_data)
            .await?;

        self.current_batch.clear();
        Ok(row_count)
    }

    /// Check if a flush is needed based on batch size.
    pub fn should_flush(&self) -> bool {
        self.current_batch.len() >= self.batch_size
    }

    /// Add a row and automatically flush if batch size is reached.
    pub async fn add_row_and_maybe_flush(&mut self, values: Vec<Value>) -> Result<usize, Error> {
        self.add_row(values)?;
        if self.should_flush() {
            self.flush().await
        } else {
            Ok(0)
        }
    }

    /// Get the number of pending rows.
    pub fn pending_rows(&self) -> usize {
        self.current_batch.len()
    }

    fn build_record_batch(&self) -> Result<RecordBatch, Error> {
        let num_rows = self.current_batch.len();
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(self.schema.fields().len());

        for (col_idx, field) in self.schema.fields().iter().enumerate() {
            let array = self.build_array(field, col_idx, num_rows)?;
            arrays.push(array);
        }

        RecordBatch::try_new(self.schema.clone(), arrays)
            .map_err(|e| Error::Batch(format!("Failed to create record batch: {}", e)))
    }

    fn build_array(&self, field: &Field, col_idx: usize, num_rows: usize) -> Result<ArrayRef, Error> {
        match field.data_type() {
            DataType::Int64 => {
                let mut builder = Int64Builder::with_capacity(num_rows);
                for row in &self.current_batch {
                    match &row[col_idx] {
                        Value::Int64(v) => builder.append_value(*v),
                        Value::Null => builder.append_null(),
                        v => {
                            return Err(Error::Batch(format!(
                                "Expected Int64 for column {}, got {:?}",
                                field.name(),
                                v
                            )))
                        }
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            DataType::Float64 => {
                let mut builder = Float64Builder::with_capacity(num_rows);
                for row in &self.current_batch {
                    match &row[col_idx] {
                        Value::Float64(v) => builder.append_value(*v),
                        Value::Int64(v) => builder.append_value(*v as f64),
                        Value::Null => builder.append_null(),
                        v => {
                            return Err(Error::Batch(format!(
                                "Expected Float64 for column {}, got {:?}",
                                field.name(),
                                v
                            )))
                        }
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            DataType::Utf8 => {
                let mut builder = StringBuilder::with_capacity(num_rows, num_rows * 32);
                for row in &self.current_batch {
                    match &row[col_idx] {
                        Value::String(v) => builder.append_value(v),
                        Value::Null => builder.append_null(),
                        v => {
                            return Err(Error::Batch(format!(
                                "Expected String for column {}, got {:?}",
                                field.name(),
                                v
                            )))
                        }
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            DataType::Boolean => {
                let mut builder = BooleanBuilder::with_capacity(num_rows);
                for row in &self.current_batch {
                    match &row[col_idx] {
                        Value::Bool(v) => builder.append_value(*v),
                        Value::Null => builder.append_null(),
                        v => {
                            return Err(Error::Batch(format!(
                                "Expected Boolean for column {}, got {:?}",
                                field.name(),
                                v
                            )))
                        }
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            DataType::Timestamp(_, _) => {
                let mut builder = TimestampMicrosecondBuilder::with_capacity(num_rows);
                for row in &self.current_batch {
                    match &row[col_idx] {
                        Value::TimestampMicros(v) => builder.append_value(*v),
                        Value::Int64(v) => builder.append_value(*v),
                        Value::Null => builder.append_null(),
                        v => {
                            return Err(Error::Batch(format!(
                                "Expected Timestamp for column {}, got {:?}",
                                field.name(),
                                v
                            )))
                        }
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            dt => Err(Error::Batch(format!("Unsupported data type: {:?}", dt))),
        }
    }
}

/// Convert a RecordBatch to Arrow IPC format.
pub fn record_batch_to_ipc(batch: &RecordBatch) -> Result<Vec<u8>, Error> {
    let mut buffer = Cursor::new(Vec::new());
    {
        let mut writer = StreamWriter::try_new(&mut buffer, batch.schema().as_ref())
            .map_err(|e| Error::Batch(format!("Failed to create IPC writer: {}", e)))?;
        writer
            .write(batch)
            .map_err(|e| Error::Batch(format!("Failed to write batch: {}", e)))?;
        writer
            .finish()
            .map_err(|e| Error::Batch(format!("Failed to finish IPC: {}", e)))?;
    }
    Ok(buffer.into_inner())
}

/// Insert a RecordBatch directly into a table.
pub async fn insert_record_batch(
    client: &Client,
    database: &str,
    table: &str,
    batch: &RecordBatch,
) -> Result<usize, Error> {
    let ipc_data = record_batch_to_ipc(batch)?;
    let row_count = batch.num_rows();
    client.ingest_ipc(database, table, &ipc_data).await?;
    Ok(row_count)
}

/// Insert multiple RecordBatches into a table.
pub async fn insert_record_batches(
    client: &Client,
    database: &str,
    table: &str,
    batches: &[RecordBatch],
) -> Result<usize, Error> {
    let mut total_rows = 0;
    for batch in batches {
        total_rows += insert_record_batch(client, database, table, batch).await?;
    }
    Ok(total_rows)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_value_creation() {
        let _null = Value::Null;
        let _int = Value::Int64(42);
        let _float = Value::Float64(3.14);
        let _string = Value::String("hello".into());
        let _bool = Value::Bool(true);
        let _ts = Value::TimestampMicros(1234567890);
    }

    #[test]
    fn test_record_batch_to_ipc() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        let id_array: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]));
        let name_array: ArrayRef = Arc::new(StringArray::from(vec!["a", "b", "c"]));

        let batch = RecordBatch::try_new(schema, vec![id_array, name_array]).unwrap();
        let ipc = record_batch_to_ipc(&batch).unwrap();

        assert!(!ipc.is_empty());
        // IPC stream starts with schema message
        assert!(ipc.len() > 100);
    }

    use arrow_array::{Int64Array, StringArray};
}
