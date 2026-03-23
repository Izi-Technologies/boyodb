import sys

def patch_file():
    with open('crates/boyodb-core/src/engine.rs', 'r') as f:
        content = f.read()

    # Apply chunk 1: matching.is_empty()
    old_chunk1 = """        if matching.is_empty() {
            return Err(EngineError::NotFound(format!(
                "no segments for {}.{} matching filters",
                db.as_deref().unwrap_or("default"),
                table.as_deref().unwrap_or("unknown")
            )));
        }"""
        
    new_chunk1 = """        if matching.is_empty() {
            if let Some(schema_specs) = &expected_schema {
                let arrow_schema = crate::engine::schema_to_arrow(schema_specs)
                    .map_err(|e| EngineError::Internal(format!("schema error: {e}")))?;
                let empty_batch = arrow::record_batch::RecordBatch::new_empty(std::sync::Arc::new(arrow_schema));
                let mut w = StreamWriter::try_new(&mut *writer, &empty_batch.schema())
                    .map_err(|e| EngineError::Internal(format!("ipc writer init failed: {e}")))?;
                w.write(&empty_batch)
                    .map_err(|e| EngineError::Internal(format!("ipc write failed: {e}")))?;
                w.finish()
                    .map_err(|e| EngineError::Internal(format!("ipc finish failed: {e}")))?;
            }
            return Ok(QueryResponse {
                records_ipc: Vec::new(),
                data_skipped_bytes: 0,
                segments_scanned: 0,
                execution_stats: None,
            });
        }"""
        
    if old_chunk1 in content:
        content = content.replace(old_chunk1, new_chunk1)
    else:
        print("Chunk 1 not found!")
        sys.exit(1)

    # Apply chunk 2: stream_writer finish
    old_chunk2 = """        if let Some(mut w) = stream_writer {
            w.finish()
                .map_err(|e| EngineError::Internal(format!("ipc writer finish failed: {e}")))?;
        }

        let mut stats = self.metrics.get_stats();"""
        
    new_chunk2 = """        if let Some(mut w) = stream_writer {
            w.finish()
                .map_err(|e| EngineError::Internal(format!("ipc writer finish failed: {e}")))?;
        } else {
            if let Some(schema_specs) = &expected_schema {
                let arrow_schema = crate::engine::schema_to_arrow(schema_specs)
                    .map_err(|e| EngineError::Internal(format!("schema error: {e}")))?;
                let empty_batch = arrow::record_batch::RecordBatch::new_empty(std::sync::Arc::new(arrow_schema));
                let mut w = StreamWriter::try_new(&mut *writer, &empty_batch.schema())
                    .map_err(|e| EngineError::Internal(format!("ipc writer init failed: {e}")))?;
                w.write(&empty_batch)
                    .map_err(|e| EngineError::Internal(format!("ipc write failed: {e}")))?;
                w.finish()
                    .map_err(|e| EngineError::Internal(format!("ipc finish failed: {e}")))?;
            }
        }

        let mut stats = self.metrics.get_stats();"""
        
    if old_chunk2 in content:
        content = content.replace(old_chunk2, new_chunk2)
    else:
        print("Chunk 2 not found!")
        sys.exit(1)

    # Apply chunk 3: VectorSimilarity
    old_chunk3 = """    match func {
        // String functions
        ScalarFunction::Upper(expr) => {"""
        
    new_chunk3 = """    match func {
        // Vector/AI Functions
        ScalarFunction::VectorSimilarity {
            vec1: _,
            vec2: _,
        } => Ok(ComputedValue::Float(0.0)),

        // String functions
        ScalarFunction::Upper(expr) => {"""
        
    if old_chunk3 in content:
        content = content.replace(old_chunk3, new_chunk3)
    else:
        print("Chunk 3 not found!")
        sys.exit(1)

    with open('crates/boyodb-core/src/engine.rs', 'w') as f:
        f.write(content)
    
    print("Patched successfully!")

patch_file()
