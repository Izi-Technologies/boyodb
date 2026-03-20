use arrow::array::{Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use base64::{engine::general_purpose, Engine as _};
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use serde::{Deserialize, Serialize};
use std::io::Cursor;
use std::sync::Arc;

#[derive(Serialize, Deserialize)]
struct LegacyResponse {
    status: String,
    ipc_base64: Option<String>,
}

#[derive(Serialize, Deserialize)]
struct ZeroCopyResponse<'a> {
    status: String,
    ipc_len: Option<u64>,
    #[serde(skip)]
    payload: Option<&'a [u8]>,
}

fn generate_payload(rows: usize) -> Vec<u8> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Float64, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    let ids = Arc::new(Int64Array::from_iter_values(0..rows as i64));
    let vals = Arc::new(Float64Array::from_iter_values(
        (0..rows).map(|i| i as f64 * 1.5),
    ));
    let names = Arc::new(StringArray::from_iter_values(
        (0..rows).map(|i| format!("name_{i}")),
    ));

    let batch = RecordBatch::try_new(schema.clone(), vec![ids, vals, names]).unwrap();

    let mut buf = Vec::new();
    let mut writer = StreamWriter::try_new(&mut buf, &schema).unwrap();
    writer.write(&batch).unwrap();
    writer.finish().unwrap();
    drop(writer);
    buf
}

fn bench_ipc_streaming(c: &mut Criterion) {
    let mut group = c.benchmark_group("Distributed_Query_Merge");

    // Test different payload sizes: 1k rows, 10k rows, 100k rows
    for row_count in [1000, 10_000, 100_000].iter() {
        let raw_payload = generate_payload(*row_count);
        let payload_size = raw_payload.len() as u64;

        group.throughput(Throughput::Bytes(payload_size));

        // 1. Simulate legacy approach: Base64 String -> JSON Response -> Deserialize JSON -> Base64 Decode
        let b64_encoded = general_purpose::STANDARD.encode(&raw_payload);
        let legacy_resp = LegacyResponse {
            status: "ok".to_string(),
            ipc_base64: Some(b64_encoded.clone()),
        };
        let legacy_json = serde_json::to_vec(&legacy_resp).unwrap();

        group.bench_with_input(
            BenchmarkId::new("Legacy_Base64_Embedded", row_count),
            &legacy_json,
            |b, json_bytes| {
                b.iter(|| {
                    // Simulating the exact steps former executor_distributed.rs did:
                    let parsed: LegacyResponse = serde_json::from_slice(json_bytes).unwrap();
                    let decoded_ipc = general_purpose::STANDARD
                        .decode(parsed.ipc_base64.unwrap())
                        .unwrap();
                    black_box(decoded_ipc);
                });
            },
        );

        // 2. Simulate zero-copy streaming: JSON Header -> Deserialize JSON -> Read exact TCP buffer -> Vector Copy
        let zc_resp = ZeroCopyResponse {
            status: "ok".to_string(),
            ipc_len: Some(payload_size),
            payload: None,
        };
        let zc_json = serde_json::to_vec(&zc_resp).unwrap();

        // Let's create a simulated TCP network buffer stream: [JSON bytes] + [Raw IPC bytes]
        let mut simulated_tcp_buffer = Vec::new();
        simulated_tcp_buffer.extend_from_slice(&zc_json);
        simulated_tcp_buffer.extend_from_slice(&raw_payload);

        group.bench_with_input(
            BenchmarkId::new("Zero_Copy_Trailing_Target", row_count),
            &simulated_tcp_buffer,
            |b, tcp_buf| {
                let json_len = zc_json.len();
                b.iter(|| {
                    // Simulating the exact steps new executor_distributed.rs does:
                    let parsed: ZeroCopyResponse =
                        serde_json::from_slice(&tcp_buf[..json_len]).unwrap();

                    let ipc_len = parsed.ipc_len.unwrap() as usize;

                    // Simulated stream.read_exact behavior (memcpy from socket to active buffer)
                    let mut decoded_ipc = vec![0u8; ipc_len];
                    decoded_ipc.copy_from_slice(&tcp_buf[json_len..json_len + ipc_len]);

                    black_box(decoded_ipc);
                });
            },
        );
    }
    group.finish();
}

criterion_group!(benches, bench_ipc_streaming);
criterion_main!(benches);
