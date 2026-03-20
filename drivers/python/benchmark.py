import time
import os
import pyarrow as pa
from boyodb.client import Client

def generate_ipc_payload(num_rows: int) -> bytes:
    print(f"Generating synthetic Arrow IPC payload with {num_rows} rows...")
    # Create simple data
    data = [
        pa.array([i for i in range(num_rows)]),
        pa.array([f"value_{i}" for i in range(num_rows)]),
        pa.array([i * 0.1 for i in range(num_rows)])
    ]
    batch = pa.RecordBatch.from_arrays(data, names=['id', 'name', 'value'])
    schema = batch.schema
    
    # Write to IPC stream
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, schema) as writer:
        writer.write_batch(batch)
    
    return sink.getvalue().to_pybytes()

def run_benchmark(host: str = "localhost:8765"):
    client = Client(host=host)
    
    try:
        print("Connecting to BoyoDB...")
        client.connect()
        print("Connected successfully.")
        
        # Prepare Database and Table
        db_name = "benchmark_db"
        table_name = "ingest_speed"
        
        client.exec(f"DROP DATABASE IF EXISTS {db_name}")
        client.create_database(db_name)
        client.create_table(db_name, table_name)
        
        # Payload sizes to test
        sizes = [1000, 10000, 100000, 500000]
        
        print("\n--- Starting Ingestion Benchmark ---\n")
        
        for size in sizes:
            payload = generate_ipc_payload(size)
            payload_size_mb = len(payload) / (1024 * 1024)
            print(f"\nPayload Size: {payload_size_mb:.2f} MB ({size} rows)")
            
            # Since the client now implements zero-copy natively, it automatically drops to base64 if it fails.
            # To specifically benchmark the speed we measure the ingestion latency directly.
            
            attempts = 5
            total_time = 0.0
            
            for i in range(attempts):
                start = time.perf_counter()
                client.ingest_ipc(db_name, table_name, payload)
                elapsed = time.perf_counter() - start
                total_time += elapsed
                
            avg_time = total_time / attempts
            throughput_mbps = payload_size_mb / avg_time
            
            print(f"-> Avg Latency: {avg_time:.4f} seconds")
            print(f"-> Throughput:  {throughput_mbps:.2f} MB/s")
            
    except Exception as e:
        print(f"Benchmark failed: {e}")
    finally:
        client.close()
        print("\nConnection closed.")

if __name__ == "__main__":
    run_benchmark()
