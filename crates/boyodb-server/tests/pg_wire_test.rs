use std::time::Duration;
use tokio::time::sleep;
use std::process::{Command, Stdio};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;


// Helper to find a free port
fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

#[tokio::test]
async fn test_pg_wire_simple_query() {
    let pg_port = free_port();
    let http_port = free_port();
    let bind_addr = format!("127.0.0.1:{}", http_port);
    let data_dir = tempfile::tempdir().unwrap();
    let data_path = data_dir.path().to_str().unwrap();

    // Build the server binary
    // Assume already built usually, but good to build

    
    // Start server process
    // Start server process (binary directly to avoid cargo lock)
    let binary_path = "../../target/debug/boyodb-server";
    let mut server = Command::new(binary_path)
        .args(&[
            data_path,
            &bind_addr,
            "--pg-port", &pg_port.to_string(),
        ])
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("failed to start server binary");

    let mut connected = false;

    for _ in 0..20 {
        sleep(Duration::from_millis(500)).await;
        // Try connecting
        match TcpStream::connect(format!("127.0.0.1:{}", pg_port)).await {
            Ok(mut stream) => {
                // Send Startup Message
                // Length (4 bytes) + Protocol(196608) + params + null
                let mut buf = Vec::new();
                buf.write_u32(196608).await.unwrap(); // Protocol version 3.0
                buf.extend_from_slice(b"user\0boyo\0database\0default\0\0");
                
                let len = buf.len() + 4;
                let mut prefix = Vec::new();
                prefix.write_u32(len as u32).await.unwrap();
                stream.write_all(&prefix).await.unwrap();
                stream.write_all(&buf).await.unwrap();

                // Expect AuthOk (R) or ReadyForQuery (Z)
                // Now we expect AuthenticationCleartextPassword (R check)
                let mut resp_buf = [0u8; 1024];
                let n = stream.read(&mut resp_buf).await.unwrap();
                
                println!("Received {} bytes: {:?}", n, &resp_buf[..n]);
                
                if n > 0 && resp_buf[0] == b'R' {
                    // Check if it is Auth Cleartext (Type 3) and it comes with length
                    // [ 'R' | len (4) | type (4) ]
                    if n >= 9 && resp_buf[8] == 3 {
                        let password = "boyo";
                        let mut p_buf = Vec::new();
                        p_buf.push(b'p');
                        let p_len = 4 + password.len() + 1;
                        p_buf.write_u32(p_len as u32).await.unwrap();
                        p_buf.extend_from_slice(password.as_bytes());
                        p_buf.push(0);
                        stream.write_all(&p_buf).await.unwrap();
                        
                        // Read subsequent response (AuthOk + KeyData + ReadyForQuery)
                        let n = stream.read(&mut resp_buf).await.unwrap();
                        println!("Received post-auth bytes: {:?}", &resp_buf[..n]);
                    }
                }
                
                // Construct Simple Query "Q"
                // 'Q', len, string query, \0
                let query = "SELECT 1";
                let mut q_buf = Vec::new();
                q_buf.push(b'Q');
                let q_len = 4 + query.len() + 1;
                q_buf.write_u32(q_len as u32).await.unwrap();
                q_buf.extend_from_slice(query.as_bytes());

                q_buf.push(0);
                
                stream.write_all(&q_buf).await.unwrap();

                // Read for RowDescription ('T') and DataRow ('D') and CommandComplete ('C')
                let n = stream.read(&mut resp_buf).await.unwrap();
                println!("Received query response {} bytes: {:?}", n, &resp_buf[..n]);
                
                // Assert no ErrorResponse ('E')
                if resp_buf[0] == b'E' {
                    panic!("Received ErrorResponse for SELECT 1: {:?}", &resp_buf[..n]);
                }
                // Assert we got RowDescription ('T')
                assert_eq!(resp_buf[0], b'T', "Expected RowDescription ('T') for SELECT 1");
                
                connected = true;
                break;
            }
            Err(_) => {}
        }
    }

    server.kill().ok();
    assert!(connected, "Failed to connect and query PG server");
}

#[tokio::test]
async fn test_pg_wire_transaction_stub() {
    let pg_port = free_port();
    let http_port = free_port();
    let bind_addr = format!("127.0.0.1:{}", http_port);
    let data_dir = tempfile::tempdir().unwrap();
    let data_path = data_dir.path().to_str().unwrap();

    let binary_path = "../../target/debug/boyodb-server";
    let mut server = Command::new(binary_path)
        .args(&[
            data_path,
            &bind_addr,
            "--pg-port", &pg_port.to_string(),
        ])
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("failed to start server binary");

    let mut connected = false;

    for _ in 0..20 {
        sleep(Duration::from_millis(500)).await;
        match TcpStream::connect(format!("127.0.0.1:{}", pg_port)).await {
            Ok(mut stream) => {
                // 1. Handshake & Auth
                let mut buf = Vec::new();
                buf.write_u32(196608).await.unwrap(); // Protocol 3.0
                buf.extend_from_slice(b"user\0boyo\0database\0default\0\0");
                
                let len = buf.len() + 4;
                let mut prefix = Vec::new();
                prefix.write_u32(len as u32).await.unwrap();
                stream.write_all(&prefix).await.unwrap();
                stream.write_all(&buf).await.unwrap();

                let mut resp_buf = [0u8; 4096];
                let n = stream.read(&mut resp_buf).await.unwrap();
                if n > 0 && resp_buf[0] == b'R' {
                    if n >= 9 && resp_buf[8] == 3 {
                        let password = "boyo";
                        let mut p_buf = Vec::new();
                        p_buf.push(b'p');
                        let p_len = 4 + password.len() + 1;
                        p_buf.write_u32(p_len as u32).await.unwrap();
                        p_buf.extend_from_slice(password.as_bytes());
                        p_buf.push(0);
                        stream.write_all(&p_buf).await.unwrap();
                        
                        let _ = stream.read(&mut resp_buf).await.unwrap();
                    }
                }

                // 2. Send BEGIN
                let query = "BEGIN";
                let mut q_buf = Vec::new();
                q_buf.push(b'Q');
                let q_len = 4 + query.len() + 1;
                q_buf.write_u32(q_len as u32).await.unwrap();
                q_buf.extend_from_slice(query.as_bytes());
                q_buf.push(0);
                stream.write_all(&q_buf).await.unwrap();

                // Expect CommandComplete ('C')
                let n = stream.read(&mut resp_buf).await.unwrap();
                println!("Received BEGIN response: {:?}", &resp_buf[..n]);
                if resp_buf[0] == b'E' {
                    panic!("Received ErrorResponse for BEGIN: {:?}", &resp_buf[..n]);
                }
                // Verify CommandComplete ('C')
                assert!((resp_buf[0] == b'C') || (resp_buf[0] == b'Z'), "Expected CommandComplete or ReadyForQuery");

                // 3. Send ROLLBACK
                let query = "ROLLBACK";
                let mut q_buf = Vec::new();
                q_buf.push(b'Q');
                let q_len = 4 + query.len() + 1;
                q_buf.write_u32(q_len as u32).await.unwrap();
                q_buf.extend_from_slice(query.as_bytes());
                q_buf.push(0);
                stream.write_all(&q_buf).await.unwrap();

                let n = stream.read(&mut resp_buf).await.unwrap();
                println!("Received ROLLBACK response: {:?}", &resp_buf[..n]);
                if resp_buf[0] == b'E' {
                    panic!("Received ErrorResponse for ROLLBACK: {:?}", &resp_buf[..n]);
                }
                
                connected = true;
                break;
            }
            Err(_) => {}
        }
    }

    server.kill().ok();
    assert!(connected, "Failed to connect and verify transaction stubs");
}

#[tokio::test]
async fn test_pg_wire_introspection_queries() {
    let pg_port = free_port();
    let http_port = free_port();
    let bind_addr = format!("127.0.0.1:{}", http_port);
    let data_dir = tempfile::tempdir().unwrap();
    let data_path = data_dir.path().to_str().unwrap();

    let binary_path = "../../target/debug/boyodb-server";
    let mut server = Command::new(binary_path)
        .args(&[
            data_path,
            &bind_addr,
            "--pg-port", &pg_port.to_string(),
        ])
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("failed to start server binary");

    let mut connected = false;

    for _ in 0..20 {
        sleep(Duration::from_millis(500)).await;
        match TcpStream::connect(format!("127.0.0.1:{}", pg_port)).await {
            Ok(mut stream) => {
                // 1. Handshake & Auth
                let mut buf = Vec::new();
                buf.write_u32(196608).await.unwrap(); // Protocol 3.0
                buf.extend_from_slice(b"user\0boyo\0database\0default\0\0");

                let len = buf.len() + 4;
                let mut prefix = Vec::new();
                prefix.write_u32(len as u32).await.unwrap();
                stream.write_all(&prefix).await.unwrap();
                stream.write_all(&buf).await.unwrap();

                let mut resp_buf = [0u8; 4096];
                let n = stream.read(&mut resp_buf).await.unwrap();
                if n > 0 && resp_buf[0] == b'R' {
                    if n >= 9 && resp_buf[8] == 3 {
                        let password = "boyo";
                        let mut p_buf = Vec::new();
                        p_buf.push(b'p');
                        let p_len = 4 + password.len() + 1;
                        p_buf.write_u32(p_len as u32).await.unwrap();
                        p_buf.extend_from_slice(password.as_bytes());
                        p_buf.push(0);
                        stream.write_all(&p_buf).await.unwrap();

                        let _ = stream.read(&mut resp_buf).await.unwrap();
                    }
                }

                // 2. Test SELECT version()
                let query = "SELECT version()";
                let mut q_buf = Vec::new();
                q_buf.push(b'Q');
                let q_len = 4 + query.len() + 1;
                q_buf.write_u32(q_len as u32).await.unwrap();
                q_buf.extend_from_slice(query.as_bytes());
                q_buf.push(0);
                stream.write_all(&q_buf).await.unwrap();

                let n = stream.read(&mut resp_buf).await.unwrap();
                println!("Received version() response: {:?}", &resp_buf[..n]);
                if resp_buf[0] == b'E' {
                    panic!("Received ErrorResponse for SELECT version(): {:?}", &resp_buf[..n]);
                }
                // Verify we got RowDescription ('T')
                assert_eq!(resp_buf[0], b'T', "Expected RowDescription ('T') for SELECT version()");
                // Check response contains "BoyoDB"
                let response_str = String::from_utf8_lossy(&resp_buf[..n]);
                assert!(response_str.contains("BoyoDB"), "Expected BoyoDB in version response");

                // 3. Test SHOW server_version
                let query = "SHOW server_version";
                let mut q_buf = Vec::new();
                q_buf.push(b'Q');
                let q_len = 4 + query.len() + 1;
                q_buf.write_u32(q_len as u32).await.unwrap();
                q_buf.extend_from_slice(query.as_bytes());
                q_buf.push(0);
                stream.write_all(&q_buf).await.unwrap();

                let n = stream.read(&mut resp_buf).await.unwrap();
                println!("Received SHOW server_version response: {:?}", &resp_buf[..n]);
                if resp_buf[0] == b'E' {
                    panic!("Received ErrorResponse for SHOW server_version: {:?}", &resp_buf[..n]);
                }
                assert_eq!(resp_buf[0], b'T', "Expected RowDescription ('T') for SHOW server_version");

                // 4. Test SET command (should just acknowledge)
                let query = "SET client_encoding TO 'UTF8'";
                let mut q_buf = Vec::new();
                q_buf.push(b'Q');
                let q_len = 4 + query.len() + 1;
                q_buf.write_u32(q_len as u32).await.unwrap();
                q_buf.extend_from_slice(query.as_bytes());
                q_buf.push(0);
                stream.write_all(&q_buf).await.unwrap();

                let n = stream.read(&mut resp_buf).await.unwrap();
                println!("Received SET response: {:?}", &resp_buf[..n]);
                if resp_buf[0] == b'E' {
                    panic!("Received ErrorResponse for SET: {:?}", &resp_buf[..n]);
                }
                // SET should return CommandComplete ('C')
                assert_eq!(resp_buf[0], b'C', "Expected CommandComplete ('C') for SET");

                connected = true;
                break;
            }
            Err(_) => {}
        }
    }

    server.kill().ok();
    assert!(connected, "Failed to connect and verify introspection queries");
}
