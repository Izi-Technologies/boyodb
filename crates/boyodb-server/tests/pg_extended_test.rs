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
async fn test_pg_wire_extended_query() {
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
                // Startup
                let mut buf = Vec::new();
                buf.write_u32(196608).await.unwrap();
                buf.extend_from_slice(b"user\0default\0database\0default\0\0");
                let len = buf.len() + 4;
                stream.write_u32(len as u32).await.unwrap();
                stream.write_all(&buf).await.unwrap();

                // Read AuthOk/Ready
                let mut resp_buf = [0u8; 1024];
                let n = stream.read(&mut resp_buf).await.unwrap();
                println!("Startup received {} bytes: {:?}", n, &resp_buf[..n]);

                if n > 0 && resp_buf[0] == b'R' {
                     // Check if it is Auth Cleartext (Type 3)
                     // If so, send password
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
                         println!("Post-auth received {} bytes: {:?}", n, &resp_buf[..n]);
                     }
                }

                // 1. Parse 'P'
                // len, name "S1", query "SELECT 1", 0 params
                let query = "SELECT 1";
                let stmt_name = "S1";
                let mut p_buf = Vec::new();
                p_buf.push(b'P');
                let mut p_content = Vec::new();
                p_content.extend_from_slice(stmt_name.as_bytes()); p_content.push(0);
                p_content.extend_from_slice(query.as_bytes()); p_content.push(0);
                p_content.write_u16(0).await.unwrap(); // 0 params
                
                p_buf.write_u32((p_content.len() + 4) as u32).await.unwrap();
                p_buf.extend(p_content);

                // 2. Bind 'B'
                // len, portal "", stmt "S1", 0 param formats, 0 params, 0 result formats
                let mut b_buf = Vec::new();
                b_buf.push(b'B');
                let mut b_content = Vec::new();
                b_content.push(0); // portal "" (unnamed)
                b_content.extend_from_slice(stmt_name.as_bytes()); b_content.push(0);
                b_content.write_u16(0).await.unwrap(); // param formats
                b_content.write_u16(0).await.unwrap(); // params len
                b_content.write_u16(0).await.unwrap(); // result formats

                b_buf.write_u32((b_content.len() + 4) as u32).await.unwrap();
                b_buf.extend(b_content);

                // 3. Execute 'E'
                // len, portal "", max_rows 0
                let mut e_buf = Vec::new();
                e_buf.push(b'E');
                let mut e_content = Vec::new();
                e_content.push(0); // portal ""
                e_content.write_u32(0).await.unwrap(); // max rows
                
                e_buf.write_u32((e_content.len() + 4) as u32).await.unwrap();
                e_buf.extend(e_content);

                // 4. Sync 'S'
                let mut s_buf = Vec::new();
                s_buf.push(b'S');
                s_buf.write_u32(4).await.unwrap();

                // Send all
                stream.write_all(&p_buf).await.unwrap();
                stream.write_all(&b_buf).await.unwrap();
                stream.write_all(&e_buf).await.unwrap();
                stream.write_all(&s_buf).await.unwrap();

                // Read Response
                // Expect: ParseComplete('1'), BindComplete('2'), DataRow('D')..., CommandComplete('C'), ReadyForQuery('Z')
                // Read Response loop
                let mut accumulated_resp = Vec::new();
                let mut read_buf = [0u8; 4096];
                
                loop {
                    let n = stream.read(&mut read_buf).await.unwrap();
                    if n == 0 { break; } // EOF
                    accumulated_resp.extend_from_slice(&read_buf[..n]);
                    
                    if accumulated_resp.contains(&b'Z') {
                         break;
                    }
                    if accumulated_resp.len() > 2000 {
                         break; // Safety break
                    }
                }
                
                println!("Extended Query Response Total {} bytes: {:?}", accumulated_resp.len(), accumulated_resp);

                if !accumulated_resp.is_empty() && accumulated_resp[0] == b'E' {
                    panic!("Received ErrorResponse: {:?}", accumulated_resp);
                }
                
                let s_resp = &accumulated_resp;

                // Check for '1' (ParseComplete)
                // Check for '2' (BindComplete)
                // Check for 'D' (DataRow)
                // Check for 'C' (CommandComplete)
                // Check for 'Z' (ReadyForQuery)
                
                // let s_resp = &read_buf[..n]; // Removed
                assert!(s_resp.contains(&b'1'), "Missing ParseComplete");
                assert!(s_resp.contains(&b'2'), "Missing BindComplete");
                assert!(s_resp.contains(&b'D'), "Missing DataRow");
                assert!(s_resp.contains(&b'C'), "Missing CommandComplete");
                assert!(s_resp.contains(&b'Z'), "Missing ReadyForQuery");

                connected = true;
                break;
            }
            Err(_) => {}
        }
    }
    server.kill().ok();
    assert!(connected, "Failed to connect and execute extended query");
}
