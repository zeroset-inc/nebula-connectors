use nebula_connector_sdk::Run;
use serde_json::json;
use std::{
    io::{BufRead, BufReader, Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    time::{Duration, Instant},
};

pub fn accept(listener: &TcpListener) -> (TcpStream, SocketAddr) {
    listener.set_nonblocking(true).unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match listener.accept() {
            Ok(connection) => return connection,
            Err(error)
                if error.kind() == std::io::ErrorKind::WouldBlock && Instant::now() < deadline =>
            {
                std::thread::sleep(Duration::from_millis(1))
            }
            Err(error) => panic!("test server did not receive request: {error}"),
        }
    }
}

pub fn request(stream: &mut TcpStream) -> (String, Vec<u8>) {
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    reader.read_line(&mut line).unwrap();
    let first = line.clone();
    let mut length = 0;
    let mut authorization = false;
    loop {
        line.clear();
        reader.read_line(&mut line).unwrap();
        if line == "\r\n" {
            break;
        }
        let lower = line.to_ascii_lowercase();
        if let Some(value) = lower.strip_prefix("content-length:") {
            length = value.trim().parse().unwrap();
        }
        if lower.trim() == "authorization: bearer nebula-grant" {
            authorization = true;
        }
    }
    assert!(authorization, "missing Nebula grant");
    assert!(length <= 1 << 20);
    let mut body = vec![0; length];
    reader.read_exact(&mut body).unwrap();
    (first, body)
}
pub fn respond(stream: &mut TcpStream, status: u16, body: &str) {
    write!(stream,"HTTP/1.1 {status} Test\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len()).unwrap();
}
pub fn run() -> Run {
    Run {
        limits: nebula_connector_sdk::client::ProtocolLimits::V1,
        id: "run".into(),
        scan_id: "scan".into(),
        fence: 1,
        scope_revision: 1,
        credential_revision: 1,
        authorization_revision: 1,
        lease_until: "2099-01-01T00:00:00Z".into(),
        lease_remaining_ms: 120_000,
        next_sequence: 0,
        cursor: json!({}),
        state: "running".into(),
    }
}
