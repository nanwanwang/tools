use std::net::{TcpStream, ToSocketAddrs};
use std::time::{Duration, Instant};

use chrono::Utc;

use crate::models::{ConnectionHealth, ConnectionRecord};
use crate::tdengine_client;

pub fn run_health_check(connection: &ConnectionRecord, secret: Option<String>) -> ConnectionHealth {
    if connection.kind == "tdengine" {
        return tdengine_client::run_health_check(connection, secret);
    }

    let checked_at = Utc::now().to_rfc3339();
    let target = format!("{}:{}", connection.host, connection.port);
    let mut details = vec![format!("Resolved target {target}.")];
    let started = Instant::now();

    let address = match target.to_socket_addrs() {
        Ok(mut addresses) => addresses.next(),
        Err(error) => {
            return ConnectionHealth {
                status: "unreachable".into(),
                summary: "DNS resolution failed.".into(),
                details: vec![error.to_string()],
                latency_ms: None,
                checked_at,
            };
        }
    };

    let Some(address) = address else {
        return ConnectionHealth {
            status: "unreachable".into(),
            summary: "No socket address could be resolved.".into(),
            details,
            latency_ms: None,
            checked_at,
        };
    };

    match TcpStream::connect_timeout(&address, Duration::from_secs(3)) {
        Ok(_) => {
            let latency_ms = started.elapsed().as_millis() as u64;
            if connection.environment == "production" {
                details.push("Production profile stays guarded for write actions.".into());
                ConnectionHealth {
                    status: "degraded".into(),
                    summary: "TCP port is reachable. Production safeguards stay enabled.".into(),
                    details,
                    latency_ms: Some(latency_ms),
                    checked_at,
                }
            } else {
                details.push("Basic socket connectivity succeeded.".into());
                ConnectionHealth {
                    status: "healthy".into(),
                    summary: "Target port is reachable from the desktop app.".into(),
                    details,
                    latency_ms: Some(latency_ms),
                    checked_at,
                }
            }
        }
        Err(error) => {
            details.push(error.to_string());
            ConnectionHealth {
                status: "unreachable".into(),
                summary: "Desktop runtime could not open the target port.".into(),
                details,
                latency_ms: None,
                checked_at,
            }
        }
    }
}
