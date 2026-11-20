use axum::{
    extract::{ ws::{ Message, WebSocket, WebSocketUpgrade }, State },
    http::{ header, StatusCode, Uri },
    response::IntoResponse,
    routing::get,
    Router,
};
use rust_embed::RustEmbed;
use serde_json::Value;
use std::{ io::{ BufRead, BufReader, Write }, time::Duration };
use sysinfo::Components;
use tokio::sync::{ broadcast, mpsc };

#[derive(RustEmbed)]
#[folder = "../web/"]
struct Assets;

#[derive(Clone)]
struct AppState {
    tx: broadcast::Sender<String>,
    cmd_tx: mpsc::UnboundedSender<String>,
}

fn temperatura_cpu() -> f32 {
    let comps = Components::new_with_refreshed_list();
    comps
        .iter()
        .filter(|c| {
            let l = c.label().to_lowercase();
            l.contains("cpu") || l.contains("core") || l.contains("package")
        })
        .map(|c| c.temperature())
        .fold(f32::NAN, f32::max)
}

async fn static_handler(uri: Uri) -> impl IntoResponse {
    let path = uri.path().trim_start_matches('/');
    let path = if path.is_empty() { "index.html" } else { path };
    match Assets::get(path) {
        Some(f) =>
            (
                [
                    (
                        header::CONTENT_TYPE,
                        mime_guess::from_path(path).first_or_octet_stream().to_string(),
                    ),
                ],
                f.data.into_owned(),
            ).into_response(),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

async fn ws_handler(ws: WebSocketUpgrade, State(s): State<AppState>) -> impl IntoResponse {
    ws.on_upgrade(move |sock| handle_socket(sock, s))
}

async fn handle_socket(mut socket: WebSocket, s: AppState) {
    let mut rx = s.tx.subscribe();
    loop {
        tokio::select! {
            Ok(msg) = rx.recv() => {
                if socket.send(Message::Text(msg)).await.is_err() { break; }
            }
            incoming = socket.recv() => match incoming {
                Some(Ok(Message::Text(t))) => { let _ = s.cmd_tx.send(t); }
                Some(Ok(_)) => {}
                _ => break,
            }
        }
    }
}

// imitador para testes:
fn simulador(tx: broadcast::Sender<String>, mut cmd_rx: mpsc::UnboundedReceiver<String>) {
    let (mut cpu, mut gpu) = (0.0f64, 0.0f64);
    let (mut auto, mut servo) = (true, 0i64);
    let mut case_t = 25.0f64;
    loop {
        while let Ok(m) = cmd_rx.try_recv() {
            if let Ok(v) = serde_json::from_str::<Value>(&m) {
                if let Some(x) = v["cpu"].as_f64() {
                    cpu = x;
                }
                if let Some(x) = v["gpu"].as_f64() {
                    gpu = x;
                }
                match v["cmd"].as_str() {
                    Some("servo") => {
                        auto = false;
                        servo = v["angle"].as_i64().unwrap_or(0).clamp(0, 90);
                    }
                    Some("mode") => {
                        auto = v["auto"].as_bool().unwrap_or(true);
                    }
                    _ => {}
                }
            }
        }
        let t = cpu.max(gpu);
        if auto {
            servo = if t >= 70.0 { 90 } else { 0 };
        }
        let alert = (t >= 85.0) as i32;
        case_t += (20.0 + t * 0.3 - case_t) * 0.1; // esquenta aos poucos
        let _ = tx.send(
            format!(
                "{{\"case\":{:.1},\"btn\":0,\"servo\":{},\"auto\":{},\"alert\":{}}}",
                case_t,
                servo,
                auto as i32,
                alert
            )
        );
        std::thread::sleep(Duration::from_millis(500));
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let porta = std::env
        ::args()
        .nth(1)
        .unwrap_or_else(|| "COM3".into());
    let sim = porta == "sim";

    let (tx, _) = broadcast::channel::<String>(64);
    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel::<String>();

    if sim {
        // --- Modo simulação ---
        let tx_s = tx.clone();
        std::thread::spawn(move || simulador(tx_s, cmd_rx));
        println!("Modo simulação ativo (sem Arduino).");
    } else {
        // --- Com o dispositivo: ---
        let port = serialport::new(&porta, 115200).timeout(Duration::from_millis(200)).open()?;
        let mut writer = port.try_clone()?;
        std::thread::sleep(Duration::from_secs(2));

        let tx_leitura = tx.clone();
        std::thread::spawn(move || {
            let mut reader = BufReader::new(port);
            let mut linha = String::new();
            loop {
                match reader.read_line(&mut linha) {
                    Ok(n) if n > 0 => {
                        let l = linha.trim().to_string();
                        if l.starts_with('{') {
                            let _ = tx_leitura.send(l);
                        }
                        linha.clear();
                    }
                    _ => {}
                }
            }
        });

        let mut cmd_rx = cmd_rx;
        std::thread::spawn(move || {
            while let Some(m) = cmd_rx.blocking_recv() {
                let _ = writeln!(writer, "{}", m.trim());
            }
        });
    }

    // temperaturas do PC -> placa + navegador
    let cmd_pc = cmd_tx.clone();
    let tx_pc = tx.clone();
    let inicio = std::time::Instant::now();
    tokio::spawn(async move {
        loop {
            let cpu = if sim {
                65.0 + 30.0 * (inicio.elapsed().as_secs_f32() / 10.0).sin()
            } else {
                let c = temperatura_cpu();
                if c.is_nan() {
                    40.0
                } else {
                    c
                }
            };
            let msg = format!("{{\"cpu\":{:.1},\"gpu\":0}}", cpu);
            let _ = cmd_pc.send(msg.clone());
            let _ = tx_pc.send(msg);
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    });

    let app = Router::new()
        .route("/ws", get(ws_handler))
        .fallback(static_handler)
        .with_state(AppState { tx, cmd_tx });

    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000").await?;
    let _ = open::that("http://127.0.0.1:3000");
    axum::serve(listener, app).await?;
    Ok(())
}
