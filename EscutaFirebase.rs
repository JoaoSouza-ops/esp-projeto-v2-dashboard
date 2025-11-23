use reqwest::{Client, header};
use tokio::io::AsyncWriteExt;
use futures_util::StreamExt;
use serde_json::Value;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // URL do seu nó no Firebase RTDB
    let database_url = "https://URL-DO-CAMINHO-A-SER-LIDO.json";

    // Cabeçalho necessário para streaming (SSE)
    let mut headers = header::HeaderMap::new();
    headers.insert("Accept", header::HeaderValue::from_static("text/event-stream"));

    // Cria cliente HTTP
    let client = Client::builder()
        .default_headers(headers)
        .build()?;

    println!("🔗 Conectando ao Firebase em stream: {}", database_url);

    // Abre conexão streaming
    let mut stream = client
        .get(database_url)
        .send()
        .await?
        .bytes_stream();

    // Abre (ou cria) arquivo de log
    let mut file = tokio::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open("firebase_logs.txt")
        .await?;

    println!("Escutando atualizações em tempo real");

    // Loop contínuo — nunca termina enquanto o programa rodar
    while let Some(chunk) = stream.next().await {
        match chunk {
            Ok(data) => {
                let text = String::from_utf8_lossy(&data);
                if text.trim().is_empty() { continue; }

                // Firebase envia eventos no formato:
                // event: put
                // data: {"path":"/","data":{...}}
                for line in text.lines() {
                    if line.starts_with("data: ") {
                        let json_part = &line[6..];
                        if json_part == "null" { continue; }

                        if let Ok(parsed) = serde_json::from_str::<Value>(json_part) {
                            let now = chrono::Local::now().format("%Y-%m-%d %H:%M:%S");
                            let log_line = format!("[{}] {}\n", now, parsed);
                            file.write_all(log_line.as_bytes()).await?;
                            file.flush().await?;
                            println!("Novo evento: {}", log_line.trim());
                        } else {
                            println!("Pacote não reconhecido: {}", json_part);
                        }
                    }
                }
            }
            Err(e) => {
                eprintln!("Erro no stream: {}", e);
                break;
            }
        }
    }

    Ok(())
}
