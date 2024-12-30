use tokio::sync::mpsc;
use tokio::time::{sleep, Duration};
use std::{sync::Arc, fs::OpenOptions, io::Write};
use chrono::Utc;

#[derive(Debug, Clone)]
struct AnalyticsData {
    screen_width: u32,
    screen_height: u32,
    viewport_width: u32,
    viewport_height: u32,
    language: String,
    timezone_offset: i32,
    referrer: String,
    page: String,
}

#[tokio::main]
async fn main() {
    let (tx, rx) = mpsc::channel::<AnalyticsData>(1024);

    // Buffer channel output and flush every 512
    tokio::spawn(async move {
        let mut buffer = Vec::new();
        loop {
            match rx.recv().await {
                Some(data) => {
                    buffer.push(data);
                    if buffer.len() >= 512 {
                        flush_buffer(&buffer).await;
                        buffer.clear();
                    }
                }
                None => break, // End when channel is closed
            }
        }
    });

    let analytics_route = warp::post()
        .and(warp::path("analytics"))
        .and(warp::body::json())
        .and(warp::any().map(move || tx.clone()))
        .and_then(handle_request);

    warp::serve(analytics_route)
        .tls()
        .cert_path("cert.pem")
        .key_path("key.pem")
        .run(([0, 0, 0, 0], 5000))
        .await;

    // Keep the main thread alive
    sleep(Duration::from_secs(10)).await;
}

async fn handle_request(data: AnalyticsData, tx: mpsc::Sender<AnalyticsData>) -> Result<impl warp::Reply, warp::Rejection> {
    tx.send(data).await.unwrap();
    warp::reply::with_status("Accepted", warp::http::StatusCode::ACCEPTED)
}

async fn flush_buffer(buffer: &[AnalyticsData]) {
    let mut file = OpenOptions::new()
        .append(true)
        .create(true)
        .open(format!("logs/temp_{}.csv", Utc::now().format("%Y-%m-%d")))
        .unwrap();

    for entry in buffer {
        let record = format!(
            "{},{},{},{},{},{},{},{}\n",
            entry.screen_width,
            entry.screen_height,
            entry.viewport_width,
            entry.viewport_height,
            entry.language,
            entry.timezone_offset,
            entry.referrer,
            entry.page
        );
        file.write_all(record.as_bytes()).unwrap();
    }
}
