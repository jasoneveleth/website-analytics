use tokio::sync::mpsc;
use warp::Filter;
use tokio::time::{sleep, Duration};
use std::{fs::OpenOptions, io::Write};
use chrono::Utc;
use serde::{Serialize, Deserialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AnalyticsData {
    user_agent: String,
    timestamp: String,
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
    let (tx, mut rx) = mpsc::channel::<AnalyticsData>(1024);

    let capacity = 128;
    let timeout_dur = Duration::from_secs(5);
    tokio::spawn(async move {
        let mut buffer = Vec::with_capacity(capacity);
        loop {
            let res = tokio::select! {
                _ = sleep(timeout_dur) => None,
                data = rx.recv() => Some(data),
            };

            match res {
                Some(Some(data)) => { // got data
                    buffer.push(data);
                    if buffer.len() >= capacity {
                        flush_buffer(&mut buffer).await;
                    }
                },
                Some(None) => { // channel closed
                    flush_buffer(&mut buffer).await;
                    break;
                },
                None => { // timeout
                    flush_buffer(&mut buffer).await;
                },
            }
        }
    });

    let cors = warp::cors()
        .allow_any_origin()
        .allow_methods(vec!["POST"])
        .allow_headers(vec!["Content-Type"]);

    let analytics_route = warp::post()
        .and(warp::path("analytics"))
        .and(warp::body::json())
        .and(warp::any().map(move || tx.clone()))
        .and_then(handle_request)
        .with(cors);

    warp::serve(analytics_route)
        .tls()
        .cert_path("/etc/letsencrypt/live/logging.jsn.vet/fullchain.pem")
        .key_path("/etc/letsencrypt/live/logging.jsn.vet/privkey.pem")
        .run(([0, 0, 0, 0], 5000))
        .await;

    // Keep the main thread alive
    sleep(Duration::from_secs(10)).await;
}

async fn handle_request(data: AnalyticsData, tx: mpsc::Sender<AnalyticsData>) -> Result<impl warp::Reply, warp::Rejection> {
    tx.send(data).await.unwrap();
    Ok(warp::reply())
}

async fn flush_buffer(buffer: &mut Vec<AnalyticsData>) {
    if buffer.is_empty() {
        return;
    }

    let mut file = OpenOptions::new()
        .append(true)
        .create(true)
        .open(format!("logs/temp_{}.csv", Utc::now().format("%Y-%m-%d")))
        .unwrap();

    for entry in buffer.iter() {
        let record = format!(
            "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
            entry.timestamp,
            entry.user_agent.replace("\t", " "),
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
    buffer.clear();
}
