use std::fs;
use std::path::Path;
use std::time::Duration;
use tokio::time::timeout;
use futures::stream::{self, StreamExt};
use std::collections::HashMap;

const TRACKERS: &[&str] = &[
    "udp://tracker.opentrackr.org:1337/announce",
    "udp://open.stealth.si:80/announce",
    "udp://tracker.torrent.eu.org:451/announce",
    "udp://exodus.desync.com:6969/announce",
    "udp://tracker.moeking.me:6969/announce",
    "udp://opentracker.i2p.rocks:6969/announce",
    "udp://tracker.bitsearch.to:1337/announce",
    "udp://tracker.tiny-vps.com:6969/announce",
    "udp://tracker.openbittorrent.com:6969/announce",
];

const BATCH_SIZE: usize = 50;
const CONCURRENCY: usize = 10;
const TIMEOUT_SECS: u64 = 8;

#[derive(Debug, Clone)]
struct TorrentStats {
    seeders: u32,
    leechers: u32,
    found: bool,
}

#[derive(Debug)]
struct CsvRecord {
    infohash: String,
    name: String,
    size_bytes: String,
    created_unix: String,
    seeders: u32,
    leechers: u32,
    completed: String,
    scraped_date: i64,
}

impl CsvRecord {
    fn from_line(line: &str) -> Option<Self> {
        let parts: Vec<&str> = line.split(';').collect();
        if parts.len() < 8 {
            return None;
        }
        
        Some(Self {
            infohash: parts[0].to_string(),
            name: parts[1].to_string(),
            size_bytes: parts[2].to_string(),
            created_unix: parts[3].to_string(),
            seeders: parts[4].parse().unwrap_or(0),
            leechers: parts[5].parse().unwrap_or(0),
            completed: parts[6].to_string(),
            scraped_date: parts[7].parse().unwrap_or(0),
        })
    }
    
    fn to_line(&self) -> String {
        format!(
            "{};{};{};{};{};{};{};{}",
            self.infohash,
            self.name,
            self.size_bytes,
            self.created_unix,
            self.seeders,
            self.leechers,
            self.completed,
            self.scraped_date
        )
    }
}

async fn scrape_tracker(
    infohashes: Vec<String>,
    tracker_url: &str,
) -> HashMap<String, TorrentStats> {
    let result = timeout(
        Duration::from_secs(TIMEOUT_SECS),
        scrape_tracker_impl(infohashes, tracker_url),
    )
    .await;
    
    result.unwrap_or_else(|_| HashMap::new())
}

async fn scrape_tracker_impl(
    infohashes: Vec<String>,
    tracker_url: &str,
) -> HashMap<String, TorrentStats> {
    use udp_tracker_client::{Client, ScrapeRequest};
    
    let mut results = HashMap::new();
    
    // Parsear URL del tracker
    let parsed = match tracker_url.parse::<url::Url>() {
        Ok(u) => u,
        Err(_) => return results,
    };
    
    let host = match parsed.host_str() {
        Some(h) => h,
        None => return results,
    };
    
    let port = parsed.port().unwrap_or(6969);
    let addr = format!("{}:{}", host, port);
    
    // Convertir infohashes a bytes
    let mut info_hashes = Vec::new();
    for hash in &infohashes {
        if let Ok(bytes) = hex::decode(hash) {
            if bytes.len() == 20 {
                let mut arr = [0u8; 20];
                arr.copy_from_slice(&bytes);
                info_hashes.push(arr);
            }
        }
    }
    
    if info_hashes.is_empty() {
        return results;
    }
    
    // Conectar al tracker UDP
    let client = match Client::connect(&addr).await {
        Ok(c) => c,
        Err(_) => return results,
    };
    
    // Hacer scrape
    let request = ScrapeRequest {
        info_hashes: &info_hashes,
    };
    
    match client.scrape(&request).await {
        Ok(response) => {
            for (i, stats) in response.stats.iter().enumerate() {
                if let Some(hash) = infohashes.get(i) {
                    results.insert(
                        hash.to_lowercase(),
                        TorrentStats {
                            seeders: stats.seeders,
                            leechers: stats.leechers,
                            found: true,
                        },
                    );
                }
            }
        }
        Err(_) => {}
    }
    
    results
}

async fn process_batch(
    batch_indices: Vec<usize>,
    batch_hashes: Vec<String>,
    data_lines: &[String],
) -> Vec<(usize, Option<CsvRecord>)> {
    // Consultar todos los trackers para este batch
    let tracker_futures: Vec<_> = TRACKERS
        .iter()
        .map(|t| scrape_tracker(batch_hashes.clone(), t))
        .collect();
    
    let all_tracker_results = futures::future::join_all(tracker_futures).await;
    
    // Consolidar resultados por infohash
    let mut results = Vec::new();
    
    for (i, &line_idx) in batch_indices.iter().enumerate() {
        let hash = batch_hashes[i].to_lowercase();
        let mut max_seeders = 0u32;
        let mut max_leechers = 0u32;
        let mut any_success = false;
        
        for tracker_result in &all_tracker_results {
            if let Some(stats) = tracker_result.get(&hash) {
                if stats.found {
                    max_seeders = max_seeders.max(stats.seeders);
                    max_leechers = max_leechers.max(stats.leechers);
                    any_success = true;
                }
            }
        }
        
        let original_line = &data_lines[line_idx];
        
        if !any_success {
            // Failed - mantener original
            if let Some(record) = CsvRecord::from_line(original_line) {
                results.push((line_idx, Some(record)));
            }
        } else if max_seeders > 0 || max_leechers > 0 {
            // Alive - actualizar
            if let Some(mut record) = CsvRecord::from_line(original_line) {
                record.seeders = max_seeders;
                record.leechers = max_leechers;
                record.scraped_date = chrono::Utc::now().timestamp();
                results.push((line_idx, Some(record)));
            }
        } else {
            // Dead - marcar para borrar
            results.push((line_idx, None));
        }
    }
    
    results
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let current_dir = std::env::current_dir()?;
    
    // Buscar archivos CSV
    let mut files: Vec<_> = fs::read_dir(&current_dir)?
        .filter_map(|e| e.ok())
        .filter(|e| {
            let name = e.file_name();
            let name_str = name.to_string_lossy();
            name_str.starts_with("torrents_part_") && name_str.ends_with(".csv")
        })
        .collect();
    
    // Ordenar por número
    files.sort_by(|a, b| {
        let extract_num = |path: &std::fs::DirEntry| -> u32 {
            let name = path.file_name();
            let name_str = name.to_string_lossy();
            name_str
                .chars()
                .filter(|c| c.is_numeric())
                .collect::<String>()
                .parse()
                .unwrap_or(0)
        };
        extract_num(a).cmp(&extract_num(b))
    });
    
    if files.is_empty() {
        println!("No torrents_part_*.csv files found.");
        return Ok(());
    }
    
    for file_entry in files {
        let csv_path = file_entry.path();
        let file_name = csv_path.file_name().unwrap().to_string_lossy();
        
        println!("\n📦 Processing {}...", file_name);
        
        let content = fs::read_to_string(&csv_path)?;
        let lines: Vec<String> = content.lines().map(String::from).collect();
        
        if lines.len() <= 1 {
            continue;
        }
        
        let header = &lines[0];
        let data_lines = &lines[1..];
        let total = data_lines.len();
        
        println!(
            "Checking {} torrents using Batch Scraping (Batch: {}, Parallel: {})...",
            total, BATCH_SIZE, CONCURRENCY
        );
        
        let mut updated_records: HashMap<usize, Option<CsvRecord>> = HashMap::new();
        let mut processed = 0;
        let mut alive = 0;
        let mut dead = 0;
        let mut failed = 0;
        
        // Procesar en batches con concurrencia
        for i in (0..total).step_by(BATCH_SIZE * CONCURRENCY) {
            let mut batch_futures = Vec::new();
            
            for j in 0..CONCURRENCY {
                let start = i + (j * BATCH_SIZE);
                if start >= total {
                    break;
                }
                
                let mut batch_indices = Vec::new();
                let mut batch_hashes = Vec::new();
                
                for k in 0..BATCH_SIZE {
                    let idx = start + k;
                    if idx >= total {
                        break;
                    }
                    
                    let line = &data_lines[idx];
                    if let Some(infohash) = line.split(';').next() {
                        if !infohash.is_empty() {
                            batch_indices.push(idx);
                            batch_hashes.push(infohash.to_string());
                        }
                    }
                }
                
                if !batch_hashes.is_empty() {
                    batch_futures.push(process_batch(
                        batch_indices,
                        batch_hashes,
                        data_lines,
                    ));
                }
            }
            
            let results = futures::future::join_all(batch_futures).await;
            
            for batch_result in results {
                for (idx, record_opt) in batch_result {
                    updated_records.insert(idx, record_opt.clone());
                    processed += 1;
                    
                    match record_opt {
                        Some(record) if record.seeders > 0 || record.leechers > 0 => alive += 1,
                        Some(_) => failed += 1,
                        None => dead += 1,
                    }
                }
                
                if processed % 100 == 0 || processed >= total {
                    let percent = (processed as f64 / total as f64 * 100.0).round();
                    print!(
                        "\r🚀 Progress: {:.2}% ({}/{}) | Alive: {} | Dead: {} | Failed: {}   ",
                        percent, processed, total, alive, dead, failed
                    );
                    use std::io::Write;
                    std::io::stdout().flush().unwrap();
                }
            }
        }
        
        println!("\nWriting updated {}...", file_name);
        
        // Reconstruir archivo
        let mut final_lines = vec![header.clone()];
        for i in 0..total {
            if let Some(Some(record)) = updated_records.get(&i) {
                final_lines.push(record.to_line());
            }
        }
        
        fs::write(&csv_path, final_lines.join("\n") + "\n")?;
    }
    
    println!("\n✅ All files updated.");
    Ok(())
}

// Cargo.toml dependencies adicionales:
// [dependencies]
// tokio = { version = "1", features = ["full"] }
// futures = "0.3"
// chrono = "0.4"
// hex = "0.4"
// url = "2.5"
// udp-tracker-client = "0.3"  # Librería para UDP tracker protocol