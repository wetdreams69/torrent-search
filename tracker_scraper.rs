use std::fs;
use std::net::UdpSocket;
use std::time::Duration;
use std::collections::HashMap;
use std::io::Cursor;

const TRACKERS: &[&str] = &[
    "tracker.opentrackr.org:1337",
    "open.stealth.si:80",
    "tracker.torrent.eu.org:451",
    "exodus.desync.com:6969",
    "tracker.moeking.me:6969",
    "opentracker.i2p.rocks:6969",
    "tracker.bitsearch.to:1337",
    "tracker.tiny-vps.com:6969",
    "tracker.openbittorrent.com:6969",
];

const BATCH_SIZE: usize = 50;
const TIMEOUT_SECS: u64 = 5;

#[derive(Debug, Clone)]
struct TorrentStats {
    seeders: u32,
    leechers: u32,
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

// Protocolo UDP Tracker simplificado
fn scrape_udp_tracker(tracker: &str, infohashes: &[Vec<u8>]) -> HashMap<String, TorrentStats> {
    let mut results = HashMap::new();
    
    // Conectar al tracker
    let socket = match UdpSocket::bind("0.0.0.0:0") {
        Ok(s) => s,
        Err(_) => return results,
    };
    
    if socket.set_read_timeout(Some(Duration::from_secs(TIMEOUT_SECS))).is_err() {
        return results;
    }
    
    if socket.connect(tracker).is_err() {
        return results;
    }
    
    // 1. Connect request
    let transaction_id: u32 = rand::random();
    let mut connect_req = Vec::new();
    connect_req.extend_from_slice(&0x41727101980u64.to_be_bytes()); // protocol_id
    connect_req.extend_from_slice(&0u32.to_be_bytes()); // action = connect
    connect_req.extend_from_slice(&transaction_id.to_be_bytes());
    
    if socket.send(&connect_req).is_err() {
        return results;
    }
    
    let mut buf = [0u8; 16];
    let connection_id = match socket.recv(&mut buf) {
        Ok(16) => {
            let recv_action = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
            let recv_trans = u32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]);
            
            if recv_action != 0 || recv_trans != transaction_id {
                return results;
            }
            
            u64::from_be_bytes([buf[8], buf[9], buf[10], buf[11], buf[12], buf[13], buf[14], buf[15]])
        }
        _ => return results,
    };
    
    // 2. Scrape request
    let scrape_trans_id: u32 = rand::random();
    let mut scrape_req = Vec::new();
    scrape_req.extend_from_slice(&connection_id.to_be_bytes());
    scrape_req.extend_from_slice(&2u32.to_be_bytes()); // action = scrape
    scrape_req.extend_from_slice(&scrape_trans_id.to_be_bytes());
    
    // Agregar hasta 74 infohashes (límite UDP)
    let chunk_size = 74.min(infohashes.len());
    for hash in &infohashes[..chunk_size] {
        scrape_req.extend_from_slice(hash);
    }
    
    if socket.send(&scrape_req).is_err() {
        return results;
    }
    
    // 3. Leer respuesta
    let mut response = vec![0u8; 2048];
    if let Ok(n) = socket.recv(&mut response) {
        if n >= 8 {
            let recv_action = u32::from_be_bytes([response[0], response[1], response[2], response[3]]);
            let recv_trans = u32::from_be_bytes([response[4], response[5], response[6], response[7]]);
            
            if recv_action == 2 && recv_trans == scrape_trans_id {
                // Parsear resultados (12 bytes por torrent)
                let mut offset = 8;
                for (i, hash) in infohashes[..chunk_size].iter().enumerate() {
                    if offset + 12 <= n {
                        let seeders = u32::from_be_bytes([
                            response[offset],
                            response[offset + 1],
                            response[offset + 2],
                            response[offset + 3],
                        ]);
                        let _completed = u32::from_be_bytes([
                            response[offset + 4],
                            response[offset + 5],
                            response[offset + 6],
                            response[offset + 7],
                        ]);
                        let leechers = u32::from_be_bytes([
                            response[offset + 8],
                            response[offset + 9],
                            response[offset + 10],
                            response[offset + 11],
                        ]);
                        
                        let hash_str = hex::encode(hash).to_lowercase();
                        results.insert(hash_str, TorrentStats { seeders, leechers });
                        
                        offset += 12;
                    }
                }
            }
        }
    }
    
    results
}

fn process_batch(
    batch_indices: Vec<usize>,
    batch_hashes: Vec<String>,
    data_lines: &[String],
) -> Vec<(usize, Option<CsvRecord>)> {
    // Convertir hashes a bytes
    let hash_bytes: Vec<Vec<u8>> = batch_hashes
        .iter()
        .filter_map(|h| hex::decode(h).ok())
        .filter(|b| b.len() == 20)
        .collect();
    
    if hash_bytes.is_empty() {
        return batch_indices
            .iter()
            .map(|&idx| {
                let record = CsvRecord::from_line(&data_lines[idx]);
                (idx, record)
            })
            .collect();
    }
    
    // Consultar todos los trackers
    let mut all_results: Vec<HashMap<String, TorrentStats>> = Vec::new();
    
    for tracker in TRACKERS {
        if let Ok(results) = std::panic::catch_unwind(|| {
            scrape_udp_tracker(tracker, &hash_bytes)
        }) {
            if !results.is_empty() {
                all_results.push(results);
            }
        }
    }
    
    // Consolidar resultados
    let mut final_results = Vec::new();
    
    for (i, &line_idx) in batch_indices.iter().enumerate() {
        if i >= batch_hashes.len() {
            break;
        }
        
        let hash = batch_hashes[i].to_lowercase();
        let mut max_seeders = 0u32;
        let mut max_leechers = 0u32;
        let mut any_success = false;
        
        for tracker_result in &all_results {
            if let Some(stats) = tracker_result.get(&hash) {
                max_seeders = max_seeders.max(stats.seeders);
                max_leechers = max_leechers.max(stats.leechers);
                any_success = true;
            }
        }
        
        let original_line = &data_lines[line_idx];
        
        if !any_success {
            // Failed - mantener original
            if let Some(record) = CsvRecord::from_line(original_line) {
                final_results.push((line_idx, Some(record)));
            }
        } else if max_seeders > 0 || max_leechers > 0 {
            // Alive - actualizar
            if let Some(mut record) = CsvRecord::from_line(original_line) {
                record.seeders = max_seeders;
                record.leechers = max_leechers;
                record.scraped_date = chrono::Utc::now().timestamp();
                final_results.push((line_idx, Some(record)));
            }
        } else {
            // Dead - marcar para borrar
            final_results.push((line_idx, None));
        }
    }
    
    final_results
}

fn main() -> std::io::Result<()> {
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
        
        println!("Checking {} torrents in batches of {}...", total, BATCH_SIZE);
        
        let mut updated_records: HashMap<usize, Option<CsvRecord>> = HashMap::new();
        let mut processed = 0;
        let mut alive = 0;
        let mut dead = 0;
        let mut failed = 0;
        
        // Procesar en batches
        for i in (0..total).step_by(BATCH_SIZE) {
            let mut batch_indices = Vec::new();
            let mut batch_hashes = Vec::new();
            
            for idx in i..(i + BATCH_SIZE).min(total) {
                let line = &data_lines[idx];
                if let Some(infohash) = line.split(';').next() {
                    if !infohash.is_empty() && infohash.len() == 40 {
                        batch_indices.push(idx);
                        batch_hashes.push(infohash.to_string());
                    }
                }
            }
            
            if !batch_hashes.is_empty() {
                let batch_results = process_batch(batch_indices, batch_hashes, data_lines);
                
                for (idx, record_opt) in batch_results {
                    updated_records.insert(idx, record_opt.clone());
                    processed += 1;
                    
                    match record_opt {
                        Some(record) if record.seeders > 0 || record.leechers > 0 => alive += 1,
                        Some(_) => failed += 1,
                        None => dead += 1,
                    }
                }
            }
            
            let percent = (processed as f64 / total as f64 * 100.0).round();
            print!(
                "\r🚀 Progress: {:.2}% ({}/{}) | Alive: {} | Dead: {} | Failed: {}   ",
                percent, processed, total, alive, dead, failed
            );
            use std::io::Write;
            std::io::stdout().flush().unwrap();
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