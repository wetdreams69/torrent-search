use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufRead, Write};
use std::path::Path;
use std::collections::HashSet;
use chrono::Utc;

#[derive(Debug, Clone)]
pub struct TorrentCsvRecord {
    pub infohash: String,
    pub name: String,
    pub size_bytes: u64,
    pub created_unix: i64,
    pub seeders: i32,
    pub leechers: i32,
    pub completed: i32,
    pub scraped_date: i64,
}

impl TorrentCsvRecord {
    pub fn to_csv_line(&self) -> String {
        format!(
            "{};{};{};{};{};{};{};{}",
            self.infohash,
            self.name.replace(";", ","), // Escapar punto y coma
            self.size_bytes,
            self.created_unix,
            self.seeders,
            self.leechers,
            self.completed,
            self.scraped_date
        )
    }
    
    pub fn from_torrent(torrent: &super::Torrent, infohash: &str) -> Self {
        Self {
            infohash: infohash.to_string(),
            name: torrent.name.clone(),
            size_bytes: parse_size_to_bytes(&torrent.size),
            created_unix: torrent.date.unwrap_or_else(|| Utc::now().timestamp()),
            seeders: torrent.seeders,
            leechers: torrent.leechers,
            completed: 0,
            scraped_date: Utc::now().timestamp(),
        }
    }
}

pub fn parse_size_to_bytes(size: &str) -> u64 {
    let size = size.trim();
    let parts: Vec<&str> = size.split_whitespace().collect();
    
    if parts.len() != 2 {
        return 0;
    }
    
    let number: f64 = parts[0].parse().unwrap_or(0.0);
    let unit = parts[1].to_uppercase();
    
    let multiplier = match unit.as_str() {
        "BYTES" => 1.0,
        "KB" => 1000.0,
        "MB" => 1000000.0,
        "GB" => 1000000000.0,
        "TB" => 1000000000000.0,
        _ => 1.0,
    };
    
    (number * multiplier) as u64
}

pub fn read_existing_infohashes(csv_path: &str) -> HashSet<String> {
    let mut infohashes = HashSet::new();
    
    if let Ok(file) = File::open(csv_path) {
        let reader = BufReader::new(file);
        
        for (i, line) in reader.lines().enumerate() {
            if i == 0 {
                continue; // Skip header
            }
            
            if let Ok(line) = line {
                if let Some(infohash) = line.split(';').next() {
                    infohashes.insert(infohash.to_string());
                }
            }
        }
    }
    
    infohashes
}

pub fn append_torrents_to_csv(
    csv_path: &str,
    records: Vec<TorrentCsvRecord>
) -> std::io::Result<usize> {
    // Leer infohashes existentes para evitar duplicados
    let existing = read_existing_infohashes(csv_path);
    
    // Filtrar registros nuevos
    let new_records: Vec<_> = records.into_iter()
        .filter(|r| !existing.contains(&r.infohash))
        .collect();
    
    if new_records.is_empty() {
        return Ok(0);
    }
    
    // Abrir archivo en modo append
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(csv_path)?;
    
    // Escribir registros
    for record in &new_records {
        writeln!(file, "{}", record.to_csv_line())?;
    }
    
    Ok(new_records.len())
}

pub fn create_csv_if_not_exists(csv_path: &str) -> std::io::Result<()> {
    if !Path::new(csv_path).exists() {
        let mut file = File::create(csv_path)?;
        writeln!(file, "infohash;name;size_bytes;created_unix;seeders;leechers;completed;scraped_date")?;
    }
    Ok(())
}

pub fn extract_infohash_from_magnet(magnet: &str) -> Option<String> {
    // Extraer infohash de magnet link: magnet:?xt=urn:btih:INFOHASH
    magnet.split("xt=urn:btih:")
        .nth(1)?
        .split('&')
        .next()
        .map(|s| s.to_lowercase())
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_parse_size() {
        assert_eq!(parse_size_to_bytes("1.5 GB"), 1500000000);
        assert_eq!(parse_size_to_bytes("500 MB"), 500000000);
        assert_eq!(parse_size_to_bytes("2.0 KB"), 2000);
    }
    
    #[test]
    fn test_extract_infohash() {
        let magnet = "magnet:?xt=urn:btih:ABC123&dn=test";
        assert_eq!(extract_infohash_from_magnet(magnet), Some("abc123".to_string()));
    }
}