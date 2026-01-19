mod torrent_search;
mod csv_writer;

use std::env;
use torrent_search::*;
use csv_writer::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Obtener el nombre del archivo CSV desde variable de entorno
    let csv_file = env::var("CSV_FILE").unwrap_or_else(|_| {
        // Buscar el último archivo torrents_part_*.csv
        find_latest_csv_file().unwrap_or_else(|| "torrents_part_1.csv".to_string())
    });
    
    println!("Using CSV file: {}", csv_file);
    
    // Crear CSV si no existe
    create_csv_if_not_exists(&csv_file)?;
    
    let mut all_records = Vec::new();
    
    println!("Fetching latest torrents...");
    
    // Obtener los últimos torrents de 1337x (sin búsqueda específica)
    let torrents = get_latest_torrents_1337x().await;
    println!("Found {} latest torrents", torrents.len());
    
    // Para cada torrent, obtener el magnet link y crear registro
    for torrent in torrents.iter() {
        match get_1337x_torrent_data(&torrent.link).await {
            data if !data.magnet.is_empty() => {
                if let Some(infohash) = extract_infohash_from_magnet(&data.magnet) {
                    let record = TorrentCsvRecord::from_torrent(torrent, &infohash);
                    all_records.push(record);
                    println!("  + Added: {} ({} seeders)", torrent.name, torrent.seeders);
                }
            }
            _ => println!("  - Skipped (no magnet): {}", torrent.name),
        }
        
        // Pequeña pausa entre requests
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    }
    
    // Guardar todos los registros en el CSV
    let added = append_torrents_to_csv(&csv_file, all_records)?;
    println!("\n✅ Added {} new torrents to {}", added, csv_file);
    
    Ok(())
}

fn find_latest_csv_file() -> Option<String> {
    use std::fs;
    
    let mut csv_files: Vec<_> = fs::read_dir(".")
        .ok()?
        .filter_map(|entry| entry.ok())
        .filter(|entry| {
            entry.file_name()
                .to_str()
                .map(|name| name.starts_with("torrents_part_") && name.ends_with(".csv"))
                .unwrap_or(false)
        })
        .collect();
    
    csv_files.sort_by(|a, b| {
        b.metadata().unwrap().modified().unwrap()
            .cmp(&a.metadata().unwrap().modified().unwrap())
    });
    
    csv_files.first()
        .and_then(|entry| entry.file_name().to_str().map(String::from))
}