use reqwest;
use scraper::{Html, Selector};
use serde::{Deserialize, Serialize};
use chrono::NaiveDateTime;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Torrent {
    pub name: String,
    pub seeders: i32,
    pub leechers: i32,
    pub size: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub date: Option<i64>,
    pub uploader: String,
    pub link: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TorrentData {
    pub magnet: String,
    pub files: Vec<String>,
}

pub struct TorrentProxies {
    pub x1337: Vec<String>,
    pub the_pirate_bay: Vec<String>,
    pub rarbg: Vec<String>,
}

impl Default for TorrentProxies {
    fn default() -> Self {
        Self {
            x1337: vec!["https://1337xx.to".to_string()],
            the_pirate_bay: vec!["https://www1.thepiratebay3.to".to_string()],
            rarbg: vec![],
        }
    }
}

const MAX_PAGES: i32 = 1;

pub fn to_int(value: &str) -> Result<i32, std::num::ParseIntError> {
    value.replace(",", "").parse()
}

pub fn convert_bytes(mut num: f64) -> String {
    let units = ["bytes", "KB", "MB", "GB", "TB"];
    let step_unit = 1000.0;
    
    for unit in units.iter() {
        if num < step_unit {
            return format!("{:.1} {}", num, unit);
        }
        num /= step_unit;
    }
    format!("{:.1} TB", num)
}

pub fn get_tpb_trackers() -> String {
    let trackers = vec![
        "udp://tracker.coppersurfer.tk:6969/announce",
        "udp://9.rarbg.to:2920/announce",
        "udp://tracker.opentrackr.org:1337",
        "udp://tracker.internetwarriors.net:1337/announce",
        "udp://tracker.leechers-paradise.org:6969/announce",
        "udp://tracker.pirateparty.gr:6969/announce",
        "udp://tracker.cyberia.is:6969/announce",
    ];
    
    trackers.iter()
        .map(|t| format!("&tr={}", urlencoding::encode(t)))
        .collect::<String>()
}



pub fn parse_date(date_str: &str, format: &str) -> Option<i64> {
    NaiveDateTime::parse_from_str(date_str, format)
        .ok()
        .map(|dt| dt.and_utc().timestamp())
}

pub async fn get(url: &str) -> Result<String, reqwest::Error> {
    let client = reqwest::Client::new();
    client
        .get(url)
        .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
        .header("Accept-Encoding", "*")
        .send()
        .await?
        .text()
        .await
}

pub async fn get_latest_torrents_1337x() -> Vec<Torrent> {
    let proxies = TorrentProxies::default();
    let mut torrents = Vec::new();
    
    for proxy in &proxies.x1337 {
        let mut pg_no = 1;
        
        while pg_no <= MAX_PAGES {
            // URL para obtener los últimos torrents subidos
            let url = format!("{}/trending", proxy);
            println!("Fetching: {}", url);
            
            match get(&url).await {
                Ok(html) => {
                    let document = Html::parse_document(&html);
                    let row_selector = Selector::parse("tbody > tr").unwrap();
                    let name_selector = Selector::parse("td.coll-1 > a").unwrap();
                    let seeders_selector = Selector::parse("td.coll-2").unwrap();
                    let leechers_selector = Selector::parse("td.coll-3").unwrap();
                    let size_selector = Selector::parse("td.coll-4").unwrap();
                    let date_selector = Selector::parse("td.coll-date").unwrap();
                    let uploader_selector = Selector::parse("td.coll-5 > a").unwrap();
                    
                    for row in document.select(&row_selector) {
                        if let Some(name_elem) = row.select(&name_selector).nth(1) {
                            let name = name_elem.text().collect::<String>();
                            
                            if let (Some(href), Some(seeders), Some(leechers), Some(size), Some(date), Some(uploader)) = (
                                name_elem.value().attr("href"),
                                row.select(&seeders_selector).next(),
                                row.select(&leechers_selector).next(),
                                row.select(&size_selector).next(),
                                row.select(&date_selector).next(),
                                row.select(&uploader_selector).next(),
                            ) {
                                let date_text = date.text().collect::<String>()
                                    .replace("nd", "").replace("th", "")
                                    .replace("rd", "").replace("st", "");
                                
                                torrents.push(Torrent {
                                    name,
                                    seeders: to_int(&seeders.text().collect::<String>()).unwrap_or(0),
                                    leechers: to_int(&leechers.text().collect::<String>()).unwrap_or(0),
                                    size: size.text().collect::<String>().split('B').next().unwrap_or("").to_string() + "B",
                                    date: parse_date(&date_text, "%b. %d '%y"),
                                    uploader: uploader.text().collect::<String>(),
                                    link: format!("{}{}", proxy, href),
                                });
                            }
                        }
                    }
                }
                Err(e) => {
                    eprintln!("Error: {}", e);
                    continue;
                }
            }
            pg_no += 1;
        }
        break;
    }
    
    torrents
}

pub async fn get_1337x_torrent_data(link: &str) -> TorrentData {
    let mut data = TorrentData {
        magnet: String::new(),
        files: Vec::new(),
    };
    
    match get(link).await {
        Ok(html) => {
            let document = Html::parse_document(&html);
            let magnet_selector = Selector::parse("ul.dropdown-menu > li a").unwrap();
            let files_selector = Selector::parse("div.file-content > ul > li").unwrap();
            
            if let Some(magnet) = document.select(&magnet_selector).last() {
                if let Some(href) = magnet.value().attr("href") {
                    data.magnet = href.to_string();
                }
            }
            
            for file in document.select(&files_selector) {
                let text: String = file.text().collect::<String>().replace("\n", "");
                data.files.push(text);
            }
        }
        Err(e) => eprintln!("Error: {}", e),
    }
    
    data
}

#[derive(Debug, Deserialize)]
struct ApiResponse {
    name: String,
    seeders: String,
    leechers: String,
    size: String,
    username: String,
    id: String,
}

pub async fn search_tpb_api(search_key: &str) -> Vec<Torrent> {
    let url = format!("http://apibay.org/q.php?q={}&cat=100,200,300,400,600", search_key);
    let mut torrents = Vec::new();
    
    match reqwest::get(&url).await {
        Ok(response) => {
            if let Ok(resp_json) = response.json::<Vec<ApiResponse>>().await {
                if resp_json.is_empty() || resp_json[0].name == "No results returned" {
                    return torrents;
                }
                
                for t in resp_json {
                    torrents.push(Torrent {
                        name: t.name,
                        seeders: to_int(&t.seeders).unwrap_or(0),
                        leechers: to_int(&t.leechers).unwrap_or(0),
                        size: convert_bytes(t.size.parse::<f64>().unwrap_or(0.0)),
                        date: None,
                        uploader: t.username,
                        link: format!("http://apibay.org/t.php?id={}", t.id),
                    });
                }
            }
        }
        Err(e) => eprintln!("Error: {}", e),
    }
    
    torrents
}

// Cargo.toml dependencies needed:
// [dependencies]
// reqwest = { version = "0.11", features = ["json"] }
// scraper = "0.17"
// serde = { version = "1.0", features = ["derive"] }
// serde_json = "1.0"
// regex = "1.10"
// chrono = "0.4"
// urlencoding = "2.1"
// tokio = { version = "1", features = ["full"] }