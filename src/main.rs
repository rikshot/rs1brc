use anyhow::{anyhow, Result};
use dashmap::DashMap;
use std::{
    fs::File,
    io::{BufRead, BufReader, Seek},
    sync::Arc,
    thread::{self, JoinHandle},
};

#[derive(Debug)]
struct Temperature {
    min: f32,
    mean: f32,
    max: f32,
    count: u32,
}

impl Temperature {
    fn new(temperature: f32) -> Self {
        Self {
            min: temperature,
            mean: temperature,
            max: temperature,
            count: 1,
        }
    }

    fn update(&mut self, other: &Temperature) {
        self.min = f32::min(self.min, other.min);
        self.mean = (self.mean * self.count as f32 + other.mean * other.count as f32)
            / (self.count + other.count) as f32;
        self.max = f32::max(self.max, other.max);
        self.count += other.count;
    }
}

fn parse_line(line: &str) -> Result<(&str, f32)> {
    let (city, temperature) = line
        .split_once(';')
        .ok_or(anyhow!("Failed to split line: {}", line))?;
    Ok((city, temperature.parse::<f32>()?))
}

fn format_results(results: &Arc<DashMap<String, Temperature>>) -> String {
    let mut results = results
        .iter()
        .map(|result| {
            format!(
                "{}={:.1}/{:.1}/{:.1}",
                result.key(),
                result.min,
                result.mean,
                result.max
            )
        })
        .collect::<Vec<String>>();
    results.sort_unstable();
    format!("{{{}}}", results.join(", "))
}

static BASELINE: &str = include_str!("../baseline.txt");

fn main() {
    let results = with_dashmap();
    let output = format_results(&results);
    assert_eq!(BASELINE, output);
    println!("{}", output);
}

fn with_dashmap() -> Arc<DashMap<String, Temperature>> {
    let file = File::open("measurements.txt").unwrap();
    let length = file.metadata().unwrap().len();

    let cpus = num_cpus::get() as u64;
    let chunk_length = length / cpus;
    let last_chunk_length = chunk_length + length % cpus;

    let mut chunks: Vec<(u64, u64)> = (0..cpus)
        .map(|index| {
            let start = index * chunk_length;
            let end = start
                + if index < cpus - 1 {
                    chunk_length
                } else {
                    last_chunk_length
                };
            (start, end)
        })
        .collect();

    (1..cpus as usize).for_each(|index| {
        let (start, _) = chunks[index];
        let mut file = File::open("measurements.txt").unwrap();
        file.seek(std::io::SeekFrom::Start(start))
            .expect("Failed to seek file");
        let mut reader = BufReader::new(file);
        let mut buffer = String::with_capacity(100);
        let count = reader.read_line(&mut buffer).expect("Failed to read line");
        if parse_line(&buffer).is_err() {
            let (_, end) = &mut chunks[index - 1];
            *end += count as u64;
            let (start, _) = &mut chunks[index];
            *start += count as u64;
        }
    });

    let results: Arc<DashMap<String, Temperature>> = Arc::new(DashMap::with_capacity(10000));
    let threads: Vec<JoinHandle<()>> = (0..cpus)
        .map(|index| {
            let results = results.clone();
            let (start, end) = chunks[index as usize];
            thread::spawn(move || {
                let mut file = File::open("measurements.txt").unwrap();
                file.seek(std::io::SeekFrom::Start(start))
                    .expect("Failed to seek file");
                let mut reader = BufReader::with_capacity(128 * 1024 * 1024, file);
                let mut buffer = String::with_capacity(100);
                let mut position = start;
                while position < end {
                    position += reader.read_line(&mut buffer).expect("Failed to read line") as u64;
                    let (city, temperature) = parse_line(buffer.trim_end()).unwrap();
                    let temperature = Temperature::new(temperature);
                    if let Some(mut value) = results.get_mut(city) {
                        value.update(&temperature);
                    } else {
                        results.insert(city.to_owned(), temperature);
                    }
                    buffer.clear();
                }
            })
        })
        .collect();

    for thread in threads {
        thread.join().unwrap();
    }

    results.clone()
}
