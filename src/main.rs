use mimalloc::MiMalloc;
use rayon::prelude::*;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

mod map;
mod parser;
mod tokio;

#[derive(Debug, Clone, Copy)]
struct Temperature {
    min: i32,
    max: i32,
    sum: i32,
    count: u32,
}

impl Temperature {
    fn new(temperature: i32) -> Self {
        Self {
            min: temperature,
            max: temperature,
            sum: temperature,
            count: 1,
        }
    }

    fn update(&mut self, other: &Temperature) {
        self.min = i32::min(self.min, other.min);
        self.max = i32::max(self.max, other.max);
        self.sum += other.sum;
        self.count += other.count;
    }

    fn update_single(&mut self, temperature: i32) {
        self.min = i32::min(self.min, temperature);
        self.max = i32::max(self.max, temperature);
        self.sum += temperature;
        self.count += 1;
    }
}

fn format_results(results: &[(String, Temperature)]) -> String {
    let mut results = results
        .par_iter()
        .map(|(city, temperature)| {
            format!(
                "{}={:.1}/{:.1}/{:.1}",
                city,
                temperature.min as f32 / 10.0,
                temperature.sum as f32 / temperature.count as f32 / 10.0,
                temperature.max as f32 / 10.0
            )
        })
        .collect::<Vec<String>>();
    results.par_sort_unstable();
    format!("{{{}}}", results.join(", "))
}

#[cfg(feature = "assert_result")]
fn main() {
    static BASELINE: &str = include_str!("../baseline.txt");
    let results = tokio::with_decoder();
    let output = format_results(&results);
    assert_eq!(BASELINE, output);
    println!("{}", output);
}

#[cfg(not(feature = "assert_result"))]
fn main() {
    let results = tokio::with_decoder();
    let output = format_results(&results);
    println!("{}", output);
}
