#[cfg(feature = "mimalloc")]
use mimalloc::MiMalloc;

use rayon::prelude::*;

use crate::temperature::Temperature;

#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

mod map;
mod parser;
mod temperature;

#[cfg(feature = "tokio")]
mod tokio;

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

fn main() {
    let results = tokio::with_decoder();
    let output = format_results(&results);
    #[cfg(feature = "assert_result")]
    {
        static BASELINE: &str = include_str!("../baseline.txt");
        pretty_assertions::assert_eq!(BASELINE, output);
    }
    println!("{output}");
}
