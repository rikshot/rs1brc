use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

mod mmap;

#[derive(Debug, Clone, Copy)]
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

fn format_results(results: &[(String, Temperature)]) -> String {
    let mut results = results
        .iter()
        .map(|(city, temperature)| {
            format!(
                "{}={:.1}/{:.1}/{:.1}",
                city, temperature.min, temperature.mean, temperature.max
            )
        })
        .collect::<Vec<String>>();
    results.sort_unstable();
    format!("{{{}}}", results.join(", "))
}

static BASELINE: &str = include_str!("../baseline.txt");

fn main() {
    let results = mmap::with_mmap();
    let output = format_results(&results);
    assert_eq!(BASELINE, output);
    println!("{}", output);
}
