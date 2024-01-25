use mimalloc::MiMalloc;
use nom::{
    bytes::complete::take_until,
    character::complete::{char, newline, u8},
    combinator::opt,
    sequence::{pair, separated_pair, terminated},
    IResult,
};

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

mod tokio;

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

    fn update_single(&mut self, temperature: f32) {
        self.min = f32::min(self.min, temperature);
        self.mean = (self.mean * self.count as f32 + temperature) / (self.count + 1) as f32;
        self.max = f32::max(self.max, temperature);
        self.count += 1;
    }
}

fn fast_float(input: &[u8]) -> IResult<&[u8], f32> {
    match pair(opt(char('-')), separated_pair(u8, char('.'), u8))(input) {
        Ok((i, (sign, (integer, fraction)))) => {
            let float = integer as f32 + fraction as f32 / 10.0;
            Ok((i, if sign.is_some() { -float } else { float }))
        }
        Err(error) => Err(error),
    }
}

fn parser(input: &[u8]) -> IResult<&[u8], (&[u8], f32)> {
    terminated(
        separated_pair(take_until(";"), char(';'), fast_float),
        newline,
    )(input)
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
    let results = tokio::with_decoder();
    let output = format_results(&results);
    assert_eq!(BASELINE, output);
    println!("{}", output);
}
