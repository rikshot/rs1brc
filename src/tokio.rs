use std::str::from_utf8_unchecked;

use nom::{
    bytes::complete::take_until,
    character::complete::{char, newline, u8},
    combinator::{iterator, opt},
    sequence::{pair, separated_pair, terminated},
    IResult,
};
use tokio::{fs::File, sync::mpsc};
use tokio_stream::StreamExt;
use tokio_util::{
    bytes::BytesMut,
    codec::{Decoder, Framed},
};

use crate::map::Map;
use crate::Temperature;

fn temperature(input: &[u8]) -> IResult<&[u8], i32> {
    match pair(opt(char('-')), separated_pair(u8, char('.'), u8))(input) {
        Ok((i, (sign, (integer, fraction)))) => {
            let temp = integer as i32 * 10 + fraction as i32;
            Ok((i, if sign.is_some() { -temp } else { temp }))
        }
        Err(error) => Err(error),
    }
}

fn parser(input: &[u8]) -> IResult<&[u8], (&[u8], i32)> {
    terminated(
        separated_pair(take_until(";"), char(';'), temperature),
        newline,
    )(input)
}

struct ChunkDecoder;

impl Decoder for ChunkDecoder {
    type Item = Vec<u8>;
    type Error = std::io::Error;

    #[inline]
    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        match src.iter().rposition(|c| *c == b'\n') {
            Some(index) => {
                let data = src.split_to(index + 1);
                Ok(Some(data.to_vec()))
            }
            None => Ok(None),
        }
    }
}

type ResultMap = Map<Temperature, 2000>;

#[tokio::main]
pub async fn with_decoder() -> Vec<(String, Temperature)> {
    let (tx, mut rx) = mpsc::unbounded_channel::<ResultMap>();
    tokio::spawn(async move {
        let file = File::open("measurements.txt").await.unwrap();
        // Tokio MAX_BUF for blocking IO: https://github.com/tokio-rs/tokio/blob/10c9eeb6c2af85961044b7cbb16a5a2d2e97287d/tokio/src/io/blocking.rs#L26
        let mut framed = Framed::with_capacity(file, ChunkDecoder, 2 * 1024 * 1024);
        while let Some(Ok(chunk)) = framed.next().await {
            let tx = tx.clone();
            tokio::task::spawn_blocking(move || {
                let mut results = ResultMap::new();
                iterator(chunk.as_slice(), parser).for_each(|(city, temperature)| {
                    let city = unsafe { from_utf8_unchecked(city) };
                    if let Some(value) = results.get_mut(city) {
                        value.update_single(temperature);
                    } else {
                        results.set(city, &Temperature::new(temperature));
                    }
                });
                tx.send(results).unwrap();
            });
        }
    });
    let results = tokio::task::spawn_blocking(move || {
        let mut results = ResultMap::new();
        while let Some(result) = rx.blocking_recv() {
            for entry in result.table.iter().flatten() {
                if let Some(value) = results.get_mut(&entry.key) {
                    value.update(&entry.value);
                } else {
                    results.set(&entry.key, &entry.value);
                }
            }
        }
        results
    });
    results
        .await
        .unwrap()
        .table
        .into_iter()
        .flatten()
        .map(|entry| (entry.key, entry.value))
        .collect::<Vec<(String, Temperature)>>()
}
