use std::{hash::BuildHasherDefault, str::from_utf8_unchecked};

use nom::{
    bytes::complete::take_until,
    character::complete::{char, newline, u8},
    combinator::{iterator, opt},
    sequence::{pair, separated_pair, terminated},
    IResult,
};
use rustc_hash::FxHashMap;
use tokio::{fs::File, sync::mpsc};
use tokio_stream::StreamExt;
use tokio_util::{
    bytes::{Buf, BytesMut},
    codec::{Decoder, Framed},
};

use crate::Temperature;

fn fast_float(input: &[u8]) -> IResult<&[u8], f32> {
    match pair(opt(char('-')), separated_pair(u8, char('.'), u8))(input) {
        Ok((i, (sign, (integer, fraction)))) => {
            let float = integer as f32 + fraction as f32 * 0.1;
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

struct ChunkDecoder;

impl Decoder for ChunkDecoder {
    type Item = Vec<u8>;
    type Error = std::io::Error;

    #[inline]
    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        match src.iter().rposition(|c| *c == b'\n') {
            Some(index) => {
                let (data, _) = src.split_at(index + 1);
                let data = data.to_owned();
                src.advance(data.len());
                Ok(Some(data))
            }
            None => Ok(None),
        }
    }
}

type ResultMap = FxHashMap<String, Temperature>;

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
                let mut results =
                    ResultMap::with_capacity_and_hasher(10000, BuildHasherDefault::default());
                iterator(chunk.as_slice(), parser).for_each(|(city, temperature)| {
                    let city = unsafe { from_utf8_unchecked(city) };
                    if let Some(value) = results.get_mut(city) {
                        value.update_single(temperature);
                    } else {
                        results.insert(city.to_owned(), Temperature::new(temperature));
                    }
                });
                tx.send(results).unwrap();
            });
        }
    });
    let results = tokio::task::spawn_blocking(move || {
        let mut results = ResultMap::with_capacity_and_hasher(10000, BuildHasherDefault::default());
        while let Some(result) = rx.blocking_recv() {
            result.iter().for_each(|(city, temperature)| {
                if let Some(value) = results.get_mut(city) {
                    value.update(temperature);
                } else {
                    results.insert(city.clone(), *temperature);
                }
            });
        }
        results
    });
    results
        .await
        .unwrap()
        .into_iter()
        .collect::<Vec<(String, Temperature)>>()
}
