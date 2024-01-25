use std::str::from_utf8_unchecked;

use ahash::AHashMap;
use nom::combinator::iterator;
use tokio::{fs::File, sync::mpsc};
use tokio_stream::StreamExt;
use tokio_util::{
    bytes::{Buf, BytesMut},
    codec::{Decoder, Framed},
};

use crate::{parser, Temperature};

struct FastDecoder;

impl Decoder for FastDecoder {
    type Item = String;
    type Error = std::io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.is_empty() {
            return Ok(None);
        }
        let mut last_newline = src.len() - 1;
        while last_newline > 0 && src[last_newline] != b'\n' {
            last_newline -= 1;
        }
        if last_newline == 0 {
            Ok(None)
        } else {
            let (data, _) = src.split_at(last_newline + 1);
            let data = unsafe { from_utf8_unchecked(data) }.to_owned();
            src.advance(data.len());
            Ok(Some(data))
        }
    }
}

#[tokio::main]
pub async fn with_decoder() -> Vec<(String, Temperature)> {
    let file = File::open("measurements.txt").await.unwrap();
    let mut framed = Framed::with_capacity(file, FastDecoder, 8 * 1024 * 1024);
    let (tx, mut rx) = mpsc::unbounded_channel::<AHashMap<String, Temperature>>();
    tokio::spawn(async move {
        while let Some(Ok(chunk)) = framed.next().await {
            let tx = tx.clone();
            tokio::task::spawn_blocking(move || {
                let mut results: AHashMap<String, Temperature> = AHashMap::with_capacity(10000);
                iterator(chunk.as_bytes(), parser).for_each(|(city, temperature)| {
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
    let mut results: AHashMap<String, Temperature> = AHashMap::with_capacity(10000);
    while let Some(result) = rx.recv().await {
        result.iter().for_each(|(city, temperature)| {
            if let Some(value) = results.get_mut(city) {
                value.update(temperature);
            } else {
                results.insert(city.clone(), *temperature);
            }
        });
    }
    results.into_iter().collect::<Vec<(String, Temperature)>>()
}
