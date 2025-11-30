
use memmap2::MmapOptions;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt::Write;
use std::hash::{Hash, Hasher};
use std::{error::Error, fs::File, sync::Arc};

// const for semicolo in bytes
// used for parsing lines
const SEMICOLON: u8 = b';';
const LINE_ENDING: u8 = b'\n';

// const for max station and measurement length
// These could be tuned for the specific data and/or configurable by the consumer to suit the data
const MAX_STATION: usize = 64;
const MAX_MEASUREMENT: usize = 8;

fn main() -> Result<(), Box<dyn Error>> {
    let result = aggregate_measurements()?;
    println!("{}", result);
    Ok(())
}

fn aggregate_measurements() -> Result<String, Box<dyn Error>> {
    let file_path = "../../../measurements.txt";
    let file = File::open(file_path)?;

    // map the file to memory, IO disk so reading isnt a bottleneck
    let mmap = unsafe { MmapOptions::new().map(&file)? };
    let mmap = Arc::new(mmap);

    let len = mmap.len();

    let cores = num_cpus::get_physical();
    let chunk_count = cores * 8;
    let chunk_size = len / chunk_count;

    let chunk_mapper = (0..chunk_count).into_par_iter().map(|i| {
        let chunk_start = i * chunk_size as usize;
        let chunk_end = ((i + 1) * chunk_size as usize).min(len);

        let mut start_slice = mmap[chunk_start..(chunk_start + 34)].to_vec();
        start_slice.reverse();

        let mut start_offset = 0;

        if i != 0 {
            for i in 0..128 {
                let byte = mmap[chunk_start - i];
                start_offset += 1;
                if byte == LINE_ENDING {
                    break;
                }
            }
        }

        let start = chunk_start - start_offset + (if i == 0 { 0 } else { 2 });

        let mut chunk_results: HashMap<SmallBuf<MAX_STATION>, (f32, f32, f32, f32)> = HashMap::new();

        let mut tuple_flag = false;
        let mut station: SmallBuf<MAX_STATION> = SmallBuf::new();
        let mut measurement: SmallBuf<MAX_MEASUREMENT> = SmallBuf::new();

        let slice = &mmap[start..chunk_end];

        for byte in slice {
            if *byte == SEMICOLON {
                tuple_flag = true;
            } else if *byte == LINE_ENDING {
                let temp = measurement.to_f32();

                chunk_results.entry(station.clone())
                    .and_modify(|e| {
                        let min = e.0.min(temp);
                        let sum = e.1 + temp;
                        let count = e.2 + 1 as f32;
                        let max = e.3.max(temp);
                        *e = (min, sum, count, max);
                    })
                    .or_insert((temp, temp, 1 as f32, temp));

                tuple_flag = false;
                station.clear();
                measurement.clear();
            } else {
                if !tuple_flag {
                    station.push(*byte);
                } else {
                    measurement.push(*byte);
                }
            }
        }

        return chunk_results;
    });

    // can I parallelize this?
    let summed_chunks = chunk_mapper.reduce(HashMap::new, |mut aggregator, chunk| {
        for (station, results) in chunk.iter() {
            aggregator
                .entry(station.to_owned())
                .and_modify(|e| {
                    let min = e.0.min(results.0);
                    let sum = e.1 + results.1;
                    let count = e.2 + results.2;
                    let max = e.3.max(results.3);
                    *e = (min, sum, count, max);
                })
                .or_insert(*results);
        }
        aggregator
    });
    
    let mut stations: Vec<_> = summed_chunks.keys().collect();
    stations.sort();

    let mut output_body: Vec<String> = Vec::with_capacity(stations.len());
    
    for station in stations {
        let (min, sum, count, max) = summed_chunks.get(station).unwrap();
        let station_name = std::str::from_utf8(station.as_slice()).unwrap(); // no allocation

        let avg_temp = sum / count;

        let mut line = String::with_capacity(station_name.len() + 32);

        write!(
            &mut line,
            "{}={:.1}/{:.1}/{:.1}",
            station_name, min, avg_temp, max
        )
        .unwrap();

        output_body.push(line);
    }

    let output_body = output_body.join(", ");

    let output = format!("{{{}}}", output_body);

    Ok(output)
}

#[derive(Clone)]
pub struct SmallBuf<const N: usize> {
    buf: [u8; N],
    len: usize,
}

impl<const N: usize> PartialEq for SmallBuf<N> {
    #[inline(always)]
    fn eq(&self, other: &Self) -> bool {
        self.len == other.len && self.buf[..self.len] == other.buf[..other.len]
    }
}

impl<const N: usize> PartialOrd for SmallBuf<N> {
    #[inline(always)]
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<const N: usize> Ord for SmallBuf<N> {
    #[inline(always)]
    fn cmp(&self, other: &Self) -> Ordering {
        self.buf[..self.len].cmp(&other.buf[..other.len])
    }
}

impl<const N: usize> Eq for SmallBuf<N> {}

impl<const N: usize> Hash for SmallBuf<N> {
    #[inline(always)]
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Hash only the slice that contains meaningful bytes
        state.write(&self.buf[..self.len]);
        self.len.hash(state); // Include length for uniqueness
    }
}

impl<const N: usize> SmallBuf<N> {
    #[inline(always)]
    pub fn new() -> Self {
        Self {
            buf: [0; N],
            len: 0,
        }
    }

    #[inline(always)]
    pub fn push(&mut self, byte: u8) {
        self.buf[self.len] = byte;
        self.len += 1;
        // Optional: omit this check in release mode for absolute zero cost:
        //debug_assert!(self.len < N);
    }

    pub fn to_f32(&self) -> f32 {
        let s = unsafe { std::str::from_utf8_unchecked(self.as_slice()) };
        let value = s.parse::<f32>().unwrap();
        value
    }

    pub fn extend_from_slice(&mut self, slice: &[u8]) {
        self.buf[self.len..self.len + slice.len()].copy_from_slice(slice);
        self.len += slice.len();
    }

    #[inline(always)]
    pub fn clear(&mut self) {
        self.len = 0;
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        self.len
    }

    #[inline(always)]
    pub fn as_slice(&self) -> &[u8] {
        &self.buf[..self.len]
    }
}
