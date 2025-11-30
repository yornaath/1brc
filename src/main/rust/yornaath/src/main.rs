use ahash::{HashMap, HashMapExt};
use memmap2::MmapOptions;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use std::cmp::Ordering;
use std::{error::Error, fs::File, sync::Arc};
use std::{
    hash::{Hash, Hasher},
};

// const for semicolo in bytes
// used for parsing lines
const SEMICOLON: u8 = b';';
const LINE_ENDING: u8 = b'\n';

// const for max station and measurement length
// These could be tuned for the specific data and/or configurable by the consumer to suit the data
const MAX_STATION: usize = 64;
const MAX_MEASUREMENT: usize = 8;

fn main() -> Result<(), Box<dyn Error>> {
    let result = calculate()?;
    println!("{}", result);
    Ok(())
}

fn calculate() -> Result<String, Box<dyn Error>> {
    let file_path = "../../../measurements.txt";
    let file = File::open(file_path)?;

    // map the file to memory, IO disk so reading isnt a bottleneck
    let mmap = unsafe { MmapOptions::new().map(&file)? };
    let mmap = Arc::new(mmap);

    let len = mmap.len();

    let cores = num_cpus::get_physical();
    let chunk_count = cores * 8;
    let chunk_size = len / chunk_count;

    let mapped_chunks = (0..chunk_count).into_par_iter().map(|i| {
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
        let slice = &mmap[(start)..(chunk_end)];

        let mut map: HashMap<SmallBuf<MAX_STATION>, Vec<SmallBuf<MAX_MEASUREMENT>>> =
            HashMap::new();

        let mut tuple_flag = false;
        let mut station: SmallBuf<MAX_STATION> = SmallBuf::new();
        let mut measurement: SmallBuf<MAX_MEASUREMENT> = SmallBuf::new();

        for byte in slice {
            if *byte == SEMICOLON {
                tuple_flag = true;
            } else if *byte == LINE_ENDING {
                map.entry(station).or_default().push(measurement);
                tuple_flag = false;
                station = SmallBuf::new();
                measurement = SmallBuf::new();
            } else {
                if !tuple_flag {
                    station.push(*byte);
                } else {
                    measurement.push(*byte);
                }
            }
        }

        let mut chunk_calculations: HashMap<SmallBuf<MAX_STATION>, (f32, f32, f32, f32)> =
            HashMap::new();

        for (key, value) in map.iter() {
            let mut sum: f32 = 0.0;
            let mut min: f32 = f32::INFINITY;
            let mut max: f32 = f32::NEG_INFINITY;
            let mut count: usize = 0;

            for temp_bytes in value {
                let temp = std::str::from_utf8(temp_bytes.as_slice())
                    .unwrap()
                    .parse::<f32>()
                    .unwrap();

                sum += temp;
                if temp < min {
                    min = temp;
                }
                if temp > max {
                    max = temp;
                }
                count += 1;
            }

            chunk_calculations.insert(key.clone(), (min, sum, count as f32, max));
        }

        return chunk_calculations;
    });

    let reduced_chunks = mapped_chunks.reduce(HashMap::new, |mut a, b| {
        //let start_time = Instant::now();
        //a.extend(b);
        for (key, value) in b.iter() {
            a.entry(key.clone())
                .and_modify(|e| {
                    let min = e.0.min(value.0);
                    let sum = e.1 + value.1;
                    let count = e.2 + value.2;
                    let max = e.3.max(value.3);
                    *e = (min, sum, count, max);
                })
                .or_insert(value.clone());
        }
        a
    });

    let mut stations: Vec<_> = reduced_chunks.keys().collect();
    stations.sort();

    let mut output_body: Vec<String> = vec![];

    for station in stations {
        let (min, sum, count, max) = reduced_chunks.get(station).unwrap();
        let station_name = std::str::from_utf8(station.as_slice()).unwrap(); // no allocation

        let avg_temp = sum / count;

        output_body.push(format!(
            "{}={:.1}/{:.1}/{:.1}",
            station_name, min, avg_temp, max
        ));
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

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}

// fn read_chunk(file: &mut File, start: u64, chunk_size: u64) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
//     let mut buffer = vec![start as u8; chunk_size as usize];
//     file.read_at(&mut buffer, start)?;
//     Ok(buffer)
// }

// fn read_whole_file() -> Result<(), Box<dyn Error>> {
//     let file_path = "../../../data/measurements.txt";
//     let mut file = File::open(file_path)?;
//     for byt in file.bytes() {

//     }
//     let file_size = file.metadata()?.len();
//     let mut buffer = vec![0; file_size as usize];
//     file.read_to_end(&mut buffer)?;
//     Ok(())
// }
