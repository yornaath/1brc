use ahash::AHashMap as HashMap;
use memmap2::MmapOptions;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use std::{
    error::Error,
    fs::File,
    sync::{Arc},
    time::Instant,
};
fn main() -> Result<(), Box<dyn Error>> {
    let start_time = Instant::now();
    buffered()?;
    println!("Buffer: Execution time: {:?}", Instant::now() - start_time);

    Ok(())
}

// const for semicolo in bytes
const SEMICOLON: u8 = b';';
const LINE_ENDING: u8 = b'\n';

fn buffered() -> Result<(), Box<dyn Error>> {
    let file_path = "../../../data/measurements.txt";
    let file = File::open(file_path)?;

    let mmap = unsafe { MmapOptions::new().map(&file)? };
    let mmap = Arc::new(mmap);

    let len = mmap.len();

    let cores = num_cpus::get_physical();
    let chunk_count = cores;
    let chunk_size = len / chunk_count;

    let a = (0..chunk_count).into_par_iter().map(|i| {
        let chunk_start = i * chunk_size as usize;
        let chunk_end = ((i + 1) * chunk_size as usize).min(len);

        let mut start_slice = mmap[chunk_start..(chunk_start + 34)].to_vec();
        start_slice.reverse();

        let mut start_offset = 0;

        if i != 0 {
            for i in 0..64 {
                let byte = mmap[chunk_start - i];
                start_offset += 1;
                if byte == LINE_ENDING {
                    break;
                }
            }
        }

        let start = chunk_start - start_offset + (if i == 0 { 0 } else { 2 });
        let slice = &mmap[(start)..(chunk_end)];

        let mut map: HashMap<Vec<u8>, Vec<Vec<u8>>> = HashMap::new();

        let mut tuple_flag = false;
        let mut station = Vec::new();
        let mut measurement = Vec::new();

        for byte in slice {
            if *byte == SEMICOLON {
                tuple_flag = true;
            }
            else if *byte == LINE_ENDING {
                map.entry(station).or_default().push(measurement);
                tuple_flag = false;
                station = Vec::new();
                measurement = Vec::new();
            }
            else {
                if !tuple_flag {
                    station.push(*byte);
                }
                else {
                    measurement.push(*byte);
                }
            }
        }

        return map;
    })
    .reduce(HashMap::new, |mut a, b| {
        a.extend(b);
        a
    });

    // (0..chunk_count).into_par_iter().map(|i| {
    //     // get chunk from a
    //     let chunk = a.get(i).unwrap();
    // });

    Ok(())
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
