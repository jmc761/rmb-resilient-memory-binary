use std::fs::File;
use std::io;
use memmap2::Mmap;

use crate::record::IndexRecord;

pub struct IndexReader {
    mmap: Mmap,
}

impl IndexReader {
    /// Opens the .idx file and asks the OS to map it into memory.
    pub fn new(path: &str) -> io::Result<Self> {
        let file = File::open(path)?;
        
        // Why do we use 'unsafe' here?
        // Because mmap assumes no other program will modify or delete the file
        // while we're reading it. We promise Rust that we control the server.
        let mmap = unsafe { memmap2::MmapOptions::new().map(&file)? };
        
        Ok(Self { mmap })
    }

    /// Searches for a law by its ID using Binary Search (O(log N))
    /// ASSUMES: That the .idx file was sorted from lowest to highest ID by our Indexer.
    pub fn find_by_id(&self, target_id: u32) -> Option<IndexRecord> {
        let record_size = 16;
        let total_records = self.mmap.len() / record_size;

        if total_records == 0 {
            return None;
        }

        let mut low = 0;
        let mut high = total_records as isize - 1;

        // Classic Binary Search, but jumping in RAM memory
        while low <= high {
            let mid = low + (high - low) / 2;
            let byte_offset = (mid as usize) * record_size;

            // Extract the exact 16 bytes from memory
            let chunk = &self.mmap[byte_offset .. byte_offset + record_size];
            let record = IndexRecord::from_le_bytes(chunk);

            if record.id == target_id {
                return Some(record); // Found!
            } else if record.id < target_id {
                low = mid + 1; // It's in the right half
            } else {
                high = mid - 1; // It's in the left half
            }
        }

        None // Does not exist in the index
    }
}