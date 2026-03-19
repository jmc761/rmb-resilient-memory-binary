pub mod record;
pub mod reader;

pub use record::IndexRecord;
pub use reader::IndexReader;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_memory_serialization() {
        // Create a test record
        let record = IndexRecord {
            offset: 5_000_000, // Suppose the record is at byte 5 million of the JSONL
            id: 23737,         // "Ley de Estupefacientes" Narcotics law
            length: 1024,      // JSON size is 1 KB
        };

        // Convert to raw bytes
        let bytes = record.to_le_bytes();
        
        // Assert it is exactly 16 bytes long
        assert_eq!(bytes.len(), 16);

        // Reconstruct from raw bytes
        let reconstructed = IndexRecord::from_le_bytes(&bytes);

        // Assert no data was lost or changed
        assert_eq!(record, reconstructed);
        
        println!("The record serializes and deserializes perfectly to 16 bytes!");
    }

    #[test]
    fn test_index_reader() {
        use std::io::Write;
        use tempfile::NamedTempFile;

        // Create a temporary file that will auto-destroy at the end of the test
        let mut temp_file = NamedTempFile::new().expect("Could not create temporary file");

        // Create 3 test records sorted by ID
        let rec1 = IndexRecord { offset: 100, id: 10, length: 50 };
        let rec2 = IndexRecord { offset: 200, id: 25, length: 80 }; // The one we'll search for
        let rec3 = IndexRecord { offset: 300, id: 40, length: 90 };

        // Write the raw bytes directly to the file on disk
        temp_file.write_all(&rec1.to_le_bytes()).unwrap();
        temp_file.write_all(&rec2.to_le_bytes()).unwrap();
        temp_file.write_all(&rec3.to_le_bytes()).unwrap();
        
        // It's vital to force the OS to flush buffers to disk before mmap
        temp_file.flush().unwrap(); 

        // Instantiate our real reader by passing the temporary file path
        let file_path = temp_file.path().to_str().unwrap();
        let reader = IndexReader::new(file_path).expect("Failed to mmap the file");

        // Test
        let result = reader.find_by_id(25);

        // Verify that the search worked and brought the correct record
        assert!(result.is_some(), "Should have found ID 25");
        
        let found_record = result.unwrap();
        assert_eq!(found_record.id, 25);
        assert_eq!(found_record.offset, 200);
        assert_eq!(found_record.length, 80);

        // Test searching for a non-existent ID to ensure it doesn't crash
        let does_not_exist = reader.find_by_id(999);
        assert!(does_not_exist.is_none(), "Should not find ID 999");

        println!("The IndexReader works perfectly with mmap!");
    }
}