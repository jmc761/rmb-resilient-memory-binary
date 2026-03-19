/// Represents an exact 16-byte entry in our .idx file
/// We use repr(C) to ensure the compiler does not reorder fields.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct IndexRecord {
    pub offset: u64, // 8 bytes (bytes 0 through 7)
    pub id: u32,     // 4 bytes (bytes 8 through 11)
    pub length: u32, // 4 bytes (bytes 12 through 15)
                     // Total exact: 16 bytes. Zero padding.
}

impl IndexRecord {
    /// Converts our struct to raw 16 bytes (Little Endian)
    /// This is what we write directly to disk.
    pub fn to_le_bytes(&self) -> [u8; 16] {
        let mut buf = [0u8; 16];
        
        // Copy each field's bytes into the general buffer
        buf[0..8].copy_from_slice(&self.offset.to_le_bytes());
        buf[8..12].copy_from_slice(&self.id.to_le_bytes());
        buf[12..16].copy_from_slice(&self.length.to_le_bytes());
        
        buf
    }

    /// Reads raw 16 bytes (from mmap) and converts it into our struct
    pub fn from_le_bytes(buf: &[u8]) -> Self {
        // Extract each chunk and convert back to numbers
        let offset = u64::from_le_bytes(buf[0..8].try_into().unwrap());
        let id = u32::from_le_bytes(buf[8..12].try_into().unwrap());
        let length = u32::from_le_bytes(buf[12..16].try_into().unwrap());
        
        Self { offset, id, length }
    }
}