use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use serde::{Deserialize, Serialize};
// Import our crate from workspace `core`
use rmb_core::IndexRecord; 

// --- 1. DATA STRUCTURES ---

/// We simulate what Postgres would emit (dirty or raw data)
#[derive(Deserialize)]
struct PostgresRow {
    id_ley: u32,
    texto_crudo: String,
    fecha_sancion: String,
}

/// We simulate the strict contract of our API (the perfect JSONL)
#[derive(Serialize)]
struct ApiRespuesta {
    id: u32,
    metadata: Metadata,
    contenido: String,
}

#[derive(Serialize)]
struct Metadata {
    fecha: String,
    sistema: String,
}

// --- 2. MAIN LOGIC ---

fn main() -> std::io::Result<()> {
    println!("Starting the RMB Indexer...");

    // 1. Open files (for this example, assume input.jsonl exists)
    // Note: we use BufReader and BufWriter to avoid exhausting RAM or disk.
    let input_file = File::open("input.jsonl").expect("Missing input.jsonl file for testing");
    let reader = BufReader::new(input_file);

    let output_jsonl = File::create("perfect.jsonl")?;
    let mut json_writer = BufWriter::new(output_jsonl);

    let output_idx = File::create("main.idx")?;
    let mut idx_writer = BufWriter::new(output_idx);

    // Our global cursor in the new perfect.jsonl file
    let mut current_offset: u64 = 0;
    let mut processed = 0;

    // 2. Read the input file line by line (pure stream)
    for line_result in reader.lines() {
        let line = line_result?;
        if line.trim().is_empty() { continue; }

        // A. Parse the dirty Postgres JSON
        let row: PostgresRow = serde_json::from_str(&line)
            .expect("Error parsing Postgres line");

        // B. Transform to API format
        let api_doc = ApiRespuesta {
            id: row.id_ley,
            metadata: Metadata {
                fecha: row.fecha_sancion,
                sistema: "RMB_Fallback".to_string(),
            },
            contenido: row.texto_crudo,
        };

        // C. Serialize the new JSON.
        // NOTE: Force newline to '\n' (1 byte) explicitly.
        // On Windows it is usually '\r\n' (2 bytes). On our Debian server it will be '\n'.
        // We must be strict so the byte math doesn't fail in production.
        let mut new_line = serde_json::to_string(&api_doc)?;
        new_line.push('\n'); 

        // Calculate the exact byte length of this line (UTF-8)
        let bytes_line = new_line.as_bytes();
        let length = bytes_line.len() as u32;

        // D. Write the string to the new JSONL
        json_writer.write_all(bytes_line)?;

        // E. Create the record for the binary engine using our `core`
        let record = IndexRecord {
            id: api_doc.id,
            offset: current_offset,
            length,
        };

        // F. Write the 16 raw bytes to .idx
        idx_writer.write_all(&record.to_le_bytes())?;

        // G. Update the cursor for the next iteration
        current_offset += length as u64;
        processed += 1;
    }

    // Ensure everything buffered in RAM is flushed to disk
    json_writer.flush()?;
    idx_writer.flush()?;

    println!("Indexing complete! Processed {} records.", processed);
    Ok(())
}