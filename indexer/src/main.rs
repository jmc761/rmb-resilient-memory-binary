use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use serde::{Deserialize, Serialize};
use rmb_core::IndexRecord; 

// --- 1. INPUT SCHEMA (Postgres source rows) ---
// Field names match the JSONL keys exactly.
#[derive(Deserialize, Debug)]
struct PostgresRow {
    id: u32,
    tipo_norma: Option<String>,
    numero_norma: Option<u32>, // Option because older norms may lack a number
    organismo_origen: Option<String>,
    fecha_sancion: Option<String>,
    titulo_resumido: Option<String>,
    texto_resumido: Option<String>,
}

// --- 2. OUTPUT SCHEMA (API "Perfect" JSON) ---
// We shape it into a nested, production-ready structure
#[derive(Serialize)]
struct ApiResponse {
    id: u32,
    identification: Identification,
    metadata: Metadata,
    content: Content,
}

#[derive(Serialize)]
struct Identification {
    tipo: String,
    numero: u32,
}

#[derive(Serialize)]
struct Metadata {
    fecha: String,
    organismo: String,
    sistema_contingencia: bool,
}

#[derive(Serialize)]
struct Content {
    titulo: String,
    resumen: String,
}

// --- 3. MAIN LOGIC ---

fn main() -> std::io::Result<()> {
    println!("Starting indexer...");

    let input_file = File::open("input.jsonl").expect("Missing input.jsonl file");
    let reader = BufReader::new(input_file);

    let output_jsonl = File::create("perfect.jsonl")?;
    let mut json_writer = BufWriter::new(output_jsonl);

    let output_idx = File::create("main.idx")?;
    let mut idx_writer = BufWriter::new(output_idx);

    let mut current_offset: u64 = 0;
    let mut processed = 0;

    for (line_num, line_result) in reader.lines().enumerate() {
        let line = line_result?;
        if line.trim().is_empty() { continue; }

        // A. Parse the raw line
        let row: PostgresRow = match serde_json::from_str(&line) {
            Ok(data) => data,
            Err(e) => {
                eprintln!("Error parsing line {}: {}", line_num + 1, e);
                continue; // If a line is broken, skip it so processing can continue
            }
        };

        // B. Defensively map data (handle nulls)
        let api_doc = ApiResponse {
            id: row.id,
            identification: Identification {
                // If missing, provide a default value
                tipo: row.tipo_norma.unwrap_or_else(|| "Unknown".to_string()),
                numero: row.numero_norma.unwrap_or(0),
            },
            metadata: Metadata {
                fecha: row.fecha_sancion.unwrap_or_else(|| "No date".to_string()),
                organismo: row.organismo_origen.unwrap_or_else(|| "Not specified".to_string()),
                sistema_contingencia: true, // Add a watermark flag from our system
            },
            content: Content {
                titulo: row.titulo_resumido.unwrap_or_default(),
                resumen: row.texto_resumido.unwrap_or_default(),
            },
        };

        // C. Serialize and compute offsets (same exact algorithm)
        let mut nueva_linea = serde_json::to_string(&api_doc)?;
        nueva_linea.push('\n'); 

        let bytes_linea = nueva_linea.as_bytes();
        let length = bytes_linea.len() as u32;

        json_writer.write_all(bytes_linea)?;

        let record = IndexRecord {
            id: api_doc.id,
            offset: current_offset,
            length,
        };

        idx_writer.write_all(&record.to_le_bytes())?;

        current_offset += length as u64;
        processed += 1;
    }

    json_writer.flush()?;
    idx_writer.flush()?;

    println!("Indexing complete! Processed {} records.", processed);
    Ok(())
}