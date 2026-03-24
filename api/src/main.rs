use axum::{
    extract::{Path, State},
    http::{header, StatusCode},
    response::IntoResponse,
    routing::get,
    Router,
};
use rmb_core::IndexReader;
use memmap2::Mmap;
use std::{fs::File, sync::Arc};
use tokio::net::TcpListener;

// --- 1. SHARED GLOBAL STATE ---

/// This structure lives in RAM and is shared between all requests (threads)
/// arriving at the server simultaneously.
struct AppState {
    // Our binary search engine (O(log N) search)
    index: IndexReader,
    // The full text file mapped directly into RAM
    jsonl_mmap: Mmap,
}

// --- 2. ENTRY POINT (MAIN) ---

#[tokio::main]
async fn main() {
    println!("Starting RMB emergency engine...");

    // 1. Load the binary index using our `core` library
    let index = IndexReader::new("main.idx")
        .expect("CRITICAL: Could not load main.idx");

    // 2. Map the perfect JSONL into RAM using the OS
    let jsonl_file = File::open("perfect.jsonl")
        .expect("CRITICAL: perfect.jsonl not found");
    
    // We commit that nobody will modify the JSONL while the server is running
    let jsonl_mmap = unsafe { 
        memmap2::MmapOptions::new().map(&jsonl_file)
            .expect("CRITICAL: JSONL mmap failed") 
    };

    // 3. Wrap state in Arc (atomic reference count)
    // This allows multiple web server threads to read the same memory region
    // simultaneously without cloning gigabytes of data.
    let shared_state = Arc::new(AppState { index, jsonl_mmap });

    // 4. Set up the API router
    let app = Router::new()
        // When someone visits /leyes/23737, run the `get_ley` handler
        .route("/leyes/{id}", get(get_ley))
        // Inject shared state into all routes
        .with_state(shared_state);

    // 5. Run the server on port 3000
    let listener = TcpListener::bind("0.0.0.0:3000").await.unwrap();
    println!("RMB Fallback server is ACTIVE at http://localhost:3000");
    
    axum::serve(listener, app).await.unwrap();
}

// --- 3. ENDPOINT (O(1) MAGIC) ---

/// Controller for `GET /leyes/:id`
async fn get_ley(
    // Extract ID from URL
    Path(id): Path<u32>,
    // Extract global state (mmaps) from memory
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    
    // STEP 1: Microsecond Binary Search in .idx
    if let Some(record) = state.index.find_by_id(id) {
        
        // STEP 2: Pointer math
        // We know exactly where the record starts and ends in RAM
        let start = record.offset as usize;
        let end = start + record.length as usize;

        // STEP 3: Byte slicing
        // Extract only that slice from gigabytes of memory.
        // NOTE: We are not parsing JSON. These are raw bytes.
        let chunk = &state.jsonl_mmap[start..end];

        // STEP 4: Return bytes directly to the network stack,
        // wrapped as JSON so the client can read it.
        (
            StatusCode::OK,
            [(header::CONTENT_TYPE, "application/json")],
            chunk.to_vec(), // Copy the slice to the HTTP output buffer
        )

    } else {
        // If the ID does not exist in the binary index
        (
            StatusCode::NOT_FOUND,
            [(header::CONTENT_TYPE, "application/json")],
            r#"{"error": "Record not found in backup index"}"#.as_bytes().to_vec(),
        )
    }
}