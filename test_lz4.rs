fn main() {
    let data = std::fs::read("/tmp/boyodb_test_data/segments/seg-0-0.ipc").unwrap();
    match lz4_flex::decompress_size_prepended(&data) {
        Ok(decompressed) => println!("Success! Original size: {}, Decompressed: {}", data.len(), decompressed.len()),
        Err(e) => println!("Failed: {:?}", e),
    }
}
