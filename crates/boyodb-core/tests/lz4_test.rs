use boyodb_core::engine::decompress_payload;

#[test]
fn test_decode_lz4() {
    let data = std::fs::read("/tmp/boyodb_test_data/segments/seg-0-0.ipc").unwrap();
    let res = decompress_payload(data, Some("lz4"));
    panic!("Result: {:?}", res);
}
