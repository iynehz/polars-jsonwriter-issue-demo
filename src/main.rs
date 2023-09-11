use std::fs;
use std::io::Cursor;
use polars::prelude::{col, IntoLazy, JsonFormat, JsonWriter, SerWriter, SerReader, ParquetReader};

fn main() {
    let buf = fs::read("demo.parquet").expect("Unable to read file");
    let reader = Cursor::new(buf);
    let frame = ParquetReader::new(reader).finish().unwrap();
    println!("Raw frame shape: {:?}", frame.shape());

    let filtered = frame
        .lazy()
        // with _pir_index == 9964 you can reproduce the issue
        // with _pir_index == 1 you see JsonWriter behaves good although schema is same.
        .filter(col("_pir_index").eq(9964 as u32))
        .collect().unwrap();
    let mut selected = filtered.select(&["TEST_NUM"]).unwrap();
    println!("Below we will print the filtered frame, and try to json serialize it. And you will see the filtered frame is non-empty but JsonWriter results empty \"[]\".");
    println!("{:?}", &selected);

    let mut buf = Vec::new();
    JsonWriter::new(&mut buf)
        .with_json_format(JsonFormat::Json)
        .finish(&mut selected).unwrap();
    let json_string = String::from_utf8_lossy(&buf).to_string();
    println!("{}", json_string);
}
