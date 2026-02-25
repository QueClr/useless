use std::fs;
use std::io::Write;

use serde::Serialize;

use crate::stats::BenchmarkResult;

#[derive(Serialize)]
struct BenchOutput<'a> {
    metadata: Metadata,
    benchmarks: &'a [BenchmarkResult],
}

#[derive(Serialize)]
struct Metadata {
    timestamp: String,
    cpu_count: usize,
    rustc_version: String,
}

pub fn write_json(output_dir: &str, results: &[BenchmarkResult]) {
    let timestamp = chrono::Local::now().format("%Y-%m-%dT%H:%M:%S").to_string();
    let cpu_count = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    let rustc_version = rustc_version();

    let output = BenchOutput {
        metadata: Metadata {
            timestamp: timestamp.clone(),
            cpu_count,
            rustc_version,
        },
        benchmarks: results,
    };

    let json = serde_json::to_string_pretty(&output).expect("failed to serialize JSON");

    let filename = format!("{}/bench-{}.json", output_dir, timestamp.replace(':', "-"));
    let mut file = fs::File::create(&filename).expect("failed to create JSON file");
    file.write_all(json.as_bytes())
        .expect("failed to write JSON");
    println!("Results written to {}", filename);
}

fn rustc_version() -> String {
    std::process::Command::new("rustc")
        .arg("--version")
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .unwrap_or_else(|| "unknown".to_string())
        .trim()
        .to_string()
}
