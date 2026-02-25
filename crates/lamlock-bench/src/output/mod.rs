pub mod json;
pub mod plot;

use crate::stats::BenchmarkResult;

pub fn write_results(output_dir: &str, results: &[BenchmarkResult]) {
    std::fs::create_dir_all(output_dir).expect("failed to create output directory");
    json::write_json(output_dir, results);
    plot::write_plots(output_dir, results);
}
