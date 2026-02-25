mod harness;
mod output;
mod schedule;
mod stats;
mod workloads;

use clap::Parser;

#[derive(Parser)]
#[command(name = "lamlock-bench", about = "Benchmark lamlock vs std::sync::Mutex")]
pub struct Cli {
    /// Comma-separated thread counts to benchmark
    #[arg(short = 't', long, default_value = "1,2,4,8")]
    threads: String,

    /// Number of operations per thread
    #[arg(short = 'n', long, default_value_t = 50000)]
    ops: usize,

    /// Number of measurement iterations
    #[arg(short = 'i', long, default_value_t = 3)]
    iterations: usize,

    /// Warmup iterations before measurement
    #[arg(long, default_value_t = 1)]
    warmup: usize,

    /// Comma-separated workload names (or "all")
    #[arg(short = 'w', long, default_value = "all")]
    workloads: String,

    /// Output directory for results
    #[arg(short = 'o', long, default_value = "results")]
    output: String,
}

impl Cli {
    pub fn thread_counts(&self) -> Vec<usize> {
        self.threads
            .split(',')
            .map(|s| s.trim().parse::<usize>().expect("invalid thread count"))
            .collect()
    }

    pub fn workload_names(&self) -> Vec<String> {
        if self.workloads == "all" {
            workloads::all_workload_names()
                .into_iter()
                .map(String::from)
                .collect()
        } else {
            self.workloads
                .split(',')
                .map(|s| s.trim().to_string())
                .collect()
        }
    }
}

fn main() {
    let cli = Cli::parse();
    let thread_counts = cli.thread_counts();
    let workload_names = cli.workload_names();

    println!(
        "lamlock-bench: threads={:?}, ops={}, iterations={}, warmup={}, workloads={:?}",
        thread_counts, cli.ops, cli.iterations, cli.warmup, workload_names
    );

    let all_results = workloads::run_all(
        &workload_names,
        &thread_counts,
        cli.ops,
        cli.iterations,
        cli.warmup,
    );

    output::write_results(&cli.output, &all_results);
}
