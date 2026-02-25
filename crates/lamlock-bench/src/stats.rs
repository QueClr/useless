use serde::Serialize;

#[derive(Debug, Clone, Serialize, Default)]
pub struct LatencyStats {
    pub mean: f64,
    pub median: f64,
    pub stddev: f64,
    pub p99: f64,
    pub min: f64,
    pub max: f64,
}

impl LatencyStats {
    pub fn from_samples(samples: &mut Vec<f64>) -> Self {
        if samples.is_empty() {
            return Self::default();
        }
        samples.sort_unstable_by(|a, b| a.partial_cmp(b).unwrap());
        let n = samples.len();
        let mean = samples.iter().sum::<f64>() / n as f64;
        let variance = samples.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / n as f64;
        let stddev = variance.sqrt();
        let median = if n % 2 == 0 {
            (samples[n / 2 - 1] + samples[n / 2]) / 2.0
        } else {
            samples[n / 2]
        };
        let p99 = samples[(n as f64 * 0.99) as usize];
        let min = samples[0];
        let max = samples[n - 1];
        Self {
            mean,
            median,
            stddev,
            p99,
            min,
            max,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct ThroughputStats {
    pub mean_ops_sec: f64,
    pub stddev_ops_sec: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct WallTimeStats {
    pub mean: f64,
    pub stddev: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct BenchmarkResult {
    pub workload: String,
    pub lock: String,
    pub thread_count: usize,
    pub total_ops: usize,
    pub throughput: ThroughputStats,
    pub latency_ns: LatencyStats,
    pub wall_time_ms: WallTimeStats,
}

impl BenchmarkResult {
    pub fn from_iterations(
        workload: &str,
        lock: &str,
        thread_count: usize,
        ops_per_thread: usize,
        wall_times: &[f64],
        throughputs: &[f64],
        latencies: &[LatencyStats],
    ) -> Self {
        let total_ops = thread_count * ops_per_thread;

        let wt_mean = wall_times.iter().sum::<f64>() / wall_times.len() as f64;
        let wt_stddev = (wall_times
            .iter()
            .map(|x| (x - wt_mean).powi(2))
            .sum::<f64>()
            / wall_times.len() as f64)
            .sqrt();

        let tp_mean = throughputs.iter().sum::<f64>() / throughputs.len() as f64;
        let tp_stddev = (throughputs
            .iter()
            .map(|x| (x - tp_mean).powi(2))
            .sum::<f64>()
            / throughputs.len() as f64)
            .sqrt();

        // Average the latency stats across iterations
        let n = latencies.len() as f64;
        let lat = LatencyStats {
            mean: latencies.iter().map(|l| l.mean).sum::<f64>() / n,
            median: latencies.iter().map(|l| l.median).sum::<f64>() / n,
            stddev: latencies.iter().map(|l| l.stddev).sum::<f64>() / n,
            p99: latencies.iter().map(|l| l.p99).sum::<f64>() / n,
            min: latencies.iter().map(|l| l.min).fold(f64::INFINITY, f64::min),
            max: latencies.iter().map(|l| l.max).fold(0.0f64, f64::max),
        };

        Self {
            workload: workload.to_string(),
            lock: lock.to_string(),
            thread_count,
            total_ops,
            throughput: ThroughputStats {
                mean_ops_sec: tp_mean,
                stddev_ops_sec: tp_stddev,
            },
            latency_ns: lat,
            wall_time_ms: WallTimeStats {
                mean: wt_mean,
                stddev: wt_stddev,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_latency_stats_basic() {
        let mut samples: Vec<f64> = (1..=100).map(|x| x as f64).collect();
        let stats = LatencyStats::from_samples(&mut samples);
        assert!((stats.mean - 50.5).abs() < 0.01);
        assert!((stats.median - 50.5).abs() < 0.01);
        assert!((stats.min - 1.0).abs() < 0.01);
        assert!((stats.max - 100.0).abs() < 0.01);
        assert!(stats.p99 >= 99.0);
    }

    #[test]
    fn test_latency_stats_empty() {
        let mut samples: Vec<f64> = Vec::new();
        let stats = LatencyStats::from_samples(&mut samples);
        assert_eq!(stats.mean, 0.0);
    }
}
