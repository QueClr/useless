use std::collections::BTreeSet;

use plotters::prelude::*;

use crate::stats::BenchmarkResult;

const COLORS: [RGBColor; 2] = [RGBColor(0x22, 0x8B, 0x22), RGBColor(0xDC, 0x14, 0x3C)];
const IMAGE_WIDTH: u32 = 800;
const IMAGE_HEIGHT: u32 = 500;

pub fn write_plots(output_dir: &str, results: &[BenchmarkResult]) {
    if results.is_empty() {
        return;
    }

    // Group results by workload
    let workload_names: BTreeSet<&str> = results.iter().map(|r| r.workload.as_str()).collect();

    for workload_name in &workload_names {
        let workload_results: Vec<&BenchmarkResult> =
            results.iter().filter(|r| r.workload == *workload_name).collect();

        if let Err(e) = plot_throughput(output_dir, workload_name, &workload_results) {
            eprintln!("Failed to plot throughput for {}: {}", workload_name, e);
        }
        if let Err(e) = plot_latency(output_dir, workload_name, &workload_results) {
            eprintln!("Failed to plot latency for {}: {}", workload_name, e);
        }
    }
}

fn plot_throughput(
    output_dir: &str,
    workload: &str,
    results: &[&BenchmarkResult],
) -> Result<(), Box<dyn std::error::Error>> {
    let path = format!("{}/{}_throughput.png", output_dir, workload);
    let root = BitMapBackend::new(&path, (IMAGE_WIDTH, IMAGE_HEIGHT)).into_drawing_area();
    root.fill(&WHITE)?;

    // Collect series data per lock type
    let lock_names: BTreeSet<&str> = results.iter().map(|r| r.lock.as_str()).collect();
    let thread_counts: BTreeSet<usize> = results.iter().map(|r| r.thread_count).collect();
    let tc_vec: Vec<usize> = thread_counts.iter().copied().collect();

    let max_throughput = results
        .iter()
        .map(|r| r.throughput.mean_ops_sec)
        .fold(0.0f64, f64::max);

    let mut chart = ChartBuilder::on(&root)
        .caption(
            format!("{} — Throughput", workload),
            ("sans-serif", 22),
        )
        .margin(15)
        .x_label_area_size(40)
        .y_label_area_size(70)
        .build_cartesian_2d(
            *tc_vec.first().unwrap() as f64..*tc_vec.last().unwrap() as f64,
            0.0..max_throughput * 1.1,
        )?;

    chart
        .configure_mesh()
        .x_desc("Thread count")
        .y_desc("ops/sec")
        .draw()?;

    for (i, lock_name) in lock_names.iter().enumerate() {
        let color = COLORS[i % COLORS.len()];
        let data: Vec<(f64, f64)> = results
            .iter()
            .filter(|r| r.lock == *lock_name)
            .map(|r| (r.thread_count as f64, r.throughput.mean_ops_sec))
            .collect();

        chart
            .draw_series(LineSeries::new(data.clone(), color.stroke_width(2)))?
            .label(*lock_name)
            .legend(move |(x, y)| PathElement::new(vec![(x, y), (x + 20, y)], color.stroke_width(2)));

        chart.draw_series(data.iter().map(|&(x, y)| Circle::new((x, y), 4, color.filled())))?;
    }

    chart.configure_series_labels().border_style(BLACK).draw()?;
    root.present()?;
    println!("  Plot: {}", path);
    Ok(())
}

fn plot_latency(
    output_dir: &str,
    workload: &str,
    results: &[&BenchmarkResult],
) -> Result<(), Box<dyn std::error::Error>> {
    let path = format!("{}/{}_latency.png", output_dir, workload);
    let root = BitMapBackend::new(&path, (IMAGE_WIDTH, IMAGE_HEIGHT)).into_drawing_area();
    root.fill(&WHITE)?;

    let lock_names: BTreeSet<&str> = results.iter().map(|r| r.lock.as_str()).collect();
    let thread_counts: BTreeSet<usize> = results.iter().map(|r| r.thread_count).collect();
    let tc_vec: Vec<usize> = thread_counts.iter().copied().collect();

    let max_latency = results
        .iter()
        .map(|r| r.latency_ns.mean)
        .fold(0.0f64, f64::max);

    let mut chart = ChartBuilder::on(&root)
        .caption(format!("{} — Mean Latency", workload), ("sans-serif", 22))
        .margin(15)
        .x_label_area_size(40)
        .y_label_area_size(70)
        .build_cartesian_2d(
            *tc_vec.first().unwrap() as f64..*tc_vec.last().unwrap() as f64,
            0.0..max_latency * 1.1,
        )?;

    chart
        .configure_mesh()
        .x_desc("Thread count")
        .y_desc("Latency (ns)")
        .draw()?;

    for (i, lock_name) in lock_names.iter().enumerate() {
        let color = COLORS[i % COLORS.len()];
        let data: Vec<(f64, f64)> = results
            .iter()
            .filter(|r| r.lock == *lock_name)
            .map(|r| (r.thread_count as f64, r.latency_ns.mean))
            .collect();

        chart
            .draw_series(LineSeries::new(data.clone(), color.stroke_width(2)))?
            .label(*lock_name)
            .legend(move |(x, y)| PathElement::new(vec![(x, y), (x + 20, y)], color.stroke_width(2)));

        chart.draw_series(data.iter().map(|&(x, y)| Circle::new((x, y), 4, color.filled())))?;
    }

    chart.configure_series_labels().border_style(BLACK).draw()?;
    root.present()?;
    println!("  Plot: {}", path);
    Ok(())
}
