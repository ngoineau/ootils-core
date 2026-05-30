//! Rust kernel POC — head-to-head against the Python ProjectionKernel.
//!
//! Reads the same JSONL dataset as `bench_python.py`, parses inputs ONCE
//! outside the bench loop, then runs the equivalent of `compute_pi_node`
//! N times.
//!
//! We bench two variants of the kernel:
//! - `compute_decimal` — uses `rust_decimal::Decimal` (same precision class
//!   as Python's `decimal.Decimal`; apples-to-apples).
//! - `compute_f64`     — uses native `f64` (max speed, lossy precision —
//!   shown for "what we leave on the table if precision is relaxed").

use std::env;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::time::Instant;

use chrono::NaiveDate;
use rust_decimal::Decimal;
use serde::Deserialize;
use std::str::FromStr;

// -------------------------------------------------------------------- //
//  Input record schema (matches extract_data.py output)
// -------------------------------------------------------------------- //

#[derive(Deserialize)]
struct RawRecord {
    #[allow(dead_code)]
    node_id: String,
    opening_stock: String,
    bucket_start: String,
    bucket_end: String,
    supplies: Vec<(String, String)>,
    demands: Vec<(String, String)>,
}

/// Pre-parsed inputs (this conversion is NOT measured; same as Python).
struct BucketInputs<T> {
    opening_stock: T,
    bucket_start: NaiveDate,
    bucket_end: NaiveDate,
    supplies: Vec<(NaiveDate, T)>,
    demands: Vec<(NaiveDate, T)>,
}

/// Output of one bucket compute. Matches the Python dict shape.
#[derive(Debug, Clone)]
#[allow(dead_code)]
struct PiResult<T> {
    opening_stock: T,
    inflows: T,
    outflows: T,
    closing_stock: T,
    has_shortage: bool,
    shortage_qty: T,
}

// -------------------------------------------------------------------- //
//  Kernel — Decimal variant (apples-to-apples vs Python Decimal)
// -------------------------------------------------------------------- //

#[inline]
fn compute_decimal(input: &BucketInputs<Decimal>) -> PiResult<Decimal> {
    let mut inflows = Decimal::ZERO;
    for (d, q) in &input.supplies {
        if *d >= input.bucket_start && *d < input.bucket_end {
            inflows += q;
        }
    }
    let mut outflows = Decimal::ZERO;
    for (d, q) in &input.demands {
        if *d >= input.bucket_start && *d < input.bucket_end {
            outflows += q;
        }
    }
    let closing_stock = input.opening_stock + inflows - outflows;
    let has_shortage = closing_stock < Decimal::ZERO;
    let shortage_qty = if has_shortage { -closing_stock } else { Decimal::ZERO };
    PiResult {
        opening_stock: input.opening_stock,
        inflows,
        outflows,
        closing_stock,
        has_shortage,
        shortage_qty,
    }
}

// -------------------------------------------------------------------- //
//  Kernel — f64 variant (max speed, lossy precision)
// -------------------------------------------------------------------- //

#[inline]
fn compute_f64(input: &BucketInputs<f64>) -> PiResult<f64> {
    let mut inflows = 0.0_f64;
    for (d, q) in &input.supplies {
        if *d >= input.bucket_start && *d < input.bucket_end {
            inflows += q;
        }
    }
    let mut outflows = 0.0_f64;
    for (d, q) in &input.demands {
        if *d >= input.bucket_start && *d < input.bucket_end {
            outflows += q;
        }
    }
    let closing_stock = input.opening_stock + inflows - outflows;
    let has_shortage = closing_stock < 0.0;
    let shortage_qty = if has_shortage { -closing_stock } else { 0.0 };
    PiResult {
        opening_stock: input.opening_stock,
        inflows,
        outflows,
        closing_stock,
        has_shortage,
        shortage_qty,
    }
}

// -------------------------------------------------------------------- //
//  Bench harness
// -------------------------------------------------------------------- //

fn parse_date(s: &str) -> NaiveDate {
    NaiveDate::parse_from_str(s, "%Y-%m-%d").expect("invalid date")
}

fn load_records(path: &str) -> Vec<RawRecord> {
    let f = File::open(path).expect("open data file");
    BufReader::new(f)
        .lines()
        .map(|l| serde_json::from_str(&l.expect("read line")).expect("parse JSON"))
        .collect()
}

fn to_decimal_inputs(raws: &[RawRecord]) -> Vec<BucketInputs<Decimal>> {
    raws.iter()
        .map(|r| BucketInputs {
            opening_stock: Decimal::from_str(&r.opening_stock).expect("opening dec"),
            bucket_start: parse_date(&r.bucket_start),
            bucket_end: parse_date(&r.bucket_end),
            supplies: r
                .supplies
                .iter()
                .map(|(d, q)| (parse_date(d), Decimal::from_str(q).expect("sup dec")))
                .collect(),
            demands: r
                .demands
                .iter()
                .map(|(d, q)| (parse_date(d), Decimal::from_str(q).expect("dem dec")))
                .collect(),
        })
        .collect()
}

fn to_f64_inputs(raws: &[RawRecord]) -> Vec<BucketInputs<f64>> {
    raws.iter()
        .map(|r| BucketInputs {
            opening_stock: r.opening_stock.parse::<f64>().unwrap_or(0.0),
            bucket_start: parse_date(&r.bucket_start),
            bucket_end: parse_date(&r.bucket_end),
            supplies: r
                .supplies
                .iter()
                .map(|(d, q)| (parse_date(d), q.parse::<f64>().unwrap_or(0.0)))
                .collect(),
            demands: r
                .demands
                .iter()
                .map(|(d, q)| (parse_date(d), q.parse::<f64>().unwrap_or(0.0)))
                .collect(),
        })
        .collect()
}

fn bench<T, F>(label: &str, inputs: &[BucketInputs<T>], iterations: usize, kernel: F)
where
    F: Fn(&BucketInputs<T>) -> PiResult<T>,
{
    let n = inputs.len();
    let mut per_call_ns: Vec<u128> = Vec::with_capacity(n * iterations);

    // Warm-up pass (not measured)
    for input in inputs {
        std::hint::black_box(kernel(input));
    }

    let t0 = Instant::now();
    for _ in 0..iterations {
        for input in inputs {
            let tc0 = Instant::now();
            let r = kernel(input);
            std::hint::black_box(&r);
            per_call_ns.push(tc0.elapsed().as_nanos());
        }
    }
    let total_ns = t0.elapsed().as_nanos();

    let total_s = total_ns as f64 / 1e9;
    let total_calls = (n * iterations) as f64;
    let throughput = total_calls / total_s;

    per_call_ns.sort_unstable();
    let p50_us = per_call_ns[per_call_ns.len() / 2] as f64 / 1000.0;
    let p95_us = per_call_ns[(per_call_ns.len() as f64 * 0.95) as usize] as f64 / 1000.0;
    let mean_us = per_call_ns.iter().sum::<u128>() as f64 / per_call_ns.len() as f64 / 1000.0;

    println!();
    println!("=== Rust kernel bench [{label}] ===");
    println!("  Buckets         : {n}");
    println!("  Iterations      : {iterations}");
    println!("  Total calls     : {total_calls:.0}");
    println!("  Total time      : {total_s:.3} s");
    println!("  Throughput      : {throughput:.0} PI/s");
    println!();
    println!(
        "  Latency / call  : p50={p50_us:.3} µs  mean={mean_us:.3} µs  p95={p95_us:.3} µs"
    );
}

fn main() {
    let args: Vec<String> = env::args().collect();
    let data_path = args
        .get(1)
        .cloned()
        .unwrap_or_else(|| "data/buckets.jsonl".to_string());
    let iterations: usize = args
        .get(2)
        .and_then(|s| s.parse().ok())
        .unwrap_or(1);

    println!("Loading {}...", data_path);
    let raws = load_records(&data_path);
    let n = raws.len();
    let total_sup: usize = raws.iter().map(|r| r.supplies.len()).sum();
    let total_dem: usize = raws.iter().map(|r| r.demands.len()).sum();
    println!("  → {n} buckets, {total_sup} supplies, {total_dem} demands");

    println!("Pre-parsing inputs (decimal + f64)...");
    let dec_inputs = to_decimal_inputs(&raws);
    let f64_inputs = to_f64_inputs(&raws);

    bench("rust_decimal", &dec_inputs, iterations, compute_decimal);
    bench("f64", &f64_inputs, iterations, compute_f64);
}
