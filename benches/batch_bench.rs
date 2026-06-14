//! 批处理累加器基准测试。
//!
//! 运行方式：`cargo bench`

use std::time::Duration;

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use windup::prelude::*;

/// 轻量级处理器用于基准测试。
struct NoopProcessor;

impl BatchProcessor<i32> for NoopProcessor {
    async fn process(&self, batch: Batch<i32>) -> Vec<()> {
        vec![(); batch.len()]
    }
}

/// 带模拟延迟的处理器。
struct SimulatedDelayProcessor;

impl BatchProcessor<i32> for SimulatedDelayProcessor {
    async fn process(&self, batch: Batch<i32>) -> Vec<()> {
        tokio::time::sleep(Duration::from_millis(5)).await;
        vec![(); batch.len()]
    }
}

/// 辅助函数：在 tokio runtime 中运行基准测试的异步逻辑。
fn run_bench<F>(rt: &tokio::runtime::Runtime, iters: u64, f: F) -> Duration
where
    F: FnOnce(u64) -> Duration,
{
    rt.block_on(async move { f(iters) })
}

/// 串行模式吞吐量基准：持续提交 item，测量处理耗时。
fn bench_throughput_serial(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    c.bench_function("throughput_serial", |b| {
        b.iter_custom(|iters| {
            run_bench(&rt, iters, |n| {
                let config = AccumulatorConfig::new(
                    Duration::from_millis(200),
                    Duration::from_millis(50),
                    Duration::from_secs(5),
                )
                .unwrap()
                .with_max_batch_size(128);

                let (handle, accumulator) = config
                    .build(
                        NoopProcessor,
                        DefaultMetrics::new(),
                        FixedController::new(Duration::from_millis(200)),
                    )
                    .unwrap();

                let join = tokio::spawn(accumulator.run());

                let start = std::time::Instant::now();
                for i in 0..n as i32 {
                    let _ = handle.submit(black_box(i));
                }
                drop(handle);
                let _ = rt.block_on(join);
                start.elapsed()
            })
        });
    });
}

/// 大批量吞吐量基准。
fn bench_large_batch(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    c.bench_function("throughput_large_batch", |b| {
        b.iter_custom(|iters| {
            run_bench(&rt, iters, |n| {
                let config = AccumulatorConfig::new(
                    Duration::from_millis(500),
                    Duration::from_millis(100),
                    Duration::from_secs(10),
                )
                .unwrap()
                .with_max_batch_size(10000);

                let (handle, accumulator) = config
                    .build(
                        NoopProcessor,
                        DefaultMetrics::new(),
                        FixedController::new(Duration::from_millis(500)),
                    )
                    .unwrap();

                let join = tokio::spawn(accumulator.run());

                let start = std::time::Instant::now();
                for i in 0..n as i32 {
                    let _ = handle.submit(black_box(i));
                }
                drop(handle);
                let _ = rt.block_on(join);
                start.elapsed()
            })
        });
    });
}

/// 串行 vs 并发模式对比（带模拟延迟的处理器）。
fn bench_serial_vs_concurrent(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("serial_vs_concurrent");

    group.bench_function("serial", |b| {
        b.iter_custom(|iters| {
            run_bench(&rt, iters, |n| {
                let config = AccumulatorConfig::new(
                    Duration::from_millis(200),
                    Duration::from_millis(50),
                    Duration::from_secs(5),
                )
                .unwrap()
                .with_concurrency_mode(ConcurrencyMode::Serial);

                let (handle, accumulator) = config
                    .build(
                        SimulatedDelayProcessor,
                        DefaultMetrics::new(),
                        FixedController::new(Duration::from_millis(200)),
                    )
                    .unwrap();

                let join = tokio::spawn(accumulator.run());

                let start = std::time::Instant::now();
                for i in 0..n as i32 {
                    let _ = handle.submit(black_box(i));
                }
                drop(handle);
                let _ = rt.block_on(join);
                start.elapsed()
            })
        });
    });

    group.bench_function("concurrent_4", |b| {
        b.iter_custom(|iters| {
            run_bench(&rt, iters, |n| {
                let config = AccumulatorConfig::new(
                    Duration::from_millis(200),
                    Duration::from_millis(50),
                    Duration::from_secs(5),
                )
                .unwrap()
                .with_concurrency_mode(ConcurrencyMode::Concurrent { max_inflight: 4 });

                let (handle, accumulator) = config
                    .build(
                        SimulatedDelayProcessor,
                        DefaultMetrics::new(),
                        FixedController::new(Duration::from_millis(200)),
                    )
                    .unwrap();

                let join = tokio::spawn(accumulator.run());

                let start = std::time::Instant::now();
                for i in 0..n as i32 {
                    let _ = handle.submit(black_box(i));
                }
                drop(handle);
                let _ = rt.block_on(join);
                start.elapsed()
            })
        });
    });

    group.finish();
}

criterion_group!(benches, bench_throughput_serial, bench_large_batch, bench_serial_vs_concurrent,);
criterion_main!(benches);
