//! 高级用法示例：cancel、pause/resume、pid 控制器、健康检查。
//!
//! ```bash
//! cargo run --example advanced
//! ```

use std::time::Duration;
use draft::prelude::*;

struct LoggingProcessor;

impl BatchProcessor<i32> for LoggingProcessor {
    async fn process(&self, batch: Batch<i32>) -> Vec<()> {
        println!(
            "📦 批次 #{} | {} 条 | 窗口 {:?} | 队列 {}",
            batch.batch_id(),
            batch.len(),
            batch.window_duration(),
            batch.queue_depth_at_flush(),
        );
        vec![(); batch.len()]
    }
}

#[tokio::main]
async fn main() {
    let config = AccumulatorConfig::new(
        Duration::from_millis(300),
        Duration::from_millis(50),
        Duration::from_secs(5),
    )
    .unwrap()
    .with_max_batch_size(20)
    .with_max_queue_depth(100);

    let (handle, _jh) = config.build_and_spawn(
        LoggingProcessor,
        DefaultMetrics::new(),
        PIDController::new(0.8, 0.15, 0.01, 0.05).unwrap(),
    );

    // 1. 正常提交
    println!("=== 1. 正常提交 ===");
    for i in 0..10 {
        handle.send(i).unwrap();
    }
    tokio::time::sleep(Duration::from_millis(500)).await;

    // 2. 暂停 — 只缓冲不 flush
    println!("\n=== 2. 暂停 ===");
    handle.pause();
    for i in 10..20 {
        handle.send(i).unwrap();
    }
    tokio::time::sleep(Duration::from_millis(500)).await;

    // 3. 恢复 — 触发 flush
    println!("\n=== 3. 恢复 ===");
    handle.resume();
    tokio::time::sleep(Duration::from_millis(500)).await;

    // 4. 健康检查
    println!("\n=== 4. 健康检查 ===");
    let health = handle.health();
    println!("  接受中: {}", health.is_accepting);
    println!("  队列利用率: {:.1}%", health.queue_utilization * 100.0);
    println!("  拒绝次数: {}", health.total_rejected);

    // 5. 统计快照
    let stats = handle.stats();
    println!(
        "\n  提交: {} | flush: {} | p50: {:?} | 平均队列等待: {:?}",
        stats.total_submitted, stats.total_flushed, stats.p50_latency, stats.avg_queue_wait,
    );

    // 6. 取消
    println!("\n=== 5. 取消 ===");
    handle.cancel();
    tokio::time::sleep(Duration::from_millis(200)).await;

    println!("\n✅ 完成");
}
