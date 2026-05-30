//! 自适应批处理累加器示例。
//!
//! ```bash
//! cargo run --example basic
//! ```

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use draft::prelude::*;

/// 模拟批处理服务。
struct PrintProcessor {
    total: AtomicUsize,
}

impl BatchProcessor<String> for PrintProcessor {
    async fn process(&self, batch: Batch<String>) {
        let n = batch.len();
        let count = self.total.fetch_add(n, Ordering::Relaxed) + n;
        println!(
            "📦 批次 #{:<3} | {} 条 | 等待 {:>6.1?} | 累计 {}",
            batch.batch_id(),
            n,
            batch.age(),
            count,
        );
        // 模拟下游处理耗时
        tokio::time::sleep(Duration::from_millis(n as u64 * 2)).await;
    }
}

#[tokio::main]
async fn main() {
    let config = AccumulatorConfig::new(
        Duration::from_millis(300),
        Duration::from_millis(50),
        Duration::from_secs(3),
    )
    .unwrap()
    .with_max_batch_size(20);

    let (handle, accumulator) = config.build(
        PrintProcessor {
            total: AtomicUsize::new(0),
        },
        DefaultMetrics::new(),
        AdaptiveController::new(0.8, 0.1).unwrap(),
    );

    let _jh = tokio::spawn(accumulator.run());

    // 场景1: 零星提交
    println!("=== 场景1: 零星提交 ===");
    for i in 1..=5 {
        handle.submit(format!("item-{i}")).unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    tokio::time::sleep(Duration::from_millis(500)).await;

    // 场景2: 突发流量
    println!("\n=== 场景2: 突发流量 ===");
    for i in 1..=25 {
        handle.submit(format!("burst-{i}")).unwrap();
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    tokio::time::sleep(Duration::from_millis(100)).await;

    // 场景3: bypass
    println!("\n=== 场景3: bypass ===");
    handle.bypass("🚀 urgent-bypass".into()).unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // 场景4: 多生产者
    println!("\n=== 场景4: 多生产者并发 ===");
    let h2 = handle.clone();
    let h3 = handle.clone();
    let t1 = tokio::spawn(async move {
        for i in 0..30 {
            h2.submit(format!("A-{i}")).unwrap();
        }
    });
    let t2 = tokio::spawn(async move {
        for i in 0..30 {
            h3.submit(format!("B-{i}")).unwrap();
        }
    });
    t1.await.unwrap();
    t2.await.unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;

    println!("\n=== 关闭 ===");
    drop(handle);
    tokio::time::sleep(Duration::from_millis(500)).await;

    println!("\n✅ 所有批次处理完成");
}
