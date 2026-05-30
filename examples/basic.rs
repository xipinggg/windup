//! 自适应批处理累加器示例。
//!
//! ```bash
//! cargo run --example basic
//! ```

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use draft::prelude::*;

/// 带结果返回的批处理器：将接收到的字符串大写后返回。
struct UpperProcessor {
    total: AtomicUsize,
}

impl BatchProcessor<String, String> for UpperProcessor {
    async fn process(&self, batch: Batch<String>) -> Vec<String> {
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
        tokio::time::sleep(Duration::from_millis(n as u64 * 5)).await;
        // 返回处理结果
        batch
            .into_inner()
            .into_iter()
            .map(|s| s.to_uppercase())
            .collect()
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
    .with_max_batch_size(20)
    .with_concurrency_mode(ConcurrencyMode::Concurrent { max_inflight: 4 });

    let (handle, accumulator) = config.build(
        UpperProcessor {
            total: AtomicUsize::new(0),
        },
        DefaultMetrics::new(),
        AdaptiveController::new(0.8, 0.1).unwrap(),
    );

    let _jh = tokio::spawn(accumulator.run());

    // 场景1: reply — 提交并等待结果
    println!("=== 场景1: submit_with_reply ===");
    let reply = handle
        .submit_with_reply("hello-world".into())
        .unwrap();
    let result = reply.await.unwrap();
    println!("  结果: {result}");

    // 场景2: fire-and-forget + reply 混合
    println!("\n=== 场景2: fire-and-forget + reply 混合 ===");
    for i in 1..=5 {
        handle.submit(format!("fire-{i}")).unwrap();
    }
    let reply2 = handle
        .submit_with_reply("mixed-reply".into())
        .unwrap();
    println!("  结果: {}", reply2.await.unwrap());

    // 场景3: bypass（不支持 reply）
    println!("\n=== 场景3: bypass ===");
    handle.bypass("🚀 urgent-bypass".into()).unwrap();

    // 场景4: 多生产者并发 reply
    println!("\n=== 场景4: 多生产者并发 reply ===");
    let mut handles = vec![];
    for i in 0..5 {
        let h = handle.clone();
        handles.push(tokio::spawn(async move {
            let reply = h
                .submit_with_reply(format!("task-{i}"))
                .unwrap();
            let result = reply.await.unwrap();
            println!("  task-{i} 结果: {result}");
        }));
    }
    for h in handles {
        h.await.unwrap();
    }

    tokio::time::sleep(Duration::from_millis(500)).await;

    println!("\n=== 关闭 ===");
    drop(handle);
    tokio::time::sleep(Duration::from_millis(500)).await;

    println!("\n✅ 所有批次处理完成");
}
