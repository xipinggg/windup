use std::time::Duration;
use draft::prelude::*;
use tracing::Level;
use tracing_subscriber::fmt;

struct DemoProcessor;

impl BatchProcessor<i32, String> for DemoProcessor {
    async fn process(&self, batch: Batch<i32>) -> Vec<String> {
        tokio::time::sleep(Duration::from_millis(5)).await;
        batch.items().iter().map(|i| format!("result-{i}")).collect()
    }
}

#[tokio::main]
async fn main() {
    // 初始化 subscriber，输出格式化的日志
    fmt()
        .with_max_level(Level::DEBUG)
        .with_target(false)
        .init();

    let config = AccumulatorConfig::new(
        Duration::from_millis(200),
        Duration::from_millis(50),
        Duration::from_secs(5),
    )
    .unwrap()
    .with_max_batch_size(10)
    .with_trace_per_item(true);

    let (handle, accumulator) = config.build(
        DemoProcessor,
        DefaultMetrics::new(),
        AdaptiveController::new(0.8, 0.1).unwrap(),
    );

    let join = tokio::spawn(accumulator.run());

    // 提交一些 item
    for i in 0..5 {
        let reply = handle.submit(i).unwrap();
        let result = reply.await.unwrap();
        println!("Got: {result}");
    }

    handle.flush_now();
    tokio::time::sleep(Duration::from_millis(500)).await;

    drop(handle);
    let _ = join.await;
}
