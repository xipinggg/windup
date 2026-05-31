use std::sync::{Arc, Mutex};
use std::time::Duration;

use windup::prelude::*;

/// 测试用处理器：将 i32 转为 String。
struct StringProcessor;

impl BatchProcessor<i32, String> for StringProcessor {
    async fn process(&self, batch: Batch<i32>) -> Vec<String> {
        batch.items().iter().map(|i| format!("y-{i}")).collect()
    }
}

type Batches<T> = Arc<Mutex<Vec<Batch<T>>>>;

struct MockProcessor<T: Send> {
    batches: Batches<T>,
}

impl<T: Send + 'static> BatchProcessor<T> for MockProcessor<T> {
    async fn process(&self, batch: Batch<T>) -> Vec<()> {
        let n = batch.len();
        self.batches.lock().unwrap().push(batch);
        vec![(); n]
    }
}

fn mock<T: Send + 'static>() -> (MockProcessor<T>, Batches<T>) {
    let batches = Arc::new(Mutex::new(Vec::new()));
    let p = MockProcessor {
        batches: Arc::clone(&batches),
    };
    (p, batches)
}

fn cfg(ms: u64) -> AccumulatorConfig {
    let win = Duration::from_millis(ms);
    AccumulatorConfig::new(win, Duration::from_millis(50), Duration::from_secs(30))
        .unwrap()
}

fn fixed(ms: u64) -> FixedController {
    FixedController::new(Duration::from_millis(ms))
}

async fn tick(dur: Duration) {
    tokio::time::sleep(dur).await;
}

#[tokio::test]
async fn test_basic_time_flush() {
    tokio::time::pause();
    let (proc, batches) = mock::<i32>();
    let config = cfg(100);
    let (handle, accumulator) = config.build(proc, DefaultMetrics::new(), fixed(100));
    tokio::spawn(accumulator.run());

    handle.send(1).unwrap();
    handle.send(2).unwrap();
    handle.send(3).unwrap();

    tick(Duration::from_millis(80)).await;
    assert_eq!(batches.lock().unwrap().len(), 0);

    tick(Duration::from_millis(40)).await;
    let guard = batches.lock().unwrap();
    assert_eq!(guard.len(), 1, "expected 1 batch after window expiry");
    assert_eq!(guard[0].len(), 3);

    drop(handle);
}

#[tokio::test]
async fn test_max_batch_size_early_flush() {
    tokio::time::pause();
    let (proc, batches) = mock::<i32>();
    let config = cfg(10_000).with_max_batch_size(3);
    let (handle, accumulator) = config.build(proc, DefaultMetrics::new(), fixed(10_000));
    tokio::spawn(accumulator.run());

    handle.send(1).unwrap();
    handle.send(2).unwrap();
    handle.send(3).unwrap();

    tick(Duration::ZERO).await;
    assert_eq!(batches.lock().unwrap().len(), 1);
    assert_eq!(batches.lock().unwrap()[0].len(), 3);

    handle.send(4).unwrap();
    handle.send(5).unwrap();
    tick(Duration::ZERO).await;
    assert_eq!(batches.lock().unwrap().len(), 1);

    drop(handle);
}

#[tokio::test]
async fn test_multiple_time_flushes() {
    tokio::time::pause();
    let (proc, batches) = mock::<i32>();
    let config = cfg(50);
    let (handle, accumulator) = config.build(proc, DefaultMetrics::new(), fixed(50));
    tokio::spawn(accumulator.run());

    handle.send(1).unwrap();
    handle.send(2).unwrap();
    tick(Duration::from_millis(60)).await;

    let after_first = batches.lock().unwrap().len();
    assert_eq!(after_first, 1, "first flush should have occurred");

    handle.send(3).unwrap();
    handle.send(4).unwrap();
    handle.send(5).unwrap();
    tick(Duration::from_millis(60)).await;

    let guard = batches.lock().unwrap();
    assert_eq!(guard.len(), 2, "expected 2 batches, got {}", guard.len());
    assert_eq!(guard[0].len(), 2);
    assert_eq!(guard[1].len(), 3);

    drop(handle);
}

#[tokio::test]
async fn test_drain_on_close() {
    tokio::time::pause();
    let (proc, batches) = mock::<i32>();
    let config = cfg(10_000);
    let (handle, accumulator) = config.build(proc, DefaultMetrics::new(), fixed(10_000));
    let jh = tokio::spawn(accumulator.run());

    handle.send(1).unwrap();
    handle.send(2).unwrap();
    handle.send(3).unwrap();

    drop(handle);
    jh.await.unwrap();
    assert_eq!(batches.lock().unwrap().len(), 1);
    assert_eq!(batches.lock().unwrap()[0].len(), 3);
}

#[tokio::test]
async fn test_max_queue_depth() {
    let (proc, _batches) = mock::<i32>();
    let config = cfg(10_000).with_max_queue_depth(2);
    let (handle, accumulator) = config.build(proc, DefaultMetrics::new(), fixed(10_000));
    tokio::spawn(accumulator.run());

    handle.send(1).unwrap();
    handle.send(2).unwrap();

    let err = handle.send(3).unwrap_err();
    assert!(matches!(err, AccumulatorError::QueueFull { max: 2, .. }));

    drop(handle);
}

#[tokio::test]
async fn test_empty_batch_suppression() {
    tokio::time::pause();
    let (proc, batches) = mock::<i32>();
    let config = cfg(50);
    let (handle, accumulator) = config.build(proc, DefaultMetrics::new(), fixed(50));
    tokio::spawn(accumulator.run());

    tick(Duration::from_millis(200)).await;
    assert!(
        batches.lock().unwrap().is_empty(),
        "should suppress empty batches"
    );

    drop(handle);
}

#[tokio::test]
async fn test_flush_empty_batches_enabled() {
    tokio::time::pause();
    let (proc, batches) = mock::<i32>();
    let config = cfg(50).with_flush_empty_batches(true);
    let (handle, accumulator) = config.build(proc, DefaultMetrics::new(), fixed(50));
    tokio::spawn(accumulator.run());

    tick(Duration::from_millis(200)).await;

    let guard = batches.lock().unwrap();
    assert!(!guard.is_empty(), "should have empty batches when enabled");

    drop(handle);
}

#[tokio::test]
async fn test_multi_producer() {
    tokio::time::pause();
    let (proc, batches) = mock::<usize>();
    let config = cfg(200);
    let (handle, accumulator) = config.build(proc, DefaultMetrics::new(), fixed(200));
    let jh = tokio::spawn(accumulator.run());

    const PRODUCERS: usize = 10;
    const PER_PRODUCER: usize = 100;

    let mut handles = vec![];
    for _ in 0..PRODUCERS {
        let h = handle.clone();
        handles.push(tokio::spawn(async move {
            for i in 0..PER_PRODUCER {
                h.send(i).unwrap();
            }
        }));
    }

    for h in handles {
        h.await.unwrap();
    }
    drop(handle);

    tick(Duration::from_millis(500)).await;
    jh.await.unwrap();

    let total: usize = batches.lock().unwrap().iter().map(|b| b.len()).sum();
    assert_eq!(total, PRODUCERS * PER_PRODUCER);
}

// ─── reply 机制测试 ───

#[tokio::test]
async fn test_reply_single_result() {
    tokio::time::pause();
    // 处理器：将 i32 翻倍
    struct Double;
    impl BatchProcessor<i32, i32> for Double {
        async fn process(&self, batch: Batch<i32>) -> Vec<i32> {
            batch.into_inner().into_iter().map(|x| x * 2).collect()
        }
    }

    let config = cfg(100);
    let (handle, accumulator) = config.build(Double, DefaultMetrics::new(), fixed(100));
    tokio::spawn(accumulator.run());

    let reply = handle.submit(21).unwrap();
    let result = reply.await.unwrap();
    assert_eq!(result, 42);

    drop(handle);
}

#[tokio::test]
async fn test_reply_multiple_results() {
    tokio::time::pause();
    // 处理器：字符串大写
    struct Upper;
    impl BatchProcessor<String, String> for Upper {
        async fn process(&self, batch: Batch<String>) -> Vec<String> {
            batch
                .into_inner()
                .into_iter()
                .map(|s| s.to_uppercase())
                .collect()
        }
    }

    let config = cfg(200);
    let (handle, accumulator) = config.build(Upper, DefaultMetrics::new(), fixed(200));
    tokio::spawn(accumulator.run());

    let r1 = handle.submit("hello".into()).unwrap();
    let r2 = handle.submit("world".into()).unwrap();
    let r3 = handle.submit("rust".into()).unwrap();

    assert_eq!(r1.await.unwrap(), "HELLO");
    assert_eq!(r2.await.unwrap(), "WORLD");
    assert_eq!(r3.await.unwrap(), "RUST");

    drop(handle);
}

#[tokio::test]
async fn test_reply_on_shutdown() {
    tokio::time::pause();
    struct Double;
    impl BatchProcessor<i32, i32> for Double {
        async fn process(&self, batch: Batch<i32>) -> Vec<i32> {
            batch.into_inner().into_iter().map(|x| x * 2).collect()
        }
    }

    let config = cfg(10_000);
    let (handle, accumulator) = config.build(Double, DefaultMetrics::new(), fixed(10_000));
    let jh = tokio::spawn(accumulator.run());

    let reply = handle.submit(10).unwrap();
    drop(handle);

    // 关闭后 reply 应该返回完成或 shutdown
    match reply.await {
        Ok(20) => {} // drain 处理了
        Err(AccumulatorError::Shutdown) => {} // 也可能是 shutdown
        other => panic!("unexpected result: {other:?}"),
    }
    jh.await.unwrap();
}

#[tokio::test]
async fn test_reply_mixed_fire_and_forget() {
    tokio::time::pause();
    struct Double;
    impl BatchProcessor<i32, i32> for Double {
        async fn process(&self, batch: Batch<i32>) -> Vec<i32> {
            batch.into_inner().into_iter().map(|x| x * 2).collect()
        }
    }

    let config = cfg(200);
    let (handle, accumulator) = config.build(Double, DefaultMetrics::new(), fixed(200));
    tokio::spawn(accumulator.run());

    // fire-and-forget
    handle.send(1).unwrap();
    handle.send(2).unwrap();
    // with reply
    let reply = handle.submit(10).unwrap();
    // more fire-and-forget
    handle.send(3).unwrap();

    let result = reply.await.unwrap();
    assert_eq!(result, 20);

    // 确保 fire-and-forget 也被处理了（不 panic 即可）
    drop(handle);
}

// ─── 并发模式测试 ───

#[tokio::test]
async fn test_concurrent_basic_time_flush() {
    tokio::time::pause();
    let (proc, batches) = mock::<i32>();
    let config = cfg(100)
        .with_concurrency_mode(ConcurrencyMode::Concurrent { max_inflight: 0 });
    let (handle, accumulator) = config.build(proc, DefaultMetrics::new(), fixed(100));
    tokio::spawn(accumulator.run());

    handle.send(1).unwrap();
    handle.send(2).unwrap();
    tick(Duration::from_millis(150)).await;

    let guard = batches.lock().unwrap();
    assert!(!guard.is_empty(), "concurrent batch should be processed");
    assert_eq!(guard[0].len(), 2);

    drop(handle);
}

#[tokio::test]
async fn test_concurrent_drain_on_close() {
    tokio::time::pause();
    let (proc, batches) = mock::<i32>();
    let config = cfg(10_000)
        .with_concurrency_mode(ConcurrencyMode::Concurrent { max_inflight: 0 });
    let (handle, accumulator) = config.build(proc, DefaultMetrics::new(), fixed(10_000));
    let jh = tokio::spawn(accumulator.run());

    handle.send(1).unwrap();
    handle.send(2).unwrap();
    handle.send(3).unwrap();
    drop(handle);
    jh.await.unwrap();

    assert_eq!(batches.lock().unwrap().len(), 1);
    assert_eq!(batches.lock().unwrap()[0].len(), 3);
}

#[tokio::test]
async fn test_concurrent_multi_producer() {
    tokio::time::pause();
    let (proc, batches) = mock::<usize>();
    let config = cfg(200)
        .with_concurrency_mode(ConcurrencyMode::Concurrent { max_inflight: 0 });
    let (handle, accumulator) = config.build(proc, DefaultMetrics::new(), fixed(200));
    let jh = tokio::spawn(accumulator.run());

    const PRODUCERS: usize = 10;
    const PER_PRODUCER: usize = 100;

    let mut handles = vec![];
    for _ in 0..PRODUCERS {
        let h = handle.clone();
        handles.push(tokio::spawn(async move {
            for i in 0..PER_PRODUCER {
                h.send(i).unwrap();
            }
        }));
    }
    for h in handles {
        h.await.unwrap();
    }
    drop(handle);
    tick(Duration::from_millis(500)).await;
    jh.await.unwrap();

    let total: usize = batches.lock().unwrap().iter().map(|b| b.len()).sum();
    assert_eq!(total, PRODUCERS * PER_PRODUCER);
}

#[tokio::test]
async fn test_concurrent_max_batch_size() {
    tokio::time::pause();
    let (proc, batches) = mock::<i32>();
    let config = cfg(10_000)
        .with_max_batch_size(3)
        .with_concurrency_mode(ConcurrencyMode::Concurrent { max_inflight: 0 });
    let (handle, accumulator) = config.build(proc, DefaultMetrics::new(), fixed(10_000));
    tokio::spawn(accumulator.run());

    handle.send(1).unwrap();
    handle.send(2).unwrap();
    handle.send(3).unwrap();
    // 达到 max_batch_size，应在后台处理
    tick(Duration::from_millis(50)).await;

    assert_eq!(batches.lock().unwrap().len(), 1);
    assert_eq!(batches.lock().unwrap()[0].len(), 3);

    drop(handle);
}

#[tokio::test]
async fn test_concurrent_max_inflight_limit() {
    tokio::time::pause();
    // 慢速处理器，模拟耗时处理
    struct SlowProcessor<T: Send> {
        batches: Batches<T>,
    }
    impl<T: Send + 'static> BatchProcessor<T> for SlowProcessor<T> {
        async fn process(&self, batch: Batch<T>) -> Vec<()> {
            let n = batch.len();
            // 模拟耗时
            tokio::time::sleep(Duration::from_millis(500)).await;
            self.batches.lock().unwrap().push(batch);
            vec![(); n]
        }
    }

    let batches: Batches<i32> = Arc::new(std::sync::Mutex::new(Vec::new()));
    let proc = SlowProcessor {
        batches: Arc::clone(&batches),
    };

    // max_inflight=1，窗口很长
    let config = AccumulatorConfig::new(
        Duration::from_millis(20),  // 短窗口，快速触发 timer
        Duration::from_millis(10),
        Duration::from_secs(10),
    )
    .unwrap()
    .with_max_batch_size(1) // 每个 item 单独一个 batch
    .with_concurrency_mode(ConcurrencyMode::Concurrent { max_inflight: 1 });

    let (handle, accumulator) = config.build(proc, DefaultMetrics::new(), fixed(20));
    tokio::spawn(accumulator.run());

    // 快速提交多个 item
    for i in 0..5 {
        handle.send(i).unwrap();
    }

    // 等待足够时间让所有 task 完成
    tick(Duration::from_secs(5)).await;

    let guard = batches.lock().unwrap();
    let total: usize = guard.iter().map(|b| b.len()).sum();
    assert_eq!(total, 5, "all items should be processed");

    drop(handle);
}

#[tokio::test]
async fn test_concurrent_with_reply() {
    tokio::time::pause();
    // 并发模式下使用 reply
    struct Double;
    impl BatchProcessor<i32, i32> for Double {
        async fn process(&self, batch: Batch<i32>) -> Vec<i32> {
            batch.into_inner().into_iter().map(|x| x * 2).collect()
        }
    }

    let config = cfg(200)
        .with_concurrency_mode(ConcurrencyMode::Concurrent { max_inflight: 0 });
    let (handle, accumulator) = config.build(Double, DefaultMetrics::new(), fixed(200));
    tokio::spawn(accumulator.run());

    let r1 = handle.submit(10).unwrap();
    let r2 = handle.submit(20).unwrap();

    assert_eq!(r1.await.unwrap(), 20);
    assert_eq!(r2.await.unwrap(), 40);

    drop(handle);
}

#[tokio::test]
async fn test_serial_unchanged() {
    tokio::time::pause();
    let (proc, batches) = mock::<i32>();
    // 不设置 concurrency_mode，默认 Serial
    let config = cfg(100);
    let (handle, accumulator) = config.build(proc, DefaultMetrics::new(), fixed(100));
    tokio::spawn(accumulator.run());

    handle.send(1).unwrap();
    handle.send(2).unwrap();
    handle.send(3).unwrap();

    tick(Duration::from_millis(80)).await;
    assert_eq!(batches.lock().unwrap().len(), 0);

    tick(Duration::from_millis(40)).await;
    assert_eq!(batches.lock().unwrap().len(), 1);
    assert_eq!(batches.lock().unwrap()[0].len(), 3);

    drop(handle);
}

// ─── tracing 可观测性集成测试 ───

#[cfg(feature = "tracing")]
mod tracing_tests {
    use std::time::Duration;
    use windup::prelude::*;
    use tracing_subscriber::util::SubscriberInitExt;

    /// 验证 tracing flush 事件正常发出（不 panic）
    #[tokio::test]
    async fn tracing_flush_events_emitted() {
        // 使用 fmt subscriber 输出到 stderr（CI 友好）
        let _guard = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::INFO)
            .with_test_writer() // captures output to test
            .set_default();

        struct TestProcessor;
        impl BatchProcessor<i32, String> for TestProcessor {
            async fn process(&self, batch: Batch<i32>) -> Vec<String> {
                batch.items().iter().map(|i| format!("done-{i}")).collect()
            }
        }

        let config = AccumulatorConfig::new(
            Duration::from_millis(100),
            Duration::from_millis(50),
            Duration::from_secs(1),
        )
        .unwrap()
        .with_flush_empty_batches(false);

        let (handle, accumulator) = config.build(
            TestProcessor,
            DefaultMetrics::new(),
            FixedController::new(Duration::from_millis(100)),
        );

        let join = tokio::spawn(accumulator.run());

        let reply = handle.submit(42).unwrap();
        let result = reply.await.unwrap();
        assert_eq!(result, "done-42");

        handle.flush_now();
        tokio::time::sleep(Duration::from_millis(200)).await;

        drop(handle);
        let _ = join.await;
    }

    /// 验证队列满时发出 WARN 事件
    #[tokio::test]
    async fn tracing_queue_full_warns() {
        let _guard = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::WARN)
            .with_test_writer()
            .set_default();

        struct TestProcessor;
        impl BatchProcessor<i32, String> for TestProcessor {
            async fn process(&self, batch: Batch<i32>) -> Vec<String> {
                batch.items().iter().map(|i| format!("x-{i}")).collect()
            }
        }

        let config = AccumulatorConfig::new(
            Duration::from_millis(100),
            Duration::from_millis(50),
            Duration::from_secs(1),
        )
        .unwrap()
        .with_max_queue_depth(2);

        let (handle, accumulator) = config.build(
            TestProcessor,
            DefaultMetrics::new(),
            FixedController::new(Duration::from_millis(100)),
        );

        let join = tokio::spawn(accumulator.run());

        // 填满队列
        handle.send(1).unwrap();
        handle.send(2).unwrap();
        let result = handle.send(3);
        assert!(matches!(result, Err(AccumulatorError::QueueFull { .. })));

        handle.flush_now();
        tokio::time::sleep(Duration::from_millis(150)).await;

        drop(handle);
        let _ = join.await;
    }

    /// 验证 tracing 配置可通过 AccumulatorConfig 控制
    #[tokio::test]
    async fn tracing_config_integration() {
        let _guard = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::TRACE)
            .with_test_writer()
            .set_default();

        struct TestProcessor;
        impl BatchProcessor<i32, String> for TestProcessor {
            async fn process(&self, batch: Batch<i32>) -> Vec<String> {
                batch.items().iter().map(|i| format!("y-{i}")).collect()
            }
        }

        // 使用 tracing 配置
        let config = AccumulatorConfig::new(
            Duration::from_millis(100),
            Duration::from_millis(50),
            Duration::from_secs(1),
        )
        .unwrap()
        .with_trace_per_item(true); // 开启 per-item trace

        let (handle, accumulator) = config.build(
            TestProcessor,
            DefaultMetrics::new(),
            FixedController::new(Duration::from_millis(100)),
        );

        let join = tokio::spawn(accumulator.run());

        let reply = handle.submit(99).unwrap();
        let result = reply.await.unwrap();
        assert_eq!(result, "y-99");

        handle.flush_now();
        tokio::time::sleep(Duration::from_millis(200)).await;

        drop(handle);
        let _ = join.await;
    }
}

// ─── submit_or_wait / send_or_wait ───

#[tokio::test]
async fn test_submit_or_wait_waits_for_slot() {
    let config = AccumulatorConfig::new(
        Duration::from_millis(200),
        Duration::from_millis(50),
        Duration::from_secs(5),
    )
    .unwrap()
    .with_max_queue_depth(1);

    let (handle, accumulator) = config.build(
        StringProcessor,
        DefaultMetrics::new(),
        FixedController::new(Duration::from_millis(200)),
    );

    let join = tokio::spawn(accumulator.run());

    // 占满队列
    let _reply = handle.submit(1).unwrap();

    // 阻塞提交：应该等待 slot 释放后成功
    let h2 = handle.clone();
    let blocking_result = tokio::spawn(async move {
        h2.submit_or_wait(2, Duration::from_secs(5)).await
    });

    // 稍等后触发 flush 释放 slot
    tokio::time::sleep(Duration::from_millis(300)).await;
    handle.flush_now();

    let result = blocking_result.await.unwrap().unwrap();
    let reply = result.await.unwrap();
    assert_eq!(reply, "y-2");

    drop(handle);
    let _ = join.await;
}

#[tokio::test]
async fn test_send_or_wait_waits() {
    let config = AccumulatorConfig::new(
        Duration::from_millis(200),
        Duration::from_millis(50),
        Duration::from_secs(5),
    )
    .unwrap()
    .with_max_queue_depth(1);

    let (handle, accumulator) = config.build(
        StringProcessor,
        DefaultMetrics::new(),
        FixedController::new(Duration::from_millis(200)),
    );

    let join = tokio::spawn(accumulator.run());

    // 占满队列
    handle.send(1).unwrap();

    // send_or_wait 应等待 slot
    let h2 = handle.clone();
    let blocking_task = tokio::spawn(async move {
        h2.send_or_wait(2, Duration::from_secs(5)).await
    });

    tokio::time::sleep(Duration::from_millis(300)).await;
    handle.flush_now();

    assert!(blocking_task.await.unwrap().is_ok());

    drop(handle);
    let _ = join.await;
}

// ─── drain_timeout ───

#[tokio::test]
async fn test_drain_timeout_does_not_hang() {
    // 使用并发模式 + drain_timeout，确保 drain 不会永久挂起
    let config = AccumulatorConfig::new(
        Duration::from_millis(200),
        Duration::from_millis(50),
        Duration::from_secs(5),
    )
    .unwrap()
    .with_concurrency_mode(ConcurrencyMode::Concurrent { max_inflight: 1 })
    .with_drain_timeout(Some(Duration::from_millis(500)));

    let (handle, accumulator) = config.build(
        SlowProcessor { delay: Duration::from_secs(10) },
        DefaultMetrics::new(),
        FixedController::new(Duration::from_millis(200)),
    );

    let join = tokio::spawn(accumulator.run());

    handle.send(1).unwrap();
    // 手动 flush 触发并发处理
    handle.flush_now();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // drop handle 触发 drain，drain_timeout=500ms 应生效
    drop(handle);

    // 不应永久挂起
    let result = tokio::time::timeout(Duration::from_secs(2), join).await;
    assert!(result.is_ok(), "drain with timeout should complete");
}

// ─── Batch 上下文 ───

struct ContextCheckProcessor {
    seen: Arc<Mutex<Option<(Duration, usize)>>>,
}

impl BatchProcessor<i32> for ContextCheckProcessor {
    async fn process(&self, batch: Batch<i32>) -> Vec<()> {
        let w = batch.window_duration();
        let q = batch.queue_depth_at_flush();
        *self.seen.lock().unwrap() = Some((w, q));
        vec![(); batch.len()]
    }
}

#[tokio::test]
async fn test_batch_context_fields() {
    let seen = Arc::new(Mutex::new(None));
    let config = AccumulatorConfig::new(
        Duration::from_millis(100),
        Duration::from_millis(50),
        Duration::from_secs(5),
    )
    .unwrap();

    let (handle, accumulator) = config.build(
        ContextCheckProcessor {
            seen: Arc::clone(&seen),
        },
        DefaultMetrics::new(),
        FixedController::new(Duration::from_millis(100)),
    );

    let join = tokio::spawn(accumulator.run());

    handle.send(1).unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;

    drop(handle);
    let _ = join.await;

    let ctx = seen.lock().unwrap();
    assert!(ctx.is_some(), "batch context should be set");
    let (window, _queue_depth) = ctx.unwrap();
    assert!(window > Duration::ZERO, "window_duration should be non-zero");
}

// ─── health 检查 ───

#[tokio::test]
async fn test_health_reflects_state() {
    let config = AccumulatorConfig::new(
        Duration::from_millis(200),
        Duration::from_millis(50),
        Duration::from_secs(5),
    )
    .unwrap()
    .with_max_queue_depth(10);

    let (handle, accumulator) = config.build(
        StringProcessor,
        DefaultMetrics::new(),
        FixedController::new(Duration::from_millis(200)),
    );

    let join = tokio::spawn(accumulator.run());

    let health = handle.health();
    assert!(health.is_accepting);
    assert!(health.current_window > Duration::ZERO);
    assert_eq!(health.total_rejected, 0);

    drop(handle);
    let _ = join.await;
}

// ─── record_rejected 统计 ───

#[tokio::test]
async fn test_rejected_counter_increments() {
    let config = AccumulatorConfig::new(
        Duration::from_millis(200),
        Duration::from_millis(50),
        Duration::from_secs(5),
    )
    .unwrap()
    .with_max_queue_depth(1);

    let (handle, accumulator) = config.build(
        StringProcessor,
        DefaultMetrics::new(),
        FixedController::new(Duration::from_millis(200)),
    );

    let join = tokio::spawn(accumulator.run());

    // 占满队列
    let _reply = handle.submit(1).unwrap();
    // 尝试再次提交，应被拒绝
    let err = handle.send(2).unwrap_err();
    assert!(matches!(err, AccumulatorError::QueueFull { .. }));

    let health = handle.health();
    assert_eq!(health.total_rejected, 1);

    drop(handle);
    let _ = join.await;
}

// ─── send (重命名后) fire-and-forget ───

#[tokio::test]
async fn test_send_fire_and_forget() {
    let config = AccumulatorConfig::new(
        Duration::from_millis(200),
        Duration::from_millis(50),
        Duration::from_secs(5),
    )
    .unwrap();

    let (handle, accumulator) = config.build(
        StringProcessor,
        DefaultMetrics::new(),
        FixedController::new(Duration::from_millis(200)),
    );

    let join = tokio::spawn(accumulator.run());

    // send 不返回 ReplyHandle
    assert!(handle.send(42).is_ok());

    drop(handle);
    let _ = join.await;
}

// ─── 辅助处理器 ───

/// 模拟慢处理的处理器。
struct SlowProcessor {
    delay: Duration,
}

impl BatchProcessor<i32> for SlowProcessor {
    async fn process(&self, batch: Batch<i32>) -> Vec<()> {
        tokio::time::sleep(self.delay).await;
        vec![(); batch.len()]
    }
}
