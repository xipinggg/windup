use std::sync::{Arc, Mutex};
use std::time::Duration;

use draft::prelude::*;

type Batches<T> = Arc<Mutex<Vec<Batch<T>>>>;

struct MockProcessor<T: Send> {
    batches: Batches<T>,
}

impl<T: Send + 'static> BatchProcessor<T> for MockProcessor<T> {
    async fn process(&self, batch: Batch<T>) {
        self.batches.lock().unwrap().push(batch);
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

    handle.submit(1).unwrap();
    handle.submit(2).unwrap();
    handle.submit(3).unwrap();

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

    handle.submit(1).unwrap();
    handle.submit(2).unwrap();
    handle.submit(3).unwrap();

    tick(Duration::ZERO).await;
    assert_eq!(batches.lock().unwrap().len(), 1);
    assert_eq!(batches.lock().unwrap()[0].len(), 3);

    handle.submit(4).unwrap();
    handle.submit(5).unwrap();
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

    handle.submit(1).unwrap();
    handle.submit(2).unwrap();
    tick(Duration::from_millis(60)).await;

    let after_first = batches.lock().unwrap().len();
    assert_eq!(after_first, 1, "first flush should have occurred");

    handle.submit(3).unwrap();
    handle.submit(4).unwrap();
    handle.submit(5).unwrap();
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

    handle.submit(1).unwrap();
    handle.submit(2).unwrap();
    handle.submit(3).unwrap();

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

    handle.submit(1).unwrap();
    handle.submit(2).unwrap();

    let err = handle.submit(3).unwrap_err();
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
                h.submit(i).unwrap();
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
