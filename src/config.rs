use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::sync::Notify;

use crate::accumulator::{AccumulatorHandle, BatchAccumulator};
use crate::batch::BatchProcessor;
use crate::controller::WindowController;
use crate::error::AccumulatorError;
use crate::metrics::MetricsCollector;

/// 累加器配置（与 item 类型无关）。
///
/// 使用 builder 模式构建，最终调用 [`build`](Self::build) 生成
/// [`AccumulatorHandle`] 和 [`BatchAccumulator`] 对。
#[derive(Debug, Clone)]
pub struct AccumulatorConfig {
    pub(crate) initial_window: Duration,
    pub(crate) min_window: Duration,
    pub(crate) max_window: Duration,
    pub(crate) max_batch_size: Option<usize>,
    pub(crate) max_queue_depth: Option<usize>,
    pub(crate) flush_empty_batches: bool,
}

impl AccumulatorConfig {
    /// 创建新配置。
    ///
    /// # Errors
    ///
    /// 当 `initial_window` 不在 `[min_window, max_window]` 范围内时返回错误。
    pub fn new(
        initial_window: Duration,
        min_window: Duration,
        max_window: Duration,
    ) -> Result<Self, AccumulatorError> {
        if initial_window < min_window {
            return Err(AccumulatorError::InvalidConfig {
                reason: format!(
                    "initial_window ({initial_window:?}) must be >= min_window ({min_window:?})"
                ),
            });
        }
        if initial_window > max_window {
            return Err(AccumulatorError::InvalidConfig {
                reason: format!(
                    "initial_window ({initial_window:?}) must be <= max_window ({max_window:?})"
                ),
            });
        }
        Ok(Self {
            initial_window,
            min_window,
            max_window,
            max_batch_size: None,
            max_queue_depth: None,
            flush_empty_batches: false,
        })
    }

    /// 设置最大批次大小。达到此大小时立即 flush，不等时间窗口到期。
    pub fn with_max_batch_size(mut self, n: usize) -> Self {
        self.max_batch_size = Some(n);
        self
    }

    /// 设置最大队列深度。超限时 [`AccumulatorHandle::submit`] 返回
    /// [`AccumulatorError::QueueFull`](crate::error::AccumulatorError::QueueFull)。
    pub fn with_max_queue_depth(mut self, n: usize) -> Self {
        self.max_queue_depth = Some(n);
        self
    }

    /// 设置是否 flush 空批次。
    pub fn with_flush_empty_batches(mut self, enabled: bool) -> Self {
        self.flush_empty_batches = enabled;
        self
    }

    /// 消费配置，构建 [`AccumulatorHandle`] 和 [`BatchAccumulator`]。
    ///
    /// item 类型 `T` 从 `processor: P` 自动推断。
    pub fn build<T, P, M, C>(
        self,
        processor: P,
        metrics: M,
        controller: C,
    ) -> (AccumulatorHandle<T>, BatchAccumulator<T, P, M, C>)
    where
        T: Send + 'static,
        P: BatchProcessor<T>,
        M: MetricsCollector,
        C: WindowController,
    {
        let (tx, rx) = mpsc::unbounded_channel();
        let (bypass_tx, bypass_rx) = mpsc::unbounded_channel();
        let pending_count = Arc::new(AtomicUsize::new(0));
        let flush_notify = Arc::new(Notify::new());

        let handle = AccumulatorHandle {
            sender: tx,
            bypass_sender: bypass_tx,
            pending_count: Arc::clone(&pending_count),
            max_queue_depth: self.max_queue_depth,
            flush_notify: Arc::clone(&flush_notify),
        };

        let current_window = self.initial_window;

        let accumulator = BatchAccumulator {
            config: self,
            processor,
            metrics,
            controller,
            item_rx: rx,
            bypass_rx,
            flush_notify,
            buffer: Vec::new(),
            current_window,
            next_batch_id: 0,
            last_flush_time: tokio::time::Instant::now(),
            pending_count,
        };

        (handle, accumulator)
    }
}

impl Default for AccumulatorConfig {
    fn default() -> Self {
        Self {
            initial_window: Duration::from_millis(200),
            min_window: Duration::from_millis(50),
            max_window: Duration::from_secs(10),
            max_batch_size: None,
            max_queue_depth: None,
            flush_empty_batches: false,
        }
    }
}
