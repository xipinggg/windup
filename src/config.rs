use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::sync::Notify;
use tokio::task::JoinSet;

use crate::accumulator::{AccumulatorHandle, BatchAccumulator};
use crate::batch::{BatchProcessor, FlushInfo};
use crate::controller::WindowController;
use crate::error::AccumulatorError;
use crate::metrics::MetricsCollector;

/// 并发模式配置。
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum ConcurrencyMode {
    /// 串行模式：每个批次在主循环中同步处理（默认，当前行为）。
    #[default]
    Serial,
    /// 并发模式：批次在后台 tokio 任务中处理，主循环可继续收集新 item。
    ///
    /// `max_inflight` 限制最大并发批次数，`0` 表示无限制。
    Concurrent {
        /// 最大并发批次数。
        max_inflight: usize,
    },
}

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
    pub(crate) concurrency_mode: ConcurrencyMode,
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
            concurrency_mode: ConcurrencyMode::Serial,
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

    /// 设置并发模式。默认为 [`ConcurrencyMode::Serial`]。
    pub fn with_concurrency_mode(mut self, mode: ConcurrencyMode) -> Self {
        self.concurrency_mode = mode;
        self
    }

    /// 消费配置，构建 [`AccumulatorHandle`] 和 [`BatchAccumulator`]。
    ///
    /// item 类型 `T` 和结果类型 `R` 从 `processor: P` 自动推断。
    #[allow(clippy::type_complexity)]
    pub fn build<T, R, P, M, C>(
        self,
        processor: P,
        metrics: M,
        controller: C,
    ) -> (AccumulatorHandle<T, R>, BatchAccumulator<T, R, P, M, C>)
    where
        T: Send + 'static,
        R: Send + 'static,
        P: BatchProcessor<T, R>,
        M: MetricsCollector,
        C: WindowController,
    {
        let (tx, rx) = mpsc::unbounded_channel();
        let (bypass_tx, bypass_rx) = mpsc::unbounded_channel();
        let (feedback_tx, feedback_rx) = mpsc::unbounded_channel::<FlushInfo>();
        let pending_count = Arc::new(AtomicUsize::new(0));
        let inflight_count = Arc::new(AtomicUsize::new(0));
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
            processor: Arc::new(processor),
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
            feedback_rx,
            feedback_tx,
            inflight: JoinSet::new(),
            inflight_count,
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
            concurrency_mode: ConcurrencyMode::Serial,
        }
    }
}
