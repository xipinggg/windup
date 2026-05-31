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
use crate::stats::AccumulatorStats;

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
    pub(crate) max_batch_weight: Option<usize>,
    /// tracing 日志级别。None 表示运行时关闭。feature 关闭时类型为 ()。
    pub(crate) tracing_level: crate::trace::TraceLevel,
    /// 是否记录 per-item TRACE 级别事件（高频，默认关闭）。
    pub(crate) trace_per_item: bool,
    /// drain 阶段等待 inflight task 完成的超时时间。
    /// `None` 表示无限等待（默认）。仅在并发模式下生效。
    pub(crate) drain_timeout: Option<Duration>,
    /// 主循环每轮非阻塞 drain 的最大批次数，防止 bypass/feedback 饿死 select!。
    /// 默认 64。
    pub(crate) drain_batch_limit: usize,
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
            max_batch_weight: None,
            tracing_level: crate::trace::default_tracing_level(),
            trace_per_item: false,
            drain_timeout: None,
            drain_batch_limit: 64,
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

    /// 设置最大批次权重。总权重达到此值时提前 flush。
    ///
    /// 需要配合 [`build_with_weight`](Self::build_with_weight) 传入权重函数。
    pub fn with_max_batch_weight(mut self, n: usize) -> Self {
        self.max_batch_weight = Some(n);
        self
    }

    /// 设置 tracing 日志级别。设为 `None` 可在运行时完全关闭 tracing。
    ///
    /// 仅在 `tracing` feature 开启时生效。
    #[cfg(feature = "tracing")]
    pub fn with_tracing_level(mut self, level: Option<tracing::Level>) -> Self {
        self.tracing_level = level;
        self
    }

    /// feature 关闭时的桩方法，接收并忽略参数，保持 API 一致性。
    #[cfg(not(feature = "tracing"))]
    pub fn with_tracing_level(mut self, level: ()) -> Self {
        self.tracing_level = level;
        self
    }

    /// 是否开启 per-item 级别追踪（`TRACE` 级别事件）。
    ///
    /// 高频路径，默认关闭。仅在 `tracing` feature 开启时生效。
    pub fn with_trace_per_item(mut self, enabled: bool) -> Self {
        self.trace_per_item = enabled;
        self
    }

    /// 设置 drain 阶段等待 inflight task 完成的超时时间。
    ///
    /// `None` 表示无限等待（默认）。仅在并发模式下生效。
    /// 超时后未完成的 inflight task 会被 abort。
    pub fn with_drain_timeout(mut self, timeout: Option<Duration>) -> Self {
        self.drain_timeout = timeout;
        self
    }

    /// 设置主循环每轮非阻塞 drain 的最大批次数。
    ///
    /// 防止 bypass/feedback 通道饿死 select!。默认 64。
    /// 高吞吐场景可适当增大。
    pub fn with_drain_batch_limit(mut self, limit: usize) -> Self {
        self.drain_batch_limit = limit;
        self
    }

    /// 消费配置，构建并启动累加器。
    ///
    /// 等价于 `build()` + `tokio::spawn(accumulator.run())`。
    /// 返回 `(Handle, JoinHandle)`，`JoinHandle` 在 accumulator 退出时完成。
    #[allow(clippy::type_complexity)]
    pub fn build_and_spawn<T, R, P, M, C>(
        self,
        processor: P,
        metrics: M,
        controller: C,
    ) -> (AccumulatorHandle<T, R>, tokio::task::JoinHandle<()>)
    where
        T: Send + 'static,
        R: Send + 'static,
        P: BatchProcessor<T, R> + Sync,
        M: MetricsCollector,
        C: WindowController,
    {
        let (handle, accumulator) = self.build(processor, metrics, controller);
        let join = tokio::spawn(accumulator.run());
        (handle, join)
    }

    /// 消费配置，构建 [`AccumulatorHandle`] 和 [`BatchAccumulator`]（无权重追踪）。
    ///
    /// 等价于 `build_with_weight(processor, metrics, controller, |_| 1)`。
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
        self.build_with_weight(processor, metrics, controller, |_| 1)
    }

    /// 消费配置，构建 [`AccumulatorHandle`] 和 [`BatchAccumulator`]，传入权重函数。
    ///
    /// item 类型 `T` 和结果类型 `R` 从 `processor: P` 自动推断。
    #[allow(clippy::type_complexity)]
    pub fn build_with_weight<T, R, P, M, C>(
        self,
        processor: P,
        metrics: M,
        controller: C,
        weight_fn: impl Fn(&T) -> usize + Send + Sync + 'static,
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
        let flush_notify = Arc::new(Notify::new());
        let queue_notify = Arc::new(Notify::new());
        let stats = Arc::new(AccumulatorStats::new());
        let shared = Arc::new(crate::accumulator::SharedState::new(self.initial_window));

        let handle = AccumulatorHandle {
            sender: tx,
            bypass_sender: bypass_tx,
            pending_count: Arc::clone(&pending_count),
            max_queue_depth: self.max_queue_depth,
            flush_notify: Arc::clone(&flush_notify),
            stats: Arc::clone(&stats),
            tracing_level: self.tracing_level,
            queue_notify: Arc::clone(&queue_notify),
            shared: Arc::clone(&shared),
        };

        let accumulator = BatchAccumulator {
            config: self,
            processor: Arc::new(processor),
            metrics,
            controller,
            item_rx: rx,
            bypass_rx,
            flush_notify,
            buffer: std::collections::VecDeque::new(),
            next_batch_id: 0,
            last_flush_time: tokio::time::Instant::now(),
            pending_count,
            feedback_rx,
            feedback_tx,
            inflight: JoinSet::new(),
            stats,
            weight_fn: Arc::new(weight_fn),
            current_weight: 0,
            queue_notify: Arc::clone(&queue_notify),
            shared,
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
            max_batch_weight: None,
            tracing_level: crate::trace::default_tracing_level(),
            trace_per_item: false,
            drain_timeout: None,
            drain_batch_limit: 64,
        }
    }
}
