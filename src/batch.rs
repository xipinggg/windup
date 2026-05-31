use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use tokio::sync::oneshot;
use tokio::time::Instant;

use crate::error::AccumulatorError;

/// 批处理器，负责消费累加器 flush 出的批次。
///
/// 由用户实现，包含实际的批量处理逻辑。
///
/// # 类型参数
///
/// - `T`：批次中 item 的类型。
/// - `R`：每个 item 的处理结果类型，默认为 `()`（fire-and-forget）。
pub trait BatchProcessor<T: Send, R: Send = ()>: Send + 'static {
    /// 处理一个批次，按 item 顺序返回每个 item 的处理结果。
    fn process(&self, batch: Batch<T>) -> impl Future<Output = Vec<R>> + Send;
}

/// 一个待处理的批次。
///
/// 包含在时间窗口内收集到的所有 item。
#[derive(Debug)]
pub struct Batch<T> {
    items: Vec<T>,
    batch_id: u64,
    created_at: Instant,
    /// flush 时的窗口大小，供处理器了解调度上下文。
    window_duration: Duration,
    /// flush 时的通道队列深度，供处理器了解当前负载。
    queue_depth_at_flush: usize,
}

impl<T> Batch<T> {
    /// 创建带上下文信息的新批次。
    pub(crate) fn with_context(
        items: Vec<T>,
        batch_id: u64,
        window_duration: Duration,
        queue_depth_at_flush: usize,
    ) -> Self {
        Self {
            items,
            batch_id,
            created_at: Instant::now(),
            window_duration,
            queue_depth_at_flush,
        }
    }

    /// 消耗批次，返回内部 item 列表。
    pub fn into_inner(self) -> Vec<T> {
        self.items
    }

    /// 返回批次中 item 的不可变引用。
    pub fn items(&self) -> &[T] {
        &self.items
    }

    /// 批次中的 item 数量。
    pub fn len(&self) -> usize {
        self.items.len()
    }

    /// 批次是否为空。
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    /// 批次编号（自增）。
    pub fn batch_id(&self) -> u64 {
        self.batch_id
    }

    /// 批次创建时刻。
    pub fn created_at(&self) -> Instant {
        self.created_at
    }

    /// 批次从创建到现在经过的时间。
    pub fn age(&self) -> Duration {
        self.created_at.elapsed()
    }

    /// flush 时的窗口大小。处理器可据此了解调度上下文。
    pub fn window_duration(&self) -> Duration {
        self.window_duration
    }

    /// flush 时通道中待处理的 item 数。
    pub fn queue_depth_at_flush(&self) -> usize {
        self.queue_depth_at_flush
    }
}

/// 调用方 [`submit`](super::accumulator::AccumulatorHandle::submit) item 后等待结果的 Future。
///
/// `.await` 后得到对应 item 的处理结果。
///
/// 可能返回的错误：
/// - [`AccumulatorError::Shutdown`]：累加器在结果返回前关闭。
/// - [`AccumulatorError::Timeout`]：item 在批处理前超时。
pub struct ReplyHandle<R> {
    rx: oneshot::Receiver<Result<R, AccumulatorError>>,
}

impl<R> ReplyHandle<R> {
    /// 创建新的 ReplyHandle（内部使用）。
    pub(crate) fn new(rx: oneshot::Receiver<Result<R, AccumulatorError>>) -> Self {
        Self { rx }
    }
}

impl<R> Future for ReplyHandle<R> {
    type Output = Result<R, AccumulatorError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match Pin::new(&mut self.rx).poll(cx) {
            Poll::Ready(Ok(Ok(r))) => Poll::Ready(Ok(r)),
            Poll::Ready(Ok(Err(e))) => Poll::Ready(Err(e)),
            Poll::Ready(Err(_recv_error)) => Poll::Ready(Err(AccumulatorError::Shutdown)),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Item 优先级。
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Priority {
    /// 普通优先级（默认）。
    #[default]
    Normal,
    /// 高优先级：插队到 buffer 前端。
    High,
}

/// Submit 选项，通过 [`AccumulatorHandle::submit_with`](super::accumulator::AccumulatorHandle::submit_with) 传入。
#[derive(Debug, Default, Clone)]
pub struct SubmitOptions {
    /// 优先级。默认为 [`Priority::Normal`]。
    pub priority: Priority,
    /// item 超时时间。`None` 表示不超时。
    pub ttl: Option<Duration>,
}

/// flush 完成后的汇总信息，供指标收集和窗口控制使用。
#[derive(Debug, Clone)]
pub struct FlushInfo {
    /// 本批次实际 item 数。
    pub batch_size: usize,
    /// 配置的最大批次大小（None 表示无上限）。
    pub max_batch_size: Option<usize>,
    /// 本次 flush 时使用的时间窗口。
    pub window_duration: Duration,
    /// 通道中剩余待处理 item 数。
    pub items_remaining: usize,
    /// 批次编号。
    pub batch_id: u64,
    /// [`BatchProcessor::process`] 的执行耗时。
    pub execution_time: Duration,
    /// 距上次 flush 的时间。
    pub time_since_last_flush: Duration,
    /// 本批次总权重（启用权重追踪时有意义）。
    pub total_weight: Option<usize>,
}
