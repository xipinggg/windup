use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::Notify;
use tokio::task::JoinSet;
use tokio::time::Instant;

use crate::batch::{Batch, BatchProcessor, FlushInfo, Priority, ReplyHandle, SubmitOptions};
use crate::config::{AccumulatorConfig, ConcurrencyMode};
use crate::controller::WindowController;
use crate::error::AccumulatorError;
use crate::metrics::MetricsCollector;
use crate::stats::{AccumulatorStats, StatsSnapshot};
use crate::trace::event_at;
#[cfg_attr(not(feature = "tracing"), allow(unused_imports))]
use crate::trace::Level;

use std::collections::VecDeque;

/// 并发满后重试 flush 的间隔。
const RETRY_DELAY: Duration = Duration::from_millis(10);

/// Reply sender 类型别名，简化复杂类型签名。
type ReplySender<R> = Option<tokio::sync::oneshot::Sender<Result<R, AccumulatorError>>>;

/// Handle 和 Accumulator 之间共享的运行时状态。
///
/// 使 `AccumulatorHandle::stats()` / `health()` 能返回准确值，
/// 不再传零占位。
pub(crate) struct SharedState {
    /// 当前时间窗口大小。
    pub current_window: RwLock<Duration>,
    /// 并发模式下的飞行中批次数。
    pub inflight_count: AtomicUsize,
    /// 累加器已被取消（外部信号触发 drain）。
    pub cancelled: AtomicBool,
    /// 累加器已暂停（缓冲 item 但不 flush）。
    pub paused: AtomicBool,
}

impl SharedState {
    pub(crate) fn new(initial_window: Duration) -> Self {
        Self {
            current_window: RwLock::new(initial_window),
            inflight_count: AtomicUsize::new(0),
            cancelled: AtomicBool::new(false),
            paused: AtomicBool::new(false),
        }
    }
}

/// Drop 时自动递减 inflight_count，确保 panic 也不会泄漏计数。
struct InflightGuard {
    shared: Option<Arc<SharedState>>,
}

impl InflightGuard {
    /// 创建守卫。若 `shared` 为 None 则不做任何操作（无限并发模式）。
    fn new(shared: Option<Arc<SharedState>>) -> Self {
        if let Some(ref s) = shared {
            s.inflight_count.fetch_add(1, Ordering::Release);
        }
        Self { shared }
    }
}

impl Drop for InflightGuard {
    fn drop(&mut self) {
        if let Some(ref s) = self.shared {
            s.inflight_count.fetch_sub(1, Ordering::Release);
        }
    }
}

/// 主通道中传递的 item 类型。
pub(crate) struct ChannelItem<T, R> {
    /// item 数据。
    pub value: T,
    /// 超时截止时间。`None` 表示不超时。
    pub deadline: Option<Instant>,
    /// 回复通道。`None` 表示 fire-and-forget。
    pub reply: Option<tokio::sync::oneshot::Sender<Result<R, AccumulatorError>>>,
    /// 优先级。
    pub priority: Priority,
    /// 调用方 submit 时的 span，用于跨 channel 传播上下文。
    pub parent_span: crate::trace::MaybeSpan,
    /// 提交时刻，用于计算队列等待时间。
    pub submitted_at: Instant,
}

/// 向累加器提交 item 的句柄。
///
/// 可安全克隆，跨 task 共享。
pub struct AccumulatorHandle<T, R> {
    pub(crate) sender: UnboundedSender<ChannelItem<T, R>>,
    pub(crate) bypass_sender: UnboundedSender<Vec<T>>,
    pub(crate) pending_count: Arc<AtomicUsize>,
    pub(crate) max_queue_depth: Option<usize>,
    pub(crate) flush_notify: Arc<Notify>,
    pub(crate) stats: Arc<AccumulatorStats>,
    pub(crate) tracing_level: crate::trace::TraceLevel,
    /// 队列空位通知：item 被消费后唤醒阻塞的 submit 等待者。
    pub(crate) queue_notify: Arc<Notify>,
    /// 共享运行时状态（窗口大小、inflight 计数等）。
    pub(crate) shared: Arc<SharedState>,
}

impl<T: Send, R: Send> Clone for AccumulatorHandle<T, R> {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
            bypass_sender: self.bypass_sender.clone(),
            pending_count: Arc::clone(&self.pending_count),
            max_queue_depth: self.max_queue_depth,
            flush_notify: Arc::clone(&self.flush_notify),
            stats: Arc::clone(&self.stats),
            tracing_level: self.tracing_level,
            queue_notify: Arc::clone(&self.queue_notify),
            shared: Arc::clone(&self.shared),
        }
    }
}

impl<T: Send, R: Send> AccumulatorHandle<T, R> {
    /// 执行 acquire_slot + sender.send + record_submit 的公共流水线。
    fn send_channel(
        &self,
        value: T,
        deadline: Option<Instant>,
        reply: Option<tokio::sync::oneshot::Sender<Result<R, AccumulatorError>>>,
        priority: Priority,
    ) -> Result<(), AccumulatorError> {
        self.acquire_slot()?;
        self.sender
            .send(ChannelItem { value, deadline, reply, priority, parent_span: crate::trace::current_span(), submitted_at: Instant::now() })
            .map_err(|_| { self.pending_count.fetch_sub(1, Ordering::Release); AccumulatorError::Shutdown })?;
        self.stats.record_submit();
        Ok(())
    }

    /// 阻塞提交/发送的核心循环。`with_reply=true` 返回 ReplyHandle。
    async fn blocking_inner(
        &self, item: T, timeout: Duration, with_reply: bool,
    ) -> Result<Option<ReplyHandle<R>>, AccumulatorError> {
        loop {
            match self.acquire_slot() {
                Ok(()) => {
                    let (tx, reply) = if with_reply {
                        let (tx, rx) = tokio::sync::oneshot::channel();
                        (Some(tx), Some(ReplyHandle::new(rx)))
                    } else { (None, None) };
                    match self.sender.send(ChannelItem {
                        value: item, deadline: None, reply: tx,
                        priority: Priority::Normal, parent_span: crate::trace::current_span(), submitted_at: Instant::now(),
                    }) {
                        Ok(()) => { self.stats.record_submit(); return Ok(reply); }
                        Err(_) => { self.pending_count.fetch_sub(1, Ordering::Release); return Err(AccumulatorError::Shutdown); }
                    }
                }
                Err(AccumulatorError::QueueFull { .. }) => {
                    let notified = tokio::select! {
                        _ = self.queue_notify.notified() => true,
                        _ = tokio::time::sleep(timeout) => false,
                    };
                    if !notified { return Err(AccumulatorError::Timeout); }
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// CAS 递增 pending_count，预留一个队列槽位。
    ///
    /// 成功返回 `Ok(())`；队列满时返回 `Err(QueueFull)` 并自动记录拒绝统计。
    fn acquire_slot(&self) -> Result<(), AccumulatorError> {
        self.pending_count
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |pending| {
                if let Some(max) = self.max_queue_depth
                    && pending >= max
                {
                    return None;
                }
                Some(pending + 1)
            })
            .map_err(|prev| {
                self.stats.record_rejected();
                let max = self.max_queue_depth.unwrap_or(0);
                event_at!(
                    Level::WARN,
                    &self.tracing_level,
                    max,
                    pending = prev,
                    "队列已满，拒绝提交"
                );
                AccumulatorError::QueueFull { max, pending: prev }
            })
            .map(|_| ())
    }

    /// 发送一个 item 到累加器（fire-and-forget，不关心结果）。
    ///
    /// 如果你需要获取处理结果，请使用 [`submit`](Self::submit)。
    ///
    /// # Errors
    ///
    /// - [`AccumulatorError::QueueFull`]：超过 `max_queue_depth` 限制。
    /// - [`AccumulatorError::Shutdown`]：累加器已关闭。
    pub fn send(&self, item: T) -> Result<(), AccumulatorError> {
        self.send_channel(item, None, None, Priority::Normal)
    }

    /// 提交一个 item，返回 [`ReplyHandle`]，`.await` 后拿到处理结果。
    ///
    /// 如果不需要结果，可使用 [`send`](Self::send)（fire-and-forget）。
    ///
    /// # Errors
    ///
    /// - [`AccumulatorError::QueueFull`]：超过 `max_queue_depth` 限制。
    /// - [`AccumulatorError::Shutdown`]：累加器已关闭。
    pub fn submit(&self, item: T) -> Result<ReplyHandle<R>, AccumulatorError> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.send_channel(item, None, Some(tx), Priority::Normal)?;
        Ok(ReplyHandle::new(rx))
    }

    /// 返回当前通道中待处理 item 数（近似值）。
    ///
    /// 注意：此数值不含 bypass 通道中的 item。
    pub fn pending_count(&self) -> usize {
        self.pending_count.load(Ordering::Acquire)
    }

    /// 手动触发一次立即 flush。
    ///
    /// 通知累加器尽快 flush 当前 buffer。
    /// 如果 flush 正在执行中，本次通知会被合并。
    pub fn flush_now(&self) {
        self.flush_notify.notify_one();
    }

    /// 获取运行统计快照。
    ///
    /// 需要在 [`AccumulatorConfig::with_stats(true)`] 开启统计。
    /// 若未开启，各计数器均为 0。
    pub fn stats(&self) -> StatsSnapshot {
        let current_window = *self.shared.current_window.read().unwrap_or_else(|e| e.into_inner());
        self.stats.snapshot(
            self.pending_count(),
            0, // buffer_size 由 Accumulator 侧提供才有准确值
            self.shared.inflight_count.load(Ordering::Acquire),
            0, // current_weight 由 Accumulator 侧提供才有准确值
            current_window,
        )
    }

    /// 获取累加器健康状态。
    ///
    /// 供外部监控系统（如 k8s liveness probe）调用。
    pub fn health(&self) -> crate::stats::AccumulatorHealth {
        let pending = self.pending_count();
        let queue_utilization = match self.max_queue_depth {
            Some(max) if max > 0 => pending as f64 / max as f64,
            _ => 0.0,
        };
        let current_window = *self.shared.current_window.read().unwrap_or_else(|e| e.into_inner());
        crate::stats::AccumulatorHealth {
            is_accepting: !self.sender.is_closed(),
            queue_utilization,
            current_window,
            inflight_count: self.shared.inflight_count.load(Ordering::Acquire),
            total_rejected: self.stats.total_rejected.load(Ordering::Acquire),
        }
    }

    /// 取消累加器：触发 drain 后退出。
    ///
    /// 等价于 drop(handle)，但可提前触发关闭而不等待所有 Handle 被 drop。
    pub fn cancel(&self) {
        self.shared.cancelled.store(true, Ordering::Release);
        self.flush_notify.notify_one();
    }

    /// 暂停累加器：继续接收 item 但不触发 flush。
    ///
    /// timer 到期、达到 max_batch_size 等均被抑制。高优先级 item 仍会触发 flush。
    /// 用于速率限制或优雅降级场景。
    pub fn pause(&self) {
        self.shared.paused.store(true, Ordering::Release);
    }

    /// 恢复累加器：重新允许 flush。
    pub fn resume(&self) {
        self.shared.paused.store(false, Ordering::Release);
        self.flush_notify.notify_one(); // 触发一次检查
    }

    /// 累加器当前是否处于暂停状态。
    pub fn is_paused(&self) -> bool {
        self.shared.paused.load(Ordering::Acquire)
    }

    /// 绕过批处理，直接交付处理器。
    ///
    /// item 打包为单 item 批，跳过 buffer 和 timer，直接交给
    /// [`BatchProcessor`] 处理。不参与利用率统计和窗口调整。
    ///
    /// 注意：bypass 不支持 reply 机制。
    ///
    /// # Errors
    ///
    /// - [`AccumulatorError::Shutdown`]：累加器已关闭。
    pub fn bypass(&self, item: T) -> Result<(), AccumulatorError> {
        self.bypass_sender
            .send(vec![item])
            .map_err(|_| AccumulatorError::Shutdown)?;
        self.stats.record_bypass();
        Ok(())
    }

    /// 提交一个 item，可指定优先级和超时。
    ///
    /// # Errors
    ///
    /// - [`AccumulatorError::QueueFull`]：超过 `max_queue_depth` 限制。
    /// - [`AccumulatorError::Shutdown`]：累加器已关闭。
    pub fn submit_with(
        &self, item: T, opts: SubmitOptions,
    ) -> Result<ReplyHandle<R>, AccumulatorError> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let deadline = opts.ttl.map(|ttl| Instant::now() + ttl);
        self.send_channel(item, deadline, Some(tx), opts.priority)?;
        Ok(ReplyHandle::new(rx))
    }

    /// 阻塞提交：队列满时等待空位，超时则返回错误。
    ///
    /// # Errors
    /// - [`AccumulatorError::Timeout`]：等待超时。
    /// - [`AccumulatorError::Shutdown`]：累加器已关闭。
    pub async fn submit_or_wait(
        &self, item: T, timeout: Duration,
    ) -> Result<ReplyHandle<R>, AccumulatorError> {
        self.blocking_inner(item, timeout, true)
            .await
            .map(|opt| opt.unwrap_or_else(|| unreachable!("blocking_inner(true) always returns Some")))
    }

    /// 阻塞发送（fire-and-forget + 阻塞等待）：队列满时等待空位。
    ///
    /// # Errors
    /// - [`AccumulatorError::Timeout`]：等待超时。
    /// - [`AccumulatorError::Shutdown`]：累加器已关闭。
    pub async fn send_or_wait(
        &self, item: T, timeout: Duration,
    ) -> Result<(), AccumulatorError> {
        self.blocking_inner(item, timeout, false).await.map(|_| ())
    }
}

/// 批累加器运行时。
///
/// 由 [`AccumulatorConfig::build`] 创建，调用 [`run`](Self::run) 启动主循环。
/// 用户通常无需直接接触此类型——通过 [`AccumulatorHandle`] 操作即可。
pub struct BatchAccumulator<T, R, P, M, C> {
    pub(crate) config: AccumulatorConfig,
    pub(crate) processor: Arc<P>,
    pub(crate) metrics: M,
    pub(crate) controller: C,
    pub(crate) item_rx: UnboundedReceiver<ChannelItem<T, R>>,
    pub(crate) bypass_rx: UnboundedReceiver<Vec<T>>,
    pub(crate) flush_notify: Arc<Notify>,
    pub(crate) buffer: VecDeque<ChannelItem<T, R>>,
    pub(crate) next_batch_id: u64,
    pub(crate) last_flush_time: Instant,
    pub(crate) pending_count: Arc<AtomicUsize>,
    pub(crate) feedback_rx: UnboundedReceiver<FlushInfo>,
    pub(crate) feedback_tx: UnboundedSender<FlushInfo>,
    pub(crate) inflight: JoinSet<()>,
    pub(crate) stats: Arc<AccumulatorStats>,
    pub(crate) weight_fn: Arc<dyn Fn(&T) -> usize + Send + Sync>,
    pub(crate) current_weight: usize,
    /// 队列空位通知：item 被消费后唤醒阻塞的 submit 等待者。
    pub(crate) queue_notify: Arc<Notify>,
    /// 共享运行时状态。
    pub(crate) shared: Arc<SharedState>,
}

impl<T, R, P, M, C> BatchAccumulator<T, R, P, M, C> {
    /// 读取当前窗口大小（从共享状态）。
    fn current_window(&self) -> Duration {
        *self.shared.current_window.read().unwrap_or_else(|e| e.into_inner())
    }

    /// 设置当前窗口大小（写入共享状态）。
    fn set_current_window(&self, w: Duration) {
        *self.shared.current_window.write().unwrap_or_else(|e| e.into_inner()) = w;
    }

    /// 并发模式的 inflight 守卫计数器（无限制或串行时返回 None）。
    fn inflight_guard(&self) -> Option<Arc<SharedState>> {
        match self.config.concurrency_mode {
            ConcurrencyMode::Concurrent { max_inflight: 0 } | ConcurrencyMode::Serial => None,
            ConcurrencyMode::Concurrent { .. } => Some(Arc::clone(&self.shared)),
        }
    }
}

impl<T, R, P, M, C> BatchAccumulator<T, R, P, M, C>
where
    T: Send + 'static,
    R: Send + 'static,
    P: BatchProcessor<T, R> + Sync,
    M: MetricsCollector,
    C: WindowController,
{
    /// 启动累加器主循环。
    pub async fn run(mut self) {
        let concurrency_mode = match self.config.concurrency_mode {
            ConcurrencyMode::Serial => "serial",
            ConcurrencyMode::Concurrent { .. } => "concurrent",
        };
        let run_span = crate::trace::run_span(
            self.config.min_window,
            self.config.max_window,
            concurrency_mode,
        );

        // Enter span for startup event — dropped before first await
        {
            let _run_guard = run_span.clone().entered();
            event_at!(
                Level::INFO,
                &self.config.tracing_level,
                "累加器启动"
            );
        }

        let mut deadline = Instant::now() + self.current_window();
        let mut running = true;

        while running {
            // 检查取消信号
            if self.shared.cancelled.load(Ordering::Acquire) {
                break;
            }
            // 非阻塞 drain bypass，限制每轮数量防止饿死 select!
            let mut bypass_count = 0;
            while bypass_count < self.config.drain_batch_limit {
                match self.bypass_rx.try_recv() {
                    Ok(items) => {
                        self.process_bypass(items).await;
                        bypass_count += 1;
                    }
                    Err(_) => break,
                }
            }

            // 非阻塞 drain feedback，防止 timer 优先导致 feedback 饥饿
            let mut feedback_count = 0;
            while feedback_count < self.config.drain_batch_limit {
                match self.feedback_rx.try_recv() {
                    Ok(info) => {
                        self.handle_feedback(info).await;
                        feedback_count += 1;
                    }
                    Err(_) => break,
                }
            }

            // 清理过期 item
            self.drain_expired();

            let sleep = tokio::time::sleep_until(deadline);

            tokio::select! {
                biased;

                // 1. 定时器优先
                _ = sleep => {
                    if !self.flush_batch().await {
                        // 并发满，短延迟后重试
                        deadline = Instant::now() + RETRY_DELAY;
                    } else {
                        deadline = Instant::now() + self.current_window();
                    }
                }
                // 2. 并发模式：后台 task 反馈
                Some(feedback) = self.feedback_rx.recv() => {
                    self.handle_feedback(feedback).await;
                }
                // 3. 手动触发 flush
                _ = self.flush_notify.notified() => {
                    if self.flush_batch().await {
                        deadline = Instant::now() + self.current_window();
                    } else {
                        // 并发满，短延迟后重试
                        deadline = Instant::now() + RETRY_DELAY;
                    }
                }
                // 4. 接收新 item
                maybe_item = self.item_rx.recv() => {
                    match maybe_item {
                        Some(mut ch) => {
                            self.pending_count.fetch_sub(1, Ordering::Release);
                            // 唤醒一个阻塞的 submit 等待者
                            self.queue_notify.notify_one();
                            // 记录队列等待时间
                            self.stats.record_wait(ch.submitted_at.elapsed());

                            // 检查刚提交的 item 是否已过期
                            if let Some(dl) = ch.deadline
                                && Instant::now() >= dl {
                                    Self::send_reply(Err(AccumulatorError::Timeout), ch.reply.take());
                                    self.stats.record_dropped_timeout(1);
                                    continue;
                                }

                            self.current_weight = self
                                .current_weight
                                .saturating_add((self.weight_fn)(&ch.value));

                            match ch.priority {
                                Priority::High => {
                                    self.buffer.push_front(ch);
                                    // 高优先级到达时触发立即 flush
                                    self.flush_notify.notify_one();
                                }
                                Priority::Normal => {
                                    self.buffer.push_back(ch);
                                }
                            }

                            if self.should_flush()
                                && self.flush_batch().await
                            {
                                deadline = Instant::now() + self.current_window();
                            }
                        }
                        None => {
                            running = false;
                        }
                    }
                }
            }
        }

        self.drain().await;

        // Enter span for shutdown event（无后续 await）
        {
            let _run_guard = run_span.entered();
            event_at!(
                Level::INFO,
                &self.config.tracing_level,
                "累加器关闭"
            );
        }
    }

    // ─── private methods ───

    /// 清空 buffer 并解构为 (items, senders, parent_spans, batch_id, batch_span, total_weight)。
    /// flush_inner 和 spawn_flush 共享此逻辑（约 35 行重复代码）。
    #[allow(clippy::type_complexity)]
    fn drain_buffer_items(
        &mut self,
    ) -> (Vec<T>, Vec<ReplySender<R>>, Vec<crate::trace::MaybeSpan>, u64, crate::trace::MaybeSpan, usize) {
        let buffer_items: Vec<ChannelItem<T, R>> = self.buffer.drain(..).collect();
        let total_weight = self.current_weight;
        self.current_weight = 0;

        let parent_spans: Vec<crate::trace::MaybeSpan> =
            buffer_items.iter().map(|bi| bi.parent_span.clone()).collect();

        let batch_id = self.next_batch_id;
        self.next_batch_id += 1;
        let queue_depth = self.pending_count.load(Ordering::Acquire);
        let batch_span = crate::trace::batch_span(batch_id, buffer_items.len(), self.current_window(), queue_depth);

        let (items, senders): (Vec<T>, Vec<ReplySender<R>>) =
            buffer_items.into_iter().map(|bi| (bi.value, bi.reply)).unzip();

        (items, senders, parent_spans, batch_id, batch_span, total_weight)
    }

    /// 窗口调整：快照 metrics → controller → clamp → 日志 → 写入共享状态。
    fn adjust_window_after_flush(&mut self) {
        let snapshot = self.metrics.snapshot();
        let new_window = self.controller.adjust_window(self.current_window(), &snapshot);
        let clamped = new_window.clamp(self.config.min_window, self.config.max_window);
        if clamped != self.current_window() {
            event_at!(Level::INFO, &self.config.tracing_level,
                prev_ms = self.current_window().as_millis() as u64,
                new_ms = clamped.as_millis() as u64, "窗口调整");
        }
        self.set_current_window(clamped);
    }

    /// 通过 spawn 隔离 processor panic。panic 时记录错误并返回空 Vec。
    async fn process_safe(tracing_level: crate::trace::TraceLevel, processor: Arc<P>, batch: Batch<T>) -> Vec<R> {
        let handle = tokio::spawn(async move { processor.process(batch).await });
        match handle.await {
            Ok(results) => results,
            Err(e) => {
                let msg = if e.is_panic() { "processor panic" } else { "processor 被取消" };
                event_at!(Level::ERROR, &tracing_level, error = msg, "processor 异常");
                Vec::new()
            }
        }
    }

    /// 发送结果到 oneshot 通道（静默忽略 receiver 已关闭）。
    fn send_reply<R2>(result: R2, tx: Option<tokio::sync::oneshot::Sender<R2>>) {
        if let Some(tx) = tx
            && tx.send(result).is_err()
        {}
    }

    /// bypass item 直接交付，不参与指标/窗口。
    async fn process_bypass(&mut self, items: Vec<T>) {
        let item_count = items.len();
        let batch_id = self.next_batch_id;
        self.next_batch_id += 1;

        let bypass_span = crate::trace::bypass_span(batch_id, item_count);

        // Enter span for startup event only — dropped before await
        {
            let _bypass_guard = bypass_span.entered();
            event_at!(
                Level::DEBUG,
                &self.config.tracing_level,
                batch_id,
                item_count,
                "bypass 处理开始"
            );
        }

        match self.config.concurrency_mode {
            ConcurrencyMode::Serial => {
                let batch = Batch::with_context(items, batch_id, Duration::ZERO, 0);
                self.processor.process(batch).await;
                event_at!(
                    Level::DEBUG,
                    &self.config.tracing_level,
                    batch_id,
                    item_count,
                    "bypass 处理完成"
                );
            }
            ConcurrencyMode::Concurrent { max_inflight } => {
                // 检查并发上限，满时回退到同步处理
                if max_inflight > 0
                    && self.shared.inflight_count.load(Ordering::Acquire) >= max_inflight
                {
                    let batch = Batch::with_context(items, batch_id, Duration::ZERO, 0);
                    self.processor.process(batch).await;
                } else {
                    self.spawn_bypass(items);
                }
            }
        }
    }

    /// 并发模式：在后台 task 中处理 bypass 批次。
    fn spawn_bypass(&mut self, items: Vec<T>) {
        let batch_id = self.next_batch_id;
        self.next_batch_id += 1;
        let item_count = items.len();
        let _tracing_level = self.config.tracing_level;
        let bypass_span = crate::trace::bypass_span(batch_id, item_count);

        let batch = Batch::with_context(items, batch_id, Duration::ZERO, 0);
        let processor = Arc::clone(&self.processor);
        // InflightGuard: 构造时 +1，drop 时 -1（含 panic）
        let guard = InflightGuard::new(self.inflight_guard());

        self.inflight.spawn(async move {
            // Enter span for start event — NOT held across await
            {
                let _bypass_guard = bypass_span.clone().entered();
                event_at!(
                    Level::DEBUG,
                    &_tracing_level,
                    batch_id,
                    item_count,
                    "bypass 处理开始（并发）"
                );
            }

            processor.process(batch).await;

            // Re-enter span for completion event
            {
                let _bypass_guard = bypass_span.entered();
                event_at!(
                    Level::DEBUG,
                    &_tracing_level,
                    batch_id,
                    item_count,
                    "bypass 处理完成（并发）"
                );
            }

            drop(guard); // 显式 drop，确保 panic 时也释放
        });
    }

    /// 并发模式：在后台 task 中处理批次。
    fn spawn_flush(&mut self, time_since_last_flush: Duration) {
        let (items, senders, parent_spans, batch_id, batch_span, total_weight) = self.drain_buffer_items();
        let batch_size = items.len();

        let _tracing_level = self.config.tracing_level;
        let trace_per_item = self.config.trace_per_item;
        let batch = Batch::with_context(items, batch_id, self.current_window(), 0);
        let processor = Arc::clone(&self.processor);
        let feedback_tx = self.feedback_tx.clone();
        let pending_count = Arc::clone(&self.pending_count);
        let max_batch_size = self.config.max_batch_size;
        let max_batch_weight = self.config.max_batch_weight;
        let window_duration = self.current_window();
        // InflightGuard: 构造时 +1，drop 时 -1（含 panic）
        let guard = InflightGuard::new(self.inflight_guard());

        self.inflight.spawn(async move {
            let start = Instant::now();
            let items_remaining;

            // Enter span for start event — NOT held across await
            {
                let _batch_guard = batch_span.clone().entered();
                event_at!(
                    Level::INFO,
                    &_tracing_level,
                    batch_id,
                    batch_size,
                    "批次处理开始（并发）"
                );
            }

            let results = processor.process(batch).await;
            let execution_time = start.elapsed();

            // Re-enter span for per-item processing and completion event
            {
                let _batch_guard = batch_span.entered();

                // 长度校验：processor 约定返回与 batch 等长的结果
                debug_assert_eq!(
                    senders.len(),
                    results.len(),
                    "BatchProcessor::process 返回 Vec 长度应与 batch 长度一致"
                );

                // 逐个发送结果 + per-item span
                for (index, (sender, result)) in senders
                    .into_iter()
                    .zip(results)
                    .enumerate()
                {
                    let _item_guard = if trace_per_item {
                        let span = crate::trace::item_span(index);
                        if index < parent_spans.len() {
                            span.follows_from(parent_spans[index].clone());
                        }
                        Some(span.entered())
                    } else {
                        None
                    };

                    Self::send_reply(Ok(result), sender);
                }

                items_remaining = pending_count.load(Ordering::Acquire);

                event_at!(
                    Level::INFO,
                    &_tracing_level,
                    batch_id,
                    batch_size,
                    execution_time_ms = execution_time.as_millis() as u64,
                    items_remaining,
                    "批次处理完成（并发）"
                );
            }

            let info = FlushInfo {
                batch_size,
                max_batch_size,
                window_duration,
                items_remaining,
                batch_id,
                execution_time,
                time_since_last_flush,
                total_weight: max_batch_weight.map(|_| total_weight),
            };

            // 先递减 inflight_count 再发送 feedback，避免竞态
            drop(guard);
            if feedback_tx.send(info).is_err() {
                // 主循环已关闭，feedback 通道断开
            }
        });
    }

    /// flush buffer 并根据并发模式分发。
    ///
    /// 返回 `true` 表示已执行 flush（或空批次跳过），`false` 表示并发满被跳过。
    async fn flush_batch(&mut self) -> bool {
        // paused 时跳过 timer 触发的空批次，但允许手动 flush
        if self.buffer.is_empty()
            && (!self.config.flush_empty_batches || self.shared.paused.load(Ordering::Acquire))
        {
            self.last_flush_time = Instant::now();
            return true;
        }

        match self.config.concurrency_mode {
            ConcurrencyMode::Serial => {
                let time_since_last_flush = self.last_flush_time.elapsed();
                self.flush_inner(time_since_last_flush).await;

                self.adjust_window_after_flush();
                true
            }
            ConcurrencyMode::Concurrent { max_inflight } => {
                if max_inflight > 0
                    && self.shared.inflight_count.load(Ordering::Acquire) >= max_inflight
                {
                    event_at!(
                        Level::WARN,
                        &self.config.tracing_level,
                        max_inflight,
                        inflight = self.shared.inflight_count.load(Ordering::Acquire),
                        buffered = self.buffer.len(),
                        "并发满，跳过本次 flush"
                    );
                    return false;
                }
                let time_since_last_flush = self.last_flush_time.elapsed();
                self.spawn_flush(time_since_last_flush);
                // last_flush_time 在 handle_feedback 中更新
                true
            }
        }
    }

    /// flush 内核心逻辑（串行模式）：处理 buffer → FlushInfo → 记录指标。
    async fn flush_inner(&mut self, time_since_last_flush: Duration) {
        let (items, senders, parent_spans, batch_id, batch_span, total_weight) = self.drain_buffer_items();
        let batch_size = items.len();

        {
            let _batch_guard = batch_span.clone().entered();
            event_at!(Level::INFO, &self.config.tracing_level, batch_id, batch_size, "批次处理开始");
        }

        let batch = Batch::with_context(items, batch_id, self.current_window(), 0);
        let proc_start = Instant::now();
        let results = Self::process_safe(self.config.tracing_level, Arc::clone(&self.processor), batch).await;
        let execution_time = proc_start.elapsed();

        // 长度校验：processor 约定返回与 batch 等长的结果
        debug_assert_eq!(
            senders.len(),
            results.len(),
            "BatchProcessor::process 返回 Vec 长度应与 batch 长度一致"
        );

        let items_remaining = self.pending_count.load(Ordering::Acquire);

        // 发送结果给调用方（在 batch span 内，以便 per-item spans 作为子 span）
        {
            let _batch_guard = batch_span.entered();

            for (index, (sender, result)) in senders.into_iter().zip(results).enumerate() {
            let _item_guard = if self.config.trace_per_item {
                let span = crate::trace::item_span(index);
                if index < parent_spans.len() { span.follows_from(parent_spans[index].clone()); }
                Some(span.entered())
            } else { None };
            Self::send_reply(Ok(result), sender);
        }

        event_at!(
            Level::INFO,
            &self.config.tracing_level,
            batch_id = batch_id,
            batch_size = batch_size,
            execution_time_ms = execution_time.as_millis() as u64,
            items_remaining = items_remaining,
            window_ms = self.current_window().as_millis() as u64,
            "批次处理完成"
        );
        }

        let info = FlushInfo {
            batch_size,
            max_batch_size: self.config.max_batch_size,
            window_duration: self.current_window(),
            items_remaining,
            batch_id,
            execution_time,
            time_since_last_flush,
            total_weight: self.config.max_batch_weight.map(|_| total_weight),
        };

        self.metrics.record_flush(&info);
        self.stats.record_flush(execution_time);

        self.last_flush_time = Instant::now();
    }

    /// 处理并发模式下的后台任务反馈。
    async fn handle_feedback(&mut self, info: FlushInfo) {
        self.metrics.record_flush(&info);
        self.stats.record_flush(info.execution_time);
        self.last_flush_time = Instant::now();
        self.adjust_window_after_flush();

        // 若 buffer 有积压，通知主循环 flush
        if !self.buffer.is_empty() {
            self.flush_notify.notify_one();
        }
    }

    /// 清理缓冲区中的过期 item。
    fn drain_expired(&mut self) {
        let now = Instant::now();
        let mut dropped_count = 0u64;

        // 收集未过期的 item，发送超时错误给过期的
        let mut kept = VecDeque::with_capacity(self.buffer.len());
        while let Some(mut item) = self.buffer.pop_front() {
            let expired = item.deadline.is_some_and(|d| now >= d);
            if expired {
                Self::send_reply(Err(AccumulatorError::Timeout), item.reply.take());
                self.current_weight = self
                    .current_weight
                    .saturating_sub((self.weight_fn)(&item.value));
                dropped_count += 1;
            } else {
                kept.push_back(item);
            }
        }
        self.buffer = kept;

        if dropped_count > 0 {
            self.stats.record_dropped_timeout(dropped_count);
            event_at!(
                Level::WARN,
                &self.config.tracing_level,
                dropped = dropped_count,
                "item 超时丢弃"
            );
        }
    }

    /// 判断是否应该触发 flush（基于 item 数或权重）。
    fn should_flush(&self) -> bool {
        if let Some(max) = self.config.max_batch_size
            && self.buffer.len() >= max
        {
            return true;
        }
        if let Some(max_weight) = self.config.max_batch_weight
            && self.current_weight >= max_weight
        {
            return true;
        }
        false
    }

    /// 通道关闭后清空所有 item（包括 bypass 通道和 inflight task）。
    async fn drain(&mut self) {
        let remaining = self.pending_count.load(Ordering::Acquire) + self.buffer.len();
        let drain_span = crate::trace::drain_span(remaining);

        // Enter span for startup event only — dropped before await
        {
            let _drain_guard = drain_span.entered();
            event_at!(
                Level::INFO,
                &self.config.tracing_level,
                remaining_items = remaining,
                "开始清空累加器"
            );
        }

        // 清空主通道
        while let Some(mut ch) = self.item_rx.recv().await {
            self.pending_count.fetch_sub(1, Ordering::Release);
            // 唤醒一个阻塞的 submit 等待者
            self.queue_notify.notify_one();

            // 跳过已过期 item
            if let Some(dl) = ch.deadline
                && Instant::now() >= dl {
                    Self::send_reply(Err(AccumulatorError::Timeout), ch.reply.take());
                    self.stats.record_dropped_timeout(1);
                    event_at!(
                        Level::WARN,
                        &self.config.tracing_level,
                        dropped = 1u64,
                        "drain 阶段 item 超时丢弃"
                    );
                    continue;
                }

            self.current_weight = self
                .current_weight
                .saturating_add((self.weight_fn)(&ch.value));

            self.buffer.push_back(ChannelItem {
                value: ch.value,
                deadline: ch.deadline,
                reply: ch.reply,
                priority: ch.priority,
                parent_span: ch.parent_span,
                submitted_at: ch.submitted_at,
            });
        }

        match self.config.concurrency_mode {
            ConcurrencyMode::Serial => {
                // 清空 bypass 通道
                while let Ok(items) = self.bypass_rx.try_recv() {
                    self.process_bypass(items).await;
                }
                // 处理最后一批（非空或开启了空批次）
                if !self.buffer.is_empty() || self.config.flush_empty_batches {
                    let time_since_last_flush = self.last_flush_time.elapsed();
                    self.flush_inner(time_since_last_flush).await;
                }
            }
            ConcurrencyMode::Concurrent { .. } => {
                // 清空 bypass 通道（并发处理）
                while let Ok(items) = self.bypass_rx.try_recv() {
                    self.spawn_bypass(items);
                }
                // 处理 buffer 最后一批（并发处理）
                if !self.buffer.is_empty() || self.config.flush_empty_batches {
                    let time_since_last_flush = self.last_flush_time.elapsed();
                    self.spawn_flush(time_since_last_flush);
                }
                // 等待所有 inflight task 完成（支持超时）
                self.join_inflight_with_timeout().await;
                // 处理剩余的 feedback，使用 handle_feedback 完整走指标+窗口流程
                while let Ok(info) = self.feedback_rx.try_recv() {
                    self.handle_feedback(info).await;
                }
            }
        }

        // 唤醒所有阻塞等待的 submit，让它们获取 Shutdown 错误
        self.queue_notify.notify_waiters();
    }

    /// 等待所有 inflight task 完成，支持 drain_timeout 超时。
    ///
    /// 超时后自动 abort 剩余 task 并记录日志。
    async fn join_inflight_with_timeout(&mut self) {
        let join_all = async {
            while let Some(result) = self.inflight.join_next().await {
                Self::log_inflight_error(result, &self.config.tracing_level);
            }
        };

        if let Some(timeout) = self.config.drain_timeout {
            if tokio::time::timeout(timeout, join_all).await.is_err() {
                self.inflight.abort_all();
                event_at!(
                    Level::WARN,
                    &self.config.tracing_level,
                    timeout_ms = timeout.as_millis() as u64,
                    "drain 超时，中止剩余 inflight task"
                );
                // 清理被中止的 task 结果
                while let Some(result) = self.inflight.join_next().await {
                    Self::log_inflight_error(result, &self.config.tracing_level);
                }
            }
        } else {
            join_all.await;
        }
    }

    /// 记录 inflight task 的异常退出（panic 或取消）。
    fn log_inflight_error(
        result: Result<(), tokio::task::JoinError>,
        tracing_level: &crate::trace::TraceLevel,
    ) {
        if let Err(e) = result {
            let msg = if e.is_panic() {
                "后台批处理 task panic"
            } else {
                "后台批处理 task 被取消"
            };
            event_at!(Level::ERROR, tracing_level, error = msg, "后台 task 异常退出");
        }
    }
}
