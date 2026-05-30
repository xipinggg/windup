use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::Notify;
use tokio::task::JoinSet;
use tokio::time::Instant;

use crate::batch::{Batch, BatchProcessor, FlushInfo, Priority, ReplyHandle, SubmitOptions};
use crate::config::{AccumulatorConfig, ConcurrencyMode};
use crate::controller::WindowController;
use crate::error::AccumulatorError;
use crate::metrics::{MetricsCollector, BYPASS_DRAIN_LIMIT};
use crate::stats::{AccumulatorStats, StatsSnapshot};

use std::collections::VecDeque;

/// 并发满后重试 flush 的间隔。
const RETRY_DELAY: Duration = Duration::from_millis(10);

/// Reply sender 类型别名，简化复杂类型签名。
type ReplySender<R> = Option<tokio::sync::oneshot::Sender<Result<R, AccumulatorError>>>;

/// Drop 时自动递减 inflight_count，确保 panic 也不会泄漏计数。
struct InflightGuard {
    count: Arc<AtomicUsize>,
}

impl InflightGuard {
    fn new(count: Arc<AtomicUsize>) -> Self {
        count.fetch_add(1, Ordering::Release);
        Self { count }
    }
}

impl Drop for InflightGuard {
    fn drop(&mut self) {
        self.count.fetch_sub(1, Ordering::Release);
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
}

/// 缓冲区中暂存的 item。
pub(crate) struct BufferItem<T, R> {
    /// item 数据。
    value: T,
    /// 超时截止时间。`None` 表示不超时。
    deadline: Option<Instant>,
    /// 回复通道。`None` 表示 fire-and-forget。
    reply: Option<tokio::sync::oneshot::Sender<Result<R, AccumulatorError>>>,
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
        }
    }
}

impl<T: Send, R: Send> AccumulatorHandle<T, R> {
    /// 提交一个 item 到累加器（fire-and-forget，不关心结果）。
    ///
    /// 如果你需要获取处理结果，请使用 [`submit`](Self::submit)。
    ///
    /// # Errors
    ///
    /// - [`AccumulatorError::QueueFull`]：超过 `max_queue_depth` 限制。
    /// - [`AccumulatorError::Shutdown`]：累加器已关闭。
    pub fn submit_no_wait(&self, item: T) -> Result<(), AccumulatorError> {
        let result = self.pending_count.fetch_update(
            Ordering::AcqRel,
            Ordering::Acquire,
            |pending| {
                if let Some(max) = self.max_queue_depth
                    && pending >= max
                {
                    return None;
                }
                Some(pending + 1)
            },
        );

        if let Err(prev) = result {
            return Err(AccumulatorError::QueueFull {
                max: self.max_queue_depth.unwrap_or(0),
                pending: prev,
            });
        }

        self.sender
            .send(ChannelItem {
                value: item,
                deadline: None,
                reply: None,
                priority: Priority::Normal,
            })
            .map_err(|_| {
                self.pending_count.fetch_sub(1, Ordering::Release);
                AccumulatorError::Shutdown
            })?;

        self.stats.record_submit();
        Ok(())
    }

    /// 提交一个 item，返回 [`ReplyHandle`]，`.await` 后拿到处理结果。
    ///
    /// 如果不需要结果，可使用 [`submit_no_wait`](Self::submit_no_wait)（fire-and-forget）。
    ///
    /// # Errors
    ///
    /// - [`AccumulatorError::QueueFull`]：超过 `max_queue_depth` 限制。
    /// - [`AccumulatorError::Shutdown`]：累加器已关闭。
    pub fn submit(&self, item: T) -> Result<ReplyHandle<R>, AccumulatorError> {
        let result = self.pending_count.fetch_update(
            Ordering::AcqRel,
            Ordering::Acquire,
            |pending| {
                if let Some(max) = self.max_queue_depth
                    && pending >= max
                {
                    return None;
                }
                Some(pending + 1)
            },
        );

        if let Err(prev) = result {
            return Err(AccumulatorError::QueueFull {
                max: self.max_queue_depth.unwrap_or(0),
                pending: prev,
            });
        }

        let (tx, rx) = tokio::sync::oneshot::channel();

        self.sender
            .send(ChannelItem {
                value: item,
                deadline: None,
                reply: Some(tx),
                priority: Priority::Normal,
            })
            .map_err(|_| {
                self.pending_count.fetch_sub(1, Ordering::Release);
                AccumulatorError::Shutdown
            })?;

        self.stats.record_submit();
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
        self.stats.snapshot(
            self.pending_count(),
            0, // buffer_size 由 Accumulator 侧提供才有准确值
            0, // inflight_count
            0, // current_weight
        )
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
        &self,
        item: T,
        opts: SubmitOptions,
    ) -> Result<ReplyHandle<R>, AccumulatorError> {
        let result = self.pending_count.fetch_update(
            Ordering::AcqRel,
            Ordering::Acquire,
            |pending| {
                if let Some(max) = self.max_queue_depth
                    && pending >= max
                {
                    return None;
                }
                Some(pending + 1)
            },
        );

        if let Err(prev) = result {
            return Err(AccumulatorError::QueueFull {
                max: self.max_queue_depth.unwrap_or(0),
                pending: prev,
            });
        }

        let (tx, rx) = tokio::sync::oneshot::channel();
        let deadline = opts.ttl.map(|ttl| Instant::now() + ttl);

        self.sender
            .send(ChannelItem {
                value: item,
                deadline,
                reply: Some(tx),
                priority: opts.priority,
            })
            .map_err(|_| {
                self.pending_count.fetch_sub(1, Ordering::Release);
                AccumulatorError::Shutdown
            })?;

        self.stats.record_submit();
        Ok(ReplyHandle::new(rx))
    }

    /// 提交高优先级 item 并等待结果。等价于
    /// `submit_with(item, SubmitOptions { priority: Priority::High, ..Default::default() })`。
    pub fn submit_high(&self, item: T) -> Result<ReplyHandle<R>, AccumulatorError> {
        self.submit_with(
            item,
            SubmitOptions {
                priority: Priority::High,
                ..Default::default()
            },
        )
    }

    /// 提交 item 并设置超时。等价于
    /// `submit_with(item, SubmitOptions { ttl: Some(ttl), ..Default::default() })`。
    pub fn submit_with_timeout(
        &self,
        item: T,
        ttl: Duration,
    ) -> Result<ReplyHandle<R>, AccumulatorError> {
        self.submit_with(
            item,
            SubmitOptions {
                ttl: Some(ttl),
                ..Default::default()
            },
        )
    }
}

/// 批累加器核心。
pub struct BatchAccumulator<T, R, P, M, C> {
    pub(crate) config: AccumulatorConfig,
    pub(crate) processor: Arc<P>,
    pub(crate) metrics: M,
    pub(crate) controller: C,
    pub(crate) item_rx: UnboundedReceiver<ChannelItem<T, R>>,
    pub(crate) bypass_rx: UnboundedReceiver<Vec<T>>,
    pub(crate) flush_notify: Arc<Notify>,
    pub(crate) buffer: VecDeque<BufferItem<T, R>>,
    pub(crate) current_window: Duration,
    pub(crate) next_batch_id: u64,
    pub(crate) last_flush_time: Instant,
    pub(crate) pending_count: Arc<AtomicUsize>,
    pub(crate) feedback_rx: UnboundedReceiver<FlushInfo>,
    pub(crate) feedback_tx: UnboundedSender<FlushInfo>,
    pub(crate) inflight: JoinSet<()>,
    pub(crate) inflight_count: Arc<AtomicUsize>,
    pub(crate) stats: Arc<AccumulatorStats>,
    pub(crate) weight_fn: Arc<dyn Fn(&T) -> usize + Send + Sync>,
    pub(crate) current_weight: usize,
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
        let mut deadline = Instant::now() + self.current_window;
        let mut running = true;

        while running {
            // 非阻塞 drain bypass，限制每轮数量防止饿死 select!
            let mut bypass_count = 0;
            while bypass_count < BYPASS_DRAIN_LIMIT {
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
            while feedback_count < BYPASS_DRAIN_LIMIT {
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
                        deadline = Instant::now() + self.current_window;
                    }
                }
                // 2. 并发模式：后台 task 反馈
                Some(feedback) = self.feedback_rx.recv() => {
                    self.handle_feedback(feedback).await;
                }
                // 3. 手动触发 flush
                _ = self.flush_notify.notified() => {
                    if self.flush_batch().await {
                        deadline = Instant::now() + self.current_window;
                    } else {
                        // 并发满，短延迟后重试
                        deadline = Instant::now() + RETRY_DELAY;
                    }
                }
                // 4. 接收新 item
                maybe_item = self.item_rx.recv() => {
                    match maybe_item {
                        Some(ch) => {
                            self.pending_count.fetch_sub(1, Ordering::Release);

                            // 检查刚提交的 item 是否已过期
                            if let Some(dl) = ch.deadline
                                && Instant::now() >= dl {
                                    if let Some(tx) = ch.reply {
                                        let _ = tx.send(Err(AccumulatorError::Timeout));
                                    }
                                    self.stats.record_dropped_timeout(1);
                                    continue;
                                }

                            let buffer_item = BufferItem {
                                value: ch.value,
                                deadline: ch.deadline,
                                reply: ch.reply,
                            };

                            self.current_weight = self
                                .current_weight
                                .saturating_add((self.weight_fn)(&buffer_item.value));

                            match ch.priority {
                                Priority::High => {
                                    self.buffer.push_front(buffer_item);
                                    // 高优先级到达时触发立即 flush
                                    self.flush_notify.notify_one();
                                }
                                Priority::Normal => {
                                    self.buffer.push_back(buffer_item);
                                }
                            }

                            if self.should_flush()
                                && self.flush_batch().await
                            {
                                deadline = Instant::now() + self.current_window;
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
    }

    // ─── private methods ───

    /// bypass item 直接交付，不参与指标/窗口。
    async fn process_bypass(&mut self, items: Vec<T>) {
        match self.config.concurrency_mode {
            ConcurrencyMode::Serial => {
                let batch_id = self.next_batch_id;
                self.next_batch_id += 1;
                let batch = Batch::new(items, batch_id);
                self.processor.process(batch).await;
            }
            ConcurrencyMode::Concurrent { max_inflight } => {
                // 检查并发上限，满时回退到同步处理
                if max_inflight > 0
                    && self.inflight_count.load(Ordering::Acquire) >= max_inflight
                {
                    let batch_id = self.next_batch_id;
                    self.next_batch_id += 1;
                    let batch = Batch::new(items, batch_id);
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
        let batch = Batch::new(items, batch_id);
        let processor = Arc::clone(&self.processor);
        let guard = InflightGuard::new(Arc::clone(&self.inflight_count));

        self.inflight.spawn(async move {
            processor.process(batch).await;
            drop(guard); // 显式 drop，确保 panic 时也释放
        });
    }

    /// 并发模式：在后台 task 中处理批次。
    fn spawn_flush(&mut self, time_since_last_flush: Duration) {
        let buffer_items: Vec<BufferItem<T, R>> = self.buffer.drain(..).collect();
        let batch_size = buffer_items.len();
        let total_weight = self.current_weight;
        self.current_weight = 0;
        let (items, senders): (Vec<T>, Vec<ReplySender<R>>) =
            buffer_items.into_iter().map(|bi| (bi.value, bi.reply)).unzip();

        let batch_id = self.next_batch_id;
        self.next_batch_id += 1;

        let batch = Batch::new(items, batch_id);
        let processor = Arc::clone(&self.processor);
        let feedback_tx = self.feedback_tx.clone();
        let pending_count = Arc::clone(&self.pending_count);
        let max_batch_size = self.config.max_batch_size;
        let window_duration = self.current_window;
        // InflightGuard: 构造时 +1，drop 时 -1（含 panic）
        let guard = InflightGuard::new(Arc::clone(&self.inflight_count));

        self.inflight.spawn(async move {
            let start = Instant::now();
            let results = processor.process(batch).await;
            let execution_time = start.elapsed();

            // 长度校验：processor 约定返回与 batch 等长的结果
            debug_assert_eq!(
                senders.len(),
                results.len(),
                "BatchProcessor::process 返回 Vec 长度应与 batch 长度一致"
            );

            // 逐个发送结果给调用方 ReplyHandle
            for (sender, result) in senders
                .into_iter()
                .zip(results)
            {
                if let Some(tx) = sender {
                    let _ = tx.send(Ok(result));
                }
            }

            let items_remaining = pending_count.load(Ordering::Acquire);

            let info = FlushInfo {
                batch_size,
                max_batch_size,
                window_duration,
                items_remaining,
                batch_id,
                execution_time,
                time_since_last_flush,
                total_weight: max_batch_size.map(|_| total_weight),
            };

            // 先递减 inflight_count 再发送 feedback，避免竞态
            drop(guard);
            let _ = feedback_tx.send(info);
        });
    }

    /// flush buffer 并根据并发模式分发。
    ///
    /// 返回 `true` 表示已执行 flush（或空批次跳过），`false` 表示并发满被跳过。
    async fn flush_batch(&mut self) -> bool {
        if self.buffer.is_empty() && !self.config.flush_empty_batches {
            self.last_flush_time = Instant::now();
            return true;
        }

        match self.config.concurrency_mode {
            ConcurrencyMode::Serial => {
                let time_since_last_flush = self.last_flush_time.elapsed();
                self.flush_inner(time_since_last_flush).await;

                let snapshot = self.metrics.snapshot();
                self.current_window = self
                    .controller
                    .adjust_window(self.current_window, &snapshot)
                    .await;
                self.current_window = self
                    .current_window
                    .clamp(self.config.min_window, self.config.max_window);
                true
            }
            ConcurrencyMode::Concurrent { max_inflight } => {
                if max_inflight > 0
                    && self.inflight_count.load(Ordering::Acquire) >= max_inflight
                {
                    // 并发满，跳过本次 flush，items 保留在 buffer 中
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
        let buffer_items: Vec<BufferItem<T, R>> = self.buffer.drain(..).collect();

        let batch_size = buffer_items.len();
        let total_weight = self.current_weight;
        self.current_weight = 0;
        let (items, senders): (Vec<T>, Vec<ReplySender<R>>) =
            buffer_items.into_iter().map(|bi| (bi.value, bi.reply)).unzip();
        let batch_id = self.next_batch_id;
        self.next_batch_id += 1;

        let batch = Batch::new(items, batch_id);
        let proc_start = Instant::now();
        let results = self.processor.process(batch).await;
        let execution_time = proc_start.elapsed();

        // 长度校验：processor 约定返回与 batch 等长的结果
        debug_assert_eq!(
            senders.len(),
            results.len(),
            "BatchProcessor::process 返回 Vec 长度应与 batch 长度一致"
        );

        // 发送结果给调用方
        for (sender, result) in senders.into_iter().zip(results) {
            if let Some(tx) = sender {
                let _ = tx.send(Ok(result));
            }
        }

        let items_remaining = self.pending_count.load(Ordering::Acquire);

        let info = FlushInfo {
            batch_size,
            max_batch_size: self.config.max_batch_size,
            window_duration: self.current_window,
            items_remaining,
            batch_id,
            execution_time,
            time_since_last_flush,
            total_weight: self.config.max_batch_weight.map(|_| total_weight),
        };

        self.metrics.record_flush(&info).await;
        self.stats.record_flush(execution_time);

        self.last_flush_time = Instant::now();
    }

    /// 处理并发模式下的后台任务反馈。
    async fn handle_feedback(&mut self, info: FlushInfo) {
        self.metrics.record_flush(&info).await;
        self.stats.record_flush(info.execution_time);
        self.last_flush_time = Instant::now();

        let snapshot = self.metrics.snapshot();
        self.current_window = self
            .controller
            .adjust_window(self.current_window, &snapshot)
            .await;
        self.current_window = self
            .current_window
            .clamp(self.config.min_window, self.config.max_window);

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
        while let Some(item) = self.buffer.pop_front() {
            let expired = item.deadline.is_some_and(|d| now >= d);
            if expired {
                if let Some(tx) = item.reply {
                    let _ = tx.send(Err(AccumulatorError::Timeout));
                }
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
        // 清空主通道
        while let Some(ch) = self.item_rx.recv().await {
            self.pending_count.fetch_sub(1, Ordering::Release);

            // 跳过已过期 item
            if let Some(dl) = ch.deadline
                && Instant::now() >= dl {
                    if let Some(tx) = ch.reply {
                        let _ = tx.send(Err(AccumulatorError::Timeout));
                    }
                    self.stats.record_dropped_timeout(1);
                    continue;
                }

            self.current_weight = self
                .current_weight
                .saturating_add((self.weight_fn)(&ch.value));

            self.buffer.push_back(BufferItem {
                value: ch.value,
                deadline: ch.deadline,
                reply: ch.reply,
            });
        }

        match self.config.concurrency_mode {
            ConcurrencyMode::Serial => {
                // 清空 bypass 通道
                while let Ok(items) = self.bypass_rx.try_recv() {
                    self.process_bypass(items).await;
                }
                // 处理最后一批
                if self.buffer.is_empty() && !self.config.flush_empty_batches {
                    return;
                }
                let time_since_last_flush = self.last_flush_time.elapsed();
                self.flush_inner(time_since_last_flush).await;
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
                // 等待所有 inflight task 完成
                while let Some(result) = self.inflight.join_next().await {
                    if let Err(e) = result {
                        // task panic：记录日志，不 panic（shutdown 阶段尽力而为）
                        let msg = if e.is_panic() {
                            "后台批处理 task panic"
                        } else {
                            "后台批处理 task 被取消"
                        };
                        eprintln!("[draft] drain: {msg}");
                    }
                }
                // 处理剩余的 feedback，使用 handle_feedback 完整走指标+窗口流程
                while let Ok(info) = self.feedback_rx.try_recv() {
                    self.handle_feedback(info).await;
                }
            }
        }
    }
}
