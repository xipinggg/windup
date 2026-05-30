use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::Notify;
use tokio::task::JoinSet;
use tokio::time::Instant;

use crate::batch::{Batch, BatchProcessor, FlushInfo, ReplyHandle};
use crate::config::{AccumulatorConfig, ConcurrencyMode};
use crate::controller::WindowController;
use crate::error::AccumulatorError;
use crate::metrics::{MetricsCollector, BYPASS_DRAIN_LIMIT};

/// 并发满后重试 flush 的间隔。
const RETRY_DELAY: Duration = Duration::from_millis(10);

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
pub(crate) enum ChannelItem<T, R> {
    /// fire-and-forget（submit）
    FireAndForget(T),
    /// 带回复通道（submit_with_reply）
    WithReply(T, tokio::sync::oneshot::Sender<R>),
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
}

impl<T: Send, R: Send> Clone for AccumulatorHandle<T, R> {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
            bypass_sender: self.bypass_sender.clone(),
            pending_count: Arc::clone(&self.pending_count),
            max_queue_depth: self.max_queue_depth,
            flush_notify: Arc::clone(&self.flush_notify),
        }
    }
}

impl<T: Send, R: Send> AccumulatorHandle<T, R> {
    /// 提交一个 item 到累加器（fire-and-forget，不关心结果）。
    ///
    /// # Errors
    ///
    /// - [`AccumulatorError::QueueFull`]：超过 `max_queue_depth` 限制。
    /// - [`AccumulatorError::Shutdown`]：累加器已关闭。
    pub fn submit(&self, item: T) -> Result<(), AccumulatorError> {
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
            .send(ChannelItem::FireAndForget(item))
            .map_err(|_| {
                self.pending_count.fetch_sub(1, Ordering::Release);
                AccumulatorError::Shutdown
            })
    }

    /// 提交一个 item 并返回 [`ReplyHandle`]，`.await` 后拿到处理结果。
    ///
    /// # Errors
    ///
    /// - [`AccumulatorError::QueueFull`]：超过 `max_queue_depth` 限制。
    /// - [`AccumulatorError::Shutdown`]：累加器已关闭。
    pub fn submit_with_reply(&self, item: T) -> Result<ReplyHandle<R>, AccumulatorError> {
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
            .send(ChannelItem::WithReply(item, tx))
            .map_err(|_| {
                self.pending_count.fetch_sub(1, Ordering::Release);
                AccumulatorError::Shutdown
            })?;

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
            .map_err(|_| AccumulatorError::Shutdown)
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
    pub(crate) buffer: Vec<(T, Option<tokio::sync::oneshot::Sender<R>>)>,
    pub(crate) current_window: Duration,
    pub(crate) next_batch_id: u64,
    pub(crate) last_flush_time: Instant,
    pub(crate) pending_count: Arc<AtomicUsize>,
    pub(crate) feedback_rx: UnboundedReceiver<FlushInfo>,
    pub(crate) feedback_tx: UnboundedSender<FlushInfo>,
    pub(crate) inflight: JoinSet<()>,
    pub(crate) inflight_count: Arc<AtomicUsize>,
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
                        Some(channel_item) => {
                            self.pending_count.fetch_sub(1, Ordering::Release);
                            match channel_item {
                                ChannelItem::FireAndForget(item) => {
                                    self.buffer.push((item, None));
                                }
                                ChannelItem::WithReply(item, tx) => {
                                    self.buffer.push((item, Some(tx)));
                                }
                            }

                            if let Some(max) = self.config.max_batch_size
                                && self.buffer.len() >= max
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
        let buffer_items = std::mem::take(&mut self.buffer);
        let (items, senders): (Vec<T>, Vec<Option<tokio::sync::oneshot::Sender<R>>>) =
            buffer_items.into_iter().unzip();

        let batch_size = items.len();
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
                    let _ = tx.send(result);
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
        let buffer_items = std::mem::take(&mut self.buffer);
        let (items, senders): (Vec<T>, Vec<Option<tokio::sync::oneshot::Sender<R>>>) =
            buffer_items.into_iter().unzip();

        let batch_size = items.len();
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
                let _ = tx.send(result);
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
        };

        self.metrics.record_flush(&info).await;

        self.last_flush_time = Instant::now();
    }

    /// 处理并发模式下的后台任务反馈。
    async fn handle_feedback(&mut self, info: FlushInfo) {
        self.metrics.record_flush(&info).await;
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

    /// 通道关闭后清空所有 item（包括 bypass 通道和 inflight task）。
    async fn drain(&mut self) {
        // 清空主通道
        while let Some(channel_item) = self.item_rx.recv().await {
            self.pending_count.fetch_sub(1, Ordering::Release);
            match channel_item {
                ChannelItem::FireAndForget(item) => {
                    self.buffer.push((item, None));
                }
                ChannelItem::WithReply(item, tx) => {
                    self.buffer.push((item, Some(tx)));
                }
            }
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
