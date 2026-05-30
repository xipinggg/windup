use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::Notify;
use tokio::time::Instant;

use crate::batch::{Batch, BatchProcessor, FlushInfo};
use crate::config::AccumulatorConfig;
use crate::controller::WindowController;
use crate::error::AccumulatorError;
use crate::metrics::{MetricsCollector, BYPASS_DRAIN_LIMIT};

/// 向累加器提交 item 的句柄。
///
/// 可安全克隆，跨 task 共享。
pub struct AccumulatorHandle<T> {
    pub(crate) sender: UnboundedSender<T>,
    pub(crate) bypass_sender: UnboundedSender<Vec<T>>,
    pub(crate) pending_count: Arc<AtomicUsize>,
    pub(crate) max_queue_depth: Option<usize>,
    pub(crate) flush_notify: Arc<Notify>,
}

impl<T: Send> Clone for AccumulatorHandle<T> {
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

impl<T: Send> AccumulatorHandle<T> {
    /// 提交一个 item 到累加器。
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

        self.sender.send(item).map_err(|_| {
            self.pending_count.fetch_sub(1, Ordering::Release);
            AccumulatorError::Shutdown
        })
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
pub struct BatchAccumulator<T, P, M, C> {
    pub(crate) config: AccumulatorConfig,
    pub(crate) processor: P,
    pub(crate) metrics: M,
    pub(crate) controller: C,
    pub(crate) item_rx: UnboundedReceiver<T>,
    pub(crate) bypass_rx: UnboundedReceiver<Vec<T>>,
    pub(crate) flush_notify: Arc<Notify>,
    pub(crate) buffer: Vec<T>,
    pub(crate) current_window: Duration,
    pub(crate) next_batch_id: u64,
    pub(crate) last_flush_time: Instant,
    pub(crate) pending_count: Arc<AtomicUsize>,
}

impl<T, P, M, C> BatchAccumulator<T, P, M, C>
where
    T: Send + 'static,
    P: BatchProcessor<T>,
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

            let sleep = tokio::time::sleep_until(deadline);

            tokio::select! {
                biased;

                _ = sleep => {
                    self.flush_batch().await;
                    deadline = Instant::now() + self.current_window;
                }
                _ = self.flush_notify.notified() => {
                    self.flush_batch().await;
                    deadline = Instant::now() + self.current_window;
                }
                maybe_item = self.item_rx.recv() => {
                    match maybe_item {
                        Some(item) => {
                            self.pending_count.fetch_sub(1, Ordering::Release);
                            self.buffer.push(item);

                            if let Some(max) = self.config.max_batch_size
                                && self.buffer.len() >= max
                            {
                                self.flush_batch().await;
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
        let batch_id = self.next_batch_id;
        self.next_batch_id += 1;
        let batch = Batch::new(items, batch_id);
        self.processor.process(batch).await;
    }

    /// flush buffer + followed by controller adjustment。
    async fn flush_batch(&mut self) {
        let time_since_last_flush = self.last_flush_time.elapsed();

        if self.buffer.is_empty() && !self.config.flush_empty_batches {
            self.last_flush_time = Instant::now();
            return;
        }

        self.flush_inner(time_since_last_flush).await;

        // 自适应调整窗口
        let snapshot = self.metrics.snapshot();
        self.current_window = self
            .controller
            .adjust_window(self.current_window, &snapshot)
            .await;
        self.current_window = self
            .current_window
            .clamp(self.config.min_window, self.config.max_window);
    }

    /// flush 内核心逻辑：处理 buffer → FlushInfo → 记录指标。
    async fn flush_inner(&mut self, time_since_last_flush: Duration) {
        let items = std::mem::take(&mut self.buffer);
        let batch_size = items.len();
        let batch_id = self.next_batch_id;
        self.next_batch_id += 1;

        let batch = Batch::new(items, batch_id);
        let proc_start = Instant::now();
        self.processor.process(batch).await;
        let execution_time = proc_start.elapsed();

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

    /// 通道关闭后清空所有 item（包括 bypass 通道）。
    async fn drain(&mut self) {
        // 清空主通道
        while let Some(item) = self.item_rx.recv().await {
            self.pending_count.fetch_sub(1, Ordering::Release);
            self.buffer.push(item);
        }

        // 清空 bypass 通道
        while let Ok(items) = self.bypass_rx.try_recv() {
            self.process_bypass(items).await;
        }

        if self.buffer.is_empty() && !self.config.flush_empty_batches {
            return;
        }

        let time_since_last_flush = self.last_flush_time.elapsed();
        self.flush_inner(time_since_last_flush).await;
    }
}
