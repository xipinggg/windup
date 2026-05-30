use std::future::Future;
use std::time::Duration;

use tokio::time::Instant;

/// 批处理器，负责消费累加器 flush 出的批次。
///
/// 由用户实现，包含实际的批量处理逻辑。
pub trait BatchProcessor<T: Send>: Send + 'static {
    /// 处理一个批次。
    fn process(&self, batch: Batch<T>) -> impl Future<Output = ()> + Send;
}

/// 一个待处理的批次。
///
/// 包含在时间窗口内收集到的所有 item。
#[derive(Debug)]
pub struct Batch<T> {
    items: Vec<T>,
    batch_id: u64,
    created_at: Instant,
}

impl<T> Batch<T> {
    /// 创建新批次。
    pub fn new(items: Vec<T>, batch_id: u64) -> Self {
        Self {
            items,
            batch_id,
            created_at: Instant::now(),
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
}
