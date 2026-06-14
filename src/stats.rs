//! 可观测性统计模块。
//!
//! 统计始终开启，可通过 [`AccumulatorHandle::stats`](crate::accumulator::AccumulatorHandle::stats)
//! 和 [`AccumulatorHandle::health`](crate::accumulator::AccumulatorHandle::health) 获取运行快照和健康状态。

use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// 累加器健康状态。
#[derive(Debug, Clone)]
pub struct AccumulatorHealth {
    /// 累加器是否仍在接收 item（通道是否开放）。
    pub is_accepting: bool,
    /// 队列利用率: pending / max_queue_depth。无上限时为 0.0。
    pub queue_utilization: f64,
    /// 当前时间窗口大小。
    pub current_window: Duration,
    /// 并发模式下的飞行中批次数。
    pub inflight_count: usize,
    /// 因队列满被拒绝的总次数。
    pub total_rejected: u64,
}

/// 延迟样本环形缓冲区容量。
const MAX_LATENCY_SAMPLES: usize = 1000;

/// 累加器运行统计快照。
#[derive(Debug, Clone)]
pub struct StatsSnapshot {
    /// 已接收的 submit 调用次数。
    pub total_submitted: u64,
    /// 已完成的 flush 批次数。
    pub total_flushed: u64,
    /// 因超时丢弃的 item 数。
    pub total_dropped_timeout: u64,
    /// bypass 调用次数。
    pub total_bypassed: u64,
    /// 因队列满被拒绝的提交次数。
    pub total_rejected: u64,
    /// 通道中待接收的 item 数。
    pub queue_depth: usize,
    /// 缓冲区中的 item 数。
    pub buffer_size: usize,
    /// 并发模式下的飞行中批次数。
    pub inflight_count: usize,
    /// 当前批次总权重（未启用权重追踪时为 0）。
    pub current_weight: usize,
    /// 当前时间窗口大小。
    pub current_window: Duration,
    /// 队列等待时间 p50（中位数）。
    pub p50_queue_wait: Duration,
    /// 队列等待时间 p99。
    pub p99_queue_wait: Duration,
    /// 队列等待时间平均值。
    pub avg_queue_wait: Duration,
    /// flush 执行时间 p50（中位数）。
    pub p50_latency: Duration,
    /// flush 执行时间 p99。
    pub p99_latency: Duration,
    /// flush 执行时间平均值。
    pub avg_latency: Duration,
    /// flush 执行时间最大值。
    pub max_latency: Duration,
}

/// 累加器内部统计（通过 Arc 在 Handle 和 Accumulator 间共享）。
pub(crate) struct AccumulatorStats {
    pub total_submitted: AtomicU64,
    pub total_flushed: AtomicU64,
    pub total_dropped_timeout: AtomicU64,
    pub total_bypassed: AtomicU64,
    pub total_rejected: AtomicU64,
    /// 延迟样本，最多 MAX_LATENCY_SAMPLES 条。
    latency_samples: Mutex<Vec<Duration>>,
    /// 队列等待时间样本。
    wait_samples: Mutex<Vec<Duration>>,
}

impl AccumulatorStats {
    /// 创建新的统计收集器。
    pub fn new() -> Self {
        Self {
            total_submitted: AtomicU64::new(0),
            total_flushed: AtomicU64::new(0),
            total_dropped_timeout: AtomicU64::new(0),
            total_bypassed: AtomicU64::new(0),
            total_rejected: AtomicU64::new(0),
            latency_samples: Mutex::new(Vec::with_capacity(MAX_LATENCY_SAMPLES)),
            wait_samples: Mutex::new(Vec::with_capacity(MAX_LATENCY_SAMPLES)),
        }
    }

    /// 记录一次 submit 调用。
    pub fn record_submit(&self) {
        self.total_submitted.fetch_add(1, Ordering::Release);
    }

    /// 记录一次队列等待时间。
    pub fn record_wait(&self, wait: Duration) {
        if let Ok(mut samples) = self.wait_samples.lock() {
            if samples.len() >= MAX_LATENCY_SAMPLES {
                samples.remove(0);
            }
            samples.push(wait);
        }
    }

    /// 记录一次 flush 完成（含执行耗时）。
    pub fn record_flush(&self, execution_time: Duration) {
        self.total_flushed.fetch_add(1, Ordering::Release);
        if let Ok(mut samples) = self.latency_samples.lock() {
            if samples.len() >= MAX_LATENCY_SAMPLES {
                // 环形：丢弃最旧的
                samples.remove(0);
            }
            samples.push(execution_time);
        }
    }

    /// 记录一次超时丢弃。
    pub fn record_dropped_timeout(&self, count: u64) {
        self.total_dropped_timeout.fetch_add(count, Ordering::Release);
    }

    /// 记录一次 bypass 调用。
    pub fn record_bypass(&self) {
        self.total_bypassed.fetch_add(1, Ordering::Release);
    }

    /// 记录一次因队列满被拒绝的提交。
    pub fn record_rejected(&self) {
        self.total_rejected.fetch_add(1, Ordering::Release);
    }

    /// 构建统计快照。
    pub fn snapshot(
        &self,
        queue_depth: usize,
        buffer_size: usize,
        inflight_count: usize,
        current_weight: usize,
        current_window: Duration,
    ) -> StatsSnapshot {
        let latency_samples: Vec<Duration> =
            self.latency_samples.lock().map(|s| s.clone()).unwrap_or_default();
        let wait_samples: Vec<Duration> =
            self.wait_samples.lock().map(|s| s.clone()).unwrap_or_default();

        let (p50, p99, avg, max) = percentiles(&latency_samples);
        let (wp50, wp99, wavg, _) = percentiles(&wait_samples);

        StatsSnapshot {
            total_submitted: self.total_submitted.load(Ordering::Acquire),
            total_flushed: self.total_flushed.load(Ordering::Acquire),
            total_dropped_timeout: self.total_dropped_timeout.load(Ordering::Acquire),
            total_bypassed: self.total_bypassed.load(Ordering::Acquire),
            total_rejected: self.total_rejected.load(Ordering::Acquire),
            queue_depth,
            buffer_size,
            inflight_count,
            current_weight,
            current_window,
            p50_queue_wait: wp50,
            p99_queue_wait: wp99,
            avg_queue_wait: wavg,
            p50_latency: p50,
            p99_latency: p99,
            avg_latency: avg,
            max_latency: max,
        }
    }
}

/// 计算延迟样本的百分位数（线性插值法）。
///
/// 使用与 NumPy `method='linear'` 相同的算法：`rank = p/100 * (n-1)`，
/// 对 rank 的整数部分和小数部分做线性插值。样本少时（如 2 条）p99 不再退化为 p50。
fn percentiles(samples: &[Duration]) -> (Duration, Duration, Duration, Duration) {
    if samples.is_empty() {
        return (Duration::ZERO, Duration::ZERO, Duration::ZERO, Duration::ZERO);
    }

    let mut sorted: Vec<&Duration> = samples.iter().collect();
    sorted.sort();

    let p50 = percentile_at(&sorted, 50.0);
    let p99 = percentile_at(&sorted, 99.0);
    let max = *sorted[sorted.len() - 1];

    let total_ns: u128 = samples.iter().map(|d| d.as_nanos()).sum();
    let len = sorted.len();
    let avg = Duration::from_nanos((total_ns / len as u128) as u64);

    (p50, p99, avg, max)
}

/// 计算给定百分位值（线性插值法）。
///
/// `p` 为 0..100 之间的值。样本数为 1 时直接返回该值。
fn percentile_at(sorted: &[&Duration], p: f64) -> Duration {
    let n = sorted.len();
    if n == 1 {
        return *sorted[0];
    }
    let rank = p * (n - 1) as f64 / 100.0;
    let lower = rank.floor() as usize;
    let upper = rank.ceil() as usize;
    if lower == upper {
        return *sorted[lower];
    }
    let frac = rank - lower as f64;
    let lower_ns = sorted[lower].as_nanos() as f64;
    let upper_ns = sorted[upper].as_nanos() as f64;
    Duration::from_nanos((lower_ns + frac * (upper_ns - lower_ns)) as u64)
}
