use std::time::Duration;

use crate::batch::FlushInfo;
use crate::error::AccumulatorError;

/// EMA 平滑系数的默认值。
pub const DEFAULT_EMA_ALPHA: f64 = 0.3;

/// 指标快照，供 [`WindowController`](super::controller::WindowController) 查询。
#[derive(Debug, Clone)]
pub struct MetricsSnapshot {
    /// 最近批次的利用率：`batch_size / max_batch_size`。
    pub batch_utilization_rate: f64,
    /// 通道中待处理的消息数。
    pub queue_depth: usize,
    /// 当前 buffer 中的 item 数。
    pub buffer_size: usize,
    /// EMA 平滑执行时间（动态基准）。
    pub avg_execution_time: Duration,
    /// 最近一批的执行时间。
    pub last_execution_time: Duration,
}

/// 指标收集器，每次 flush 后由累加器调用。
///
/// 实现者负责维护内部状态（如 EMA 平滑值），并通过
/// [`snapshot`](Self::snapshot) 暴露给窗口控制器。
pub trait MetricsCollector: Send + 'static {
    /// 每次 flush 完成后调用。
    async fn record_flush(&mut self, info: &FlushInfo);

    /// 返回当前指标快照（同步，不阻塞）。
    fn snapshot(&self) -> MetricsSnapshot;
}

/// 内置指标收集器，基于 EMA 平滑批利用率和执行时间。
pub struct DefaultMetrics {
    alpha: f64,
    smoothed_utilization: f64,
    smoothed_execution_time: Duration,
    last_flush_info: Option<FlushInfo>,
}

impl DefaultMetrics {
    /// 创建新的默认指标收集器。
    pub fn new() -> Self {
        Self {
            alpha: DEFAULT_EMA_ALPHA,
            smoothed_utilization: 0.0,
            smoothed_execution_time: Duration::ZERO,
            last_flush_info: None,
        }
    }

    /// 设置 EMA 平滑系数。
    ///
    /// # Errors
    ///
    /// 当 `alpha` 不在 `[0.0, 1.0]` 范围内时返回错误。
    pub fn with_alpha(mut self, alpha: f64) -> Result<Self, AccumulatorError> {
        if !(0.0..=1.0).contains(&alpha) {
            return Err(AccumulatorError::InvalidConfig {
                reason: format!("alpha must be in [0.0, 1.0], got {alpha}"),
            });
        }
        self.alpha = alpha;
        Ok(self)
    }

    /// 当前 EMA 平滑利用率。
    pub fn smoothed_utilization(&self) -> f64 {
        self.smoothed_utilization
    }

    /// 当前 EMA 平滑执行时间（动态基准）。
    pub fn smoothed_execution_time(&self) -> Duration {
        self.smoothed_execution_time
    }
}

impl Default for DefaultMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl MetricsCollector for DefaultMetrics {
    async fn record_flush(&mut self, info: &FlushInfo) {
        let actual_util = if let Some(max) = info.max_batch_size {
            if max == 0 {
                0.0
            } else {
                (info.batch_size as f64 / max as f64).min(1.0)
            }
        } else {
            0.0
        };

        self.smoothed_utilization =
            self.alpha * actual_util + (1.0 - self.alpha) * self.smoothed_utilization;

        // EMA 平滑执行时间
        let exec_secs = info.execution_time.as_secs_f64();
        let smoothed_secs = self.smoothed_execution_time.as_secs_f64();
        let new_smoothed = self.alpha * exec_secs + (1.0 - self.alpha) * smoothed_secs;
        self.smoothed_execution_time = Duration::from_secs_f64(new_smoothed);

        self.last_flush_info = Some(info.clone());
    }

    fn snapshot(&self) -> MetricsSnapshot {
        let last_exec = self
            .last_flush_info
            .as_ref()
            .map(|f| f.execution_time)
            .unwrap_or(Duration::ZERO);

        MetricsSnapshot {
            batch_utilization_rate: self.smoothed_utilization,
            queue_depth: self
                .last_flush_info
                .as_ref()
                .map(|f| f.items_remaining)
                .unwrap_or(0),
            buffer_size: self
                .last_flush_info
                .as_ref()
                .map(|f| f.batch_size)
                .unwrap_or(0),
            avg_execution_time: self.smoothed_execution_time,
            last_execution_time: last_exec,
        }
    }
}
