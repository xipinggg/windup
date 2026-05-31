use std::time::Duration;

use crate::error::AccumulatorError;
use crate::metrics::MetricsSnapshot;

/// 窗口控制器，根据指标快照自适应调整时间窗口。
///
/// 每次 flush 后由累加器调用。返回值由累加器的 `[min_window, max_window]`
/// 做 clamp，控制器无需自行边界检查。
pub trait WindowController: Send + 'static {
    /// 读取指标快照，返回调整后的窗口时长（同步方法）。
    fn adjust_window(
        &mut self,
        current_window: Duration,
        metrics: &MetricsSnapshot,
    ) -> Duration;
}

/// 基于批利用率的自适应窗口控制器。
///
/// # 算法
///
/// ```text
/// 如果 utilization == 0（空闲期）: 窗口不变
/// 否则:
///   error = target_utilization - actual_utilization
///   factor = 1.0 + adjustment_rate * error
///   new = current * factor
/// ```
///
/// 窗口边界由累加器负责 clamp。
pub struct AdaptiveController {
    target_utilization: f64,
    adjustment_rate: f64,
}

impl AdaptiveController {
    /// 创建利用率自适应控制器。
    ///
    /// - `target_utilization`：目标批利用率（0.0 ~ 1.0）。
    /// - `adjustment_rate`：调整速率系数（推荐 0.05 ~ 0.2）。
    ///
    /// # Errors
    ///
    /// 当参数不在有效范围时返回 [`AccumulatorError::InvalidConfig`]。
    pub fn new(
        target_utilization: f64,
        adjustment_rate: f64,
    ) -> Result<Self, AccumulatorError> {
        if !(0.0..=1.0).contains(&target_utilization) {
            return Err(AccumulatorError::InvalidConfig {
                reason: format!(
                    "target_utilization must be in [0.0, 1.0], got {target_utilization}"
                ),
            });
        }
        if adjustment_rate <= 0.0 {
            return Err(AccumulatorError::InvalidConfig {
                reason: format!("adjustment_rate must be positive, got {adjustment_rate}"),
            });
        }
        Ok(Self {
            target_utilization,
            adjustment_rate,
        })
    }
}

impl WindowController for AdaptiveController {
    fn adjust_window(
        &mut self,
        current_window: Duration,
        metrics: &MetricsSnapshot,
    ) -> Duration {
        let util = metrics.batch_utilization_rate;

        // 空闲期或无数据：窗口不变
        if util == 0.0 {
            return current_window;
        }

        let error = self.target_utilization - util;
        let factor = 1.0 + self.adjustment_rate * error;

        let current_secs = current_window.as_secs_f64();
        let new_secs = (current_secs * factor).max(0.0);
        Duration::from_secs_f64(new_secs)
    }
}

/// 基于执行时间的自适应窗口控制器。
///
/// # 算法
///
/// ```text
/// baseline = avg_execution_time  (EMA 基准)
/// current  = last_execution_time (最近一批)
///
/// 若 baseline == 0（首次 flush）: 窗口不变
///
/// ratio = current / baseline
/// error = target_ratio - ratio
/// factor = 1.0 + adjustment_rate * error
/// new = current_window * factor
/// ```
pub struct LatencyAdaptiveController {
    target_ratio: f64,
    adjustment_rate: f64,
}

impl LatencyAdaptiveController {
    /// 创建延迟自适应控制器。
    ///
    /// - `target_ratio`：期望的执行时间 / 基准比值（1.0 = 与基准持平）。
    /// - `adjustment_rate`：调整速率系数（推荐 0.05 ~ 0.2）。
    ///
    /// # Errors
    ///
    /// 当参数不在有效范围时返回 [`AccumulatorError::InvalidConfig`]。
    pub fn new(target_ratio: f64, adjustment_rate: f64) -> Result<Self, AccumulatorError> {
        if target_ratio <= 0.0 {
            return Err(AccumulatorError::InvalidConfig {
                reason: format!("target_ratio must be positive, got {target_ratio}"),
            });
        }
        if adjustment_rate <= 0.0 {
            return Err(AccumulatorError::InvalidConfig {
                reason: format!("adjustment_rate must be positive, got {adjustment_rate}"),
            });
        }
        Ok(Self {
            target_ratio,
            adjustment_rate,
        })
    }
}

impl WindowController for LatencyAdaptiveController {
    fn adjust_window(
        &mut self,
        current_window: Duration,
        metrics: &MetricsSnapshot,
    ) -> Duration {
        let baseline = metrics.avg_execution_time;
        let current = metrics.last_execution_time;

        if baseline.is_zero() {
            return current_window;
        }

        let baseline_secs = baseline.as_secs_f64();
        let current_secs = current.as_secs_f64();
        let ratio = current_secs / baseline_secs;

        let error = self.target_ratio - ratio;
        let factor = 1.0 + self.adjustment_rate * error;

        let new_secs = (current_window.as_secs_f64() * factor).max(0.0);
        Duration::from_secs_f64(new_secs)
    }
}

/// 固定窗口控制器：永远返回窗口，不做自适应调整。
pub struct FixedController {
    window: Duration,
}

impl FixedController {
    /// 创建固定窗口控制器。
    pub fn new(window: Duration) -> Self {
        Self { window }
    }
}

impl WindowController for FixedController {
    fn adjust_window(
        &mut self,
        _current_window: Duration,
        _metrics: &MetricsSnapshot,
    ) -> Duration {
        self.window
    }
}

/// PID 自适应窗口控制器。
pub struct PIDController { pub target: f64, pub kp: f64, pub ki: f64, pub kd: f64, integral: f64, prev_error: f64 }
impl PIDController {
    pub fn new(target: f64, kp: f64, ki: f64, kd: f64) -> Result<Self, AccumulatorError> {
        if !(0.0..=1.0).contains(&target) { return Err(AccumulatorError::InvalidConfig { reason: "target out of [0,1]".into() }); }
        Ok(Self { target, kp, ki, kd, integral: 0.0, prev_error: 0.0 })
    }
}
impl WindowController for PIDController {
    fn adjust_window(&mut self, current: Duration, metrics: &MetricsSnapshot) -> Duration {
        if metrics.batch_utilization_rate == 0.0 { return current; }
        let error = self.target - metrics.batch_utilization_rate;
        self.integral = (self.integral + error).clamp(-5.0, 5.0);
        let derivative = error - self.prev_error; self.prev_error = error;
        let factor = 1.0 + self.kp * error + self.ki * self.integral + self.kd * derivative;
        Duration::from_secs_f64((current.as_secs_f64() * factor).max(0.0))
    }
}

/// 指数退避窗口控制器：满批时指数放大，空闲时缓慢回缩。
pub struct BackoffController { min_window: Duration, max_window: Duration, factor: f64, full: u64, empty: u64 }
impl BackoffController {
    pub fn new(min: Duration, max: Duration, factor: f64) -> Self {
        Self { min_window: min, max_window: max, factor, full: 0, empty: 0 }
    }
}
impl WindowController for BackoffController {
    fn adjust_window(&mut self, current: Duration, metrics: &MetricsSnapshot) -> Duration {
        if metrics.batch_utilization_rate >= 0.95 { self.full += 1; self.empty = 0;
            current.mul_f64(self.factor.powi(self.full as i32)).clamp(self.min_window, self.max_window)
        } else if metrics.batch_utilization_rate == 0.0 && metrics.queue_depth == 0 { self.empty += 1; self.full = 0;
            if self.empty >= 3 { Duration::from_secs_f64((current.as_secs_f64() / self.factor).max(self.min_window.as_secs_f64())) } else { current }
        } else { self.full = 0; self.empty = 0; current }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snap_util(util: f64) -> MetricsSnapshot {
        MetricsSnapshot {
            batch_utilization_rate: util,
            queue_depth: 0,
            buffer_size: 0,
            avg_execution_time: Duration::ZERO,
            last_execution_time: Duration::ZERO,
        }
    }

    fn snap_latency(baseline: Duration, current: Duration) -> MetricsSnapshot {
        MetricsSnapshot {
            batch_utilization_rate: 0.0,
            queue_depth: 0,
            buffer_size: 0,
            avg_execution_time: baseline,
            last_execution_time: current,
        }
    }

    fn ac(target: f64, rate: f64) -> AdaptiveController {
        AdaptiveController::new(target, rate).unwrap()
    }

    fn lc(target: f64, rate: f64) -> LatencyAdaptiveController {
        LatencyAdaptiveController::new(target, rate).unwrap()
    }

    #[tokio::test]
    async fn adaptive_increases_on_low_utilization() {
        let mut ctrl = ac(0.8, 0.1);
        let current = Duration::from_millis(500);
        let new = ctrl.adjust_window(current, &snap_util(0.3));
        assert!(new > current, "expected window to increase, got {new:?}");
    }

    #[tokio::test]
    async fn adaptive_decreases_on_high_utilization() {
        let mut ctrl = ac(0.8, 0.1);
        let current = Duration::from_millis(500);
        let new = ctrl.adjust_window(current, &snap_util(0.95));
        assert!(new < current, "expected window to decrease, got {new:?}");
    }

    #[tokio::test]
    async fn adaptive_clamps_to_bounds() {
        let mut ctrl = ac(0.8, 0.5);
        // 极高利用率 → factor 变小但不会负
        let new = ctrl
            .adjust_window(Duration::from_millis(200), &snap_util(1.0))
            ;
        assert!(new < Duration::from_millis(200), "high util should shrink");
    }

    #[tokio::test]
    async fn adaptive_idle_unchanged() {
        let mut ctrl = ac(0.8, 0.1);
        let current = Duration::from_millis(500);
        let new = ctrl.adjust_window(current, &snap_util(0.0));
        assert_eq!(new, current, "idle window should not change");
    }

    #[tokio::test]
    async fn fixed_controller_always_same() {
        let mut ctrl = FixedController::new(Duration::from_millis(300));
        let new = ctrl
            .adjust_window(Duration::from_millis(500), &snap_util(1.0))
            ;
        assert_eq!(new, Duration::from_millis(300));
    }

    #[tokio::test]
    async fn adaptive_on_target_unchanged() {
        let mut ctrl = ac(0.8, 0.1);
        let current = Duration::from_millis(500);
        let new = ctrl.adjust_window(current, &snap_util(0.8));
        assert_eq!(new, current);
    }

    #[tokio::test]
    async fn latency_slower_than_baseline_decreases_window() {
        let mut ctrl = lc(1.0, 0.1);
        let current = Duration::from_millis(500);
        let snap = snap_latency(Duration::from_millis(100), Duration::from_millis(110));
        let new = ctrl.adjust_window(current, &snap);
        assert!(new < current, "slower should decrease, got {new:?}");
    }

    #[tokio::test]
    async fn latency_faster_than_baseline_increases_window() {
        let mut ctrl = lc(1.0, 0.1);
        let current = Duration::from_millis(500);
        let snap = snap_latency(Duration::from_millis(100), Duration::from_millis(90));
        let new = ctrl.adjust_window(current, &snap);
        assert!(new > current, "faster should increase, got {new:?}");
    }

    #[tokio::test]
    async fn latency_equal_to_baseline_unchanged() {
        let mut ctrl = lc(1.0, 0.1);
        let current = Duration::from_millis(500);
        let snap = snap_latency(Duration::from_millis(100), Duration::from_millis(100));
        let new = ctrl.adjust_window(current, &snap);
        assert_eq!(new, current);
    }

    #[tokio::test]
    async fn latency_first_flush_unchanged() {
        let mut ctrl = lc(1.0, 0.1);
        let current = Duration::from_millis(500);
        let snap = snap_latency(Duration::ZERO, Duration::from_millis(100));
        let new = ctrl.adjust_window(current, &snap);
        assert_eq!(new, current, "first flush should not change window");
    }

    #[tokio::test]
    async fn latency_clamped_to_bounds() {
        let mut ctrl = lc(1.0, 0.5);
        let snap = snap_latency(Duration::from_millis(100), Duration::from_millis(300));
        let new = ctrl
            .adjust_window(Duration::from_millis(200), &snap)
            ;
        assert!(new < Duration::from_millis(200));
    }
}
