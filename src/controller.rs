use std::time::Duration;

use crate::error::AccumulatorError;
use crate::metrics::MetricsSnapshot;

/// 窗口控制器，根据指标快照自适应调整时间窗口。
///
/// 每次 flush 后由累加器调用。返回值由累加器的 `[min_window, max_window]`
/// 做 clamp，控制器无需自行边界检查。
pub trait WindowController: Send + 'static {
    /// 读取指标快照，返回调整后的窗口时长（同步方法）。
    fn adjust_window(&mut self, current_window: Duration, metrics: &MetricsSnapshot) -> Duration;
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
    pub fn new(target_utilization: f64, adjustment_rate: f64) -> Result<Self, AccumulatorError> {
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
        Ok(Self { target_utilization, adjustment_rate })
    }
}

impl WindowController for AdaptiveController {
    fn adjust_window(&mut self, current_window: Duration, metrics: &MetricsSnapshot) -> Duration {
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
        Ok(Self { target_ratio, adjustment_rate })
    }
}

impl WindowController for LatencyAdaptiveController {
    fn adjust_window(&mut self, current_window: Duration, metrics: &MetricsSnapshot) -> Duration {
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
    fn adjust_window(&mut self, _current_window: Duration, _metrics: &MetricsSnapshot) -> Duration {
        self.window
    }
}

/// PID 自适应窗口控制器。
///
/// # 算法
///
/// 使用 PID（比例-积分-微分）控制算法消除稳态误差和振荡。
///
/// ```text
/// error = target - actual_utilization
/// P = kp * error
/// I = ki * integral      (积分项，消除稳态误差)
/// D = kd * derivative    (微分项，抑制振荡)
/// factor = 1.0 + P + I + D
/// new_window = current * factor
/// ```
///
/// 积分项被限制在 `[PID_INTEGRAL_MIN, PID_INTEGRAL_MAX]` 范围内防止积分饱和。
///
/// # 参数建议
///
/// - `target`: 目标利用率，0.7 ~ 0.9
/// - `kp`: 比例系数，推荐 0.1 ~ 0.3
/// - `ki`: 积分系数，推荐 0.01 ~ 0.05
/// - `kd`: 微分系数，推荐 0.05 ~ 0.15
pub struct PIDController {
    /// 目标批利用率（0.0 ~ 1.0）。
    target: f64,
    /// 比例系数。
    kp: f64,
    /// 积分系数。
    ki: f64,
    /// 微分系数。
    kd: f64,
    /// 积分累积项。
    integral: f64,
    /// 上一次误差（用于计算导数）。
    prev_error: f64,
}

/// PID 控制器积分项下限（防止积分饱和）。
const PID_INTEGRAL_MIN: f64 = -5.0;
/// PID 控制器积分项上限（防止积分饱和）。
const PID_INTEGRAL_MAX: f64 = 5.0;

impl PIDController {
    /// 创建 PID 自适应窗口控制器。
    ///
    /// # Errors
    ///
    /// 当 `target` 不在 `[0.0, 1.0]` 范围内时返回错误。
    pub fn new(target: f64, kp: f64, ki: f64, kd: f64) -> Result<Self, AccumulatorError> {
        if !(0.0..=1.0).contains(&target) {
            return Err(AccumulatorError::InvalidConfig {
                reason: format!("target must be in [0.0, 1.0], got {target}"),
            });
        }
        if kp < 0.0 {
            return Err(AccumulatorError::InvalidConfig {
                reason: format!("kp must be >= 0, got {kp}"),
            });
        }
        if ki < 0.0 {
            return Err(AccumulatorError::InvalidConfig {
                reason: format!("ki must be >= 0, got {ki}"),
            });
        }
        if kd < 0.0 {
            return Err(AccumulatorError::InvalidConfig {
                reason: format!("kd must be >= 0, got {kd}"),
            });
        }
        Ok(Self { target, kp, ki, kd, integral: 0.0, prev_error: 0.0 })
    }
}

impl WindowController for PIDController {
    fn adjust_window(&mut self, current: Duration, metrics: &MetricsSnapshot) -> Duration {
        // 空闲期或无数据：窗口不变
        if metrics.batch_utilization_rate == 0.0 {
            return current;
        }

        let error = self.target - metrics.batch_utilization_rate;
        self.integral = (self.integral + error).clamp(PID_INTEGRAL_MIN, PID_INTEGRAL_MAX);
        let derivative = error - self.prev_error;
        self.prev_error = error;

        let factor = 1.0 + self.kp * error + self.ki * self.integral + self.kd * derivative;
        Duration::from_secs_f64((current.as_secs_f64() * factor).max(0.0))
    }
}

/// 指数退避窗口控制器：满批时指数放大窗口，空闲时缓慢回缩。
///
/// # 算法
///
/// ```text
/// 若 batch_utilization >= 0.95（满批）:
///   full_count += 1
///   窗口 *= factor^full_count  （指数放大以容纳更多 item）
///
/// 若 utilization == 0 且队列为空（空闲）:
///   empty_count += 1
///   若 empty_count >= 3:
///     窗口 /= factor  （缓慢回缩以降低延迟）
///
/// 否则：计数器重置，窗口不变
/// ```
///
/// 窗口始终被限制在构造时指定的 `[min_window, max_window]` 范围内。
///
/// # 适用场景
///
/// 突发流量场景——流量高峰时快速放大窗口提高吞吐，空闲时缓慢缩小窗口降低延迟。
pub struct BackoffController {
    /// 最小窗口大小。
    min_window: Duration,
    /// 最大窗口大小。
    max_window: Duration,
    /// 退避因子（>1.0 放大，反之缩小）。
    factor: f64,
    /// 连续满批次数。
    full_count: u64,
    /// 连续空闲次数。
    empty_count: u64,
}

/// Backoff 控制器满批判定阈值（利用率 >= 此值视为满批）。
const BACKOFF_FULL_THRESHOLD: f64 = 0.95;
/// Backoff 控制器回缩所需的连续空闲次数。
const BACKOFF_SHRINK_EMPTY_COUNT: u64 = 3;

impl BackoffController {
    /// 创建指数退避窗口控制器。
    ///
    /// - `min`: 最小窗口。
    /// - `max`: 最大窗口。
    /// - `factor`: 退避因子。满批时窗口乘以 factor，空闲时除以 factor。
    ///   推荐 1.5 ~ 2.0。
    pub fn new(min: Duration, max: Duration, factor: f64) -> Result<Self, AccumulatorError> {
        if factor <= 0.0 {
            return Err(AccumulatorError::InvalidConfig {
                reason: format!("factor must be > 0, got {factor}"),
            });
        }
        Ok(Self { min_window: min, max_window: max, factor, full_count: 0, empty_count: 0 })
    }
}

impl WindowController for BackoffController {
    fn adjust_window(&mut self, current: Duration, metrics: &MetricsSnapshot) -> Duration {
        if metrics.batch_utilization_rate >= BACKOFF_FULL_THRESHOLD {
            // 满批：指数放大窗口
            self.full_count += 1;
            self.empty_count = 0;
            current
                .mul_f64(self.factor.powi(self.full_count as i32))
                .clamp(self.min_window, self.max_window)
        } else if metrics.batch_utilization_rate == 0.0 && metrics.queue_depth == 0 {
            // 空闲：连续多次后缓慢回缩
            self.empty_count += 1;
            self.full_count = 0;
            if self.empty_count >= BACKOFF_SHRINK_EMPTY_COUNT {
                let shrunk = current.as_secs_f64() / self.factor;
                Duration::from_secs_f64(shrunk.max(self.min_window.as_secs_f64()))
            } else {
                current
            }
        } else {
            // 正常范围内：重置计数器
            self.full_count = 0;
            self.empty_count = 0;
            current
        }
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
        let new = ctrl.adjust_window(Duration::from_millis(200), &snap_util(1.0));
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
        let new = ctrl.adjust_window(Duration::from_millis(500), &snap_util(1.0));
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
        let new = ctrl.adjust_window(Duration::from_millis(200), &snap);
        assert!(new < Duration::from_millis(200));
    }

    // ─── PID 控制器测试 ───

    fn pid(target: f64, kp: f64, ki: f64, kd: f64) -> PIDController {
        PIDController::new(target, kp, ki, kd).unwrap()
    }

    fn snap_with_queue(util: f64, queue: usize) -> MetricsSnapshot {
        MetricsSnapshot {
            batch_utilization_rate: util,
            queue_depth: queue,
            buffer_size: 0,
            avg_execution_time: Duration::ZERO,
            last_execution_time: Duration::ZERO,
        }
    }

    #[tokio::test]
    async fn pid_decreases_on_high_utilization() {
        let mut ctrl = pid(0.8, 0.15, 0.01, 0.05);
        let current = Duration::from_millis(500);
        let new = ctrl.adjust_window(current, &snap_util(0.95));
        assert!(new < current, "PID should decrease on high util, got {new:?}");
    }

    #[tokio::test]
    async fn pid_increases_on_low_utilization() {
        let mut ctrl = pid(0.8, 0.15, 0.01, 0.05);
        let current = Duration::from_millis(500);
        let new = ctrl.adjust_window(current, &snap_util(0.3));
        assert!(new > current, "PID should increase on low util, got {new:?}");
    }

    #[tokio::test]
    async fn pid_idle_unchanged() {
        let mut ctrl = pid(0.8, 0.15, 0.01, 0.05);
        let current = Duration::from_millis(500);
        let new = ctrl.adjust_window(current, &snap_util(0.0));
        assert_eq!(new, current, "PID idle should not change window");
    }

    #[tokio::test]
    async fn pid_integral_builds_up() {
        // 持续低利用率，积分项累积，调整幅度逐步增大
        let mut ctrl = pid(0.8, 0.15, 0.1, 0.0);
        let current = Duration::from_millis(500);

        let snap1 = ctrl.adjust_window(current, &snap_util(0.3));
        let snap2 = ctrl.adjust_window(current, &snap_util(0.3));
        let snap3 = ctrl.adjust_window(current, &snap_util(0.3));

        // 积分累积后，第三次调整应比第一次更激进
        let delta1 = snap1.as_millis() as i64 - current.as_millis() as i64;
        let delta2 = snap2.as_millis() as i64 - current.as_millis() as i64;
        let delta3 = snap3.as_millis() as i64 - current.as_millis() as i64;
        assert!(
            delta3 > delta1,
            "PID integral should build up: delta1={delta1}, delta2={delta2}, delta3={delta3}"
        );
    }

    #[tokio::test]
    async fn pid_integral_clamped() {
        // 使用大 ki 验证积分不会无限累积
        let mut ctrl = pid(0.8, 0.0, 1.0, 0.0);
        let current = Duration::from_millis(500);

        for _ in 0..20 {
            ctrl.adjust_window(current, &snap_util(0.0));
        }
        // 积分被 clamp 在 [-5.0, 5.0]，不应 panic 或产生异常值
        let new = ctrl.adjust_window(current, &snap_util(0.5));
        assert!(!new.is_zero(), "clamped integral should still work");
    }

    #[tokio::test]
    async fn pid_on_target_stable() {
        // 恰好命中目标时，窗口应不变
        let mut ctrl = pid(0.8, 0.15, 0.01, 0.05);
        let current = Duration::from_millis(500);
        let new = ctrl.adjust_window(current, &snap_util(0.8));
        assert_eq!(new, current, "PID on-target should not change");
    }

    // ─── Backoff 控制器测试 ───

    fn bo(min: Duration, max: Duration, factor: f64) -> BackoffController {
        BackoffController::new(min, max, factor).unwrap()
    }

    #[tokio::test]
    async fn backoff_full_batch_grows_window() {
        let mut ctrl = bo(Duration::from_millis(100), Duration::from_secs(10), 2.0);
        let current = Duration::from_millis(200);
        let new = ctrl.adjust_window(current, &snap_util(0.98));
        assert!(new > current, "full batch should grow window, got {new:?}");
        assert_eq!(new, Duration::from_millis(400), "factor=2.0, 1 full → 200*2=400");
    }

    #[tokio::test]
    async fn backoff_consecutive_full_exponential() {
        let mut ctrl = bo(Duration::from_millis(100), Duration::from_secs(10), 2.0);
        let current = Duration::from_millis(200);

        // 第一次满批：200 * 2^1 = 400
        let w1 = ctrl.adjust_window(current, &snap_util(0.99));
        assert_eq!(w1, Duration::from_millis(400));

        // 第二次满批：200 * 2^2 = 800
        let w2 = ctrl.adjust_window(current, &snap_util(0.99));
        assert_eq!(w2, Duration::from_millis(800));

        // 第三次满批：200 * 2^3 = 1600
        let w3 = ctrl.adjust_window(current, &snap_util(0.99));
        assert_eq!(w3, Duration::from_millis(1600));
    }

    #[tokio::test]
    async fn backoff_idle_shrinks_after_consecutive() {
        let mut ctrl = bo(Duration::from_millis(100), Duration::from_secs(10), 2.0);
        let current = Duration::from_millis(400);
        let idle_snap = snap_with_queue(0.0, 0);

        // 1-2 次空闲：不变
        let w1 = ctrl.adjust_window(current, &idle_snap);
        assert_eq!(w1, current);
        let w2 = ctrl.adjust_window(current, &idle_snap);
        assert_eq!(w2, current);

        // 第 3 次空闲：回缩
        let w3 = ctrl.adjust_window(current, &idle_snap);
        assert_eq!(w3, Duration::from_millis(200), "3rd idle should shrink: 400/2=200");
    }

    #[tokio::test]
    async fn backoff_idle_with_queue_depth_does_not_shrink() {
        // 队列中有待处理项时不算真正的空闲
        let mut ctrl = bo(Duration::from_millis(100), Duration::from_secs(10), 2.0);
        let current = Duration::from_millis(400);
        let snap = snap_with_queue(0.0, 5); // queue_depth > 0

        for _ in 0..5 {
            let w = ctrl.adjust_window(current, &snap);
            assert_eq!(w, current, "queue_depth > 0 should not trigger shrink");
        }
    }

    #[tokio::test]
    async fn backoff_normal_utilization_resets_counters() {
        let mut ctrl = bo(Duration::from_millis(100), Duration::from_secs(10), 2.0);
        let current = Duration::from_millis(200);

        // 先积累 2 次满批
        ctrl.adjust_window(current, &snap_util(0.99));
        ctrl.adjust_window(current, &snap_util(0.99));

        // 一次正常利用率（0.5），重置计数器
        let w = ctrl.adjust_window(current, &snap_util(0.5));
        assert_eq!(w, current, "normal util should reset, go back to current");

        // 再满批，从头计数
        let w2 = ctrl.adjust_window(current, &snap_util(0.99));
        assert_eq!(w2, Duration::from_millis(400), "after reset, full count should restart at 1");
    }

    #[tokio::test]
    async fn backoff_clamped_to_bounds() {
        let mut ctrl = bo(Duration::from_millis(100), Duration::from_millis(1000), 3.0);
        let current = Duration::from_millis(500);

        // 多次满批，应被 max_window 限制
        for _ in 0..5 {
            ctrl.adjust_window(current, &snap_util(0.99));
        }
        // 不会超过 max_window
        let w = ctrl.adjust_window(current, &snap_util(0.99));
        assert!(w <= Duration::from_millis(1000), "should clamp to max_window, got {w:?}");

        // 从极小窗口开始多次空闲，应被 min_window 限制
        let mut ctrl2 = bo(Duration::from_millis(50), Duration::from_secs(10), 4.0);
        let small = Duration::from_millis(60);
        for _ in 0..10 {
            ctrl2.adjust_window(small, &snap_with_queue(0.0, 0));
        }
        let w2 = ctrl2.adjust_window(small, &snap_with_queue(0.0, 0));
        assert!(w2 >= Duration::from_millis(50), "should clamp to min_window, got {w2:?}");
    }

    #[tokio::test]
    async fn backoff_full_then_idle_resets_full_counter() {
        let mut ctrl = bo(Duration::from_millis(100), Duration::from_secs(10), 2.0);
        let current = Duration::from_millis(200);

        // 满批 2 次
        ctrl.adjust_window(current, &snap_util(0.99));
        ctrl.adjust_window(current, &snap_util(0.99));

        // 空闲 1 次（重置 full，+1 empty）
        let w = ctrl.adjust_window(current, &snap_with_queue(0.0, 0));
        assert_eq!(w, current, "idle should reset full counter, not yet shrink");

        // 再满批 -> 从头计数
        let w2 = ctrl.adjust_window(current, &snap_util(0.99));
        assert_eq!(w2, Duration::from_millis(400), "full after idle reset should restart at 1");
    }
}
