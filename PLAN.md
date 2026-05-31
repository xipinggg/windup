# 自适应时间窗口批处理库

## Context

在 Rust 中实现一个通用的批处理累加器库：积攒一个时间窗口内的所有请求，到期后整批交付处理器执行。时间窗口根据用户自定义指标自适应调整。

## 模块结构

```
src/
├── lib.rs           -- 顶层 re-export + prelude
├── error.rs         -- AccumulatorError 错误类型
├── config.rs        -- AccumulatorConfig builder（非泛型）
├── batch.rs         -- Batch<T>, FlushInfo, BatchProcessor trait
├── metrics.rs       -- MetricsCollector trait, MetricsSnapshot, DefaultMetrics
├── controller.rs    -- WindowController trait + 4 种内置控制器
└── accumulator.rs   -- BatchAccumulator 主循环 + AccumulatorHandle
```

## 核心 Trait 与类型

### BatchProcessor<T>
```rust
pub trait BatchProcessor<T: Send>: Send + 'static {
    fn process(&self, batch: Batch<T>) -> impl Future<Output = ()> + Send;
}
```

### FlushInfo
```rust
pub struct FlushInfo {
    pub batch_size: usize,
    pub max_batch_size: Option<usize>,
    pub window_duration: Duration,
    pub items_remaining: usize,
    pub batch_id: u64,
    pub execution_time: Duration,   // process() 耗时
}
```

### MetricsSnapshot
```rust
pub struct MetricsSnapshot {
    pub batch_utilization_rate: f64,
    pub queue_depth: usize,
    pub buffer_size: usize,
    pub time_since_last_flush: Duration,
    pub avg_execution_time: Duration,    // EMA 基准执行时间
    pub last_execution_time: Duration,   // 最近一批执行时间
}
```

### MetricsCollector
```rust
pub trait MetricsCollector: Send + 'static {
    async fn record_flush(&mut self, info: &FlushInfo);
    fn snapshot(&self) -> MetricsSnapshot;
}
```

### WindowController
```rust
pub trait WindowController: Send + 'static {
    async fn adjust_window(&mut self, current: Duration, metrics: &MetricsSnapshot) -> Duration;
}
```

## 内置控制器

| 控制器 | 驱动指标 | 算法 |
|--------|---------|------|
| `FixedController` | 无 | 永远返回固定窗口 |
| `AdaptiveController` | `batch_size / max_batch_size` | `factor = 1.0 + rate * (target - util)` |
| `LatencyAdaptiveController` | `exec_time / EMA基线` | `factor = 1.0 + rate * (target - ratio)` |
| `PIDController` | `batch_size / max_batch_size` | PID 控制算法消除稳态误差和振荡 |
| `BackoffController` | 连续满批/空闲计数 | 满批时指数退避放大窗口，空闲时缓慢回缩 |
| 自定义 | 任意 | 实现 `WindowController` trait |

### AdaptiveController
- 利用率低 → 窗口增大；利用率高 → 窗口缩小
- 无 `max_batch_size` 时 utilization = 0.0（窗口不变）

### LatencyAdaptiveController
- 执行变慢 → 窗口缩小（减轻压力）；执行变快 → 窗口增大（提高吞吐）
- 基准用 EMA 自动建立（`DefaultMetrics`）
- 首次 flush 无基准，窗口不变

## AccumulatorConfig（非泛型）

```rust
let config = AccumulatorConfig::new(200ms, 50ms, 5s)
    .with_max_batch_size(100)
    .with_max_queue_depth(10000)
    .with_flush_empty_batches(false);
```

`build()` 从 `processor: P` 自动推断 item 类型 `T`。

## AccumulatorHandle API

```rust
handle.send(item)              // fire-and-forget
handle.submit(item)            // 提交并获取 ReplyHandle
handle.submit_with(item, opts) // 带优先级/超时
handle.submit_or_wait(item, t) // 阻塞等待
handle.send_or_wait(item, t)   // fire-and-forget + 阻塞等待
handle.bypass(item)            // 跳过批处理
handle.flush_now()             // 手动触发立即 flush
handle.pending_count()         // 当前待处理数
handle.pause()                 // 暂停 flush（继续缓冲）
handle.resume()                // 恢复 flush
handle.is_paused()             // 是否已暂停
handle.cancel()                // 触发优雅关闭
handle.stats()                 // 统计快照（含队列等待时间）
handle.health()                // 健康状态
```

## 主循环 (select! biased)

```
timer 优先：deadline 到期 → flush ← 解决 timer 饥饿
其次 Notify：flush_now() → flush
最后 recv：收 item → buffer，达到 max_batch_size → flush
通道关闭 → drain 剩余 → 退出
```

## 设计决策记录

### 原子队列深度
`submit()` 使用 `AtomicUsize::fetch_update` 做原子 CAS 检查+递增，避免 TOCTOU 竞态。

### 负 Duration 防护
所有控制器在 `Duration::from_secs_f64()` 前加 `.max(0.0)`，防止负值 panic。

### 魔数常量化
EMA 平滑系数默认值定义为 `DEFAULT_EMA_ALPHA: f64 = 0.3`。

### Config 非泛型
`AccumulatorConfig` 不含 `PhantomData<T>`，可跨 item 类型复用。

## Cargo.toml 依赖

```toml
[dependencies]
tokio = { version = "1", features = ["time", "sync", "macros"] }
thiserror = "2"

[dev-dependencies]
tokio = { version = "1", features = ["full", "test-util"] }
```

## 术语表

| 术语 | 定义 |
|------|------|
| **批利用率 (Batch Utilization)** | `batch_size / max_batch_size`，衡量当前批次对配置上限的使用程度。利用率越低，说明 window 太小或负载不足；利用率越高，说明 window 可能太大致使批次接近上限。 |
| **EMA 平滑 (Exponential Moving Average)** | 指数移动平均，`new = α × current + (1−α) × old`。α 越大对新值越敏感，越小越平滑。`DefaultMetrics` 用 EMA 对利用率和执行时间做平滑，作为自适应控制器的输入。 |
| **bypass** | 绕过批处理和 timer，直接将 item 打包成单 item 批交付处理器。不参与利用率和窗口调整。适用于低延迟要求的紧急 item。 |
| **窗口收敛 (Window Convergence)** | 自适应控制器从初始窗口调整到稳态的过程。收敛速度由 `adjustment_rate` 决定，rate 越大收敛越快但可能振荡。 |
| **inflight** | 并发模式下正在后台 task 中执行的批次数。`max_inflight` 限制最大并发数，防止资源耗尽。 |
| **权重追踪 (Weight Tracking)** | 按 item 的"重量"而非数量来判断何时提前 flush。通过 `build_with_weight` 传入权重函数，配合 `max_batch_weight` 使用。 |
| **flush** | 将缓冲区中积攒的 item 打包成 `Batch`，交付 `BatchProcessor::process` 处理。触发条件：timer 到期 / 达到 `max_batch_size` / 达到 `max_batch_weight` / 手动 `flush_now()`。 |
| **drain** | 累加器关闭后的清理阶段：清空通道中剩余 item，处理最后一批，等待 inflight task 完成。支持超时控制（`drain_timeout`）。 |

## 调参指南

### AdaptiveController

基于批利用率调整窗口。

```rust
AdaptiveController::new(target_utilization, adjustment_rate)?
```

| 参数 | 推荐值 | 说明 |
|------|--------|------|
| `target_utilization` | 0.7 ~ 0.9 | 目标利用率。低于此值 → 窗口增大（等更多 item）；高于此值 → 窗口缩小（更快 flush）。 |
| `adjustment_rate` | 0.05 ~ 0.2 | 调整速度。越大越快但可能振荡；越小越稳定但收敛慢。 |

**典型场景**：已知 `max_batch_size=100`，希望每批约 80 条。设置 `target_utilization=0.8, adjustment_rate=0.1`。

### LatencyAdaptiveController

基于执行时间 vs EMA 基准调整窗口。

```rust
LatencyAdaptiveController::new(target_ratio, adjustment_rate)?
```

| 参数 | 推荐值 | 说明 |
|------|--------|------|
| `target_ratio` | 1.0 | 期望的执行时间与基准的比值。>1.0 允许执行变慢（窗口增大，吞吐优先）；<1.0 要求执行更快（窗口缩小，延迟优先）。 |
| `adjustment_rate` | 0.1 ~ 0.3 | 调整速度。 |

**典型场景**：处理器延迟敏感，执行变慢时自动减小窗口降低压力。设置 `target_ratio=1.0, adjustment_rate=0.2`。

### EMA Alpha

`DefaultMetrics::new().with_alpha(alpha)?`

| alpha | 效果 |
|-------|------|
| 0.1 ~ 0.2 | 强平滑，适合稳定负载 |
| 0.3 (默认) | 均衡 |
| 0.5 ~ 0.7 | 快速响应，适合波动负载 |

### 最佳实践

1. **容量规划**：`max_queue_depth` 应大于 `max_batch_size × 并发数 × 2`，留足缓冲空间。
2. **窗口与批次配合**：初始窗口应能积攒到 `max_batch_size × target_utilization` 条左右。例如每秒 1000 QPS、`max_batch_size=100` → 初始窗口约 80ms。
3. **并发模式选择**：CPU 密集型处理器用 `Serial`；I/O 密集型（网络/数据库）用 `Concurrent { max_inflight: 4~8 }`。
4. **避免空批次**：默认 `flush_empty_batches=false`（不 flush 空批次）。只在需要心跳/保活场景下开启。
