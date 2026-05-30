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
handle.submit(item)    // 提交 item，可能返回 QueueFull 或 Shutdown
handle.flush_now()     // 手动触发立即 flush（非阻塞）
handle.pending_count() // 当前待处理数
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

## 测试结果

- 11 单元测试（6 个 AdaptiveController + 5 个 LatencyAdaptiveController）
- 8 集成测试
- 全部通过，零 warning
