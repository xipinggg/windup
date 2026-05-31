# Tracing 可观测性设计

## 概述

为批处理累加器库添加结构化日志与分布式追踪能力，基于 `tracing` 生态，支持全链路 span 传播。

## 需求背景

- **可观测性**：库内部关键事件（flush、窗口调整、队列满、超时）需暴露给下游
- **可排障性**：每个 item 从 `submit` 到完成需有独立 span，可与上游调用方 span 串联
- **运行时可控**：日志级别通过 `AccumulatorConfig` 配置
- **零开销退出**：feature 关闭后完全无代价

## 架构决策

| 决策 | 选择 | 理由 |
|------|------|------|
| 生态 | `tracing` crate | Rust 异步生态标配，天然支持 OTLP 导出 |
| Feature 控制 | `tracing` feature，默认开启 | 编译期 opt-out，零代价 |
| 配置方式 | `AccumulatorConfig` builder | 运行时级别过滤，避免无效事件构造 |
| 跨 channel 传播 | `Span` 随 `ChannelItem` 传递 | 最小侵入，保持无锁 |

## Feature Flag

```toml
[dependencies]
tracing = { version = "0.1", default-features = false, optional = true }

[features]
default = ["tracing"]
tracing = ["dep:tracing"]
```

`tracing` 默认开启。依赖方可 `default-features = false` 关闭。

## 模块结构

新增 `src/trace.rs`（条件编译模块）：

```
src/
├── lib.rs           -- #[cfg(feature = "tracing")] pub mod trace;
├── trace.rs         -- span 名称常量、辅助类型、event_if! 宏
├── accumulator.rs   -- 核心插桩点
├── batch.rs         -- ChannelItem/BufferItem 的 parent_span 字段
├── config.rs        -- with_tracing_level / with_trace_per_item
├── ... (其余不变)
```

`trace.rs` 集中管理所有 tracing 相关的类型别名和辅助函数，减少核心文件中的 `#[cfg]` 污染。

## Span 层级

### 层级树

span 之间的父子关系（`::` 是 tracing-tree 等 subscriber 的展示惯例，非 span 名称字面值）：

```
accumulator::run             ← span 名 "run"，生命周期 = 累加器主循环
 ├── batch                   ← span 名 "batch"，每次 flush 批次
 │    ├── item               ← span 名 "item"，每个 item 的处理
 │    └── ...
 ├── bypass                  ← span 名 "bypass"，直接交付
 └── drain                   ← span 名 "drain"，shutdown 清空
```

### Span 字段

| Span 名 | 字段 | 说明 |
|---------|------|------|
| `run` | `config.min_window`, `config.max_window`, `config.concurrency_mode` | 启动时记录配置 |
| `batch` | `batch_id`, `batch_size`, `window_duration`, `queue_depth` | flush 时记录 |
| `item` | `item_index`（在批次中的序号，0-based） | 处理单个 item |
| `bypass` | `batch_id`, `item_count` | bypass 时记录 |
| `drain` | `remaining_items` | shutdown 时剩余 item 数 |

## 事件分级

| 级别 | 事件 | 触发时机 |
|------|------|---------|
| `ERROR` | 并发 task panic | `inflight.join_next()` 返回 Err(panic) |
| `WARN` | 队列满 | `submit` 返回 `QueueFull` |
| `WARN` | item 超时丢弃 | `drain_expired` / `drain` |
| `WARN` | 并发满跳过 flush | `flush_batch` 返回 false |
| `INFO` | 批次 flush 开始/完成 | `flush_inner` / `spawn_flush` 前后 |
| `INFO` | 窗口调整 | `adjust_window` 前后值不同 |
| `INFO` | 累加器启动/关闭 | `run` 开始 / `run` 结束 |
| `DEBUG` | item 进入 buffer | 主循环 `recv` 后 |
| `DEBUG` | bypass 交付 | `process_bypass` |
| `TRACE` | 单个 item 处理完成 | `send_reply` 成功 |

### INFO flush 事件字段

```rust
tracing::info!(
    batch_id,
    batch_size,
    execution_time_ms,
    items_remaining,
    window_ms,
    "批次处理完成"
);
```

### 去噪规则

- 窗口调整事件仅在窗口实际变化时发出（clamp 前对比）
- `TRACE` 级别 per-item 事件默认不开启，需 `with_trace_per_item(true)`
- 空批次（`flush_empty_batches=true` 触发）使用 `DEBUG` 级别

## 跨 Channel Span 传播

### 原理

`submit()` → mpsc channel → 主循环 → buffer → flush 链路中，span 上下文会因跨 task 而丢失。通过将 `Span` 随 item 数据一起传递来解决。

### 数据结构变更

```rust
pub(crate) struct ChannelItem<T, R> {
    pub value: T,
    pub deadline: Option<Instant>,
    pub reply: Option<oneshot::Sender<Result<R, AccumulatorError>>>,
    pub priority: Priority,
    /// 调用方 submit 时的 span（仅在 tracing feature 开启时存在）
    #[cfg(feature = "tracing")]
    pub parent_span: tracing::Span,
}

pub(crate) struct BufferItem<T, R> {
    value: T,
    deadline: Option<Instant>,
    reply: Option<oneshot::Sender<Result<R, AccumulatorError>>>,
    #[cfg(feature = "tracing")]
    parent_span: tracing::Span,
}
```

### 传播关系

- `submit()` 捕获 `tracing::Span::current()` 存入 `parent_span`
- `flush_inner` / `spawn_flush` 中创建 `item::<i>` span 时，用 `.follows_from(&parent_span)` 建立因果关系
- 并发模式：`spawn_flush` 在 spawn 前捕获当前 `batch` span，task 内 `.entered()` 恢复

### follows_from vs parent

```
提交方 span ──follows_from──→ item span ←──parent── batch span
```

item span 的父是 batch span。提交方 span 身处不同的时间线和调用栈，用 `follows_from` 表达"由它触发"的因果关系，而非调用父子。

## Config 集成

### 新增配置项

```rust
/// tracing 日志级别。None 表示不启用 tracing。
pub(crate) tracing_level: Option<tracing::Level>,

/// 是否记录 per-item TRACE 事件（高频，默认关闭）。
pub(crate) trace_per_item: bool,
```

### Builder 方法

```rust
/// 启用 tracing，设置最低记录级别。默认 INFO。
pub fn with_tracing_level(mut self, level: tracing::Level) -> Self;

/// 是否开启 per-item 追踪。默认 false。
pub fn with_trace_per_item(mut self, enabled: bool) -> Self;
```

### Feature off 处理

`trace.rs` 中定义类型别名：

```rust
#[cfg(feature = "tracing")]
pub(crate) type TraceLevel = Option<tracing::Level>;
#[cfg(not(feature = "tracing"))]
pub(crate) type TraceLevel = ();

#[cfg(feature = "tracing")]
pub(crate) type MaybeSpan = tracing::Span;
#[cfg(not(feature = "tracing"))]
pub(crate) type MaybeSpan = ();
```

feature 关闭时 `MaybeSpan` 是零大小类型，编译器优化掉所有读写；`tracing::event!` 宏不展开。

### 运行时过滤

库内自行过滤级别，避免无效事件构造：

```rust
macro_rules! event_if {
    ($level:expr, $threshold:expr, $($arg:tt)*) => {
        if let Some(threshold) = $threshold {
            if $level <= *threshold {
                tracing::event!($level, $($arg)*);
            }
        }
    };
}
```

不依赖 `tracing-subscriber` 的全局 filter，减少字符串/字段构造开销。

## Span 名称常量

```rust
// trace.rs
pub(crate) const SPAN_RUN: &str = "run";
pub(crate) const SPAN_BATCH: &str = "batch";
pub(crate) const SPAN_ITEM: &str = "item";
pub(crate) const SPAN_BYPASS: &str = "bypass";
pub(crate) const SPAN_DRAIN: &str = "drain";
```

## 并发模式特殊处理

### 串行模式
- 所有 span 在主循环 task 中自然嵌套
- 无需手动捕获/恢复

### 并发模式
- `spawn_flush` 在 `tokio::spawn` 前捕获当前 `batch` span
- 新 task 内 `.entered()` 恢复 span 上下文
- `InflightGuard` drop 时记录 task 完成事件

## 向后兼容

- 不改变任何公开 API 签名
- `ChannelItem`/`BufferItem` 增加的字段是 `pub(crate)` 可见，不影响外部
- 默认行为（`tracing` feature 开启 + `INFO` 级别）与当前行为一致（不输出，因为无 subscriber）
- 关闭 feature 后二进制大小和性能与当前一致

## 依赖

```toml
[dependencies]
tracing = { version = "0.1", default-features = false, optional = true }
```

仅新增 `tracing`，不引入 `tracing-subscriber`、`tracing-opentelemetry`、`opentelemetry` 等。这些由下游应用决定。

## 不做什么

- ❌ 不自动配置 subscriber（库的职责边界）
- ❌ 不引入 `opentelemetry` SDK 依赖（由下游通过 `tracing-opentelemetry` 层桥接）
- ❌ 不在 `StatsSnapshot` 中增加 tracing 字段（数值统计和结构化日志职责分离）
- ❌ 不记录 item 的 payload 内容（安全性 + 日志量不可控）
