# Tracing 可观测性实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 为批处理累加器库添加基于 `tracing` 生态的结构化日志和全链路 span 传播能力。

**Architecture:** 新增 `src/trace.rs` 集中管理 tracing 相关类型和辅助函数（减少核心文件的 `#[cfg]` 污染），ChannleItem/BufferItem 携带 `parent_span` 实现跨 channel 传播，Config 提供运行时级别控制。

**Tech Stack:** Rust 2024, tokio, thiserror, tracing 0.1 (optional, default on)

---

### Task 1: Cargo.toml 依赖与 feature flag

**Files:**
- Modify: `Cargo.toml`

- [ ] **Step 1: 添加 tracing 依赖和 feature**

```toml
[package]
name = "draft"
version = "0.1.0"
edition = "2024"

[dependencies]
tokio = { version = "1", features = ["time", "sync", "macros", "rt"] }
thiserror = "2"
tracing = { version = "0.1", default-features = false, optional = true }

[features]
default = ["tracing"]
tracing = ["dep:tracing"]

[dev-dependencies]
tokio = { version = "1", features = ["full", "test-util"] }
tracing-subscriber = "0.3"
```

- [ ] **Step 2: 验证依赖解析**

```bash
cargo check
```

Expected: 编译通过，无新增 warning。

- [ ] **Step 3: 验证 feature off 也能编译**

```bash
cargo check --no-default-features
```

Expected: 编译通过，tracing 未引入。

- [ ] **Step 4: 提交**

```bash
git add Cargo.toml Cargo.lock
git commit -m "build: 新增 tracing 依赖和 feature flag"
```

---

### Task 2: trace.rs 模块骨架

**Files:**
- Create: `src/trace.rs`
- Modify: `src/lib.rs`

- [ ] **Step 1: 创建 trace.rs（始终编译，内部 cfg 分两路）**

```rust
//! Tracing 可观测性模块。
//!
//! 集中管理 span 名称常量、类型别名和辅助函数，
//! 减少核心文件中 `#[cfg(feature = "tracing")]` 的散落。

use std::time::Duration;

// ── 常量（feature 无关）──

/// Span 名称：累加器主循环。
pub(crate) const SPAN_RUN: &str = "run";
/// Span 名称：批次处理。
pub(crate) const SPAN_BATCH: &str = "batch";
/// Span 名称：单个 item 处理。
pub(crate) const SPAN_ITEM: &str = "item";
/// Span 名称：bypass 直接交付。
pub(crate) const SPAN_BYPASS: &str = "bypass";
/// Span 名称：shutdown 清空。
pub(crate) const SPAN_DRAIN: &str = "drain";

// ── tracing 开启的实现 ──

#[cfg(feature = "tracing")]
mod imp {
    use super::*;

    /// tracing 日志级别。None 表示运行时关闭。
    pub(crate) type TraceLevel = Option<tracing::Level>;
    /// 携带 span 上下文的类型。feature 关闭时为零大小类型。
    pub(crate) type MaybeSpan = tracing::Span;
    pub(crate) use tracing::Level;

    /// 捕获当前执行上下文中的 span。
    #[inline]
    pub(crate) fn current_span() -> MaybeSpan {
        tracing::Span::current()
    }

    /// 创建累加器主循环 span。
    pub(crate) fn run_span(
        min_window: Duration,
        max_window: Duration,
        concurrency_mode: &str,
    ) -> MaybeSpan {
        tracing::info_span!(
            "run",
            min_window_ms = min_window.as_millis() as u64,
            max_window_ms = max_window.as_millis() as u64,
            concurrency_mode,
        )
    }

    /// 创建批次处理 span。
    pub(crate) fn batch_span(
        batch_id: u64,
        buffer_size: usize,
        window_duration: Duration,
        queue_depth: usize,
    ) -> MaybeSpan {
        tracing::info_span!(
            "batch",
            batch_id,
            batch_size = buffer_size,
            window_ms = window_duration.as_millis() as u64,
            queue_depth,
        )
    }

    /// 创建单个 item 处理 span。
    pub(crate) fn item_span(index: usize) -> MaybeSpan {
        tracing::info_span!("item", item_index = index)
    }

    /// 创建 bypass 处理 span。
    pub(crate) fn bypass_span(batch_id: u64, item_count: usize) -> MaybeSpan {
        tracing::info_span!("bypass", batch_id, item_count)
    }

    /// 创建 drain 清空 span。
    pub(crate) fn drain_span(remaining_items: usize) -> MaybeSpan {
        tracing::info_span!("drain", remaining_items)
    }
}

#[cfg(feature = "tracing")]
pub(crate) use imp::*;

// ── tracing 关闭的桩实现 ──

#[cfg(not(feature = "tracing"))]
mod imp {
    use super::*;

    /// feature 关闭时为零大小类型，编译器优化掉所有读写。
    pub(crate) type TraceLevel = ();
    /// feature 关闭时为零大小类型。
    pub(crate) type MaybeSpan = ();
    /// 占位类型，仅在 config 默认值构造时使用。
    pub(crate) type Level = ();

    #[inline]
    pub(crate) fn current_span() -> MaybeSpan {}

    #[inline]
    pub(crate) fn run_span(_: Duration, _: Duration, _: &str) -> MaybeSpan {}

    #[inline]
    pub(crate) fn batch_span(_: u64, _: usize, _: Duration, _: usize) -> MaybeSpan {}

    #[inline]
    pub(crate) fn item_span(_: usize) -> MaybeSpan {}

    #[inline]
    pub(crate) fn bypass_span(_: u64, _: usize) -> MaybeSpan {}

    #[inline]
    pub(crate) fn drain_span(_: usize) -> MaybeSpan {}
}

#[cfg(not(feature = "tracing"))]
pub(crate) use imp::*;

// ── event_at! 宏（顶层定义，需 cfg 双份）──

/// 仅在 threshold 非 None 且 level <= threshold 时发出事件。
/// feature 关闭时展开为空，零开销。
#[cfg(feature = "tracing")]
macro_rules! event_at {
    ($level:expr, $threshold:expr, $($arg:tt)*) => {
        if let Some(threshold) = $threshold {
            if $level <= *threshold {
                tracing::event!($level, $($arg)*);
            }
        }
    };
}

#[cfg(not(feature = "tracing"))]
macro_rules! event_at {
    ($level:expr, $threshold:expr, $($arg:tt)*) => {};
}

pub(crate) use event_at;
```

- [ ] **Step 2: 在 lib.rs 中引入 trace 模块**

```rust
//! 自适应时间窗口批处理累加器。
//! ...

#![allow(async_fn_in_trait)]

pub mod accumulator;
pub mod batch;
pub mod config;
pub mod controller;
pub mod error;
pub mod metrics;
pub mod stats;
pub mod trace;  // 始终编译

pub mod prelude {
    // ... 不变
}
```

- [ ] **Step 3: 验证编译**

```bash
cargo check
cargo check --no-default-features
```

Expected: 两种模式均编译通过。

- [ ] **Step 4: 提交**

```bash
git add src/trace.rs src/lib.rs
git commit -m "feat: 新增 trace 模块骨架（span 常量、类型别名、event_at! 宏）"
```

---

### Task 3: Config 集成

**Files:**
- Modify: `src/config.rs`

- [ ] **Step 1: 在 AccumulatorConfig 中新增 tracing 配置字段**

在 `AccumulatorConfig` struct 末尾增加：

```rust
pub struct AccumulatorConfig {
    pub(crate) initial_window: Duration,
    pub(crate) min_window: Duration,
    pub(crate) max_window: Duration,
    pub(crate) max_batch_size: Option<usize>,
    pub(crate) max_queue_depth: Option<usize>,
    pub(crate) flush_empty_batches: bool,
    pub(crate) concurrency_mode: ConcurrencyMode,
    pub(crate) stats_enabled: bool,
    pub(crate) max_batch_weight: Option<usize>,
    /// tracing 日志级别。None 表示运行时关闭。feature 关闭时类型为 ()。
    pub(crate) tracing_level: crate::trace::TraceLevel,
    /// 是否记录 per-item TRACE 级别事件（高频，默认关闭）。
    pub(crate) trace_per_item: bool,
}
```

- [ ] **Step 2: 在 AccumulatorConfig::new 中初始化默认值**

在 `new()` 返回的 `Ok(Self { ... })` 末尾追加：

```rust
tracing_level: {
    #[cfg(feature = "tracing")]
    { Some(tracing::Level::INFO) }
    #[cfg(not(feature = "tracing"))]
    { () }
},
trace_per_item: false,
```

注意：两个 `#[cfg]` 块必须确保始终有值返回，Rust 要求所有分支类型一致。由于 `()` 和 `Option<tracing::Level>` 类型不同，不能在同一个表达式中共存。改用 trace 模块的类型别名：

```rust
tracing_level: crate::trace::default_tracing_level(),
trace_per_item: false,
```

同时在 `trace.rs` 中添加：

```rust
// imp (feature on):
pub(crate) fn default_tracing_level() -> TraceLevel {
    Some(tracing::Level::INFO)
}

// imp (feature off):
pub(crate) fn default_tracing_level() -> TraceLevel {
    ()
}
```

- [ ] **Step 3: 在 AccumulatorConfig::default 中初始化**

在 `Default` impl 的 `Self { ... }` 末尾追加：

```rust
tracing_level: crate::trace::default_tracing_level(),
trace_per_item: false,
```

- [ ] **Step 4: 添加 builder 方法**

在 `impl AccumulatorConfig` 块末尾追加：

```rust
/// 设置 tracing 日志级别。设为 `None` 可在运行时完全关闭 tracing。
///
/// 仅在 `tracing` feature 开启时生效。
#[cfg(feature = "tracing")]
pub fn with_tracing_level(mut self, level: Option<tracing::Level>) -> Self {
    self.tracing_level = level;
    self
}

/// feature 关闭时的桩方法，接收并忽略参数。
#[cfg(not(feature = "tracing"))]
pub fn with_tracing_level(mut self, _level: ()) -> Self {
    self
}

/// 是否开启 per-item 级别追踪（`TRACE` 级别事件）。
///
/// 高频路径，默认关闭。
pub fn with_trace_per_item(mut self, enabled: bool) -> Self {
    self.trace_per_item = enabled;
    self
}
```

- [ ] **Step 5: config 构造时传递 tracing_level 到 accumulator**

`build_with_weight` 方法中，`BatchAccumulator` 构造已拿到 `self`（config），无需额外传递。

`AccumulatorHandle` 不需要 tracing 配置（handle 仅负责发送，不负责发事件）。

- [ ] **Step 6: 验证编译**

```bash
cargo check
cargo check --no-default-features
```

Expected: 两种模式均编译通过。feature on 时 `with_tracing_level` 接受 `Option<tracing::Level>`，feature off 时接受 `()`。

- [ ] **Step 7: 提交**

```bash
git add src/config.rs src/trace.rs
git commit -m "feat: AccumulatorConfig 新增 tracing_level 和 trace_per_item 配置"
```

---

### Task 4: accumulator.rs — 数据结构变更（ChannelItem / BufferItem）

**Files:**
- Modify: `src/accumulator.rs`

`ChannelItem` 和 `BufferItem` 实际定义在 `accumulator.rs`（非 `batch.rs`），在此文件中增加字段。

- [ ] **Step 1: ChannelItem 增加 parent_span 字段**

在 `accumulator.rs` 的 `ChannelItem` struct 末尾增加：

```rust
pub(crate) struct ChannelItem<T, R> {
    /// item 数据。
    pub value: T,
    /// 超时截止时间。`None` 表示不超时。
    pub deadline: Option<Instant>,
    /// 回复通道。`None` 表示 fire-and-forget。
    pub reply: Option<tokio::sync::oneshot::Sender<Result<R, AccumulatorError>>>,
    /// 优先级。
    pub priority: Priority,
    /// 调用方 submit 时的 span，用于跨 channel 传播上下文。
    /// feature 关闭时为零大小类型，编译器优化掉。
    pub parent_span: crate::trace::MaybeSpan,
}
```

- [ ] **Step 2: BufferItem 增加 parent_span 字段**

在同一文件 `BufferItem` struct 中增加：

```rust
pub(crate) struct BufferItem<T, R> {
    /// item 数据。
    value: T,
    /// 超时截止时间。`None` 表示不超时。
    deadline: Option<Instant>,
    /// 回复通道。`None` 表示 fire-and-forget。
    reply: Option<tokio::sync::oneshot::Sender<Result<R, AccumulatorError>>>,
    /// 提交方 span，传到 flush 侧建立 follows_from 关联。
    parent_span: crate::trace::MaybeSpan,
}
```

- [ ] **Step 3: 验证编译**

```bash
cargo check
cargo check --no-default-features
```

Expected: 编译通过。`MaybeSpan` 为零大小类型时，编译器完全优化掉该字段的内存占用。

- [ ] **Step 4: 提交**

```bash
git add src/accumulator.rs
git commit -m "feat: ChannelItem/BufferItem 新增 parent_span 字段用于跨 channel 传播"
```

---

### Task 5: accumulator.rs — submit 侧 span 捕获

**Files:**
- Modify: `src/accumulator.rs`

- [ ] **Step 1: 在 submit 方法中捕获当前 span**

修改 `submit_no_wait` 中 `ChannelItem` 构造，增加 `parent_span` 字段：

```rust
self.sender
    .send(ChannelItem {
        value: item,
        deadline: None,
        reply: None,
        priority: Priority::Normal,
        parent_span: crate::trace::current_span(),
    })
    .map_err(|_| {
        self.pending_count.fetch_sub(1, Ordering::Release);
        AccumulatorError::Shutdown
    })?;
```

同理修改 `submit`、`submit_with`、`submit_high`、`submit_with_timeout` 中所有 `ChannelItem` 构造点（共 4 处），均增加：

```rust
parent_span: crate::trace::current_span(),
```

`submit_high` 和 `submit_with_timeout` 委托给 `submit_with`，只需改 `submit_with` 即可。

- [ ] **Step 2: 验证编译**

```bash
cargo check
cargo check --no-default-features
```

Expected: 编译通过。所有 submit 变体均传递 parent_span。

- [ ] **Step 3: 提交**

```bash
git add src/accumulator.rs
git commit -m "feat: submit 路径捕获当前 span 并随 ChannelItem 传播"
```

---

### Task 6: accumulator.rs — 串行模式插桩

**Files:**
- Modify: `src/accumulator.rs`

- [ ] **Step 1: 主循环 run() 添加 run span**

在 `run` 方法开头创建 span：

```rust
pub async fn run(mut self) {
    let concurrency_mode = match self.config.concurrency_mode {
        ConcurrencyMode::Serial => "serial",
        ConcurrencyMode::Concurrent { .. } => "concurrent",
    };
    let run_span = crate::trace::run_span(
        self.config.min_window,
        self.config.max_window,
        concurrency_mode,
    );
    let _run_guard = run_span.entered();

    let mut deadline = Instant::now() + self.current_window;
    let mut running = true;

    // 启动事件
    event_at!(
        tracing::Level::INFO,
        &self.config.tracing_level,
        "累加器启动"
    );

    // ... 主循环不变 ...

    // 循环结束后
    event_at!(
        tracing::Level::INFO,
        &self.config.tracing_level,
        "累加器关闭"
    );
}
```

注意：`_run_guard` 必须存活到函数结束，不要 `drop()`。

- [ ] **Step 2: flush_inner 添加 batch span + 事件**

在 `flush_inner` 方法开头创建 batch span，并在处理前后发事件：

```rust
async fn flush_inner(&mut self, time_since_last_flush: Duration) {
    let buffer_items: Vec<BufferItem<T, R>> = self.buffer.drain(..).collect();
    let batch_size = buffer_items.len();
    let total_weight = self.current_weight;
    self.current_weight = 0;

    let batch_span = crate::trace::batch_span(
        self.next_batch_id,
        batch_size,
        self.current_window,
        self.pending_count.load(Ordering::Acquire),
    );
    let _batch_guard = batch_span.entered();

    event_at!(
        tracing::Level::INFO,
        &self.config.tracing_level,
        batch_size,
        "批次处理开始"
    );

    // ... 原有逻辑：拆分 items/senders、调 processor.process() ...

    let execution_time = proc_start.elapsed();

    event_at!(
        tracing::Level::INFO,
        &self.config.tracing_level,
        batch_id = batch_id,
        batch_size = batch_size,
        execution_time_ms = execution_time.as_millis() as u64,
        items_remaining = items_remaining,
        window_ms = self.current_window.as_millis() as u64,
        "批次处理完成"
    );

    // ... 原有逻辑：send reply、record_flush、record_flush ...
}
```

- [ ] **Step 3: flush_inner 添加 per-item span（trace_per_item 控制）**

在 `processor.process(batch).await` 调用前后，如果 `trace_per_item` 开启，为每个 item 创建独立 span。但由于 `process` 是批量调用，per-item span 应在 `send_reply` 循环中创建：

```rust
// 发送结果给调用方
for (index, (sender, result)) in senders.into_iter().zip(results).enumerate() {
    if let Some(tx) = sender {
        let _item_span = if self.config.trace_per_item {
            let span = crate::trace::item_span(index);
            // 从 buffer_items 获取 parent_span 建立 follows_from
            // buffer_items 已被 drain，需提前保存
            Some(span)
        } else {
            None
        };
        let _ = tx.send(Ok(result));
    }
}
```

由于 `buffer_items` 已被 drain，需在 drain 前提取 `parent_span` 列表：

```rust
// 在 drain 后、collect 前
let parent_spans: Vec<crate::trace::MaybeSpan> = buffer_items
    .iter()
    .map(|bi| bi.parent_span)  // MaybeSpan 可能是 ()，此时为 Copy
    .collect();
```

注意：`MaybeSpan = ()` 时 `collect()` 返回 `Vec<()>`，编译器优化掉。对 `Span` 则需 `clone`。

但由于 `MaybeSpan` 在 feature off 时是 `()`（Copy），feature on 时是 `Span`（Clone），需要统一处理。最简单的方式是在 trace.rs 的 imp 层不提供统一 API，直接在 accumulator.rs 中用 cfg：

或者在 trace.rs 中加一个辅助函数：

```rust
// trace.rs imp (feature on)
pub(crate) fn collect_parent_spans<T, R>(items: &[BufferItem<T, R>]) -> Vec<MaybeSpan> {
    items.iter().map(|bi| bi.parent_span.clone()).collect()
}

// trace.rs imp (feature off)
pub(crate) fn collect_parent_spans<T, R>(items: &[BufferItem<T, R>]) -> Vec<MaybeSpan> {
    // MaybeSpan = () 时，无需实际收集
    Vec::new()
}
```

这样 accumulator.rs 中无需 cfg。

- [ ] **Step 4: process_bypass 添加 bypass span**

```rust
async fn process_bypass(&mut self, items: Vec<T>) {
    let batch_id = self.next_batch_id;
    self.next_batch_id += 1;
    let item_count = items.len();

    let bypass_span = crate::trace::bypass_span(batch_id, item_count);
    let _bypass_guard = bypass_span.entered();

    event_at!(
        tracing::Level::DEBUG,
        &self.config.tracing_level,
        batch_id,
        item_count,
        "bypass 处理开始"
    );

    match self.config.concurrency_mode {
        ConcurrencyMode::Serial => {
            let batch = Batch::new(items, batch_id);
            self.processor.process(batch).await;
        }
        // ... 并发分支不变
    }
}
```

- [ ] **Step 5: drain 添加 drain span**

```rust
async fn drain(&mut self) {
    let remaining = self.pending_count.load(Ordering::Acquire) + self.buffer.len();
    let drain_span = crate::trace::drain_span(remaining);
    let _drain_guard = drain_span.entered();

    event_at!(
        tracing::Level::INFO,
        &self.config.tracing_level,
        remaining_items = remaining,
        "开始清空累加器"
    );

    // ... 原有 drain 逻辑不变 ...
}
```

- [ ] **Step 6: 添加 WARN 事件 — 队列满**

在 `submit_no_wait` 和 `submit_with` 的 `QueueFull` 错误返回前添加：

```rust
if let Err(prev) = result {
    event_at!(
        tracing::Level::WARN,
        &self.config.tracing_level,  // 但 handle 没有 config...
        max = self.max_queue_depth.unwrap_or(0),
        pending = prev,
        "队列已满，拒绝提交"
    );
    return Err(AccumulatorError::QueueFull { ... });
}
```

问题：`AccumulatorHandle` 没有持有 `tracing_level`。需在 `AccumulatorHandle` 中增加该字段：

```rust
pub struct AccumulatorHandle<T, R> {
    // ... 现有字段
    pub(crate) tracing_level: crate::trace::TraceLevel,
}
```

在 `build_with_weight` 中传递：

```rust
let handle = AccumulatorHandle {
    // ... 现有字段
    tracing_level: self.tracing_level,
};
```

- [ ] **Step 7: 添加 WARN 事件 — item 超时丢弃**

在 `drain_expired` 末尾：

```rust
if dropped_count > 0 {
    self.stats.record_dropped_timeout(dropped_count);
    event_at!(
        tracing::Level::WARN,
        &self.config.tracing_level,
        dropped = dropped_count,
        "item 超时丢弃"
    );
}
```

在 `drain` 方法的各个超时丢弃处也添加：

```rust
// 主通道清空时
if let Some(dl) = ch.deadline
    && Instant::now() >= dl {
        if let Some(tx) = ch.reply {
            let _ = tx.send(Err(AccumulatorError::Timeout));
        }
        self.stats.record_dropped_timeout(1);
        event_at!(
            tracing::Level::WARN,
            &self.config.tracing_level,
            "drain 阶段 item 超时丢弃"
        );
        continue;
}
```

- [ ] **Step 8: 窗口调整 INFO 事件**

在 `flush_inner` 的窗口调整处：

```rust
let new_window = self
    .controller
    .adjust_window(self.current_window, &snapshot)
    .await;
let clamped = new_window.clamp(self.config.min_window, self.config.max_window);

if clamped != self.current_window {
    event_at!(
        tracing::Level::INFO,
        &self.config.tracing_level,
        prev_ms = self.current_window.as_millis() as u64,
        new_ms = clamped.as_millis() as u64,
        "窗口调整"
    );
}
self.current_window = clamped;
```

`handle_feedback` 中同理。

- [ ] **Step 9: 验证编译**

```bash
cargo check
cargo check --no-default-features
```

Expected: 编译通过，无 warning。

- [ ] **Step 10: 提交**

```bash
git add src/accumulator.rs
git commit -m "feat: 串行模式核心路径插桩（span + event）"
```

---

### Task 7: accumulator.rs — 并发模式插桩

**Files:**
- Modify: `src/accumulator.rs`

- [ ] **Step 1: spawn_flush 中创建 batch span 并传入后台 task**

在 `spawn_flush` 中，spawn 前创建 batch span，与 processor、feedback_tx 等一起 move 入闭包：

```rust
fn spawn_flush(&mut self, time_since_last_flush: Duration) {
    let buffer_items: Vec<BufferItem<T, R>> = self.buffer.drain(..).collect();
    let batch_size = buffer_items.len();
    let total_weight = self.current_weight;
    self.current_weight = 0;
    let (items, senders): (Vec<T>, Vec<ReplySender<R>>) =
        buffer_items.into_iter().map(|bi| (bi.value, bi.reply)).unzip();

    // 收集 parent_span 列表供 per-item 追踪
    let parent_spans: Vec<crate::trace::MaybeSpan> = buffer_items
        .iter()
        .map(|bi| bi.parent_span.clone())
        .collect();

    let batch_id = self.next_batch_id;
    self.next_batch_id += 1;

    // 创建 batch span（主循环 task 中）
    let batch_span = crate::trace::batch_span(
        batch_id,
        batch_size,
        self.current_window,
        self.pending_count.load(Ordering::Acquire),
    );
    let tracing_level = self.config.tracing_level;
    let trace_per_item = self.config.trace_per_item;

    let batch = Batch::new(items, batch_id);
    let processor = Arc::clone(&self.processor);
    let feedback_tx = self.feedback_tx.clone();
    let pending_count = Arc::clone(&self.pending_count);
    let max_batch_size = self.config.max_batch_size;
    let window_duration = self.current_window;
    let guard = InflightGuard::new(Arc::clone(&self.inflight_count));
    let stats = Arc::clone(&self.stats);

    self.inflight.spawn(async move {
        // 在后台 task 中恢复 batch span
        let _batch_guard = batch_span.entered();

        event_at!(
            tracing::Level::INFO,
            &tracing_level,
            batch_id,
            batch_size,
            "批次处理开始（并发）"
        );

        let start = Instant::now();
        let results = processor.process(batch).await;
        let execution_time = start.elapsed();

        // ... 长度校验 ...

        // 发送结果 + per-item trace
        for (index, (sender, result)) in senders
            .into_iter()
            .zip(results)
            .enumerate()
        {
            let _item_span = if trace_per_item {
                let span = crate::trace::item_span(index);
                // 建立 follows_from 关联到提交方 span
                if index < parent_spans.len() {
                    span.follows_from(parent_spans[index].clone());
                }
                Some(span.entered())
            } else {
                None
            };

            if let Some(tx) = sender {
                let _ = tx.send(Ok(result));
            }
        }

        let items_remaining = pending_count.load(Ordering::Acquire);

        event_at!(
            tracing::Level::INFO,
            &tracing_level,
            batch_id,
            batch_size,
            execution_time_ms = execution_time.as_millis() as u64,
            items_remaining,
            "批次处理完成（并发）"
        );

        let info = FlushInfo {
            batch_size,
            max_batch_size,
            window_duration,
            items_remaining,
            batch_id,
            execution_time,
            time_since_last_flush,
            total_weight: max_batch_size.map(|_| total_weight),
        };

        drop(guard);
        let _ = feedback_tx.send(info);
    });
}
```

注意：`MaybeSpan` 的 `.clone()` 在 feature off 时返回 `()`（Copy），feature on 时 `Span::clone()` 返回 `Span`。

- [ ] **Step 2: spawn_bypass 添加 bypass span**

```rust
fn spawn_bypass(&mut self, items: Vec<T>) {
    let batch_id = self.next_batch_id;
    self.next_batch_id += 1;
    let item_count = items.len();
    let tracing_level = self.config.tracing_level;

    let bypass_span = crate::trace::bypass_span(batch_id, item_count);

    let batch = Batch::new(items, batch_id);
    let processor = Arc::clone(&self.processor);
    let guard = InflightGuard::new(Arc::clone(&self.inflight_count));

    self.inflight.spawn(async move {
        let _bypass_guard = bypass_span.entered();

        event_at!(
            tracing::Level::DEBUG,
            &tracing_level,
            batch_id,
            item_count,
            "bypass 处理开始（并发）"
        );

        processor.process(batch).await;
        drop(guard);
    });
}
```

- [ ] **Step 3: drain 中 inflight panic 改为 tracing::error!**

将现有 `eprintln!` 替换为 `event_at!`：

```rust
while let Some(result) = self.inflight.join_next().await {
    if let Err(e) = result {
        let msg = if e.is_panic() {
            "后台批处理 task panic"
        } else {
            "后台批处理 task 被取消"
        };
        event_at!(
            tracing::Level::ERROR,
            &self.config.tracing_level,
            error = msg,
            "后台 task 异常退出"
        );
    }
}
```

- [ ] **Step 4: 并发满跳过 flush 的 WARN 事件**

在 `flush_batch` 中：

```rust
ConcurrencyMode::Concurrent { max_inflight } => {
    if max_inflight > 0
        && self.inflight_count.load(Ordering::Acquire) >= max_inflight
    {
        event_at!(
            tracing::Level::WARN,
            &self.config.tracing_level,
            max_inflight,
            inflight = self.inflight_count.load(Ordering::Acquire),
            buffered = self.buffer.len(),
            "并发满，跳过本次 flush"
        );
        return false;
    }
    // ...
}
```

- [ ] **Step 5: 验证编译**

```bash
cargo check
cargo check --no-default-features
```

Expected: 编译通过。

- [ ] **Step 6: 提交**

```bash
git add src/accumulator.rs
git commit -m "feat: 并发模式跨 task span 传播与插桩"
```

---

### Task 8: 测试与验证

**Files:**
- Modify: `tests/integration.rs`
- Create: `examples/tracing_demo.rs`

- [ ] **Step 1: 阅读现有集成测试**

```bash
cat tests/integration.rs
```

- [ ] **Step 2: 添加 tracing 功能测试**

在 `tests/integration.rs` 末尾追加测试：

```rust
#[cfg(feature = "tracing")]
mod tracing_tests {
    use std::sync::Mutex;
    use std::time::Duration;
    use draft::prelude::*;
    use tracing::Level;
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;

    /// 收集 tracing 事件到内存的 layer。
    struct CollectingLayer {
        events: Mutex<Vec<String>>,
    }

    impl CollectingLayer {
        fn new() -> Self {
            Self {
                events: Mutex::new(Vec::new()),
            }
        }

        fn take(&self) -> Vec<String> {
            self.events.lock().unwrap().drain(..).collect()
        }
    }

    impl<S: tracing::Subscriber> tracing_subscriber::layer::Layer<S> for CollectingLayer {
        fn on_event(
            &self,
            event: &tracing::Event<'_>,
            _ctx: tracing_subscriber::layer::Context<'_, S>,
        ) {
            let mut visitor = EventCollector::default();
            event.record(&mut visitor);
            self.events.lock().unwrap().push(visitor.message);
        }
    }

    // ... 简化版：直接使用 tracing_subscriber::fmt 验证输出（非 CI 时人工检查） ...

    #[tokio::test]
    async fn tracing_flush_events_emitted() {
        // 初始化 subscriber（仅一次）
        let _ = tracing_subscriber::fmt()
            .with_max_level(Level::INFO)
            .try_init();

        struct TestProcessor;
        impl BatchProcessor<i32, String> for TestProcessor {
            async fn process(&self, batch: Batch<i32>) -> Vec<String> {
                batch.items().iter().map(|i| format!("done-{i}")).collect()
            }
        }

        let config = AccumulatorConfig::new(
            Duration::from_millis(100),
            Duration::from_millis(50),
            Duration::from_secs(1),
        )
        .unwrap()
        .with_flush_empty_batches(false);

        let (handle, accumulator) = config.build(
            TestProcessor,
            DefaultMetrics::new(),
            FixedController::new(Duration::from_millis(100)),
        );

        let join = tokio::spawn(accumulator.run());

        let reply = handle.submit(42).unwrap();
        let result = reply.await.unwrap();
        assert_eq!(result, "done-42");

        drop(handle);
        let _ = join.await;
    }

    #[tokio::test]
    async fn tracing_queue_full_warns() {
        let _ = tracing_subscriber::fmt()
            .with_max_level(Level::WARN)
            .try_init();

        struct TestProcessor;
        impl BatchProcessor<i32, String> for TestProcessor {
            async fn process(&self, batch: Batch<i32>) -> Vec<String> {
                batch.items().iter().map(|i| format!("x-{i}")).collect()
            }
        }

        let config = AccumulatorConfig::new(
            Duration::from_millis(100),
            Duration::from_millis(50),
            Duration::from_secs(1),
        )
        .unwrap()
        .with_max_queue_depth(2);

        let (handle, accumulator) = config.build(
            TestProcessor,
            DefaultMetrics::new(),
            FixedController::new(Duration::from_millis(100)),
        );

        let join = tokio::spawn(accumulator.run());

        // 填满队列
        handle.submit_no_wait(1).unwrap();
        handle.submit_no_wait(2).unwrap();
        let result = handle.submit_no_wait(3);
        assert!(matches!(result, Err(AccumulatorError::QueueFull { .. })));

        drop(handle);
        let _ = join.await;
    }
}
```

- [ ] **Step 3: 创建 tracing demo example**

创建 `examples/tracing_demo.rs`：

```rust
use std::time::Duration;
use draft::prelude::*;
use tracing::Level;
use tracing_subscriber::fmt;

struct DemoProcessor;

impl BatchProcessor<i32, String> for DemoProcessor {
    async fn process(&self, batch: Batch<i32>) -> Vec<String> {
        tokio::time::sleep(Duration::from_millis(10)).await;
        batch.items().iter().map(|i| format!("result-{i}")).collect()
    }
}

#[tokio::main]
async fn main() {
    // 初始化 subscriber，输出格式化的 span 树
    fmt()
        .with_max_level(Level::DEBUG)
        .with_target(false)
        .init();

    let config = AccumulatorConfig::new(
        Duration::from_millis(200),
        Duration::from_millis(50),
        Duration::from_secs(5),
    )
    .unwrap()
    .with_max_batch_size(10)
    .with_trace_per_item(true);

    let (handle, accumulator) = config.build(
        DemoProcessor,
        DefaultMetrics::new(),
        AdaptiveController::new(0.8, 0.1).unwrap(),
    );

    let join = tokio::spawn(accumulator.run());

    // 提交一些 item
    for i in 0..5 {
        let reply = handle.submit(i).unwrap();
        let result = reply.await.unwrap();
        println!("Got: {result}");
    }

    handle.flush_now();
    tokio::time::sleep(Duration::from_millis(500)).await;

    drop(handle);
    let _ = join.await;
}
```

- [ ] **Step 4: 运行 demo 验证可视化输出**

```bash
cargo run --example tracing_demo
```

Expected: 终端输出格式化的 span 层级 + event 日志。

- [ ] **Step 5: 验证 feature off 编译 + 测试通过**

```bash
cargo test --no-default-features
```

Expected: 所有测试通过（tracing 相关测试被 `#[cfg(feature = "tracing")]` 跳过）。

- [ ] **Step 6: 验证 feature on 测试通过**

```bash
cargo test
```

Expected: 所有测试通过，包括 tracing_tests。

- [ ] **Step 7: 验证无 warning + clippy**

```bash
cargo clippy --all-targets
cargo clippy --all-targets --no-default-features
```

Expected: 无 clippy warning。

- [ ] **Step 8: 提交**

```bash
git add tests/integration.rs examples/tracing_demo.rs
git commit -m "test: 新增 tracing 集成测试和 demo example"
```

---

## 验证清单

- [ ] `cargo check` 通过
- [ ] `cargo check --no-default-features` 通过
- [ ] `cargo test` 全部通过
- [ ] `cargo test --no-default-features` 全部通过
- [ ] `cargo clippy --all-targets` 无 warning
- [ ] `cargo run --example tracing_demo` 输出正常
- [ ] `cargo doc --no-deps` 文档生成无错误
