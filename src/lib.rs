//! 自适应时间窗口批处理累加器。
//!
//! 在可配置的时间窗口内积攒 item，到期后整批交付用户定义的处理器。
//! 时间窗口基于批利用率或执行延迟**自适应调整**。支持串行/并发处理、
//! 优先级、可失败处理器、tracing 可观测性。
//!
//! # 快速开始
//!
//! ```rust,ignore
//! use std::time::Duration;
//! use draft::prelude::*;
//!
//! struct MyProcessor;
//! impl BatchProcessor<i32> for MyProcessor {
//!     async fn process(&self, batch: Batch<i32>) -> Vec<()> {
//!         println!("处理批次 {}: {} 条", batch.batch_id(), batch.len());
//!         vec![(); batch.len()]
//!     }
//! }
//!
//! let config = AccumulatorConfig::new(
//!     Duration::from_millis(200),
//!     Duration::from_millis(50),
//!     Duration::from_secs(5),
//! )?
//! .with_max_batch_size(100);
//!
//! let (handle, _jh) = config.build_and_spawn(
//!     MyProcessor,
//!     DefaultMetrics::new(),
//!     AdaptiveController::new(0.8, 0.1)?,
//! );
//!
//! let reply = handle.submit(42)?;
//! let result = reply.await?;  // 拿到处理结果
//! ```
//!
//! # 核心概念
//!
//! - **时间窗口**：item 在缓冲区内积攒的时间。到期后整批交付处理器。
//!   - 最小窗口 (`min_window`) 和最大窗口 (`max_window`) 限制自适应范围。
//! - **自适应控制**：每次 flush 后根据指标调整窗口大小。
//!   - [`AdaptiveController`](crate::controller::AdaptiveController)：批利用率低 → 窗口增大，利用率高 → 窗口缩小。
//!   - [`LatencyAdaptiveController`](crate::controller::LatencyAdaptiveController)：执行变慢 → 窗口缩小，执行变快 → 窗口增大。
//!   - [`FixedController`](crate::controller::FixedController)：固定窗口，不做自适应。
//! - **并发模式**：
//!   - [`ConcurrencyMode::Serial`](crate::config::ConcurrencyMode::Serial)（默认）：批次在主循环中同步处理。
//!   - [`ConcurrencyMode::Concurrent`](crate::config::ConcurrencyMode::Concurrent)：批次在后台 tokio task 中处理，主循环可继续收集 item。
//!
//! # 特性
//!
//! | 特性 | 说明 |
//! |------|------|
//! | 自适应窗口 | 5 种控制器：Fixed / Adaptive / Latency / PID / Backoff |
//! | 串行/并发 | 两种处理模式 |
//! | 优先级 | Normal / High 两级，高优先级插队 |
//! | 可失败处理器 | `TryBatchProcessor` 逐项返回 `Result<R, E>` |
//! | 超时控制 | item 级别 TTL + drain 超时 |
//! | 权重追踪 | 按 item "重量" 触发提前 flush |
//! | 阻塞提交 | 队列满时等待空位（`submit_or_wait`） |
//! | 暂停/恢复 | `pause()`/`resume()` 缓冲不 flush + `cancel()` 优雅关闭 |
//! | bypass | 跳过批处理，直接交付 |
//! | 恐慌恢复 | processor panic 被隔离，不影响 accumulator |
//! | 可观测性 | tracing + stats 快照 + health + 队列等待时间 |
//! | 零开销抽象 | tracing feature 可关闭，编译器优化掉所有桩代码 |
//!
//! # 选择处理器
//!
//! | 处理器类型 | 适用场景 |
//! |-----------|---------|
//! | `NoopProcessor`（自定义） | 基准测试 / 丢弃数据 |
//! | 批量写数据库 | `max_batch_size=500`，窗口 1s |
//! | 批量发 HTTP | `Concurrent { max_inflight: 8 }` |
//! | 低延迟 bypass | 紧急事件用 `handle.bypass(item)` |
//!
//! # 调参建议
//!
//! - 初始窗口 = `max_batch_size / 预期QPS`。例如 QPS=500, max_batch_size=100 → 初始窗口 ≈ 200ms。
//! - `adjustment_rate` 从小开始（0.05），观察收敛曲线后逐步增大。
//! - `drain_timeout` 设置为单批处理的 p99 延迟 × 3。

// async fn in trait 是有意设计选择：
// - 零开销：无需 Box<dyn Future> 堆分配，编译器内联生成
// - 生态一致：draft 面向 tokio 生态，用户已在 stable 2024 edition 上使用
// - 备选方案 `async_trait` 会增加 proc-macro 依赖和间接调用开销
#![allow(async_fn_in_trait)]

pub mod accumulator;
pub mod batch;
pub mod config;
pub mod controller;
pub mod error;
pub mod metrics;
pub mod stats;
pub mod trace;

/// 常用类型和 trait 的批量导入。
pub mod prelude {
    pub use crate::accumulator::AccumulatorHandle;
    pub use crate::batch::{
        Batch, BatchProcessor, FlushInfo, Priority, ReplyHandle, SubmitOptions,
        TryBatchProcessor,
    };
    pub use crate::config::{AccumulatorConfig, ConcurrencyMode};
    pub use crate::controller::{
        AdaptiveController, BackoffController, FixedController,
        LatencyAdaptiveController, PIDController, WindowController,
    };
    pub use crate::error::AccumulatorError;
    pub use crate::metrics::{
        DefaultMetrics, MetricsCollector, MetricsSnapshot,
    };
    pub use crate::stats::{AccumulatorHealth, StatsSnapshot};
}
