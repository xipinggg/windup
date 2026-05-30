//! 自适应时间窗口批处理累加器。
//!
//! 本库提供了一个通用的批处理框架，可在可配置的时间窗口内积攒 item，
//! 到期后整批交付用户定义的处理器。时间窗口基于自定义指标自适应调整。
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
//! let (handle, accumulator) = config.build(
//!     MyProcessor,
//!     DefaultMetrics::new(),
//!     AdaptiveController::new(0.8, 0.1)?,
//! );
//!
//! let join_handle = tokio::spawn(accumulator.run());
//! let reply = handle.submit(42)?;
//! let result = reply.await?;  // 拿到处理结果
//! ```
//!
//! # 自适应行为
//!
//! 内置两种控制器：
//! - [`AdaptiveController`]：基于批利用率调整窗口
//! - [`LatencyAdaptiveController`]：基于执行时间 vs EMA 基准调整窗口
//!
//! 用户可实现 [`MetricsCollector`] + [`WindowController`] 自定义策略。

// async fn in trait 是有意设计选择，用户无需关心 Send bound。
#![allow(async_fn_in_trait)]

pub mod accumulator;
pub mod batch;
pub mod config;
pub mod controller;
pub mod error;
pub mod metrics;
pub mod stats;

/// 常用类型和 trait 的批量导入。
pub mod prelude {
    pub use crate::accumulator::AccumulatorHandle;
    pub use crate::batch::{Batch, BatchProcessor, Priority, ReplyHandle, SubmitOptions};
    pub use crate::config::{AccumulatorConfig, ConcurrencyMode};
    pub use crate::controller::{
        AdaptiveController, FixedController, LatencyAdaptiveController, WindowController,
    };
    pub use crate::error::AccumulatorError;
    pub use crate::metrics::{
        DefaultMetrics, MetricsCollector, MetricsSnapshot, BYPASS_DRAIN_LIMIT, DEFAULT_EMA_ALPHA,
    };
    pub use crate::stats::StatsSnapshot;
}
