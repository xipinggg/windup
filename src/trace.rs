//! Tracing 可观测性模块。
//!
//! 集中管理 span 名称常量、类型别名和辅助函数，
//! 减少核心文件中 `#[cfg(feature = "tracing")]` 的散落。

use std::time::Duration;

// ── 常量（feature 无关）──
// Span 名称在各 span 创建函数中内联使用字符串字面量。

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

    /// 返回默认的 tracing 级别（INFO）。
    pub(crate) fn default_tracing_level() -> TraceLevel {
        Some(tracing::Level::INFO)
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
    /// 零大小 span 类型，提供与 tracing Span 兼容的方法签名。
    /// feature 关闭时所有方法均为空操作。
    ///
    /// 注意：不实现 Copy，因为 feature 开启时 tracing::Span 也不实现 Copy，
    /// 这样两套配置下 `.clone()` 的行为保持一致，避免 clippy::clone_on_copy 警告。
    #[derive(Clone)]
    pub(crate) struct MaybeSpan;

    impl MaybeSpan {
        /// 进入 span（空操作），返回自身作为 guard。
        #[inline]
        pub(crate) fn entered(self) -> Self {
            self
        }

        /// 建立 follows_from 关联（空操作）。
        #[inline]
        pub(crate) fn follows_from(&self, _: Self) {}

        /// 将 future 包装到 span 中（空操作，直接返回 future）。
        #[inline]
        #[allow(dead_code)]
        pub(crate) fn instrument<F: std::future::Future>(self, f: F) -> F {
            f
        }
    }

    /// 占位类型，仅在 config 默认值构造时使用。
    #[allow(dead_code)]
    pub(crate) type Level = ();

    #[inline]
    pub(crate) fn current_span() -> MaybeSpan {
        MaybeSpan
    }

    #[inline]
    pub(crate) fn run_span(_: Duration, _: Duration, _: &str) -> MaybeSpan {
        MaybeSpan
    }

    #[inline]
    pub(crate) fn batch_span(_: u64, _: usize, _: Duration, _: usize) -> MaybeSpan {
        MaybeSpan
    }

    #[inline]
    pub(crate) fn item_span(_: usize) -> MaybeSpan {
        MaybeSpan
    }

    #[inline]
    pub(crate) fn bypass_span(_: u64, _: usize) -> MaybeSpan {
        MaybeSpan
    }

    #[inline]
    pub(crate) fn drain_span(_: usize) -> MaybeSpan {
        MaybeSpan
    }

    #[inline]
    #[allow(clippy::unused_unit)]
    pub(crate) fn default_tracing_level() -> TraceLevel {}
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
