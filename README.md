# draft — 自适应时间窗口批处理累加器

[![Rust](https://img.shields.io/badge/rust-2024%20edition-orange)](https://www.rust-lang.org/)
[![License](https://img.shields.io/badge/license-MIT-blue)](LICENSE)
[![docs.rs](https://img.shields.io/docsrs/draft)](https://docs.rs/draft)

通用的异步批处理框架。在可配置的时间窗口内积攒 item，到期后整批交付用户定义的处理器。时间窗口基于批利用率或执行延迟**自适应调整**。

## 快速开始

```rust
use std::time::Duration;
use draft::prelude::*;

struct MyProcessor;
impl BatchProcessor<i32> for MyProcessor {
    async fn process(&self, batch: Batch<i32>) -> Vec<()> {
        println!("处理批次 #{}: {} 条", batch.batch_id(), batch.len());
        vec![(); batch.len()]
    }
}

#[tokio::main]
async fn main() {
    let config = AccumulatorConfig::new(
        Duration::from_millis(200),  // 初始窗口
        Duration::from_millis(50),   // 最小窗口
        Duration::from_secs(5),      // 最大窗口
    )?
    .with_max_batch_size(100);

    // 一行构建+启动
    let (handle, _jh) = config.build_and_spawn(
        MyProcessor,
        DefaultMetrics::new(),
        AdaptiveController::new(0.8, 0.1)?,
    );

    let reply = handle.submit(42)?;
    let result = reply.await?; // "处理批次 #0: 1 条"
}
```

## 核心概念

```
用户代码                    框架内部
  │                          │
  ├─ handle.send(item) ────→ ├─ 入队 (unbounded channel)
  ├─ handle.submit(item) ──→ │
  │                          ├─ 积攒至 buffer
  │                          ├─ 时间窗口到期 → flush
  │                          ├─ 打包为 Batch<T>
  │                          ├─ 调用 processor.process(batch)
  │                          ├─ 逐 item 发回 ReplyHandle
  │                          │
  ◄── ReplyHandle.await ──── ┘
```

## API 速览

### 提交方式

| 方法 | 签名 | 说明 |
|------|------|------|
| `send` | `(T) -> Result<(), Error>` | fire-and-forget，不关心结果 |
| `submit` | `(T) -> Result<ReplyHandle<R>, Error>` | 返回 Future，可 `.await` 拿结果 |
| `submit_with` | `(T, SubmitOptions) -> Result<ReplyHandle<R>, Error>` | 带优先级 + TTL 超时 |
| `submit_or_wait` | `(T, Duration) -> Result<ReplyHandle<R>, Error>` | 队列满时阻塞等待（async） |
| `send_or_wait` | `(T, Duration) -> Result<(), Error>` | fire-and-forget + 阻塞等待 |
| `bypass` | `(T) -> Result<(), Error>` | 跳过批处理，直接交付 |

```rust
// 基础用法
handle.send(item)?;                              // 发了就忘
let reply = handle.submit(item)?;                // 获取结果
let result: R = reply.await?;

// 带选项
use draft::prelude::*;
let reply = handle.submit_with(item, SubmitOptions {
    priority: Priority::High,
    ttl: Some(Duration::from_secs(30)),
})?;

// 阻塞等待（队列满时不下车）
let reply = handle.submit_or_wait(item, Duration::from_secs(5)).await?;
```

### 可失败处理器

```rust
struct DbBatchProcessor;
impl TryBatchProcessor<MyItem, RowId> for DbBatchProcessor {
    type Error = sqlx::Error;

    async fn try_process(&self, batch: Batch<MyItem>) -> Vec<Result<RowId, sqlx::Error>> {
        // 对 batch 中每个 item 返回独立的成功/失败
        todo!()
    }
}
```

> 普通 `BatchProcessor` 自动实现 `TryBatchProcessor`（错误类型为 `Infallible`）。

### 配置

```rust
let config = AccumulatorConfig::new(
    Duration::from_millis(200),  // 初始窗口
    Duration::from_millis(50),   // 最小窗口
    Duration::from_secs(5),      // 最大窗口
)?
.with_max_batch_size(100)        // 达到立即 flush
.with_max_queue_depth(10000)     // 背压上限
.with_max_batch_weight(1024)     // 按重量提前 flush
.with_concurrency_mode(ConcurrencyMode::Concurrent { max_inflight: 4 })
.with_flush_empty_batches(false) // 默认：空批次不交付
.with_drain_timeout(Some(Duration::from_secs(30)))
.with_drain_batch_limit(128)     // 主循环每轮 drain 上限
.with_trace_per_item(false);     // per-item TRACE 事件（高频默认关）
```

### 可观测性

```rust
// 运行统计
let stats: StatsSnapshot = handle.stats();
println!("提交: {}, flush: {}, p50: {:?}, p99: {:?}",
    stats.total_submitted, stats.total_flushed,
    stats.p50_latency, stats.p99_latency);

// 健康检查
let health: AccumulatorHealth = handle.health();
println!("接受中: {}, 队列利用率: {:.1}%, 拒绝: {}",
    health.is_accepting,
    health.queue_utilization * 100.0,
    health.total_rejected);
```

### 窗口控制器

| 控制器 | 策略 | 适用场景 |
|--------|------|----------|
| `FixedController` | 永远返回固定窗口 | 不需要自适应 |
| `AdaptiveController` | 利用率低→增大，高→缩小 | 吞吐优先 |
| `LatencyAdaptiveController` | 执行慢→缩小，快→增大 | 延迟优先 |
| `PIDController` | PID 算法消除稳态误差和振荡 | 精确控制 |
| `BackoffController` | 满批指数退避，空闲缓慢回缩 | 突发流量 |
| 自定义 | 实现 `WindowController` | 任意策略 |

### 运行控制

```rust
handle.pause();                   // 暂停 flush（继续缓冲）
handle.resume();                  // 恢复 flush
assert!(handle.is_paused());      // 检查暂停状态
handle.cancel();                  // 触发优雅关闭
```

### 可观测性

```rust
let stats = handle.stats();
// StatsSnapshot 新增:
stats.p50_queue_wait              // 队列等待时间 p50
stats.p99_queue_wait              // 队列等待时间 p99
stats.avg_queue_wait              // 队列平均等待时间

// 健康检查
let health = handle.health();
health.is_accepting               // 是否仍在接收
health.queue_utilization          // 队列利用率 0.0~1.0
health.total_rejected             // 累计拒绝次数
```

## 安装

```toml
[dependencies]
draft = "0.1"
tokio = { version = "1", features = ["full"] }
```

`tracing` feature 默认开启。如需关闭：

```toml
draft = { version = "0.1", default-features = false }
```

## 项目结构

```
src/
├── accumulator.rs   # AccumulatorHandle + BatchAccumulator (1043 行)
├── batch.rs         # Batch / BatchProcessor / TryBatchProcessor / FlushInfo
├── config.rs        # AccumulatorConfig builder + ConcurrencyMode
├── controller.rs    # WindowController trait + 3 种内置控制器
├── error.rs         # AccumulatorError 错误类型
├── metrics.rs       # MetricsCollector trait + DefaultMetrics
├── stats.rs         # StatsSnapshot + AccumulatorHealth
├── trace.rs         # tracing 可观测性（span/event，feature 可关闭）
└── lib.rs           # prelude 统一导出
```

## 许可证

MIT
