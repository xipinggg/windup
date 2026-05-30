/// 累加器错误类型。
#[derive(Debug, thiserror::Error)]
pub enum AccumulatorError {
    /// 累加器已关闭，无法再提交新 item。
    #[error("accumulator has shut down")]
    Shutdown,

    /// 队列已满，拒绝提交。
    #[error("queue is full: max={max}, pending={pending}")]
    QueueFull {
        /// 配置的最大队列深度。
        max: usize,
        /// 当前待处理 item 数。
        pending: usize,
    },

    /// 配置参数无效。
    #[error("invalid config: {reason}")]
    InvalidConfig {
        /// 无效原因。
        reason: String,
    },

    /// item 在批处理前已超时。
    #[error("item timed out before processing")]
    Timeout,
}
