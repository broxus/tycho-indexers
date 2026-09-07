use std::fmt::Display;
use std::future::Future;
use std::time::Duration;

pub(super) async fn retry<T, E, F, Fut>(operation: &str, delay: Duration, mut action: F) -> T
where
    E: Display,
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
{
    let mut attempt = 0u64;
    loop {
        attempt = attempt.saturating_add(1);
        match action().await {
            Ok(value) => return value,
            Err(error) => {
                tracing::warn!(operation, attempt, error = %format_args!("{error:#}"), "operation failed, retrying");
                tokio::time::sleep(delay).await;
            }
        }
    }
}
