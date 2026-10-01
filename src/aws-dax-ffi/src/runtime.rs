use std::{future::Future, sync::OnceLock};

use tokio::{
    runtime::{Builder, Runtime},
    task::JoinHandle,
};

use crate::DaxFfiError;

static RUNTIME: OnceLock<Result<Runtime, String>> = OnceLock::new();

#[allow(
    clippy::result_large_err,
    reason = "the runtime bridge preserves the rich UniFFI error contract"
)]
pub(crate) async fn run<F, T>(future: F) -> Result<T, DaxFfiError>
where
    F: Future<Output = Result<T, DaxFfiError>> + Send + 'static,
    T: Send + 'static,
{
    let runtime = runtime()?;
    let mut task = AbortOnDrop(runtime.spawn(future));
    match (&mut task.0).await {
        Ok(result) => result,
        Err(error) if error.is_cancelled() => {
            Err(DaxFfiError::internal("Rust runtime task was cancelled"))
        }
        Err(error) => Err(DaxFfiError::internal(format!(
            "Rust runtime task failed: {error}"
        ))),
    }
}

#[allow(
    clippy::result_large_err,
    reason = "the runtime bridge preserves the rich UniFFI error contract"
)]
fn runtime() -> Result<&'static Runtime, DaxFfiError> {
    match RUNTIME.get_or_init(|| {
        Builder::new_multi_thread()
            .enable_all()
            .thread_name("aws-dax-ffi")
            .build()
            .map_err(|error| error.to_string())
    }) {
        Ok(runtime) => Ok(runtime),
        Err(error) => Err(DaxFfiError::internal(format!(
            "failed to initialize Rust async runtime: {error}"
        ))),
    }
}

struct AbortOnDrop<T>(JoinHandle<T>);

impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };

    use super::*;

    #[test]
    fn runs_tokio_work_from_a_non_tokio_executor() {
        let output = futures::executor::block_on(run(async {
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            Ok::<_, DaxFfiError>(42)
        }))
        .unwrap();

        assert_eq!(output, 42);
    }

    #[test]
    fn aborts_runtime_task_when_foreign_future_is_cancelled() {
        let dropped = Arc::new(AtomicBool::new(false));
        let dropped_in_future = Arc::clone(&dropped);
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let mut future = Box::pin(run(async move {
            let _guard = DropFlag(dropped_in_future);
            started_tx.send(()).unwrap();
            std::future::pending::<()>().await;
            Ok::<_, DaxFfiError>(())
        }));

        futures::executor::block_on(std::future::poll_fn(|context| {
            assert!(future.as_mut().poll(context).is_pending());
            std::task::Poll::Ready(())
        }));
        started_rx
            .recv_timeout(std::time::Duration::from_secs(1))
            .unwrap();
        drop(future);

        for _ in 0..100 {
            if dropped.load(Ordering::Acquire) {
                return;
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        panic!("cancelled foreign future did not abort its Rust runtime task");
    }

    struct DropFlag(Arc<AtomicBool>);

    impl Drop for DropFlag {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Release);
        }
    }
}
