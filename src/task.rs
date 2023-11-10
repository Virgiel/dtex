use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    thread::Thread,
};

use crate::{
    duckdb::{ConnCtx, Connection},
    Source,
};

#[derive(Clone)]
pub struct Runner(Thread);

impl Runner {
    pub fn from_waker(waker: Thread) -> Self {
        Self(waker)
    }

    /// Start a new duckdb background task
    pub fn duckdb<T: Send + 'static>(
        &self,
        source: Arc<Source>,
        task: impl FnOnce(Arc<Source>, Connection) -> crate::error::Result<T> + Send + 'static,
    ) -> DuckTask<T> {
        let (sender, receiver) = oneshot::channel();
        let wake = self.0.clone();
        let done = Arc::new(AtomicBool::new(false));

        let con = source.conn().expect("TODO");
        let ctx = con.ctx();
        {
            let done = done.clone();
            std::thread::spawn(move || {
                let result = task(source, con);
                done.store(true, Ordering::Relaxed);
                if sender.send(result).is_ok() {
                    // Only succeeded if the result is expected
                    wake.unpark();
                }
            });
        }
        DuckTask {
            receiver,
            ctx,
            done,
        }
    }

    /// Start a new background task
    pub fn task<S: Send + Sync + 'static, T: Send + 'static>(
        &self,
        state: S,
        result: T,
        task: impl FnOnce(Ctx<S, T>) + Send + 'static,
    ) -> Task<S, T> {
        let inner = Arc::new(Inner {
            state,
            lock: Mutex::new(result),
        });
        let worker = {
            let ctx = Ctx {
                inner: inner.clone(),
                wake: self.0.clone(),
            };
            std::thread::spawn(move || task(ctx)).thread().clone()
        };
        Task {
            inner: Some(inner),
            worker,
        }
    }
}

struct Inner<S, T> {
    state: S,
    lock: Mutex<T>,
}

pub struct Ctx<S, T> {
    inner: Arc<Inner<S, T>>,
    wake: Thread,
}

impl<S, T> Ctx<S, T> {
    pub fn canceled(&self) -> bool {
        Arc::strong_count(&self.inner) == 1
    }

    pub fn state(&self) -> &S {
        &self.inner.state
    }

    pub fn lock(&self, update: impl FnOnce(&mut T)) {
        let mut lock = self.inner.lock.lock().unwrap();
        update(&mut lock);
        drop(lock);
    }

    pub fn wait(&self) {
        std::thread::park();
        self.wake.unpark();
    }
}

impl<S, T> Drop for Ctx<S, T> {
    fn drop(&mut self) {
        self.wake.unpark();
    }
}

pub struct Task<S, T> {
    inner: Option<Arc<Inner<S, T>>>,
    worker: Thread,
}

impl<S, T> Task<S, T> {
    pub fn state(&self) -> &S {
        &self.inner.as_ref().unwrap().state
    }

    pub fn wake(&self) {
        self.worker.unpark()
    }

    pub fn lock<R>(&self, update: impl FnOnce(&mut T) -> R) -> R {
        let mut lock = self.inner.as_ref().unwrap().lock.lock().unwrap();
        let result = update(&mut lock);
        drop(lock);
        result
    }
}

impl<S, T> Drop for Task<S, T> {
    fn drop(&mut self) {
        drop(self.inner.take()); // Reduce arc count
        self.worker.unpark() // Wake worker for cancelation
    }
}

pub struct DuckTask<T> {
    receiver: oneshot::Receiver<crate::error::Result<T>>,
    ctx: ConnCtx,
    done: Arc<AtomicBool>,
}

impl<T> DuckTask<T> {
    pub fn progress(&self) -> f64 {
        self.ctx.progress()
    }

    pub fn tick(&mut self) -> Option<crate::error::Result<T>> {
        match self.receiver.try_recv() {
            Ok(result) => Some(result),
            Err(it) => match it {
                oneshot::TryRecvError::Empty => None,
                oneshot::TryRecvError::Disconnected => {
                    Some(Err("Task failed without error".to_string().into()))
                }
            },
        }
    }
}

impl<T> Drop for DuckTask<T> {
    fn drop(&mut self) {
        if !self.done.load(Ordering::Relaxed) {
            self.ctx.interrupt()
        }
    }
}
