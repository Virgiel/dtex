use std::{
    sync::{Arc, Mutex},
    thread::Thread,
};

#[derive(Clone)]
pub struct Runner(Thread);

impl Runner {
    pub fn from_waker(waker: Thread) -> Self {
        Self(waker)
    }

    /// Start a new one shot background task
    pub fn once<T: Send + 'static>(
        &self,
        task: impl FnOnce(OnceCtx) -> crate::error::Result<T> + Send + 'static,
    ) -> OnceTask<T> {
        let _witness = Arc::new(());
        let (sender, receiver) = oneshot::channel();
        let wake = self.0.clone();
        {
            let _witness = _witness.clone();
            std::thread::spawn(move || {
                let result = task(OnceCtx(_witness));
                if sender.send(result).is_ok() {
                    // Only succeeded if the result is expected
                    wake.unpark();
                }
            });
        }
        OnceTask { receiver, _witness }
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

pub struct OnceCtx(Arc<()>);

impl OnceCtx {
    pub fn canceled(&self) -> bool {
        Arc::strong_count(&self.0) == 1
    }
}

pub struct OnceTask<T> {
    receiver: oneshot::Receiver<crate::error::Result<T>>,
    _witness: Arc<()>,
}

impl<T> OnceTask<T> {
    pub fn tick(&mut self) -> crate::error::Result<Option<T>> {
        match self.receiver.try_recv() {
            Ok(result) => Some(result).transpose(),
            Err(it) => match it {
                oneshot::TryRecvError::Empty => Ok(None),
                oneshot::TryRecvError::Disconnected => {
                    Err("Task failed without error".to_string().into())
                }
            },
        }
    }
}
