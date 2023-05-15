use std::{
    sync::mpsc::{sync_channel, Receiver},
    thread::{Builder, Thread},
};

use notify::RecommendedWatcher;

/// Task orchestrator that generate event on task completion
#[derive(Clone)]
pub struct Orchestrator(Thread);

impl Orchestrator {
    /// Start a new background task
    pub fn task<T: Send + 'static>(
        &self,
        spawn: impl Fn() -> crate::error::Result<T> + Send + 'static,
    ) -> Task<T> {
        let (sender, receiver) = oneshot::channel();
        let wake = self.0.clone();
        std::thread::spawn(move || {
            let result = spawn();
            if sender.send(result).is_ok() {
                // Only succeeded if the result is expected
                wake.unpark();
            }
        });
        Task(receiver)
    }
}

pub struct Task<T>(oneshot::Receiver<crate::error::Result<T>>);

impl<T> Task<T> {
    pub fn tick(&mut self) -> crate::error::Result<Option<T>> {
        match self.0.try_recv() {
            Ok(result) => Some(result).transpose(),
            Err(it) => match it {
                oneshot::TryRecvError::Empty => Ok(None),
                oneshot::TryRecvError::Disconnected => {
                    Err(format!("Task failed without error").into())
                }
            },
        }
    }
}

pub enum Event {
    Term(tui::crossterm::event::Event),
    File(notify::Event),
    Task,
}

/// Create a channel to receive all the event and a task orchestrator
pub fn event_listener() -> (Receiver<Event>, RecommendedWatcher, Orchestrator) {
    let (sender, receiver) = sync_channel(100);
    // Task completion listener
    let waker = {
        let sender = sender.clone();
        Builder::new()
            .name("task_listener".into())
            .spawn(move || loop {
                std::thread::park();
                // No need to insist if there is already other event in the queue
                sender.try_send(Event::Task).ok();
            })
            .expect("Failed to start task_listener thread")
            .thread()
            .clone()
    };
    // File system event
    let watcher = {
        let sender = sender.clone();
        notify::recommended_watcher(move |res: notify::Result<notify::Event>| {
            let event = res.expect("Failed to receive file system event");
            sender
                .send(Event::File(event))
                .expect("Failed to send file system event")
        })
        .expect("Failed to start file system watcher")
    };
    // Term event listener
    Builder::new()
        .name("term_listener".into())
        .spawn(move || loop {
            match tui::crossterm::event::read() {
                Ok(event) => {
                    sender
                        .send(Event::Term(event))
                        .expect("Failed to send terminal event");
                }
                Err(err) => panic!("{err}"),
            }
        })
        .expect("Failed to start term_listener thread");

    (receiver, watcher, Orchestrator(waker))
}
