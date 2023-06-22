use std::{
    sync::mpsc::{sync_channel, Receiver, TrySendError},
    thread::Builder,
    time::Duration,
};

use notify::RecommendedWatcher;
use notify_debouncer_full::{new_debouncer, FileIdMap};

use crate::task::Runner;

pub enum Event {
    Term(tui::crossterm::event::Event),
    FS(Result<Vec<notify_debouncer_full::DebouncedEvent>, Vec<notify::Error>>),
    Task,
}

/// Create a channel to receive all the event and a task orchestrator
pub fn event_listener() -> (
    Receiver<Event>,
    notify_debouncer_full::Debouncer<RecommendedWatcher, FileIdMap>,
    Runner,
) {
    let (sender, receiver) = sync_channel(100);
    // Task completion listener
    let waker = {
        let sender = sender.clone();
        Builder::new()
            .name("task_listener".into())
            .spawn(move || loop {
                std::thread::park();

                if let Err(e) = sender.try_send(Event::Task) {
                    match e {
                        TrySendError::Full(_) => {
                            // No need to insist if there is already other event in the queue
                        }
                        TrySendError::Disconnected(_) => {
                            return; // Graceful shutdown
                        }
                    }
                }
            })
            .expect("Failed to start task_listener thread")
            .thread()
            .clone()
    };
    // File system event
    let debouncer = {
        let sender = sender.clone();
        let (tx, rx) = std::sync::mpsc::channel();
        let debouncer = new_debouncer(Duration::from_secs(1), None, tx)
            .expect("Failed to setup file system watcher");

        Builder::new()
            .name("fs_listener".into())
            .spawn(move || loop {
                match rx.recv() {
                    Ok(event) => {
                        if sender.send(Event::FS(event)).is_err() {
                            return; // Graceful shutdown
                        }
                    }
                    Err(err) => panic!("{err}"),
                }
            })
            .expect("Failed to start fs_listener thread");
        debouncer
    };
    // Term event listener
    Builder::new()
        .name("term_listener".into())
        .spawn(move || loop {
            match tui::crossterm::event::read() {
                Ok(event) => {
                    if sender.send(Event::Term(event)).is_err() {
                        return; // Graceful shutdown
                    }
                }
                Err(err) => panic!("{err}"),
            }
        })
        .expect("Failed to start term_listener thread");

    (receiver, debouncer, Runner::from_waker(waker))
}
