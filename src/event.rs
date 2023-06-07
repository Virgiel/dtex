use std::{
    sync::mpsc::{sync_channel, Receiver},
    thread::Builder,
    time::Duration,
};

use notify::RecommendedWatcher;
use notify_debouncer_full::{new_debouncer, FileIdMap};

use crate::task::Runner;

pub enum Event {
    Term(tui::crossterm::event::Event),
    FS(Result<Vec<notify::Event>, Vec<notify::Error>>),
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
                // No need to insist if there is already other event in the queue
                sender.try_send(Event::Task).ok();
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
                        sender
                            .send(Event::FS(event))
                            .expect("Failed to file terminal event");
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
                    sender
                        .send(Event::Term(event))
                        .expect("Failed to send terminal event");
                }
                Err(err) => panic!("{err}"),
            }
        })
        .expect("Failed to start term_listener thread");

    (receiver, debouncer, Runner::from_waker(waker))
}
