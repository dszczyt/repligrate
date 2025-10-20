use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tracing::info;

/// Manages the pause/continue state of the replication listener
#[derive(Clone)]
pub struct ListenerState {
    paused: Arc<AtomicBool>,
}

#[allow(dead_code)]
impl ListenerState {
    /// Create a new listener state (initially unpaused)
    pub fn new() -> Self {
        Self {
            paused: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Pause the listener
    pub fn pause(&self) {
        self.paused.store(true, Ordering::SeqCst);
        info!("Replication listener paused");
    }

    /// Continue the listener
    pub fn continue_listening(&self) {
        self.paused.store(false, Ordering::SeqCst);
        info!("Replication listener continued");
    }

    /// Check if the listener is paused
    pub fn is_paused(&self) -> bool {
        self.paused.load(Ordering::SeqCst)
    }

    /// Toggle pause state
    pub fn toggle(&self) {
        let current = self.is_paused();
        if current {
            self.continue_listening();
        } else {
            self.pause();
        }
    }

    /// Get the current state as a string
    pub fn status(&self) -> &'static str {
        if self.is_paused() {
            "PAUSED"
        } else {
            "LISTENING"
        }
    }
}

impl Default for ListenerState {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_initial_state_unpaused() {
        let state = ListenerState::new();
        assert!(!state.is_paused());
        assert_eq!(state.status(), "LISTENING");
    }

    #[test]
    fn test_pause() {
        let state = ListenerState::new();
        state.pause();
        assert!(state.is_paused());
        assert_eq!(state.status(), "PAUSED");
    }

    #[test]
    fn test_continue_listening() {
        let state = ListenerState::new();
        state.pause();
        assert!(state.is_paused());
        state.continue_listening();
        assert!(!state.is_paused());
        assert_eq!(state.status(), "LISTENING");
    }

    #[test]
    fn test_toggle() {
        let state = ListenerState::new();
        assert!(!state.is_paused());

        state.toggle();
        assert!(state.is_paused());

        state.toggle();
        assert!(!state.is_paused());
    }

    #[test]
    fn test_clone_shares_state() {
        let state1 = ListenerState::new();
        let state2 = state1.clone();

        state1.pause();
        assert!(state2.is_paused());

        state2.continue_listening();
        assert!(!state1.is_paused());
    }
}
