pub mod pqueue;
pub mod ringbuf;
pub mod slab;
pub mod stack;

use crate::schedule::Schedule;

pub trait Workload: Sync + 'static {
    type State: Send;
    fn name(&self) -> &'static str;
    fn description(&self) -> &'static str;
    fn init_state(&self) -> Self::State;
    fn run_thread<S: Schedule<Self::State>>(
        &self,
        lock: &S,
        thread_id: usize,
        thread_count: usize,
        ops: usize,
    );
}
