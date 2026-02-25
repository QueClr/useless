pub mod combining;
pub mod database;
pub mod kdtree;
pub mod lru;
pub mod nbody;
pub mod slab;
pub mod wal;

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
