use lamlock::Lock;
use lamlock_bench::schedule::Schedule;
use lamlock_bench::workloads::btree::BTreeWorkload;
use lamlock_bench::workloads::hashtable::HashTableWorkload;
use lamlock_bench::workloads::pqueue::PQueueWorkload;
use lamlock_bench::workloads::ringbuf::RingBufWorkload;
use lamlock_bench::workloads::slab::SlabWorkload;
use lamlock_bench::workloads::stack::StackWorkload;
use lamlock_bench::workloads::Workload;
use std::sync::Barrier;

const THREADS: usize = 64;
const OPS_PER_THREAD: usize = 5_000;

fn run_workload_test<W: Workload, S: Schedule<W::State>>(workload: &W) {
    let lock = S::new(workload.init_state());
    let barrier = Barrier::new(THREADS + 1);
    std::thread::scope(|scope| {
        for tid in 0..THREADS {
            let barrier = &barrier;
            let lock = &lock;
            scope.spawn(move || {
                barrier.wait();
                workload.run_thread(lock, tid, THREADS, OPS_PER_THREAD);
            });
        }
        barrier.wait();
    });
}

macro_rules! variant_tests {
    ($workload_ctor:expr, $workload_name:ident) => {
        mod $workload_name {
            use super::*;

            #[test]
            fn lamlock_futex_panic() {
                run_workload_test::<_, Lock<_, true, true>>(&$workload_ctor);
            }

            #[test]
            fn lamlock_futex_no_panic() {
                run_workload_test::<_, Lock<_, true, false>>(&$workload_ctor);
            }

            #[test]
            fn lamlock_spin_panic() {
                run_workload_test::<_, Lock<_, false, true>>(&$workload_ctor);
            }

            #[test]
            fn lamlock_spin_no_panic() {
                run_workload_test::<_, Lock<_, false, false>>(&$workload_ctor);
            }

            #[test]
            fn std_mutex() {
                run_workload_test::<_, std::sync::Mutex<_>>(&$workload_ctor);
            }
        }
    };
}

variant_tests!(StackWorkload, stack);
variant_tests!(PQueueWorkload, pqueue);
variant_tests!(RingBufWorkload, ringbuf);
variant_tests!(SlabWorkload, slab);
variant_tests!(HashTableWorkload, hashtable);
variant_tests!(BTreeWorkload, btree);
