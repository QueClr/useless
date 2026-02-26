use rand::Rng;
use rand::SeedableRng;
use rand_xoshiro::Xoshiro256PlusPlus;

use crate::schedule::Schedule;
use crate::workloads::Workload;

// Arena-based B-Tree: no heap allocation during insert/delete/search.
// All nodes live in a pre-allocated Vec<ArenaNode>, and "pointers" are usize indices.

const MIN_DEGREE: usize = 4; // Order-8 B-Tree: max 7 keys, min 3 keys per node
const MAX_KEYS: usize = 2 * MIN_DEGREE - 1; // 7
const MAX_CHILDREN: usize = 2 * MIN_DEGREE; // 8
const ARENA_CAPACITY: usize = 16_384;
const INITIAL_KEYS: usize = 8_000;
const KEY_SPACE: u64 = 20_000;
const ZIPF_EXPONENT: f64 = 1.2;

/// Batch size: how many ops each thread submits per lock.schedule() call.
/// This is the critical tuning knob — batching is what makes flat-combining win.
const BATCH_SIZE: usize = 1000;

const NONE: usize = usize::MAX;

/// Zipfian distribution: generates keys biased toward hot keys.
fn zipfian_key(rng: &mut Xoshiro256PlusPlus, n: u64) -> u64 {
    let u: f64 = rng.random::<f64>();
    let rank =
        (u * (n as f64).powf(1.0 - ZIPF_EXPONENT) + (1.0 - u)).powf(1.0 / (1.0 - ZIPF_EXPONENT));
    (rank as u64).min(n - 1)
}

#[derive(Clone)]
struct ArenaNode {
    keys: [u64; MAX_KEYS],
    values: [u64; MAX_KEYS],
    children: [usize; MAX_CHILDREN],
    num_keys: u8,
    is_leaf: bool,
}

impl ArenaNode {
    fn new(is_leaf: bool) -> Self {
        Self {
            keys: [0; MAX_KEYS],
            values: [0; MAX_KEYS],
            children: [NONE; MAX_CHILDREN],
            num_keys: 0,
            is_leaf,
        }
    }

    fn n(&self) -> usize {
        self.num_keys as usize
    }
}

pub struct BTree {
    arena: Vec<ArenaNode>,
    root: usize,
    free_head: usize, // free-list through arena slots
    size: usize,
}

impl BTree {
    fn new() -> Self {
        let mut arena = Vec::with_capacity(ARENA_CAPACITY);
        // Slot 0 reserved as root (leaf)
        arena.push(ArenaNode::new(true));
        // Build a free-list from slots 1..ARENA_CAPACITY
        for i in 1..ARENA_CAPACITY {
            let mut node = ArenaNode::new(true);
            // Abuse children[0] as "next free" pointer
            node.children[0] = if i + 1 < ARENA_CAPACITY { i + 1 } else { NONE };
            arena.push(node);
        }
        Self {
            arena,
            root: 0,
            free_head: 1,
            size: 0,
        }
    }

    fn alloc_node(&mut self, is_leaf: bool) -> usize {
        debug_assert!(self.free_head != NONE, "B-Tree arena exhausted");
        let idx = self.free_head;
        self.free_head = self.arena[idx].children[0];
        self.arena[idx] = ArenaNode::new(is_leaf);
        idx
    }

    fn free_node(&mut self, idx: usize) {
        self.arena[idx].children[0] = self.free_head;
        self.free_head = idx;
    }

    fn search(&self, key: u64) -> Option<u64> {
        self.search_at(self.root, key)
    }

    fn search_at(&self, idx: usize, key: u64) -> Option<u64> {
        if idx == NONE {
            return None;
        }
        let node = &self.arena[idx];
        let n = node.n();
        let mut i = 0;
        while i < n && key > node.keys[i] {
            i += 1;
        }
        if i < n && node.keys[i] == key {
            Some(node.values[i])
        } else if node.is_leaf {
            None
        } else {
            self.search_at(node.children[i], key)
        }
    }

    fn insert(&mut self, key: u64, value: u64) {
        let root = self.root;
        if self.arena[root].n() == MAX_KEYS {
            let new_root = self.alloc_node(false);
            self.arena[new_root].children[0] = root;
            self.split_child(new_root, 0);
            self.root = new_root;
            self.insert_non_full(new_root, key, value);
        } else {
            self.insert_non_full(root, key, value);
        }
        self.size += 1;
    }

    fn insert_non_full(&mut self, idx: usize, key: u64, value: u64) {
        let n = self.arena[idx].n();
        if self.arena[idx].is_leaf {
            let mut i = n as isize - 1;
            while i >= 0 && self.arena[idx].keys[i as usize] > key {
                self.arena[idx].keys[(i + 1) as usize] = self.arena[idx].keys[i as usize];
                self.arena[idx].values[(i + 1) as usize] = self.arena[idx].values[i as usize];
                i -= 1;
            }
            if i >= 0 && self.arena[idx].keys[i as usize] == key {
                // Update existing
                self.arena[idx].values[i as usize] = value;
            } else {
                self.arena[idx].keys[(i + 1) as usize] = key;
                self.arena[idx].values[(i + 1) as usize] = value;
                self.arena[idx].num_keys += 1;
            }
        } else {
            let mut i = n as isize - 1;
            while i >= 0 && self.arena[idx].keys[i as usize] > key {
                i -= 1;
            }
            if i >= 0 && self.arena[idx].keys[i as usize] == key {
                self.arena[idx].values[i as usize] = value;
                return;
            }
            i += 1;
            let child = self.arena[idx].children[i as usize];
            if self.arena[child].n() == MAX_KEYS {
                self.split_child(idx, i as usize);
                if self.arena[idx].keys[i as usize] < key {
                    i += 1;
                } else if self.arena[idx].keys[i as usize] == key {
                    self.arena[idx].values[i as usize] = value;
                    return;
                }
            }
            let child = self.arena[idx].children[i as usize];
            self.insert_non_full(child, key, value);
        }
    }

    fn split_child(&mut self, parent: usize, i: usize) {
        let y = self.arena[parent].children[i];
        let y_leaf = self.arena[y].is_leaf;
        let z = self.alloc_node(y_leaf);

        // Copy upper half of y into z
        for j in 0..MIN_DEGREE - 1 {
            self.arena[z].keys[j] = self.arena[y].keys[j + MIN_DEGREE];
            self.arena[z].values[j] = self.arena[y].values[j + MIN_DEGREE];
        }
        if !y_leaf {
            for j in 0..MIN_DEGREE {
                self.arena[z].children[j] = self.arena[y].children[j + MIN_DEGREE];
            }
        }
        self.arena[z].num_keys = (MIN_DEGREE - 1) as u8;

        let mid_key = self.arena[y].keys[MIN_DEGREE - 1];
        let mid_val = self.arena[y].values[MIN_DEGREE - 1];
        self.arena[y].num_keys = (MIN_DEGREE - 1) as u8;

        // Shift parent's children and keys to make room
        let pn = self.arena[parent].n();
        for j in (i + 1..=pn).rev() {
            self.arena[parent].children[j + 1] = self.arena[parent].children[j];
        }
        self.arena[parent].children[i + 1] = z;
        for j in (i..pn).rev() {
            self.arena[parent].keys[j + 1] = self.arena[parent].keys[j];
            self.arena[parent].values[j + 1] = self.arena[parent].values[j];
        }
        self.arena[parent].keys[i] = mid_key;
        self.arena[parent].values[i] = mid_val;
        self.arena[parent].num_keys += 1;
    }

    fn remove(&mut self, key: u64) -> bool {
        if self.root == NONE {
            return false;
        }
        let removed = self.remove_at(self.root, key);
        // Shrink root if empty
        let root = self.root;
        if self.arena[root].n() == 0 && !self.arena[root].is_leaf {
            let new_root = self.arena[root].children[0];
            self.free_node(root);
            self.root = new_root;
        }
        if removed {
            self.size -= 1;
        }
        removed
    }

    fn remove_at(&mut self, idx: usize, key: u64) -> bool {
        let n = self.arena[idx].n();
        let mut i = 0;
        while i < n && self.arena[idx].keys[i] < key {
            i += 1;
        }

        if i < n && self.arena[idx].keys[i] == key {
            if self.arena[idx].is_leaf {
                // Shift left
                for j in i..n - 1 {
                    self.arena[idx].keys[j] = self.arena[idx].keys[j + 1];
                    self.arena[idx].values[j] = self.arena[idx].values[j + 1];
                }
                self.arena[idx].num_keys -= 1;
                true
            } else {
                self.remove_internal(idx, i)
            }
        } else if self.arena[idx].is_leaf {
            false
        } else {
            let child = self.arena[idx].children[i];
            if self.arena[child].n() < MIN_DEGREE {
                self.fill(idx, i);
                // Recalculate i after fill (merge may have shifted things)
                let n = self.arena[idx].n();
                if i > n {
                    let child = self.arena[idx].children[i - 1];
                    return self.remove_at(child, key);
                }
            }
            let child = self.arena[idx].children[i];
            self.remove_at(child, key)
        }
    }

    fn remove_internal(&mut self, idx: usize, i: usize) -> bool {
        let key = self.arena[idx].keys[i];
        let left = self.arena[idx].children[i];
        let right = self.arena[idx].children[i + 1];

        if self.arena[left].n() >= MIN_DEGREE {
            let (pk, pv) = self.get_predecessor(left);
            self.arena[idx].keys[i] = pk;
            self.arena[idx].values[i] = pv;
            self.remove_at(left, pk)
        } else if self.arena[right].n() >= MIN_DEGREE {
            let (sk, sv) = self.get_successor(right);
            self.arena[idx].keys[i] = sk;
            self.arena[idx].values[i] = sv;
            self.remove_at(right, sk)
        } else {
            self.merge(idx, i);
            let child = self.arena[idx].children[i];
            self.remove_at(child, key)
        }
    }

    fn get_predecessor(&self, mut idx: usize) -> (u64, u64) {
        while !self.arena[idx].is_leaf {
            let n = self.arena[idx].n();
            idx = self.arena[idx].children[n];
        }
        let n = self.arena[idx].n();
        (self.arena[idx].keys[n - 1], self.arena[idx].values[n - 1])
    }

    fn get_successor(&self, mut idx: usize) -> (u64, u64) {
        while !self.arena[idx].is_leaf {
            idx = self.arena[idx].children[0];
        }
        (self.arena[idx].keys[0], self.arena[idx].values[0])
    }

    fn fill(&mut self, parent: usize, i: usize) {
        let pn = self.arena[parent].n();
        if i != 0 {
            let left_sib = self.arena[parent].children[i - 1];
            if self.arena[left_sib].n() >= MIN_DEGREE {
                self.borrow_from_prev(parent, i);
                return;
            }
        }
        if i != pn {
            let right_sib = self.arena[parent].children[i + 1];
            if self.arena[right_sib].n() >= MIN_DEGREE {
                self.borrow_from_next(parent, i);
                return;
            }
        }
        if i != pn {
            self.merge(parent, i);
        } else {
            self.merge(parent, i - 1);
        }
    }

    fn borrow_from_prev(&mut self, parent: usize, i: usize) {
        let child = self.arena[parent].children[i];
        let sibling = self.arena[parent].children[i - 1];
        let cn = self.arena[child].n();
        let sn = self.arena[sibling].n();

        // Shift child keys/children right
        for j in (0..cn).rev() {
            self.arena[child].keys[j + 1] = self.arena[child].keys[j];
            self.arena[child].values[j + 1] = self.arena[child].values[j];
        }
        if !self.arena[child].is_leaf {
            for j in (0..=cn).rev() {
                self.arena[child].children[j + 1] = self.arena[child].children[j];
            }
        }

        // Move parent key down to child
        self.arena[child].keys[0] = self.arena[parent].keys[i - 1];
        self.arena[child].values[0] = self.arena[parent].values[i - 1];

        // Move sibling's last child to child
        if !self.arena[child].is_leaf {
            self.arena[child].children[0] = self.arena[sibling].children[sn];
        }

        // Move sibling's last key up to parent
        self.arena[parent].keys[i - 1] = self.arena[sibling].keys[sn - 1];
        self.arena[parent].values[i - 1] = self.arena[sibling].values[sn - 1];

        self.arena[child].num_keys += 1;
        self.arena[sibling].num_keys -= 1;
    }

    fn borrow_from_next(&mut self, parent: usize, i: usize) {
        let child = self.arena[parent].children[i];
        let sibling = self.arena[parent].children[i + 1];
        let cn = self.arena[child].n();
        let sn = self.arena[sibling].n();

        // Move parent key down to child's end
        self.arena[child].keys[cn] = self.arena[parent].keys[i];
        self.arena[child].values[cn] = self.arena[parent].values[i];

        // Move sibling's first child to child
        if !self.arena[child].is_leaf {
            self.arena[child].children[cn + 1] = self.arena[sibling].children[0];
        }

        // Move sibling's first key up to parent
        self.arena[parent].keys[i] = self.arena[sibling].keys[0];
        self.arena[parent].values[i] = self.arena[sibling].values[0];

        // Shift sibling left
        for j in 0..sn - 1 {
            self.arena[sibling].keys[j] = self.arena[sibling].keys[j + 1];
            self.arena[sibling].values[j] = self.arena[sibling].values[j + 1];
        }
        if !self.arena[sibling].is_leaf {
            for j in 0..sn {
                self.arena[sibling].children[j] = self.arena[sibling].children[j + 1];
            }
        }

        self.arena[child].num_keys += 1;
        self.arena[sibling].num_keys -= 1;
    }

    fn merge(&mut self, parent: usize, i: usize) {
        let child = self.arena[parent].children[i];
        let sibling = self.arena[parent].children[i + 1];
        let cn = self.arena[child].n();
        let sn = self.arena[sibling].n();

        // Pull parent key down into child
        self.arena[child].keys[cn] = self.arena[parent].keys[i];
        self.arena[child].values[cn] = self.arena[parent].values[i];

        // Copy sibling's keys/values into child
        for j in 0..sn {
            self.arena[child].keys[cn + 1 + j] = self.arena[sibling].keys[j];
            self.arena[child].values[cn + 1 + j] = self.arena[sibling].values[j];
        }
        if !self.arena[child].is_leaf {
            for j in 0..=sn {
                self.arena[child].children[cn + 1 + j] = self.arena[sibling].children[j];
            }
        }
        self.arena[child].num_keys = (cn + 1 + sn) as u8;

        // Shift parent's keys/children left to fill gap
        let pn = self.arena[parent].n();
        for j in i..pn - 1 {
            self.arena[parent].keys[j] = self.arena[parent].keys[j + 1];
            self.arena[parent].values[j] = self.arena[parent].values[j + 1];
        }
        for j in i + 1..pn {
            self.arena[parent].children[j] = self.arena[parent].children[j + 1];
        }
        self.arena[parent].num_keys -= 1;

        self.free_node(sibling);
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.size
    }
}

#[derive(Clone, Copy)]
enum BTreeOp {
    Insert(u64, u64),
    Search(u64),
    Delete(u64),
}

pub struct BTreeWorkload;

impl Workload for BTreeWorkload {
    type State = BTree;

    fn name(&self) -> &'static str {
        "btree"
    }

    fn description(&self) -> &'static str {
        "B-Tree arena-based — batched ops: 45% insert, 25% delete, 30% search per batch"
    }

    fn init_state(&self) -> Self::State {
        let mut btree = BTree::new();
        let mut rng = Xoshiro256PlusPlus::seed_from_u64(0xBADD_CAFE);
        for _ in 0..INITIAL_KEYS {
            let k = zipfian_key(&mut rng, KEY_SPACE);
            let v = rng.random::<u64>();
            btree.insert(k, v);
        }
        btree
    }

    fn run_thread<S: Schedule<Self::State>>(
        &self,
        lock: &S,
        thread_id: usize,
        _thread_count: usize,
        ops: usize,
    ) {
        let mut rng = Xoshiro256PlusPlus::seed_from_u64(thread_id as u64 * 31415 + 9265);
        let n = KEY_SPACE;

        // Pre-generate all operations outside the lock
        let operations: Vec<BTreeOp> = (0..ops)
            .map(|_| {
                let r: f64 = rng.random();
                if r < 0.45 {
                    BTreeOp::Insert(zipfian_key(&mut rng, n), rng.random::<u64>())
                } else if r < 0.70 {
                    BTreeOp::Delete(zipfian_key(&mut rng, n))
                } else {
                    BTreeOp::Search(zipfian_key(&mut rng, n))
                }
            })
            .collect();

        // Submit work in BATCHES — this is the key to making flat-combining win.
        // The combiner processes a large batch of tree operations with
        // perfect cache locality, instead of acquiring/releasing per op.
        for batch in operations.chunks(BATCH_SIZE) {
            lock.schedule(|tree| {
                for op in batch {
                    match *op {
                        BTreeOp::Insert(k, v) => {
                            tree.insert(k, v);
                        }
                        BTreeOp::Delete(k) => {
                            tree.remove(k);
                        }
                        BTreeOp::Search(k) => {
                            let _ = tree.search(k);
                        }
                    }
                }
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_btree_insert_and_search() {
        let mut tree = BTree::new();
        tree.insert(10, 100);
        tree.insert(20, 200);
        tree.insert(5, 50);

        assert_eq!(tree.search(10), Some(100));
        assert_eq!(tree.search(20), Some(200));
        assert_eq!(tree.search(5), Some(50));
        assert_eq!(tree.search(15), None);
    }

    #[test]
    fn test_btree_delete() {
        let mut tree = BTree::new();
        tree.insert(10, 100);
        tree.insert(20, 200);
        tree.insert(5, 50);

        assert!(tree.remove(10));
        assert_eq!(tree.search(10), None);
        assert!(!tree.remove(10));

        assert!(tree.remove(5));
        assert_eq!(tree.search(5), None);

        assert!(tree.remove(20));
        assert_eq!(tree.search(20), None);
        assert_eq!(tree.len(), 0);
    }

    #[test]
    fn test_btree_split() {
        let mut tree = BTree::new();
        for i in 1..=20 {
            tree.insert(i, i * 10);
        }
        for i in 1..=20 {
            assert_eq!(tree.search(i), Some(i * 10));
        }
    }

    #[test]
    fn test_btree_bulk() {
        let mut tree = BTree::new();
        for i in 0..1000 {
            tree.insert(i, i * 2);
        }
        assert_eq!(tree.len(), 1000);
        for i in 0..500 {
            assert!(tree.remove(i));
        }
        for i in 0..500 {
            assert_eq!(tree.search(i), None);
        }
        for i in 500..1000 {
            assert_eq!(tree.search(i), Some(i * 2));
        }
    }
}
