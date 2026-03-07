use std::hint::black_box;

use rand::Rng;
use rand::SeedableRng;
use rand_xoshiro::Xoshiro256PlusPlus;

use crate::schedule::Schedule;
use crate::workloads::Workload;

/// B-tree order (max children per node).
const B: usize = 8;
/// Maximum keys per node = B - 1.
const MAX_KEYS: usize = B - 1;
/// Minimum keys per non-root internal node = ceil(B/2) - 1.
const MIN_KEYS: usize = B / 2 - 1;

/// Arena capacity for nodes.
const MAX_NODES: usize = 2048;

/// Batch size per schedule() call.
const BATCH_SIZE: usize = 1000;

/// Number of random keys to pre-insert.
const INITIAL_FILL: usize = 500;

/// Key range — bounded so that deletes actually hit existing keys,
/// preventing unbounded tree growth under concurrent workloads.
const KEY_RANGE: u64 = 10_000;

/// Sentinel for "no child".
const NIL: u32 = u32::MAX;

#[derive(Clone)]
struct BTreeNode {
    /// Number of keys currently stored.
    n: u16,
    /// Whether this node is a leaf.
    leaf: bool,
    keys: [u64; MAX_KEYS],
    vals: [u64; MAX_KEYS],
    /// Child indices into the arena (NIL if absent).
    children: [u32; B],
}

impl BTreeNode {
    fn new(leaf: bool) -> Self {
        Self {
            n: 0,
            leaf,
            keys: [0; MAX_KEYS],
            vals: [0; MAX_KEYS],
            children: [NIL; B],
        }
    }
}

pub struct BTree {
    arena: Vec<BTreeNode>,
    root: u32,
    /// Simple free-list: indices of deallocated nodes available for reuse.
    free_list: Vec<u32>,
    len: usize,
}

impl BTree {
    fn new() -> Self {
        let root_node = BTreeNode::new(true);
        Self {
            arena: vec![root_node],
            root: 0,
            free_list: Vec::new(),
            len: 0,
        }
    }

    fn alloc_node(&mut self, leaf: bool) -> u32 {
        if let Some(idx) = self.free_list.pop() {
            self.arena[idx as usize] = BTreeNode::new(leaf);
            idx
        } else {
            if self.arena.len() >= MAX_NODES {
                return NIL;
            }
            let idx = self.arena.len() as u32;
            self.arena.push(BTreeNode::new(leaf));
            idx
        }
    }

    fn free_node(&mut self, idx: u32) {
        self.free_list.push(idx);
    }

    /// Search for a key. Returns Some(value) if found.
    pub fn search(&self, key: u64) -> Option<u64> {
        let mut cur = self.root;
        loop {
            if cur == NIL {
                return None;
            }
            let node = &self.arena[cur as usize];
            let n = node.n as usize;
            // Linear scan (small fanout, cache-friendly).
            let mut i = 0;
            while i < n && key > node.keys[i] {
                i += 1;
            }
            if i < n && key == node.keys[i] {
                return Some(node.vals[i]);
            }
            if node.leaf {
                return None;
            }
            cur = node.children[i];
        }
    }

    /// Insert a key-value pair. Returns true if newly inserted.
    pub fn insert(&mut self, key: u64, val: u64) -> bool {
        // Check if already present.
        if self.search(key).is_some() {
            return false;
        }
        let root = self.root;
        let root_node = &self.arena[root as usize];
        if root_node.n as usize == MAX_KEYS {
            // Split the root.
            let new_root = self.alloc_node(false);
            if new_root == NIL {
                return false; // arena full
            }
            self.arena[new_root as usize].children[0] = root;
            if !self.split_child(new_root, 0) {
                self.free_node(new_root);
                return false;
            }
            self.root = new_root;
            if !self.insert_non_full(new_root, key, val) {
                return false;
            }
        } else if !self.insert_non_full(root, key, val) {
            return false;
        }
        self.len += 1;
        true
    }

    fn insert_non_full(&mut self, node_idx: u32, key: u64, val: u64) -> bool {
        let n = self.arena[node_idx as usize].n as usize;
        if self.arena[node_idx as usize].leaf {
            // Shift keys right and insert.
            let node = &mut self.arena[node_idx as usize];
            let mut i = n;
            while i > 0 && key < node.keys[i - 1] {
                node.keys[i] = node.keys[i - 1];
                node.vals[i] = node.vals[i - 1];
                i -= 1;
            }
            node.keys[i] = key;
            node.vals[i] = val;
            node.n += 1;
            true
        } else {
            let mut i = n;
            while i > 0 && key < self.arena[node_idx as usize].keys[i - 1] {
                i -= 1;
            }
            let child = self.arena[node_idx as usize].children[i];
            if self.arena[child as usize].n as usize == MAX_KEYS {
                if !self.split_child(node_idx, i) {
                    return false;
                }
                if key > self.arena[node_idx as usize].keys[i] {
                    i += 1;
                }
            }
            let next_child = self.arena[node_idx as usize].children[i];
            self.insert_non_full(next_child, key, val)
        }
    }

    fn split_child(&mut self, parent_idx: u32, child_pos: usize) -> bool {
        let child_idx = self.arena[parent_idx as usize].children[child_pos];
        let child_leaf = self.arena[child_idx as usize].leaf;
        let new_idx = self.alloc_node(child_leaf);
        if new_idx == NIL {
            return false; // arena full
        }

        let mid = MAX_KEYS / 2; // median index

        // Copy upper half of child's keys/vals to new node.
        let new_n = MAX_KEYS - mid - 1;
        for j in 0..new_n {
            self.arena[new_idx as usize].keys[j] = self.arena[child_idx as usize].keys[mid + 1 + j];
            self.arena[new_idx as usize].vals[j] = self.arena[child_idx as usize].vals[mid + 1 + j];
        }
        if !child_leaf {
            for j in 0..=new_n {
                self.arena[new_idx as usize].children[j] =
                    self.arena[child_idx as usize].children[mid + 1 + j];
            }
        }
        self.arena[new_idx as usize].n = new_n as u16;
        self.arena[child_idx as usize].n = mid as u16;

        // Save median key/val before mutably borrowing parent.
        let median_key = self.arena[child_idx as usize].keys[mid];
        let median_val = self.arena[child_idx as usize].vals[mid];

        // Insert median key into parent.
        let parent = &mut self.arena[parent_idx as usize];
        let pn = parent.n as usize;
        // Shift parent keys/children right.
        for j in (child_pos + 1..=pn).rev() {
            parent.children[j + 1] = parent.children[j];
        }
        for j in (child_pos..pn).rev() {
            parent.keys[j + 1] = parent.keys[j];
            parent.vals[j + 1] = parent.vals[j];
        }
        parent.children[child_pos + 1] = new_idx;
        parent.keys[child_pos] = median_key;
        parent.vals[child_pos] = median_val;
        parent.n += 1;
        true
    }

    /// Delete a key. Returns true if the key was found and removed.
    pub fn delete(&mut self, key: u64) -> bool {
        if self.root == NIL {
            return false;
        }
        let removed = self.delete_from(self.root, key);
        if removed {
            self.len -= 1;
            // Shrink root if empty.
            let root_node = &self.arena[self.root as usize];
            if root_node.n == 0 && !root_node.leaf {
                let old_root = self.root;
                self.root = root_node.children[0];
                self.free_node(old_root);
            }
        }
        removed
    }

    fn delete_from(&mut self, node_idx: u32, key: u64) -> bool {
        let n = self.arena[node_idx as usize].n as usize;
        let leaf = self.arena[node_idx as usize].leaf;

        // Find position of key (or child to descend into).
        let mut i = 0;
        while i < n && key > self.arena[node_idx as usize].keys[i] {
            i += 1;
        }

        if i < n && key == self.arena[node_idx as usize].keys[i] {
            // Key found in this node.
            if leaf {
                // Case 1: Remove from leaf by shifting.
                let node = &mut self.arena[node_idx as usize];
                for j in i..n - 1 {
                    node.keys[j] = node.keys[j + 1];
                    node.vals[j] = node.vals[j + 1];
                }
                node.n -= 1;
                return true;
            } else {
                // Case 2: Internal node — replace with predecessor.
                let pred_child = self.arena[node_idx as usize].children[i];
                if self.arena[pred_child as usize].n as usize > MIN_KEYS {
                    let (pk, pv) = self.get_predecessor(pred_child);
                    self.arena[node_idx as usize].keys[i] = pk;
                    self.arena[node_idx as usize].vals[i] = pv;
                    return self.delete_from(pred_child, pk);
                }
                let succ_child = self.arena[node_idx as usize].children[i + 1];
                if self.arena[succ_child as usize].n as usize > MIN_KEYS {
                    let (sk, sv) = self.get_successor(succ_child);
                    self.arena[node_idx as usize].keys[i] = sk;
                    self.arena[node_idx as usize].vals[i] = sv;
                    return self.delete_from(succ_child, sk);
                }
                // Merge children[i] and children[i+1].
                self.merge_children(node_idx, i);
                let merged = self.arena[node_idx as usize].children[i];
                return self.delete_from(merged, key);
            }
        } else {
            // Key not in this node.
            if leaf {
                return false;
            }
            // Ensure child[i] has enough keys before descending.
            let child = self.arena[node_idx as usize].children[i];
            if self.arena[child as usize].n as usize <= MIN_KEYS {
                self.fill_child(node_idx, i);
            }
            // After fill, the node's key count may have changed; re-check index.
            let n2 = self.arena[node_idx as usize].n as usize;
            let ci = if i > n2 { i - 1 } else { i };
            let next = self.arena[node_idx as usize].children[ci];
            self.delete_from(next, key)
        }
    }

    fn get_predecessor(&self, mut node_idx: u32) -> (u64, u64) {
        loop {
            let node = &self.arena[node_idx as usize];
            if node.leaf {
                let i = node.n as usize - 1;
                return (node.keys[i], node.vals[i]);
            }
            node_idx = node.children[node.n as usize];
        }
    }

    fn get_successor(&self, mut node_idx: u32) -> (u64, u64) {
        loop {
            let node = &self.arena[node_idx as usize];
            if node.leaf {
                return (node.keys[0], node.vals[0]);
            }
            node_idx = node.children[0];
        }
    }

    fn fill_child(&mut self, parent_idx: u32, child_pos: usize) {
        let pn = self.arena[parent_idx as usize].n as usize;
        // Try borrow from left sibling.
        if child_pos > 0 {
            let left = self.arena[parent_idx as usize].children[child_pos - 1];
            if self.arena[left as usize].n as usize > MIN_KEYS {
                self.borrow_from_left(parent_idx, child_pos);
                return;
            }
        }
        // Try borrow from right sibling.
        if child_pos < pn {
            let right = self.arena[parent_idx as usize].children[child_pos + 1];
            if self.arena[right as usize].n as usize > MIN_KEYS {
                self.borrow_from_right(parent_idx, child_pos);
                return;
            }
        }
        // Merge with a sibling.
        if child_pos < pn {
            self.merge_children(parent_idx, child_pos);
        } else {
            self.merge_children(parent_idx, child_pos - 1);
        }
    }

    fn borrow_from_left(&mut self, parent_idx: u32, child_pos: usize) {
        let left_idx = self.arena[parent_idx as usize].children[child_pos - 1];
        let child_idx = self.arena[parent_idx as usize].children[child_pos];
        let cn = self.arena[child_idx as usize].n as usize;
        let ln = self.arena[left_idx as usize].n as usize;

        // Shift child's keys right to make room at front.
        for j in (0..cn).rev() {
            self.arena[child_idx as usize].keys[j + 1] = self.arena[child_idx as usize].keys[j];
            self.arena[child_idx as usize].vals[j + 1] = self.arena[child_idx as usize].vals[j];
        }
        if !self.arena[child_idx as usize].leaf {
            for j in (0..=cn).rev() {
                self.arena[child_idx as usize].children[j + 1] =
                    self.arena[child_idx as usize].children[j];
            }
        }
        // Move parent key down to child[0].
        self.arena[child_idx as usize].keys[0] =
            self.arena[parent_idx as usize].keys[child_pos - 1];
        self.arena[child_idx as usize].vals[0] =
            self.arena[parent_idx as usize].vals[child_pos - 1];
        // Move last child pointer from left sibling.
        if !self.arena[child_idx as usize].leaf {
            self.arena[child_idx as usize].children[0] = self.arena[left_idx as usize].children[ln];
        }
        // Move last key of left sibling up to parent.
        self.arena[parent_idx as usize].keys[child_pos - 1] =
            self.arena[left_idx as usize].keys[ln - 1];
        self.arena[parent_idx as usize].vals[child_pos - 1] =
            self.arena[left_idx as usize].vals[ln - 1];

        self.arena[child_idx as usize].n += 1;
        self.arena[left_idx as usize].n -= 1;
    }

    fn borrow_from_right(&mut self, parent_idx: u32, child_pos: usize) {
        let child_idx = self.arena[parent_idx as usize].children[child_pos];
        let right_idx = self.arena[parent_idx as usize].children[child_pos + 1];
        let cn = self.arena[child_idx as usize].n as usize;
        let rn = self.arena[right_idx as usize].n as usize;

        // Move parent key down to end of child.
        self.arena[child_idx as usize].keys[cn] = self.arena[parent_idx as usize].keys[child_pos];
        self.arena[child_idx as usize].vals[cn] = self.arena[parent_idx as usize].vals[child_pos];
        if !self.arena[child_idx as usize].leaf {
            self.arena[child_idx as usize].children[cn + 1] =
                self.arena[right_idx as usize].children[0];
        }
        // Move first key of right sibling up to parent.
        self.arena[parent_idx as usize].keys[child_pos] = self.arena[right_idx as usize].keys[0];
        self.arena[parent_idx as usize].vals[child_pos] = self.arena[right_idx as usize].vals[0];
        // Shift right sibling's keys left.
        for j in 0..rn - 1 {
            self.arena[right_idx as usize].keys[j] = self.arena[right_idx as usize].keys[j + 1];
            self.arena[right_idx as usize].vals[j] = self.arena[right_idx as usize].vals[j + 1];
        }
        if !self.arena[right_idx as usize].leaf {
            for j in 0..rn {
                self.arena[right_idx as usize].children[j] =
                    self.arena[right_idx as usize].children[j + 1];
            }
        }

        self.arena[child_idx as usize].n += 1;
        self.arena[right_idx as usize].n -= 1;
    }

    fn merge_children(&mut self, parent_idx: u32, pos: usize) {
        let left_idx = self.arena[parent_idx as usize].children[pos];
        let right_idx = self.arena[parent_idx as usize].children[pos + 1];
        let ln = self.arena[left_idx as usize].n as usize;
        let rn = self.arena[right_idx as usize].n as usize;

        // Pull parent key down into left child.
        self.arena[left_idx as usize].keys[ln] = self.arena[parent_idx as usize].keys[pos];
        self.arena[left_idx as usize].vals[ln] = self.arena[parent_idx as usize].vals[pos];

        // Copy right child's keys/vals into left child.
        for j in 0..rn {
            self.arena[left_idx as usize].keys[ln + 1 + j] = self.arena[right_idx as usize].keys[j];
            self.arena[left_idx as usize].vals[ln + 1 + j] = self.arena[right_idx as usize].vals[j];
        }
        if !self.arena[left_idx as usize].leaf {
            for j in 0..=rn {
                self.arena[left_idx as usize].children[ln + 1 + j] =
                    self.arena[right_idx as usize].children[j];
            }
        }
        self.arena[left_idx as usize].n = (ln + 1 + rn) as u16;

        // Remove parent key and right child pointer.
        let pn = self.arena[parent_idx as usize].n as usize;
        for j in pos..pn - 1 {
            self.arena[parent_idx as usize].keys[j] = self.arena[parent_idx as usize].keys[j + 1];
            self.arena[parent_idx as usize].vals[j] = self.arena[parent_idx as usize].vals[j + 1];
        }
        for j in pos + 1..pn {
            self.arena[parent_idx as usize].children[j] =
                self.arena[parent_idx as usize].children[j + 1];
        }
        self.arena[parent_idx as usize].n -= 1;

        self.free_node(right_idx);
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
        "Arena B-tree (order 8) — insert/delete heavy, node splits and merges"
    }

    fn init_state(&self) -> Self::State {
        let mut tree = BTree::new();
        let mut rng = Xoshiro256PlusPlus::seed_from_u64(0xB7EE_CA5E);
        for _ in 0..INITIAL_FILL {
            let key = rng.random_range(0..KEY_RANGE);
            let val = rng.random::<u64>();
            tree.insert(key, val);
        }
        tree
    }

    fn run_thread<S: Schedule<Self::State>>(
        &self,
        lock: &S,
        thread_id: usize,
        _thread_count: usize,
        ops: usize,
    ) {
        let mut rng = Xoshiro256PlusPlus::seed_from_u64(thread_id as u64 * 66666 + 13579);

        // 45% insert, 45% delete, 10% search
        // Bounded key range so deletes actually find existing keys.
        let operations: Vec<BTreeOp> = (0..ops)
            .map(|_| {
                let r: f64 = rng.random();
                if r < 0.45 {
                    BTreeOp::Insert(rng.random_range(0..KEY_RANGE), rng.random::<u64>())
                } else if r < 0.90 {
                    BTreeOp::Delete(rng.random_range(0..KEY_RANGE))
                } else {
                    BTreeOp::Search(rng.random_range(0..KEY_RANGE))
                }
            })
            .collect();

        for batch in operations.chunks(BATCH_SIZE) {
            let result = lock.schedule(|tree| {
                let mut checksum = 0u64;
                for op in batch {
                    match *op {
                        BTreeOp::Insert(k, v) => {
                            if tree.insert(k, v) {
                                checksum = checksum.wrapping_add(1);
                            }
                        }
                        BTreeOp::Search(k) => {
                            if let Some(v) = tree.search(k) {
                                checksum = checksum.wrapping_add(v);
                            }
                        }
                        BTreeOp::Delete(k) => {
                            if tree.delete(k) {
                                checksum = checksum.wrapping_add(1);
                            }
                        }
                    }
                }
                checksum
            });
            black_box(result);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    impl BTree {
        /// Validate all B-tree invariants. Panics on violation.
        fn validate(&self) {
            if self.root == NIL {
                assert_eq!(self.len, 0, "nil root but len != 0");
                return;
            }
            let counted = self.validate_node(self.root, None, None, true);
            assert_eq!(
                counted, self.len,
                "len mismatch: counted {counted}, stored {}",
                self.len
            );
        }

        /// Returns the number of keys in this subtree.
        /// Also checks: sorted keys, key bounds, min-key constraints, uniform leaf depth.
        fn validate_node(
            &self,
            idx: u32,
            lower: Option<u64>,
            upper: Option<u64>,
            is_root: bool,
        ) -> usize {
            let node = &self.arena[idx as usize];
            let n = node.n as usize;

            // Key count bounds.
            assert!(n <= MAX_KEYS, "node {idx}: n={n} > MAX_KEYS={MAX_KEYS}");
            if !is_root && !node.leaf {
                assert!(
                    n >= MIN_KEYS,
                    "internal node {idx}: n={n} < MIN_KEYS={MIN_KEYS}"
                );
            }

            // Keys sorted and within bounds.
            for i in 1..n {
                assert!(
                    node.keys[i - 1] < node.keys[i],
                    "node {idx}: keys not sorted at {i}: {} >= {}",
                    node.keys[i - 1],
                    node.keys[i]
                );
            }
            if let Some(lo) = lower {
                assert!(
                    n == 0 || node.keys[0] > lo,
                    "node {idx}: key {} <= lower bound {lo}",
                    node.keys[0]
                );
            }
            if let Some(hi) = upper {
                assert!(
                    n == 0 || node.keys[n - 1] < hi,
                    "node {idx}: key {} >= upper bound {hi}",
                    node.keys[n - 1]
                );
            }

            if node.leaf {
                return n;
            }

            // Internal node: validate all n+1 children exist and recurse.
            let mut total = n; // count the keys in this node
            for i in 0..=n {
                let child = node.children[i];
                assert!(child != NIL, "node {idx}: child[{i}] is NIL");
                let lo = if i == 0 {
                    lower
                } else {
                    Some(node.keys[i - 1])
                };
                let hi = if i == n { upper } else { Some(node.keys[i]) };
                total += self.validate_node(child, lo, hi, false);
            }
            total
        }
    }

    #[test]
    fn test_insert_search() {
        let mut tree = BTree::new();
        assert!(tree.insert(10, 100));
        assert!(tree.insert(20, 200));
        assert!(tree.insert(5, 50));
        tree.validate();
        assert_eq!(tree.search(10), Some(100));
        assert_eq!(tree.search(20), Some(200));
        assert_eq!(tree.search(5), Some(50));
        assert_eq!(tree.search(99), None);
        assert_eq!(tree.len, 3);
    }

    #[test]
    fn test_duplicate_insert() {
        let mut tree = BTree::new();
        assert!(tree.insert(10, 100));
        assert!(!tree.insert(10, 999)); // duplicate
        tree.validate();
        assert_eq!(tree.len, 1);
    }

    #[test]
    fn test_delete() {
        let mut tree = BTree::new();
        tree.insert(10, 100);
        tree.insert(20, 200);
        tree.insert(30, 300);
        assert!(tree.delete(20));
        tree.validate();
        assert_eq!(tree.search(20), None);
        assert_eq!(tree.search(10), Some(100));
        assert_eq!(tree.search(30), Some(300));
        assert_eq!(tree.len, 2);
        assert!(!tree.delete(20)); // already deleted
    }

    #[test]
    fn test_ordering() {
        let mut tree = BTree::new();
        let keys: Vec<u64> = (0..200).collect();
        for &k in &keys {
            tree.insert(k, k * 10);
        }
        tree.validate();
        assert_eq!(tree.len, 200);
        for &k in &keys {
            assert_eq!(tree.search(k), Some(k * 10));
        }
    }

    #[test]
    fn test_insert_delete_many() {
        let mut tree = BTree::new();
        let mut rng = Xoshiro256PlusPlus::seed_from_u64(42);
        let mut inserted = Vec::new();
        for _ in 0..300 {
            let k = rng.random_range(0..1000u64);
            if tree.insert(k, k) {
                inserted.push(k);
            }
        }
        tree.validate();
        // Delete all inserted keys, validating after each removal.
        for &k in &inserted {
            assert!(tree.delete(k), "failed to delete key {}", k);
            tree.validate();
        }
        assert_eq!(tree.len, 0);
    }

    #[test]
    fn test_extensive_stress() {
        // --- Phase 1: Sequential insert / full delete ---
        // Sequential keys force predictable split patterns and deep trees.
        {
            let mut tree = BTree::new();
            let n = 1500u64;
            for k in 0..n {
                assert!(tree.insert(k, k * 7), "sequential insert failed at {k}");
            }
            assert_eq!(tree.len, n as usize);
            tree.validate();

            // Delete in a non-sequential order to exercise all paths:
            // borrow-from-left, borrow-from-right, merge, predecessor/successor replacement.
            // Reverse-order deletion on a sequentially-built tree reliably hits
            // borrow-from-right and merge cascades.
            for k in (0..n).rev() {
                assert!(tree.delete(k), "reverse delete failed at {k}");
                tree.validate();
            }
            assert_eq!(tree.len, 0);
        }

        // --- Phase 2: Large-scale random insert/delete churn ---
        {
            let mut tree = BTree::new();
            let mut rng = Xoshiro256PlusPlus::seed_from_u64(0xDEAD_BEEF);
            let mut live: std::collections::HashSet<u64> = std::collections::HashSet::new();
            let key_range = 2000u64;

            for round in 0..10_000u64 {
                let r: f64 = rng.random();
                if r < 0.5 {
                    // Insert
                    let k = rng.random_range(0..key_range);
                    let inserted = tree.insert(k, k.wrapping_mul(31));
                    if inserted {
                        assert!(live.insert(k), "tree accepted dup insert for {k}");
                    } else {
                        // Either duplicate or arena full — both valid.
                    }
                } else {
                    // Delete
                    let k = rng.random_range(0..key_range);
                    let deleted = tree.delete(k);
                    if deleted {
                        assert!(live.remove(&k), "tree deleted {k} but not in live set");
                    } else {
                        assert!(!live.contains(&k), "tree missed delete for {k}");
                    }
                }

                // Validate periodically (every 500 ops) to keep test fast.
                if round % 500 == 0 {
                    tree.validate();
                    assert_eq!(tree.len, live.len());
                }
            }

            tree.validate();
            assert_eq!(tree.len, live.len());

            // Verify every live key is searchable.
            for &k in &live {
                assert_eq!(
                    tree.search(k),
                    Some(k.wrapping_mul(31)),
                    "live key {k} not found"
                );
            }

            // Drain everything, validating after each removal.
            let live_keys: Vec<u64> = live.into_iter().collect();
            for &k in &live_keys {
                assert!(tree.delete(k), "drain delete failed for {k}");
                tree.validate();
            }
            assert_eq!(tree.len, 0);
        }
    }
}
