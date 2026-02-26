use rand::Rng;
use rand::SeedableRng;
use rand_xoshiro::Xoshiro256PlusPlus;

use crate::schedule::Schedule;
use crate::workloads::Workload;

const DIMS: usize = 3;
const INITIAL_POINTS: usize = 10_000;
const ARENA_CAPACITY: usize = 32_768;
const NONE: usize = usize::MAX;

/// Batch size: how many tree ops each thread submits per lock.schedule() call.
const BATCH_SIZE: usize = 1000;

type Point = [f64; DIMS];

/// Arena-based KD-tree node — no heap allocation during operations.
struct KdNode {
    point: Point,
    left: usize,  // index into arena, NONE = no child
    right: usize,
}

impl KdNode {
    fn empty() -> Self {
        Self {
            point: [0.0; DIMS],
            left: NONE,
            right: NONE,
        }
    }
}

pub struct KdTree {
    arena: Vec<KdNode>,
    root: usize,
    next_free: usize,
}

impl KdTree {
    fn new() -> Self {
        let arena = (0..ARENA_CAPACITY).map(|_| KdNode::empty()).collect();
        Self {
            arena,
            root: NONE,
            next_free: 0,
        }
    }

    fn alloc_node(&mut self, point: Point) -> usize {
        if self.next_free >= ARENA_CAPACITY {
            return NONE; // arena full
        }
        let idx = self.next_free;
        self.next_free += 1;
        self.arena[idx] = KdNode { point, left: NONE, right: NONE };
        idx
    }

    fn build(points: &mut [Point]) -> Self {
        let mut tree = Self::new();
        tree.root = tree.build_recursive(points, 0);
        tree
    }

    fn build_recursive(&mut self, points: &mut [Point], depth: usize) -> usize {
        if points.is_empty() {
            return NONE;
        }
        let axis = depth % DIMS;
        points.sort_unstable_by(|a, b| a[axis].partial_cmp(&b[axis]).unwrap());
        let mid = points.len() / 2;
        let point = points[mid];
        let idx = self.alloc_node(point);
        if idx == NONE {
            return NONE;
        }
        self.arena[idx].left = self.build_recursive(&mut points[..mid], depth + 1);
        if mid + 1 < points.len() {
            self.arena[idx].right = self.build_recursive(&mut points[mid + 1..], depth + 1);
        }
        idx
    }

    fn insert(&mut self, point: Point) {
        if self.root == NONE {
            self.root = self.alloc_node(point);
            return;
        }
        self.insert_at(self.root, point, 0);
    }

    fn insert_at(&mut self, idx: usize, point: Point, depth: usize) {
        let axis = depth % DIMS;
        if point[axis] < self.arena[idx].point[axis] {
            if self.arena[idx].left == NONE {
                self.arena[idx].left = self.alloc_node(point);
            } else {
                self.insert_at(self.arena[idx].left, point, depth + 1);
            }
        } else {
            if self.arena[idx].right == NONE {
                self.arena[idx].right = self.alloc_node(point);
            } else {
                self.insert_at(self.arena[idx].right, point, depth + 1);
            }
        }
    }

    fn nearest(&self, target: &Point) -> Option<Point> {
        if self.root == NONE {
            return None;
        }
        let mut best_point = self.arena[self.root].point;
        let mut best_dist = distance_sq(&best_point, target);
        self.nearest_recursive(self.root, target, 0, &mut best_point, &mut best_dist);
        Some(best_point)
    }

    fn nearest_recursive(
        &self,
        idx: usize,
        target: &Point,
        depth: usize,
        best: &mut Point,
        best_dist: &mut f64,
    ) {
        if idx == NONE {
            return;
        }
        let node = &self.arena[idx];
        let dist = distance_sq(&node.point, target);
        if dist < *best_dist {
            *best_dist = dist;
            *best = node.point;
        }

        let axis = depth % DIMS;
        let diff = target[axis] - node.point[axis];
        let (first, second) = if diff < 0.0 {
            (node.left, node.right)
        } else {
            (node.right, node.left)
        };

        self.nearest_recursive(first, target, depth + 1, best, best_dist);
        if diff * diff < *best_dist {
            self.nearest_recursive(second, target, depth + 1, best, best_dist);
        }
    }
}

fn distance_sq(a: &Point, b: &Point) -> f64 {
    let mut sum = 0.0;
    for i in 0..DIMS {
        let d = a[i] - b[i];
        sum += d * d;
    }
    sum
}

#[derive(Clone, Copy)]
enum KdOp {
    Search(Point),
    Insert(Point),
}

pub struct KdTreeWorkload;

impl Workload for KdTreeWorkload {
    type State = KdTree;

    fn name(&self) -> &'static str {
        "kdtree"
    }

    fn description(&self) -> &'static str {
        "Arena KD-tree — batched search/insert, no allocations inside lock"
    }

    fn init_state(&self) -> Self::State {
        let mut rng = Xoshiro256PlusPlus::seed_from_u64(0xCAFEBABE);
        let mut points: Vec<Point> = (0..INITIAL_POINTS)
            .map(|_| {
                [
                    rng.random::<f64>() * 1000.0,
                    rng.random::<f64>() * 1000.0,
                    rng.random::<f64>() * 1000.0,
                ]
            })
            .collect();
        KdTree::build(&mut points)
    }

    fn run_thread<S: Schedule<Self::State>>(
        &self,
        lock: &S,
        thread_id: usize,
        _thread_count: usize,
        ops: usize,
    ) {
        let mut rng = Xoshiro256PlusPlus::seed_from_u64(thread_id as u64 * 54321 + 11111);

        let operations: Vec<KdOp> = (0..ops)
            .map(|_| {
                let point = [
                    rng.random::<f64>() * 1000.0,
                    rng.random::<f64>() * 1000.0,
                    rng.random::<f64>() * 1000.0,
                ];
                if rng.random::<f64>() < 0.7 {
                    KdOp::Search(point)
                } else {
                    KdOp::Insert(point)
                }
            })
            .collect();

        for batch in operations.chunks(BATCH_SIZE) {
            lock.schedule(|tree| {
                for &op in batch {
                    match op {
                        KdOp::Search(target) => {
                            let _ = tree.nearest(&target);
                        }
                        KdOp::Insert(point) => {
                            tree.insert(point);
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
    fn test_kdtree_insert_and_nearest() {
        let mut tree = KdTree::new();
        tree.insert([0.0, 0.0, 0.0]);
        tree.insert([1.0, 1.0, 1.0]);
        tree.insert([5.0, 5.0, 5.0]);

        let nearest = tree.nearest(&[0.1, 0.1, 0.1]).unwrap();
        assert_eq!(nearest, [0.0, 0.0, 0.0]);

        let nearest = tree.nearest(&[4.9, 4.9, 4.9]).unwrap();
        assert_eq!(nearest, [5.0, 5.0, 5.0]);
    }

    #[test]
    fn test_kdtree_build() {
        let mut points: Vec<Point> = vec![
            [1.0, 2.0, 3.0],
            [4.0, 5.0, 6.0],
            [7.0, 8.0, 9.0],
            [2.0, 3.0, 1.0],
        ];
        let tree = KdTree::build(&mut points);
        assert!(tree.root != NONE);

        let nearest = tree.nearest(&[1.0, 2.0, 3.0]).unwrap();
        assert_eq!(nearest, [1.0, 2.0, 3.0]);
    }
}
