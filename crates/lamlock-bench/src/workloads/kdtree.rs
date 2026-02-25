use rand::Rng;
use rand::SeedableRng;
use rand_xoshiro::Xoshiro256PlusPlus;

use crate::harness::ThreadRecorder;
use crate::schedule::Schedule;
use crate::workloads::Workload;

const DIMS: usize = 3;
const INITIAL_POINTS: usize = 10_000;

type Point = [f64; DIMS];

struct KdNode {
    point: Point,
    left: Option<Box<KdNode>>,
    right: Option<Box<KdNode>>,
}

pub struct KdTree {
    root: Option<Box<KdNode>>,
    size: usize,
}

impl KdTree {
    #[cfg(test)]
    fn new() -> Self {
        Self {
            root: None,
            size: 0,
        }
    }

    fn build(points: &mut [Point]) -> Self {
        let root = Self::build_recursive(points, 0);
        let size = points.len();
        Self {
            root,
            size,
        }
    }

    fn build_recursive(points: &mut [Point], depth: usize) -> Option<Box<KdNode>> {
        if points.is_empty() {
            return None;
        }
        let axis = depth % DIMS;
        points.sort_unstable_by(|a, b| a[axis].partial_cmp(&b[axis]).unwrap());
        let mid = points.len() / 2;
        let point = points[mid];
        let left = Self::build_recursive(&mut points[..mid], depth + 1);
        let right = if mid + 1 < points.len() {
            Self::build_recursive(&mut points[mid + 1..], depth + 1)
        } else {
            None
        };
        Some(Box::new(KdNode { point, left, right }))
    }

    fn insert(&mut self, point: Point) {
        self.root = Self::insert_recursive(self.root.take(), point, 0);
        self.size += 1;
    }

    fn insert_recursive(
        node: Option<Box<KdNode>>,
        point: Point,
        depth: usize,
    ) -> Option<Box<KdNode>> {
        match node {
            None => Some(Box::new(KdNode {
                point,
                left: None,
                right: None,
            })),
            Some(mut n) => {
                let axis = depth % DIMS;
                if point[axis] < n.point[axis] {
                    n.left = Self::insert_recursive(n.left, point, depth + 1);
                } else {
                    n.right = Self::insert_recursive(n.right, point, depth + 1);
                }
                Some(n)
            }
        }
    }

    fn nearest(&self, target: &Point) -> Option<Point> {
        let mut best = None;
        let mut best_dist = f64::INFINITY;
        Self::nearest_recursive(self.root.as_deref(), target, 0, &mut best, &mut best_dist);
        best
    }

    fn nearest_recursive(
        node: Option<&KdNode>,
        target: &Point,
        depth: usize,
        best: &mut Option<Point>,
        best_dist: &mut f64,
    ) {
        let Some(node) = node else { return };

        let dist = distance_sq(&node.point, target);
        if dist < *best_dist {
            *best_dist = dist;
            *best = Some(node.point);
        }

        let axis = depth % DIMS;
        let diff = target[axis] - node.point[axis];
        let (first, second) = if diff < 0.0 {
            (node.left.as_deref(), node.right.as_deref())
        } else {
            (node.right.as_deref(), node.left.as_deref())
        };

        Self::nearest_recursive(first, target, depth + 1, best, best_dist);
        if diff * diff < *best_dist {
            Self::nearest_recursive(second, target, depth + 1, best, best_dist);
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

pub struct KdTreeWorkload;

impl Workload for KdTreeWorkload {
    type State = KdTree;

    fn name(&self) -> &'static str {
        "kdtree"
    }

    fn description(&self) -> &'static str {
        "KD-tree concurrent search/insert — 70% nearest-neighbor, 30% insert"
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
        ops: usize,
        recorder: &mut ThreadRecorder,
    ) {
        let mut rng = Xoshiro256PlusPlus::seed_from_u64(thread_id as u64 * 54321 + 11111);

        // Pre-generate operation types and points
        let op_types: Vec<bool> = (0..ops).map(|_| rng.random::<f64>() < 0.7).collect();
        let points: Vec<Point> = (0..ops)
            .map(|_| {
                [
                    rng.random::<f64>() * 1000.0,
                    rng.random::<f64>() * 1000.0,
                    rng.random::<f64>() * 1000.0,
                ]
            })
            .collect();

        recorder.record();
        for i in 0..ops {
            if op_types[i] {
                // 70%: nearest-neighbor search
                let target = points[i];
                lock.schedule(|tree| {
                    let _ = tree.nearest(&target);
                });
            } else {
                // 30%: insert
                let point = points[i];
                lock.schedule(|tree| {
                    tree.insert(point);
                });
            }
            recorder.record();
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
        assert_eq!(tree.size, 4);

        let nearest = tree.nearest(&[1.0, 2.0, 3.0]).unwrap();
        assert_eq!(nearest, [1.0, 2.0, 3.0]);
    }
}
