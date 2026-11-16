use std::collections::HashMap;
use std::sync::{Arc, mpsc};
use crate::concurrency::thread_pool::ThreadPool;
use crate::core::traits::{MapReduceEngine, MapReduceTask};

pub struct ThreadPoolEngine {
    pool: ThreadPool,
    num_threads: usize,
}

impl ThreadPoolEngine {
    pub fn new(num_threads: usize) -> Self {
        assert!(num_threads > 0, "Number of threads must be greater than 0");
        Self {
            pool: ThreadPool::new(num_threads),
            num_threads,
        }
    }

    fn split_into_chunks<I>(mut items: Vec<I>, num_chunks: usize) -> Vec<Vec<I>> {
        if items.is_empty() {
            return Vec::new();
        }
        let chunk_size = (items.len() + num_chunks - 1) / num_chunks;
        let mut chunks = Vec::with_capacity(num_chunks);

        while !items.is_empty() {
            let drain_end = chunk_size.min(items.len());
            let chunk: Vec<I> = items.drain(0..drain_end).collect();
            chunks.push(chunk);
        }
        chunks
    }
}

impl MapReduceEngine for ThreadPoolEngine {
    fn execute<T>(&self, task: Arc<T>, inputs: Vec<T::Input>) -> HashMap<T::IntermediateKey, T::Output>
    where
        T: MapReduceTask + 'static,
    {
        if inputs.is_empty() {
            return HashMap::new();
        }

        let input_chunks = Self::split_into_chunks(inputs, self.num_threads);
        let num_map_tasks = input_chunks.len();

        let (map_tx, map_rx) = mpsc::channel();

        for chunk in input_chunks {
            let task_clone = Arc::clone(&task);
            let tx_clone = map_tx.clone();

            self.pool.execute(move || {
                let mut local_results = Vec::new();
                for item in chunk {
                    local_results.extend(task_clone.map(item));
                }
                // Відправляємо пачку проміжних пар (Key, Value) назад у координатор
                tx_clone.send(local_results).expect("Failed to send map results");
            });
        }
        drop(map_tx);

        let mut grouped: HashMap<T::IntermediateKey, Vec<T::IntermediateValue>> = HashMap::new();

        for _ in 0..num_map_tasks {
            let batch = map_rx.recv().expect("Worker failed during map phase");
            for (key, value) in batch {
                grouped.entry(key).or_default().push(value);
            }
        }

        let grouped_vec: Vec<(T::IntermediateKey, Vec<T::IntermediateValue>)> =
            grouped.into_iter().collect();
        let reduce_chunks = Self::split_into_chunks(grouped_vec, self.num_threads);
        let num_reduce_tasks = reduce_chunks.len();

        let (reduce_tx, reduce_rx) = mpsc::channel();

        for chunk in reduce_chunks {
            let task_clone = Arc::clone(&task);
            let tx_clone = reduce_tx.clone();

            self.pool.execute(move || {
                let mut local_reduced = Vec::with_capacity(chunk.len());
                for (key, values) in chunk {
                    let output = task_clone.reduce(&key, &values);
                    local_reduced.push((key, output));
                }
                tx_clone
                    .send(local_reduced)
                    .expect("Failed to send reduce results");
            });
        }
        drop(reduce_tx);

        let mut final_result = HashMap::new();
        for _ in 0..num_reduce_tasks {
            let batch = reduce_rx.recv().expect("Worker failed during reduce phase");
            final_result.extend(batch);
        }

        final_result
    }
}
