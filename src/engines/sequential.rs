use std::collections::HashMap;
use std::sync::Arc;
use crate::core::traits::{MapReduceEngine, MapReduceTask};

pub struct SequentialEngine;

impl MapReduceEngine for SequentialEngine {
    fn execute<T>(&self, task: Arc<T>, inputs: Vec<T::Input>) -> HashMap<T::IntermediateKey, T::Output>
    where
        T: MapReduceTask + 'static
    {
        let mut grouped: HashMap<T::IntermediateKey, Vec<T::IntermediateValue>> = HashMap::new();
        for input in inputs {
            for (key, value) in task.map(input) {
                grouped.entry(key).or_default().push(value);
            }
        }

        grouped
            .into_iter()
            .map(|(key, values)| {
                let output = task.reduce(&key, &values);
                (key, output)
            })
            .collect()
    }
}
