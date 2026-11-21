use std::collections::HashMap;
use std::sync::Arc;

use rayon::prelude::*;

use crate::core::traits::{MapReduceEngine, MapReduceTask};

pub struct RayonEngine;

impl MapReduceEngine for RayonEngine {
    fn execute<T>(&self, task: Arc<T>, inputs: Vec<T::Input>) -> HashMap<T::IntermediateKey, T::Output>
    where
        T: MapReduceTask + 'static,
    {
        if inputs.is_empty() {
            return HashMap::new();
        }

        let mapped_pairs: Vec<(T::IntermediateKey, T::IntermediateValue)> = inputs
            .into_par_iter()
            .flat_map(|item| task.map(item))
            .collect();

        let mut grouped: HashMap<T::IntermediateKey, Vec<T::IntermediateValue>> = HashMap::new();
        for (key, value) in mapped_pairs {
            grouped.entry(key).or_default().push(value);
        }

        grouped
            .into_par_iter()
            .map(|(key, values)| {
                let output = task.reduce(&key, &values);
                (key, output)
            })
            .collect()
    }
}
