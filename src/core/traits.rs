use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;

pub trait MapReduceTask: Send + Sync {
    type Input: Send + 'static;
    type IntermediateValue: Send + 'static;
    type IntermediateKey: Send + Hash + Eq + 'static;
    type Output: Send + 'static;

    fn map(&self, input: Self::Input) -> Vec<(Self::IntermediateKey, Self::IntermediateValue)>;
    fn reduce(&self, key: &Self::IntermediateKey, values: &[Self::IntermediateValue]) -> Self::Output;
}

pub trait MapReduceEngine {
    fn execute<T>(&self, task: Arc<T>, inputs: Vec<T::Input>) -> HashMap<T::IntermediateKey, T::Output>
    where
        T: MapReduceTask + 'static;
}
