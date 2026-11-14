use std::collections::HashMap;
use std::hash::Hash;

pub trait MapReduceTask: Send + Sync {
    type Input: Send + 'static;
    type IntermediateValue: Send + 'static;
    type IntermediateKey: Send + Hash + Eq + 'static;
    type Output: Send + 'static;
    
    fn map(&self, input: Self::Input) -> Vec<(Self::IntermediateKey, Self::IntermediateValue)>;
    
    fn reduce(&self, key: &Self::IntermediateKey, values: &[Self::IntermediateValue]) -> Self::Output;
}

pub trait MapReduceEngine {
    fn execute<T>(&self, task: &T, inputs: Vec<T::Input>) -> HashMap<T::IntermediateKey, T::Output>
    where
        T: MapReduceTask;
}
