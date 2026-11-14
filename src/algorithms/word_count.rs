use crate::core::traits::MapReduceTask;

pub struct WordCountTask;

impl MapReduceTask for WordCountTask {
    type Input = String;
    type IntermediateValue = usize;
    type IntermediateKey = String;
    type Output = usize;

    fn map(&self, input: Self::Input) -> Vec<(Self::IntermediateKey, Self::IntermediateValue)> {
        input
            .split_whitespace()
            .map(|word| (word.to_lowercase(), 1))
            .collect()
    }

    fn reduce(&self, _key: &Self::IntermediateKey, values: &[Self::IntermediateValue]) -> Self::Output {
        values.iter().sum()
    }
}
