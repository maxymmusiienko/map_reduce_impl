use crate::core::traits::MapReduceTask;

pub struct MatrixVectorMultiplicationTask;

impl MapReduceTask for MatrixVectorMultiplicationTask {
    type Input = Vec<f64>;
    type IntermediateValue = f64;
    type IntermediateKey = usize;
    type Output = f64;

    fn map(&self, input: Self::Input) -> Vec<(Self::IntermediateKey, Self::IntermediateValue)> {
        todo!()
    }

    fn reduce(&self, key: &Self::IntermediateKey, values: &[Self::IntermediateValue]) -> Self::Output {
        todo!()
    }
}
