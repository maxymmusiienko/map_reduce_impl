use map_reduce_impl::algorithms::word_count::WordCountTask;
use map_reduce_impl::core::traits::MapReduceEngine;
use map_reduce_impl::engines::sequential::SequentialEngine;

fn main() {
    let engine = SequentialEngine;
    let task = WordCountTask;

    let sample_data = vec![
        String::from("Rust concurrency is fearless"),
        String::from("Fearless concurrency in Rust is fast"),
    ];

    let result = engine.execute(&task, sample_data);
    println!("Sequential result: {:?}", result);
}
