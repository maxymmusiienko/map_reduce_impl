use std::sync::Arc;
use map_reduce_impl::algorithms::word_count::WordCountTask;
use map_reduce_impl::core::traits::MapReduceEngine;
use map_reduce_impl::engines::sequential::SequentialEngine;
use map_reduce_impl::engines::thread_pool::ThreadPoolEngine;

fn main() {
    let task = Arc::new(WordCountTask);
    let sample_data = vec![
        String::from("Rust concurrency is fearless"),
        String::from("Fearless concurrency in Rust is fast"),
        String::from("MapReduce pattern works great with Rust threads"),
    ];

    let seq_engine = SequentialEngine;
    let pool_engine = ThreadPoolEngine::new(4);

    let seq_res = seq_engine.execute(Arc::clone(&task), sample_data.clone());
    let pool_res = pool_engine.execute(Arc::clone(&task), sample_data);

    assert_eq!(seq_res, pool_res);
    println!("Success! Both engines produced identical results: {:?}", pool_res);
}
