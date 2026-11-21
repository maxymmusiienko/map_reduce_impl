use std::sync::Arc;
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};

use map_reduce_impl::algorithms::word_count::WordCountTask;
use map_reduce_impl::core::traits::MapReduceEngine;
use map_reduce_impl::engines::rayon_pool::RayonEngine;
use map_reduce_impl::engines::sequential::SequentialEngine;
use map_reduce_impl::engines::thread_pool::ThreadPoolEngine;

fn generate_dataset(num_lines: usize) -> Vec<String> {
    let base_phrases = [
        "Rust concurrency is fearless and memory safe without garbage collection",
        "MapReduce pattern distributes data processing across multiple worker threads",
        "Systems programming requires deep understanding of ownership and lifetimes",
        "Work stealing schedulers balance CPU load dynamically at runtime",
        "Cryptographic algorithms and lattice reductions benefit from parallel execution",
    ];

    (0..num_lines)
        .map(|i| base_phrases[i % base_phrases.len()].to_string())
        .collect()
}

fn bench_word_count(c: &mut Criterion) {
    let mut group = c.benchmark_group("WordCount_Engines");

    group.sample_size(20);

    let dataset_size = 100_000;
    let dataset = generate_dataset(dataset_size);
    let task = Arc::new(WordCountTask);

    let num_threads = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4);

    let seq_engine = SequentialEngine;
    group.bench_with_input(
        BenchmarkId::new("Sequential", dataset_size),
        &dataset,
        |b, data| {
            b.iter(|| {
                let res = seq_engine.execute(Arc::clone(&task), black_box(data.clone()));
                black_box(res);
            });
        },
    );

    let pool_engine = ThreadPoolEngine::new(num_threads);
    group.bench_with_input(
        BenchmarkId::new(format!("CustomThreadPool_{}t", num_threads), dataset_size),
        &dataset,
        |b, data| {
            b.iter(|| {
                let res = pool_engine.execute(Arc::clone(&task), black_box(data.clone()));
                black_box(res);
            });
        },
    );

    let rayon_engine = RayonEngine;
    group.bench_with_input(
        BenchmarkId::new("Rayon", dataset_size),
        &dataset,
        |b, data| {
            b.iter(|| {
                let res = rayon_engine.execute(Arc::clone(&task), black_box(data.clone()));
                black_box(res);
            });
        },
    );

    group.finish();
}

criterion_group!(benches, bench_word_count);
criterion_main!(benches);
