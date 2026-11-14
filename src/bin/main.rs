use map_reduce_impl::algorithms::word_count::WordCountTask;
use map_reduce_impl::core::traits::MapReduceTask;
use std::collections::HashMap;

fn main() {
    let task = WordCountTask;
    let sample_data = vec![
        String::from("Rust concurrency is fearless"),
        String::from("Fearless concurrency in Rust is fast"),
    ];

    // 1. Тестуємо фазу Map
    let mut mapped_pairs = Vec::new();
    for line in sample_data {
        mapped_pairs.extend(task.map(line));
    }
    println!("Map output: {:?}", mapped_pairs);

    // 2. Імітуємо фазу Shuffle (групуємо значення за ключами)
    let mut grouped: HashMap<String, Vec<usize>> = HashMap::new();
    for (key, value) in mapped_pairs {
        grouped.entry(key).or_default().push(value);
    }

    // 3. Тестуємо фазу Reduce
    println!("\nFinal Word Count:");
    for (key, values) in grouped {
        let count = task.reduce(&key, &values);
        println!("{}: {}", key, count);
    }
}
