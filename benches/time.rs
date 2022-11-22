use std::time::{Instant, SystemTime};
use criterion::{black_box, criterion_group, criterion_main, Criterion};

fn instant() -> Instant {
    Instant::now()
}

fn system_time() -> SystemTime {
    SystemTime::now()
}

fn get_time_benchmark(c: &mut Criterion) {
    c.bench_function("instant", |b| b.iter(|| instant()));
    c.bench_function("system_time", |b| b.iter(|| system_time()));
}

criterion_group!(benches, get_time_benchmark);
criterion_main!(benches);

