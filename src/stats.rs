use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use axum::{
    Json,
    extract::{Request, State},
    middleware::Next,
    response::Response,
};

use crate::tracker::Tracker;

const REQUEST_DURATION_BUCKET_BOUNDS_MS: [u64; 7] = [25, 50, 100, 250, 500, 1000, 2500];
const QUEUE_NAMES: [&str; 6] = [
    "announces",
    "histories",
    "peers",
    "torrents",
    "users",
    "unregistered info hashes",
];

pub struct Stats {
    created_at: AtomicF64,
    last_request_at: AtomicF64,
    last_announce_response_at: AtomicF64,
    requests_per_1s: AtomicF64,
    requests_per_10s: AtomicF64,
    requests_per_60s: AtomicF64,
    requests_per_900s: AtomicF64,
    requests_per_7200s: AtomicF64,
    announce_responses_per_1s: AtomicF64,
    announce_responses_per_10s: AtomicF64,
    announce_responses_per_60s: AtomicF64,
    announce_responses_per_900s: AtomicF64,
    announce_responses_per_7200s: AtomicF64,
    request_completed_total: AtomicU64,
    request_duration_ms_buckets: [AtomicU64; 8],
    queue_metrics: [QueueMetrics; 6],
    peer_expiry_runs: AtomicU64,
    peer_expiry_last_ms: AtomicU64,
    peer_expiry_max_ms: AtomicU64,
}

#[derive(Default)]
struct QueueMetrics {
    successful_batches: AtomicU64,
    failed_batch_attempts: AtomicU64,
    successful_records: AtomicU64,
    failed_record_attempts: AtomicU64,
    last_batch_ms: AtomicU64,
    max_batch_ms: AtomicU64,
    total_batch_ms: AtomicU64,
}

#[derive(serde::Serialize)]
pub struct QueueMetricsSnapshot {
    successful_batches: u64,
    failed_batch_attempts: u64,
    successful_records: u64,
    failed_record_attempts: u64,
    last_batch_ms: u64,
    max_batch_ms: u64,
    total_batch_ms: u64,
}

impl QueueMetrics {
    fn snapshot(&self) -> QueueMetricsSnapshot {
        let load = |value: &AtomicU64| value.load(Ordering::Relaxed);
        QueueMetricsSnapshot {
            successful_batches: load(&self.successful_batches),
            failed_batch_attempts: load(&self.failed_batch_attempts),
            successful_records: load(&self.successful_records),
            failed_record_attempts: load(&self.failed_record_attempts),
            last_batch_ms: load(&self.last_batch_ms),
            max_batch_ms: load(&self.max_batch_ms),
            total_batch_ms: load(&self.total_batch_ms),
        }
    }
}

impl Default for Stats {
    /// Initializes the stats struct.
    fn default() -> Stats {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time must go forwards.")
            .as_secs_f64();

        Stats {
            created_at: AtomicF64::new(now),
            last_request_at: AtomicF64::new(now),
            last_announce_response_at: AtomicF64::new(now),
            requests_per_1s: Default::default(),
            requests_per_10s: Default::default(),
            requests_per_60s: Default::default(),
            requests_per_900s: Default::default(),
            requests_per_7200s: Default::default(),
            announce_responses_per_1s: Default::default(),
            announce_responses_per_10s: Default::default(),
            announce_responses_per_60s: Default::default(),
            announce_responses_per_900s: Default::default(),
            announce_responses_per_7200s: Default::default(),
            request_completed_total: AtomicU64::new(0),
            request_duration_ms_buckets: std::array::from_fn(|_| AtomicU64::new(0)),
            queue_metrics: std::array::from_fn(|_| QueueMetrics::default()),
            peer_expiry_runs: AtomicU64::new(0),
            peer_expiry_last_ms: AtomicU64::new(0),
            peer_expiry_max_ms: AtomicU64::new(0),
        }
    }
}

impl Stats {
    fn record_request_duration(&self, elapsed_ms: u64) {
        let bucket = REQUEST_DURATION_BUCKET_BOUNDS_MS
            .iter()
            .position(|bound| elapsed_ms <= *bound)
            .unwrap_or(REQUEST_DURATION_BUCKET_BOUNDS_MS.len());
        self.request_duration_ms_buckets[bucket].fetch_add(1, Ordering::Relaxed);
        self.request_completed_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_queue_batch(&self, name: &str, records: usize, elapsed_ms: u64, success: bool) {
        if records == 0 {
            return;
        }
        let Some(index) = QUEUE_NAMES.iter().position(|candidate| *candidate == name) else {
            return;
        };
        let metrics = &self.queue_metrics[index];
        if success {
            metrics.successful_batches.fetch_add(1, Ordering::Relaxed);
            metrics
                .successful_records
                .fetch_add(records as u64, Ordering::Relaxed);
        } else {
            metrics
                .failed_batch_attempts
                .fetch_add(1, Ordering::Relaxed);
            metrics
                .failed_record_attempts
                .fetch_add(records as u64, Ordering::Relaxed);
        }
        metrics.last_batch_ms.store(elapsed_ms, Ordering::Relaxed);
        metrics
            .max_batch_ms
            .fetch_max(elapsed_ms, Ordering::Relaxed);
        metrics
            .total_batch_ms
            .fetch_add(elapsed_ms, Ordering::Relaxed);
    }

    pub fn record_peer_expiry(&self, elapsed_ms: u64) {
        self.peer_expiry_last_ms
            .store(elapsed_ms, Ordering::Relaxed);
        self.peer_expiry_max_ms
            .fetch_max(elapsed_ms, Ordering::Relaxed);
        self.peer_expiry_runs.fetch_add(1, Ordering::Relaxed);
    }

    pub fn increment_request(&self) {
        let now = SystemTime::now().duration_since(UNIX_EPOCH);

        if let Ok(now) = now {
            let elapsed = now.as_secs_f64() - self.last_request_at.load(Ordering::Relaxed);
            self.requests_per_1s
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |rate| {
                    rate * f64::exp(-1f64 * elapsed / 1f64) + 1f64
                });
            self.requests_per_10s
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |rate| {
                    rate * f64::exp(-1f64 * elapsed / 10f64) + 1f64
                });
            self.requests_per_60s
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |rate| {
                    rate * f64::exp(-1f64 * elapsed / 60f64) + 1f64
                });
            self.requests_per_900s
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |rate| {
                    rate * f64::exp(-1f64 * elapsed / 900f64) + 1f64
                });
            self.requests_per_7200s
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |rate| {
                    rate * f64::exp(-1f64 * elapsed / 7200f64) + 1f64
                });
            self.last_request_at
                .store(now.as_secs_f64(), Ordering::Relaxed);
        }
    }

    pub fn increment_announce_response(&self) {
        let now = SystemTime::now().duration_since(UNIX_EPOCH);

        if let Ok(now) = now {
            let elapsed =
                now.as_secs_f64() - self.last_announce_response_at.load(Ordering::Relaxed);
            self.announce_responses_per_1s.fetch_update(
                Ordering::Relaxed,
                Ordering::Relaxed,
                |rate| rate * f64::exp(-1f64 * elapsed / 1f64) + 1f64,
            );
            self.announce_responses_per_10s.fetch_update(
                Ordering::Relaxed,
                Ordering::Relaxed,
                |rate| rate * f64::exp(-1f64 * elapsed / 10f64) + 1f64,
            );
            self.announce_responses_per_60s.fetch_update(
                Ordering::Relaxed,
                Ordering::Relaxed,
                |rate| rate * f64::exp(-1f64 * elapsed / 60f64) + 1f64,
            );
            self.announce_responses_per_900s.fetch_update(
                Ordering::Relaxed,
                Ordering::Relaxed,
                |rate| rate * f64::exp(-1f64 * elapsed / 900f64) + 1f64,
            );
            self.announce_responses_per_7200s.fetch_update(
                Ordering::Relaxed,
                Ordering::Relaxed,
                |rate| rate * f64::exp(-1f64 * elapsed / 7200f64) + 1f64,
            );
            self.last_announce_response_at
                .store(now.as_secs_f64(), Ordering::Relaxed);
        }
    }
}

pub async fn show(State(tracker): State<Arc<Tracker>>) -> Json<APIGetStats> {
    Json(APIGetStats {
        created_at: tracker.stats.created_at.load(Ordering::Relaxed),
        last_request_at: tracker.stats.last_request_at.load(Ordering::Relaxed),
        last_announce_response_at: tracker
            .stats
            .last_announce_response_at
            .load(Ordering::Relaxed),
        requests_per_1s: tracker.stats.requests_per_1s.load(Ordering::Relaxed),
        requests_per_10s: tracker.stats.requests_per_10s.load(Ordering::Relaxed) / 10f64,
        requests_per_60s: tracker.stats.requests_per_60s.load(Ordering::Relaxed) / 60f64,
        requests_per_900s: tracker.stats.requests_per_900s.load(Ordering::Relaxed) / 900f64,
        requests_per_7200s: tracker.stats.requests_per_7200s.load(Ordering::Relaxed) / 7200f64,
        announce_responses_per_1s: tracker
            .stats
            .announce_responses_per_1s
            .load(Ordering::Relaxed),
        announce_responses_per_10s: tracker
            .stats
            .announce_responses_per_10s
            .load(Ordering::Relaxed)
            / 10f64,
        announce_responses_per_60s: tracker
            .stats
            .announce_responses_per_60s
            .load(Ordering::Relaxed)
            / 60f64,
        announce_responses_per_900s: tracker
            .stats
            .announce_responses_per_900s
            .load(Ordering::Relaxed)
            / 900f64,
        announce_responses_per_7200s: tracker
            .stats
            .announce_responses_per_7200s
            .load(Ordering::Relaxed)
            / 7200f64,
        request_completed_total: tracker
            .stats
            .request_completed_total
            .load(Ordering::Relaxed),
        request_duration_ms_bucket_bounds: REQUEST_DURATION_BUCKET_BOUNDS_MS,
        request_duration_ms_buckets: std::array::from_fn(|index| {
            tracker.stats.request_duration_ms_buckets[index].load(Ordering::Relaxed)
        }),
        queue_metrics: QueueMetricsOutput {
            announces: tracker.stats.queue_metrics[0].snapshot(),
            histories: tracker.stats.queue_metrics[1].snapshot(),
            peers: tracker.stats.queue_metrics[2].snapshot(),
            torrents: tracker.stats.queue_metrics[3].snapshot(),
            users: tracker.stats.queue_metrics[4].snapshot(),
            unregistered_info_hashes: tracker.stats.queue_metrics[5].snapshot(),
        },
        peer_expiry_runs: tracker.stats.peer_expiry_runs.load(Ordering::Relaxed),
        peer_expiry_last_ms: tracker.stats.peer_expiry_last_ms.load(Ordering::Relaxed),
        peer_expiry_max_ms: tracker.stats.peer_expiry_max_ms.load(Ordering::Relaxed),
    })
}

#[derive(serde::Serialize)]
pub struct APIGetStats {
    created_at: f64,
    last_request_at: f64,
    last_announce_response_at: f64,
    requests_per_1s: f64,
    requests_per_10s: f64,
    requests_per_60s: f64,
    requests_per_900s: f64,
    requests_per_7200s: f64,
    announce_responses_per_1s: f64,
    announce_responses_per_10s: f64,
    announce_responses_per_60s: f64,
    announce_responses_per_900s: f64,
    announce_responses_per_7200s: f64,
    request_completed_total: u64,
    request_duration_ms_bucket_bounds: [u64; 7],
    request_duration_ms_buckets: [u64; 8],
    queue_metrics: QueueMetricsOutput,
    peer_expiry_runs: u64,
    peer_expiry_last_ms: u64,
    peer_expiry_max_ms: u64,
}

#[derive(serde::Serialize)]
struct QueueMetricsOutput {
    announces: QueueMetricsSnapshot,
    histories: QueueMetricsSnapshot,
    peers: QueueMetricsSnapshot,
    torrents: QueueMetricsSnapshot,
    users: QueueMetricsSnapshot,
    unregistered_info_hashes: QueueMetricsSnapshot,
}

pub async fn record_request(
    State(state): State<Arc<Tracker>>,
    request: Request,
    next: Next,
) -> Response {
    state.stats.increment_request();
    let start = Instant::now();
    let response = next.run(request).await;
    state
        .stats
        .record_request_duration(start.elapsed().as_millis() as u64);
    response
}

struct AtomicF64 {
    content: AtomicU64,
}

impl Default for AtomicF64 {
    fn default() -> Self {
        Self {
            content: AtomicU64::new(0f64.to_bits()),
        }
    }
}

impl AtomicF64 {
    fn new(value: f64) -> Self {
        Self {
            content: AtomicU64::new(value.to_bits()),
        }
    }

    fn load(&self, order: Ordering) -> f64 {
        f64::from_bits(self.content.load(order))
    }

    fn store(&self, value: f64, order: Ordering) {
        self.content.store(value.to_bits(), order)
    }

    fn fetch_update<F>(&self, set_order: Ordering, fetch_order: Ordering, mut f: F)
    where
        F: FnMut(f64) -> f64,
    {
        let _ = self.content.fetch_update(set_order, fetch_order, |num| {
            Some(f(f64::from_bits(num)).to_bits())
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metrics_count_completed_requests_and_retry_attempts() {
        let stats = Stats::default();
        stats.record_request_duration(25);
        stats.record_request_duration(26);
        stats.record_request_duration(3000);
        assert_eq!(stats.request_completed_total.load(Ordering::Relaxed), 3);
        let counts: Vec<u64> = stats
            .request_duration_ms_buckets
            .iter()
            .map(|bucket| bucket.load(Ordering::Relaxed))
            .collect();
        assert_eq!(counts, [1, 1, 0, 0, 0, 0, 0, 1]);

        stats.record_queue_batch("histories", 100, 250, false);
        stats.record_queue_batch("histories", 100, 300, true);
        let history = stats.queue_metrics[1].snapshot();
        assert_eq!(history.failed_batch_attempts, 1);
        assert_eq!(history.failed_record_attempts, 100);
        assert_eq!(history.successful_batches, 1);
        assert_eq!(history.successful_records, 100);
        assert_eq!(history.total_batch_ms, 550);
        assert_eq!(history.last_batch_ms, 300);
        stats.record_peer_expiry(42);
        assert_eq!(stats.peer_expiry_runs.load(Ordering::Relaxed), 1);
        assert_eq!(stats.peer_expiry_last_ms.load(Ordering::Relaxed), 42);
    }
}
