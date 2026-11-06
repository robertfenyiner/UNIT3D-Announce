use std::{cmp::min, collections::VecDeque, hash::Hash, slice::Iter, sync::Arc, vec::IntoIter};

pub mod announce_update;
pub mod history_update;
pub mod peer_update;
pub mod torrent_update;
pub mod unregistered_info_hash_update;
pub mod user_update;

use crate::tracker::Tracker;
use chrono::{Duration, Utc};
use futures_util::future::join_all;
use parking_lot::Mutex;
use ringmap::RingMap;
use tokio::{join, time::Instant};
use torrent_update::{Index, TorrentUpdate};
use tracing::info;

pub async fn handle(tracker: &Arc<Tracker>, mut shutdown: tokio::sync::watch::Receiver<bool>) {
    let mut interval = tokio::time::interval(std::time::Duration::from_millis(1));
    let mut counter = 0_u64;

    loop {
        tokio::select! {
            _ = interval.tick() => {
                counter += 1;

                if counter % tracker.config.read().flush_interval_milliseconds == 0 {
                    flush(tracker).await;
                }

                if counter % (tracker.config.read().peer_expiry_interval * 1000) == 0 {
                    reap(tracker).await;
                }
            }
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break;
                }
            }
        }
    }
}

/// Includes all six queues persisted by flush(). Call only after the scheduler stopped.
pub fn has_pending_updates(tracker: &Arc<Tracker>) -> bool {
    !tracker.announce_updates.lock().is_empty()
        || tracker.history_updates.lock().is_not_empty()
        || tracker.peer_updates.lock().is_not_empty()
        || tracker.torrent_updates.lock().is_not_empty()
        || tracker.user_updates.lock().is_not_empty()
        || tracker.unregistered_info_hash_updates.lock().is_not_empty()
}

/// Send queued updates to mysql database
pub async fn flush(tracker: &Arc<Tracker>) {
    // Persist the parent torrent rows before peers and histories acquire
    // foreign-key locks on those same rows. This removes an internal
    // parent/child lock-order inversion observed in InnoDB deadlocks.
    tracker.torrent_updates.flush(tracker, "torrents").await;

    join!(
        flush_announce_updates(tracker),
        tracker.history_updates.flush(tracker, "histories"),
        tracker.peer_updates.flush(tracker, "peers"),
        tracker.user_updates.flush(tracker, "users"),
        tracker
            .unregistered_info_hash_updates
            .flush(tracker, "unregistered info hashes"),
    );
}

/// Send announce updates to mysql database
async fn flush_announce_updates(tracker: &Arc<Tracker>) {
    let announce_update_batch = tracker.announce_updates.lock().take_batch();
    let start = Instant::now();
    let len = announce_update_batch.len();
    let result = announce_update_batch.flush_to_db(tracker).await;
    let elapsed = start.elapsed().as_millis();

    tracker
        .stats
        .record_queue_batch("announces", len, elapsed as u64, result.is_ok());

    match result {
        Ok(_) => {
            info!("Upserted {len} announces in {elapsed} ms.");
        }
        Err(e) => {
            info!("Failed to update {len} announces after {elapsed} ms: {e}");
            tracker
                .announce_updates
                .lock()
                .upsert_batch(announce_update_batch);
        }
    }
}

/// Remove peers that have not announced for some time
pub async fn reap(tracker: &Arc<Tracker>) {
    let started = Instant::now();
    let ttl = Duration::seconds(tracker.config.read().active_peer_ttl.try_into().unwrap());
    let active_cutoff = Utc::now().checked_sub_signed(ttl).unwrap();
    let ttl = Duration::seconds(tracker.config.read().inactive_peer_ttl.try_into().unwrap());
    let inactive_cutoff = Utc::now().checked_sub_signed(ttl).unwrap();

    for (_index, torrent) in tracker.torrents.lock().iter_mut() {
        let mut seeder_delta: i32 = 0;
        let mut leecher_delta: i32 = 0;

        // If a peer is marked as inactive and it has not announced for
        // more than inactive_peer_ttl, then it is permanently deleted.
        torrent
            .peers
            .retain(|_index, peer| inactive_cutoff <= peer.updated_at || peer.is_active);

        for (index, peer) in torrent.peers.iter_mut() {
            // Peers get marked as inactive if not announced for more than
            // active_peer_ttl seconds. User peer count and torrent peer
            // count are updated to reflect.
            if peer.updated_at < active_cutoff && peer.is_active {
                peer.is_active = false;

                if peer.is_visible {
                    tracker
                        .users
                        .write()
                        .entry(index.user_id)
                        .and_modify(|user| {
                            if peer.is_seeder {
                                user.num_seeding = user.num_seeding.saturating_sub(1);
                            } else {
                                user.num_leeching = user.num_leeching.saturating_sub(1);
                            }
                        });
                    match peer.is_seeder {
                        true => seeder_delta -= 1,
                        false => leecher_delta -= 1,
                    }
                }
            }
        }

        // Update peer count of torrents and users
        if seeder_delta != 0 || leecher_delta != 0 {
            torrent.seeders = torrent.seeders.saturating_add_signed(seeder_delta);
            torrent.leechers = torrent.leechers.saturating_add_signed(leecher_delta);

            tracker.torrent_updates.lock().upsert(
                Index {
                    torrent_id: torrent.id,
                },
                TorrentUpdate {
                    seeder_delta,
                    leecher_delta,
                    times_completed_delta: 0,
                    balance_delta: 0,
                },
            );
        }
    }
    tracker
        .stats
        .record_peer_expiry(started.elapsed().as_millis() as u64);
}

pub struct Queue<K, V> {
    records: RingMap<K, V>,
    config: QueueConfig,
}

pub struct QueueConfig {
    pub max_bindings_per_flush: usize,
    pub bindings_per_record: usize,
    pub extra_bindings_per_flush: usize,
}

impl QueueConfig {
    fn max_batch_size(&mut self, tracker: &Arc<Tracker>) -> usize {
        let max_bindings = (self.max_bindings_per_flush - self.extra_bindings_per_flush)
            / self.bindings_per_record;

        if let Some(max_records) = tracker.config.read().max_records_per_batch {
            return max_bindings.min(max_records);
        }

        max_bindings
    }
}

impl<K, V> Queue<K, V>
where
    K: Hash + Eq + Ord,
    V: Clone + Mergeable,
{
    /// Initialize a new queue
    pub fn new(config: QueueConfig) -> Queue<K, V> {
        Self {
            records: RingMap::new(),
            config,
        }
    }

    /// Upsert a single update into the queue
    pub fn upsert(&mut self, key: K, value: V) {
        self.records
            .entry(key)
            .and_modify(|update| update.merge(&value))
            .or_insert(value);
    }

    /// Take a portion of the updates from the start of the queue with a max
    /// size defined by the buffer config
    fn take_batches(&mut self, tracker: &Arc<Tracker>) -> VecDeque<Batch<K, V>> {
        let max_batches = tracker.config.read().max_batches_per_flush;
        let max_batch_size = self.config.max_batch_size(tracker);

        let mut records = self
            .records
            .drain(0..min(self.records.len(), max_batches * max_batch_size))
            .collect::<Vec<(K, V)>>();

        records.sort_unstable_by(move |a, b| K::cmp(&a.0, &b.0));

        let mut batches = VecDeque::new();

        while records.len() > 0 {
            let batch = records.split_off(records.len() - min(records.len(), max_batch_size));
            batches.push_front(Batch(batch));
        }

        batches
    }

    /// Bulk upsert a batch into the end of the queue
    fn upsert_batch(&mut self, batch: Batch<K, V>) {
        batch.into_iter().for_each(|(k, v)| self.upsert(k, v));
    }

    pub fn is_not_empty(&self) -> bool {
        !self.records.is_empty()
    }
}

pub trait Mergeable {
    /// Merge an existing record with a new record
    fn merge(&mut self, new: &Self);
}

trait MutexQueueExt {
    async fn flush<'a>(&self, tracker: &Arc<Tracker>, record_type: &'a str);
}

impl<K, V> MutexQueueExt for Mutex<Queue<K, V>>
where
    K: Hash + Eq + Ord,
    V: Clone + Mergeable,
    Batch<K, V>: Flushable<V>,
{
    async fn flush<'a>(&self, tracker: &Arc<Tracker>, record_type: &'a str) {
        let batches = self.lock().take_batches(tracker);

        if batches.is_empty() {
            info!("Upserted 0 {record_type} in 0 ms.");

            return;
        }

        let tasks = batches
            .into_iter()
            .map(|batch| async move {
                let start = Instant::now();
                let len = batch.len();
                let result = batch.flush_to_db(tracker).await;
                let elapsed = start.elapsed().as_millis();

                (len, elapsed, result, batch)
            })
            .collect::<Vec<_>>();

        let results = join_all(tasks).await;

        for (len, elapsed, result, batch) in results {
            tracker
                .stats
                .record_queue_batch(record_type, len, elapsed as u64, result.is_ok());
            match result {
                Ok(_) => {
                    info!("Upserted {len} {record_type} in {elapsed} ms.");
                }
                Err(e) => {
                    info!("Failed to update {len} {record_type} after {elapsed} ms: {e}",);
                    self.lock().upsert_batch(batch);
                }
            }
        }
    }
}

pub struct Batch<K, V>(Vec<(K, V)>);

impl<'a, K, V> Batch<K, V> {
    fn is_empty(&self) -> bool {
        self.0.len() == 0
    }

    fn iter(&'a self) -> Iter<'a, (K, V)> {
        self.0.iter()
    }

    fn into_iter(self) -> IntoIter<(K, V)> {
        self.0.into_iter()
    }

    fn len(&self) -> usize {
        self.0.len()
    }
}

pub trait Flushable<T> {
    /// Flushes batch of updates to MySQL database
    ///
    /// **Warning**: this function does not make sure that the query isn't too long
    /// or doesn't use too many bindings
    async fn flush_to_db(&self, tracker: &Arc<Tracker>) -> Result<u64, sqlx::Error>;
}
