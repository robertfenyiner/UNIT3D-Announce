use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::Arc;

use anyhow::{Context, Result};
use axum::extract::State;
use sqlx::MySqlPool;
use tracing::info;

use crate::tracker::Tracker;

/// Stores ASN -> organization name mapping for blocked ASNs
pub type Map = HashMap<u32, String>;

pub async fn from_db(pool: &MySqlPool) -> Result<Map> {
    let records = sqlx::query!("SELECT asn, organization FROM blocked_asns")
        .fetch_all(pool)
        .await
        .context("Failed loading blocked ASNs.")?;

    let mut map = Map::new();

    for record in records {
        map.insert(record.asn, record.organization);
    }

    Ok(map)
}

/// API endpoint to reload blocked ASNs from database into tracker memory
pub async fn upsert(State(tracker): State<Arc<Tracker>>) {
    info!("Reloading blocked ASNs from database...");

    match from_db(&tracker.pool).await {
        Ok(fresh) => {
            let count = fresh.len();
            *tracker.blocked_asns.write() = fresh;
            info!("Successfully reloaded {} blocked ASNs.", count);
        }
        Err(e) => {
            tracing::error!("Failed to reload blocked ASNs: {}", e);
        }
    }
}

/// API endpoint to remove all blocked ASNs from tracker memory
pub async fn destroy(State(tracker): State<Arc<Tracker>>) {
    info!("Clearing all blocked ASNs from memory...");
    tracker.blocked_asns.write().clear();
    info!("All blocked ASNs cleared.");
}

/// Look up ASN from ip_asn_cache table for a given IP
static IP_ASN_CACHE: std::sync::OnceLock<parking_lot::Mutex<super::asn_cache::AsnCache>> =
    std::sync::OnceLock::new();
static IP_ASN_LOOKUPS: std::sync::OnceLock<[tokio::sync::Mutex<()>; 64]> =
    std::sync::OnceLock::new();

pub async fn get_asn_for_ip(pool: &MySqlPool, ip: IpAddr) -> Result<Option<(u32, String)>> {
    use std::{
        hash::{Hash, Hasher},
        time::Instant,
    };
    let cache =
        IP_ASN_CACHE.get_or_init(|| parking_lot::Mutex::new(super::asn_cache::AsnCache::new(8192)));
    if let Some(value) = cache.lock().get(ip, Instant::now()) {
        return Ok(value);
    }
    // Coalesce simultaneous misses; no synchronous lock is held across an await.
    let mut hash = std::collections::hash_map::DefaultHasher::new();
    ip.hash(&mut hash);
    let locks = IP_ASN_LOOKUPS.get_or_init(|| std::array::from_fn(|_| tokio::sync::Mutex::new(())));
    let _lookup = locks[(hash.finish() % 64) as usize].lock().await;
    if let Some(value) = cache.lock().get(ip, Instant::now()) {
        return Ok(value);
    }
    let value = get_asn_for_ip_uncached(pool, ip).await?;
    cache.lock().insert(ip, value.clone(), Instant::now());
    Ok(value)
}

async fn get_asn_for_ip_uncached(pool: &MySqlPool, ip: IpAddr) -> Result<Option<(u32, String)>> {
    let ip_bytes = match ip {
        IpAddr::V4(ipv4) => ipv4.to_ipv6_mapped().octets().to_vec(),
        IpAddr::V6(ipv6) => ipv6.octets().to_vec(),
    };

    let record = sqlx::query!(
        "SELECT asn, organization FROM ip_asn_cache WHERE ip_address = ? AND asn IS NOT NULL",
        ip_bytes
    )
    .fetch_optional(pool)
    .await?;

    Ok(record.and_then(|r| {
        r.asn.map(|asn| {
            (
                asn,
                r.organization.unwrap_or_else(|| String::from("Unknown")),
            )
        })
    }))
}
