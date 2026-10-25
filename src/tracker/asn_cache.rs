use std::{
    collections::{HashMap, VecDeque},
    net::IpAddr,
    time::{Duration, Instant},
};

pub type Asn = (u32, String);

/// Bounded FIFO cache. Errors are never passed to insert. Block rules stay live.
pub struct AsnCache {
    entries: HashMap<IpAddr, (Instant, Option<Asn>)>,
    order: VecDeque<IpAddr>,
    capacity: usize,
}

impl AsnCache {
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > 0);
        Self {
            entries: HashMap::new(),
            order: VecDeque::new(),
            capacity,
        }
    }

    /// Outer None is a miss; Some(None) is a cached unknown IP.
    pub fn get(&self, ip: IpAddr, now: Instant) -> Option<Option<Asn>> {
        self.entries
            .get(&ip)
            .filter(|(expires, _)| *expires > now)
            .map(|(_, asn)| asn.clone())
    }

    pub fn insert(&mut self, ip: IpAddr, value: Option<Asn>, now: Instant) {
        if !self.entries.contains_key(&ip) {
            if self.entries.len() >= self.capacity {
                if let Some(oldest) = self.order.pop_front() {
                    self.entries.remove(&oldest);
                }
            }
            self.order.push_back(ip);
        }
        let ttl = if value.is_some() { 60 } else { 5 };
        self.entries
            .insert(ip, (now + Duration::from_secs(ttl), value));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn ip(n: u8) -> IpAddr {
        IpAddr::from([192, 0, 2, n])
    }
    fn value(n: u32) -> Option<Asn> {
        Some((n, "test".into()))
    }
    #[test]
    fn positive_expires_at_sixty_seconds() {
        let t = Instant::now();
        let mut c = AsnCache::new(2);
        c.insert(ip(1), value(1), t);
        assert_eq!(c.get(ip(1), t + Duration::from_secs(59)), Some(value(1)));
        assert_eq!(c.get(ip(1), t + Duration::from_secs(60)), None);
    }
    #[test]
    fn negative_expires_at_five_seconds() {
        let t = Instant::now();
        let mut c = AsnCache::new(2);
        c.insert(ip(1), None, t);
        assert_eq!(c.get(ip(1), t), Some(None));
        assert_eq!(c.get(ip(1), t + Duration::from_secs(5)), None);
    }
    #[test]
    fn bounded_fifo_and_replacement() {
        let t = Instant::now();
        let mut c = AsnCache::new(2);
        c.insert(ip(1), value(1), t);
        c.insert(ip(2), value(2), t);
        c.insert(ip(2), value(9), t);
        assert_eq!(c.entries.len(), 2);
        assert_eq!(c.order.len(), 2);
        c.insert(ip(3), value(3), t);
        assert_eq!(c.get(ip(1), t), None);
        assert_eq!(c.get(ip(2), t), Some(value(9)));
        assert_eq!(c.entries.len(), 2);
    }
    #[test]
    fn expired_entry_can_refresh_without_growth() {
        let t = Instant::now();
        let mut c = AsnCache::new(1);
        c.insert(ip(1), None, t);
        c.insert(ip(1), value(7), t + Duration::from_secs(6));
        assert_eq!(c.get(ip(1), t + Duration::from_secs(7)), Some(value(7)));
        assert_eq!(c.order.len(), 1);
    }
    #[test]
    fn ipv6_is_preserved() {
        let t = Instant::now();
        let ip = "2001:db8::1".parse().unwrap();
        let mut c = AsnCache::new(1);
        c.insert(ip, value(4), t);
        assert_eq!(c.get(ip, t), Some(value(4)));
    }
}
