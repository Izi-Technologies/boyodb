// Network Types - IP addresses, CIDR, MAC addresses for BoyoDB
//
// Provides PostgreSQL-style network type support:
// - IPv4 and IPv6 addresses (inet)
// - CIDR notation for network blocks
// - MAC addresses (macaddr, macaddr8)
// - Network operators (contains, contained_by, overlap)
// - Index support for network queries

use std::cmp::Ordering;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::str::FromStr;

// ============================================================================
// IP Address Types
// ============================================================================

/// An IPv4 or IPv6 address with optional netmask (inet type)
#[derive(Debug, Clone, Copy)]
pub struct InetAddr {
    /// The IP address
    pub addr: IpAddr,
    /// Network prefix length (netmask)
    pub prefix_len: u8,
}

impl InetAddr {
    /// Create a new inet address
    pub fn new(addr: IpAddr, prefix_len: u8) -> Result<Self, NetworkError> {
        let max_prefix = if addr.is_ipv4() { 32 } else { 128 };
        if prefix_len > max_prefix {
            return Err(NetworkError::InvalidPrefixLength(prefix_len, max_prefix));
        }
        Ok(Self { addr, prefix_len })
    }

    /// Create from IPv4 address with /32 prefix
    pub fn from_ipv4(addr: Ipv4Addr) -> Self {
        Self {
            addr: IpAddr::V4(addr),
            prefix_len: 32,
        }
    }

    /// Create from IPv6 address with /128 prefix
    pub fn from_ipv6(addr: Ipv6Addr) -> Self {
        Self {
            addr: IpAddr::V6(addr),
            prefix_len: 128,
        }
    }

    /// Check if this is IPv4
    pub fn is_ipv4(&self) -> bool {
        self.addr.is_ipv4()
    }

    /// Check if this is IPv6
    pub fn is_ipv6(&self) -> bool {
        self.addr.is_ipv6()
    }

    /// Get the address family (4 or 6)
    pub fn family(&self) -> u8 {
        if self.is_ipv4() {
            4
        } else {
            6
        }
    }

    /// Get the netmask as an IP address
    pub fn netmask(&self) -> IpAddr {
        if self.is_ipv4() {
            let mask = if self.prefix_len == 0 {
                0u32
            } else {
                u32::MAX << (32 - self.prefix_len)
            };
            IpAddr::V4(Ipv4Addr::from(mask))
        } else {
            let mask = if self.prefix_len == 0 {
                0u128
            } else {
                u128::MAX << (128 - self.prefix_len)
            };
            IpAddr::V6(Ipv6Addr::from(mask))
        }
    }

    /// Get the network address (host bits zeroed)
    pub fn network(&self) -> IpAddr {
        match self.addr {
            IpAddr::V4(addr) => {
                let bits = u32::from(addr);
                let mask = if self.prefix_len == 0 {
                    0u32
                } else {
                    u32::MAX << (32 - self.prefix_len)
                };
                IpAddr::V4(Ipv4Addr::from(bits & mask))
            }
            IpAddr::V6(addr) => {
                let bits = u128::from(addr);
                let mask = if self.prefix_len == 0 {
                    0u128
                } else {
                    u128::MAX << (128 - self.prefix_len)
                };
                IpAddr::V6(Ipv6Addr::from(bits & mask))
            }
        }
    }

    /// Get the broadcast address
    pub fn broadcast(&self) -> IpAddr {
        match self.addr {
            IpAddr::V4(addr) => {
                let bits = u32::from(addr);
                let mask = if self.prefix_len == 32 {
                    0u32
                } else {
                    u32::MAX >> self.prefix_len
                };
                IpAddr::V4(Ipv4Addr::from(bits | mask))
            }
            IpAddr::V6(addr) => {
                let bits = u128::from(addr);
                let mask = if self.prefix_len == 128 {
                    0u128
                } else {
                    u128::MAX >> self.prefix_len
                };
                IpAddr::V6(Ipv6Addr::from(bits | mask))
            }
        }
    }

    /// Get the host part (network bits zeroed)
    pub fn hostmask(&self) -> IpAddr {
        match self.addr {
            IpAddr::V4(addr) => {
                let bits = u32::from(addr);
                let mask = if self.prefix_len == 32 {
                    0u32
                } else {
                    u32::MAX >> self.prefix_len
                };
                IpAddr::V4(Ipv4Addr::from(bits & mask))
            }
            IpAddr::V6(addr) => {
                let bits = u128::from(addr);
                let mask = if self.prefix_len == 128 {
                    0u128
                } else {
                    u128::MAX >> self.prefix_len
                };
                IpAddr::V6(Ipv6Addr::from(bits & mask))
            }
        }
    }

    /// Check if this address/network contains another
    pub fn contains(&self, other: &InetAddr) -> bool {
        if self.is_ipv4() != other.is_ipv4() {
            return false;
        }

        if self.prefix_len > other.prefix_len {
            return false;
        }

        // Compare network portions
        match (&self.addr, &other.addr) {
            (IpAddr::V4(a), IpAddr::V4(b)) => {
                let mask = if self.prefix_len == 0 {
                    0u32
                } else {
                    u32::MAX << (32 - self.prefix_len)
                };
                (u32::from(*a) & mask) == (u32::from(*b) & mask)
            }
            (IpAddr::V6(a), IpAddr::V6(b)) => {
                let mask = if self.prefix_len == 0 {
                    0u128
                } else {
                    u128::MAX << (128 - self.prefix_len)
                };
                (u128::from(*a) & mask) == (u128::from(*b) & mask)
            }
            _ => false,
        }
    }

    /// Check if this address/network is contained by another
    pub fn contained_by(&self, other: &InetAddr) -> bool {
        other.contains(self)
    }

    /// Check if networks overlap
    pub fn overlaps(&self, other: &InetAddr) -> bool {
        self.contains(other) || other.contains(self)
    }

    /// Check if this is the same as another (strict equality)
    pub fn same_as(&self, other: &InetAddr) -> bool {
        self.addr == other.addr && self.prefix_len == other.prefix_len
    }

    /// Abbreviated display (like PostgreSQL)
    pub fn abbrev(&self) -> String {
        let max_prefix = if self.is_ipv4() { 32 } else { 128 };
        if self.prefix_len == max_prefix {
            format!("{}", self.addr)
        } else {
            format!("{}/{}", self.addr, self.prefix_len)
        }
    }

    /// Set the netmask length
    pub fn set_masklen(&self, len: u8) -> Result<Self, NetworkError> {
        InetAddr::new(self.addr, len)
    }

    /// Compare two addresses
    pub fn compare(&self, other: &InetAddr) -> Ordering {
        // First compare by family
        match self.family().cmp(&other.family()) {
            Ordering::Equal => {}
            ord => return ord,
        }

        // Then compare addresses byte by byte
        match (&self.addr, &other.addr) {
            (IpAddr::V4(a), IpAddr::V4(b)) => match u32::from(*a).cmp(&u32::from(*b)) {
                Ordering::Equal => self.prefix_len.cmp(&other.prefix_len),
                ord => ord,
            },
            (IpAddr::V6(a), IpAddr::V6(b)) => match u128::from(*a).cmp(&u128::from(*b)) {
                Ordering::Equal => self.prefix_len.cmp(&other.prefix_len),
                ord => ord,
            },
            _ => Ordering::Equal,
        }
    }
}

impl PartialEq for InetAddr {
    fn eq(&self, other: &Self) -> bool {
        self.same_as(other)
    }
}

impl Eq for InetAddr {}

impl PartialOrd for InetAddr {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for InetAddr {
    fn cmp(&self, other: &Self) -> Ordering {
        self.compare(other)
    }
}

impl Hash for InetAddr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self.addr {
            IpAddr::V4(addr) => {
                0u8.hash(state);
                addr.octets().hash(state);
            }
            IpAddr::V6(addr) => {
                1u8.hash(state);
                addr.octets().hash(state);
            }
        }
        self.prefix_len.hash(state);
    }
}

impl fmt::Display for InetAddr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.addr, self.prefix_len)
    }
}

impl FromStr for InetAddr {
    type Err = NetworkError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts: Vec<&str> = s.split('/').collect();

        let addr: IpAddr = parts[0]
            .parse()
            .map_err(|_| NetworkError::InvalidAddress(s.to_string()))?;

        let prefix_len = if parts.len() > 1 {
            parts[1]
                .parse()
                .map_err(|_| NetworkError::InvalidPrefixLength(0, 0))?
        } else {
            if addr.is_ipv4() {
                32
            } else {
                128
            }
        };

        InetAddr::new(addr, prefix_len)
    }
}

// ============================================================================
// CIDR Type
// ============================================================================

/// A CIDR network block (like inet but network bits must be zero)
#[derive(Debug, Clone, Copy)]
pub struct Cidr {
    inner: InetAddr,
}

impl Cidr {
    /// Create a new CIDR block
    pub fn new(addr: IpAddr, prefix_len: u8) -> Result<Self, NetworkError> {
        let inet = InetAddr::new(addr, prefix_len)?;

        // Verify that host bits are zero
        if inet.network() != inet.addr {
            return Err(NetworkError::InvalidCidr(
                "non-zero host bits in network address".to_string(),
            ));
        }

        Ok(Self { inner: inet })
    }

    /// Get the network address
    pub fn network(&self) -> IpAddr {
        self.inner.network()
    }

    /// Get the prefix length
    pub fn prefix_len(&self) -> u8 {
        self.inner.prefix_len
    }

    /// Get the netmask
    pub fn netmask(&self) -> IpAddr {
        self.inner.netmask()
    }

    /// Get the broadcast address
    pub fn broadcast(&self) -> IpAddr {
        self.inner.broadcast()
    }

    /// Check if an address is in this network
    pub fn contains(&self, addr: &InetAddr) -> bool {
        self.inner.contains(addr)
    }

    /// Check if this network is contained by another
    pub fn contained_by(&self, other: &Cidr) -> bool {
        other.inner.contains(&self.inner)
    }

    /// Check if networks overlap
    pub fn overlaps(&self, other: &Cidr) -> bool {
        self.inner.overlaps(&other.inner)
    }

    /// Number of addresses in this network
    pub fn num_addresses(&self) -> u128 {
        if self.inner.is_ipv4() {
            1u128 << (32 - self.inner.prefix_len as u32)
        } else {
            1u128 << (128 - self.inner.prefix_len as u32)
        }
    }
}

impl PartialEq for Cidr {
    fn eq(&self, other: &Self) -> bool {
        self.inner == other.inner
    }
}

impl Eq for Cidr {}

impl PartialOrd for Cidr {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Cidr {
    fn cmp(&self, other: &Self) -> Ordering {
        self.inner.cmp(&other.inner)
    }
}

impl Hash for Cidr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.inner.hash(state);
    }
}

impl fmt::Display for Cidr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.inner)
    }
}

impl FromStr for Cidr {
    type Err = NetworkError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let inet: InetAddr = s.parse()?;
        Cidr::new(inet.addr, inet.prefix_len)
    }
}

// ============================================================================
// MAC Address Types
// ============================================================================

/// A 6-byte MAC address (macaddr)
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MacAddr {
    octets: [u8; 6],
}

impl MacAddr {
    /// Create from bytes
    pub fn new(octets: [u8; 6]) -> Self {
        Self { octets }
    }

    /// Get the bytes
    pub fn octets(&self) -> &[u8; 6] {
        &self.octets
    }

    /// Check if this is a broadcast address
    pub fn is_broadcast(&self) -> bool {
        self.octets == [0xff, 0xff, 0xff, 0xff, 0xff, 0xff]
    }

    /// Check if this is a multicast address
    pub fn is_multicast(&self) -> bool {
        (self.octets[0] & 0x01) != 0
    }

    /// Check if this is a locally administered address
    pub fn is_local(&self) -> bool {
        (self.octets[0] & 0x02) != 0
    }

    /// Check if this is a universally administered address
    pub fn is_universal(&self) -> bool {
        !self.is_local()
    }

    /// Truncate to canonical form (set local bit to 0)
    pub fn trunc(&self) -> Self {
        let mut octets = self.octets;
        octets[0] &= 0xfc; // Clear both multicast and local bits
        Self { octets }
    }

    /// Set the 7th bit (to convert EUI-64 to modified EUI-64)
    pub fn set_7th_bit(&self) -> Self {
        let mut octets = self.octets;
        octets[0] ^= 0x02;
        Self { octets }
    }
}

impl fmt::Display for MacAddr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{:02x}:{:02x}:{:02x}:{:02x}:{:02x}:{:02x}",
            self.octets[0],
            self.octets[1],
            self.octets[2],
            self.octets[3],
            self.octets[4],
            self.octets[5]
        )
    }
}

impl FromStr for MacAddr {
    type Err = NetworkError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // Support multiple formats: aa:bb:cc:dd:ee:ff, aa-bb-cc-dd-ee-ff, aabbccddeeff
        let normalized = s.replace(['-', '.'], ":");
        let parts: Vec<&str> = if normalized.contains(':') {
            normalized.split(':').collect()
        } else {
            // No separator - should be 12 hex chars
            if s.len() != 12 {
                return Err(NetworkError::InvalidMacAddress(s.to_string()));
            }
            vec![
                &s[0..2],
                &s[2..4],
                &s[4..6],
                &s[6..8],
                &s[8..10],
                &s[10..12],
            ]
        };

        if parts.len() != 6 {
            return Err(NetworkError::InvalidMacAddress(s.to_string()));
        }

        let mut octets = [0u8; 6];
        for (i, part) in parts.iter().enumerate() {
            octets[i] = u8::from_str_radix(part, 16)
                .map_err(|_| NetworkError::InvalidMacAddress(s.to_string()))?;
        }

        Ok(Self { octets })
    }
}

/// An 8-byte MAC address (macaddr8, EUI-64)
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MacAddr8 {
    octets: [u8; 8],
}

impl MacAddr8 {
    /// Create from bytes
    pub fn new(octets: [u8; 8]) -> Self {
        Self { octets }
    }

    /// Get the bytes
    pub fn octets(&self) -> &[u8; 8] {
        &self.octets
    }

    /// Create from a 6-byte MAC address (insert ff:fe in the middle)
    pub fn from_mac(mac: &MacAddr) -> Self {
        let m = mac.octets();
        Self {
            octets: [m[0], m[1], m[2], 0xff, 0xfe, m[3], m[4], m[5]],
        }
    }

    /// Check if this is a broadcast address
    pub fn is_broadcast(&self) -> bool {
        self.octets == [0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff]
    }

    /// Check if this is a multicast address
    pub fn is_multicast(&self) -> bool {
        (self.octets[0] & 0x01) != 0
    }

    /// Check if this is a locally administered address
    pub fn is_local(&self) -> bool {
        (self.octets[0] & 0x02) != 0
    }

    /// Extract the OUI (first 3 bytes)
    pub fn oui(&self) -> [u8; 3] {
        [self.octets[0], self.octets[1], self.octets[2]]
    }
}

impl fmt::Display for MacAddr8 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{:02x}:{:02x}:{:02x}:{:02x}:{:02x}:{:02x}:{:02x}:{:02x}",
            self.octets[0],
            self.octets[1],
            self.octets[2],
            self.octets[3],
            self.octets[4],
            self.octets[5],
            self.octets[6],
            self.octets[7]
        )
    }
}

impl FromStr for MacAddr8 {
    type Err = NetworkError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let normalized = s.replace(['-', '.'], ":");
        let parts: Vec<&str> = if normalized.contains(':') {
            normalized.split(':').collect()
        } else {
            if s.len() != 16 {
                return Err(NetworkError::InvalidMacAddress(s.to_string()));
            }
            vec![
                &s[0..2],
                &s[2..4],
                &s[4..6],
                &s[6..8],
                &s[8..10],
                &s[10..12],
                &s[12..14],
                &s[14..16],
            ]
        };

        if parts.len() != 8 {
            return Err(NetworkError::InvalidMacAddress(s.to_string()));
        }

        let mut octets = [0u8; 8];
        for (i, part) in parts.iter().enumerate() {
            octets[i] = u8::from_str_radix(part, 16)
                .map_err(|_| NetworkError::InvalidMacAddress(s.to_string()))?;
        }

        Ok(Self { octets })
    }
}

// ============================================================================
// Network Functions
// ============================================================================

/// Bitwise AND of two addresses
pub fn inet_and(a: &InetAddr, b: &InetAddr) -> Option<IpAddr> {
    if a.is_ipv4() != b.is_ipv4() {
        return None;
    }

    match (&a.addr, &b.addr) {
        (IpAddr::V4(av), IpAddr::V4(bv)) => {
            Some(IpAddr::V4(Ipv4Addr::from(u32::from(*av) & u32::from(*bv))))
        }
        (IpAddr::V6(av), IpAddr::V6(bv)) => Some(IpAddr::V6(Ipv6Addr::from(
            u128::from(*av) & u128::from(*bv),
        ))),
        _ => None,
    }
}

/// Bitwise OR of two addresses
pub fn inet_or(a: &InetAddr, b: &InetAddr) -> Option<IpAddr> {
    if a.is_ipv4() != b.is_ipv4() {
        return None;
    }

    match (&a.addr, &b.addr) {
        (IpAddr::V4(av), IpAddr::V4(bv)) => {
            Some(IpAddr::V4(Ipv4Addr::from(u32::from(*av) | u32::from(*bv))))
        }
        (IpAddr::V6(av), IpAddr::V6(bv)) => Some(IpAddr::V6(Ipv6Addr::from(
            u128::from(*av) | u128::from(*bv),
        ))),
        _ => None,
    }
}

/// Bitwise NOT of an address
pub fn inet_not(a: &InetAddr) -> IpAddr {
    match a.addr {
        IpAddr::V4(addr) => IpAddr::V4(Ipv4Addr::from(!u32::from(addr))),
        IpAddr::V6(addr) => IpAddr::V6(Ipv6Addr::from(!u128::from(addr))),
    }
}

/// Add an offset to an address
pub fn inet_add(a: &InetAddr, offset: i64) -> Option<InetAddr> {
    match a.addr {
        IpAddr::V4(addr) => {
            let val = u32::from(addr);
            let new_val = if offset >= 0 {
                val.checked_add(offset as u32)?
            } else {
                val.checked_sub((-offset) as u32)?
            };
            Some(InetAddr {
                addr: IpAddr::V4(Ipv4Addr::from(new_val)),
                prefix_len: a.prefix_len,
            })
        }
        IpAddr::V6(addr) => {
            let val = u128::from(addr);
            let new_val = if offset >= 0 {
                val.checked_add(offset as u128)?
            } else {
                val.checked_sub((-offset) as u128)?
            };
            Some(InetAddr {
                addr: IpAddr::V6(Ipv6Addr::from(new_val)),
                prefix_len: a.prefix_len,
            })
        }
    }
}

/// Subtract two addresses to get the offset
pub fn inet_subtract(a: &InetAddr, b: &InetAddr) -> Option<i128> {
    if a.is_ipv4() != b.is_ipv4() {
        return None;
    }

    match (&a.addr, &b.addr) {
        (IpAddr::V4(av), IpAddr::V4(bv)) => Some(u32::from(*av) as i128 - u32::from(*bv) as i128),
        (IpAddr::V6(av), IpAddr::V6(bv)) => Some(u128::from(*av) as i128 - u128::from(*bv) as i128),
        _ => None,
    }
}

/// Check if an address is a loopback
pub fn is_loopback(addr: &InetAddr) -> bool {
    addr.addr.is_loopback()
}

/// Check if an address is link-local
pub fn is_link_local(addr: &InetAddr) -> bool {
    match addr.addr {
        IpAddr::V4(v4) => {
            // 169.254.0.0/16
            v4.octets()[0] == 169 && v4.octets()[1] == 254
        }
        IpAddr::V6(v6) => {
            // fe80::/10
            let octets = v6.octets();
            octets[0] == 0xfe && (octets[1] & 0xc0) == 0x80
        }
    }
}

/// Check if an address is private/site-local
pub fn is_private(addr: &InetAddr) -> bool {
    match addr.addr {
        IpAddr::V4(v4) => {
            let octets = v4.octets();
            // 10.0.0.0/8
            octets[0] == 10 ||
            // 172.16.0.0/12
            (octets[0] == 172 && (octets[1] & 0xf0) == 16) ||
            // 192.168.0.0/16
            (octets[0] == 192 && octets[1] == 168)
        }
        IpAddr::V6(v6) => {
            let octets = v6.octets();
            // fc00::/7 (ULA)
            (octets[0] & 0xfe) == 0xfc
        }
    }
}

// ============================================================================
// Errors
// ============================================================================

#[derive(Debug)]
pub enum NetworkError {
    InvalidAddress(String),
    InvalidPrefixLength(u8, u8),
    InvalidCidr(String),
    InvalidMacAddress(String),
    FamilyMismatch,
}

impl fmt::Display for NetworkError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidAddress(s) => write!(f, "Invalid IP address: {}", s),
            Self::InvalidPrefixLength(got, max) => {
                write!(f, "Invalid prefix length {} (max {})", got, max)
            }
            Self::InvalidCidr(s) => write!(f, "Invalid CIDR: {}", s),
            Self::InvalidMacAddress(s) => write!(f, "Invalid MAC address: {}", s),
            Self::FamilyMismatch => write!(f, "IP address family mismatch"),
        }
    }
}

impl std::error::Error for NetworkError {}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_inet_creation() {
        let addr: InetAddr = "192.168.1.1/24".parse().unwrap();
        assert!(addr.is_ipv4());
        assert_eq!(addr.prefix_len, 24);
        assert_eq!(addr.family(), 4);
    }

    #[test]
    fn test_inet_ipv6() {
        let addr: InetAddr = "2001:db8::1/64".parse().unwrap();
        assert!(addr.is_ipv6());
        assert_eq!(addr.prefix_len, 64);
        assert_eq!(addr.family(), 6);
    }

    #[test]
    fn test_inet_no_prefix() {
        let addr: InetAddr = "192.168.1.1".parse().unwrap();
        assert_eq!(addr.prefix_len, 32);

        let addr6: InetAddr = "2001:db8::1".parse().unwrap();
        assert_eq!(addr6.prefix_len, 128);
    }

    #[test]
    fn test_network_address() {
        let addr: InetAddr = "192.168.1.100/24".parse().unwrap();
        let network = addr.network();
        assert_eq!(network.to_string(), "192.168.1.0");
    }

    #[test]
    fn test_broadcast_address() {
        let addr: InetAddr = "192.168.1.100/24".parse().unwrap();
        let broadcast = addr.broadcast();
        assert_eq!(broadcast.to_string(), "192.168.1.255");
    }

    #[test]
    fn test_netmask() {
        let addr: InetAddr = "192.168.1.100/24".parse().unwrap();
        let netmask = addr.netmask();
        assert_eq!(netmask.to_string(), "255.255.255.0");

        let addr2: InetAddr = "10.0.0.1/8".parse().unwrap();
        let netmask2 = addr2.netmask();
        assert_eq!(netmask2.to_string(), "255.0.0.0");
    }

    #[test]
    fn test_contains() {
        let network: InetAddr = "192.168.1.0/24".parse().unwrap();
        let host1: InetAddr = "192.168.1.100/32".parse().unwrap();
        let host2: InetAddr = "192.168.2.100/32".parse().unwrap();

        assert!(network.contains(&host1));
        assert!(!network.contains(&host2));
    }

    #[test]
    fn test_contained_by() {
        let host: InetAddr = "192.168.1.100/32".parse().unwrap();
        let network: InetAddr = "192.168.1.0/24".parse().unwrap();

        assert!(host.contained_by(&network));
    }

    #[test]
    fn test_overlaps() {
        let net1: InetAddr = "192.168.1.0/24".parse().unwrap();
        let net2: InetAddr = "192.168.0.0/16".parse().unwrap();
        let net3: InetAddr = "10.0.0.0/8".parse().unwrap();

        assert!(net1.overlaps(&net2));
        assert!(net2.overlaps(&net1));
        assert!(!net1.overlaps(&net3));
    }

    #[test]
    fn test_abbrev() {
        let addr: InetAddr = "192.168.1.1/32".parse().unwrap();
        assert_eq!(addr.abbrev(), "192.168.1.1");

        let net: InetAddr = "192.168.1.0/24".parse().unwrap();
        assert_eq!(net.abbrev(), "192.168.1.0/24");
    }

    #[test]
    fn test_cidr() {
        let cidr: Cidr = "192.168.1.0/24".parse().unwrap();
        assert_eq!(cidr.prefix_len(), 24);
        assert_eq!(cidr.num_addresses(), 256);

        // Invalid CIDR (non-zero host bits)
        let result: Result<Cidr, _> = "192.168.1.1/24".parse();
        assert!(result.is_err());
    }

    #[test]
    fn test_cidr_contains() {
        let cidr: Cidr = "192.168.0.0/16".parse().unwrap();
        let addr: InetAddr = "192.168.1.100/32".parse().unwrap();

        assert!(cidr.contains(&addr));
    }

    #[test]
    fn test_mac_address() {
        let mac: MacAddr = "aa:bb:cc:dd:ee:ff".parse().unwrap();
        assert_eq!(mac.octets(), &[0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff]);
        assert_eq!(mac.to_string(), "aa:bb:cc:dd:ee:ff");
    }

    #[test]
    fn test_mac_formats() {
        let mac1: MacAddr = "aa:bb:cc:dd:ee:ff".parse().unwrap();
        let mac2: MacAddr = "aa-bb-cc-dd-ee-ff".parse().unwrap();
        let mac3: MacAddr = "aabbccddeeff".parse().unwrap();

        assert_eq!(mac1, mac2);
        assert_eq!(mac2, mac3);
    }

    #[test]
    fn test_mac_properties() {
        let broadcast: MacAddr = "ff:ff:ff:ff:ff:ff".parse().unwrap();
        assert!(broadcast.is_broadcast());
        assert!(broadcast.is_multicast());

        let unicast: MacAddr = "00:11:22:33:44:55".parse().unwrap();
        assert!(!unicast.is_multicast());
        assert!(unicast.is_universal());

        let local: MacAddr = "02:11:22:33:44:55".parse().unwrap();
        assert!(local.is_local());
    }

    #[test]
    fn test_mac8_from_mac() {
        let mac: MacAddr = "aa:bb:cc:dd:ee:ff".parse().unwrap();
        let mac8 = MacAddr8::from_mac(&mac);

        assert_eq!(
            mac8.octets(),
            &[0xaa, 0xbb, 0xcc, 0xff, 0xfe, 0xdd, 0xee, 0xff]
        );
    }

    #[test]
    fn test_mac8_parse() {
        let mac8: MacAddr8 = "aa:bb:cc:dd:ee:ff:00:11".parse().unwrap();
        assert_eq!(mac8.to_string(), "aa:bb:cc:dd:ee:ff:00:11");
    }

    #[test]
    fn test_inet_and() {
        let a: InetAddr = "192.168.1.100/24".parse().unwrap();
        let b: InetAddr = "255.255.255.0/32".parse().unwrap();

        let result = inet_and(&a, &b).unwrap();
        assert_eq!(result.to_string(), "192.168.1.0");
    }

    #[test]
    fn test_inet_or() {
        let a: InetAddr = "192.168.1.0/24".parse().unwrap();
        let b: InetAddr = "0.0.0.255/32".parse().unwrap();

        let result = inet_or(&a, &b).unwrap();
        assert_eq!(result.to_string(), "192.168.1.255");
    }

    #[test]
    fn test_inet_not() {
        let a: InetAddr = "255.255.255.0/24".parse().unwrap();
        let result = inet_not(&a);
        assert_eq!(result.to_string(), "0.0.0.255");
    }

    #[test]
    fn test_inet_add() {
        let a: InetAddr = "192.168.1.1/24".parse().unwrap();
        let result = inet_add(&a, 10).unwrap();
        assert_eq!(result.addr.to_string(), "192.168.1.11");

        let result2 = inet_add(&a, -1).unwrap();
        assert_eq!(result2.addr.to_string(), "192.168.1.0");
    }

    #[test]
    fn test_inet_subtract() {
        let a: InetAddr = "192.168.1.10/24".parse().unwrap();
        let b: InetAddr = "192.168.1.1/24".parse().unwrap();

        let diff = inet_subtract(&a, &b).unwrap();
        assert_eq!(diff, 9);
    }

    #[test]
    fn test_is_loopback() {
        let lo4: InetAddr = "127.0.0.1/8".parse().unwrap();
        assert!(is_loopback(&lo4));

        let lo6: InetAddr = "::1/128".parse().unwrap();
        assert!(is_loopback(&lo6));

        let pub_addr: InetAddr = "8.8.8.8/32".parse().unwrap();
        assert!(!is_loopback(&pub_addr));
    }

    #[test]
    fn test_is_private() {
        let priv1: InetAddr = "10.0.0.1/8".parse().unwrap();
        assert!(is_private(&priv1));

        let priv2: InetAddr = "172.16.0.1/12".parse().unwrap();
        assert!(is_private(&priv2));

        let priv3: InetAddr = "192.168.1.1/24".parse().unwrap();
        assert!(is_private(&priv3));

        let pub_addr: InetAddr = "8.8.8.8/32".parse().unwrap();
        assert!(!is_private(&pub_addr));
    }

    #[test]
    fn test_is_link_local() {
        let ll4: InetAddr = "169.254.1.1/16".parse().unwrap();
        assert!(is_link_local(&ll4));

        let ll6: InetAddr = "fe80::1/64".parse().unwrap();
        assert!(is_link_local(&ll6));
    }

    #[test]
    fn test_inet_comparison() {
        let a: InetAddr = "192.168.1.0/24".parse().unwrap();
        let b: InetAddr = "192.168.1.0/25".parse().unwrap();
        let c: InetAddr = "192.168.2.0/24".parse().unwrap();

        assert!(a < b); // Same address, smaller prefix
        assert!(b < c); // Different network
    }

    #[test]
    fn test_error_display() {
        let errors = vec![
            NetworkError::InvalidAddress("bad".to_string()),
            NetworkError::InvalidPrefixLength(64, 32),
            NetworkError::InvalidCidr("bad cidr".to_string()),
            NetworkError::InvalidMacAddress("bad mac".to_string()),
            NetworkError::FamilyMismatch,
        ];

        for error in errors {
            let msg = format!("{}", error);
            assert!(!msg.is_empty());
        }
    }

    #[test]
    fn test_mac_trunc() {
        let mac: MacAddr = "03:11:22:33:44:55".parse().unwrap();
        let truncated = mac.trunc();
        assert_eq!(truncated.octets()[0] & 0x03, 0);
    }
}
