use std::net::SocketAddr;
use std::sync::Arc;

use hickory_resolver::TokioResolver;
use hickory_resolver::config::{
    CLOUDFLARE_IPS, GOOGLE_IPS, LookupIpStrategy, NameServerConfigGroup, QUAD9_IPS, ResolverConfig,
};
use hickory_resolver::name_server::TokioConnectionProvider;
use reqwest::dns::{Addrs, Name, Resolve, Resolving};

const FALLBACK_DNS_PORT: u16 = 53;
pub const DEFAULT_BROWSER_USER_AGENT: &str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36";
pub const APP_HTTP_USER_AGENT: &str = concat!("MovieBox-Tui/", env!("CARGO_PKG_VERSION"));

static GLOBAL_RESOLVER: std::sync::LazyLock<Arc<TokioResolver>> =
    std::sync::LazyLock::new(|| Arc::new(build_resolver()));

#[derive(Debug, Default, Clone)]
pub struct FallbackResolver;

impl FallbackResolver {
    pub fn new() -> Self {
        Self
    }
}

fn fallback_servers() -> Vec<std::net::IpAddr> {
    let mut servers = Vec::with_capacity(CLOUDFLARE_IPS.len() + GOOGLE_IPS.len() + QUAD9_IPS.len());
    servers.extend_from_slice(CLOUDFLARE_IPS);
    servers.extend_from_slice(GOOGLE_IPS);
    servers.extend_from_slice(QUAD9_IPS);
    servers
}

fn fallback_config() -> ResolverConfig {
    ResolverConfig::from_parts(
        None,
        Vec::new(),
        NameServerConfigGroup::from_ips_clear(&fallback_servers(), FALLBACK_DNS_PORT, true),
    )
}

fn build_resolver() -> TokioResolver {
    let mut builder = match TokioResolver::builder_tokio() {
        Ok(builder) => builder,
        Err(e) => {
            log::warn!(
                "system DNS resolver failed ({e}); falling back to public DNS (Cloudflare/Google/Quad9)"
            );
            TokioResolver::builder_with_config(
                fallback_config(),
                TokioConnectionProvider::default(),
            )
        }
    };
    builder.options_mut().ip_strategy = LookupIpStrategy::Ipv4AndIpv6;
    builder.build()
}

impl Resolve for FallbackResolver {
    fn resolve(&self, name: Name) -> Resolving {
        let resolver = Arc::clone(&GLOBAL_RESOLVER);
        Box::pin(async move {
            let lookup = resolver.lookup_ip(name.as_str()).await?;
            let mut addresses = lookup
                .into_iter()
                .filter(|address| crate::config::is_test_environment() || is_public_ip(*address))
                .map(|address| SocketAddr::new(address, 0));
            let first = addresses.next().ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    "outbound destination resolves to a private or local address",
                )
            })?;
            let addrs: Addrs = Box::new(std::iter::once(first).chain(addresses));
            Ok(addrs)
        })
    }
}

pub fn is_public_ip(address: std::net::IpAddr) -> bool {
    match address {
        std::net::IpAddr::V4(address) => {
            let octets = address.octets();
            let first = octets[0];
            let second = octets[1];
            let is_shared_address_space = first == 100 && (64..=127).contains(&second);
            let is_benchmarking = first == 198 && (18..=19).contains(&second);
            let is_protocol_assignment = first == 192 && second == 0 && octets[2] == 0;
            !(address.is_private()
                || address.is_loopback()
                || address.is_link_local()
                || address.is_broadcast()
                || address.is_documentation()
                || address.is_unspecified()
                || first == 0
                || first >= 224
                || is_shared_address_space
                || is_benchmarking
                || is_protocol_assignment)
        }
        std::net::IpAddr::V6(address) => {
            if let Some(mapped) = address.to_ipv4_mapped() {
                return is_public_ip(std::net::IpAddr::V4(mapped));
            }
            let segments = address.segments();
            let is_global_unicast = segments[0] & 0xe000 == 0x2000;
            let is_documentation = segments[0] == 0x2001 && segments[1] == 0x0db8;
            let is_benchmarking = segments[0] == 0x2001 && segments[1] == 0x0002;
            let is_orchid = segments[0] == 0x2001 && segments[1] & 0xfff0 == 0x0010;
            is_global_unicast
                && !is_documentation
                && !is_benchmarking
                && !is_orchid
                && !address.is_loopback()
                && !address.is_unspecified()
                && !address.is_unique_local()
                && !address.is_unicast_link_local()
        }
    }
}

const MAX_URL_INPUT_BYTES: usize = 32 * 1024;
const MAX_RAW_HOST_BYTES: usize = 1024;

fn has_bounded_url_authority(raw: &str) -> bool {
    if raw.len() > MAX_URL_INPUT_BYTES {
        return false;
    }
    let Some((scheme, remainder)) = raw.split_once("://") else {
        return false;
    };
    if !scheme.eq_ignore_ascii_case("http") && !scheme.eq_ignore_ascii_case("https") {
        return false;
    }
    let authority = remainder.split(['/', '?', '#']).next().unwrap_or_default();
    let authority = authority
        .rsplit_once('@')
        .map_or(authority, |(_, host)| host);
    if authority.starts_with('[') {
        return authority.find(']').is_some_and(|end| end <= 64);
    }
    let host = authority
        .split_once(':')
        .map_or(authority, |(host, _)| host)
        .strip_suffix('.')
        .unwrap_or_else(|| {
            authority
                .split_once(':')
                .map_or(authority, |(host, _)| host)
        });
    !host.is_empty()
        && host.len() <= MAX_RAW_HOST_BYTES
        && host.chars().count() <= 253
        && host
            .split('.')
            .all(|label| !label.is_empty() && label.len() <= 256 && label.chars().count() <= 63)
}

fn normalized_host_has_bounded_dns_size(host: &str) -> bool {
    if host.parse::<std::net::IpAddr>().is_ok() {
        return true;
    }
    let host = host.strip_suffix('.').unwrap_or(host);
    !host.is_empty()
        && host.len() <= 253
        && host
            .split('.')
            .all(|label| !label.is_empty() && label.len() <= 63)
}

pub fn validate_url_syntax(raw: &str, allow_http: bool) -> Result<reqwest::Url, String> {
    let trimmed = raw.trim();
    if !has_bounded_url_authority(trimmed) {
        return Err("invalid URL".to_string());
    }
    let url = reqwest::Url::parse(trimmed).map_err(|_| "invalid URL".to_string())?;
    let scheme_ok = url.scheme() == "https" || (allow_http && url.scheme() == "http");
    let host = url.host_str().unwrap_or_default().to_ascii_lowercase();
    let host_size_ok = normalized_host_has_bounded_dns_size(&host);
    let literal_private = host
        .parse::<std::net::IpAddr>()
        .is_ok_and(|address| !is_public_ip(address));
    if !scheme_ok
        || url.host_str().is_none()
        || !host_size_ok
        || !url.username().is_empty()
        || url.password().is_some()
        || host == "localhost"
        || host.ends_with(".local")
        || literal_private
    {
        return Err("URL is not an allowed public HTTP(S) destination".to_string());
    }
    Ok(url)
}

pub async fn validate_public_url(raw: &str, allow_http: bool) -> Result<reqwest::Url, String> {
    if crate::config::is_test_environment() {
        let trimmed = raw.trim();
        if !has_bounded_url_authority(trimmed) {
            return Err("invalid URL".to_string());
        }
        let url = reqwest::Url::parse(trimmed).map_err(|_| "invalid URL".to_string())?;
        let scheme_ok = url.scheme() == "https" || (allow_http && url.scheme() == "http");
        if scheme_ok
            && url
                .host_str()
                .is_some_and(normalized_host_has_bounded_dns_size)
        {
            return Ok(url);
        }
        return Err("URL is not an allowed HTTP(S) test destination".to_string());
    }
    let url = validate_url_syntax(raw, allow_http)?;
    let host = url
        .host_str()
        .ok_or_else(|| "URL has no host".to_string())?
        .to_string();
    let port = url
        .port_or_known_default()
        .ok_or_else(|| "URL has no known port".to_string())?;
    let mut addresses = tokio::net::lookup_host((host, port))
        .await
        .map_err(|_| "destination DNS lookup failed".to_string())?;
    if addresses.all(|address| is_public_ip(address.ip())) {
        Ok(url)
    } else {
        Err("destination resolves to a private or local address".to_string())
    }
}

pub fn http_client_builder_base() -> reqwest::ClientBuilder {
    reqwest::Client::builder()
        .dns_resolver(Arc::new(FallbackResolver::new()))
        .redirect(reqwest::redirect::Policy::custom(|attempt| {
            if validate_url_syntax(attempt.url().as_str(), true).is_ok() {
                attempt.follow()
            } else {
                attempt.error("redirect target is not an allowed public HTTP(S) URL")
            }
        }))
        .tcp_nodelay(true)
        .tcp_keepalive(Some(std::time::Duration::from_secs(45)))
        .pool_idle_timeout(Some(std::time::Duration::from_secs(90)))
        .pool_max_idle_per_host(8)
        .connect_timeout(std::time::Duration::from_secs(15))
}

pub fn http_client_builder() -> reqwest::ClientBuilder {
    http_client_builder_base().timeout(std::time::Duration::from_secs(60))
}

pub async fn probe_url(url: &str, timeout: std::time::Duration) -> bool {
    static PROBE_CLIENT: std::sync::LazyLock<reqwest::Client> = std::sync::LazyLock::new(|| {
        http_client_builder_base()
            .build()
            .unwrap_or_else(|_| reqwest::Client::new())
    });
    match tokio::time::timeout(timeout, PROBE_CLIENT.head(url).send()).await {
        Ok(Ok(resp)) if resp.status().is_success() || resp.status().is_redirection() => {
            return true;
        }
        Ok(Ok(resp)) => {
            log::debug!(
                "probe HEAD non-success for {}: {}",
                crate::logging::sanitize_url(url),
                resp.status()
            );
        }
        Ok(Err(e)) => {
            log::debug!(
                "probe HEAD error for {}: {e}",
                crate::logging::sanitize_url(url)
            );
        }
        Err(_) => {
            log::debug!(
                "probe HEAD timed out for {}",
                crate::logging::sanitize_url(url)
            );
        }
    }
    match tokio::time::timeout(
        timeout,
        PROBE_CLIENT
            .get(url)
            .header(reqwest::header::RANGE, "bytes=0-0")
            .send(),
    )
    .await
    {
        Ok(Ok(resp)) => resp.status().is_success() || resp.status().is_redirection(),
        Ok(Err(e)) => {
            log::debug!(
                "probe GET error for {}: {e}",
                crate::logging::sanitize_url(url)
            );
            false
        }
        Err(_) => false,
    }
}

pub fn is_http_url(source: &str) -> bool {
    let trimmed = source.trim();
    trimmed.starts_with("http://") || trimmed.starts_with("https://")
}

pub fn open_external_url(url: &str) -> std::io::Result<()> {
    if crate::config::is_test_environment() {
        return Ok(());
    }
    open::that(url)
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fallback_config_contains_public_resolvers() {
        let config = fallback_config();
        let servers = config.name_servers();
        assert!(servers.len() >= 12);
        assert!(servers.iter().any(|server| server.socket_addr.ip()
            == std::net::IpAddr::V4(std::net::Ipv4Addr::new(1, 1, 1, 1))));
        assert!(
            servers
                .iter()
                .any(|server| server.socket_addr.port() == FALLBACK_DNS_PORT)
        );
    }

    #[test]
    fn fallback_servers_deduplicate_nothing_and_cover_all_providers() {
        let servers = fallback_servers();
        assert_eq!(
            servers.len(),
            CLOUDFLARE_IPS.len() + GOOGLE_IPS.len() + QUAD9_IPS.len()
        );
    }

    #[tokio::test]
    async fn built_resolver_prefers_ipv4_and_ipv6_lookup() {
        let resolver = build_resolver();
        assert_eq!(
            resolver.options().ip_strategy,
            LookupIpStrategy::Ipv4AndIpv6
        );
    }

    #[tokio::test]
    async fn probe_url_falls_back_from_head_to_ranged_get() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            for _ in 0..2 {
                if let Ok((mut stream, _)) = listener.accept().await {
                    let mut buf = [0u8; 1024];
                    let n = stream.read(&mut buf).await.unwrap_or(0);
                    let req = String::from_utf8_lossy(&buf[..n]);
                    if req.starts_with("HEAD ") {
                        let _ = stream
                            .write_all(
                                b"HTTP/1.1 405 Method Not Allowed\r\nContent-Length: 0\r\n\r\n",
                            )
                            .await;
                    } else if req.starts_with("GET ")
                        && req.to_ascii_lowercase().contains("range: bytes=0-0")
                    {
                        let _ = stream
                            .write_all(
                                b"HTTP/1.1 206 Partial Content\r\nContent-Length: 1\r\n\r\nX",
                            )
                            .await;
                    } else {
                        let _ = stream
                            .write_all(b"HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\n\r\n")
                            .await;
                    }
                }
            }
        });
        let url = format!("http://{addr}/video.mkv");
        assert!(probe_url(&url, std::time::Duration::from_secs(2)).await);
    }

    #[test]
    fn url_validation_rejects_oversized_authorities_before_idna_parsing() {
        let overlong_label = format!("https://{}.example/video", "a".repeat(64));
        let oversized_host = format!("https://{}/video", "a".repeat(MAX_RAW_HOST_BYTES + 1));
        let oversized_url = format!("https://example.com/{}", "x".repeat(MAX_URL_INPUT_BYTES));

        assert!(validate_url_syntax(&overlong_label, true).is_err());
        assert!(validate_url_syntax(&oversized_host, true).is_err());
        assert!(validate_url_syntax(&oversized_url, true).is_err());
        assert!(validate_url_syntax("https://例え.テスト/video", true).is_ok());
    }

    #[test]
    fn test_is_http_url() {
        assert!(is_http_url("http://example.com"));
        assert!(is_http_url("https://example.com/playlist.m3u8"));
        assert!(is_http_url("   https://example.com   "));
        assert!(!is_http_url("/local/path/file.m3u"));
        assert!(!is_http_url("stremio://addon.example.com"));
        assert!(!is_http_url(""));
    }

    #[test]
    fn outbound_url_policy_allows_public_http_media_but_rejects_private_targets() {
        assert!(validate_url_syntax("http://media.example/video.m3u8", true).is_ok());
        assert!(validate_url_syntax("https://media.example/video.m3u8", true).is_ok());
        assert!(validate_url_syntax("http://media.example/video.m3u8", false).is_err());
        assert!(validate_url_syntax("http://127.0.0.1/video.m3u8", true).is_err());
        assert!(validate_url_syntax("https://user:pass@media.example/video.m3u8", true).is_err());
        assert!(!is_public_ip("::ffff:127.0.0.1".parse().unwrap()));
        assert!(!is_public_ip("::ffff:10.0.0.8".parse().unwrap()));
        assert!(is_public_ip("::ffff:8.8.8.8".parse().unwrap()));
        assert!(!is_public_ip("100.64.0.1".parse().unwrap()));
        assert!(!is_public_ip("198.18.0.1".parse().unwrap()));
        assert!(!is_public_ip("224.0.0.1".parse().unwrap()));
        assert!(!is_public_ip("2001:db8::1".parse().unwrap()));
        assert!(!is_public_ip("2001:2::1".parse().unwrap()));
        assert!(is_public_ip("2606:4700:4700::1111".parse().unwrap()));
    }
}
