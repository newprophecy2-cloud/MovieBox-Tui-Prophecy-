use super::client::FourKHdHubError;
use crate::providers::models::ResolutionIntent;
use reqwest::Url;
use scraper::{Html, Selector};
use std::net::IpAddr;
use std::sync::LazyLock;

static SEL_DOWNLOAD: LazyLock<Selector> = LazyLock::new(|| {
    Selector::parse("a#download, a.btn-primary, a.btn-success, a.btn[href*='/download/'], a[href*='/download/'], a[href*='gamerxyt.com'], a[href*='hubcloud.php']").unwrap()
});
static SEL_LINKS: LazyLock<Selector> = LazyLock::new(|| Selector::parse("a[href]").unwrap());

pub async fn resolve(
    client: &reqwest::Client,
    drive_url: &str,
    intent: ResolutionIntent,
) -> Result<Vec<(String, String, Vec<(String, String)>)>, FourKHdHubError> {
    validate_resolver_url(drive_url)?;
    let drive_html = client
        .get(drive_url)
        .send()
        .await?
        .error_for_status()?
        .text()
        .await?;
    let resolver_url = {
        let document = Html::parse_document(&drive_html);
        document
            .select(&SEL_DOWNLOAD)
            .filter_map(|node| node.value().attr("href"))
            .find(|href| href.starts_with("https://"))
            .map(str::to_string)
            .ok_or_else(|| FourKHdHubError::Parse("HubCloud resolver link missing".into()))?
    };

    let resolver_html = client
        .get(&resolver_url)
        .send()
        .await?
        .error_for_status()?
        .text()
        .await?;
    let resolver = Html::parse_document(&resolver_html);

    let mut candidates = extract_script_pixeldrain_urls(&resolver_html)
        .into_iter()
        .map(|url| {
            (
                score(&url, "PixelDrain", intent),
                url,
                "PixelDrain".to_string(),
            )
        })
        .collect::<Vec<_>>();
    candidates.extend(
        resolver
            .select(&SEL_LINKS)
            .filter_map(|node| {
                let href = node.value().attr("href")?;
                let label = node.text().collect::<String>();
                if let Some(unwrapped) = unwrap_watch_online_url(href)
                    && let Ok(valid) = validate_playback_url(&unwrapped)
                {
                    return Some((
                        score(&valid, "Watch Online", intent),
                        valid,
                        "Watch Online".to_string(),
                    ));
                }
                validate_playback_url(href).ok().map(|url| {
                    let url = pixeldrain_api_url(&url).unwrap_or(url);
                    (score(&url, &label, intent), url, clean_label(&label))
                })
            })
            .collect::<Vec<_>>(),
    );
    candidates.sort_by_key(|candidate| candidate.0);
    let mut resolved = candidates
        .into_iter()
        .map(|(_, url, label)| (url, label, Vec::new()))
        .collect::<Vec<_>>();
    resolved.dedup_by(|left, right| left.0 == right.0);
    if resolved.is_empty() {
        Err(FourKHdHubError::NoPlayableMirror(
            "no candidates found".into(),
        ))
    } else {
        Ok(resolved)
    }
}

pub async fn resolve_hubdrive(
    client: &reqwest::Client,
    drive_url: &str,
    intent: ResolutionIntent,
) -> Result<Vec<(String, String, Vec<(String, String)>)>, FourKHdHubError> {
    validate_hubdrive_url(drive_url)?;
    let html = client
        .get(drive_url)
        .send()
        .await?
        .error_for_status()?
        .text()
        .await?;
    let hubcloud_url = extract_hubcloud_drive_url(&html)
        .ok_or_else(|| FourKHdHubError::Parse("HubDrive HubCloud mirror missing".into()))?;
    resolve(client, &hubcloud_url, intent).await
}

pub async fn resolve_greenmotors(
    client: &reqwest::Client,
    drive_url: &str,
    intent: ResolutionIntent,
) -> Result<Vec<(String, String, Vec<(String, String)>)>, FourKHdHubError> {
    validate_greenmotors_url(drive_url)?;
    let html = client
        .get(drive_url)
        .send()
        .await?
        .error_for_status()?
        .text()
        .await?;
    let target_url = unpack_greenmotors_url(&html)
        .ok_or_else(|| FourKHdHubError::Parse("GreenMotors mirror target missing".into()))?;
    if target_url.contains("hubcloud.") {
        resolve(client, &target_url, intent).await
    } else if target_url.contains("hubdrive.") {
        resolve_hubdrive(client, &target_url, intent).await
    } else {
        validate_playback_url(&target_url).map(|url| vec![(url, "Direct".to_string(), Vec::new())])
    }
}

fn validate_greenmotors_url(raw: &str) -> Result<(), FourKHdHubError> {
    let url = Url::parse(raw).map_err(|_| FourKHdHubError::InvalidUrl(raw.into()))?;
    let host = url.host_str().unwrap_or_default();
    if url.scheme() != "https"
        || (!host.contains("greenmotors.") && !host.contains("greenmountmotors."))
    {
        return Err(FourKHdHubError::InvalidUrl(raw.into()));
    }
    Ok(())
}

fn unpack_greenmotors_url(html: &str) -> Option<String> {
    let payload = extract_greenmotors_payload(html)?;
    decode_greenmotors_payload(&payload)
}

fn extract_greenmotors_payload(html: &str) -> Option<String> {
    let needle = "s(";
    let mut search_idx = 0;
    while let Some(pos) = html[search_idx..].find(needle) {
        let abs_pos = search_idx + pos + needle.len();
        let rest = html[abs_pos..].trim_start();
        let after_key = if let Some(stripped) = rest.strip_prefix("'o'") {
            stripped
        } else if let Some(stripped) = rest.strip_prefix("\"o\"") {
            stripped
        } else {
            search_idx = abs_pos;
            continue;
        };
        let after_comma = if let Some(stripped) = after_key.trim_start().strip_prefix(',') {
            stripped
        } else {
            search_idx = abs_pos;
            continue;
        };
        let trimmed = after_comma.trim_start();
        let quote = match trimmed.chars().next() {
            Some(c @ ('\'' | '"')) => c,
            _ => {
                search_idx = abs_pos;
                continue;
            }
        };
        let payload_slice = &trimmed[1..];
        if let Some(end) = payload_slice.find(quote) {
            return Some(payload_slice[..end].to_string());
        }
        search_idx = abs_pos;
    }
    None
}

fn rot13(input: &str) -> String {
    input
        .chars()
        .map(|c| match c {
            'a'..='m' | 'A'..='M' => ((c as u8) + 13) as char,
            'n'..='z' | 'N'..='Z' => ((c as u8) - 13) as char,
            _ => c,
        })
        .collect()
}

fn decode_greenmotors_payload(payload: &str) -> Option<String> {
    use base64::Engine;
    let b64 = &base64::engine::general_purpose::STANDARD;
    let step1_bytes = b64.decode(payload.as_bytes()).ok()?;
    let step1_str = String::from_utf8(step1_bytes).ok()?;
    let step2_bytes = b64.decode(step1_str.as_bytes()).ok()?;
    let step2_str = String::from_utf8(step2_bytes).ok()?;
    let step3_rot = rot13(&step2_str);
    let step4_bytes = b64.decode(step3_rot.as_bytes()).ok()?;
    let step4_str = String::from_utf8(step4_bytes).ok()?;
    let json_val: serde_json::Value = serde_json::from_str(&step4_str).ok()?;
    let target_b64 = json_val.get("o")?.as_str()?;
    let target_bytes = b64.decode(target_b64.as_bytes()).ok()?;
    String::from_utf8(target_bytes).ok()
}

fn extract_hubcloud_drive_url(html: &str) -> Option<String> {
    let document = Html::parse_document(html);
    document.select(&SEL_LINKS).find_map(|node| {
        let raw = node.value().attr("href")?;
        let url = Url::parse(raw).ok()?;
        let host = url.host_str()?;
        (host.contains("hubcloud.") && url.path().starts_with("/drive/")).then(|| url.to_string())
    })
}

fn unwrap_watch_online_url(raw: &str) -> Option<String> {
    let url = Url::parse(raw).ok()?;
    if url.host_str().is_some_and(|h| h.contains("pages.dev")) {
        let b64 = url
            .query_pairs()
            .find(|(k, _)| k == "u")
            .map(|(_, v)| v.into_owned())?;
        use base64::Engine;
        let decoded_bytes = base64::engine::general_purpose::STANDARD
            .decode(b64.as_bytes())
            .ok()?;
        let decoded_str = String::from_utf8(decoded_bytes).ok()?;
        if decoded_str.starts_with("https://") {
            return Some(decoded_str);
        }
    }
    None
}

fn extract_script_pixeldrain_urls(html: &str) -> Vec<String> {
    let mut urls = Vec::new();

    let mut search_prefix = |prefix: &str| {
        let mut remainder = html;
        while let Some(url_offset) = remainder.find(prefix) {
            let candidate = &remainder[url_offset..];
            let end = candidate
                .find(|character: char| {
                    character == '"'
                        || character == '\''
                        || character.is_whitespace()
                        || character == '<'
                        || character == '\\'
                })
                .unwrap_or(candidate.len());
            if let Some(url) = pixeldrain_api_url(&candidate[..end])
                && !urls.contains(&url)
            {
                urls.push(url);
            }
            remainder = &candidate[end..];
        }
    };

    search_prefix("https://pixeldrain.dev/u/");
    search_prefix("https://pixeldrain.com/u/");
    search_prefix("https://pixeldrain.dev/api/file/");
    search_prefix("https://pixeldrain.com/api/file/");
    urls
}

fn pixeldrain_api_url(raw: &str) -> Option<String> {
    let url = Url::parse(raw).ok()?;
    let host = url.host_str()?;
    if !host.contains("pixeldrain.") {
        return None;
    }
    let id = if let Some(stripped) = url.path().strip_prefix("/u/") {
        stripped.trim_matches('/')
    } else {
        let stripped = url.path().strip_prefix("/api/file/")?;
        stripped.trim_matches('/')
    };
    if id.is_empty()
        || !id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
    {
        return None;
    }
    Some(format!("https://{}/api/file/{}?download", host, id))
}

fn validate_resolver_url(raw: &str) -> Result<(), FourKHdHubError> {
    let url = Url::parse(raw).map_err(|_| FourKHdHubError::InvalidUrl(raw.into()))?;
    let host = url.host_str().unwrap_or_default();
    if url.scheme() != "https" || !host.contains("hubcloud.") || !url.path().starts_with("/drive/")
    {
        return Err(FourKHdHubError::InvalidUrl(raw.into()));
    }
    Ok(())
}

fn validate_hubdrive_url(raw: &str) -> Result<(), FourKHdHubError> {
    let url = Url::parse(raw).map_err(|_| FourKHdHubError::InvalidUrl(raw.into()))?;
    let host = url.host_str().unwrap_or_default();
    if url.scheme() != "https" || !host.contains("hubdrive.") || !url.path().starts_with("/file/") {
        return Err(FourKHdHubError::InvalidUrl(raw.into()));
    }
    Ok(())
}

pub fn validate_playback_url(raw: &str) -> Result<String, FourKHdHubError> {
    let url =
        normalize_playback_url_str(raw).ok_or_else(|| FourKHdHubError::InvalidUrl(raw.into()))?;
    if url.scheme() != "https" || url.host_str().is_none() {
        return Err(FourKHdHubError::InvalidUrl(raw.into()));
    }
    let host = url.host_str().unwrap_or_default().to_ascii_lowercase();
    let path = url.path().to_ascii_lowercase();
    if host == "localhost"
        || host.ends_with(".local")
        || host
            .parse::<IpAddr>()
            .is_ok_and(|address| !is_public_ip(address))
        || path.ends_with(".zip")
        || path.contains("login.php")
        || path.contains("logout")
        || host.contains("greenmotors.")
        || host.contains("greenmountmotors.")
    {
        return Err(FourKHdHubError::InvalidUrl(raw.into()));
    }
    Ok(url.to_string())
}

fn normalize_playback_url_str(raw: &str) -> Option<Url> {
    if let Ok(url) = Url::parse(raw) {
        return Some(url);
    }
    let (scheme_host, rest) = {
        let idx = raw.find("://")?;
        let after_scheme = &raw[idx + 3..];
        let host_end = after_scheme
            .find(['/', '?', '#'])
            .unwrap_or(after_scheme.len());
        let host = &after_scheme[..host_end];
        if host.is_empty() || host.contains(' ') {
            return None;
        }
        (&raw[..idx + 3 + host_end], &after_scheme[host_end..])
    };

    let (path_part, query_fragment) = if let Some(pos) = rest.find(['?', '#']) {
        (&rest[..pos], &rest[pos..])
    } else {
        (rest, "")
    };

    let encoded_path = path_part
        .split('/')
        .map(|segment| {
            segment
                .split(':')
                .map(|sub| {
                    percent_encoding::utf8_percent_encode(sub, percent_encoding::NON_ALPHANUMERIC)
                        .to_string()
                })
                .collect::<Vec<_>>()
                .join(":")
        })
        .collect::<Vec<_>>()
        .join("/");

    let full = format!("{}{}{}", scheme_host, encoded_path, query_fragment);
    Url::parse(&full).ok()
}

fn is_public_ip(address: IpAddr) -> bool {
    match address {
        IpAddr::V4(address) => {
            !(address.is_private()
                || address.is_loopback()
                || address.is_link_local()
                || address.is_broadcast()
                || address.is_documentation()
                || address.is_unspecified())
        }
        IpAddr::V6(address) => {
            !(address.is_loopback()
                || address.is_unspecified()
                || address.is_unique_local()
                || address.is_unicast_link_local())
        }
    }
}

pub fn score(url: &str, label: &str, intent: ResolutionIntent) -> u8 {
    let value = format!("{} {}", url, label).to_ascii_lowercase();
    match intent {
        ResolutionIntent::Playback | ResolutionIntent::DirectPlayback => {
            if value.contains("pixel.hubcloud.")
                || value.contains("googleusercontent.com")
                || value.contains("googlevideo.com")
                || value.contains("cloudflarestorage.com")
                || value.contains("r2.cloudflarestorage.com")
                || value.contains("fsl server")
                || value.contains("10gbps")
                || value.contains("r2.dev")
                || value.contains("watch online")
            {
                0
            } else if value.contains("storage.googleapis.com")
                || value.contains("hubcloud.cx/re/")
                || value.contains("hubcloud.fans/re/")
            {
                1
            } else if value.contains("pixeldrain.com")
                || value.contains("pixeldrain.dev")
                || value.contains("pixeldrain")
            {
                2
            } else if value.contains("testzip.php")
                || value.contains("vcloud.php")
                || value.contains("drive.php")
                || value.contains("gpdl.")
            {
                3
            } else {
                4
            }
        }
        ResolutionIntent::Download => {
            if value.contains("pixel.hubcloud.")
                || value.contains("googleusercontent.com")
                || value.contains("cloudflarestorage.com")
                || value.contains("r2.cloudflarestorage.com")
                || value.contains("fsl server")
                || value.contains("r2.dev")
                || value.contains("workers.dev")
                || value.contains("watch online")
            {
                0
            } else if value.contains("storage.googleapis.com")
                || value.contains("hubcloud.cx/re/")
                || value.contains("hubcloud.fans/re/")
            {
                1
            } else if value.contains("pixeldrain.com")
                || value.contains("pixeldrain.dev")
                || value.contains("pixeldrain")
            {
                2
            } else if value.contains("googlevideo.com")
                || value.contains("testzip.php")
                || value.contains("vcloud.php")
                || value.contains("drive.php")
                || value.contains("gpdl.")
            {
                3
            } else {
                4
            }
        }
    }
}

fn clean_label(label: &str) -> String {
    let clean = label.split_whitespace().collect::<Vec<_>>().join(" ");
    if clean.is_empty() {
        "Direct".into()
    } else {
        clean
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_and_encodes_unencoded_playback_paths() {
        let raw = "https://cdn.example.com/0:/Game of Thrones/Game.of.Thrones.S01E01.[1080p].mkv";
        let validated = validate_playback_url(raw).expect("valid url with spaces");
        assert!(validated.starts_with("https://cdn.example.com/0:/Game%20of%20Thrones/"));
        assert!(validated.ends_with(".mkv"));
    }

    #[test]
    fn rejects_invalid_and_insecure_playback_urls() {
        assert!(validate_playback_url("http://insecure.com/file.mkv").is_err());
        assert!(validate_playback_url("https://localhost/file.mkv").is_err());
        assert!(validate_playback_url("https://127.0.0.1/file.mkv").is_err());
        assert!(validate_playback_url("https://192.168.1.100/file.mkv").is_err());
        assert!(validate_playback_url("https://example.com/archive.zip").is_err());
        assert!(validate_playback_url("https://example.com/login.php").is_err());
    }

    #[test]
    fn scores_mirrors_by_priority() {
        assert_eq!(
            score(
                "https://pixel.hubcloud.ist/?id=123",
                "Download [Server : 10Gbps]",
                ResolutionIntent::Playback
            ),
            0
        );
        assert_eq!(
            score(
                "https://worker.sub.workers.dev/file.mkv",
                "Cloudflare",
                ResolutionIntent::Playback
            ),
            4
        );
        assert_eq!(
            score(
                "https://worker.sub.workers.dev/file.mkv",
                "Cloudflare",
                ResolutionIntent::Download
            ),
            0
        );
        assert_eq!(
            score(
                "https://c357acb6.r2.cloudflarestorage.com/file.mkv",
                "FSL Server",
                ResolutionIntent::Playback
            ),
            0
        );
        assert_eq!(
            score(
                "https://storage.googleapis.com/bucket/file.mkv",
                "Storage",
                ResolutionIntent::Playback
            ),
            1
        );
        assert_eq!(
            score(
                "https://pixeldrain.com/api/file/abc?download",
                "PixelDrain",
                ResolutionIntent::Playback
            ),
            2
        );
        assert_eq!(
            score(
                "https://unknown-mirror.org/file.mkv",
                "Unknown",
                ResolutionIntent::Playback
            ),
            4
        );
    }

    #[test]
    fn extracts_and_validates_supported_pixeldrain_script_mirror() {
        let markup = r#"<script>const download = "https://pixeldrain.com/u/A1b2_C3";</script>"#;
        let candidates = extract_script_pixeldrain_urls(markup);
        assert_eq!(
            candidates,
            vec!["https://pixeldrain.com/api/file/A1b2_C3?download"]
        );
        assert_eq!(
            validate_playback_url(&candidates[0]).unwrap(),
            "https://pixeldrain.com/api/file/A1b2_C3?download"
        );
    }

    #[test]
    fn decodes_greenmotors_payload_pipeline() {
        let payload = "Y214WE0xWjNZbXRhVUdwMmIxQldObFo2ZFRCeFZVOXRRbmxxYVV0UU9XRndla2w1YjNveGFYRlVPV3h3YkRWM2IxVkpka3RRT1dKdk1qRjViMVJUYUUxVVNXeExVRGgyV1ZCWGFWWjNZblpNU0hWR1dsUkJWa2RIVFZweVIzbHBUVk54V0c1NlYxVkNSMU51UkcxSmFreHRRVVZ4ZVdOV1JtRlBlRzlKU1RKTWJVRkNjbnBCYUVwaGVYbHZlWGswU25vMWVISktXbTFIZDFaMmMwUTlQUT09";
        let target = decode_greenmotors_payload(payload);
        assert_eq!(
            target.as_deref(),
            Some("https://hubcloud.ist/drive/sssrvrzv1fwrssv")
        );
    }

    #[test]
    fn extracts_greenmotors_script_payload() {
        let snippet = r#"<script>function s(e,t,i){}s('o','TEST_PAYLOAD_ABC123',180000);</script>"#;
        assert_eq!(
            extract_greenmotors_payload(snippet).as_deref(),
            Some("TEST_PAYLOAD_ABC123")
        );
    }
}
