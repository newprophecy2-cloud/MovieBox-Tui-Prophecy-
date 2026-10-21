use super::models::{AddonManifest, MetaDetail, MetaItem, StreamItem};
use reqwest::Client;
use std::time::Duration;

#[derive(Clone, Debug)]
pub struct AddonClient {
    http: Client,
}

impl Default for AddonClient {
    fn default() -> Self {
        Self::new()
    }
}

impl AddonClient {
    pub fn new() -> Self {
        let http = crate::net::http_client_builder()
            .timeout(Duration::from_secs(12))
            .connect_timeout(Duration::from_secs(8))
            .user_agent("MovieBox-Tui/1.0 (Addon-Client)")
            .build()
            .unwrap_or_default();
        Self { http }
    }

    pub fn normalize_manifest_url(raw: &str) -> String {
        let mut url = raw.trim().to_string();
        if url.starts_with("stremio://") {
            url = format!("https://{}", &url["stremio://".len()..]);
        } else if !crate::net::is_http_url(&url) {
            url = format!("https://{url}");
        }
        let (base, suffix) = match url.find(['?', '#']) {
            Some(idx) => (&url[..idx], &url[idx..]),
            None => (url.as_str(), ""),
        };
        if !base.ends_with("/manifest.json") {
            let trimmed_base = base.trim_end_matches('/');
            url = format!("{trimmed_base}/manifest.json{suffix}");
        }
        url
    }

    pub fn base_addon_url(manifest_url: &str) -> String {
        let normalized = Self::normalize_manifest_url(manifest_url);
        if let Some(pos) = normalized.rfind("/manifest.json") {
            normalized[..pos].to_string()
        } else {
            normalized.trim_end_matches('/').to_string()
        }
    }

    pub async fn fetch_manifest(&self, manifest_url: &str) -> Result<AddonManifest, String> {
        let url = Self::normalize_manifest_url(manifest_url);
        crate::net::validate_public_url(&url, true)
            .await
            .map_err(|e| format!("Rejected addon manifest URL: {e}"))?;
        let url_clone = url.clone();
        if let Ok(Some(manifest)) = tokio::task::spawn_blocking(move || {
            crate::cache::get_addon_manifest_cache_typed(&url_clone)
        })
        .await
        {
            if !manifest.name.trim().is_empty() {
                return Ok(manifest);
            }
        }

        let resp = self
            .http
            .get(&url)
            .send()
            .await
            .map_err(|e| format!("Failed to reach manifest: {e}"))?;

        if !resp.status().is_success() {
            return Err(format!("Manifest returned HTTP {}", resp.status()));
        }

        let manifest: AddonManifest = resp
            .json()
            .await
            .map_err(|e| format!("Failed to parse manifest JSON: {e}"))?;

        if manifest.name.trim().is_empty() {
            return Err("Addon manifest missing valid name".to_string());
        }

        let url_clone = url.clone();
        let manifest_clone = manifest.clone();
        tokio::task::spawn_blocking(move || {
            crate::cache::set_addon_manifest_cache_typed(&url_clone, &manifest_clone);
        });

        Ok(manifest)
    }

    pub async fn fetch_catalog_search(
        &self,
        base_url: &str,
        r#type: &str,
        catalog_id: &str,
        query: &str,
    ) -> Result<Vec<MetaItem>, String> {
        let encoded_query =
            percent_encoding::utf8_percent_encode(query.trim(), percent_encoding::NON_ALPHANUMERIC)
                .to_string();

        let endpoint =
            format!("{base_url}/catalog/{type}/{catalog_id}/search={encoded_query}.json");
        crate::net::validate_public_url(&endpoint, true)
            .await
            .map_err(|e| format!("Rejected addon catalog URL: {e}"))?;
        let resp = self
            .http
            .get(&endpoint)
            .send()
            .await
            .map_err(|e| format!("Catalog search request failed: {e}"))?;

        if !resp.status().is_success() {
            return Err(format!("Catalog returned HTTP {}", resp.status()));
        }

        let raw: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| format!("Invalid catalog response JSON: {e}"))?;

        parse_catalog_metas(raw)
    }

    pub async fn fetch_catalog(
        &self,
        base_url: &str,
        r#type: &str,
        catalog_id: &str,
        extra: Option<&str>,
    ) -> Result<Vec<MetaItem>, String> {
        let endpoint = if let Some(ext) = extra.filter(|s| !s.trim().is_empty()) {
            format!("{base_url}/catalog/{type}/{catalog_id}/{ext}.json")
        } else {
            format!("{base_url}/catalog/{type}/{catalog_id}.json")
        };
        crate::net::validate_public_url(&endpoint, true)
            .await
            .map_err(|e| format!("Rejected addon catalog URL: {e}"))?;
        let resp = self
            .http
            .get(&endpoint)
            .send()
            .await
            .map_err(|e| format!("Catalog request failed: {e}"))?;

        if !resp.status().is_success() {
            return Err(format!("Catalog returned HTTP {}", resp.status()));
        }

        let raw: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| format!("Invalid catalog response JSON: {e}"))?;

        parse_catalog_metas(raw)
    }

    pub async fn fetch_meta(
        &self,
        base_url: &str,
        r#type: &str,
        id: &str,
    ) -> Result<MetaDetail, String> {
        let endpoint = format!("{base_url}/meta/{type}/{id}.json");
        crate::net::validate_public_url(&endpoint, true)
            .await
            .map_err(|e| format!("Rejected addon metadata URL: {e}"))?;
        let resp = self
            .http
            .get(&endpoint)
            .send()
            .await
            .map_err(|e| format!("Metadata request failed: {e}"))?;

        if !resp.status().is_success() {
            return Err(format!("Metadata returned HTTP {}", resp.status()));
        }

        let raw: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| format!("Invalid metadata response JSON: {e}"))?;

        let meta_target = if let Some(m) = raw.get("meta").filter(|m| m.is_object()) {
            m.clone()
        } else {
            raw
        };

        let detail: MetaDetail = serde_json::from_value(meta_target)
            .map_err(|e| format!("Invalid metadata detail format: {e}"))?;

        Ok(detail)
    }

    pub async fn fetch_streams(
        &self,
        base_url: &str,
        r#type: &str,
        id: &str,
    ) -> Result<Vec<StreamItem>, String> {
        let endpoint = format!("{base_url}/stream/{type}/{id}.json");
        crate::net::validate_public_url(&endpoint, true)
            .await
            .map_err(|e| format!("Rejected addon stream URL: {e}"))?;
        let resp = self
            .http
            .get(&endpoint)
            .send()
            .await
            .map_err(|e| format!("Streams request failed: {e}"))?;

        if !resp.status().is_success() {
            return Err(format!("Streams returned HTTP {}", resp.status()));
        }

        let raw: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| format!("Invalid streams response JSON: {e}"))?;

        parse_stream_response(raw)
    }
}

fn parse_stream_response(mut raw: serde_json::Value) -> Result<Vec<StreamItem>, String> {
    if let Some(val) = raw.get_mut("streams").map(serde_json::Value::take) {
        serde_json::from_value(val).map_err(|e| format!("Failed to parse streams: {e}"))
    } else if raw.is_array() {
        serde_json::from_value(raw).map_err(|e| format!("Failed to parse raw stream array: {e}"))
    } else {
        Err("Invalid streams response: missing streams array".to_string())
    }
}

fn parse_catalog_metas(mut raw: serde_json::Value) -> Result<Vec<MetaItem>, String> {
    if let Some(val) = raw.get_mut("metas").map(serde_json::Value::take) {
        serde_json::from_value(val).map_err(|e| format!("Failed to parse metas: {e}"))
    } else if let Some(val) = raw.get_mut("items").map(serde_json::Value::take) {
        serde_json::from_value(val).map_err(|e| format!("Failed to parse items: {e}"))
    } else if let Some(val) = raw.get_mut("results").map(serde_json::Value::take) {
        serde_json::from_value(val).map_err(|e| format!("Failed to parse items: {e}"))
    } else if raw.is_array() {
        serde_json::from_value(raw).map_err(|e| format!("Failed to parse raw array metas: {e}"))
    } else {
        Ok(Vec::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_streams_field_is_not_treated_as_empty_success() {
        let error = parse_stream_response(serde_json::json!({"meta": {}})).unwrap_err();
        assert!(error.contains("missing streams array"));
    }

    #[test]
    fn stream_array_response_is_supported() {
        let streams = parse_stream_response(serde_json::json!([
            {"name": "Test", "url": "https://cdn.example/test.mkv"}
        ]))
        .unwrap();
        assert_eq!(streams.len(), 1);
    }
}
