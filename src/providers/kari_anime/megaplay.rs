use aes::Aes256;
use aes::cipher::{BlockDecrypt, KeyInit, generic_array::GenericArray};
use base64::{
    Engine as _,
    engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD},
};
use hmac_legacy::{Hmac, Mac};
use serde::Deserialize;
use sha2_legacy::Sha256;
use std::time::{SystemTime, UNIX_EPOCH};

const AES_KEY: &[u8; 32] = b"i?LMTAx0Q6,:}50U\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0";
const AES_IV: &[u8; 16] = b"W0;27ToaUpl_P%'c";
const CDN_HMAC_SECRET: &[u8] = b"MpCdnT0k3n!9f2K#xQ7vL5mR8wN1pY4s";

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct Track {
    #[serde(default)]
    pub file: String,
    #[serde(default)]
    pub label: String,
    #[serde(default)]
    pub kind: String,
    #[serde(default)]
    pub default: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct Cue {
    pub start: f64,
    pub end: f64,
}

#[derive(Debug, Deserialize)]
struct SourcesResponse {
    #[serde(default)]
    tracks: Vec<Track>,
    #[serde(default)]
    intro: Option<Cue>,
    #[serde(default)]
    outro: Option<Cue>,
    #[serde(default)]
    enc: String,
}

#[derive(Debug, Deserialize)]
struct DecryptedFile {
    file: String,
}

#[derive(Debug, Clone)]
pub(crate) struct Resolved {
    pub stream_url: String,
    pub tracks: Vec<Track>,
    pub intro: Option<Cue>,
    pub outro: Option<Cue>,
    pub embed_origin: String,
}

pub(crate) fn decrypt_enc(encoded: &str) -> Result<String, String> {
    let standard = encoded.replace('-', "+").replace('_', "/");
    let padded = match standard.len() % 4 {
        0 => standard,
        n => format!("{standard}{}", "=".repeat(4 - n)),
    };
    let ciphertext = STANDARD
        .decode(padded)
        .map_err(|e| format!("MegaPlay base64: {e}"))?;
    if ciphertext.is_empty() || ciphertext.len() % 16 != 0 {
        return Err(format!(
            "MegaPlay ciphertext has invalid length {}",
            ciphertext.len()
        ));
    }
    let cipher = Aes256::new_from_slice(AES_KEY).map_err(|e| format!("MegaPlay AES key: {e}"))?;
    let mut plaintext = Vec::with_capacity(ciphertext.len());
    let mut previous = *AES_IV;
    for chunk in ciphertext.chunks_exact(16) {
        let mut block = GenericArray::clone_from_slice(chunk);
        cipher.decrypt_block(&mut block);
        for (index, byte) in block.iter_mut().enumerate() {
            *byte ^= previous[index];
        }
        plaintext.extend_from_slice(&block);
        previous.copy_from_slice(chunk);
    }
    let padding = *plaintext
        .last()
        .ok_or_else(|| "MegaPlay plaintext is empty".to_string())? as usize;
    if padding == 0
        || padding > 16
        || padding > plaintext.len()
        || !plaintext[plaintext.len() - padding..]
            .iter()
            .all(|byte| *byte as usize == padding)
    {
        return Err("MegaPlay PKCS#7 padding is invalid".into());
    }
    plaintext.truncate(plaintext.len() - padding);
    let payload: DecryptedFile = serde_json::from_slice(&plaintext)
        .map_err(|e| format!("MegaPlay decrypted payload: {e}"))?;
    if payload.file.trim().is_empty() {
        return Err("MegaPlay decrypted payload has no file URL".into());
    }
    Ok(payload.file)
}

pub(crate) fn sign_cdn_url(raw: &str, ttl_seconds: i64, now: i64) -> String {
    let segments: Vec<&str> = raw.split('/').collect();
    let path_pair = segments.windows(2).find_map(|pair| {
        let valid = |part: &str| part.len() == 32 && part.bytes().all(|b| b.is_ascii_hexdigit());
        (valid(pair[0]) && valid(pair[1])).then(|| {
            format!(
                "{}/{}",
                pair[0].to_ascii_lowercase(),
                pair[1].to_ascii_lowercase()
            )
        })
    });
    let Some(path_key) = path_pair else {
        return raw.to_string();
    };
    let message = format!("{}|{}", now.saturating_add(ttl_seconds), path_key);
    let prefix = URL_SAFE_NO_PAD.encode(message.as_bytes());
    let Ok(mut mac) = <Hmac<Sha256> as Mac>::new_from_slice(CDN_HMAC_SECRET) else {
        return raw.to_string();
    };
    mac.update(message.as_bytes());
    let token = format!(
        "{}.{}",
        prefix,
        URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes())
    );
    format!(
        "{}{}token={}",
        raw,
        if raw.contains('?') { '&' } else { '?' },
        token
    )
}

fn file_id_from_html(html: &str) -> Option<String> {
    let doc = scraper::Html::parse_document(html);
    let selector = scraper::Selector::parse("[data-id]").ok()?;
    for node in doc.select(&selector) {
        if let Some(id) = node.value().attr("data-id").map(str::trim)
            && !id.is_empty()
            && id.bytes().all(|b| b.is_ascii_digit())
        {
            return Some(id.to_string());
        }
    }
    let bytes = html.as_bytes();
    for (offset, _) in html.match_indices("File ") {
        let tail = &html[offset + 5..];
        let digits: String = tail.chars().take_while(char::is_ascii_digit).collect();
        if !digits.is_empty() && tail[digits.len()..].starts_with(" -") {
            return Some(digits);
        }
    }
    let _ = bytes;
    None
}

pub(crate) async fn resolve_embed(
    client: &reqwest::Client,
    embed_url: &str,
    referer: &str,
    user_agent: &str,
) -> Result<Resolved, crate::providers::models::ProviderError> {
    use crate::providers::models::ProviderError;
    let embed =
        crate::net::validate_url_syntax(embed_url, false).map_err(ProviderError::Parsing)?;
    let _embed_permit = crate::rate_limit::acquire_url(embed.as_str())
        .await
        .map_err(ProviderError::Unavailable)?;
    let page = client
        .get(embed.clone())
        .header(reqwest::header::USER_AGENT, user_agent)
        .header(reqwest::header::REFERER, referer)
        .send()
        .await
        .map_err(|e| ProviderError::Network(e.to_string()))?;
    let status = page.status();
    if !status.is_success() {
        return Err(if status.as_u16() == 429 {
            ProviderError::RateLimited(None)
        } else {
            ProviderError::Unavailable(format!("MegaPlay embed HTTP status {}", status.as_u16()))
        });
    }
    let html = page
        .text()
        .await
        .map_err(|e| ProviderError::Parsing(format!("MegaPlay embed HTML: {e}")))?;
    let file_id = file_id_from_html(&html).ok_or_else(|| {
        ProviderError::Unavailable("MegaPlay embed has no numeric file ID".into())
    })?;
    let origin = format!(
        "{}://{}/",
        embed.scheme(),
        embed.host_str().unwrap_or_default()
    );
    let endpoint = format!(
        "{}://{}/stream/getSources?id={}",
        embed.scheme(),
        embed.host_str().unwrap_or_default(),
        file_id
    );
    let _sources_permit = crate::rate_limit::acquire_url(&endpoint)
        .await
        .map_err(ProviderError::Unavailable)?;
    let response = client
        .get(&endpoint)
        .header(reqwest::header::USER_AGENT, user_agent)
        .header(reqwest::header::REFERER, embed_url)
        .header("X-Requested-With", "XMLHttpRequest")
        .header(reqwest::header::ACCEPT, "application/json")
        .send()
        .await
        .map_err(|e| ProviderError::Network(e.to_string()))?;
    let status = response.status();
    if !status.is_success() {
        return Err(if status.as_u16() == 429 {
            ProviderError::RateLimited(None)
        } else {
            ProviderError::Unavailable(format!(
                "MegaPlay getSources HTTP status {}",
                status.as_u16()
            ))
        });
    }
    let value: SourcesResponse = response
        .json()
        .await
        .map_err(|e| ProviderError::Parsing(format!("MegaPlay getSources JSON: {e}")))?;
    if value.enc.trim().is_empty() {
        return Err(ProviderError::Unavailable(
            "MegaPlay getSources response has no enc token".into(),
        ));
    }
    let decrypted = decrypt_enc(&value.enc).map_err(ProviderError::Parsing)?;
    let stream = sign_cdn_url(
        &decrypted,
        86_400,
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64,
    );
    let url = crate::net::validate_url_syntax(&stream, false)
        .map_err(|e| ProviderError::Parsing(format!("MegaPlay media URL: {e}")))?;
    Ok(Resolved {
        stream_url: url.to_string(),
        tracks: value.tracks,
        intro: value.intro,
        outro: value.outro,
        embed_origin: origin,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decrypts_pinned_kari_encrypted_fixture() {
        let encrypted = "wdeBruh3qqn_i5wUNnyaPcXqidp1UWP84FfPHzGyKXDMoY_RzXqbC0h49XmoI7d0vYZArA5rcKY-FQxnEk8NNEWezec9dd2jwxp1UbLN43p_7CMxXPDF5BUX86bUm0_Uuw0-dlv_yT9MKsnmlOgDFm3ReCPZPJloNlqBF7aftTk";
        let actual = decrypt_enc(encrypted).unwrap();
        assert_eq!(
            actual,
            "https://fetch.nexabloom.top/anime/8d9a0adb7c204239c9635426f35c9522/25e29cc9773a7a51e35941a5f67ef1f9/master.m3u8"
        );
    }

    #[test]
    fn signs_only_expected_cdn_path_and_uses_url_safe_hmac_token() {
        let raw = "https://cdn.example/0123456789abcdef0123456789abcdef/abcdef0123456789abcdef0123456789/master.m3u8";
        let signed = sign_cdn_url(raw, 86_400, 1_700_000_000);
        assert!(signed.starts_with(raw));
        let token = signed.split("token=").nth(1).unwrap();
        let (message, signature) = token.split_once('.').unwrap();
        assert_eq!(
            URL_SAFE_NO_PAD.decode(message).unwrap(),
            b"1700086400|0123456789abcdef0123456789abcdef/abcdef0123456789abcdef0123456789"
        );
        assert_eq!(URL_SAFE_NO_PAD.decode(signature).unwrap().len(), 32);
        assert_eq!(
            sign_cdn_url("https://cdn.example/no-token/master.m3u8", 10, 1),
            "https://cdn.example/no-token/master.m3u8"
        );
    }

    #[test]
    fn extracts_numeric_embed_file_id() {
        assert_eq!(
            file_id_from_html("<div data-id=\"12345\"></div>"),
            Some("12345".into())
        );
        assert_eq!(
            file_id_from_html("<title>File 98271 - Video</title>"),
            Some("98271".into())
        );
        assert_eq!(file_id_from_html("<title>Error</title>"), None);
    }
}
