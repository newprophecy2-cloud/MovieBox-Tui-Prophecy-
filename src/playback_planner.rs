#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PlaybackStrategy {
    DirectRangePassthrough,
    NativeHls,
    NativeDash,
    RemuxRequired,
    UnsupportedOrUnverified,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct VerifiedMediaCapabilities {
    pub format_name: Option<String>,
    pub video_codec: Option<String>,
    pub audio_codecs: Vec<String>,
    pub range_supported: bool,
}

pub(crate) fn plan_playback(
    source_url: &str,
    verified: Option<&VerifiedMediaCapabilities>,
) -> PlaybackStrategy {
    let parsed = match url::Url::parse(source_url) {
        Ok(url) => url,
        Err(_) => return PlaybackStrategy::UnsupportedOrUnverified,
    };
    let path = parsed.path().to_ascii_lowercase();
    if path.ends_with(".m3u8") {
        return PlaybackStrategy::NativeHls;
    }
    if path.ends_with(".mpd") {
        return PlaybackStrategy::NativeDash;
    }

    let Some(capabilities) = verified else {
        return PlaybackStrategy::UnsupportedOrUnverified;
    };
    let is_h264 = capabilities
        .video_codec
        .as_deref()
        .is_some_and(crate::codec_verification::is_allowed_codec);
    if !is_h264 {
        return PlaybackStrategy::UnsupportedOrUnverified;
    }
    let Some(format_name) = capabilities.format_name.as_deref() else {
        return PlaybackStrategy::RemuxRequired;
    };
    let format_name = format_name.to_ascii_lowercase();
    let is_mp4_family = format_name
        .split(',')
        .any(|format| matches!(format.trim(), "mp4" | "mov" | "m4a" | "3gp" | "3g2" | "mj2"));
    let audio_is_aac = capabilities
        .audio_codecs
        .iter()
        .all(|codec| codec.eq_ignore_ascii_case("aac"));

    if is_mp4_family && audio_is_aac && capabilities.range_supported {
        PlaybackStrategy::DirectRangePassthrough
    } else {
        PlaybackStrategy::RemuxRequired
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn compatible() -> VerifiedMediaCapabilities {
        VerifiedMediaCapabilities {
            format_name: Some("mov,mp4,m4a,3gp,3g2,mj2".into()),
            video_codec: Some("h264".into()),
            audio_codecs: vec!["aac".into()],
            range_supported: true,
        }
    }

    #[test]
    fn selects_direct_only_for_verified_compatible_range_source() {
        assert_eq!(
            plan_playback("https://media.example/video", Some(&compatible())),
            PlaybackStrategy::DirectRangePassthrough
        );
    }

    #[test]
    fn native_manifests_keep_their_delivery_strategy() {
        assert_eq!(
            plan_playback("https://media.example/master.m3u8?token=x", None),
            PlaybackStrategy::NativeHls
        );
        assert_eq!(
            plan_playback("https://media.example/manifest.mpd", None),
            PlaybackStrategy::NativeDash
        );
    }

    #[test]
    fn missing_or_invalid_evidence_never_selects_direct() {
        assert_eq!(
            plan_playback("https://media.example/video.mp4", None),
            PlaybackStrategy::UnsupportedOrUnverified
        );
        let mut evidence = compatible();
        evidence.range_supported = false;
        assert_eq!(
            plan_playback("https://media.example/no-extension", Some(&evidence)),
            PlaybackStrategy::RemuxRequired
        );
        evidence = compatible();
        evidence.format_name = None;
        assert_eq!(
            plan_playback("https://media.example/video.mp4", Some(&evidence)),
            PlaybackStrategy::RemuxRequired
        );
    }

    #[test]
    fn incompatible_container_codec_or_audio_never_selects_direct() {
        let mut evidence = compatible();
        evidence.format_name = Some("matroska,webm".into());
        assert_eq!(
            plan_playback("https://media.example/asset", Some(&evidence)),
            PlaybackStrategy::RemuxRequired
        );
        evidence = compatible();
        evidence.video_codec = Some("hevc".into());
        assert_eq!(
            plan_playback("https://media.example/asset", Some(&evidence)),
            PlaybackStrategy::UnsupportedOrUnverified
        );
        evidence = compatible();
        evidence.audio_codecs = vec!["eac3".into()];
        assert_eq!(
            plan_playback("https://media.example/asset", Some(&evidence)),
            PlaybackStrategy::RemuxRequired
        );
    }
}
