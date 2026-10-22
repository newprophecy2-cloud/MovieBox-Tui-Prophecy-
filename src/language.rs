#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestedLanguage {
    Hindi,
    Japanese,
    English,
    Bangla,
}

impl RequestedLanguage {
    pub const fn code(self) -> &'static str {
        match self {
            Self::Hindi => "hi",
            Self::Japanese => "ja",
            Self::English => "en",
            Self::Bangla => "bn",
        }
    }

    pub const fn name(self) -> &'static str {
        match self {
            Self::Hindi => "Hindi",
            Self::Japanese => "Japanese",
            Self::English => "English",
            Self::Bangla => "Bangla",
        }
    }

    pub const fn result_label(self) -> &'static str {
        match self {
            Self::Hindi => "Hindi Dub",
            Self::Japanese => "Japanese",
            Self::English => "English Dub",
            Self::Bangla => "Bangla Dub",
        }
    }

    fn aliases(self) -> &'static [&'static str] {
        match self {
            Self::Hindi => &["hindi"],
            Self::Japanese => &["japanese", "japan"],
            Self::English => &["english", "eng"],
            Self::Bangla => &["bangla", "bengali"],
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchIntent {
    pub original: String,
    pub title: String,
    pub language: Option<RequestedLanguage>,
}

pub fn parse_query(query: &str) -> SearchIntent {
    let original = query.trim().to_string();
    let tokens: Vec<&str> = original.split_whitespace().collect();
    if tokens.is_empty() {
        return SearchIntent {
            original,
            title: String::new(),
            language: None,
        };
    }

    let mut language = None;
    let mut remove = 0usize;
    for candidate in [
        RequestedLanguage::Hindi,
        RequestedLanguage::Japanese,
        RequestedLanguage::English,
        RequestedLanguage::Bangla,
    ] {
        for alias in candidate.aliases() {
            for suffix in [
                vec![*alias],
                vec![*alias, "dub"],
                vec![*alias, "audio"],
                vec!["original", *alias],
            ] {
                if tokens.len() > suffix.len()
                    && tokens
                        .iter()
                        .rev()
                        .take(suffix.len())
                        .rev()
                        .map(|token| {
                            token
                                .trim_matches(|c: char| !c.is_alphanumeric())
                                .to_ascii_lowercase()
                        })
                        .eq(suffix.iter().map(|token| (*token).to_string()))
                {
                    let remaining = tokens.len() - suffix.len();
                    let title = tokens[..remaining].join(" ");
                    if !title.eq_ignore_ascii_case("the")
                        && !title.eq_ignore_ascii_case("a")
                        && !title.eq_ignore_ascii_case("an")
                    {
                        language = Some(candidate);
                        remove = suffix.len();
                    }
                }
            }
        }
        if language.is_some() {
            break;
        }
    }

    let title = if language.is_some() {
        tokens[..tokens.len() - remove].join(" ")
    } else {
        original.clone()
    };
    SearchIntent {
        original,
        title,
        language,
    }
}

pub fn language_from_provider_text(text: &str) -> Option<RequestedLanguage> {
    let lower = text.to_ascii_lowercase();
    let explicit = lower.contains("[")
        || lower.contains("]")
        || lower.contains(" dub")
        || lower.contains(" audio")
        || lower.contains("-dub")
        || lower.contains("_dub");
    if !explicit {
        return None;
    }
    [
        RequestedLanguage::Hindi,
        RequestedLanguage::Japanese,
        RequestedLanguage::English,
        RequestedLanguage::Bangla,
    ]
    .into_iter()
    .find(|language| language.aliases().iter().any(|alias| lower.contains(alias)))
}

/// Returns whether provider-supplied title text hints at the requested language.
/// This is a title/filename hint only; it does not verify embedded audio tracks.
pub fn title_language_hint_matches(title: &str, requested: RequestedLanguage) -> bool {
    language_from_provider_text(title) == Some(requested)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_supported_language_suffixes() {
        for (query, title, language) in [
            ("Naruto Hindi", "Naruto", RequestedLanguage::Hindi),
            (
                "Naruto Shippuden Hindi Dub",
                "Naruto Shippuden",
                RequestedLanguage::Hindi,
            ),
            (
                "Naruto Japanese Audio",
                "Naruto",
                RequestedLanguage::Japanese,
            ),
            ("Naruto English", "Naruto", RequestedLanguage::English),
            ("Naruto Bangla", "Naruto", RequestedLanguage::Bangla),
        ] {
            let parsed = parse_query(query);
            assert_eq!(parsed.title, title);
            assert_eq!(parsed.language, Some(language));
        }
    }

    #[test]
    fn does_not_strip_language_like_title_words() {
        let parsed = parse_query("The English");
        assert_eq!(parsed.title, "The English");
        assert_eq!(parsed.language, None);
        assert_eq!(parse_query("English").language, None);
    }

    #[test]
    fn title_language_hints_are_not_audio_verification() {
        assert!(title_language_hint_matches(
            "Naruto Shippuden [Hindi Dub]",
            RequestedLanguage::Hindi
        ));
        assert!(!title_language_hint_matches(
            "Naruto Shippuden",
            RequestedLanguage::Hindi
        ));
        assert!(!title_language_hint_matches(
            "Naruto Shippuden [English Dub]",
            RequestedLanguage::Hindi
        ));
    }

    #[test]
    fn only_explicit_provider_language_markers_are_labeled() {
        assert_eq!(
            language_from_provider_text("Naruto [Hindi] S1-S2"),
            Some(RequestedLanguage::Hindi)
        );
        assert_eq!(language_from_provider_text("Naruto 2002"), None);
    }
}
