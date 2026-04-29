#![allow(missing_docs)]

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use mempalace_core::DrawerRecord;
use thiserror::Error;
use time::format_description::FormatItem;
use time::macros::format_description;

pub use mempalace_core as core;

const DEFAULT_WAKE_UP_MAX_DRAWERS: usize = 15;
const DEFAULT_WAKE_UP_MAX_CHARS: usize = 3_200;
const QUOTE_LIMIT: usize = 55;
const TOPIC_LIMIT: usize = 3;
const ENTITY_LIMIT: usize = 3;
const EMOTION_LIMIT: usize = 3;
const FLAG_LIMIT: usize = 3;

const DECISION_WORDS: &[&str] = &[
    "decided",
    "because",
    "instead",
    "prefer",
    "switched",
    "chose",
    "realized",
    "important",
    "key",
    "critical",
    "discovered",
    "learned",
    "conclusion",
    "solution",
    "reason",
    "why",
    "breakthrough",
    "insight",
];

const EMOTION_SIGNALS: &[(&str, &str)] = &[
    ("decided", "determ"),
    ("prefer", "convict"),
    ("worried", "anx"),
    ("excited", "excite"),
    ("frustrated", "frust"),
    ("confused", "confuse"),
    ("love", "love"),
    ("hate", "rage"),
    ("hope", "hope"),
    ("fear", "fear"),
    ("trust", "trust"),
    ("happy", "joy"),
    ("sad", "grief"),
    ("surprised", "surprise"),
    ("grateful", "grat"),
    ("curious", "curious"),
    ("wonder", "wonder"),
    ("anxious", "anx"),
    ("relieved", "relief"),
    ("satisf", "satis"),
    ("disappoint", "grief"),
    ("concern", "anx"),
];

const FLAG_SIGNALS: &[(&str, &str)] = &[
    ("decided", "DECISION"),
    ("chose", "DECISION"),
    ("switched", "DECISION"),
    ("migrated", "DECISION"),
    ("replaced", "DECISION"),
    ("instead of", "DECISION"),
    ("because", "DECISION"),
    ("founded", "ORIGIN"),
    ("created", "ORIGIN"),
    ("started", "ORIGIN"),
    ("born", "ORIGIN"),
    ("launched", "ORIGIN"),
    ("first time", "ORIGIN"),
    ("core", "CORE"),
    ("fundamental", "CORE"),
    ("essential", "CORE"),
    ("principle", "CORE"),
    ("belief", "CORE"),
    ("always", "CORE"),
    ("never forget", "CORE"),
    ("turning point", "PIVOT"),
    ("changed everything", "PIVOT"),
    ("realized", "PIVOT"),
    ("breakthrough", "PIVOT"),
    ("epiphany", "PIVOT"),
    ("api", "TECHNICAL"),
    ("database", "TECHNICAL"),
    ("architecture", "TECHNICAL"),
    ("deploy", "TECHNICAL"),
    ("infrastructure", "TECHNICAL"),
    ("algorithm", "TECHNICAL"),
    ("framework", "TECHNICAL"),
    ("server", "TECHNICAL"),
    ("config", "TECHNICAL"),
];

const STOP_WORDS: &[&str] = &[
    "the", "a", "an", "is", "are", "was", "were", "be", "been", "being", "have", "has", "had",
    "do", "does", "did", "will", "would", "could", "should", "may", "might", "shall", "can", "to",
    "of", "in", "for", "on", "with", "at", "by", "from", "as", "into", "about", "between",
    "through", "during", "before", "after", "above", "below", "up", "down", "out", "off", "over",
    "under", "again", "further", "then", "once", "here", "there", "when", "where", "why", "how",
    "all", "each", "every", "both", "few", "more", "most", "other", "some", "such", "no", "nor",
    "not", "only", "own", "same", "so", "than", "too", "very", "just", "don", "now", "and", "but",
    "or", "if", "while", "that", "this", "these", "those", "it", "its", "i", "we", "you", "he",
    "she", "they", "me", "him", "her", "us", "them", "my", "your", "his", "our", "their", "what",
    "which", "who", "whom", "also", "much", "many", "like", "because", "since", "get", "got",
    "use", "used", "using", "make", "made", "thing", "things", "way", "well", "really", "want",
    "need",
];

static DATE_FORMAT: &[FormatItem<'static>] = format_description!("[year]-[month]-[day]");

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceMetadata<'a> {
    pub source_file: Option<&'a str>,
    pub wing: Option<&'a str>,
    pub room: Option<&'a str>,
    pub date: Option<&'a str>,
}

impl<'a> Default for SourceMetadata<'a> {
    fn default() -> Self {
        Self { source_file: None, wing: None, room: None, date: None }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct CompressionStats {
    pub original_tokens: usize,
    pub compressed_tokens: usize,
    pub ratio: f64,
    pub original_chars: usize,
    pub compressed_chars: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WakeUpAaaKConfig {
    pub max_drawers: usize,
    pub max_chars: usize,
}

impl Default for WakeUpAaaKConfig {
    fn default() -> Self {
        Self { max_drawers: DEFAULT_WAKE_UP_MAX_DRAWERS, max_chars: DEFAULT_WAKE_UP_MAX_CHARS }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReverseParsingSupport {
    DeferredForV1 { reason: &'static str },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedDialect;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ParseError {
    #[error("AAAK reverse parsing is deferred for Rust v1: {reason}")]
    DeferredForV1 { reason: &'static str },
}

pub const REVERSE_PARSING_DECISION: ReverseParsingSupport = ReverseParsingSupport::DeferredForV1 {
    reason: "Phase 7 keeps Rust AAAK write-only for v1; decode support is intentionally deferred until a concrete product consumer requires loss-aware reverse parsing.",
};

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Dialect {
    entity_codes: BTreeMap<String, String>,
    skip_names: BTreeSet<String>,
}

impl Dialect {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_entities<I, K, V>(entities: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<str>,
        V: AsRef<str>,
    {
        let mut dialect = Self::new();
        for (name, code) in entities {
            let name = name.as_ref().to_lowercase();
            let code = code.as_ref().to_owned();
            dialect.entity_codes.insert(name, code);
        }
        dialect
    }

    pub fn with_skip_names<I, S>(mut self, skip_names: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.skip_names = skip_names.into_iter().map(|name| name.as_ref().to_lowercase()).collect();
        self
    }

    pub fn compress(&self, text: &str, metadata: &SourceMetadata<'_>) -> String {
        let entities = self.detect_entities(text);
        let entity_str = if entities.is_empty() { "???".to_owned() } else { entities.join("+") };

        let topics = self.extract_topics(text);
        let topic_str = if topics.is_empty() { "misc".to_owned() } else { topics.join("_") };

        let quote = self.extract_key_sentence(text);
        let emotions = self.detect_emotions(text);
        let flags = self.detect_flags(text);

        let mut lines = Vec::new();
        if metadata.source_file.is_some() || metadata.wing.is_some() {
            let source = source_stem(metadata.source_file.unwrap_or("?"));
            let header = [
                metadata.wing.unwrap_or("?"),
                metadata.room.unwrap_or("?"),
                metadata.date.unwrap_or("?"),
                source.as_str(),
            ]
            .join("|");
            lines.push(header);
        }

        let mut parts = vec![format!("0:{entity_str}"), topic_str];
        if !quote.is_empty() {
            parts.push(format!("\"{}\"", sanitize_aaak_field(&quote)));
        }
        if !emotions.is_empty() {
            parts.push(emotions.join("+"));
        }
        if !flags.is_empty() {
            parts.push(flags.join("+"));
        }
        lines.push(parts.join("|"));
        lines.join("\n")
    }

    pub fn compression_stats(&self, original_text: &str, compressed: &str) -> CompressionStats {
        let original_tokens = count_tokens(original_text);
        let compressed_tokens = count_tokens(compressed);
        CompressionStats {
            original_tokens,
            compressed_tokens,
            ratio: original_tokens as f64 / compressed_tokens.max(1) as f64,
            original_chars: char_count(original_text),
            compressed_chars: char_count(compressed),
        }
    }

    pub fn reverse_parsing_support(&self) -> ReverseParsingSupport {
        REVERSE_PARSING_DECISION
    }

    pub fn decode(&self, _dialect_text: &str) -> Result<ParsedDialect, ParseError> {
        match REVERSE_PARSING_DECISION {
            ReverseParsingSupport::DeferredForV1 { reason } => {
                Err(ParseError::DeferredForV1 { reason })
            }
        }
    }

    pub fn render_wake_up_aaak(
        &self,
        identity: &str,
        drawers: &[DrawerRecord],
        config: &WakeUpAaaKConfig,
    ) -> String {
        let identity = identity.trim();
        let mut ordered = drawers.to_vec();
        order_drawers(&mut ordered);

        if ordered.is_empty() {
            return format!("{identity}\n\n## L1 — AAAK STORY\nNo memories yet.");
        }

        let mut grouped = BTreeMap::<String, Vec<DrawerRecord>>::new();
        for record in ordered.into_iter().take(config.max_drawers) {
            grouped.entry(record.room.as_str().to_owned()).or_default().push(record);
        }

        let mut lines = Vec::new();
        let mut rendered_chars = 0;
        push_line(&mut lines, &mut rendered_chars, identity.to_owned());
        push_line(&mut lines, &mut rendered_chars, String::new());
        push_line(&mut lines, &mut rendered_chars, "## L1 — AAAK STORY".to_owned());

        for (room, records) in grouped {
            let room_lines = vec![String::new(), format!("[{room}]")];
            let mut room_has_entries = false;

            for record in records {
                let rendered_date = record.date.and_then(|date| date.format(DATE_FORMAT).ok());
                let header = self.compress(
                    &record.content,
                    &SourceMetadata {
                        source_file: Some(&record.source_file),
                        wing: Some(record.wing.as_str()),
                        room: Some(record.room.as_str()),
                        date: rendered_date.as_deref(),
                    },
                );
                let entry = format!("  - {}", header.replace('\n', " :: "));
                let room_lines_chars = if room_has_entries {
                    0
                } else {
                    appended_char_count(lines.len(), &room_lines)
                };
                let entry_chars = appended_char_count(
                    lines.len() + if room_has_entries { 0 } else { room_lines.len() },
                    std::slice::from_ref(&entry),
                );

                if rendered_chars + room_lines_chars + entry_chars > config.max_chars {
                    let ellipsis = "  ... (more in L3 search)".to_owned();
                    let mut truncated = lines.clone();
                    let mut truncated_chars = rendered_chars;
                    if !room_has_entries {
                        let room_with_ellipsis_chars = room_lines_chars
                            + appended_char_count(
                                lines.len() + room_lines.len(),
                                std::slice::from_ref(&ellipsis),
                            );
                        if truncated_chars + room_with_ellipsis_chars <= config.max_chars {
                            for room_line in &room_lines {
                                push_line(&mut truncated, &mut truncated_chars, room_line.clone());
                            }
                        }
                    }
                    if truncated_chars
                        + appended_char_count(truncated.len(), std::slice::from_ref(&ellipsis))
                        <= config.max_chars
                    {
                        push_line(&mut truncated, &mut truncated_chars, ellipsis);
                        lines = truncated;
                    }
                    return lines.join("\n");
                }

                if !room_has_entries {
                    for room_line in &room_lines {
                        push_line(&mut lines, &mut rendered_chars, room_line.clone());
                    }
                    room_has_entries = true;
                }

                push_line(&mut lines, &mut rendered_chars, entry);
            }
        }

        lines.join("\n")
    }

    fn detect_entities(&self, text: &str) -> Vec<String> {
        let text_lower = text.to_lowercase();
        let mut found = Vec::new();

        for (name, code) in &self.entity_codes {
            if self.skip_names.iter().any(|skip| name.contains(skip)) {
                continue;
            }
            if text_lower.contains(name) && !found.contains(code) {
                found.push(code.clone());
                if found.len() >= ENTITY_LIMIT {
                    break;
                }
            }
        }
        if !found.is_empty() {
            return found;
        }

        for (index, word) in text.split_whitespace().enumerate() {
            let clean = word.chars().filter(|ch| ch.is_alphanumeric()).collect::<String>();
            if clean.len() < 2 || index == 0 {
                continue;
            }
            if !is_fallback_entity_candidate(&clean) || is_stop_word(&clean.to_lowercase()) {
                continue;
            }

            let clean_lower = clean.to_lowercase();
            let code = clean.chars().take(3).collect::<String>().to_uppercase();
            if self.skip_names.iter().any(|skip| clean_lower.contains(skip)) {
                continue;
            }
            if !found.contains(&code) {
                found.push(code);
            }
            if found.len() >= ENTITY_LIMIT {
                break;
            }
        }

        found
    }

    fn detect_emotions(&self, text: &str) -> Vec<&'static str> {
        let text_lower = text.to_lowercase();
        let mut detected = Vec::new();
        for (keyword, code) in EMOTION_SIGNALS {
            if text_lower.contains(keyword) && !detected.contains(code) {
                detected.push(*code);
            }
            if detected.len() >= EMOTION_LIMIT {
                break;
            }
        }
        detected
    }

    fn detect_flags(&self, text: &str) -> Vec<&'static str> {
        let text_lower = text.to_lowercase();
        let mut detected = Vec::new();
        for (keyword, flag) in FLAG_SIGNALS {
            if text_lower.contains(keyword) && !detected.contains(flag) {
                detected.push(*flag);
            }
            if detected.len() >= FLAG_LIMIT {
                break;
            }
        }
        detected
    }

    fn extract_topics(&self, text: &str) -> Vec<String> {
        let tokens = tokenize_words(text);
        let mut scores = BTreeMap::<String, i32>::new();

        for token in &tokens {
            let lower = token.to_lowercase();
            if lower.len() < 3 || is_stop_word(&lower) {
                continue;
            }
            *scores.entry(lower).or_default() += 1;
        }

        for token in &tokens {
            let lower = token.to_lowercase();
            if is_stop_word(&lower) {
                continue;
            }
            if let Some(score) = scores.get_mut(&lower) {
                if token.chars().next().is_some_and(|ch| ch.is_ascii_uppercase()) {
                    *score += 2;
                }
                if token.contains('_')
                    || token.contains('-')
                    || token.chars().skip(1).any(|ch| ch.is_ascii_uppercase())
                {
                    *score += 2;
                }
            }
        }

        let mut ranked = scores.into_iter().collect::<Vec<_>>();
        ranked.sort_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0)));
        ranked.into_iter().take(TOPIC_LIMIT).map(|(word, _)| word).collect()
    }

    fn extract_key_sentence(&self, text: &str) -> String {
        let sentences = split_sentences(text);
        let mut scored = sentences
            .into_iter()
            .enumerate()
            .filter_map(|(index, sentence)| {
                let sentence = collapse_whitespace(&sentence);
                if char_count(&sentence) <= 10 {
                    return None;
                }

                let sentence_lower = sentence.to_lowercase();
                let mut score = 0_i32;
                for word in DECISION_WORDS {
                    if sentence_lower.contains(word) {
                        score += 2;
                    }
                }
                if char_count(&sentence) < 80 {
                    score += 1;
                }
                if char_count(&sentence) < 40 {
                    score += 1;
                }
                if char_count(&sentence) > 150 {
                    score -= 2;
                }

                Some((score, index, sentence))
            })
            .collect::<Vec<_>>();

        scored.sort_by(|left, right| right.0.cmp(&left.0).then_with(|| left.1.cmp(&right.1)));

        let mut best =
            scored.into_iter().next().map(|(_, _, sentence)| sentence).unwrap_or_default();
        if char_count(&best) > QUOTE_LIMIT {
            best = truncate_chars(&best, QUOTE_LIMIT.saturating_sub(3));
            best.push_str("...");
        }
        best
    }
}

pub fn count_tokens(text: &str) -> usize {
    char_count(text) / 3
}

fn order_drawers(drawers: &mut [DrawerRecord]) {
    drawers.sort_by(|left, right| {
        right
            .importance
            .or(right.emotional_weight)
            .or(right.weight)
            .unwrap_or(3.0)
            .partial_cmp(&left.importance.or(left.emotional_weight).or(left.weight).unwrap_or(3.0))
            .unwrap_or(Ordering::Equal)
            .then_with(|| left.room.as_str().cmp(right.room.as_str()))
            .then_with(|| compare_option_dates(right.date, left.date))
            .then_with(|| right.filed_at.cmp(&left.filed_at))
            .then_with(|| source_label(&left.source_file).cmp(source_label(&right.source_file)))
            .then_with(|| left.chunk_index.cmp(&right.chunk_index))
            .then_with(|| left.id.as_str().cmp(right.id.as_str()))
    });
}

fn compare_option_dates(left: Option<time::Date>, right: Option<time::Date>) -> Ordering {
    match (left, right) {
        (Some(left), Some(right)) => left.cmp(&right),
        (Some(_), None) => Ordering::Greater,
        (None, Some(_)) => Ordering::Less,
        (None, None) => Ordering::Equal,
    }
}

fn tokenize_words(text: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut current = String::new();

    for ch in text.chars() {
        let valid = if current.is_empty() {
            ch.is_alphanumeric()
        } else {
            ch.is_alphanumeric() || ch == '_' || ch == '-'
        };

        if valid {
            current.push(ch);
        } else if !current.is_empty() {
            tokens.push(std::mem::take(&mut current));
        }
    }

    if !current.is_empty() {
        tokens.push(current);
    }

    tokens
}

fn split_sentences(text: &str) -> Vec<String> {
    let mut sentences = Vec::new();
    let mut current = String::new();
    let chars = text.chars().collect::<Vec<_>>();

    for (index, ch) in chars.iter().copied().enumerate() {
        if ch == '\n' {
            if !current.trim().is_empty() {
                sentences.push(current.trim().to_owned());
            }
            current.clear();
            continue;
        }

        current.push(ch);

        if matches!(ch, '.' | '!' | '?') && sentence_boundary(&chars, index) {
            if !current.trim().is_empty() {
                sentences.push(current.trim().to_owned());
            }
            current.clear();
        } else {
            continue;
        }
    }

    if !current.trim().is_empty() {
        sentences.push(current.trim().to_owned());
    }

    sentences
}

fn collapse_whitespace(text: &str) -> String {
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn truncate_chars(text: &str, limit: usize) -> String {
    text.chars().take(limit).collect()
}

fn sanitize_aaak_field(text: &str) -> String {
    text.replace('|', "/").replace('"', "'")
}

fn push_line(lines: &mut Vec<String>, rendered_chars: &mut usize, line: String) {
    if !lines.is_empty() {
        *rendered_chars += 1;
    }
    *rendered_chars += char_count(&line);
    lines.push(line);
}

fn appended_char_count(existing_len: usize, appended: &[String]) -> usize {
    appended
        .iter()
        .enumerate()
        .map(|(index, line)| char_count(line) + if existing_len == 0 && index == 0 { 0 } else { 1 })
        .sum()
}

fn char_count(text: &str) -> usize {
    text.chars().count()
}

fn is_stop_word(word: &str) -> bool {
    STOP_WORDS.contains(&word)
}

fn is_fallback_entity_candidate(word: &str) -> bool {
    let mut chars = word.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    if !first.is_uppercase() {
        return false;
    }
    let rest = chars.collect::<Vec<_>>();
    if rest.is_empty() {
        return false;
    }

    let has_lowercase = rest.iter().any(|ch| ch.is_lowercase());
    let has_uppercase = rest.iter().any(|ch| ch.is_uppercase());

    if has_lowercase && has_uppercase {
        return false;
    }

    rest.iter().all(|ch| ch.is_lowercase() || ch.is_uppercase() || ch.is_numeric())
}

fn sentence_boundary(chars: &[char], index: usize) -> bool {
    let Some(current) = chars.get(index) else {
        return false;
    };
    if !matches!(current, '.' | '!' | '?') {
        return false;
    }

    let previous = index.checked_sub(1).and_then(|value| chars.get(value)).copied();
    let next = chars.get(index + 1).copied();

    if matches!((previous, next), (Some(left), Some(right)) if left.is_alphanumeric() && right.is_alphanumeric())
    {
        return false;
    }

    next.is_none_or(|ch| ch.is_whitespace())
}

fn source_label(source_file: &str) -> &str {
    Path::new(source_file).file_name().and_then(|value| value.to_str()).unwrap_or(source_file)
}

fn source_stem(source_file: &str) -> String {
    Path::new(source_file)
        .file_stem()
        .and_then(|value| value.to_str())
        .unwrap_or(source_file)
        .to_owned()
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::{
        CompressionStats, Dialect, ParseError, ReverseParsingSupport, SourceMetadata,
        WakeUpAaaKConfig, char_count, count_tokens,
    };
    use mempalace_core::{DrawerId, DrawerRecord, RoomId, WingId};
    use serde_json::Value;
    use std::collections::BTreeSet;
    use std::fs;
    use std::path::PathBuf;
    use time::macros::{date, datetime};

    fn fixture_path(relative: &str) -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../../").join(relative)
    }

    fn sample_record(
        id: &str,
        wing: &str,
        room: &str,
        source_file: &str,
        content: &str,
        importance: Option<f32>,
        filed_at: time::OffsetDateTime,
    ) -> DrawerRecord {
        DrawerRecord {
            id: DrawerId::new(id).unwrap(),
            wing: WingId::new(wing).unwrap(),
            room: RoomId::new(room).unwrap(),
            hall: Some("facts".to_owned()),
            date: Some(date!(2026 - 04 - 11)),
            source_file: source_file.to_owned(),
            chunk_index: 0,
            ingest_mode: "projects".to_owned(),
            extract_mode: Some("full".to_owned()),
            added_by: "tester".to_owned(),
            filed_at,
            importance,
            emotional_weight: None,
            weight: None,
            content: content.to_owned(),
            content_hash: format!("hash-{id}"),
            embedding: vec![0.0; 384],
        }
    }

    fn sample_drawers() -> Vec<DrawerRecord> {
        vec![
            sample_record(
                "wing_team/auth-migration/0001",
                "wing_team",
                "auth-migration",
                "fixtures/team.txt",
                "The team decided the auth migration must preserve CLI and MCP parity.",
                Some(10.0),
                datetime!(2026-04-11 09:45:00 UTC),
            ),
            sample_record(
                "wing_code/auth-migration/0002",
                "wing_code",
                "auth-migration",
                "fixtures/code.txt",
                "Code notes: auth migration keeps search filter semantics exact while storage changes underneath.",
                Some(9.0),
                datetime!(2026-04-11 09:30:00 UTC),
            ),
            sample_record(
                "wing_code/backend/0003",
                "wing_code",
                "backend",
                "fixtures/auth.py",
                "We switched from opaque session blobs to signed session tokens because the old format made auth debugging painful.",
                Some(8.0),
                datetime!(2026-04-11 09:00:00 UTC),
            ),
        ]
    }

    #[test]
    fn compress_matches_phase0_fixture() {
        let fixture =
            fs::read_to_string(fixture_path("tests/fixtures/phase0/goldens/aaak.json")).unwrap();
        let expected: Value = serde_json::from_str(&fixture).unwrap();
        let source = expected.get("source").and_then(Value::as_str).unwrap();
        let rendered = expected.get("rendered").and_then(Value::as_str).unwrap();
        let stats = expected.get("stats").unwrap();

        let dialect = Dialect::new();
        let compressed = dialect.compress(
            source,
            &SourceMetadata {
                source_file: None,
                wing: Some("wing_code"),
                room: Some("planning"),
                date: None,
            },
        );

        assert_eq!(compressed, rendered);
        assert_eq!(
            dialect.compression_stats(source, &compressed),
            CompressionStats {
                original_tokens: stats.get("original_tokens").and_then(Value::as_u64).unwrap()
                    as usize,
                compressed_tokens: stats.get("compressed_tokens").and_then(Value::as_u64).unwrap()
                    as usize,
                ratio: stats.get("ratio").and_then(Value::as_f64).unwrap(),
                original_chars: stats.get("original_chars").and_then(Value::as_u64).unwrap()
                    as usize,
                compressed_chars: stats.get("compressed_chars").and_then(Value::as_u64).unwrap()
                    as usize,
            }
        );
    }

    #[test]
    fn compress_is_deterministic_across_repeated_runs() {
        let dialect = Dialect::new();
        let text = "We decided to keep the CLI stable because drift would make parity impossible.";
        let metadata = SourceMetadata {
            source_file: Some("notes.md"),
            wing: Some("wing_code"),
            room: Some("planning"),
            date: Some("2026-04-11"),
        };

        let first = dialect.compress(text, &metadata);
        let second = dialect.compress(text, &metadata);
        let third = dialect.compress(text, &metadata);

        assert_eq!(first, second);
        assert_eq!(second, third);
    }

    #[test]
    fn compress_truncates_long_quotes_and_preserves_budget_signal() {
        let dialect = Dialect::new();
        let original = "We decided to preserve the search formatting because the exact output is the only trustworthy product contract while the storage layer changes underneath and every fixture must stay reproducible for CI review. ".repeat(64);
        let compressed = dialect.compress(&original, &SourceMetadata::default());

        assert!(compressed.contains("...\"|") || compressed.ends_with("...\""));
        assert!(count_tokens(&compressed) <= 64);
        assert!(count_tokens(&compressed) < count_tokens(&original));
        assert!(compressed.lines().count() == 1);
    }

    #[test]
    fn compress_sanitizes_quotes_and_pipes() {
        let dialect = Dialect::new();
        let compressed = dialect.compress(
            "We decided \"quoted | structured\" output should stay readable.",
            &SourceMetadata::default(),
        );

        assert!(compressed.contains("'quoted / structured'"));
        assert!(!compressed.contains("\"quoted | structured\""));
    }

    #[test]
    fn compress_uses_registered_entities_case_insensitively_and_caps_output() {
        let dialect = Dialect::with_entities([
            ("alice", "ALC"),
            ("github", "GIT"),
            ("postgres", "PGS"),
            ("tokio", "TOK"),
        ]);
        let compressed = dialect.compress(
            "alice paired GitHub with Postgres while tokio handled the runtime.",
            &SourceMetadata::default(),
        );
        let entity_field = compressed.split('|').next().unwrap();

        assert_eq!(entity_field, "0:ALC+GIT+PGS");
    }

    #[test]
    fn compress_respects_skip_names_for_registered_entities() {
        let dialect = Dialect::with_entities([("alice", "ALC"), ("github", "GIT")])
            .with_skip_names(["github"]);
        let compressed = dialect
            .compress("alice kept GitHub in the release checklist.", &SourceMetadata::default());

        assert!(compressed.starts_with("0:ALC|"));
        assert!(!compressed.contains("GIT"));
    }

    #[test]
    fn compress_detects_uppercase_acronyms_in_fallback_entities() {
        let dialect = Dialect::new();
        let compressed = dialect.compress(
            "We kept MCP and CLI parity while the API stayed stable.",
            &SourceMetadata::default(),
        );
        let entity_field = compressed.split('|').next().unwrap();
        let entities = entity_field.trim_start_matches("0:").split('+').collect::<BTreeSet<_>>();

        assert_eq!(entities, BTreeSet::from(["API", "CLI", "MCP"]));
    }

    #[test]
    fn compress_preserves_alphanumeric_and_non_ascii_topics() {
        let dialect = Dialect::new();
        let compressed = dialect.compress(
            "München v1_0 stayed online after port 8080 moved behind the edge.",
            &SourceMetadata::default(),
        );
        let topic_field = compressed.split('|').nth(1).unwrap();
        let topics = topic_field.split('_').collect::<Vec<_>>();

        assert!(topics.contains(&"münchen"));
        assert!(topics.contains(&"8080"));
        assert!(topic_field.contains("v1_0"));
    }

    #[test]
    fn compress_keeps_decimal_versions_inside_a_sentence() {
        let dialect = Dialect::new();
        let compressed = dialect.compress(
            "We shipped v1.0 because the old fallback kept breaking users.",
            &SourceMetadata::default(),
        );

        assert!(compressed.contains("\"We shipped v1.0 because the old fallback kept brea...\""));
    }

    #[test]
    fn render_wake_up_aaak_is_grouped_and_stable() {
        let dialect = Dialect::new();
        let rendered = dialect.render_wake_up_aaak(
            "## L0 — IDENTITY\nReady.",
            &sample_drawers(),
            &WakeUpAaaKConfig::default(),
        );

        assert!(rendered.starts_with("## L0 — IDENTITY\nReady.\n\n## L1 — AAAK STORY"));
        let auth_index = rendered.find("[auth-migration]").unwrap();
        let backend_index = rendered.find("[backend]").unwrap();
        assert!(auth_index < backend_index);
        assert!(rendered.contains("wing_team|auth-migration|2026-04-11|team"));
        assert!(rendered.contains("\"The team decided the auth migration must preserve CLI...\""));
    }

    #[test]
    fn render_wake_up_aaak_truncates_without_emitting_orphan_room_headers() {
        let dialect = Dialect::new();
        let drawers = vec![
            sample_record(
                "wing_code/alpha/0001",
                "wing_code",
                "alpha",
                "fixtures/alpha.txt",
                "Tiny note.",
                Some(10.0),
                datetime!(2026-04-11 09:45:00 UTC),
            ),
            sample_record(
                "wing_code/beta/0002",
                "wing_code",
                "beta",
                "fixtures/beta.txt",
                "Second room should truncate before its first entry lands in the wake-up output.",
                Some(9.0),
                datetime!(2026-04-11 09:30:00 UTC),
            ),
        ];
        let rendered = dialect.render_wake_up_aaak(
            "## L0 — IDENTITY\nReady.",
            &drawers,
            &WakeUpAaaKConfig { max_drawers: 2, max_chars: 70 },
        );

        assert!(rendered.contains("[alpha]"));
        assert!(rendered.contains("... (more in L3 search)"));
        assert!(!rendered.contains("[beta]"));
    }

    #[test]
    fn render_wake_up_aaak_honors_full_output_budget() {
        let dialect = Dialect::new();
        let identity = "## L0 — IDENTITY\nReady.";
        let rendered = dialect.render_wake_up_aaak(
            identity,
            &sample_drawers(),
            &WakeUpAaaKConfig {
                max_drawers: 3,
                max_chars: char_count(
                    "## L0 — IDENTITY\nReady.\n\n## L1 — AAAK STORY\n\n[auth-migration]\n  ... (more in L3 search)",
                ),
            },
        );

        assert_eq!(char_count(&rendered), 81);
        assert_eq!(
            rendered,
            "## L0 — IDENTITY\nReady.\n\n## L1 — AAAK STORY\n\n[auth-migration]\n  ... (more in L3 search)"
        );
        assert!(!rendered.contains("wing_team|"));
    }

    #[test]
    fn render_wake_up_aaak_preserves_room_context_in_truncation_fallback() {
        let dialect = Dialect::new();
        let drawers = vec![sample_record(
            "wing_code/auth-migration/0001",
            "wing_code",
            "auth-migration",
            "fixtures/code.txt",
            "This entry is intentionally long enough that the AAAK output cannot fit once the entry itself is appended.",
            Some(10.0),
            datetime!(2026-04-11 09:45:00 UTC),
        )];
        let expected = "## L0 — IDENTITY\nReady.\n\n## L1 — AAAK STORY\n\n[auth-migration]\n  ... (more in L3 search)";

        let rendered = dialect.render_wake_up_aaak(
            "## L0 — IDENTITY\nReady.",
            &drawers,
            &WakeUpAaaKConfig { max_drawers: 1, max_chars: char_count(expected) },
        );

        assert_eq!(rendered, expected);
    }

    #[test]
    fn render_wake_up_aaak_is_deterministic_across_repeated_runs() {
        let dialect = Dialect::new();
        let identity = "## L0 — IDENTITY\nReady.";
        let config = WakeUpAaaKConfig::default();

        let first = dialect.render_wake_up_aaak(identity, &sample_drawers(), &config);
        let second = dialect.render_wake_up_aaak(identity, &sample_drawers(), &config);
        let third = dialect.render_wake_up_aaak(identity, &sample_drawers(), &config);

        assert_eq!(first, second);
        assert_eq!(second, third);
    }

    #[test]
    fn reverse_parsing_is_explicitly_deferred_for_v1() {
        let dialect = Dialect::new();

        assert!(matches!(
            dialect.reverse_parsing_support(),
            ReverseParsingSupport::DeferredForV1 { .. }
        ));
        assert!(matches!(dialect.decode("0:ALC|misc"), Err(ParseError::DeferredForV1 { .. })));
    }
}
