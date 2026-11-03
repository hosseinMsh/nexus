//! Automation rules for intelligent download categorization, destination routing, and transfer policy overrides.

use serde::{Deserialize, Serialize};
use url::Url;
use uuid::Uuid;

/// Unique identifier for an automation rule.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RuleId(pub Uuid);

impl Default for RuleId {
    fn default() -> Self {
        Self::new()
    }
}

impl RuleId {
    #[must_use]
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl std::fmt::Display for RuleId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Matching conditions for triggering an automation rule.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct RuleConditions {
    pub url_contains: Option<String>,
    pub file_extensions: Vec<String>,
    pub domains: Vec<String>,
    pub min_size: Option<u64>,
    pub max_size: Option<u64>,
}

impl RuleConditions {
    /// Evaluates whether the given download candidate satisfies all specified conditions.
    #[must_use]
    pub fn matches(&self, url: &Url, filename: Option<&str>, size: Option<u64>) -> bool {
        // 1. Check URL substring
        if let Some(ref pattern) = self.url_contains {
            if !url.as_str().contains(pattern) {
                return false;
            }
        }

        // 2. Check domains
        if !self.domains.is_empty() {
            let host = url.host_str().unwrap_or("");
            if !self
                .domains
                .iter()
                .any(|d| host == d || host.ends_with(&format!(".{d}")))
            {
                return false;
            }
        }

        // 3. Check file extensions (supports compound extensions like .tar.gz or simple .iso)
        if !self.file_extensions.is_empty() {
            let check_name =
                filename.or_else(|| url.path_segments().and_then(|mut s| s.next_back()));

            match check_name {
                Some(name) => {
                    let name_lower = name.to_ascii_lowercase();
                    let matches_any = self.file_extensions.iter().any(|target| {
                        let t = target.trim_start_matches('.').to_ascii_lowercase();
                        let dot_target = format!(".{t}");
                        name_lower.ends_with(&dot_target)
                    });
                    if !matches_any {
                        return false;
                    }
                }
                None => return false,
            }
        }

        // 4. Check min/max size
        if let Some(actual_size) = size {
            if let Some(min) = self.min_size {
                if actual_size < min {
                    return false;
                }
            }
            if let Some(max) = self.max_size {
                if actual_size > max {
                    return false;
                }
            }
        }

        true
    }
}

/// Actions executed when an automation rule matches.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct RuleActions {
    pub category: Option<String>,
    pub destination_dir: Option<String>,
    pub speed_limit_bps: Option<u64>,
    pub max_connections: Option<u32>,
    pub priority: Option<u8>,
}

/// Complete automation rule entity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AutomationRule {
    pub id: RuleId,
    pub name: String,
    pub priority: u32, // Higher number = higher precedence
    pub enabled: bool,
    pub conditions: RuleConditions,
    pub actions: RuleActions,
}

/// Output result of applying active rules against a download candidate.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct AppliedRuleResult {
    pub matched_rule_id: Option<RuleId>,
    pub category: Option<String>,
    pub destination_dir: Option<String>,
    pub speed_limit_bps: Option<u64>,
    pub max_connections: Option<u32>,
    pub priority: Option<u8>,
}

/// Evaluator and repository of automation rules.
#[derive(Debug, Default)]
pub struct RuleEngine {
    rules: std::sync::RwLock<Vec<AutomationRule>>,
}

impl RuleEngine {
    #[must_use]
    pub fn new() -> Self {
        Self {
            rules: std::sync::RwLock::new(Vec::new()),
        }
    }

    /// Adds a new automation rule, keeping the list sorted by priority descending.
    pub fn add_rule(&self, rule: AutomationRule) {
        let mut rules = self.rules.write().unwrap();
        rules.push(rule);
        rules.sort_by_key(|a| std::cmp::Reverse(a.priority));
    }

    /// Lists all registered automation rules.
    #[must_use]
    pub fn list_rules(&self) -> Vec<AutomationRule> {
        self.rules.read().unwrap().clone()
    }

    /// Evaluates a candidate against all active rules and returns the combined or first matching action.
    #[must_use]
    pub fn evaluate(
        &self,
        url: &Url,
        filename: Option<&str>,
        size: Option<u64>,
    ) -> AppliedRuleResult {
        let mut result = AppliedRuleResult::default();
        let rules = self.rules.read().unwrap();

        for rule in rules.iter().filter(|r| r.enabled) {
            if rule.conditions.matches(url, filename, size) {
                if result.matched_rule_id.is_none() {
                    result.matched_rule_id = Some(rule.id);
                }
                if result.category.is_none() {
                    result.category = rule.actions.category.clone();
                }
                if result.destination_dir.is_none() {
                    result.destination_dir = rule.actions.destination_dir.clone();
                }
                if result.speed_limit_bps.is_none() {
                    result.speed_limit_bps = rule.actions.speed_limit_bps;
                }
                if result.max_connections.is_none() {
                    result.max_connections = rule.actions.max_connections;
                }
                if result.priority.is_none() {
                    result.priority = rule.actions.priority;
                }
            }
        }

        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rule_matching_extensions_and_domain() {
        let engine = RuleEngine::new();

        let iso_rule = AutomationRule {
            id: RuleId::new(),
            name: "Linux ISO Categorizer".to_string(),
            priority: 10,
            enabled: true,
            conditions: RuleConditions {
                url_contains: None,
                file_extensions: vec!["iso".to_string(), "img".to_string()],
                domains: vec!["archlinux.org".to_string(), "ubuntu.com".to_string()],
                min_size: Some(100_000_000), // > 100 MB
                max_size: None,
            },
            actions: RuleActions {
                category: Some("Operating Systems".to_string()),
                destination_dir: Some("/downloads/os".to_string()),
                speed_limit_bps: None,
                max_connections: Some(8),
                priority: Some(3),
            },
        };

        engine.add_rule(iso_rule);

        // Matching candidate
        let url = Url::parse("https://geo.mirror.archlinux.org/iso/archlinux-2026.03.iso").unwrap();
        let res = engine.evaluate(&url, Some("archlinux-2026.03.iso"), Some(850_000_000));
        assert_eq!(res.category, Some("Operating Systems".to_string()));
        assert_eq!(res.destination_dir, Some("/downloads/os".to_string()));
        assert_eq!(res.max_connections, Some(8));

        // Non-matching size candidate (too small)
        let res_small = engine.evaluate(&url, Some("archlinux-2026.03.iso"), Some(50_000_000));
        assert_eq!(res_small.category, None);

        // Non-matching domain
        let other_url = Url::parse("https://random-site.net/file.iso").unwrap();
        let res_other = engine.evaluate(&other_url, Some("file.iso"), Some(500_000_000));
        assert_eq!(res_other.category, None);
    }
}
