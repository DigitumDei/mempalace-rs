//! A2A Agent Card generation from palace identity, configured wings, and the capability list.
//!
//! Verified against the A2A v1.0.1 proto (`specification/a2a.proto` in `a2aproject/A2A` at tag
//! `v1.0.1`, lines ~361-455): `AgentCard`, `AgentProvider`, `AgentCapabilities`,
//! `AgentExtension`, `AgentSkill`, `AgentInterface`. Field names are rendered camelCase to match
//! protojson's default mapping.
//!
//! # What this module deliberately does not include
//!
//! The real `AgentCard` also carries `securitySchemes`, `securityRequirements`, `signatures`,
//! `documentationUrl`, and `iconUrl`. None of those have an obvious source in
//! `mempalace_storage`/`mempalace_federation` yet (no auth-scheme description, no signing key,
//! no docs/icon URL configured anywhere this crate can see), so per the "a smaller correct card
//! beats a larger invented one" instruction they are left out entirely rather than populated
//! with placeholder values. `AgentCard::supported_interfaces` is a required array in the real
//! spec (at least one entry, each needing a live URL) but this crate builds no HTTP surface
//! (Stage 5's open question 3 is explicitly unresolved) and cannot know the palace's own
//! endpoint address, so [`build_agent_card`] takes it as a caller-supplied, possibly-empty list.
//! See deviation entry 24 in `docs/Coordination-Phase-3-Design.md`.

use mempalace_federation::InfoResponse;
use serde::{Deserialize, Serialize};

/// A self-describing manifest for an agent, per A2A's `AgentCard`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentCard {
    /// Human-readable agent name.
    pub name: String,
    /// Human-readable description of the agent's purpose.
    pub description: String,
    /// Ordered list of supported interfaces; the first is preferred. Caller-supplied — see the
    /// module docs for why this crate cannot populate it itself.
    pub supported_interfaces: Vec<AgentInterface>,
    /// Service provider of the agent, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider: Option<AgentProvider>,
    /// Agent version string.
    pub version: String,
    /// A2A capability set supported by the agent.
    pub capabilities: AgentCapabilities,
    /// Input media types supported across all skills, unless overridden per skill.
    pub default_input_modes: Vec<String>,
    /// Output media types supported across all skills, unless overridden per skill.
    pub default_output_modes: Vec<String>,
    /// Skills this agent can perform.
    pub skills: Vec<AgentSkill>,
}

/// Service provider of an agent, per A2A's `AgentProvider`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentProvider {
    /// URL for the provider's website or relevant documentation.
    pub url: String,
    /// Name of the provider organization.
    pub organization: String,
}

/// Optional capabilities supported by an agent, per A2A's `AgentCapabilities`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentCapabilities {
    /// Whether the agent supports streaming responses. MemPalace coordination is poll-based
    /// (see the Phase 3 design's "no streaming or push" non-goal), so this is always `false`.
    pub streaming: bool,
    /// Whether the agent supports push notifications for asynchronous task updates. Same
    /// non-goal as `streaming`; always `false`.
    pub push_notifications: bool,
    /// Whether the agent supports an authenticated extended agent card. Not implemented;
    /// always `false`.
    pub extended_agent_card: bool,
    /// Protocol extensions supported by the agent. Always empty — MemPalace declares none.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub extensions: Vec<AgentExtension>,
}

/// A declared protocol extension, per A2A's `AgentExtension`. MemPalace declares none today;
/// this type exists so [`AgentCapabilities::extensions`] has a concrete element type ready for
/// when one is added.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentExtension {
    /// Unique URI identifying the extension.
    pub uri: String,
    /// Human-readable description of how this agent uses the extension.
    pub description: String,
    /// Whether a client must understand and comply with the extension's requirements.
    pub required: bool,
}

/// A distinct capability or function an agent can perform, per A2A's `AgentSkill`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentSkill {
    /// Unique identifier for the skill.
    pub id: String,
    /// Human-readable skill name.
    pub name: String,
    /// Detailed description of the skill.
    pub description: String,
    /// Keywords describing the skill's capabilities.
    pub tags: Vec<String>,
}

/// A target URL, transport, and protocol version for interacting with the agent, per A2A's
/// `AgentInterface`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentInterface {
    /// The URL where this interface is available.
    pub url: String,
    /// The protocol binding supported at this URL (e.g. `"JSONRPC"`, `"HTTP+JSON"`).
    pub protocol_binding: String,
    /// The version of the A2A protocol this interface exposes (e.g. `"1.0"`).
    pub protocol_version: String,
}

/// Inputs to [`build_agent_card`].
#[derive(Debug, Clone)]
pub struct AgentCardInputs<'a> {
    /// Human-readable agent (palace) name.
    pub name: &'a str,
    /// Human-readable description of the palace's purpose.
    pub description: &'a str,
    /// Agent (palace) version string.
    pub version: &'a str,
    /// Service provider, if any.
    pub provider: Option<AgentProvider>,
    /// Configured wings, normalised form (e.g. `"wing_myproject"`). One [`AgentSkill`] is
    /// generated per wing.
    pub wings: &'a [String],
    /// Server capability list and version info, as returned by the federation `/info` endpoint.
    pub info: &'a InfoResponse,
    /// Interfaces this card advertises. Caller-supplied — see the module docs for why this
    /// crate cannot determine its own endpoint address.
    pub interfaces: Vec<AgentInterface>,
}

/// Build an [`AgentCard`] describing this palace's coordination surface.
///
/// One [`AgentSkill`] is generated per entry in `inputs.wings`, tagged with
/// `inputs.info.capabilities` so a client can see which server-level features (e.g. `"kg"`,
/// `"changes"`) back that wing's coordination surface.
pub fn build_agent_card(inputs: &AgentCardInputs<'_>) -> AgentCard {
    let skills = inputs
        .wings
        .iter()
        .map(|wing| AgentSkill {
            id: format!("coordination:{wing}"),
            name: wing.clone(),
            description: format!(
                "Coordinate tasks in the `{wing}` wing via MemPalace's coordination surface."
            ),
            tags: inputs.info.capabilities.clone(),
        })
        .collect();
    AgentCard {
        name: inputs.name.to_owned(),
        description: inputs.description.to_owned(),
        supported_interfaces: inputs.interfaces.clone(),
        provider: inputs.provider.clone(),
        version: inputs.version.to_owned(),
        capabilities: AgentCapabilities::default(),
        default_input_modes: vec!["application/json".to_owned()],
        default_output_modes: vec!["application/json".to_owned()],
        skills,
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use mempalace_federation::{FEDERATION_API_VERSION, MaintenanceStatus};

    use super::*;

    fn sample_info() -> InfoResponse {
        InfoResponse {
            server_version: "2.0.0".to_owned(),
            federation_api_version: FEDERATION_API_VERSION,
            embedding_profile: "balanced".to_owned(),
            capabilities: vec!["drawers".to_owned(), "kg".to_owned()],
            maintenance_enabled: false,
            maintenance_background_enabled: false,
            maintenance_idle_secs: 0,
            maintenance_last_run: None,
            maintenance_status: MaintenanceStatus::Idle,
        }
    }

    #[test]
    fn builds_one_skill_per_wing_tagged_with_capabilities() {
        let info = sample_info();
        let wings = vec!["wing_myproject".to_owned(), "wing_other".to_owned()];
        let inputs = AgentCardInputs {
            name: "MemPalace",
            description: "A local-first memory store",
            version: "2.0.0",
            provider: None,
            wings: &wings,
            info: &info,
            interfaces: Vec::new(),
        };
        let card = build_agent_card(&inputs);
        assert_eq!(card.skills.len(), 2);
        assert_eq!(card.skills[0].id, "coordination:wing_myproject");
        assert_eq!(card.skills[0].tags, vec!["drawers".to_owned(), "kg".to_owned()]);
        assert!(!card.capabilities.streaming);
        assert!(!card.capabilities.push_notifications);
        assert!(card.supported_interfaces.is_empty());
    }

    #[test]
    fn card_round_trips_through_json() {
        let info = sample_info();
        let wings = vec!["wing_x".to_owned()];
        let inputs = AgentCardInputs {
            name: "MemPalace",
            description: "desc",
            version: "1.0.0",
            provider: Some(AgentProvider {
                url: "https://example.com".to_owned(),
                organization: "Example Org".to_owned(),
            }),
            wings: &wings,
            info: &info,
            interfaces: vec![AgentInterface {
                url: "https://palace.example.com/a2a".to_owned(),
                protocol_binding: "JSONRPC".to_owned(),
                protocol_version: "1.0".to_owned(),
            }],
        };
        let card = build_agent_card(&inputs);
        let json = serde_json::to_string(&card).unwrap();
        let decoded: AgentCard = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, card);
    }
}
