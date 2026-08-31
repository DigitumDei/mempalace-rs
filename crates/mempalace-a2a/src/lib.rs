//! Translation library between the [A2A protocol](https://a2a-protocol.org) (v1.0, Linux
//! Foundation, 2026) and MemPalace's local coordination storage.
//!
//! This crate is a **pure translation layer**: it adds no HTTP routes, no MCP tools, and no
//! config fields. It depends only on [`mempalace_storage`] and [`mempalace_federation`] plus a
//! small set of workspace crates (`serde`, `serde_json`, `thiserror`, `time`, `blake3`). Whether
//! the A2A adapter gets its own HTTP surface is an open question in
//! `docs/Coordination-Phase-3-Design.md` (Stage 5) and is explicitly out of scope here.
//!
//! # Isolation rule
//!
//! No A2A field may become a column in MemPalace's core schema. Fields with no internal home
//! are preserved by storing the inbound envelope verbatim — the exact JSON text as received on
//! the wire, not a re-serialization of a parsed value (see [`envelope::envelope_artifact`] for
//! why that distinction matters) — as an immutable `role = "protocol_envelope"` artifact (see
//! [`envelope`]). This keeps the exchange auditable without the adapter dictating the storage
//! schema. `Message` and `Artifact` bodies are already opaque JSON/text columns in
//! `mempalace_storage`, so their A2A mappings ([`message`], [`artifact`]) apply the same "store
//! verbatim" principle directly rather than needing a separate envelope.
//!
//! # State mapping
//!
//! A2A defines nine task states against MemPalace's seven, so the mapping is not bijective in
//! either direction. See [`state`] for the mapping tables and the [`Mapped`]/[`Coercion`] types
//! that report *whether* a coercion happened, never silently.
//!
//! # Module map
//!
//! - [`state`] — [`A2aTaskState`] and the inbound/outbound state mappings.
//! - [`envelope`] — the envelope-as-artifact isolation mechanism for the whole inbound task.
//! - [`agent_card`] — Agent Card generation from palace identity, wings, and capabilities.
//! - [`task`] — A2A `Task` ↔ `coordination_tasks`.
//! - [`message`] — A2A `Message` ↔ `coordination_messages`.
//! - [`artifact`] — A2A `Artifact` ↔ `coordination_artifacts`.

pub mod agent_card;
pub mod artifact;
pub mod envelope;
pub mod message;
pub mod part;
pub mod state;
pub mod task;

mod error;

pub use agent_card::{
    AgentCapabilities, AgentCard, AgentCardInputs, AgentExtension, AgentInterface, AgentProvider,
    AgentSkill, build_agent_card,
};
pub use artifact::{
    A2A_ARTIFACT_MEDIA_TYPE, A2A_ARTIFACT_ROLE, A2aArtifact, a2a_artifact_to_new_artifact,
    artifact_to_a2a_artifact,
};
pub use envelope::{PROTOCOL_ENVELOPE_MEDIA_TYPE, PROTOCOL_ENVELOPE_ROLE, envelope_artifact};
pub use error::A2aError;
pub use message::{A2A_MESSAGE_KIND, A2aMessage, a2a_message_to_new_message, message_to_a2a_message};
pub use part::{A2aPart, A2aRole};
pub use state::{A2aTaskState, Coercion, Mapped, map_inbound_task_state, map_outbound_task_state};
pub use task::{
    A2aTask, A2aTaskStatus, NewTaskConversion, NewTaskInputs, a2a_task_to_new_task,
    task_to_a2a_task,
};
