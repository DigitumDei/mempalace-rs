//! A2A `Artifact` ↔ `coordination_artifacts`.
//!
//! `Artifact` fields verified against the A2A v1.0.1 proto (`specification/a2a.proto` in
//! `a2aproject/A2A` at tag `v1.0.1`, lines ~274-286): `artifact_id`, `name`, `description`,
//! `parts`, `metadata`, `extensions`.
//!
//! # Isolation
//!
//! `mempalace_storage::coordination::Artifact` stores its body as an opaque `content: String`
//! column with generic `role`/`media_type` metadata columns — it has no per-A2A-field columns
//! of its own. Mapping an A2A `Artifact` into that shape serializes the entire `A2aArtifact`
//! verbatim into `content` (as it does for [`crate::message`]), so no A2A field is dropped,
//! reinterpreted, or promoted to a column. `role` is fixed to [`A2A_ARTIFACT_ROLE`] and
//! `media_type` to [`A2A_ARTIFACT_MEDIA_TYPE`] so a reader can recognise the content shape
//! before deserializing it — distinct from [`crate::envelope::PROTOCOL_ENVELOPE_ROLE`], which
//! is reserved for the whole inbound *task* envelope, not an individual `Artifact` object.

use mempalace_storage::{Artifact, NewArtifact};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::A2aError;
use crate::part::A2aPart;

/// `coordination_artifacts.role` used for a stored A2A artifact.
pub const A2A_ARTIFACT_ROLE: &str = "a2a_artifact";
/// `coordination_artifacts.media_type` used for a stored A2A artifact.
pub const A2A_ARTIFACT_MEDIA_TYPE: &str = "application/vnd.a2a.artifact+json";

/// Wire mirror of the A2A `Artifact` object.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct A2aArtifact {
    /// Unique identifier for the artifact, unique within its task.
    pub artifact_id: String,
    /// Human-readable name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Human-readable description.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Artifact content; must contain at least one part per the A2A spec.
    pub parts: Vec<A2aPart>,
    /// Optional metadata.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Value>,
    /// URIs of extensions present on this artifact.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub extensions: Vec<String>,
}

/// Build a [`NewArtifact`] carrying `a2a` verbatim as its content.
///
/// `task_id` is the owning MemPalace task; `created_by`/`idempotency_key` are resolved by the
/// caller the same way every other coordination write resolves them (see
/// `mempalace_storage::coordination::NewArtifact`).
pub fn a2a_artifact_to_new_artifact(
    a2a: &A2aArtifact,
    task_id: &str,
    created_by: &str,
    idempotency_key: String,
) -> Result<NewArtifact, A2aError> {
    let content = serde_json::to_string(a2a)?;
    Ok(NewArtifact {
        task_id: task_id.to_owned(),
        created_by: created_by.to_owned(),
        role: A2A_ARTIFACT_ROLE.to_owned(),
        media_type: A2A_ARTIFACT_MEDIA_TYPE.to_owned(),
        content,
        idempotency_key,
    })
}

/// Decode a stored [`Artifact`] back into an [`A2aArtifact`].
///
/// Fails with [`A2aError::InvalidStoredShape`] if `artifact.role` is not [`A2A_ARTIFACT_ROLE`],
/// or `content` does not decode as an `A2aArtifact` — e.g. an artifact written by a non-A2A
/// caller (including a [`crate::envelope::PROTOCOL_ENVELOPE_ROLE`] envelope artifact, which has
/// a different shape entirely).
pub fn artifact_to_a2a_artifact(artifact: &Artifact) -> Result<A2aArtifact, A2aError> {
    if artifact.role != A2A_ARTIFACT_ROLE {
        return Err(A2aError::InvalidStoredShape {
            shape: "Artifact",
            detail: format!("role `{}` is not `{A2A_ARTIFACT_ROLE}`", artifact.role),
        });
    }
    serde_json::from_str(&artifact.content).map_err(|err| A2aError::InvalidStoredShape {
        shape: "Artifact",
        detail: err.to_string(),
    })
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn sample() -> A2aArtifact {
        A2aArtifact {
            artifact_id: "artifact_1".to_owned(),
            name: Some("output.txt".to_owned()),
            description: None,
            parts: vec![A2aPart { text: Some("done".to_owned()), ..Default::default() }],
            metadata: None,
            extensions: Vec::new(),
        }
    }

    #[test]
    fn round_trips_through_new_artifact_and_back() {
        let original = sample();
        let new_artifact =
            a2a_artifact_to_new_artifact(&original, "task_1", "agent-a", "key_1".to_owned())
                .unwrap();
        assert_eq!(new_artifact.role, A2A_ARTIFACT_ROLE);
        assert_eq!(new_artifact.media_type, A2A_ARTIFACT_MEDIA_TYPE);

        let stored = Artifact {
            artifact_id: "artifact_stored_1".to_owned(),
            task_id: new_artifact.task_id.clone(),
            created_by: new_artifact.created_by.clone(),
            role: new_artifact.role.clone(),
            media_type: new_artifact.media_type.clone(),
            content: new_artifact.content.clone(),
            content_hash: "irrelevant-for-this-test".to_owned(),
            created_at: time::OffsetDateTime::now_utc(),
        };
        let decoded = artifact_to_a2a_artifact(&stored).unwrap();
        assert_eq!(decoded, original);
    }

    #[test]
    fn rejects_an_artifact_with_the_wrong_role() {
        let stored = Artifact {
            artifact_id: "artifact_stored_1".to_owned(),
            task_id: "task_1".to_owned(),
            created_by: "a".to_owned(),
            role: "protocol_envelope".to_owned(),
            media_type: "application/vnd.a2a+json".to_owned(),
            content: "{}".to_owned(),
            content_hash: "irrelevant-for-this-test".to_owned(),
            created_at: time::OffsetDateTime::now_utc(),
        };
        let err = artifact_to_a2a_artifact(&stored).expect_err("wrong role must be rejected");
        assert!(matches!(err, A2aError::InvalidStoredShape { shape: "Artifact", .. }));
    }
}
