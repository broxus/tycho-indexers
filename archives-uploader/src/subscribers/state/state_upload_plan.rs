use std::fmt;

use anyhow::Context;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use tycho_core::storage::{PersistentStateKind, PersistentStateMeta};

use super::verify_existing_manifest;
#[cfg(test)]
use super::verify_uploaded_manifest_bytes;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum StateObjectRole {
    Main,
    Manifest,
    Part(u64),
}

impl fmt::Display for StateObjectRole {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Main => f.write_str("main"),
            Self::Manifest => f.write_str("manifest"),
            Self::Part(prefix) => write!(f, "part({prefix:016x})"),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SizeMatch {
    Match,
    Mismatch {
        expected: u64,
        role: StateObjectRole,
    },
    NotChecked,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RemoteObject {
    Missing,
    Zero,
    Present { size: u64, size_match: SizeMatch },
}

impl RemoteObject {
    async fn inspect(
        client: &dyn ObjectStore,
        location: &Path,
        expected_size: Option<u64>,
        role: StateObjectRole,
    ) -> anyhow::Result<RemoteObject> {
        match client.head(location).await {
            Ok(meta) if meta.size == 0 => Ok(RemoteObject::Zero),
            Ok(meta) => Ok(RemoteObject::Present {
                size: meta.size,
                size_match: match expected_size {
                    Some(expected) if meta.size == expected => SizeMatch::Match,
                    Some(expected) => SizeMatch::Mismatch { expected, role },
                    None => SizeMatch::NotChecked,
                },
            }),
            Err(object_store::Error::NotFound { .. }) => Ok(RemoteObject::Missing),
            Err(e) => Err(e).context(format!("failed to inspect remote {role}")),
        }
    }

    fn ensure_size_matches(self) -> anyhow::Result<Self> {
        if let Self::Present {
            size,
            size_match: SizeMatch::Mismatch { expected, role },
        } = self
        {
            anyhow::bail!("remote {role} has unexpected size: expected={expected}, actual={size}");
        }
        Ok(self)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum UploadAction {
    Reuse,
    Upload,
}

#[derive(Debug)]
pub(super) struct StateUploadPlan {
    pub(super) parts: Vec<(u64, UploadAction)>,
    pub(super) manifest: Option<UploadAction>,
    pub(super) main: UploadAction,
}

impl StateUploadPlan {
    pub(super) async fn make(
        client: &dyn ObjectStore,
        kind: PersistentStateKind,
        main: (&Path, u64),
        manifest_location: &Path,
        manifest: Option<&PersistentStateMeta>,
        parts: &[(u64, Path, u64)],
        enable_duplication: bool,
    ) -> anyhow::Result<StateUploadPlan> {
        let (main_location, main_size) = main;
        let Some(expected_meta) = manifest else {
            // check remote manifest only for the shard state
            if kind == PersistentStateKind::Shard {
                match RemoteObject::inspect(
                    client,
                    manifest_location,
                    None,
                    StateObjectRole::Manifest,
                )
                .await?
                {
                    RemoteObject::Missing => {}
                    RemoteObject::Zero | RemoteObject::Present { .. } => {
                        anyhow::bail!("remote manifest conflicts with local single-file state")
                    }
                }
            }
            let main = match RemoteObject::inspect(
                client,
                main_location,
                Some(main_size),
                StateObjectRole::Main,
            )
            .await?
            .ensure_size_matches()?
            {
                RemoteObject::Present { .. } if !enable_duplication => UploadAction::Reuse,
                RemoteObject::Missing | RemoteObject::Zero | RemoteObject::Present { .. } => {
                    UploadAction::Upload
                }
            };
            return Ok(StateUploadPlan {
                parts: Vec::new(),
                manifest: None,
                main,
            });
        };

        let manifest =
            match RemoteObject::inspect(client, manifest_location, None, StateObjectRole::Manifest)
                .await?
            {
                RemoteObject::Present { .. } => {
                    verify_existing_manifest(client, manifest_location, expected_meta).await?;
                    UploadAction::Reuse
                }
                RemoteObject::Missing | RemoteObject::Zero => {
                    match RemoteObject::inspect(
                        client,
                        main_location,
                        Some(main_size),
                        StateObjectRole::Main,
                    )
                    .await?
                    .ensure_size_matches()?
                    {
                        RemoteObject::Missing | RemoteObject::Zero => {}
                        RemoteObject::Present { .. } => {
                            anyhow::bail!("remote legacy main conflicts with local split state")
                        }
                    }
                    UploadAction::Upload
                }
            };

        let mut part_actions = Vec::with_capacity(parts.len());
        for (prefix, location, size) in parts {
            let role = StateObjectRole::Part(*prefix);
            let action = match RemoteObject::inspect(client, location, Some(*size), role)
                .await?
                .ensure_size_matches()?
            {
                RemoteObject::Present { .. } if !enable_duplication => UploadAction::Reuse,
                RemoteObject::Missing | RemoteObject::Zero | RemoteObject::Present { .. } => {
                    UploadAction::Upload
                }
            };
            part_actions.push((*prefix, action));
        }
        let main = match RemoteObject::inspect(
            client,
            main_location,
            Some(main_size),
            StateObjectRole::Main,
        )
        .await?
        .ensure_size_matches()?
        {
            RemoteObject::Present { .. } if !enable_duplication => UploadAction::Reuse,
            RemoteObject::Missing | RemoteObject::Zero | RemoteObject::Present { .. } => {
                UploadAction::Upload
            }
        };

        Ok(StateUploadPlan {
            parts: part_actions,
            manifest: Some(if enable_duplication {
                UploadAction::Upload
            } else {
                manifest
            }),
            main,
        })
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use object_store::memory::InMemory;

    use super::*;

    impl StateUploadPlan {
        async fn for_shard(
            client: &dyn ObjectStore,
            main: (&Path, u64),
            manifest_location: &Path,
            manifest: Option<&PersistentStateMeta>,
            parts: &[(u64, Path, u64)],
            enable_duplication: bool,
        ) -> anyhow::Result<StateUploadPlan> {
            Self::make(
                client,
                PersistentStateKind::Shard,
                main,
                manifest_location,
                manifest,
                parts,
                enable_duplication,
            )
            .await
        }
    }

    fn locations() -> (Path, Path, Vec<(u64, Path, u64)>) {
        (
            Path::from("state.boc"),
            Path::from("state.meta.json"),
            vec![
                (
                    0x2000000000000000,
                    Path::from("state_part_2000000000000000.boc"),
                    3,
                ),
                (
                    0xa000000000000000,
                    Path::from("state_part_a000000000000000.boc"),
                    4,
                ),
            ],
        )
    }

    fn meta() -> PersistentStateMeta {
        PersistentStateMeta::new(2, vec![0xa000000000000000, 0x2000000000000000])
    }

    async fn put(store: &InMemory, path: &Path, bytes: &[u8]) {
        store
            .put(path, Bytes::copy_from_slice(bytes).into())
            .await
            .unwrap();
    }

    fn assert_error_contains(error: anyhow::Error, expected: &str) {
        let error = format!("{error:#}");
        assert!(error.contains(expected), "unexpected error: {error}");
    }

    #[tokio::test]
    async fn split_plan_handles_missing_incomplete_complete_and_duplicate_states() {
        let store = InMemory::new();
        let (main, manifest, parts) = locations();
        let meta = meta();

        // start with no remote objects and plan a canonical complete split bundle
        let plan =
            StateUploadPlan::for_shard(&store, (&main, 5), &manifest, Some(&meta), &parts, false)
                .await
                .unwrap();

        assert_eq!(plan.parts, vec![
            (0x2000000000000000, UploadAction::Upload),
            (0xa000000000000000, UploadAction::Upload),
        ]);
        assert_eq!(plan.manifest, Some(UploadAction::Upload));
        assert_eq!(plan.main, UploadAction::Upload);

        // make the manifest and one part reusable while leaving a zero-sized part and missing main
        put(
            &store,
            &manifest,
            br#"{"parts":["a000000000000000","2000000000000000"],"version":1,"split_depth":2}"#,
        )
        .await;
        put(&store, &parts[0].1, b"one").await;
        put(&store, &parts[1].1, b"").await;

        // resume only the incomplete objects
        let plan =
            StateUploadPlan::for_shard(&store, (&main, 5), &manifest, Some(&meta), &parts, false)
                .await
                .unwrap();
        assert_eq!(plan.parts, vec![
            (parts[0].0, UploadAction::Reuse),
            (parts[1].0, UploadAction::Upload)
        ]);
        assert_eq!(plan.manifest, Some(UploadAction::Reuse));
        assert_eq!(plan.main, UploadAction::Upload);

        // reuse the bundle once every object is present with the expected size
        put(&store, &main, b"main!").await;
        put(&store, &parts[1].1, b"two!").await;

        let plan =
            StateUploadPlan::for_shard(&store, (&main, 5), &manifest, Some(&meta), &parts, false)
                .await
                .unwrap();
        assert_eq!(plan.parts, vec![
            (parts[0].0, UploadAction::Reuse),
            (parts[1].0, UploadAction::Reuse)
        ]);
        assert_eq!(plan.manifest, Some(UploadAction::Reuse));
        assert_eq!(plan.main, UploadAction::Reuse);

        // duplication overrides reuse and schedules every split object again
        let duplicate_plan =
            StateUploadPlan::for_shard(&store, (&main, 5), &manifest, Some(&meta), &parts, true)
                .await
                .unwrap();
        assert!(
            duplicate_plan
                .parts
                .iter()
                .all(|(_, action)| *action == UploadAction::Upload)
        );
        assert_eq!(duplicate_plan.manifest, Some(UploadAction::Upload));
        assert_eq!(duplicate_plan.main, UploadAction::Upload);
    }

    #[tokio::test]
    async fn split_conflicts_with_legacy_or_invalid_manifest() {
        let store = InMemory::new();
        let (main, manifest, parts) = locations();
        let meta = meta();
        let bytes = meta.to_bytes().unwrap();

        // reject a legacy main that conflicts with the local split representation
        put(&store, &main, b"main!").await;
        assert_error_contains(
            StateUploadPlan::for_shard(&store, (&main, 5), &manifest, Some(&meta), &parts, false)
                .await
                .unwrap_err(),
            "remote legacy main conflicts with local split state",
        );

        // fail preflight for malformed metadata instead of treating it as absent
        let store = InMemory::new();
        let mut malformed = bytes.clone();
        malformed[0] = b'!';
        put(&store, &manifest, &malformed).await;
        assert_error_contains(
            StateUploadPlan::for_shard(&store, (&main, 5), &manifest, Some(&meta), &parts, false)
                .await
                .unwrap_err(),
            "failed to parse remote manifest",
        );
    }

    #[tokio::test]
    async fn single_file_rejects_manifest_and_uploads_missing_or_zero_main() {
        let store = InMemory::new();
        let (main, manifest, _) = locations();

        // plan the first single-file upload when no remote object exists
        let plan = StateUploadPlan::for_shard(&store, (&main, 5), &manifest, None, &[], false)
            .await
            .unwrap();
        assert_eq!(plan.main, UploadAction::Upload);

        // replace a zero-sized main object as an incomplete upload
        put(&store, &main, b"").await;
        let plan = StateUploadPlan::for_shard(&store, (&main, 5), &manifest, None, &[], false)
            .await
            .unwrap();
        assert_eq!(plan.main, UploadAction::Upload);

        // reject metadata that conflicts with a local single-file representation
        put(&store, &manifest, b"manifest").await;
        assert_error_contains(
            StateUploadPlan::for_shard(&store, (&main, 5), &manifest, None, &[], false)
                .await
                .unwrap_err(),
            "remote manifest conflicts with local single-file state",
        );

        // reuse a complete main object unless duplication requests replacement
        let store = InMemory::new();
        put(&store, &main, b"main!").await;
        let plan = StateUploadPlan::for_shard(&store, (&main, 5), &manifest, None, &[], false)
            .await
            .unwrap();
        assert_eq!(plan.main, UploadAction::Reuse);
        let duplicate_plan =
            StateUploadPlan::for_shard(&store, (&main, 5), &manifest, None, &[], true)
                .await
                .unwrap();
        assert_eq!(duplicate_plan.main, UploadAction::Upload);
    }

    #[tokio::test]
    async fn wrong_sizes_and_manifest_metadata_are_conflicts() {
        let store = InMemory::new();
        let (main, manifest, parts) = locations();
        let meta = meta();
        let bytes = meta.to_bytes().unwrap();

        // reject a present split part whose size conflicts with local expectations
        put(&store, &parts[0].1, b"wrong").await;
        assert_error_contains(
            StateUploadPlan::for_shard(&store, (&main, 5), &manifest, Some(&meta), &parts, false)
                .await
                .unwrap_err(),
            "remote part(2000000000000000) has unexpected size: expected=3, actual=5",
        );

        // accept semantically equivalent manifest metadata
        put(&store, &manifest, &bytes).await;
        verify_existing_manifest(&store, &manifest, &meta)
            .await
            .unwrap();

        // replace it with semantically different metadata to reject the preflight
        let mismatch = PersistentStateMeta::new(2, vec![0x6000000000000000, 0xe000000000000000]);
        let mismatch_bytes = mismatch.to_bytes().unwrap();
        put(&store, &manifest, &mismatch_bytes).await;
        assert_error_contains(
            StateUploadPlan::for_shard(&store, (&main, 5), &manifest, Some(&meta), &parts, false)
                .await
                .unwrap_err(),
            "remote manifest does not match the local split metadata",
        );

        // replace it again to reject an exact uploaded-byte mismatch
        let mut changed = bytes.clone();
        changed[0] = b'!';
        put(&store, &manifest, &changed).await;
        assert_error_contains(
            verify_uploaded_manifest_bytes(&store, &manifest, &bytes)
                .await
                .unwrap_err(),
            "remote manifest bytes do not match the uploaded payload",
        );
    }

    #[tokio::test]
    async fn queue_ignores_shard_manifest() {
        let store = InMemory::new();
        let (_, manifest, _) = locations();
        let main = Path::from("queue.boc");

        // upload a missing queue despite an existing shard manifest
        put(&store, &manifest, &meta().to_bytes().unwrap()).await;
        let plan = StateUploadPlan::make(
            &store,
            PersistentStateKind::Queue,
            (&main, 5),
            &manifest,
            None,
            &[],
            false,
        )
        .await
        .unwrap();
        assert!(plan.parts.is_empty());
        assert_eq!(plan.manifest, None);
        assert_eq!(plan.main, UploadAction::Upload);

        // replace an empty queue object
        put(&store, &main, b"").await;
        let plan = StateUploadPlan::make(
            &store,
            PersistentStateKind::Queue,
            (&main, 5),
            &manifest,
            None,
            &[],
            false,
        )
        .await
        .unwrap();
        assert_eq!(plan.main, UploadAction::Upload);

        // reuse a queue object with the expected size
        put(&store, &main, b"queue").await;
        let plan = StateUploadPlan::make(
            &store,
            PersistentStateKind::Queue,
            (&main, 5),
            &manifest,
            None,
            &[],
            false,
        )
        .await
        .unwrap();
        assert_eq!(plan.main, UploadAction::Reuse);
    }
}
