//! Shared directory metadata tokens survive process restart without listing.
use crate::demand::SourceVersion;
use imbl::OrdMap;
use serde::{Deserialize, Serialize};

#[derive(Clone, Default)]
pub(super) struct DirectoryVersions(OrdMap<String, Option<SourceVersion>>);

impl DirectoryVersions {
    pub(super) fn insert(&mut self, path: String, version: Option<SourceVersion>) {
        self.0.insert(path, version);
    }

    pub(super) fn contains_key(&self, path: &str) -> bool {
        self.0.contains_key(path)
    }

    pub(super) fn iter(&self) -> impl Iterator<Item = (&str, Option<SourceVersion>)> {
        self.0
            .iter()
            .map(|(path, version)| (path.as_str(), *version))
    }

    pub(super) fn entries(&self) -> impl Iterator<Item = (&str, &Option<SourceVersion>)> {
        self.0
            .iter()
            .map(|(path, version)| (path.as_str(), version))
    }

    pub(super) fn changes<'a>(
        &'a self,
        previous: &'a Self,
    ) -> impl Iterator<Item = (&'a str, Option<&'a Option<SourceVersion>>)> {
        previous.0.diff(&self.0).map(|change| match change {
            imbl::ordmap::DiffItem::Add(path, version) => (path.as_str(), Some(version)),
            imbl::ordmap::DiffItem::Update {
                new: (path, version),
                ..
            } => (path.as_str(), Some(version)),
            imbl::ordmap::DiffItem::Remove(path, _) => (path.as_str(), None),
        })
    }

    pub(super) fn forget_subtree(&mut self, path: &str) {
        self.0.remove(path);
        let prefix = format!("{path}/");
        let removed: Vec<_> = self
            .0
            .range(prefix.clone()..)
            .take_while(|(directory, _)| directory.starts_with(&prefix))
            .map(|(directory, _)| directory.clone())
            .collect();
        for directory in removed {
            self.0.remove(&directory);
        }
    }
}

impl Serialize for DirectoryVersions {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_map(self.iter())
    }
}

impl<'de> Deserialize<'de> for DirectoryVersions {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Visitor;
        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = DirectoryVersions;
            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("directory version tokens")
            }
            fn visit_map<M: serde::de::MapAccess<'de>>(
                self,
                mut map: M,
            ) -> Result<Self::Value, M::Error> {
                let mut versions = DirectoryVersions::default();
                while let Some((path, version)) =
                    map.next_entry::<String, Option<SourceVersion>>()?
                {
                    if versions.contains_key(&path) {
                        return Err(serde::de::Error::custom(
                            "duplicate directory version token",
                        ));
                    }
                    versions.insert(path, version);
                }
                Ok(versions)
            }
        }
        deserializer.deserialize_map(Visitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn name_search_directory_token_snapshots_persist_only_changed_keys() {
        let mut original = DirectoryVersions::default();
        for ordinal in 0..10_000 {
            original.insert(format!("d{ordinal:05}"), Some(SourceVersion([1; 32])));
        }
        let mut changed = original.clone();
        assert_eq!(changed.changes(&original).count(), 0);
        changed.insert("d00001".into(), Some(SourceVersion([2; 32])));
        changed.insert("d00001/child".into(), None);
        changed.insert("d00001-other".into(), None);
        changed.forget_subtree("d00001");
        assert!(changed.contains_key("d00001-other"));
        assert!(!changed.contains_key("d00001/child"));
        assert!(original.contains_key("d00001"));
        assert_eq!(changed.changes(&original).count(), 2);
        // Negative control: a full snapshot visits all ten thousand keys.
        assert_eq!(changed.iter().count(), 10_000);
        let bytes = serde_json::to_vec(&changed).unwrap();
        let restored: DirectoryVersions = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(
            restored.iter().collect::<Vec<_>>(),
            changed.iter().collect::<Vec<_>>()
        );
        assert!(serde_json::from_str::<DirectoryVersions>(r#"{"a":null,"a":null}"#).is_err());
    }
}
