use std::collections::HashMap;
use std::hash::Hash;
use std::marker::PhantomData;
use std::{collections::HashSet, fmt::Display};

use daggy::petgraph::EdgeDirection;
use daggy::Dag;

use crate::{Migration, MigrationDirection, Migrator, MigratorError};

/// Like Adapter but async
#[async_trait::async_trait]
pub trait AsyncAdapter<I> {
    /// Type migrations Imust implement for this adapter.
    type MigrationType: Migration<I>;

    /// Type of errors returned by this adapter.
    type Error: std::error::Error + 'static;

    /// Returns the set of IDs for migrations that have been applied.
    async fn applied_migrations(&mut self) -> Result<HashSet<I>, Self::Error>;

    /// Apply a single migration.
    async fn apply_migration(&mut self, _: &Self::MigrationType) -> Result<(), Self::Error>;

    /// Revert a single migration.
    async fn revert_migration(&mut self, _: &Self::MigrationType) -> Result<(), Self::Error>;
}

impl<I, T> Migrator<I, T, T::MigrationType, T::Error>
where
    I: Hash + Display + Eq + Clone,
    T: AsyncAdapter<I>,
{
    /// Create a `Migrator` using the given `Adapter`.
    pub fn new_async(adapter: T) -> Migrator<I, T, T::MigrationType, T::Error> {
        Migrator {
            adapter,
            dependencies: Dag::new(),
            id_map: HashMap::new(),
            _error_type: PhantomData::default(),
        }
    }

    /// Apply migrations as necessary to so that the specified migration is
    /// applied (inclusive).
    ///
    /// If `to` is `None`, apply all registered migrations.
    pub async fn up_async(&mut self, to: Option<I>) -> Result<(), MigratorError<I, T::Error>> {
        // Register the edges
        self.register_edges()?;

        let target_idxs = self
            .induced_stream(to, EdgeDirection::Incoming)
            .map_err(MigratorError::Dependency)?;

        // TODO: This is assuming the applied_migrations state is consistent
        // with the dependency graph.
        let applied_migrations = self.adapter.applied_migrations().await?;
        for idx in target_idxs {
            let migration = &self.dependencies[idx];
            let id = migration.id();
            if applied_migrations.contains(&id) {
                continue;
            }

            self.adapter.apply_migration(migration).await.map_err(|e| {
                MigratorError::Migration {
                    id,
                    description: migration.description(),
                    direction: MigrationDirection::Up,
                    error: e,
                }
            })?;
        }

        Ok(())
    }

    /// Revert migrations as necessary so that no migrations dependent on the
    /// specified migration are applied. If the specified migration was already
    /// applied, it will still be applied.
    ///
    /// If `to` is `None`, revert all applied migrations.
    pub async fn down_async(&mut self, to: Option<I>) -> Result<(), MigratorError<I, T::Error>> {
        // Register the edges
        self.register_edges()?;

        let mut target_idxs = self
            .induced_stream(to.clone(), EdgeDirection::Outgoing)
            .map_err(MigratorError::Dependency)?;
        if let Some(sink_id) = to {
            target_idxs.remove(
                self.id_map
                    .get(&sink_id)
                    .expect("Id is checked in induced_stream and exists"),
            );
        }

        let applied_migrations = self.adapter.applied_migrations().await?;
        for idx in target_idxs {
            let migration = &self.dependencies[idx];
            let id = migration.id();
            if !applied_migrations.contains(&id) {
                continue;
            }

            self.adapter
                .revert_migration(migration)
                .await
                .map_err(|e| MigratorError::Migration {
                    id,
                    description: migration.description(),
                    direction: MigrationDirection::Down,
                    error: e,
                })?;
        }

        Ok(())
    }
}
