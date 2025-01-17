//! Test harness for applying a generic test suite to any backend-specific
//! schemer adapter.

use std::str::FromStr;

use super::*;

/// A trait required for running the generic test suite on an `Adapter`.
pub trait TestAdapter<I>: Adapter<I> {
    /// Construct a mock, no-op migration of the adapter's `MigrationType`.
    ///
    /// For convenience adapters can implement their migration traits on
    /// `TestMigration` and construct those here.
    fn mock(id: I, dependencies: HashSet<I>) -> Self::MigrationType;
}

/// A trivial struct implementing `Migration` on which adapters can build their
/// mock migrations.
pub struct TestMigration<I> {
    id: I,
    dependencies: HashSet<I>,
}

impl<I> TestMigration<I> {
    pub fn new(id: I, dependencies: HashSet<I>) -> Self {
        TestMigration { id, dependencies }
    }
}

impl<I: Clone> Migration<I> for TestMigration<I> {
    fn id(&self) -> I {
        self.id.clone()
    }

    fn dependencies(&self) -> HashSet<I> {
        self.dependencies.clone()
    }

    fn description(&self) -> &'static str {
        "Test Migration"
    }
}

/// Test an `Adapter` with the generic test suite.
///
/// Note that the adapter must also implement the `TestAdapter` trait. This
/// should be done only for the testing configuration of the adapter's crate,
/// as it is not necessary for normal behavior.
///
/// # Examples
///
/// ```rust,ignore
/// #[macro_use] extern crate schemer;
///
/// fn construct_my_adapter_test_fixture() -> MyAdapterType {
///     MyAdapterType {}
/// }
///
/// test_schemer_adapter!(construct_my_adapter_test_fixture());
/// ```
#[macro_export]
macro_rules! test_schemer_adapter {
    ($constructor:expr, $id_ter:expr) => {
        test_schemer_adapter!({}, $constructor, $id_ter);
    };
    ($setup:stmt, $constructor:expr, $id_ter:expr) => {
        test_schemer_adapter!($setup, $constructor, $id_ter,
            test_single_migration,
            test_migration_chain,
            test_multi_component_dag,
            test_branching_dag,
            test_migration_chain_reversed,
        );
    };
    ($setup:stmt, $constructor:expr, $id_ter:expr, $($test_fn:ident),* $(,)*) => {
        $(
            #[test]
            fn $test_fn() {
                $setup
                let adapter = $constructor;
                $crate::testing::$test_fn(adapter, $id_ter);
            }
        )*
    }
}

/// Test the application and reversion of a singleton migration.
pub fn test_single_migration<I, A, T>(adapter: A, mut id_iter: T)
where
    I: Clone + FromStr + Debug + Display + Hash + Eq,
    I::Err: Debug,
    A: TestAdapter<I>,
    T: Iterator<Item = I>,
{
    let migration1 = A::mock(id_iter.next().unwrap(), HashSet::new());
    let uuid1 = migration1.id();

    let mut migrator: Migrator<I, A> = Migrator::new(adapter);

    migrator
        .register(migration1)
        .expect("Migration 1 registration failed");
    migrator.up(None).expect("Up migration failed");

    assert!(migrator
        .adapter
        .applied_migrations()
        .unwrap()
        .contains(&uuid1,));

    migrator.down(None).expect("Down migration failed");

    assert!(!migrator
        .adapter
        .applied_migrations()
        .unwrap()
        .contains(&uuid1,));
}

/// Test the partial application and reversion of a chain of three dependent
/// migrations.
pub fn test_migration_chain<I, A, T>(adapter: A, mut id_iter: T)
where
    I: Clone + FromStr + Debug + Display + Hash + Eq,
    I::Err: Debug,
    A: TestAdapter<I>,
    T: Iterator<Item = I>,
{
    let migration1 = A::mock(id_iter.next().unwrap(), HashSet::new());
    let migration2 = A::mock(
        id_iter.next().unwrap(),
        vec![migration1.id()].into_iter().collect(),
    );
    let migration3 = A::mock(
        id_iter.next().unwrap(),
        vec![migration2.id()].into_iter().collect(),
    );

    let uuid1 = migration1.id();
    let uuid2 = migration2.id();
    let uuid3 = migration3.id();

    let mut migrator = Migrator::new(adapter);

    migrator
        .register_multiple(vec![migration1, migration2, migration3].into_iter())
        .expect("Migration registration failed");

    migrator
        .up(Some(uuid2.clone()))
        .expect("Up migration failed");

    {
        let applied = migrator.adapter.applied_migrations().unwrap();
        assert!(applied.contains(&uuid1));
        assert!(applied.contains(&uuid2));
        assert!(!applied.contains(&uuid3));
    }

    migrator
        .down(Some(uuid1.clone()))
        .expect("Down migration failed");

    {
        let applied = migrator.adapter.applied_migrations().unwrap();
        assert!(applied.contains(&uuid1));
        assert!(!applied.contains(&uuid2));
        assert!(!applied.contains(&uuid3));
    }
}

/// Test that application and reversion of two DAG components are independent.
pub fn test_multi_component_dag<I, A, T>(adapter: A, mut id_iter: T)
where
    I: Clone + FromStr + Debug + Display + Hash + Eq,
    I::Err: Debug,
    A: TestAdapter<I>,
    T: Iterator<Item = I>,
{
    let migration1 = A::mock(id_iter.next().unwrap(), HashSet::new());
    let migration2 = A::mock(
        id_iter.next().unwrap(),
        vec![migration1.id()].into_iter().collect(),
    );
    let migration3 = A::mock(id_iter.next().unwrap(), HashSet::new());
    let migration4 = A::mock(
        id_iter.next().unwrap(),
        vec![migration3.id()].into_iter().collect(),
    );

    let uuid1 = migration1.id();
    let uuid2 = migration2.id();
    let uuid3 = migration3.id();
    let uuid4 = migration4.id();

    let mut migrator = Migrator::new(adapter);

    migrator
        .register_multiple(vec![migration1, migration2, migration3, migration4].into_iter())
        .expect("Migration registration failed");

    migrator
        .up(Some(uuid2.clone()))
        .expect("Up migration failed");

    {
        let applied = migrator.adapter.applied_migrations().unwrap();
        assert!(applied.contains(&uuid1));
        assert!(applied.contains(&uuid2));
        assert!(!applied.contains(&uuid3));
        assert!(!applied.contains(&uuid4));
    }

    migrator
        .down(Some(uuid1.clone()))
        .expect("Down migration failed");

    {
        let applied = migrator.adapter.applied_migrations().unwrap();
        assert!(applied.contains(&uuid1));
        assert!(!applied.contains(&uuid2));
        assert!(!applied.contains(&uuid3));
        assert!(!applied.contains(&uuid4));
    }

    migrator
        .up(Some(uuid3.clone()))
        .expect("Up migration failed");

    {
        let applied = migrator.adapter.applied_migrations().unwrap();
        assert!(applied.contains(&uuid1));
        assert!(!applied.contains(&uuid2));
        assert!(applied.contains(&uuid3));
        assert!(!applied.contains(&uuid4));
    }

    migrator.up(None).expect("Up migration failed");

    {
        let applied = migrator.adapter.applied_migrations().unwrap();
        assert!(applied.contains(&uuid1));
        assert!(applied.contains(&uuid2));
        assert!(applied.contains(&uuid3));
        assert!(applied.contains(&uuid4));
    }

    migrator.down(None).expect("Down migration failed");

    {
        let applied = migrator.adapter.applied_migrations().unwrap();
        assert!(!applied.contains(&uuid1));
        assert!(!applied.contains(&uuid2));
        assert!(!applied.contains(&uuid3));
        assert!(!applied.contains(&uuid4));
    }
}

/// Test application and reversion on a branching DAG.
pub fn test_branching_dag<I, A, T>(adapter: A, mut id_iter: T)
where
    I: Clone + FromStr + Debug + Display + Hash + Eq,
    I::Err: Debug,
    A: TestAdapter<I>,
    T: Iterator<Item = I>,
{
    let migration1 = A::mock(id_iter.next().unwrap(), HashSet::new());
    let migration2 = A::mock(id_iter.next().unwrap(), HashSet::new());
    let migration3 = A::mock(
        id_iter.next().unwrap(),
        vec![migration1.id(), migration2.id()].into_iter().collect(),
    );
    let migration4 = A::mock(
        id_iter.next().unwrap(),
        vec![migration3.id()].into_iter().collect(),
    );
    let migration5 = A::mock(
        id_iter.next().unwrap(),
        vec![migration3.id()].into_iter().collect(),
    );

    let uuid1 = migration1.id();
    let uuid2 = migration2.id();
    let uuid3 = migration3.id();
    let uuid4 = migration4.id();
    let uuid5 = migration5.id();

    let mut migrator = Migrator::new(adapter);

    migrator
        .register_multiple(
            vec![migration1, migration2, migration3, migration4, migration5].into_iter(),
        )
        .expect("Migration registration failed");

    migrator
        .up(Some(uuid4.clone()))
        .expect("Up migration failed");

    {
        let applied = migrator.adapter.applied_migrations().unwrap();
        assert!(applied.contains(&uuid1));
        assert!(applied.contains(&uuid2));
        assert!(applied.contains(&uuid3));
        assert!(applied.contains(&uuid4));
        assert!(!applied.contains(&uuid5));
    }

    migrator
        .down(Some(uuid1.clone()))
        .expect("Down migration failed");

    {
        let applied = migrator.adapter.applied_migrations().unwrap();
        assert!(applied.contains(&uuid1));
        assert!(applied.contains(&uuid2));
        assert!(!applied.contains(&uuid3));
        assert!(!applied.contains(&uuid4));
        assert!(!applied.contains(&uuid5));
    }
}

/// Test the partial application and reversion of a chain of three dependent
/// migrations, inserted in reverse order.
pub fn test_migration_chain_reversed<I, A, T>(adapter: A, mut id_iter: T)
where
    I: Clone + FromStr + Debug + Display + Hash + Eq,
    I::Err: Debug,
    A: TestAdapter<I>,
    T: Iterator<Item = I>,
{
    let migration1 = A::mock(id_iter.next().unwrap(), HashSet::new());
    let migration2 = A::mock(
        id_iter.next().unwrap(),
        vec![migration1.id()].into_iter().collect(),
    );
    let migration3 = A::mock(
        id_iter.next().unwrap(),
        vec![migration2.id()].into_iter().collect(),
    );

    let uuid3 = migration3.id();
    let uuid2 = migration2.id();
    let uuid1 = migration1.id();

    let mut migrator = Migrator::new(adapter);

    migrator
        .register(migration3)
        .expect("Migration registration failed");

    migrator
        .register(migration2)
        .expect("Migration registration failed");

    migrator
        .register(migration1)
        .expect("Migration registration failed");

    migrator
        .up(Some(uuid2.clone()))
        .expect("Up migration failed");

    {
        let applied = migrator.adapter.applied_migrations().unwrap();
        assert!(applied.contains(&uuid1));
        assert!(applied.contains(&uuid2));
        assert!(!applied.contains(&uuid3));
    }

    migrator
        .down(Some(uuid1.clone()))
        .expect("Down migration failed");

    {
        let applied = migrator.adapter.applied_migrations().unwrap();
        assert!(applied.contains(&uuid1));
        assert!(!applied.contains(&uuid2));
        assert!(!applied.contains(&uuid3));
    }
}
