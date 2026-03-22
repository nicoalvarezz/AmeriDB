use std::{
    collections::HashMap,
    io,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use crate::storage::disk::StorageManager;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DatabaseId(pub u64);

pub struct DatabaseCluster {
    /// Root directroy of the cluster
    cluster_dir: PathBuf,

    /// Default page size for the new database (can be overridden per-db later)
    default_page_size: usize,

    /// Open storage managers, keyed by database OID
    /// Wrapped in Arc<Mutex<<>> so we can share & mutate across threads laters
    databases: Mutex<HashMap<DatabaseId, Arc<Mutex<StorageManager>>>>,
}

impl DatabaseCluster {
    /// Create a new cluster manager.
    /// The directroy must exist or be creatable
    pub fn new(cluster_dir: impl AsRef<Path>, default_page_size: usize) -> io::Result<Self> {
        let cluster_path = cluster_dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&cluster_path)?;
        std::fs::create_dir_all(cluster_path.join("base"))?;

        Ok(Self {
            cluster_dir: cluster_path,
            default_page_size,
            databases: Mutex::new(HashMap::new()),
        })
    }

    /// Open (or create) a database by its OID.
    /// Returns a shared reference to the StorageManager.
    /// If already open -> returns exsisting instance
    pub fn open_database(&self, db_id: DatabaseId) -> io::Result<Arc<Mutex<StorageManager>>> {
        let mut dbs = self.databases.lock().unwrap();

        if let Some(existing) = dbs.get(&db_id) {
            return Ok(existing.clone());
        }

        let db_dir = self.cluster_dir.join("base").join(db_id.0.to_string());
        std::fs::create_dir_all(&db_dir)?;

        let data_path = db_dir.join("database.data");

        let manager = StorageManager::open(data_path, self.default_page_size)?;

        let arc_manager = Arc::new(Mutex::new(manager));
        dbs.insert(db_id, arc_manager.clone());

        Ok(arc_manager)
    }

    // Convnience: open by database name -> but needs name -> oid mapping
    // (For now we keep it oid-based; catalog comes later)
    // pub fn open_database_by_name(&self, name: &str) -> io::Result<..>

    /// Close / drop a specific database's manager (optional, mostly for testing)
    pub fn close_database(&self, db_id: DatabaseId) {
        let mut dbs = self.databases.lock().unwrap();
        dbs.remove(&db_id);
    }

    pub fn list_open_databses(&self) -> Vec<DatabaseId> {
        let dbs = self.databases.lock().unwrap();
        dbs.keys().cloned().collect()
    }

    pub fn cluster_dir(&self) -> &Path {
        &self.cluster_dir
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::disk::{PAGE_SIZE, Page, PageId};

    use super::*;
    use tempfile::tempdir;

    #[test]
    fn cluster_new_creates_bas_directory() -> io::Result<()> {
        let temp_dir = tempdir()?;
        let cluster = DatabaseCluster::new(temp_dir.path(), PAGE_SIZE)?;

        assert!(cluster.cluster_dir().join("base").exists());

        Ok(())
    }

    #[test]
    fn cluster_open_same_db_id_returns_cached_instance() -> io::Result<()> {
        let temp_dir = tempdir()?;
        let cluster = DatabaseCluster::new(temp_dir.path(), PAGE_SIZE)?;

        let db_id = DatabaseId(123);
        let first = cluster.open_database(db_id)?;
        let second = cluster.open_database(db_id)?;

        assert!(
            Arc::ptr_eq(&first, &second),
            "opening the same database ID should return cached Arc instance"
        );
        assert_eq!(cluster.list_open_databses().len(), 1);

        Ok(())
    }

    #[test]
    fn cluster_different_db_ids_get_differet_managers() -> io::Result<()> {
        let temp_diir = tempdir()?;
        let cluster = DatabaseCluster::new(temp_diir.path(), PAGE_SIZE)?;

        let first = cluster.open_database(DatabaseId(123))?;
        let second = cluster.open_database(DatabaseId(456))?;

        assert!(!Arc::ptr_eq(&first, &second));
        assert_eq!(cluster.list_open_databses().len(), 2);

        Ok(())
    }

    #[test]
    fn cluster_open_creates_a_database_directory_and_file() -> io::Result<()> {
        let temp_dir = tempdir()?;
        let cluster = DatabaseCluster::new(temp_dir.path(), PAGE_SIZE)?;

        let db_id = DatabaseId(123);
        cluster.open_database(db_id)?;

        let db_dir = cluster.cluster_dir().join("base").join(db_id.0.to_string());
        assert!(db_dir.exists());
        assert!(db_dir.join("database.data").exists());

        Ok(())
    }

    #[test]
    fn cluster_allocate_affects_only_target_databases() -> io::Result<()> {
        let temp_dir = tempdir()?;
        let cluster = DatabaseCluster::new(temp_dir.path(), PAGE_SIZE)?;

        let db_123 = cluster.open_database(DatabaseId(123))?;
        let db_456 = cluster.open_database(DatabaseId(456))?;
        let db_789 = cluster.open_database(DatabaseId(789))?;

        // All of the storgae managers have their own allocated pages
        assert_eq!(PageId(0), db_123.lock().unwrap().allocate_page()?);
        assert_eq!(PageId(0), db_456.lock().unwrap().allocate_page()?);
        assert_eq!(PageId(0), db_789.lock().unwrap().allocate_page()?);

        Ok(())
    }

    #[test]
    fn cluster_databases_are_isolated_write_does_not_leak() -> io::Result<()> {
        let temp_dir = tempdir()?;
        let cluster = DatabaseCluster::new(temp_dir.path(), PAGE_SIZE)?;

        let db_123 = cluster.open_database(DatabaseId(123))?;
        let db_456 = cluster.open_database(DatabaseId(456))?;

        let mut mgr_123 = db_123.lock().unwrap();
        simple_write(&mut mgr_123, "hello")?;

        let mut mgr_456 = db_456.lock().unwrap();
        simple_write(&mut mgr_456, "world")?;

        // Verify database 123 has "hello" and not "world"
        let content_123 = mgr_123.read_page(PageId(0))?;
        assert_eq!(
            &content_123.data[0..5],
            b"hello",
            "Database 123 should contain 'hello'"
        );

        // Verify database 456 has "world" and not "hello"
        let content_456 = mgr_456.read_page(PageId(0))?;
        assert_eq!(
            &content_456.data[0..5],
            b"world",
            "Database 456 should contain 'world'"
        );

        Ok(())
    }

    #[test]
    fn cluster_reopen_recovers_previously_opened_databases() -> io::Result<()> {
        let temp_dir = tempdir()?;
        let cluster = DatabaseCluster::new(temp_dir.path(), PAGE_SIZE)?;
        let db_id = DatabaseId(123);

        // Open, write some data, then close the DB manager
        {
            let db = cluster.open_database(db_id)?;
            let mut manager = db.lock().unwrap();
            simple_write(&mut manager, "reopen")?;
        }
        cluster.close_database(db_id);

        // Re-open and verify the content
        let db_reopened = cluster.open_database(db_id)?;
        let mut manager_reopened = db_reopened.lock().unwrap();
        let page = manager_reopened.read_page(PageId(0))?;

        let content = "reopen";
        assert_eq!(
            &page.data[0..content.len()],
            content.as_bytes(),
            "data should persist after closing and reopening"
        );

        Ok(())
    }

    /// Close database removes it from open cache
    #[test]
    fn cluster_close_database_removes_from_cache() -> io::Result<()> {
        let temp_dir = tempdir()?;
        let cluster = DatabaseCluster::new(temp_dir.path(), PAGE_SIZE)?;
        let db_id = DatabaseId(42);

        cluster.open_database(db_id)?;
        assert_eq!(cluster.list_open_databses().len(), 1);

        cluster.close_database(db_id);
        assert!(
            cluster.list_open_databses().is_empty(),
            "closed database should be removed from open cache"
        );

        Ok(())
    }

    /// Re-opening after close re-creates fresh manager
    #[test]
    fn cluster_reopen_after_close_gives_new_instance() -> io::Result<()> {
        let temp_dir = tempdir()?;
        let cluster = DatabaseCluster::new(temp_dir.path(), PAGE_SIZE)?;
        let db_id = DatabaseId(42);

        let first = cluster.open_database(db_id)?;
        cluster.close_database(db_id);
        let second = cluster.open_database(db_id)?;

        assert!(
            !Arc::ptr_eq(&first, &second),
            "re-opening after close should return a fresh Arc<Mutex<StorageManager>>"
        );

        Ok(())
    }

    /// List open databases returns correct set of ids
    #[test]
    fn cluster_list_open_databases_returns_expected_ids() -> io::Result<()> {
        let temp_dir = tempdir()?;
        let cluster = DatabaseCluster::new(temp_dir.path(), PAGE_SIZE)?;

        let id_1 = DatabaseId(101);
        let id_2 = DatabaseId(202);
        let id_3 = DatabaseId(303);

        cluster.open_database(id_1)?;
        cluster.open_database(id_2)?;
        cluster.open_database(id_3)?;
        cluster.close_database(id_2);

        let open_ids = cluster.list_open_databses();
        assert_eq!(open_ids.len(), 2);
        assert!(open_ids.contains(&id_1));
        assert!(open_ids.contains(&id_3));
        assert!(!open_ids.contains(&id_2));

        Ok(())
    }

    fn simple_write(manager: &mut StorageManager, content: &str) -> io::Result<()> {
        let page_id = manager.allocate_page()?;
        let mut page = Page::new(page_id, manager.page_size()?);
        page.data[0..content.len()].copy_from_slice(content.as_bytes());
        manager.write_page(&page)?;
        manager.sync_data()?;
        Ok(())
    }
}
