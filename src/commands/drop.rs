use crate::error::{Result, SearchDbError};
use crate::storage::Storage;

/// Delete an index and all its data from disk.
pub fn run(storage: &Storage, name: &str) -> Result<()> {
    if !storage.exists(name) {
        return Err(SearchDbError::IndexNotFound(name.to_string()));
    }

    storage.drop(name)?;
    eprintln!("[dsrch] Dropped '{name}'");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::new_index;

    #[test]
    fn test_drop_existing_index() {
        let dir = tempfile::tempdir().unwrap();
        let storage = Storage::new(dir.path().to_str().unwrap());
        new_index::run(
            &storage,
            "test",
            Some(r#"{"fields":{"name":"keyword"}}"#),
            false,
            None,
            false,
        )
        .unwrap();
        assert!(storage.exists("test"));

        run(&storage, "test").unwrap();
        assert!(!storage.exists("test"));
    }

    #[test]
    fn test_drop_nonexistent_index() {
        let dir = tempfile::tempdir().unwrap();
        let storage = Storage::new(dir.path().to_str().unwrap());
        let result = run(&storage, "nope");
        assert!(result.is_err());
    }
}
