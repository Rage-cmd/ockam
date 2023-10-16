use core::str::FromStr;

use sqlx::*;

use ockam_core::async_trait;
use ockam_core::compat::sync::Arc;
use ockam_core::Result;
use ockam_node::database::{FromSqlxError, SqlxDatabase, SqlxType, ToSqlxType, ToVoid};

use crate::models::{ChangeHistory, Identifier};
use crate::{ChangeHistoryRepository, Identity, TimestampInSeconds};

/// Implementation of `IdentitiesRepository` trait based on an underlying database
/// using sqlx as its API, and Sqlite as its driver
#[derive(Clone)]
pub struct ChangeHistorySqlxDatabase {
    database: Arc<SqlxDatabase>,
}

impl ChangeHistorySqlxDatabase {
    /// Create a new database
    pub fn new(database: Arc<SqlxDatabase>) -> Self {
        Self { database }
    }

    /// Create a new in-memory database
    pub fn create() -> Arc<Self> {
        Arc::new(Self::new(Arc::new(SqlxDatabase::in_memory())))
    }
}

#[async_trait]
impl ChangeHistoryRepository for ChangeHistorySqlxDatabase {
    async fn store_identity(&self, identity: &Identity) -> Result<()> {
        let query = query("INSERT INTO identity VALUES (?, ?)")
            .bind(identity.identifier().to_sql())
            .bind(identity.change_history().to_sql());
        query.execute(&self.database.pool).await.void()
    }

    async fn update_identity(&self, identity: &Identity) -> Result<()> {
        let query = query("UPDATE identity SET change_history = ? WHERE identifier = ?")
            .bind(identity.change_history().to_sql())
            .bind(identity.identifier().to_sql());
        query.execute(&self.database.pool).await.void()
    }

    async fn delete_identity(&self, identifier: &Identifier) -> Result<()> {
        let transaction = self.database.pool.acquire().await.into_core()?;
        let query1 = query("DELETE FROM identity where identifier=?").bind(identifier.to_sql());
        query1.execute(&self.database.pool).await.void()?;

        let query2 =
            query("DELETE FROM identity_attributes where identifier=?").bind(identifier.to_sql());
        query2.execute(&self.database.pool).await.void()?;
        transaction.close().await.into_core()?;
        Ok(())
    }

    async fn get_change_history_optional(
        &self,
        identifier: &Identifier,
    ) -> Result<Option<ChangeHistory>> {
        let query =
            query_as("SELECT * FROM identity WHERE identifier=$1").bind(identifier.to_sql());
        let row: Option<ChangeHistoryRow> = query
            .fetch_optional(&self.database.pool)
            .await
            .into_core()?;
        row.map(|r| r.change_history()).transpose()
    }
}

impl ToSqlxType for Identifier {
    fn to_sql(&self) -> SqlxType {
        self.to_string().to_sql()
    }
}

impl ToSqlxType for TimestampInSeconds {
    fn to_sql(&self) -> SqlxType {
        self.0.to_sql()
    }
}

impl ToSqlxType for ChangeHistory {
    fn to_sql(&self) -> SqlxType {
        self.export().unwrap().to_sql()
    }
}

#[derive(sqlx::FromRow)]
pub(crate) struct ChangeHistoryRow {
    identifier: String,
    change_history: Vec<u8>,
}

impl ChangeHistoryRow {
    #[allow(dead_code)]
    pub(crate) fn identifier(&self) -> Result<Identifier> {
        Identifier::from_str(&self.identifier)
    }

    pub(crate) fn change_history(&self) -> Result<ChangeHistory> {
        ChangeHistory::import(self.change_history.as_slice())
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use tempfile::NamedTempFile;

    use crate::{Identity, Vault};

    use super::*;

    #[tokio::test]
    async fn test_identities_repository() -> Result<()> {
        let identity1 = create_identity1().await?;
        let identity2 = create_identity2().await?;
        let db_file = NamedTempFile::new().unwrap();
        let repository = create_repository(db_file.path()).await?;

        // store and retrieve or get an identity
        repository.store_identity(&identity1).await?;

        // the change history can be retrieved as an Option
        let result = repository
            .get_change_history_optional(&identity1.identifier())
            .await?;
        assert_eq!(result, Some(identity1.change_history().clone()));

        // trying to retrieve a missing identity returns None
        let result = repository
            .get_change_history_optional(&identity2.identifier())
            .await?;
        assert_eq!(result, None);

        // get returns an error if an identity is not found
        let result = repository
            .get_change_history(&identity1.identifier())
            .await?;
        assert_eq!(result, identity1.change_history().clone());

        let result = repository.get_change_history(&identity2.identifier()).await;
        assert!(result.is_err());
        Ok(())
    }

    /// HELPERS
    async fn create_identity1() -> Result<Identity> {
        let change_history = ChangeHistory::import(&hex::decode("81a201583ba20101025835a4028201815820530d1c2e9822433b679a66a60b9c2ed47c370cd0ce51cbe1a7ad847b5835a96303f4041a64dd4060051a77a94360028201815840042fff8f6c80603fb1cec4a3cf1ff169ee36889d3ed76184fe1dfbd4b692b02892df9525c61c2f1286b829586d13d5abf7d18973141f734d71c1840520d40a0e").unwrap())?;
        Identity::import_from_change_history(None, change_history, Vault::create_verifying_vault())
            .await
    }

    async fn create_identity2() -> Result<Identity> {
        let change_history = ChangeHistory::import(&hex::decode("81a201583ba20101025835a4028201815820afbca9cf5d440147450f9f0d0a038a337b3fe5c17086163f2c54509558b62ef403f4041a64dd404a051a77a9434a0282018158407754214545cda6e7ff49136f67c9c7973ec309ca4087360a9f844aac961f8afe3f579a72c0c9530f3ff210f02b7c5f56e96ce12ee256b01d7628519800723805").unwrap())?;
        Identity::import_from_change_history(None, change_history, Vault::create_verifying_vault())
            .await
    }

    async fn create_repository(path: &Path) -> Result<Arc<dyn ChangeHistoryRepository>> {
        let db = SqlxDatabase::create(path).await?;
        Ok(Arc::new(ChangeHistorySqlxDatabase::new(Arc::new(db))))
    }
}
