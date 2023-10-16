use ockam_core::errcode::{Kind, Origin};
use ockam_core::Result;
use ockam_core::{async_trait, Error};

use crate::models::{ChangeHistory, Identifier};
use crate::Identity;

/// This repository stores identity change histories
#[async_trait]
pub trait ChangeHistoryRepository: Send + Sync + 'static {
    /// Store changes if there are new key changes associated to that identity
    async fn store_identity(&self, identity: &Identity) -> Result<()>;

    /// Store changes if there are new key changes associated to that identity
    async fn update_identity(&self, identity: &Identity) -> Result<()>;

    /// Delete an identity given its identifier
    async fn delete_identity(&self, identifier: &Identifier) -> Result<()>;

    /// Return the change history of a persisted identity
    async fn get_change_history_optional(
        &self,
        identifier: &Identifier,
    ) -> Result<Option<ChangeHistory>>;

    /// Return the change history of a persisted identity
    async fn get_change_history(&self, identifier: &Identifier) -> Result<ChangeHistory> {
        match self.get_change_history_optional(identifier).await? {
            Some(change_history) => Ok(change_history),
            None => Err(Error::new(
                Origin::Core,
                Kind::NotFound,
                format!("identity not found for identifier {}", identifier),
            )),
        }
    }
}
