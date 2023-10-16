use crate::identities::{ChangeHistoryRepository, IdentitiesKeys};
use crate::purpose_keys::storage::{PurposeKeysRepository, PurposeKeysSqlxDatabase};
use crate::{
    ChangeHistorySqlxDatabase, Credentials, CredentialsServer, CredentialsServerModule, Identifier,
    IdentitiesBuilder, IdentitiesCreation, Identity, IdentityAttributesRepository,
    IdentityAttributesSqlxDatabase, PurposeKeys, Vault,
};

use ockam_core::compat::sync::Arc;
use ockam_core::compat::vec::Vec;
use ockam_core::Result;

/// This struct supports all the services related to identities
#[derive(Clone)]
pub struct Identities {
    vault: Vault,
    change_history_repository: Arc<dyn ChangeHistoryRepository>,
    identity_attributes_repository: Arc<dyn IdentityAttributesRepository>,
    purpose_keys_repository: Arc<dyn PurposeKeysRepository>,
}

impl Identities {
    /// Vault
    pub fn vault(&self) -> Vault {
        self.vault.clone()
    }

    /// Return the identities repository
    pub fn change_history_repository(&self) -> Arc<dyn ChangeHistoryRepository> {
        self.change_history_repository.clone()
    }

    /// Return the identity attributes repository
    pub fn identity_attributes_repository(&self) -> Arc<dyn IdentityAttributesRepository> {
        self.identity_attributes_repository.clone()
    }

    /// Return the purpose keys repository
    pub fn purpose_keys_repository(&self) -> Arc<dyn PurposeKeysRepository> {
        self.purpose_keys_repository.clone()
    }

    /// Get an [`Identity`] from the repository
    pub async fn get_identity(&self, identifier: &Identifier) -> Result<Identity> {
        let change_history = self
            .change_history_repository
            .get_change_history(identifier)
            .await?;
        Identity::import_from_change_history(
            Some(identifier),
            change_history,
            self.vault.verifying_vault.clone(),
        )
        .await
    }

    /// Export an [`Identity`] from the repository
    pub async fn export_identity(&self, identifier: &Identifier) -> Result<Vec<u8>> {
        self.get_identity(identifier).await?.export()
    }

    /// Return the [`PurposeKeys`] instance
    pub fn purpose_keys(&self) -> Arc<PurposeKeys> {
        Arc::new(PurposeKeys::new(
            self.vault.clone(),
            self.change_history_repository.clone(),
            self.identities_keys(),
            self.purpose_keys_repository.clone(),
        ))
    }

    /// Return the identities keys management service
    pub fn identities_keys(&self) -> Arc<IdentitiesKeys> {
        Arc::new(IdentitiesKeys::new(
            self.vault.identity_vault.clone(),
            self.vault.verifying_vault.clone(),
        ))
    }

    /// Return the identities creation service
    pub fn identities_creation(&self) -> Arc<IdentitiesCreation> {
        Arc::new(IdentitiesCreation::new(
            self.change_history_repository(),
            self.vault.identity_vault.clone(),
            self.vault.verifying_vault.clone(),
        ))
    }

    /// Return the identities credentials service
    pub fn credentials(&self) -> Arc<Credentials> {
        Arc::new(Credentials::new(
            self.vault.credential_vault.clone(),
            self.vault.verifying_vault.clone(),
            self.purpose_keys(),
            self.change_history_repository.clone(),
            self.identity_attributes_repository.clone(),
        ))
    }

    /// Return the identities credentials server
    pub fn credentials_server(&self) -> Arc<dyn CredentialsServer> {
        Arc::new(CredentialsServerModule::new(self.credentials()))
    }
}

impl Identities {
    /// Create a new identities module
    pub(crate) fn new(
        vault: Vault,
        change_history_repository: Arc<dyn ChangeHistoryRepository>,
        identity_attributes_repository: Arc<dyn IdentityAttributesRepository>,
        purpose_keys_repository: Arc<dyn PurposeKeysRepository>,
    ) -> Identities {
        Identities {
            vault,
            change_history_repository,
            identity_attributes_repository,
            purpose_keys_repository,
        }
    }

    /// Return a default builder for identities
    pub fn builder() -> IdentitiesBuilder {
        IdentitiesBuilder {
            vault: Vault::create(),
            change_history_repository: ChangeHistorySqlxDatabase::create(),
            identity_attributes_repository: IdentityAttributesSqlxDatabase::create(),
            purpose_keys_repository: PurposeKeysSqlxDatabase::create(),
        }
    }
}
