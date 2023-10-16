use ockam::identity::{Identifier, Identity, Vault};
use ockam_core::errcode::{Kind, Origin};
use ockam_core::Error;

use crate::cli_state::{random_name, CliState, Result};
use crate::identity::NamedIdentity;

impl CliState {
    /// Create an identity associated with a name and a specific vault name
    pub async fn create_identity_with_name_and_vault(
        &self,
        name: &str,
        vault_name: &str,
    ) -> Result<Identifier> {
        let vault = self.get_vault(vault_name).await?;
        let identifier = self
            .create_identity_with_vault(vault.vault().await?)
            .await?;
        self.identities_repository()
            .await?
            .name_identity(&identifier, name)
            .await?;
        Ok(identifier)
    }

    /// Create an identity associated with a name and the default vault
    pub async fn create_identity_with_name(&self, name: &str) -> Result<Identifier> {
        let vault = self.get_default_vault().await?;
        let identifier = self
            .create_identity_with_vault(vault.vault().await?)
            .await?;
        self.identities_repository()
            .await?
            .name_identity(&identifier, name)
            .await?;
        Ok(identifier)
    }

    /// Create an identity associated with no name
    pub async fn create_identity(&self) -> Result<Identifier> {
        let vault = self.get_default_vault().await?;
        self.create_identity_with_vault(vault.vault().await?).await
    }

    /// Create an identity associated with no name
    async fn create_identity_with_vault(&self, vault: Vault) -> Result<Identifier> {
        Ok(self
            .get_identities(vault)
            .await?
            .identities_creation()
            .create_identity()
            .await?
            .identifier()
            .clone())
    }

    pub async fn create_identity_with_random_name(&self) -> Result<Identifier> {
        self.create_identity_with_name_and_vault(
            &random_name(),
            &self.get_default_vault_name().await?,
        )
        .await
    }

    pub async fn get_identifier_by_name(&self, name: &str) -> Result<Option<Identifier>> {
        Ok(self
            .identities_repository()
            .await?
            .get_identifier_by_name(name)
            .await?)
    }

    pub async fn get_named_identities(&self) -> Result<Vec<NamedIdentity>> {
        Ok(self
            .identities_repository()
            .await?
            .get_named_identities()
            .await?)
    }

    pub async fn get_identifier_by_optional_name(
        &self,
        name: &Option<String>,
    ) -> Result<Identifier> {
        let repository = self.identities_repository().await?;
        let result = match name {
            Some(name) => repository.get_identifier_by_name(name).await?,
            None => repository.get_default_identifier().await?,
        };

        result.ok_or_else(|| Self::missing_identifier(name).into())
    }

    pub async fn get_identifier_by_optional_name_or_create_identity(
        &self,
        name: &Option<String>,
    ) -> Result<Identifier> {
        let identifier = match name {
            Some(name) => {
                self.identities_repository()
                    .await?
                    .get_identifier_by_name(name)
                    .await?
            }

            None => {
                self.identities_repository()
                    .await?
                    .get_default_identifier()
                    .await?
            }
        };

        match identifier {
            Some(identifier) => Ok(identifier),
            None => match name {
                Some(name) => self.create_identity_with_name(name).await,
                None => self.create_identity().await,
            },
        }
    }

    pub async fn get_identity_by_optional_name(&self, name: &Option<String>) -> Result<Identity> {
        let named_identity = match name {
            Some(name) => {
                self.identities_repository()
                    .await?
                    .get_named_identity(name)
                    .await?
            }

            None => {
                self.identities_repository()
                    .await?
                    .get_default_named_identity()
                    .await?
            }
        };
        match named_identity {
            Some(identity) => {
                let change_history = self
                    .change_history_repository()
                    .await?
                    .get_change_history(&identity.identifier())
                    .await?;
                Ok(Identity::import_from_change_history(
                    Some(&identity.identifier()),
                    change_history,
                    self.get_default_vault()
                        .await?
                        .vault()
                        .await?
                        .verifying_vault,
                )
                .await?)
            }
            None => Err(Self::missing_identifier(name).into()),
        }
    }

    /// Return the name of the default identity
    pub async fn get_default_identity_name(&self) -> Result<String> {
        match self
            .identities_repository()
            .await?
            .get_default_identity_name()
            .await?
        {
            Some(name) => Ok(name),
            None => {
                Err(Error::new(Origin::Api, Kind::NotFound, "no default identity found").into())
            }
        }
    }

    /// Return:
    /// - the given name if defined
    /// - or the name of the default identity if it exists
    /// - or "default" to be used to create a default identity
    pub async fn get_identity_name_or_default(&self, name: &Option<String>) -> Result<String> {
        match name {
            Some(name) => Ok(name.clone()),
            None => Ok(match self.get_default_identity_name().await.ok() {
                Some(name) => name,
                None => "default".to_string(),
            }),
        }
    }

    /// Return true if there is an identity with that name and it is the default one
    pub async fn is_default_identity_by_name(&self, name: &str) -> Result<bool> {
        Ok(self
            .identities_repository()
            .await?
            .is_default_identity_by_name(name)
            .await?)
    }

    /// Return the name of the default identity
    pub async fn set_as_default_identity(&self, name: &str) -> Result<()> {
        Ok(self
            .identities_repository()
            .await?
            .set_as_default_by_name(name)
            .await?)
    }

    /// Delete an identity by name
    /// This first removes the name assocation to the identity then the identity change history
    pub async fn delete_identity_by_name(&self, name: &str) -> Result<()> {
        if let Some(identifier) = self
            .identities_repository()
            .await?
            .delete_identity_by_name(name)
            .await?
        {
            self.change_history_repository()
                .await?
                .delete_identity(&identifier)
                .await?;
        };
        Ok(())
    }

    fn missing_identifier(name: &Option<String>) -> Error {
        let message = name
            .clone()
            .map_or("no default identifier found".to_string(), |n| {
                format!("no identifier found with name {}", n)
            });
        Error::new(Origin::Api, Kind::NotFound, message).into()
    }
}
