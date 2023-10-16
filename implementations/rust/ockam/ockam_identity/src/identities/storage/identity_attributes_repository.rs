use crate::{AttributesEntry, Identifier};
use async_trait::async_trait;
use ockam_core::Result;

/// Trait implementing read access to attributes
#[async_trait]
pub trait IdentityAttributesRepository: Send + Sync + 'static {
    /// Get the attributes associated with the given identity identifier
    async fn get_attributes(&self, identity: &Identifier) -> Result<Option<AttributesEntry>>;

    /// List all identities with their attributes
    async fn list(&self) -> Result<Vec<(Identifier, AttributesEntry)>>;

    /// Set the attributes associated with the given identity identifier.
    /// Previous values gets overridden.
    async fn put_attributes(&self, identity: &Identifier, entry: AttributesEntry) -> Result<()>;

    /// Store an attribute name/value pair for a given identity
    async fn put_attribute_value(
        &self,
        subject: &Identifier,
        attribute_name: Vec<u8>,
        attribute_value: Vec<u8>,
    ) -> Result<()>;

    /// Remove all attributes for a given identity identifier
    async fn delete(&self, identity: &Identifier) -> Result<()>;
}
