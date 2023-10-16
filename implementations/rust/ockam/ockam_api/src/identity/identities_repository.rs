use ockam::identity::Identifier;
use ockam_core::async_trait;
use ockam_core::Result;

#[async_trait]
pub trait IdentitiesRepository: Send + Sync + 'static {
    /// Associate a name to an identity
    async fn name_identity(&self, identifier: &Identifier, name: &str) -> Result<()>;

    /// Set an identity as the default one
    async fn set_as_default(&self, identifier: &Identifier) -> Result<()>;

    /// Set an identity as the default one, given its name
    async fn set_as_default_by_name(&self, name: &str) -> Result<()>;

    /// Delete an identity given its name
    async fn delete_identity_by_name(&self, name: &str) -> Result<Option<Identifier>>;

    /// Return the identifier associated to a named identity
    async fn get_identifier_by_name(&self, name: &str) -> Result<Option<Identifier>>;

    /// Return the default identifier if there is one
    async fn get_default_identifier(&self) -> Result<Option<Identifier>>;

    /// Return identities which are associated with a name
    async fn get_named_identities(&self) -> Result<Vec<NamedIdentity>>;

    /// Return the named identity with a specific name
    async fn get_named_identity(&self, name: &str) -> Result<Option<NamedIdentity>>;

    /// Return the default named identity
    async fn get_default_named_identity(&self) -> Result<Option<NamedIdentity>>;

    /// Return the name of the default identity if there is one
    async fn get_default_identity_name(&self) -> Result<Option<String>>;

    /// Return true if there is an identity with this name and it is the default one
    async fn is_default_identity_by_name(&self, name: &str) -> Result<bool>;
}

/// A named identity associates a name with a persisted identity.
/// This is a convenience for users since they can refer to an identity by the name "alice"
/// instead of the identifier "I1234561234561234561234561234561234561234"
///
/// Additionally one identity can be marked as being the default identity and taken to
/// establish a secure channel or create credentials without having to specify it.
pub struct NamedIdentity {
    identifier: Identifier,
    name: String,
    is_default: bool,
}

impl NamedIdentity {
    /// Create a new named identity
    pub fn new(identifier: Identifier, name: String, is_default: bool) -> Self {
        Self {
            identifier,
            name,
            is_default,
        }
    }

    /// Return the identity identifier
    pub fn identifier(&self) -> Identifier {
        self.identifier.clone()
    }

    /// Return the identity name
    pub fn name(&self) -> String {
        self.name.clone()
    }

    /// Return true if this identity is the default one
    pub fn is_default(&self) -> bool {
        self.is_default
    }
}
