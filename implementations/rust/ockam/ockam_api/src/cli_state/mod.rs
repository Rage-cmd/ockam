use std::path::{Path, PathBuf};

use miette::Diagnostic;
use rand::random;
use thiserror::Error;

use ockam::identity::storage::{PurposeKeysRepository, PurposeKeysSqlxDatabase};
use ockam::identity::{
    ChangeHistoryRepository, ChangeHistorySqlxDatabase, Identities, IdentityAttributesRepository,
    IdentityAttributesSqlxDatabase, Vault,
};
use ockam::SqlxDatabase;
use ockam_abac::{PoliciesRepository, PolicySqlxDatabase};
use ockam_core::compat::sync::Arc;
use ockam_core::env::get_env_with_default;
use ockam_core::errcode::{Kind, Origin};
use ockam_node::Executor;

pub use crate::cli_state::credentials::*;
use crate::cli_state::enrollment::{EnrollmentsRepository, EnrollmentsSqlxDatabase};
pub use crate::cli_state::nodes::*;
pub use crate::cli_state::projects::*;
pub use crate::cli_state::spaces::*;
pub use crate::cli_state::traits::*;
pub use crate::cli_state::trust_contexts::*;
use crate::cli_state::user_info::UsersInfoState;
pub use crate::cli_state::vaults::*;
use crate::identity::{
    IdentitiesRepository, IdentitiesSqlxDatabase, NamedVault, VaultsRepository, VaultsSqlxDatabase,
};
use crate::nodes::{NodesRepository, NodesSqlxDatabase};

pub mod credentials;
pub mod enrollment;
pub mod identities;
pub mod nodes;
pub mod projects;
pub mod spaces;
pub mod traits;
pub mod trust_contexts;
pub mod user_info;
pub mod vaults;

type Result<T> = std::result::Result<T, CliStateError>;

#[derive(Debug, Error, Diagnostic)]
pub enum CliStateError {
    #[error(transparent)]
    #[diagnostic(code("OCK500"))]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    #[diagnostic(code("OCK500"))]
    Serde(#[from] serde_json::Error),

    #[error(transparent)]
    #[diagnostic(code("OCK500"))]
    Ockam(#[from] ockam_core::Error),

    #[error("A {resource} named {name} already exists")]
    #[diagnostic(
        code("OCK409"),
        help("Please try using a different name or delete the existing {resource}")
    )]
    AlreadyExists { resource: String, name: String },

    #[error("Unable to find {resource} named {name}")]
    #[diagnostic(code("OCK404"))]
    ResourceNotFound { resource: String, name: String },

    #[error("The path {0} is invalid")]
    #[diagnostic(code("OCK500"))]
    InvalidPath(String),

    #[error("The path is empty")]
    #[diagnostic(code("OCK500"))]
    EmptyPath,

    #[error("{0}")]
    #[diagnostic(code("OCK500"))]
    InvalidData(String),

    #[error("{0}")]
    #[diagnostic(code("OCK500"))]
    InvalidOperation(String),

    #[error("Invalid configuration version '{0}'")]
    #[diagnostic(
        code("OCK500"),
        help("Please try running 'ockam reset' to reset your local configuration")
    )]
    InvalidVersion(String),
}

impl From<&str> for CliStateError {
    fn from(e: &str) -> Self {
        CliStateError::InvalidOperation(e.to_string())
    }
}

impl From<CliStateError> for ockam_core::Error {
    fn from(e: CliStateError) -> Self {
        match e {
            CliStateError::Ockam(e) => e,
            _ => ockam_core::Error::new(
                ockam_core::errcode::Origin::Application,
                ockam_core::errcode::Kind::Internal,
                e,
            ),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct CliState {
    pub vaults: VaultsState,
    pub spaces: SpacesState,
    pub projects: ProjectsState,
    pub credentials: CredentialsState,
    pub trust_contexts: TrustContextsState,
    pub users_info: UsersInfoState,
    pub dir: PathBuf,
}

impl CliState {
    /// Return an initialized CliState
    /// There should only be one call to this function since it also performs a migration
    /// of configuration files if necessary
    pub fn initialize() -> Result<Self> {
        let dir = Self::default_dir()?;
        std::fs::create_dir_all(dir.join("defaults"))?;
        Executor::execute_future(Self::initialize_cli_state())?
    }

    /// Create a new CliState by initializing all of its components
    /// The calls to 'init(dir)' are loading each piece of configuration and possibly doing some
    /// configuration migration if necessary
    async fn initialize_cli_state() -> Result<CliState> {
        let default = Self::default_dir()?;
        let dir = default.as_path();
        let state = Self {
            vaults: VaultsState::init(dir).await?,
            spaces: SpacesState::init(dir).await?,
            projects: ProjectsState::init(dir).await?,
            credentials: CredentialsState::init(dir).await?,
            trust_contexts: TrustContextsState::init(dir).await?,
            users_info: UsersInfoState::init(dir).await?,
            dir: dir.to_path_buf(),
        };
        Ok(state)
    }

    pub async fn change_history_repository(&self) -> Result<Arc<dyn ChangeHistoryRepository>> {
        Ok(Arc::new(ChangeHistorySqlxDatabase::new(
            self.database().await?,
        )))
    }

    pub async fn identity_attributes_repository(
        &self,
    ) -> Result<Arc<dyn IdentityAttributesRepository>> {
        Ok(Arc::new(IdentityAttributesSqlxDatabase::new(
            self.database().await?,
        )))
    }

    pub async fn identities_repository(&self) -> Result<Arc<dyn IdentitiesRepository>> {
        Ok(Arc::new(IdentitiesSqlxDatabase::new(
            self.database().await?,
        )))
    }

    pub async fn purpose_keys_repository(&self) -> Result<Arc<dyn PurposeKeysRepository>> {
        Ok(Arc::new(PurposeKeysSqlxDatabase::new(
            self.database().await?,
        )))
    }

    pub async fn vaults_repository(&self) -> Result<Arc<dyn VaultsRepository>> {
        Ok(Arc::new(VaultsSqlxDatabase::new(self.database().await?)))
    }

    async fn enrollment_repository(&self) -> Result<Arc<dyn EnrollmentsRepository>> {
        Ok(Arc::new(EnrollmentsSqlxDatabase::new(
            self.database().await?,
        )))
    }

    async fn nodes_repository(&self) -> Result<Arc<dyn NodesRepository>> {
        Ok(Arc::new(NodesSqlxDatabase::new(self.database().await?)))
    }

    pub async fn policies_repository(&self) -> Result<Arc<dyn PoliciesRepository>> {
        Ok(Arc::new(PolicySqlxDatabase::new(self.database().await?)))
    }

    pub async fn database(&self) -> Result<Arc<SqlxDatabase>> {
        Ok(Arc::new(SqlxDatabase::create(self.database_path()).await?))
    }

    pub fn database_path(&self) -> PathBuf {
        self.dir.join("database.sqlite3")
    }

    pub fn node_stdout_log(&self, node_name: &str) -> Result<PathBuf> {
        Ok(self.node_dir(node_name)?.join("stdout.log"))
    }

    pub fn node_stderr_log(&self, node_name: &str) -> Result<PathBuf> {
        Ok(self.node_dir(node_name)?.join("stderr.log"))
    }

    pub fn node_dir(&self, node_name: &str) -> Result<PathBuf> {
        let path = self.dir.join("nodes").join(node_name);
        std::fs::create_dir_all(&path)?;
        Ok(path)
    }

    pub async fn get_identities(&self, vault: Vault) -> Result<Arc<Identities>> {
        Ok(Identities::builder()
            .with_vault(vault)
            .with_change_history_repository(self.change_history_repository().await?)
            .with_purpose_keys_repository(self.purpose_keys_repository().await?)
            .build())
    }

    pub async fn get_vault(&self, vault_name: &str) -> Result<NamedVault> {
        let result = self
            .vaults_repository()
            .await?
            .get_vault_by_name(vault_name)
            .await?;
        result.ok_or_else(|| {
            ockam_core::Error::new(
                Origin::Api,
                Kind::NotFound,
                format!("no vault found with name {vault_name}"),
            )
            .into()
        })
    }

    pub async fn get_default_vault(&self) -> Result<NamedVault> {
        let result = self.vaults_repository().await?.get_default_vault().await?;
        result.ok_or_else(|| {
            ockam_core::Error::new(
                Origin::Api,
                Kind::NotFound,
                format!("no default vault found"),
            )
            .into()
        })
    }

    pub async fn get_default_vault_name(&self) -> Result<String> {
        let result = self
            .vaults_repository()
            .await?
            .get_default_vault_name()
            .await?;
        result.ok_or_else(|| {
            ockam_core::Error::new(
                Origin::Api,
                Kind::NotFound,
                format!("no default vault found"),
            )
            .into()
        })
    }

    /// fault identity but if it has not been initialized yet
    // /// then initialize it
    // pub async fn initialize_identity_if_default(opts: &CommandGlobalOpts, name: &Option<String>) {
    //     let name = get_identity_name(&opts.state, name).await?;
    //     if name == "default" && opts.state.identities.default().is_err() {
    //         create_default_identity(opts);
    //     }
    // }
    //
    //
    // /// Create the default identity
    // fn create_default_identity(opts: &CommandGlobalOpts) {
    //     let default = "default";
    //     let create_command = CreateCommand::new(default.into(), None);
    //     create_command.run(opts.clone().set_quiet());
    //
    //     // Retrieve the identifier if available
    //     // Otherwise, use the name of the identity
    //     let identifier = match opts.state.identities.get(default) {
    //         Ok(i) => i.identifier().to_string(),
    //         Err(_) => default.to_string(),
    //     };
    //
    //     if let Ok(mut logs) = PARSER_LOGS.lock() {
    //         logs.push(fmt_log!(
    //             "There is no identity, on this machine, marked as your default."
    //         ));
    //         logs.push(fmt_log!("Creating a new Ockam identity for you..."));
    //         logs.push(fmt_ok!(
    //             "Created: {}",
    //             identifier.color(OckamColor::PrimaryResource.color())
    //         ));
    //         logs.push(fmt_log!(
    //             "Marked this new identity as your default, on this machine.\n"
    //         ));
    //     }
    // }

    /// Reset all directories and return a new CliState
    pub async fn reset(&self) -> Result<CliState> {
        Self::delete_at(&self.dir)?;
        Self::initialize_cli_state().await
    }

    pub fn backup_and_reset() -> Result<CliState> {
        let dir = Self::default_dir()?;

        // Reset backup directory
        let backup_dir = Self::backup_default_dir()?;
        if backup_dir.exists() {
            let _ = std::fs::remove_dir_all(&backup_dir);
        }
        std::fs::create_dir_all(&backup_dir)?;

        // Move state to backup directory
        for entry in std::fs::read_dir(&dir)? {
            let entry = entry?;
            let from = entry.path();
            let to = backup_dir.join(entry.file_name());
            std::fs::rename(from, to)?;
        }

        // Reset state
        Self::delete_at(&dir)?;
        let state = Self::initialize()?;

        let dir = &state.dir;
        let backup_dir = CliState::backup_default_dir().unwrap();
        eprintln!("The {dir:?} directory has been reset and has been backed up to {backup_dir:?}");
        Ok(state)
    }

    pub fn delete_at(root_path: &PathBuf) -> Result<()> {
        // Delete nodes' state and processes, if possible
        // let nodes_state = NodesState::new(root_path);
        // let _ = nodes_state.list().map(|nodes| {
        //     nodes.iter().for_each(|n| {
        //         let _ = n.delete_sigkill(true);
        //     });
        // });

        // Delete all other state directories
        for dir in &[
            VaultsState::new(root_path).dir(),
            SpacesState::new(root_path).dir(),
            ProjectsState::new(root_path).dir(),
            CredentialsState::new(root_path).dir(),
            TrustContextsState::new(root_path).dir(),
            UsersInfoState::new(root_path).dir(),
            &root_path.join("defaults"),
        ] {
            let _ = std::fs::remove_dir_all(dir);
        }

        // Delete config files located at the root of the state directory
        let config_file = root_path.join("config.json");
        let _ = std::fs::remove_file(config_file);

        // If the state directory is now empty, delete it
        let is_empty = std::fs::read_dir(root_path)
            .map(|mut d| d.next().is_none())
            .unwrap_or(false);
        if is_empty {
            let _ = std::fs::remove_dir(root_path);
        }

        Ok(())
    }

    pub fn delete() -> Result<()> {
        Self::delete_at(&Self::default_dir()?)
    }

    /// Returns the default directory for the CLI state.
    fn default_dir() -> Result<PathBuf> {
        Ok(get_env_with_default::<PathBuf>(
            "OCKAM_HOME",
            home::home_dir()
                .ok_or(CliStateError::InvalidPath("$HOME".to_string()))?
                .join(".ockam"),
        )?)
    }

    /// Returns the default backup directory for the CLI state.
    fn backup_default_dir() -> Result<PathBuf> {
        let dir = Self::default_dir()?;
        let dir_name =
            dir.file_name()
                .and_then(|n| n.to_str())
                .ok_or(CliStateError::InvalidOperation(
                    "The $OCKAM_HOME directory does not have a valid name".to_string(),
                ))?;
        let parent = dir.parent().ok_or(CliStateError::InvalidOperation(
            "The $OCKAM_HOME directory does not a valid parent directory".to_string(),
        ))?;
        Ok(parent.join(format!("{dir_name}.bak")))
    }

    /// Returns the directory where the default objects are stored.
    fn defaults_dir(dir: &Path) -> Result<PathBuf> {
        Ok(dir.join("defaults"))
    }

    pub async fn create_vault_state(&self, vault_name: Option<&str>) -> Result<VaultState> {
        // Try to get the vault with the given name
        let vault_state = if let Some(v) = vault_name {
            self.vaults.get(v)?
        }
        // Or get the default
        else if let Ok(v) = self.vaults.default() {
            v
        }
        // Or create a new one with a random name
        else {
            let n = random_name();
            let c = VaultConfig::default();
            self.vaults.create_async(&n, c).await?
        };
        Ok(vault_state)
    }

    /// Return true if the user is enrolled.
    /// At the moment this check only verifies that there is a default project.
    /// This project should be the project that is created at the end of the enrollment procedure
    pub async fn is_enrolled(&self) -> miette::Result<bool> {
        if !self.is_default_identity_enrolled().await? {
            return Ok(false);
        }

        let default_space_exists = self.spaces.default().is_ok();
        if !default_space_exists {
            let message =
                "There should be a default space set for the current user. Please re-enroll";
            error!("{}", message);
            return Err(CliStateError::from(message).into());
        }

        let default_project_exists = self.projects.default().is_ok();
        if !default_project_exists {
            let message =
                "There should be a default project set for the current user. Please re-enroll";
            error!("{}", message);
            return Err(CliStateError::from(message).into());
        }

        Ok(true)
    }
}

/// Test support
impl CliState {
    #[cfg(test)]
    /// Initialize CliState at the given directory
    async fn initialize_at(dir: &Path) -> Result<Self> {
        std::fs::create_dir_all(dir.join("defaults"))?;
        let state = Self {
            vaults: VaultsState::init(dir).await?,
            spaces: SpacesState::init(dir).await?,
            projects: ProjectsState::init(dir).await?,
            credentials: CredentialsState::init(dir).await?,
            trust_contexts: TrustContextsState::init(dir).await?,
            users_info: UsersInfoState::init(dir).await?,
            dir: dir.to_path_buf(),
        };
        Ok(state)
    }

    /// Create a new CliState (but do not run migrations)
    fn new(dir: &Path) -> Result<Self> {
        std::fs::create_dir_all(dir.join("defaults"))?;
        Ok(Self {
            vaults: VaultsState::load(dir)?,
            spaces: SpacesState::load(dir)?,
            projects: ProjectsState::load(dir)?,
            credentials: CredentialsState::load(dir)?,
            trust_contexts: TrustContextsState::load(dir)?,
            users_info: UsersInfoState::load(dir)?,
            dir: dir.to_path_buf(),
        })
    }

    /// Return a test CliState with a random root directory
    pub fn test() -> Result<Self> {
        Self::new(&Self::test_dir()?)
    }

    /// Return a random root directory
    pub fn test_dir() -> Result<PathBuf> {
        Ok(home::home_dir()
            .ok_or(CliStateError::InvalidPath("$HOME".to_string()))?
            .join(".ockam")
            .join(".tests")
            .join(random_name()))
    }
}

pub fn random_name() -> String {
    petname::petname(2, "-").unwrap_or(hex::encode(random::<[u8; 4]>()))
}

fn file_stem(path: &Path) -> Result<String> {
    let path_str = path.to_str().ok_or(CliStateError::EmptyPath)?;
    path.file_stem()
        .ok_or(CliStateError::InvalidPath(path_str.to_string()))?
        .to_str()
        .map(|name| name.to_string())
        .ok_or(CliStateError::InvalidPath(path_str.to_string()))
}

#[cfg(test)]
mod tests {
    use ockam_core::compat::rand::random_string;

    use crate::cloud::enroll::auth0::UserInfo;
    use crate::config::cli::TrustContextConfig;

    use super::*;

    #[ockam_macros::test(crate = "ockam")]
    async fn integration(ctx: &mut ockam::Context) -> ockam::Result<()> {
        let sut = CliState::test()?;

        // Vaults
        let vault_name = {
            let name = random_name();
            let config = VaultConfig::default();

            let state = sut.vaults.create_async(&name, config).await.unwrap();
            let got = sut.vaults.get(&name).unwrap();
            assert_eq!(got, state);

            let got = sut.vaults.default().unwrap();
            assert_eq!(got, state);

            name
        };

        // Nodes
        let node_name = {
            let name = random_name();

            let node_info = sut.create_node(&name).await.unwrap();
            let got = sut.get_node(&name).await.unwrap();
            assert_eq!(got, node_info);

            let got = sut.get_default_node().await.unwrap();
            assert_eq!(got, node_info);

            name
        };

        // Spaces
        let space_name = {
            let name = random_name();
            let id = random_string();
            let config = SpaceConfig {
                name: name.clone(),
                id,
            };

            let state = sut.spaces.create(&name, config).unwrap();
            let got = sut.spaces.get(&name).unwrap();
            assert_eq!(got, state);

            name
        };

        // Projects
        let project_name = {
            let name = random_name();
            let config = ProjectConfig::default();

            let state = sut.projects.create(&name, config).unwrap();
            let got = sut.projects.get(&name).unwrap();
            assert_eq!(got, state);

            name
        };

        // Trust Contexts
        let trust_context_name = {
            let name = random_name();
            let config = TrustContextConfig::new(name.to_string(), None);

            let state = sut.trust_contexts.create(&name, config).unwrap();
            let got = sut.trust_contexts.get(&name).unwrap();
            assert_eq!(got, state);

            name
        };

        // Users Info
        let user_info_email = {
            let email = random_name();
            let config = UserInfo {
                email: email.clone(),
                ..Default::default()
            };

            let state = sut.users_info.create(&email, config).unwrap();
            let got = sut.users_info.get(&email).unwrap();
            assert_eq!(got, state);

            email
        };

        // Check structure
        let mut expected_entries = vec![
            "vaults".to_string(),
            format!("vaults/{vault_name}.json"),
            "vaults/data".to_string(),
            format!("vaults/data/{vault_name}-storage.json"),
            "nodes".to_string(),
            format!("nodes/{node_name}"),
            "spaces".to_string(),
            format!("spaces/{space_name}.json"),
            "projects".to_string(),
            format!("projects/{project_name}.json"),
            "trust_contexts".to_string(),
            format!("trust_contexts/{trust_context_name}.json"),
            "users_info".to_string(),
            format!("users_info/{user_info_email}.json"),
            "credentials".to_string(),
            "defaults".to_string(),
            "defaults/vault".to_string(),
            "defaults/identity".to_string(),
            "defaults/node".to_string(),
            "defaults/space".to_string(),
            "defaults/project".to_string(),
            "defaults/trust_context".to_string(),
            "defaults/user_info".to_string(),
        ];
        expected_entries.sort();
        let mut found_entries = vec![];
        sut.dir.read_dir().unwrap().for_each(|entry| {
            let entry = entry.unwrap();
            let dir_name = entry.file_name().into_string().unwrap();
            match dir_name.as_str() {
                "vaults" => {
                    assert!(entry.path().is_dir());
                    found_entries.push(dir_name.clone());
                    entry.path().read_dir().unwrap().for_each(|entry| {
                        let entry = entry.unwrap();
                        let entry_name = entry.file_name().into_string().unwrap();
                        found_entries.push(format!("{dir_name}/{entry_name}"));
                        if entry.path().is_dir() {
                            assert_eq!(entry_name, DATA_DIR_NAME);
                            entry.path().read_dir().unwrap().for_each(|entry| {
                                let entry = entry.unwrap();
                                let file_name = entry.file_name().into_string().unwrap();
                                if !file_name.ends_with(".lock") {
                                    found_entries
                                        .push(format!("{dir_name}/{entry_name}/{file_name}"));
                                    assert_eq!(file_name, format!("{vault_name}-storage.json"));
                                }
                            });
                        } else {
                            assert_eq!(entry_name, format!("{vault_name}.json"));
                        }
                    });
                }
                "nodes" => {
                    assert!(entry.path().is_dir());
                    found_entries.push(dir_name.clone());
                    entry.path().read_dir().unwrap().for_each(|entry| {
                        let entry = entry.unwrap();
                        assert!(entry.path().is_dir());
                        let file_name = entry.file_name().into_string().unwrap();
                        found_entries.push(format!("{dir_name}/{file_name}"));
                    });
                }
                "defaults" | "spaces" | "projects" | "credentials" | "trust_contexts"
                | "users_info" => {
                    assert!(entry.path().is_dir());
                    found_entries.push(dir_name.clone());
                    entry.path().read_dir().unwrap().for_each(|entry| {
                        let entry = entry.unwrap();
                        let entry_name = entry.file_name().into_string().unwrap();
                        found_entries.push(format!("{dir_name}/{entry_name}"));
                    });
                }
                _ => panic!("unexpected file"),
            }
        });
        found_entries.sort();
        assert_eq!(expected_entries, found_entries);

        sut.spaces.delete(&space_name).unwrap();
        sut.projects.delete(&project_name).unwrap();
        sut.vaults.delete(&vault_name).unwrap();

        ctx.stop().await?;
        Ok(())
    }
}
