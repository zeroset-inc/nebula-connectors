use serde::{Deserialize, Serialize};
use std::{
    fs::{File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
};
use zeroize::{Zeroize, Zeroizing};

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Credentials {
    pub endpoint: String,
    pub runner_id: Option<String>,
    pub secret: String,
    pub pending_secret: Option<String>,
    pub pairing: Option<Pairing>,
}
impl Drop for Credentials {
    fn drop(&mut self) {
        self.secret.zeroize();
        self.pending_secret.zeroize();
    }
}
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Pairing {
    pub pairing_id: String,
    pub user_code: String,
    pub verification_uri: Option<String>,
    pub expires_at: u64,
    pub interval: u64,
}
pub struct CredentialFile {
    path: PathBuf,
    _lock: File,
}
impl CredentialFile {
    pub fn open(path: PathBuf) -> Result<Self, String> {
        let parent = path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .ok_or("runner_state_path_invalid")?;
        let mut directory = std::fs::DirBuilder::new();
        directory.recursive(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            directory.mode(0o700);
        }
        directory
            .create(parent)
            .map_err(|_| "runner_state_unavailable")?;
        check_private(parent)?;
        let lock_path = path.with_extension("lock");
        let lock = private_open(&lock_path, false)?;
        lock.try_lock().map_err(|_| "runner_already_running")?;
        Ok(Self { path, _lock: lock })
    }
    pub fn read(&self) -> Result<Option<Credentials>, String> {
        if !self
            .path
            .try_exists()
            .map_err(|_| "runner_state_unavailable")?
        {
            return Ok(None);
        }
        check_private(&self.path)?;
        let mut bytes = Zeroizing::new(Vec::new());
        File::open(&self.path)
            .map_err(|_| "runner_state_unavailable")?
            .take(65537)
            .read_to_end(&mut bytes)
            .map_err(|_| "runner_state_unavailable")?;
        nebula_connector_sdk::validation::decode_control(&bytes, 65536)
            .map(Some)
            .map_err(|_| "runner_state_invalid".into())
    }
    pub fn save(&self, state: &Credentials) -> Result<(), String> {
        let bytes = Zeroizing::new(serde_json::to_vec(state).map_err(|_| "runner_state_invalid")?);
        if bytes.len() > 65536 {
            return Err("runner_state_invalid".into());
        }
        let path = self
            .path
            .with_extension(format!("{}.tmp", uuid::Uuid::new_v4()));
        let result = (|| {
            let mut file = private_open(&path, true)?;
            file.write_all(&bytes)
                .map_err(|_| "runner_state_unavailable")?;
            file.sync_all().map_err(|_| "runner_state_unavailable")?;
            std::fs::rename(&path, &self.path).map_err(|_| "runner_state_unavailable")?;
            File::open(self.path.parent().unwrap())
                .and_then(|f| f.sync_all())
                .map_err(|_| "runner_state_unavailable")?;
            Ok(())
        })();
        if result.is_err() {
            let _ = std::fs::remove_file(path);
        }
        result
    }
}
fn check_private(path: &Path) -> Result<(), String> {
    let meta = std::fs::symlink_metadata(path).map_err(|_| "runner_state_unavailable")?;
    if meta.file_type().is_symlink() {
        return Err("runner_state_permissions_invalid".into());
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if meta.permissions().mode() & 0o077 != 0 {
            return Err("runner_state_permissions_invalid".into());
        }
    }
    Ok(())
}
fn private_open(path: &Path, exclusive: bool) -> Result<File, String> {
    if path.try_exists().map_err(|_| "runner_state_unavailable")? {
        check_private(path)?;
    }
    let mut options = OpenOptions::new();
    options
        .read(true)
        .write(true)
        .create(!exclusive)
        .create_new(exclusive);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options
        .open(path)
        .map_err(|_| "runner_state_unavailable".into())
}

#[cfg(test)]
mod tests {
    use super::*;
    struct Directory(PathBuf);
    impl Drop for Directory {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    #[test]
    fn state_replacement_is_private_and_requires_one_rotation_owner() {
        let dir =
            Directory(std::env::temp_dir().join(format!("nebula-runner-{}", uuid::Uuid::new_v4())));
        let path = dir.0.join("runner.json");
        let file = CredentialFile::open(path.clone()).unwrap();
        assert!(matches!(CredentialFile::open(path.clone()),Err(e) if e=="runner_already_running"));
        let mut state = Credentials {
            endpoint: "https://example.com".into(),
            runner_id: Some("runner_a".into()),
            secret: "first".into(),
            pending_secret: None,
            pairing: None,
        };
        file.save(&state).unwrap();
        state.secret = "replacement".into();
        file.save(&state).unwrap();
        drop(file);
        let file = CredentialFile::open(path.clone()).unwrap();
        assert_eq!(file.read().unwrap().unwrap().secret, "replacement");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
        assert_eq!(std::fs::read_dir(&dir.0).unwrap().count(), 2);
    }
    #[cfg(unix)]
    #[test]
    fn shared_state_directory_is_refused() {
        use std::os::unix::fs::PermissionsExt;
        let dir =
            Directory(std::env::temp_dir().join(format!("nebula-runner-{}", uuid::Uuid::new_v4())));
        std::fs::create_dir(&dir.0).unwrap();
        std::fs::set_permissions(&dir.0, std::fs::Permissions::from_mode(0o755)).unwrap();
        assert!(
            matches!(CredentialFile::open(dir.0.join("runner.json")),Err(e) if e=="runner_state_permissions_invalid")
        );
    }
}
