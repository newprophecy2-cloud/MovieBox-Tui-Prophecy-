use std::fs::File;
use std::io::Write;
use std::path::Path;

use flate2::read::GzDecoder;
use tar::Archive;
use tempfile::NamedTempFile;

pub fn extract_binary(
    archive_path: &Path,
    asset_name: &str,
    expected_binary_name: &str,
    output_staged_path: &Path,
) -> Result<(), String> {
    if asset_name.ends_with(".tar.gz") || asset_name.ends_with(".tgz") {
        extract_tar_gz(archive_path, expected_binary_name, output_staged_path)
    } else if asset_name.ends_with(".zip") {
        extract_zip(archive_path, expected_binary_name, output_staged_path)
    } else {
        Err(format!("unsupported archive format: {asset_name}"))
    }
}

fn validate_relative_path(path: &Path) -> Result<(), String> {
    if path.is_absolute() {
        return Err("archive entry has absolute path".to_string());
    }

    for component in path.components() {
        match component {
            std::path::Component::ParentDir => {
                return Err("archive entry contains '..' traversal".to_string());
            }
            std::path::Component::Prefix(_) => {
                return Err("archive entry contains volume prefix".to_string());
            }
            std::path::Component::RootDir => {
                return Err("archive entry contains root directory".to_string());
            }
            _ => {}
        }
    }

    Ok(())
}

fn create_staged_temp(output_staged_path: &Path) -> Result<NamedTempFile, String> {
    let parent = output_staged_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    tempfile::Builder::new()
        .prefix(".moviebox-update-")
        .tempfile_in(parent)
        .map_err(|e| format!("failed to create temporary staged binary: {e}"))
}

fn persist_staged_binary(
    mut staged_file: NamedTempFile,
    output_staged_path: &Path,
) -> Result<(), String> {
    staged_file
        .as_file_mut()
        .flush()
        .map_err(|e| format!("failed to flush staged binary: {e}"))?;
    validate_and_set_permissions(staged_file.path())?;

    // remove_file unlinks a pre-existing symlink itself; it never follows the link.
    match std::fs::remove_file(output_staged_path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(format!("failed to clear previous staged binary: {error}")),
    }

    staged_file
        .persist_noclobber(output_staged_path)
        .map(|_| ())
        .map_err(|error| format!("failed to persist staged binary: {}", error.error))
}

fn extract_tar_gz(
    archive_path: &Path,
    expected_binary_name: &str,
    output_staged_path: &Path,
) -> Result<(), String> {
    let file = File::open(archive_path).map_err(|e| format!("failed to open archive: {e}"))?;
    let decoder = GzDecoder::new(file);
    let mut archive = Archive::new(decoder);

    let mut staged_file = None;

    for entry_result in archive
        .entries()
        .map_err(|e| format!("corrupt tar archive: {e}"))?
    {
        let mut entry = entry_result.map_err(|e| format!("tar entry read error: {e}"))?;
        let entry_path = entry
            .path()
            .map_err(|e| format!("tar entry path error: {e}"))?
            .to_path_buf();

        validate_relative_path(&entry_path)?;

        let filename = entry_path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("");
        if filename == expected_binary_name {
            let entry_type = entry.header().entry_type();
            if !matches!(
                entry_type,
                tar::EntryType::Regular | tar::EntryType::Continuous
            ) {
                return Err("expected binary in tar archive is not a regular file".to_string());
            }

            let mut staged = create_staged_temp(output_staged_path)?;
            std::io::copy(&mut entry, staged.as_file_mut())
                .map_err(|e| format!("failed to write staged binary: {e}"))?;
            staged_file = Some(staged);
            break;
        }
    }

    let staged_file = staged_file
        .ok_or_else(|| format!("binary '{expected_binary_name}' not found inside archive"))?;
    persist_staged_binary(staged_file, output_staged_path)
}

fn extract_zip(
    archive_path: &Path,
    expected_binary_name: &str,
    output_staged_path: &Path,
) -> Result<(), String> {
    let file = File::open(archive_path).map_err(|e| format!("failed to open zip archive: {e}"))?;
    let mut zip = zip::ZipArchive::new(file).map_err(|e| format!("corrupt zip archive: {e}"))?;

    for i in 0..zip.len() {
        let mut file = zip
            .by_index(i)
            .map_err(|e| format!("zip entry read error: {e}"))?;
        let enclosed_name = file
            .enclosed_name()
            .ok_or("zip entry path traversal detected")?;
        validate_relative_path(&enclosed_name)?;

        if file.is_dir() {
            continue;
        }
        if file.is_symlink() {
            return Err("zip archive contains a symbolic-link entry".to_string());
        }
        if !file.is_file() {
            return Err("zip archive contains a non-regular file entry".to_string());
        }

        let filename = enclosed_name
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("");
        if filename == expected_binary_name {
            let mut staged = create_staged_temp(output_staged_path)?;
            std::io::copy(&mut file, staged.as_file_mut())
                .map_err(|e| format!("failed to write zip entry data: {e}"))?;
            return persist_staged_binary(staged, output_staged_path);
        }
    }

    Err(format!(
        "binary '{expected_binary_name}' not found inside zip"
    ))
}

fn validate_and_set_permissions(path: &Path) -> Result<(), String> {
    let metadata =
        std::fs::metadata(path).map_err(|e| format!("staged binary metadata error: {e}"))?;
    if metadata.len() == 0 {
        return Err("extracted binary is empty".to_string());
    }

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = metadata.permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(path, perms)
            .map_err(|e| format!("failed to set executable permissions: {e}"))?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use zip::write::{SimpleFileOptions, ZipWriter};

    fn write_zip_entry(path: &Path, name: &str, options: SimpleFileOptions, data: &[u8]) {
        let file = File::create(path).unwrap();
        let mut zip = ZipWriter::new(file);
        zip.start_file(name, options).unwrap();
        zip.write_all(data).unwrap();
        zip.finish().unwrap();
    }

    #[test]
    fn test_extract_zip_success() {
        let temp = tempfile::tempdir().unwrap();
        let zip_path = temp.path().join("archive.zip");
        let output_path = temp.path().join("moviebox-tui.exe");

        write_zip_entry(
            &zip_path,
            "moviebox-tui.exe",
            SimpleFileOptions::default().compression_method(zip::CompressionMethod::Deflated),
            b"binary-content-windows",
        );

        extract_zip(&zip_path, "moviebox-tui.exe", &output_path).unwrap();
        let content = std::fs::read(&output_path).unwrap();
        assert_eq!(content, b"binary-content-windows");
    }

    #[test]
    fn test_extract_zip_rejects_traversal() {
        let temp = tempfile::tempdir().unwrap();
        let zip_path = temp.path().join("traversal.zip");
        let output_path = temp.path().join("moviebox-tui.exe");

        write_zip_entry(
            &zip_path,
            "../moviebox-tui.exe",
            SimpleFileOptions::default(),
            b"malicious payload",
        );

        let error = extract_zip(&zip_path, "moviebox-tui.exe", &output_path).unwrap_err();
        assert!(error.contains("traversal"));
        assert!(!output_path.exists());
    }

    #[test]
    fn test_extract_zip_rejects_symlink_entry() {
        let temp = tempfile::tempdir().unwrap();
        let zip_path = temp.path().join("symlink.zip");
        let output_path = temp.path().join("moviebox-tui.exe");
        let symlink_options = SimpleFileOptions::default()
            .system(zip::System::Unix)
            .external_attributes((0o120777_u32) << 16);

        write_zip_entry(
            &zip_path,
            "moviebox-tui.exe",
            symlink_options,
            b"../../outside-target",
        );

        let error = extract_zip(&zip_path, "moviebox-tui.exe", &output_path).unwrap_err();
        assert!(error.contains("symbolic-link"));
        assert!(!output_path.exists());
    }

    #[cfg(unix)]
    #[test]
    fn test_extract_zip_does_not_follow_staging_symlink() {
        use std::os::unix::fs::symlink;

        let temp = tempfile::tempdir().unwrap();
        let zip_path = temp.path().join("archive.zip");
        let output_path = temp.path().join("moviebox-tui.exe");
        let outside_path = temp.path().join("outside-target");
        std::fs::write(&outside_path, b"must not be overwritten").unwrap();
        symlink(&outside_path, &output_path).unwrap();

        write_zip_entry(
            &zip_path,
            "moviebox-tui.exe",
            SimpleFileOptions::default(),
            b"safe binary payload",
        );

        extract_zip(&zip_path, "moviebox-tui.exe", &output_path).unwrap();
        assert_eq!(
            std::fs::read(&outside_path).unwrap(),
            b"must not be overwritten"
        );
        assert_eq!(std::fs::read(&output_path).unwrap(), b"safe binary payload");
        assert!(
            !std::fs::symlink_metadata(&output_path)
                .unwrap()
                .file_type()
                .is_symlink()
        );
    }

    #[test]
    fn test_extract_zip_missing_binary() {
        let temp = tempfile::tempdir().unwrap();
        let zip_path = temp.path().join("archive.zip");
        let output_path = temp.path().join("output.exe");

        write_zip_entry(
            &zip_path,
            "other.exe",
            SimpleFileOptions::default(),
            b"other-content",
        );

        let err = extract_zip(&zip_path, "moviebox-tui.exe", &output_path).unwrap_err();
        assert!(err.contains("not found inside zip"));
    }
}
