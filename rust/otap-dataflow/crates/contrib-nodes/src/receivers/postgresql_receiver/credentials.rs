// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Fresh, bounded reads from one opened handle, including atomic Secret mounts.

use super::error::{Error, Result};
use secrecy::{ExposeSecret, ExposeSecretMut, SecretBox, SecretString};
use std::{
    fs::File,
    io::Read,
    path::{Component, Path},
};

pub(crate) fn validate_path(path: &Path) -> Result<()> {
    if !path.is_absolute() || path.components().any(|c| matches!(c, Component::ParentDir)) {
        return Err(Error::Config);
    }
    Ok(())
}

pub(crate) fn read(path: &Path, limit: usize) -> Result<SecretBox<Vec<u8>>> {
    let file = File::open(path).map_err(|_| Error::Credential)?;
    let mut bytes = SecretBox::new(Box::new(Vec::with_capacity(limit + 1)));
    let _ = file
        .take((limit + 1) as u64)
        .read_to_end(bytes.expose_secret_mut())
        .map_err(|_| Error::Credential)?;
    if bytes.expose_secret().len() > limit {
        return Err(Error::Credential);
    }
    Ok(bytes)
}

pub(crate) fn text(path: &Path, limit: usize) -> Result<SecretString> {
    let bytes = read(path, limit)?;
    let text = std::str::from_utf8(bytes.expose_secret()).map_err(|_| Error::Credential)?;
    let text = text.trim_end_matches(['\r', '\n']);
    if text.is_empty() || text.chars().any(char::is_control) {
        return Err(Error::Credential);
    }
    Ok(SecretString::from(text.to_owned()))
}

pub(crate) fn username(path: &Path) -> Result<String> {
    let secret = text(path, 256)?;
    if secret.expose_secret().len() > 63 {
        return Err(Error::Credential);
    }
    Ok(secret.expose_secret().to_owned())
}
