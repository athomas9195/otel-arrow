#!/bin/sh

# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

set -eu

if [ "$#" -ne 1 ]; then
  echo "usage: $0 <oracle-receiver-password-file>" >&2
  exit 1
fi

password_file=$1
if [ ! -f "$password_file" ] || [ ! -r "$password_file" ]; then
  echo "receiver password file must be a readable regular file" >&2
  exit 1
fi

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
config_dir=$(CDPATH= cd -- "$script_dir/../configs/oracle" && pwd)
secrets_dir="$config_dir/secrets"
checkpoints_dir="$config_dir/checkpoints"

mkdir -p "$secrets_dir" "$checkpoints_dir"
printf '%s' "OTAP_RECEIVER" >"$secrets_dir/username"
install -m 600 "$password_file" "$secrets_dir/password"
chmod 755 "$config_dir" "$secrets_dir" "$checkpoints_dir"
chmod 600 "$secrets_dir/username" "$secrets_dir/password"

if command -v getenforce >/dev/null 2>&1 &&
  [ "$(getenforce)" != "Disabled" ]; then
  if ! command -v semanage >/dev/null 2>&1; then
    echo "SELinux is enabled but semanage is unavailable" >&2
    echo "Install policycoreutils-python-utils and rerun this script" >&2
    exit 1
  fi
  sudo semanage fcontext -a -t container_file_t "${config_dir}(/.*)?" \
    2>/dev/null ||
    sudo semanage fcontext -m -t container_file_t "${config_dir}(/.*)?"
  sudo restorecon -RF "$config_dir"
fi

echo "Prepared receiver credentials, checkpoint storage, and SELinux labels."
