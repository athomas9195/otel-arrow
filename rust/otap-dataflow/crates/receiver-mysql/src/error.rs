// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

pub enum MySqlReceiverError {
    InvalidConfig,
    Credential,
    Connect,
    Prepare,
    Bind,
    Fetch,
    Cancel,
    Convert,
}
