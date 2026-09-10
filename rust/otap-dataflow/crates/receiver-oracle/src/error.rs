// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

pub enum OracleReceiverError {
    InvalidConfig,
    Credential,
    NativeClientUnavailable,
    Connect,
    Prepare,
    Bind,
    Fetch,
    Cancel,
    Convert,
}
