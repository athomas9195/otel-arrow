// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#[derive(Debug)]
pub enum AgentError {
    MissingInput,
    UnsupportedInputVersion,
    UnsupportedReceiver,
    ReceiverNotCompiled,
    InvalidEndpoint,
    InvalidFileReference,
    PipelineConstruction,
    PipelineValidation,
    Controller,
}

// Errors identify the invalid setting or failed stage without including
// credentials, connection strings, SQL text, row values, or cursor values.
