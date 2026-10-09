// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Deterministic load generator for the PostgreSQL receiver's watermark table.
//! Development-only: connections use unencrypted TCP.

#![allow(clippy::print_stdout)]

use secrecy::{ExposeSecret, SecretString};
use std::{error::Error, time::Duration};
use tokio_postgres::{Client, Config, NoTls, config::SslMode};

const CREATE_TABLE: &str = "CREATE TABLE IF NOT EXISTS public.receiver_events (
    event_ts timestamptz(6) NOT NULL,
    event_id bigint NOT NULL PRIMARY KEY,
    actor text NOT NULL,
    amount numeric NOT NULL,
    payload jsonb NOT NULL
)";
const CREATE_INDEX: &str = "CREATE INDEX IF NOT EXISTS receiver_events_cursor
    ON public.receiver_events (event_ts, event_id)";
const INSERT_ROWS: &str = "INSERT INTO public.receiver_events
    (event_ts, event_id, actor, amount, payload)
SELECT TIMESTAMPTZ '2026-10-05T00:00:00Z'
           + (((id - 1) / $2::bigint)::double precision * INTERVAL '1 second'),
       id, 'actor', 9007199254740993.1200::numeric,
       '{\"n\":9007199254740993}'::jsonb
FROM generate_series(1::bigint, $1::bigint) AS id
ON CONFLICT (event_id) DO NOTHING";

#[derive(Debug, PartialEq)]
struct Options {
    rows: i64,
    collision_size: i64,
    reset: bool,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    let arguments = std::env::args_os()
        .skip(1)
        .map(|argument| {
            argument
                .into_string()
                .map_err(|_| "arguments must contain valid Unicode")
        })
        .collect::<Result<Vec<_>, _>>()?;
    let options = parse_options(arguments.into_iter())?;
    let mut config = Config::new();
    _ = config
        .host(&required_env("PG_HOST")?)
        .dbname(&required_env("PG_DATABASE")?)
        .user(&required_env("PG_USERNAME")?)
        .ssl_mode(SslMode::Disable)
        .connect_timeout(Duration::from_secs(10));
    if let Some(port) = std::env::var_os("PG_PORT") {
        let port: u16 = port
            .to_str()
            .ok_or("PG_PORT must contain valid Unicode")?
            .parse()
            .map_err(|_| "PG_PORT must be an integer from 1 to 65535")?;
        if port == 0 {
            return Err("PG_PORT must be an integer from 1 to 65535".into());
        }
        _ = config.port(port);
    }
    let password = SecretString::from(
        std::fs::read_to_string(required_env("PG_PASSWORD_FILE")?)
            .map_err(|_| "cannot read PG_PASSWORD_FILE as UTF-8")?,
    );
    _ = config.password(password.expose_secret().trim_end_matches(['\r', '\n']));
    let (mut client, connection) = config
        .connect(NoTls)
        .await
        .map_err(|_| "PostgreSQL connection failed; check credentials and connection settings")?;

    // Drive the protocol and fixture writes together without a background task.
    let inserted = tokio::select! {
        result = populate(&mut client, &options) => result?,
        result = connection => {
            result.map_err(|_| "PostgreSQL connection failed during fixture setup")?;
            return Err("PostgreSQL connection closed before fixture setup completed".into());
        }
    };
    println!(
        "Prepared public.receiver_events: {inserted} new rows, requested IDs 1..={}, collision groups of {}",
        options.rows, options.collision_size
    );
    Ok(())
}

async fn populate(client: &mut Client, options: &Options) -> Result<u64, Box<dyn Error>> {
    let transaction = client
        .transaction()
        .await
        .map_err(|_| "cannot start fixture transaction")?;
    if options.reset {
        transaction
            .batch_execute("DROP TABLE IF EXISTS public.receiver_events")
            .await
            .map_err(|_| "cannot reset public.receiver_events")?;
    }
    transaction
        .batch_execute(CREATE_TABLE)
        .await
        .map_err(|_| "cannot create public.receiver_events; check fixture-owner permissions")?;
    transaction
        .batch_execute(CREATE_INDEX)
        .await
        .map_err(|_| "cannot create fixture cursor index; check existing schema and permissions")?;
    let inserted = transaction
        .execute(INSERT_ROWS, &[&options.rows, &options.collision_size])
        .await
        .map_err(|_| "cannot insert fixture rows; check existing schema and requested row count")?;
    transaction
        .commit()
        .await
        .map_err(|_| "cannot commit fixture rows")?;
    Ok(inserted)
}

fn parse_options(mut arguments: impl Iterator<Item = String>) -> Result<Options, Box<dyn Error>> {
    let mut rows = 1_000i64;
    let mut collision_size = 10i64;
    let mut reset = false;
    while let Some(argument) = arguments.next() {
        match argument.as_str() {
            "--rows" => {
                rows = arguments.next().ok_or("--rows requires a value")?.parse()?;
            }
            "--collision-size" => {
                collision_size = arguments
                    .next()
                    .ok_or("--collision-size requires a value")?
                    .parse()?;
            }
            "--reset" => reset = true,
            _ => {
                return Err(
                    "unknown argument; expected --rows, --collision-size or --reset".into(),
                );
            }
        }
    }
    if rows <= 0 {
        return Err("--rows must be greater than zero".into());
    }
    if collision_size <= 0 {
        return Err("--collision-size must be greater than zero".into());
    }
    Ok(Options {
        rows,
        collision_size,
        reset,
    })
}

fn required_env(name: &'static str) -> Result<String, Box<dyn Error>> {
    let value = std::env::var(name).map_err(|_| {
        format!("environment variable {name} is required and must contain valid Unicode")
    })?;
    if value.is_empty() {
        return Err(format!("environment variable {name} must not be empty").into());
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Scenario: The generator is invoked with defaults or Oracle-style load options.
    /// Guarantees: Defaults are 1,000 rows in groups of 10; explicit options control rows, ties, and reset.
    #[test]
    fn accepts_defaults_and_explicit_options() {
        assert_eq!(
            parse_options(std::iter::empty()).expect("defaults"),
            Options {
                rows: 1_000,
                collision_size: 10,
                reset: false,
            }
        );
        assert_eq!(
            parse_options(
                ["--rows", "2305", "--collision-size", "2305", "--reset"]
                    .into_iter()
                    .map(str::to_owned)
            )
            .expect("E2E fixture options"),
            Options {
                rows: 2305,
                collision_size: 2305,
                reset: true,
            }
        );
    }

    /// Scenario: A row count or timestamp collision group is nonpositive or missing.
    /// Guarantees: Invalid options fail before connecting or modifying a database.
    #[test]
    fn rejects_invalid_counts() {
        for flag in ["--rows", "--collision-size"] {
            for value in ["0", "-1", "9223372036854775808"] {
                assert!(parse_options([flag, value].into_iter().map(str::to_owned)).is_err());
            }
            assert!(parse_options([flag].into_iter().map(str::to_owned)).is_err());
        }
    }

    /// Scenario: Sensitive text is accidentally supplied as an option or a numeric value.
    /// Guarantees: CLI failures never echo the supplied text in display or debug output.
    #[test]
    fn invalid_options_do_not_echo_values() {
        const SENTINEL: &str = "PRIVATE_ARGUMENT_SENTINEL";
        for args in [
            vec![SENTINEL],
            vec!["--rows", SENTINEL],
            vec!["--collision-size", SENTINEL],
        ] {
            let error =
                parse_options(args.into_iter().map(str::to_owned)).expect_err("invalid option");
            assert!(!error.to_string().contains(SENTINEL));
            assert!(!format!("{error:?}").contains(SENTINEL));
        }
    }
}
