#!/bin/sh

# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

set -eu

: "${ORACLE_SYS_PASSWORD:?ORACLE_SYS_PASSWORD is required}"

ORACLE_CONTAINER="${ORACLE_CONTAINER:-oracle-ee}"
ORACLE_PDB="${ORACLE_PDB:-ORCLPDB1}"
ORACLE_WIDE_BENCHMARK_ROWS="${ORACLE_WIDE_BENCHMARK_ROWS:-2500000}"
ORACLE_WIDE_BATCH_ROWS="${ORACLE_WIDE_BATCH_ROWS:-10000}"
ORACLE_COLLISION_SIZE="${ORACLE_COLLISION_SIZE:-100}"

for variable in \
  ORACLE_WIDE_BENCHMARK_ROWS \
  ORACLE_WIDE_BATCH_ROWS \
  ORACLE_COLLISION_SIZE; do
  eval "value=\${$variable}"
  case "$value" in
    *[!0-9]* | 0)
      echo "$variable must be a positive integer" >&2
      exit 1
      ;;
  esac
done

if ! printf '%s' "$ORACLE_SYS_PASSWORD" |
  grep -Eq '^[A-Za-z0-9_#$!@%+=.-]{12,64}$'; then
  echo "ORACLE_SYS_PASSWORD contains unsupported characters or is not 12-64 characters" >&2
  exit 1
fi

docker exec -i "$ORACLE_CONTAINER" sqlplus -L -s /nolog <<SQL
WHENEVER SQLERROR EXIT SQL.SQLCODE
CONNECT SYSTEM/"${ORACLE_SYS_PASSWORD}"@//localhost:1521/${ORACLE_PDB}
SET ECHO ON
SET SERVEROUTPUT ON
SET TIMING ON

DECLARE
  principal_count PLS_INTEGER;
BEGIN
  SELECT COUNT(*)
  INTO principal_count
  FROM DBA_USERS
  WHERE USERNAME IN ('OTAP_BENCH', 'OTAP_RECEIVER');

  IF principal_count != 2 THEN
    RAISE_APPLICATION_ERROR(
      -20001,
      'Run prepare-oracle.sh before prepare-oracle-wide.sh'
    );
  END IF;
END;
/

DECLARE
  table_missing EXCEPTION;
  PRAGMA EXCEPTION_INIT(table_missing, -942);
BEGIN
  EXECUTE IMMEDIATE 'DROP TABLE OTAP_BENCH.OTAP_ORACLE_WIDE_EVENTS PURGE';
EXCEPTION
  WHEN table_missing THEN
    NULL;
END;
/

CREATE TABLE OTAP_BENCH.OTAP_ORACLE_WIDE_EVENTS (
  EVENT_TS TIMESTAMP(9) NOT NULL,
  EVENT_ID NUMBER(19) NOT NULL,
  PAYLOAD_01 VARCHAR2(4000) NOT NULL,
  PAYLOAD_02 VARCHAR2(4000) NOT NULL,
  PAYLOAD_03 VARCHAR2(4000) NOT NULL,
  PAYLOAD_04 VARCHAR2(4000) NOT NULL,
  PAYLOAD_05 VARCHAR2(4000) NOT NULL,
  PAYLOAD_06 VARCHAR2(4000) NOT NULL,
  PAYLOAD_07 VARCHAR2(4000) NOT NULL,
  PAYLOAD_08 VARCHAR2(4000) NOT NULL,
  PAYLOAD_09 VARCHAR2(4000) NOT NULL,
  PAYLOAD_10 VARCHAR2(4000) NOT NULL,
  PAYLOAD_11 VARCHAR2(4000) NOT NULL,
  PAYLOAD_12 VARCHAR2(4000) NOT NULL,
  PAYLOAD_13 VARCHAR2(4000) NOT NULL,
  PAYLOAD_14 VARCHAR2(4000) NOT NULL,
  PAYLOAD_15 VARCHAR2(4000) NOT NULL,
  PAYLOAD_16 VARCHAR2(4000) NOT NULL
) NOLOGGING;

DECLARE
  total_rows CONSTANT PLS_INTEGER := ${ORACLE_WIDE_BENCHMARK_ROWS};
  batch_rows CONSTANT PLS_INTEGER := ${ORACLE_WIDE_BATCH_ROWS};
  first_id PLS_INTEGER := 1;
BEGIN
  WHILE first_id <= total_rows LOOP
    INSERT /*+ APPEND */ INTO OTAP_BENCH.OTAP_ORACLE_WIDE_EVENTS (
      EVENT_TS,
      EVENT_ID,
      PAYLOAD_01,
      PAYLOAD_02,
      PAYLOAD_03,
      PAYLOAD_04,
      PAYLOAD_05,
      PAYLOAD_06,
      PAYLOAD_07,
      PAYLOAD_08,
      PAYLOAD_09,
      PAYLOAD_10,
      PAYLOAD_11,
      PAYLOAD_12,
      PAYLOAD_13,
      PAYLOAD_14,
      PAYLOAD_15,
      PAYLOAD_16
    )
    SELECT
      TIMESTAMP '2026-01-01 00:00:00'
        + NUMTODSINTERVAL(
            FLOOR((event_id - 1) / ${ORACLE_COLLISION_SIZE}),
            'SECOND'
          ),
      event_id,
      RPAD('A', 4000, 'A'),
      RPAD('B', 4000, 'B'),
      RPAD('C', 4000, 'C'),
      RPAD('D', 4000, 'D'),
      RPAD('E', 4000, 'E'),
      RPAD('F', 4000, 'F'),
      RPAD('G', 4000, 'G'),
      RPAD('H', 4000, 'H'),
      RPAD('I', 4000, 'I'),
      RPAD('J', 4000, 'J'),
      RPAD('K', 4000, 'K'),
      RPAD('L', 4000, 'L'),
      RPAD('M', 4000, 'M'),
      RPAD('N', 4000, 'N'),
      RPAD('O', 4000, 'O'),
      RPAD('P', 4000, 'P')
    FROM (
      SELECT first_id + LEVEL - 1 AS event_id
      FROM DUAL
      CONNECT BY LEVEL <= LEAST(batch_rows, total_rows - first_id + 1)
    );
    COMMIT;
    DBMS_OUTPUT.PUT_LINE(
      'Prepared ' || LEAST(first_id + batch_rows - 1, total_rows) ||
      ' of ' || total_rows || ' rows'
    );
    first_id := first_id + batch_rows;
  END LOOP;
END;
/

CREATE UNIQUE INDEX OTAP_BENCH.OTAP_ORACLE_WIDE_CURSOR_IDX
  ON OTAP_BENCH.OTAP_ORACLE_WIDE_EVENTS (EVENT_TS, EVENT_ID)
  NOLOGGING;
GRANT SELECT ON OTAP_BENCH.OTAP_ORACLE_WIDE_EVENTS TO OTAP_RECEIVER;

BEGIN
  DBMS_STATS.GATHER_TABLE_STATS(
    ownname => 'OTAP_BENCH',
    tabname => 'OTAP_ORACLE_WIDE_EVENTS',
    estimate_percent => DBMS_STATS.AUTO_SAMPLE_SIZE,
    cascade => TRUE
  );
END;
/

SELECT
  COUNT(*) AS BENCHMARK_ROWS,
  64000 AS PAYLOAD_BYTES_PER_ROW
FROM OTAP_BENCH.OTAP_ORACLE_WIDE_EVENTS;
EXIT
SQL

printf '\nPrepared %s Oracle rows with 64,000 payload bytes per row.\n' \
  "$ORACLE_WIDE_BENCHMARK_ROWS"
