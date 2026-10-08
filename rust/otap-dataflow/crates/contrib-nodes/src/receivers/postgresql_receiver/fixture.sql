-- Copyright The OpenTelemetry Authors
-- SPDX-License-Identifier: Apache-2.0
-- Harness-owned setup in an empty fixture database, not executed by the receiver.
-- Grant the externally provisioned receiver role SELECT separately.
CREATE TABLE public.receiver_events (
    event_ts timestamptz(6) NOT NULL,
    event_id bigint NOT NULL,
    actor text NOT NULL,
    amount numeric NOT NULL,
    payload jsonb NOT NULL
);
-- Deliberately no UNIQUE constraint: stable pair uniqueness is a data contract.
INSERT INTO public.receiver_events
SELECT '2026-10-05T00:00:00Z'::timestamptz, id, 'actor',
       9007199254740993.1200::numeric, '{"n":9007199254740993}'::jsonb
FROM generate_series(1,2305) AS id;
