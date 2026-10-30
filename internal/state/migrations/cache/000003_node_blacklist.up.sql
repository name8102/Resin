ALTER TABLE nodes_dynamic ADD COLUMN blacklist_until_ns INTEGER NOT NULL DEFAULT 0;
ALTER TABLE nodes_dynamic ADD COLUMN blacklist_strikes INTEGER NOT NULL DEFAULT 0;

-- Existing recorded failures are already sufficient evidence under the new
-- first-failure policy. Do not wait for each known-bad node to hurt traffic again.
UPDATE nodes_dynamic
SET blacklist_until_ns = (CAST(strftime('%s','now') AS INTEGER) + 3600) * 1000000000,
    blacklist_strikes = 1,
    circuit_open_since = CASE WHEN circuit_open_since = 0
        THEN CAST(strftime('%s','now') AS INTEGER) * 1000000000
        ELSE circuit_open_since END
WHERE failure_count > 0;
