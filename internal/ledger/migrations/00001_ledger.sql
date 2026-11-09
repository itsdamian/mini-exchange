-- +goose Up
-- Double-entry ledger. Amounts are integer base or quote units (no floating
-- point). An entry's delta is its effect on the account balance: a debit of
-- the paying account is negative, the matching credit is positive. Every
-- settlement posts entries that sum to zero per asset.

CREATE TABLE settlements (
    event_id   text        PRIMARY KEY,           -- "<symbol>:<seq>" for a trade, "deposit:<ref>" for a deposit
    kind       text        NOT NULL CHECK (kind IN ('trade', 'deposit')),
    symbol     text        NOT NULL,
    seq        bigint,                            -- engine event sequence (trades)
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX settlements_trade_seq ON settlements (symbol, seq) WHERE kind = 'trade';

CREATE TABLE entries (
    id         bigserial PRIMARY KEY,
    event_id   text   NOT NULL REFERENCES settlements (event_id),
    entry_kind text   NOT NULL CHECK (entry_kind IN ('cash', 'asset', 'fee', 'deposit')),
    acct_kind  text   NOT NULL CHECK (acct_kind IN ('customer', 'fee', 'external')),
    owner      bigint NOT NULL,                   -- customer account id, 0 for system accounts
    asset      text   NOT NULL,
    delta      bigint NOT NULL CHECK (delta <> 0)
);

CREATE INDEX entries_event ON entries (event_id);
CREATE INDEX entries_account ON entries (acct_kind, owner, asset);

-- Running balances, updated in the same transaction as the entries. The
-- check makes a negative customer balance impossible to commit.
CREATE TABLE accounts (
    acct_kind text   NOT NULL CHECK (acct_kind IN ('customer', 'fee', 'external')),
    owner     bigint NOT NULL,
    asset     text   NOT NULL,
    amount    bigint NOT NULL DEFAULT 0,
    PRIMARY KEY (acct_kind, owner, asset),
    CONSTRAINT customer_not_negative CHECK (acct_kind <> 'customer' OR amount >= 0)
);

-- apply_posting settles one posting atomically and idempotently. It returns
-- false when the event id was settled before (nothing changes). Balance rows
-- are created at zero first and then updated in key order, because a CHECK is
-- evaluated on the proposed row of an INSERT .. ON CONFLICT before the
-- conflict is resolved, so a negative delta cannot be upserted in one step;
-- the key order keeps two writers from deadlocking.
-- +goose StatementBegin
CREATE FUNCTION apply_posting(
    p_event_id text, p_kind text, p_symbol text, p_seq bigint,
    p_entry_kinds text[], p_acct_kinds text[], p_owners bigint[], p_assets text[], p_deltas bigint[]
) RETURNS boolean LANGUAGE plpgsql AS $$
DECLARE
    r record;
BEGIN
    INSERT INTO settlements (event_id, kind, symbol, seq)
    VALUES (p_event_id, p_kind, p_symbol, p_seq)
    ON CONFLICT (event_id) DO NOTHING;
    IF NOT FOUND THEN
        RETURN false;
    END IF;

    INSERT INTO entries (event_id, entry_kind, acct_kind, owner, asset, delta)
    SELECT p_event_id, t.k, t.ak, t.o, t.a, t.d
    FROM unnest(p_entry_kinds, p_acct_kinds, p_owners, p_assets, p_deltas) AS t(k, ak, o, a, d);

    FOR r IN
        SELECT t.ak AS acct_kind, t.o AS owner, t.a AS asset, sum(t.d) AS delta
        FROM unnest(p_acct_kinds, p_owners, p_assets, p_deltas) AS t(ak, o, a, d)
        GROUP BY 1, 2, 3 ORDER BY 1, 2, 3
    LOOP
        INSERT INTO accounts (acct_kind, owner, asset, amount)
        VALUES (r.acct_kind, r.owner, r.asset, 0)
        ON CONFLICT DO NOTHING;
        UPDATE accounts SET amount = amount + r.delta
        WHERE acct_kind = r.acct_kind AND owner = r.owner AND asset = r.asset;
    END LOOP;
    RETURN true;
END
$$;
-- +goose StatementEnd

-- +goose Down
DROP FUNCTION apply_posting(text, text, text, bigint, text[], text[], bigint[], text[], bigint[]);
DROP TABLE accounts;
DROP TABLE entries;
DROP TABLE settlements;
