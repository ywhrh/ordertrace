CREATE TABLE parents (
    root_order_id varchar(80) PRIMARY KEY,
    status varchar(24) NOT NULL,
    quantity bigint,
    filled_quantity bigint NOT NULL CHECK (filled_quantity >= 0),
    child_count integer NOT NULL,
    version bigint NOT NULL CHECK (version > 0),
    business_updated_at timestamptz NOT NULL,
    persisted_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    warnings jsonb NOT NULL
);
CREATE INDEX parents_status_root ON parents(status, root_order_id);
CREATE TABLE children (
    root_order_id varchar(80) NOT NULL REFERENCES parents(root_order_id),
    order_id varchar(80) NOT NULL,
    status varchar(24) NOT NULL,
    quantity bigint,
    filled_quantity bigint NOT NULL CHECK (filled_quantity >= 0),
    price numeric(20,6),
    pending_cancel boolean NOT NULL,
    pending_replace boolean NOT NULL,
    warnings jsonb NOT NULL,
    PRIMARY KEY(root_order_id, order_id)
);
CREATE TABLE events (
    root_order_id varchar(80) NOT NULL REFERENCES parents(root_order_id),
    event_id varchar(100) NOT NULL,
    order_id varchar(80) NOT NULL,
    source varchar(16) NOT NULL,
    event_type varchar(32) NOT NULL,
    occurred_at timestamptz NOT NULL,
    source_sequence bigint NOT NULL,
    payload jsonb NOT NULL,
    PRIMARY KEY(root_order_id, event_id),
    UNIQUE(root_order_id, source, order_id, source_sequence)
);
CREATE INDEX events_timeline ON events(root_order_id, occurred_at, event_id);
CREATE INDEX events_child_timeline ON events(root_order_id, order_id, occurred_at, event_id);
