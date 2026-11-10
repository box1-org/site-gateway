-- Site-local store of site-gateway (box1-docs#127, #129). PostgreSQL in the site VM, data under /var.
-- A rollback runs the previous release on this schema, so later migrations must stay backward compatible.

-- Outbox: every reading and site event, written in the same transaction as its local effect and published to Kafka
-- by one publisher in seq order. A row is marked published only after the broker acknowledged it. Published rows are
-- kept for a bounded time so site-side counts can be compared with Kafka, then pruned.
CREATE TABLE outbox (
    seq bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id uuid NOT NULL UNIQUE,
    event_class text NOT NULL CHECK (event_class IN ('READING', 'SITE_EVENT')),
    event_type text NOT NULL,
    topic text NOT NULL,
    record_key text NOT NULL,
    envelope text NOT NULL,
    occurred_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    published_at timestamptz,
    attempts integer NOT NULL DEFAULT 0
);
CREATE INDEX ix_outbox_pending ON outbox (seq) WHERE published_at IS NULL;
CREATE INDEX ix_outbox_pending_class ON outbox (event_class, seq) WHERE published_at IS NULL;
CREATE INDEX ix_outbox_published ON outbox (published_at) WHERE published_at IS NOT NULL;

-- Every bound-enforcement drop, so site-side counts stay explainable (recorded = published + pending + dropped).
CREATE TABLE outbox_drop (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    dropped_at timestamptz NOT NULL DEFAULT now(),
    reason text NOT NULL CHECK (reason IN ('SIZE', 'AGE')),
    rows_dropped integer NOT NULL CHECK (rows_dropped > 0),
    first_seq bigint NOT NULL,
    last_seq bigint NOT NULL
);

-- The site's pick list, from inventory.reserved.v1 for this site, closed by expiry or shipment.
CREATE TABLE pick_item (
    order_id text PRIMARY KEY,
    reservation_id text NOT NULL,
    sku text NOT NULL,
    quantity integer NOT NULL CHECK (quantity > 0),
    expires_at timestamptz NOT NULL,
    reserved_event_id uuid NOT NULL,
    status text NOT NULL CHECK (status IN ('OPEN', 'PICKED', 'EXPIRED', 'SHIPPED')),
    received_at timestamptz NOT NULL DEFAULT now(),
    pick_id text UNIQUE,
    picked_at timestamptz,
    closed_at timestamptz
);
CREATE INDEX ix_pick_item_status ON pick_item (status, received_at);

-- A closing event (expiry, shipment) that arrived before its reservation, so the reservation stays closed.
CREATE TABLE pick_closure (
    order_id text PRIMARY KEY,
    reason text NOT NULL CHECK (reason IN ('EXPIRED', 'SHIPPED')),
    closed_at timestamptz NOT NULL,
    received_at timestamptz NOT NULL DEFAULT now()
);

-- Holds placed at the site.
CREATE TABLE site_hold (
    hold_id text PRIMARY KEY,
    sku text NOT NULL,
    quantity integer NOT NULL CHECK (quantity > 0),
    reason_code text NOT NULL,
    status text NOT NULL CHECK (status IN ('PLACED', 'RELEASED')),
    placed_at timestamptz NOT NULL,
    released_at timestamptz
);

-- One row per operator action. request_id comes from the site-ops form, so a double submit is harmless.
CREATE TABLE site_action (
    request_id uuid PRIMARY KEY,
    action text NOT NULL CHECK (action IN ('PICK', 'COUNT', 'HOLD_PLACED', 'HOLD_RELEASED')),
    local_id text NOT NULL,
    event_id uuid NOT NULL UNIQUE,
    order_id text,
    sku text NOT NULL,
    quantity integer NOT NULL CHECK (quantity >= 0),
    recorded_at timestamptz NOT NULL
);
CREATE INDEX ix_site_action_recent ON site_action (action, recorded_at DESC);
