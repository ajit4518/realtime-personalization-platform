-- OLTP schema for the streaming service. This is the system of record that
-- Debezium tails; every table here is replicated into Kafka as a change stream.
--
-- Design notes:
--   * REPLICA IDENTITY FULL on tables where downstream consumers need the
--     "before" image (subscription tier changes drive churn features).
--     It costs WAL volume, so it is opt-in per table rather than global.
--   * Surrogate BIGINT keys everywhere. Natural keys change; Kafka keys must not.
--   * updated_at is maintained by trigger so CDC ordering survives bulk updates
--     that forget to set it.

CREATE SCHEMA IF NOT EXISTS streaming;
SET search_path TO streaming, public;

-- ── Reference data ────────────────────────────────────────────────────────

CREATE TABLE regions (
    region_id     SMALLINT PRIMARY KEY,
    region_code   TEXT NOT NULL UNIQUE,
    display_name  TEXT NOT NULL,
    cdn_provider  TEXT NOT NULL
);

CREATE TABLE genres (
    genre_id    SMALLINT PRIMARY KEY,
    genre_name  TEXT NOT NULL UNIQUE
);

-- ── Subscribers ───────────────────────────────────────────────────────────

CREATE TABLE subscribers (
    subscriber_id   BIGSERIAL PRIMARY KEY,
    external_ref    UUID NOT NULL UNIQUE,
    email_hash      TEXT NOT NULL,          -- never store raw PII in the OLTP row
    region_id       SMALLINT NOT NULL REFERENCES regions(region_id),
    signup_at       TIMESTAMPTZ NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_subscribers_region ON subscribers(region_id);

-- Tier changes are the strongest churn signal we have, so consumers need the
-- previous value of every update. That requires the full pre-image in the WAL.
CREATE TABLE subscriptions (
    subscription_id  BIGSERIAL PRIMARY KEY,
    subscriber_id    BIGINT NOT NULL REFERENCES subscribers(subscriber_id),
    tier             TEXT NOT NULL CHECK (tier IN ('basic', 'standard', 'premium')),
    status           TEXT NOT NULL CHECK (status IN ('active', 'paused', 'cancelled')),
    monthly_price    NUMERIC(8,2) NOT NULL,
    started_at       TIMESTAMPTZ NOT NULL,
    ended_at         TIMESTAMPTZ,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);
ALTER TABLE subscriptions REPLICA IDENTITY FULL;
CREATE INDEX idx_subscriptions_subscriber ON subscriptions(subscriber_id);
CREATE INDEX idx_subscriptions_status ON subscriptions(status) WHERE status = 'active';

CREATE TABLE profiles (
    profile_id     BIGSERIAL PRIMARY KEY,
    subscriber_id  BIGINT NOT NULL REFERENCES subscribers(subscriber_id),
    display_name   TEXT NOT NULL,
    is_kids        BOOLEAN NOT NULL DEFAULT false,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_profiles_subscriber ON profiles(subscriber_id);

-- ── Catalog ───────────────────────────────────────────────────────────────

CREATE TABLE titles (
    title_id        BIGSERIAL PRIMARY KEY,
    external_ref    UUID NOT NULL UNIQUE,
    title_name      TEXT NOT NULL,
    content_type    TEXT NOT NULL CHECK (content_type IN ('movie', 'series', 'documentary', 'short')),
    runtime_seconds INTEGER NOT NULL CHECK (runtime_seconds > 0),
    release_date    DATE NOT NULL,
    maturity_rating TEXT NOT NULL,
    is_original     BOOLEAN NOT NULL DEFAULT false,
    licence_expires DATE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_titles_type ON titles(content_type);
CREATE INDEX idx_titles_release ON titles(release_date DESC);

CREATE TABLE title_genres (
    title_id  BIGINT NOT NULL REFERENCES titles(title_id),
    genre_id  SMALLINT NOT NULL REFERENCES genres(genre_id),
    PRIMARY KEY (title_id, genre_id)
);

-- Availability is region-scoped and time-bounded. The recommender must never
-- surface a title the viewer cannot actually play.
CREATE TABLE title_availability (
    title_id      BIGINT NOT NULL REFERENCES titles(title_id),
    region_id     SMALLINT NOT NULL REFERENCES regions(region_id),
    available_from TIMESTAMPTZ NOT NULL,
    available_to   TIMESTAMPTZ,
    PRIMARY KEY (title_id, region_id)
);

-- ── Engagement (written by the playback service) ──────────────────────────

CREATE TABLE watch_sessions (
    session_id      UUID PRIMARY KEY,
    profile_id      BIGINT NOT NULL REFERENCES profiles(profile_id),
    title_id        BIGINT NOT NULL REFERENCES titles(title_id),
    device_type     TEXT NOT NULL,
    started_at      TIMESTAMPTZ NOT NULL,
    ended_at        TIMESTAMPTZ,
    seconds_watched INTEGER NOT NULL DEFAULT 0,
    completed       BOOLEAN NOT NULL DEFAULT false,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_sessions_profile_started ON watch_sessions(profile_id, started_at DESC);
CREATE INDEX idx_sessions_title ON watch_sessions(title_id);

CREATE TABLE ratings (
    profile_id  BIGINT NOT NULL REFERENCES profiles(profile_id),
    title_id    BIGINT NOT NULL REFERENCES titles(title_id),
    rating      SMALLINT NOT NULL CHECK (rating BETWEEN 1 AND 5),
    rated_at    TIMESTAMPTZ NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (profile_id, title_id)
);

-- ── updated_at maintenance ────────────────────────────────────────────────
-- CDC consumers order by updated_at when the LSN is not available (for example
-- after a snapshot re-read), so it cannot be left to application discipline.

CREATE OR REPLACE FUNCTION touch_updated_at() RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DO $$
DECLARE t TEXT;
BEGIN
    FOREACH t IN ARRAY ARRAY[
        'subscribers', 'subscriptions', 'profiles', 'titles',
        'watch_sessions', 'ratings'
    ] LOOP
        EXECUTE format(
            'CREATE TRIGGER trg_%1$s_touch BEFORE UPDATE ON %1$s
             FOR EACH ROW EXECUTE FUNCTION touch_updated_at()', t);
    END LOOP;
END $$;

-- ── Logical replication ───────────────────────────────────────────────────
-- Debezium reads this publication. Tables are listed explicitly rather than
-- FOR ALL TABLES so that adding a scratch table does not silently start
-- shipping it to Kafka.

CREATE PUBLICATION streaming_cdc FOR TABLE
    subscribers,
    subscriptions,
    profiles,
    titles,
    title_availability,
    watch_sessions,
    ratings;

-- Debezium needs a replication-capable role. In AWS RDS the master user is
-- granted rds_replication; this mirrors that locally.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'debezium') THEN
        CREATE ROLE debezium WITH REPLICATION LOGIN PASSWORD 'debezium';
    END IF;
END $$;

GRANT USAGE ON SCHEMA streaming TO debezium;
GRANT SELECT ON ALL TABLES IN SCHEMA streaming TO debezium;
ALTER DEFAULT PRIVILEGES IN SCHEMA streaming GRANT SELECT ON TABLES TO debezium;
