-- Seed data for local development. Small enough to load in a second, varied
-- enough that the recommender and the QoE detector both have something to say.
SET search_path TO streaming, public;

INSERT INTO regions (region_id, region_code, display_name, cdn_provider) VALUES
    (1, 'us-east',   'United States (East)', 'cloudfront'),
    (2, 'eu-west',   'Europe (West)',        'cloudfront'),
    (3, 'ap-south',  'India',                'fastly'),
    (4, 'ap-south-east', 'Southeast Asia',   'fastly'),
    (5, 'sa-east',   'Brazil',               'akamai');

INSERT INTO genres (genre_id, genre_name) VALUES
    (1, 'drama'), (2, 'comedy'), (3, 'thriller'), (4, 'documentary'),
    (5, 'sci-fi'), (6, 'romance'), (7, 'action'), (8, 'animation');

-- 2,000 subscribers spread across regions, signed up over the last two years.
INSERT INTO subscribers (external_ref, email_hash, region_id, signup_at)
SELECT
    gen_random_uuid(),
    encode(sha256(('subscriber-' || g)::bytea), 'hex'),
    1 + (g % 5),
    now() - (random() * interval '730 days')
FROM generate_series(1, 2000) g;

INSERT INTO subscriptions (subscriber_id, tier, status, monthly_price, started_at)
SELECT
    s.subscriber_id,
    tier.name,
    CASE WHEN random() < 0.08 THEN 'cancelled'
         WHEN random() < 0.12 THEN 'paused'
         ELSE 'active' END,
    tier.price,
    s.signup_at
FROM subscribers s
CROSS JOIN LATERAL (
    SELECT * FROM (VALUES
        ('basic', 6.99), ('standard', 11.99), ('premium', 17.99)
    ) AS t(name, price)
    ORDER BY random() LIMIT 1
) tier;

-- One to four profiles per household.
INSERT INTO profiles (subscriber_id, display_name, is_kids)
SELECT
    s.subscriber_id,
    'profile-' || s.subscriber_id || '-' || p,
    p > 2
FROM subscribers s
CROSS JOIN generate_series(1, 1 + floor(random() * 3)::int) p;

-- 800 titles with a plausible runtime distribution per content type.
INSERT INTO titles (external_ref, title_name, content_type, runtime_seconds,
                    release_date, maturity_rating, is_original, licence_expires)
SELECT
    gen_random_uuid(),
    'Title ' || g,
    ct.name,
    ct.runtime,
    (current_date - (random() * 3650)::int),
    (ARRAY['G', 'PG', 'PG-13', 'R', 'TV-MA'])[1 + floor(random() * 5)::int],
    random() < 0.35,
    CASE WHEN random() < 0.4 THEN current_date + (random() * 900)::int END
FROM generate_series(1, 800) g
CROSS JOIN LATERAL (
    SELECT * FROM (VALUES
        ('movie',       5400 + floor(random() * 3600)::int),
        ('series',      1500 + floor(random() * 1500)::int),
        ('documentary', 3000 + floor(random() * 2400)::int),
        ('short',        300 + floor(random() *  600)::int)
    ) AS t(name, runtime)
    ORDER BY random() LIMIT 1
) ct;

INSERT INTO title_genres (title_id, genre_id)
SELECT DISTINCT t.title_id, 1 + floor(random() * 8)::int
FROM titles t
CROSS JOIN generate_series(1, 2);

-- Most titles are available in most regions, but not all. The recommender has
-- to respect this or it will surface unplayable content.
INSERT INTO title_availability (title_id, region_id, available_from, available_to)
SELECT t.title_id, r.region_id,
       (t.release_date::timestamptz),
       CASE WHEN t.licence_expires IS NOT NULL THEN t.licence_expires::timestamptz END
FROM titles t
CROSS JOIN regions r
WHERE random() < 0.82;

-- Historical watch sessions so the offline feature models have something to
-- chew on before any streaming events arrive.
INSERT INTO watch_sessions (session_id, profile_id, title_id, device_type,
                            started_at, ended_at, seconds_watched, completed)
SELECT
    gen_random_uuid(),
    p.profile_id,
    t.title_id,
    (ARRAY['smart_tv', 'mobile', 'web', 'tablet', 'console'])[1 + floor(random() * 5)::int],
    started,
    started + make_interval(secs => watched),
    watched,
    watched::numeric / t.runtime_seconds > 0.9
FROM profiles p
CROSS JOIN LATERAL (
    SELECT title_id, runtime_seconds FROM titles ORDER BY random() LIMIT 12
) t
CROSS JOIN LATERAL (
    SELECT now() - (random() * interval '180 days') AS started
) s
CROSS JOIN LATERAL (
    SELECT greatest(60, floor(t.runtime_seconds * (0.15 + random() * 0.85))::int) AS watched
) w;

INSERT INTO ratings (profile_id, title_id, rating, rated_at)
SELECT DISTINCT ON (ws.profile_id, ws.title_id)
    ws.profile_id, ws.title_id,
    -- People who finish something rate it higher. Keeps the label signal honest.
    least(5, greatest(1, round(2.5 + (CASE WHEN ws.completed THEN 1.5 ELSE -0.5 END)
                               + (random() - 0.5) * 2)::int)),
    ws.ended_at
FROM watch_sessions ws
WHERE random() < 0.25;

ANALYZE;
