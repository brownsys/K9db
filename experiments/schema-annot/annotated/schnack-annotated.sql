CREATE DATA_SUBJECT TABLE user (
    id int NOT NULL,
    name text,
    display_name text,
    provider text,
    provider_id text,
    created_at text NOT NULL,
    blocked int,
    trusted int,
    PRIMARY KEY (id)
);

CREATE TABLE comment (
    id int NOT NULL,
    user_id int NOT NULL,
    slug text NOT NULL,
    created_at text NOT NULL,
    comment text NOT NULL,
    rejected int,
    approved int,
    PRIMARY KEY (id),
    FOREIGN KEY (user_id) OWNED_BY user(id)
);

CREATE TABLE setting (
    name text PRIMARY KEY NOT NULL,
    active int NOT NULL
);

CREATE TABLE subscription (
    endpoint text PRIMARY KEY NOT NULL,
    publicKey text NOT NULL,
    auth text NOT NULL
);

CREATE TABLE oauth_provider (
    id int PRIMARY KEY NOT NULL,
    provider text,
    provider_app_id text,
    domain text,
    client_id text,
    client_secret text,
    created_at text NOT NULL
);

EXPLAIN COMPLIANCE;