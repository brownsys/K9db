-- Up
CREATE TABLE comment (
    id INTEGER PRIMARY KEY NOT NULL,
    user_id NOT NULL,
    slug CHAR(128) NOT NULL,
    created_at TEXT NOT NULL,
    comment CHAR(4000) NOT NULL,
    rejected BOOLEAN,
    approved BOOLEAN,
    FOREIGN KEY (user_id) REFERENCES user(id)
);

CREATE TABLE user (
    id INTEGER PRIMARY KEY NOT NULL,
    PII_name CHAR(128),
    display_name CHAR(128),
    provider CHAR(128),
    provider_id CHAR(128),
    created_at TEXT NOT NULL,
    blocked BOOLEAN,
    trusted BOOLEAN
);

-- Down
DROP TABLE comment;
DROP TABLE user;


-- Up
CREATE TABLE setting (
    name CHAR(128) PRIMARY KEY NOT NULL,
    active BOOLEAN NOT NULL
);

INSERT INTO setting (name, active) VALUES ('notification', 1);

CREATE TABLE subscription (
    endpoint CHAR(600) PRIMARY KEY NOT NULL,
    publicKey CHAR(4096) NOT NULL,
    auth CHAR(600) NOT NULL
);

-- Down
DROP TABLE setting;
DROP TABLE subscription;


-- Up
ALTER TABLE comment ADD COLUMN reply_to INTEGER;

-- Down
ALTER TABLE comment RENAME TO comment_old;
CREATE TABLE comment (
    id INTEGER PRIMARY KEY NOT NULL,
    user_id NOT NULL,
    slug CHAR(128) NOT NULL,
    created_at TEXT NOT NULL,
    comment CHAR(4000) NOT NULL,
    rejected BOOLEAN,
    approved BOOLEAN,
    FOREIGN KEY (user_id) REFERENCES user(id)
);
INSERT INTO comment SELECT id, user_id, slug, created_at, comment, rejected, approved FROM comment_old;
DROP TABLE comment_old;


-- Up
CREATE UNIQUE INDEX idx_user_id ON user(id);
CREATE INDEX idx_user_blocked ON user(blocked);
CREATE INDEX idx_user_trusted ON user(trusted);
CREATE UNIQUE INDEX idx_comment_id ON comment(id);
CREATE INDEX idx_comment_approved ON comment(approved);
CREATE INDEX idx_comment_created_at ON comment(created_at);
CREATE INDEX idx_comment_rejected ON comment(rejected);
CREATE INDEX idx_comment_user_id ON comment(user_id);
CREATE UNIQUE INDEX idx_setting_name ON setting(name);
CREATE INDEX idx_subscription_endpoint ON subscription(endpoint);

-- Down
DROP INDEX idx_user_id;
DROP INDEX idx_user_blocked;
DROP INDEX idx_user_trusted;
DROP INDEX idx_comment_id;
DROP INDEX idx_comment_approved;
DROP INDEX idx_comment_created_at;
DROP INDEX idx_comment_rejected;
DROP INDEX idx_comment_user_id;
DROP INDEX idx_setting_name;
DROP INDEX idx_subscription_endpoint;


-- Up
CREATE UNIQUE INDEX idx_user_provider_id ON user(provider, provider_id);

-- Down
DROP INDEX idx_user_provider_id;


-- Up
ALTER TABLE user ADD COLUMN url CHAR(255);

-- Down
ALTER TABLE user RENAME TO user_old;
CREATE TABLE user (
    id INTEGER PRIMARY KEY NOT NULL,
    PII_name CHAR(128),
    display_name CHAR(128),
    provider CHAR(128),
    provider_id CHAR(128),
    created_at TEXT NOT NULL,
    blocked BOOLEAN,
    trusted BOOLEAN
);
INSERT INTO user SELECT id, name, display_name, provider, provider_id,
	created_at, blocked, trusted FROM user_old;
DROP TABLE user_old;


-- Up
CREATE TABLE oauth_provider (
    id INTEGER PRIMARY KEY NOT NULL,
    provider CHAR(128),
    provider_app_id CHAR(255),
    domain CHAR(255),
    client_id CHAR(255),
    client_secret CHAR(255),
    created_at TEXT NOT NULL
);

-- Down
DROP TABLE oauth_provider;
