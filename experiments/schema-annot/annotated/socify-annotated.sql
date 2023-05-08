CREATE DATA_SUBJECT TABLE users (
    id int,
    name text,
    email text,
    encrypted_password text,
    bio text,
    avatar text,
    cover text,
    reset_password_token text,
    reset_password_sent_at datetime,
    remember_created_at datetime,
    sign_in_count int,
    current_sign_in_at datetime,
    last_sign_in_at datetime,
    current_sign_in_ip text,
    last_sign_in_ip text,
    confirmation_token text,
    confirmed_at datetime,
    confirmation_sent_at datetime,
    created_at datetime,
    updated_at datetime,
    sex text,
    location text,
    dob text,
    phone_number text,
    posts_count int,
    slug text,
    profile_complete text,
    first_name text,
    last_name text,
    hometown text,
    works_at text,
    photo_albums_count int,
    sash_id int,
    level int,
    PRIMARY KEY (id)
);

CREATE TABLE activities (
    id int,
    trackable_type text,
    trackable_id int,
    owner_type text,
    owner_id int,
    -- key text,
    parameters text,
    recipient_type text,
    recipient_id int,
    created_at datetime,
    updated_at datetime,
    PRIMARY KEY (id),
    FOREIGN KEY (owner_id) OWNED_BY users(id)
);

CREATE TABLE attachments (
    file_name text,
    attachable_type text,
    attachable_id int,
    created_at datetime,
    updated_at datetime,
    PRIMARY KEY (attachable_id)
);

CREATE TABLE authentications (
    uuid text,
    provider text,
    oauth_token text,
    user_id int,
    created_at datetime,
    updated_at datetime,
    PRIMARY KEY (uuid)
);

CREATE TABLE badges_sashes (
    badge_id int,
    sash_id int,
    notified_user int,
    updated_at datetime,
    PRIMARY KEY (badge_id)
)

CREATE TABLE comments (
    title text,
    comment text,
    commentable_type text,
    commentable_id int,
    user_id int,
    role text,
    created_at datetime,
    updated_at datetime,
    comment_html text,
    PRIMARY KEY (commentable_id),
    FOREIGN KEY (user_id) OWNED_BY users(id)
);

CREATE TABLE event_attendees (
    event_id int,
    user_id int,
    created_at datetime,
    updated_at datetime,
    status text,
    PRIMARY KEY (event_id),
    FOREIGN KEY (user_id) OWNED_BY users(id)
);

CREATE TABLE events (
    name text,
    event_datetime text,
    user_id int,
    created_at datetime,
    updated_at datetime,
    cached_votes_up int,
    comments_count int,
    location text,
    latlon text,
    PRIMARY KEY (name),
    FOREIGN KEY (user_id) OWNED_BY users(id)
);

CREATE TABLE follows (
    followable_type text,
    followable_id int,
    follower_type text,
    follower_id int,
    blocked text,
    created_at datetime,
    updated_at datetime,
    PRIMARY KEY (followable_id),
    FOREIGN KEY (followable_id) OWNED_BY users(id),
    FOREIGN KEY (follower_id) OWNED_BY users(id)
)

CREATE TABLE friendly_id_slugs (
    slug text,
    sluggable_id int,
    sluggable_type text,
    scope text,
    created_at datetime,
    PRIMARY KEY (slug)
);

CREATE TABLE merit_actions (
    user_id int,
    action_method text,
    action_value text,
    had_errors text,
    target_model text,
    target_id int,
    processed text,
    log text,
    created_at datetime,
    updated_at datetime,
    PRIMARY KEY (user_id)
);

CREATE TABLE merit_activity_logs (
    action_id int,
    related_change_type text,
    related_change_id int,
    description text,
    created_at datetime,
    PRIMARY KEY (action_id)
);

CREATE TABLE merit_score_points (
    score_id int,
    num_points int,
    log text,
    created_at datetime,
    PRIMARY KEY (score_id)
);

CREATE TABLE merit_scores (
    sash_id int,
    category text,
    PRIMARY KEY (sash_id)
);

CREATE TABLE photo_albums (
    title text,
    front_image_url text,
    photos_count int,
    user_id int,
    created_at datetime,
    updated_at datetime,
    slug text,
    cached_votes_up int,
    comments_count int,
    PRIMARY KEY (title),
    FOREIGN KEY (user_id) OWNED_BY users (id)
);

CREATE TABLE photos (
    title text,
    file text,
    photo_album_id int,
    created_at datetime,
    updated_at datetime,
    PRIMARY KEY (file)
);

CREATE TABLE posts (
    content text,
    user_id int,
    attachment text,
    created_at datetime,
    updated_at datetime,
    cached_votes_up int,
    comments_count int,
    preview_html text,
    PRIMARY KEY (user_id),
    FOREIGN KEY (user_id) OWNED_BY users(id)
);

CREATE TABLE sashes (
    id int,
    created_at datetime,
    updated_at datetime,
    PRIMARY KEY (id)
);


CREATE TABLE votes (
    votable_type text,
    votable_id int,
    voter_type text,
    voter_id int,
    vote_flag text,
    vote_scope text,
    vote_weight int,
    created_at datetime,
    updated_at datetime,
    PRIMARY KEY (votable_id)
);

EXPLAIN COMPLIANCE;