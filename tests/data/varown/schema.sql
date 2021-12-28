CREATE TABLE user ( \
    id INTEGER PRIMARY KEY, \
    PII_name TEXT \
);
CREATE TABLE t ( \
    id INT PRIMARY KEY, \
    content TEXT \
);
CREATE TABLE rel ( \
    id INT PRIMARY KEY, \
    from_t INT REFERENCES user(id), \
    OWNING_to_t INT REFERENCES t(id) \
);
