CREATE TABLE user ( \
    id INTEGER PRIMARY KEY, \
    PII_name TEXT \
);
CREATE TABLE t ( \
    id INT PRIMARY KEY, \
    content TEXT, \
    ownr INT REFERENCES user(id) \
);
CREATE TABLE rel (\
    id INT PRIMARY KEY, \
    OWNER_from_t INT REFERENCES user(id), \
    ACCESSING_to_t INT REFERENCES t(id) \
);
