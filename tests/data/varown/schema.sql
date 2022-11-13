CREATE DATA_SUBJECT TABLE user ( \
    id INTEGER PRIMARY KEY, \
    name TEXT \
);
CREATE TABLE t ( \
    id INT PRIMARY KEY, \
    content TEXT \
);
CREATE TABLE rel ( \
    id INT PRIMARY KEY, \
    from_t INT OWNED_BY user(id), \
    to_t INT OWNS t(id) \
);
