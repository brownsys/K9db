CREATE DATA_SUBJECT TABLE user ( \
    id INTEGER PRIMARY KEY, \
    name TEXT \
);
CREATE TABLE t ( \
    id INT PRIMARY KEY, \
    content TEXT, \
    ownr INT REFERENCES user(id) \
);
CREATE TABLE rel (\
    id INT PRIMARY KEY, \
    from_t INT OWNED_BY user(id), \
    to_t INT ACCESSES t(id) \
);
