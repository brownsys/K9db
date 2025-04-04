CREATE DATA_SUBJECT TABLE users ( \
    email varchar(255), \
    apikey varchar(255), \
    is_admin int, \
    PRIMARY KEY (email), \
    UNIQUE (apikey) \
);

CREATE TABLE lectures ( \
    id int, \
    label varchar(255) DEFAULT 'lecture x', \
    PRIMARY KEY (id) \
);

CREATE TABLE questions ( \
    id int AUTO_INCREMENT, \
    lecture_id int, \
    question_number int, \
    question text, \
    PRIMARY KEY (id), \
    FOREIGN KEY (lecture_id) REFERENCES lectures(id) \
);

CREATE TABLE answers ( \
    id varchar(255), \
    email varchar(255), \
    question_id int, \
    answer text, \
    submitted_at datetime, \
    grade int DEFAULT 0, \
    PRIMARY KEY (id), \
    FOREIGN KEY (email) OWNED_BY users(email), \
    FOREIGN KEY (question_id) REFERENCES questions(id) \
);

CREATE TABLE presenters ( \
    id int AUTO_INCREMENT, \
    lecture_id int, \
    email varchar(255) OWNED_BY users(email), \
    PRIMARY KEY (id), \
    FOREIGN KEY (lecture_id) REFERENCES lectures(id) \
);
