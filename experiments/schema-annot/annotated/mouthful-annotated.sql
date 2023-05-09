-- modified from original, see issue #171
CREATE DATA_SUBJECT TABLE user (
    id text PRIMARY KEY
);

CREATE TABLE Thread(
    Id int PRIMARY KEY,
    CreatedAt datetime not null,
    Path text not null
);

CREATE TABLE Comment(
    Id int PRIMARY KEY,
    ThreadId int not null,
    Body text not null,
    Author text not null,
    Confirmed int not null,
    CreatedAt datetime not null,
    ReplyTo int not null,
    DeletedAt datetime not null,
    FOREIGN KEY(ThreadId) references Thread(Id),
    FOREIGN KEY(Author) OWNED_BY user(id)
);

EXPLAIN COMPLIANCE;