SET echo;

CREATE DATA_SUBJECT TABLE users (
  ID INT,
  name TEXT,
  PRIMARY KEY(ID)
);

INSERT INTO users VALUES (1, 'Alice');
INSERT INTO users VALUES (2, 'Bob');

-- groups is an SQL keyword, we use usergroups instead.
CREATE TABLE usergroups (
  ID INT,
  name TEXT,
  PRIMARY KEY (ID)
);

CREATE TABLE members (
  ID INT,
  user_id INT,
  group_id INT,
  PRIMARY KEY (ID),
  FOREIGN KEY (user_id) OWNED_BY users(ID),
  FOREIGN KEY (group_id) OWNS usergroups(ID)
);

CTX START;
INSERT INTO usergroups VALUES (1, 'Group 1');
INSERT INTO members VALUES (1, 1, 1);
CTX COMMIT;

INSERT INTO members VALUES (2, 2, 1);

CREATE TABLE files (
  ID INT,
  path TEXT,
  PRIMARY KEY (ID)
);

CREATE TABLE shares (
  ID INT,
  creator INT,
  share_with_user INT,
  share_with_group INT,
  file_id INT,
  PRIMARY KEY (ID),
  FOREIGN KEY (creator) OWNED_BY users(ID),
  FOREIGN KEY (share_with_user) OWNED_BY users(ID),
  FOREIGN KEY (share_with_group) OWNED_BY usergroups(ID),
  FOREIGN KEY (file_id) OWNS files(ID)
);

CTX START;
INSERT INTO files VALUES (1, 'file 1');
INSERT INTO shares VALUES (1, 1, NULL, NULL, 1);
CTX COMMIT;

-- Share file 1 with bob directly.
-- Application ensures creator is the same.
INSERT INTO shares VALUES (2, 1, 2, NULL, 1);

-- Share file 1 with members of group 1.
INSERT INTO shares VALUES (3, 1, NULL, 1, 1);

-- File remains even after alice (the creator) is removed.
GDPR FORGET users 1;
SELECT * FROM files;
SELECT * FROM usergroups;

-- File goes away when all users it is shared with are gone.
-- Groups is also removed cause all members are gone.
GDPR FORGET users 2;
SELECT * FROM files;
SELECT * FROM usergroups;
