SET echo;

CREATE DATA_SUBJECT TABLE users (
  ID INT,
  name TEXT,
  PRIMARY KEY(ID)
);

INSERT INTO users VALUES (1, 'Alice');
INSERT INTO users VALUES (2, 'Bob');

-- escape groups because it's an SQL keyword
CREATE TABLE `groups` (
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
  FOREIGN KEY (group_id) OWNS `groups`(ID)
);

CTX START;
INSERT INTO `groups` VALUES (1, 'Group 1');
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
  -- The creator owns the file.
  FOREIGN KEY (creator) OWNED_BY users(ID),
  -- Sharing with a user or a group only gives them access to the file
  FOREIGN KEY (share_with_user) ACCESSED_BY users(ID),
  FOREIGN KEY (share_with_group) ACCESSED_BY `groups`(ID),
  -- Whoever owns this share (i.e. the creator) also owns the file.
  -- K9db automatically downgrades (but doesnt upgrade) rights of data subjects
  -- when following a path with mixed access and ownership types.
  -- Thus, accessors of this share become accessors of the file.
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

-- Bob has access to file
GDPR GET users 2;

-- File remains even after Bob removes his data.
GDPR FORGET users 2;
SELECT * FROM files;
SELECT * FROM `groups`;

-- File does not remain when creator requests deletion.
-- Groups is also removed cause all members are gone.
GDPR FORGET users 1;
SELECT * FROM files;
SELECT * FROM `groups`;
