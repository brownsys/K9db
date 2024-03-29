CREATE TABLE config ( \
  allowNewOwners int NOT NULL, \
  PRIMARY KEY(allowNewOwners) \
);

-- INSERT INTO
-- config (allowNewOwners)
-- VALUES (true);

CREATE DATA_SUBJECT TABLE owners ( \
  ownerHex TEXT NOT NULL UNIQUE PRIMARY KEY, \
  email TEXT NOT NULL, \
  name TEXT NOT NULL, \
  passwordHash TEXT NOT NULL, \
  confirmedEmail TEXT NOT NULL,
  joinDate datetime NOT NULL \
);

CREATE TABLE ownerSessions ( \
  session TEXT NOT NULL UNIQUE PRIMARY KEY, \
  ownerHex TEXT NOT NULL, \
  loginDate datetime NOT NULL, \
  FOREIGN KEY (ownerHex) OWNED_BY owners(ownerHex) \
);

CREATE TABLE ownerConfirmHexes (
  confirmHex TEXT NOT NULL UNIQUE PRIMARY KEY, \
  ownerHex TEXT NOT NULL, \
  sendDate TEXT NOT NULL, \
  FOREIGN KEY (ownerHex) OWNED_BY owners(ownerHex) \
);

CREATE TABLE ownerResetHexes ( \
  resetHex TEXT NOT NULL UNIQUE PRIMARY KEY, \
  ownerHex TEXT NOT NULL, \
  sendDate TEXT NOT NULL, \
  FOREIGN KEY (ownerHex) OWNED_BY owners(ownerHex) \
);

CREATE TABLE domains ( \
  domain TEXT NOT NULL UNIQUE PRIMARY KEY, \
  ownerHex TEXT NOT NULL, \
  name TEXT NOT NULL, \
  creationDate datetime NOT NULL, \
  state TEXT NOT NULL, \
  importedComments TEXT NOT NULL, \
  autoSpamFilter int NOT NULL, \
  requireModeration int NOT NULL, \
  requireIdentification int NOT NULL, \
  viewsThisMonth INTEGER NOT NULL, \
  FOREIGN KEY (ownerHex) OWNED_BY owners(ownerHex) \
);

CREATE DATA_SUBJECT TABLE moderators ( \
  id int NOT NULL,
  domain TEXT NOT NULL, \
  email TEXT NOT NULL, \
  addDate datetime NOT NULL, \
  PRIMARY KEY(id), \
  FOREIGN KEY (domain) REFERENCES ONLY domains(domain) \
);

CREATE DATA_SUBJECT TABLE commenters ( \
  commenterHex TEXT NOT NULL UNIQUE PRIMARY KEY, \
  email TEXT NOT NULL, \
  name TEXT NOT NULL, \
  link TEXT NOT NULL, \
  photo TEXT NOT NULL, \
  provider TEXT NOT NULL, \
  joinDate datetime NOT NULL, \
  state TEXT NOT NULL \
);

CREATE TABLE commenterSessions ( \
  session TEXT NOT NULL UNIQUE PRIMARY KEY, \
  commenterHex TEXT NOT NULL, \
  creationDate datetime NOT NULL, \
  FOREIGN KEY (commenterHex) OWNED_BY commenters(commenterHex) \
);

CREATE TABLE comments ( \
  commentHex TEXT NOT NULL UNIQUE PRIMARY KEY, \
  domain TEXT NOT NULL, \
  path TEXT NOT NULL, \
  commenterHex TEXT NOT NULL, \
  markdown TEXT NOT NULL, \
  html TEXT NOT NULL, \
  parentHex TEXT NOT NULL, \
  score INTEGER NOT NULL, \
  state TEXT NOT NULL, \
  creationDate datetime NOT NULL, \
  FOREIGN KEY (commenterHex) OWNED_BY commenters(commenterHex), \
  FOREIGN KEY (domain) ACCESSED_BY domains(domain), \
  ON DEL parentHex DELETE_ROW, \
  FOREIGN KEY (parentHex) ACCESSED_BY comments(commentHex), \
  ON GET parentHex ANON (commenterHex, score, state), \
  ON GET domain ANON (commenterHex, score, state) \
);

-- DELETEing a comment should recursively delete all children
-- CREATE OR REPLACE FUNCTION commentsDeleteTriggerFunction() RETURNS TRIGGER AS $trigger$
-- BEGIN
--   DELETE FROM comments
--   WHERE parentHex = old.commentHex;

--   RETURN NULL;
-- END;
-- $trigger$ LANGUAGE plpgsql;

-- CREATE TRIGGER commentsDeleteTrigger AFTER DELETE ON comments
-- FOR EACH ROW EXECUTE PROCEDURE commentsDeleteTriggerFunction();

CREATE TABLE votes ( \
  commentHex TEXT NOT NULL, \
  commenterHex TEXT NOT NULL, \
  direction INTEGER NOT NULL, \
  voteDate datetime NOT NULL, \
  PRIMARY KEY(commentHex), \
  FOREIGN KEY (commenterHex) OWNED_BY commenters(commenterHex) \
);

-- CREATE UNIQUE INDEX votesUniqueIndex ON votes(commentHex, commenterHex);

-- CREATE OR REPLACE FUNCTION votesInsertTriggerFunction() RETURNS TRIGGER AS $trigger$
-- BEGIN
--   UPDATE comments
--   SET score = score + new.direction
--   WHERE commentHex = new.commentHex;

--   RETURN NEW;
-- END;
-- $trigger$ LANGUAGE plpgsql;

-- CREATE TRIGGER votesInsertTrigger AFTER INSERT ON votes
-- FOR EACH ROW EXECUTE PROCEDURE votesInsertTriggerFunction();

-- CREATE OR REPLACE FUNCTION votesUpdateTriggerFunction() RETURNS TRIGGER AS $trigger$
-- BEGIN
--   UPDATE comments
--   SET score = score - old.direction + new.direction
--   WHERE commentHex = old.commentHex;

--   RETURN NEW;
-- END;
-- $trigger$ LANGUAGE plpgsql;

-- CREATE TRIGGER votesUpdateTrigger AFTER UPDATE ON votes
-- FOR EACH ROW EXECUTE PROCEDURE votesUpdateTriggerFunction();

-- note: had to add PRIMARY KEY(domain) to avoid "invalid PK" error
CREATE TABLE views ( \
  id int NOT NULL UNIQUE PRIMARY KEY, \
  domain text NOT NULL, \
  commenterHex text NOT NULL, \
  viewDate datetime NOT NULL, \
  FOREIGN KEY (commenterHex) OWNED_BY commenters(commenterHex), \
  FOREIGN KEY (domain) REFERENCES domains(domain) \
);

-- CREATE INDEX IF NOT EXISTS domainIndex ON views(domain);

-- CREATE OR REPLACE FUNCTION viewsInsertTriggerFunction() RETURNS TRIGGER AS $trigger$
-- BEGIN
--   UPDATE domains
--   SET viewsThisMonth = viewsThisMonth + 1
--   WHERE domain = new.domain;

--   RETURN NULL;
-- END;
-- $trigger$ LANGUAGE plpgsql;

-- CREATE TRIGGER viewsInsertTrigger AFTER INSERT ON views
-- FOR EACH ROW EXECUTE PROCEDURE viewsInsertTriggerFunction();

EXPLAIN COMPLIANCE;