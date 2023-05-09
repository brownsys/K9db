CREATE DATA_SUBJECT TABLE users (
  id INT PRIMARY KEY,
  email TEXT
);

CREATE DATA_SUBJECT TABLE invited_users (
  id INT PRIMARY KEY,
  email TEXT,
  inviting_user INT OWNED_BY users(id)
);

EXPLAIN COMPLIANCE;
