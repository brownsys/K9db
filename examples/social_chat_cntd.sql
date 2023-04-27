SET echo;

-- We can pick off where we left of after closing a database connection, or
-- even restarting the database server.
SELECT * FROM chat;

-- Can access data.
GDPR GET users 1;

-- Leaving may leave data if it still has owners.
GDPR FORGET users 1;
SELECT * FROM chat;

GDPR FORGET users 2;
SELECT * FROM chat;
