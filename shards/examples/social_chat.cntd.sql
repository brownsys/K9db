SET echo;
SET verbose;

INSERT INTO Users VALUES (1, 'Alice');
INSERT INTO Users VALUES (2, 'Bob');

SELECT * FROM chat;

INSERT INTO chat VALUES (1, 1, 2, 'HELLO from alice');

SELECT * FROM chat;
GET Users 1;

DELETE FROM Users WHERE id = 1;
SELECT * FROM chat;

DELETE FROM Users WHERE id = 2;
SELECT * FROM chat;
