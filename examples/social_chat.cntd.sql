SET echo;

INSERT INTO Users VALUES (1, 'Alice');
INSERT INTO Users VALUES (2, 'Bob');

SELECT * FROM chat;

INSERT INTO chat VALUES (1, 1, 2, 'HELLO from alice');

SELECT * FROM chat;
GDPR GET Users 1;

GDPR FORGET Users 1;
SELECT * FROM chat;

GDPR FORGET Users 2;
SELECT * FROM chat;
