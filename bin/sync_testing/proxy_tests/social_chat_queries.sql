SELECT * FROM chat1;

UPDATE chat1 SET message = 'Hello 2' WHERE message = 'HELLO from carl 2';
UPDATE chat1 SET message = 'From Alice' WHERE OWNER_sender_id = 1;
UPDATE chat1 SET message = 'To Alice' WHERE OWNER_receiver_id = 1;
SELECT * FROM chat1;

INSERT INTO chat1 VALUES (5, 1, 2, 'Message will move');
UPDATE chat1 SET OWNER_sender_id = 3 WHERE id = 5 AND OWNER_receiver_id = 2 AND OWNER_sender_id = 1;
SELECT * FROM chat1 WHERE id = 5;
UPDATE chat1 SET OWNER_sender_id = 2, OWNER_receiver_id = 3 WHERE id = 5;
SELECT * FROM chat1 WHERE id = 5;
SELECT * FROM chat1 WHERE OWNER_sender_id = 1;
SELECT * FROM chat1 WHERE OWNER_sender_id = 2;
SELECT * FROM chat1 WHERE OWNER_sender_id = 3;

DELETE FROM users1 WHERE id = 1;
SELECT * FROM chat1;
SELECT * FROM chat1 WHERE OWNER_sender_id = 1;

DELETE FROM users1 WHERE id = 2;
SELECT * FROM chat1;
