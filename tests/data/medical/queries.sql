SELECT * FROM chat;
SELECT * FROM chat WHERE doctor_id = 2;

SELECT id FROM doctors;
SELECT message FROM chat;
SELECT message, OWNER_patient_id FROM chat;

SELECT 1, 'str' FROM chat WHERE doctor_id = 2;

SELECT * FROM chat WHERE message IN ('HELLO', 'Good bye');
SELECT * FROM chat WHERE doctor_id IN (2);