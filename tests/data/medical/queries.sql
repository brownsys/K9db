SELECT * FROM chat;
SELECT * FROM chat WHERE doctor_id = 2;

SELECT id FROM doctors;
SELECT message FROM chat;
SELECT message, OWNER_patient_id FROM chat;
