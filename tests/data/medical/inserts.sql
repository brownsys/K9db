INSERT INTO doctors VALUES (1, 'Alice');
INSERT INTO doctors(id, PII_name) VALUES (2, 'Bob');

INSERT INTO patients VALUES (10, 'Carl');
INSERT INTO patients VALUES (20, 'Dracula');

INSERT INTO address_doctors VALUES (1, 1, 'Boston');
INSERT INTO address_doctors VALUES (2, 1, 'Providence');
INSERT INTO address_doctors VALUES (3, 2, 'Damascus');

INSERT INTO address_patients VALUES (1, 10, 'Boston');
INSERT INTO address_patients VALUES (2, 20, 'Providence');

INSERT INTO chat (OWNER_patient_id, doctor_id, message) VALUES (10, 1, 'HELLO');
INSERT INTO chat (OWNER_patient_id, doctor_id, message) VALUES (10, 2, 'Good bye');
INSERT INTO chat (OWNER_patient_id, doctor_id, message) VALUES (20, 1, 'HELLO');
INSERT INTO chat (OWNER_patient_id, doctor_id, message) VALUES (20, 2, 'HELLO 2');
INSERT INTO chat (OWNER_patient_id, doctor_id, message) VALUES (20, 1, 'HELLO 3');
