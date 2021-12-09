SET echo;

CREATE TABLE doctors ( \
  id int, \
  PII_name text, \
  PRIMARY KEY(id) \
);

CREATE TABLE patients ( \
  id int, \
  PII_name text, \
  PRIMARY KEY(id) \
);

CREATE TABLE chat ( \
  id int, \
  OWNER_patient_id int, \
  ACCESSOR_ANONYMIZE_doctor_id int, \
  message text, \
  PRIMARY KEY(id), \
  FOREIGN KEY (OWNER_patient_id) REFERENCES patients(id), \
  FOREIGN KEY (ACCESSOR_ANONYMIZE_doctor_id) REFERENCES doctors(id) \
);

INSERT INTO doctors VALUES (1, 'Alice');
INSERT INTO doctors(id, PII_name) VALUES (2, 'Bob');

INSERT INTO patients VALUES (10, 'Carl');
INSERT INTO patients VALUES (20, 'Dracula');
INSERT INTO patients VALUES (30, 'Banji');
INSERT INTO patients VALUES (40, 'Patient 4');
INSERT INTO patients VALUES (50, 'Patient 5');

INSERT INTO chat VALUES (1, 10, 1, 'HELLO');
INSERT INTO chat VALUES (2, 10, 2, 'Good bye');
INSERT INTO chat VALUES (3, 20, 1, 'HELLO');
INSERT INTO chat VALUES (4, 20, 2, 'HELLO 2');
INSERT INTO chat VALUES (5, 20, 1, 'HELLO 3');
INSERT INTO chat VALUES (6, 30, 1, 'HELLO');
INSERT INTO chat VALUES (7, 40, 1, 'HELLO 2');
INSERT INTO chat VALUES (8, 50, 1, 'HELLO 3');

SELECT * FROM chat;
SELECT * FROM chat WHERE ACCESSOR_doctor_id = 2;
SELECT * FROM chat WHERE OWNER_patient_id = 10;
GDPR GET patients 10;
GDPR GET doctors 2;
ref_doctors0_OWNER_patient_id
SELECT * FROM chat;

-- GDPR FORGET patients 10;
SELECT * FROM patients;
SELECT * FROM chat;
-- SELECT * FROM ref_doctors_OWNER_patient_id;