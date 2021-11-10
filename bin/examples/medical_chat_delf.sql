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
  ACCESSOR_doctor_id int, \
  message text, \
  PRIMARY KEY(id), \
  FOREIGN KEY (OWNER_patient_id) REFERENCES patients(id), \
  FOREIGN KEY (ACCESSOR_doctor_id) REFERENCES doctors(id) \
);

CREATE INDEX ref_accessor ON chat(ACCESSOR_doctor_id);

INSERT INTO doctors VALUES (1, 'Alice');
INSERT INTO doctors(id, PII_name) VALUES (2, 'Bob');

INSERT INTO patients VALUES (10, 'Carl');
INSERT INTO patients VALUES (20, 'Dracula');

INSERT INTO chat VALUES (1, 10, 1, 'HELLO');
INSERT INTO chat VALUES (2, 10, 2, 'Good bye');
INSERT INTO chat VALUES (3, 20, 1, 'HELLO');
INSERT INTO chat VALUES (4, 20, 2, 'HELLO 2');
INSERT INTO chat VALUES (5, 20, 1, 'HELLO 3');

SELECT * FROM chat;
SELECT * FROM chat WHERE ACCESSOR_doctor_id = 2;
SELECT * FROM chat WHERE OWNER_patient_id = 10;
GDPR GET patients 10;
GDPR GET doctors 2;

DELETE FROM doctors WHERE id = 2;
SELECT * FROM chat;

GDPR FORGET patients 10;
SELECT * FROM patients;
SELECT * FROM chat;
