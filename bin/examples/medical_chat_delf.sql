SET echo;

CREATE DATA_SUBJECT TABLE doctors ( \
  id int, \
  name text, \
  PRIMARY KEY(id) \
);

CREATE DATA_SUBJECT TABLE patients ( \
  id int, \
  name text, \
  PRIMARY KEY(id) \
);

CREATE TABLE chat ( \
  id int, \
  patient_id int, \
  doctor_id int, \
  message text, \
  PRIMARY KEY(id), \
  FOREIGN KEY (patient_id) OWNED_BY patients(id), \
  FOREIGN KEY (doctor_id) ACCESSED_BY doctors(id) \
);

INSERT INTO doctors VALUES (1, 'Alice');
INSERT INTO doctors(id, name) VALUES (2, 'Bob');

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
SELECT * FROM chat WHERE doctor_id = 2;
SELECT * FROM chat WHERE patient_id = 10;
GDPR GET patients 10;
GDPR GET doctors 2;
SELECT * FROM chat;

-- GDPR FORGET patients 10;
SELECT * FROM patients;
SELECT * FROM chat;
