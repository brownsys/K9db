SET echo;
SET verbose;

CREATE TABLE doctors ( \
  id int, \
  PII_name varchar(100), \
  PRIMARY KEY(id) \
);

CREATE TABLE patients ( \
  id int, \
  PII_name varchar(100), \
  PRIMARY KEY(id) \
);

CREATE TABLE address_doctors ( \
  id int, \
  doctor_id int REFERENCES doctors(id), \
  city string, \
  PRIMARY KEY(id) \
);

CREATE TABLE address_patients ( \
  id int, \
  patient_id int REFERENCES patients(id), \
  city string, \
  PRIMARY KEY(id) \
);

CREATE TABLE chat ( \
  id int, \
  OWNER_patient_id int, \
  doctor_id int, \
  message string, \
  PRIMARY KEY(id), \
  FOREIGN KEY (OWNER_patient_id) REFERENCES patients(id), \
  FOREIGN KEY (doctor_id) REFERENCES doctors(id) \
);

INSERT INTO doctors VALUES (1, 'Alice');
INSERT INTO doctors(id, PII_name) VALUES (2, 'Bob');

INSERT INTO patients VALUES (10, 'Carl');
INSERT INTO patients VALUES (20, 'Dracula');

INSERT INTO address_doctors VALUES (1, 1, 'Boston');
INSERT INTO address_doctors VALUES (2, 1, 'Providence');
INSERT INTO address_doctors VALUES (3, 2, 'Damascus');

INSERT INTO address_patients VALUES (1, 10, 'Boston');
INSERT INTO address_patients VALUES (2, 20, 'Providence');

INSERT INTO chat VALUES (1, 10, 1, 'HELLO');
INSERT INTO chat VALUES (2, 10, 2, 'Good bye');
INSERT INTO chat VALUES (3, 20, 1, 'HELLO');
INSERT INTO chat VALUES (4, 20, 2, 'HELLO 2');
INSERT INTO chat VALUES (5, 20, 1, 'HELLO 3');

SELECT * FROM chat;
SELECT * FROM chat WHERE doctor_id = 2;
SELECT * FROM chat WHERE OWNER_patient_id = 10;
GET patients 10;
GET doctors 2;

DELETE FROM doctors WHERE id = 2;
SELECT * FROM chat;

DELETE FROM patients WHERE id = 10;
SELECT * FROM patients;
SELECT * FROM chat;