SET echo;

CREATE DATA_SUBJECT TABLE doctors (
  id int,
  name text,
  PRIMARY KEY(id)
);

CREATE DATA_SUBJECT TABLE patients (
  id int,
  name text,
  PRIMARY KEY(id)
);

CREATE TABLE address_doctors (
  id int,
  doctor_id int REFERENCES doctors(id),
  city text,
  PRIMARY KEY(id)
);

CREATE TABLE address_patients (
  id int,
  patient_id int REFERENCES patients(id),
  city text,
  PRIMARY KEY(id)
);

CREATE TABLE chat (
  id int,
  patient_id int,
  doctor_id int,
  message text,
  PRIMARY KEY(id),
  FOREIGN KEY (patient_id) OWNED_BY patients(id),
  FOREIGN KEY (doctor_id) REFERENCES doctors(id)
);

INSERT INTO doctors VALUES (1, 'Alice');
INSERT INTO doctors(id, name) VALUES (2, 'Bob');

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
SELECT * FROM chat WHERE patient_id = 10;
#GDPR GET patients 10;
#GDPR GET doctors 2;

-- Test view creation after inserts.
CREATE VIEW v1 AS '"SELECT doctors.name, patients.name FROM doctors JOIN chat ON doctors.id = chat.doctor_id JOIN patients ON chat.patient_id = patients.id"';
SELECT * FROM v1;

#GDPR FORGET doctors 1;
SELECT * FROM chat;

#GDPR FORGET patients 10;
SELECT * FROM patients;
SELECT * FROM chat;

SELECT * FROM v1;

SHOW MEMORY;
SHOW VIEW v1;
SHOW SHARDS;
