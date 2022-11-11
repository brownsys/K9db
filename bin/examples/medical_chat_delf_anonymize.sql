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
  doctor_name text, \
  message text, \
  PRIMARY KEY(id), \
  FOREIGN KEY (doctor_id) ACCESSED_BY doctors(id) \
);

INSERT INTO doctors VALUES (1, 'Alice');
INSERT INTO doctors(id, name) VALUES (2, 'Bob');

INSERT INTO patients VALUES (10, 'Carl');
INSERT INTO patients VALUES (20, 'Dracula');

INSERT INTO chat VALUES (1, 10, 1, 'Alice', 'HELLO');
INSERT INTO chat VALUES (2, 10, 2, 'Bob', 'Good bye');
INSERT INTO chat VALUES (3, 20, 1, 'Alice', 'HELLO');
INSERT INTO chat VALUES (4, 20, 2, 'Bob', 'HELLO 2');
INSERT INTO chat VALUES (5, 20, 1, 'Alice', 'HELLO 3');

SELECT * FROM chat;
GDPR FORGET doctors 2;
SELECT * FROM chat;
