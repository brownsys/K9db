SET echo;

-- Create tables.
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

-- Chat is owned by patient:
-- 1) when patient leaves, all messages exchange with that patient are deleted
-- 2) when doctor leaves, the messages stay for the benefit of the patient
--    but with the doctor's identity anonymized.
CREATE TABLE chat (
  id int,
  patient_id int,
  doctor_id int,
  message text,
  PRIMARY KEY(id),
  FOREIGN KEY (patient_id) OWNED_BY patients(id),
  FOREIGN KEY (doctor_id) ACCESSED_BY doctors(id),
  ON GET doctor_id ANON (patient_id),
  ON DEL doctor_id ANON (doctor_id)
);

-- Insert some data
INSERT INTO patients VALUES (1, 'Alice');
INSERT INTO patients(id, name) VALUES (2, 'Bob');

INSERT INTO doctors VALUES (10, 'Carl');
INSERT INTO doctors VALUES (20, 'Dracula');

INSERT INTO chat VALUES (1, 1, 10, 'Alice (1)');
INSERT INTO chat VALUES (2, 1, 10, 'Alice (2)');
INSERT INTO chat VALUES (3, 2, 10, 'Bob (1)');
INSERT INTO chat VALUES (4, 1, 20, 'Alice (3)');
INSERT INTO chat VALUES (5, 2, 20, 'Bob (2)');

-- Validate data.
SELECT * FROM chat;

-- Doctors and patients can access their data.
GDPR GET doctors 10;
GDPR GET patients 1;

-- Doctors leaving only anonymize chat.
GDPR FORGET doctors 10;
SELECT * FROM chat;

-- Patients leaving removes chat.
GDPR FORGET patients 1;
SELECT * FROM chat;
