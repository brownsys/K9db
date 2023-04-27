SET echo;

-- Create tables.
CREATE DATA_SUBJECT TABLE doctors (
  ID int,
  name text,
  PRIMARY KEY(ID)
);

CREATE DATA_SUBJECT TABLE patients (
  ID int,
  name text,
  PRIMARY KEY(ID)
);

-- Chat is owned by patient:
-- 1) when patient leaves, all messages exchange with that patient are deleted
-- 2) when doctor leaves, the messages stay for the benefit of the patient
--    but with the doctor's identity anonymized.
CREATE TABLE chat (
  ID int,
  patient_id int,
  doctor_id int,
  message text,
  PRIMARY KEY(ID),
  FOREIGN KEY (patient_id) OWNED_BY patients(ID),
  FOREIGN KEY (doctor_id) ACCESSED_BY doctors(ID),
  ON GET doctor_id ANON (patient_id),
  ON DEL doctor_id ANON (doctor_id),
  ON GET patient_id ANON (doctor_id)
);

-- Insert some data
INSERT INTO patients VALUES (1, 'Alice');
INSERT INTO patients(ID, name) VALUES (2, 'Bob');

INSERT INTO doctors VALUES (10, 'Carl');
INSERT INTO doctors VALUES (20, 'Dracula');

INSERT INTO chat VALUES (1, 1, 10, 'Msg (1)');
INSERT INTO chat VALUES (2, 1, 10, 'Msg (2)');
INSERT INTO chat VALUES (3, 2, 10, 'Msg (3)');
INSERT INTO chat VALUES (4, 1, 20, 'Msg (4)');
INSERT INTO chat VALUES (5, 2, 20, 'Msg (5)');

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
