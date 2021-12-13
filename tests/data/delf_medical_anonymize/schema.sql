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
