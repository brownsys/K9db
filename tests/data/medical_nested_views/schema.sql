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
CREATE TABLE address_doctors ( \
  id int, \
  doctor_id int REFERENCES doctors(id), \
  city text, \
  PRIMARY KEY(id) \
);
CREATE TABLE address_patients ( \
  id int, \
  patient_id int REFERENCES patients(id), \
  city text, \
  PRIMARY KEY(id) \
);
CREATE TABLE chat ( \
  id int AUTO_INCREMENT, \
  OWNER_patient_id int, \
  doctor_id int, \
  message text, \
  PRIMARY KEY(id), \
  FOREIGN KEY (OWNER_patient_id) REFERENCES patients(id), \
  FOREIGN KEY (doctor_id) REFERENCES doctors(id) \
);
