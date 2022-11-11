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
  id int, \
  patient_id int, \
  doctor_id int, \
  message text, \
  PRIMARY KEY(id), \
  FOREIGN KEY (patient_id) OWNED_BY patients(id), \
  FOREIGN KEY (doctor_id) REFERENCES doctors(id) \
);
