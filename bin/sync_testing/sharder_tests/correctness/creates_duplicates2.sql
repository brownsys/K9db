CREATE TABLE students19 (ID int, PII_Name text, PRIMARY KEY(ID));
CREATE TABLE students20 (ID int, PII_Name text, PRIMARY KEY(ID));
CREATE TABLE students21 (ID int, PII_Name text, PRIMARY KEY(ID));
CREATE TABLE students22 (ID int, PII_Name text, PRIMARY KEY(ID));
CREATE TABLE students23 (ID int, PII_Name text, PRIMARY KEY(ID));
CREATE TABLE students24 (ID int, PII_Name text, PRIMARY KEY(ID));
CREATE TABLE students25 (ID int, PII_Name text, PRIMARY KEY(ID));
CREATE TABLE students26 (ID int, PII_Name text, PRIMARY KEY(ID));
CREATE TABLE students27 (ID int, PII_Name text, PRIMARY KEY(ID));
CREATE TABLE students28 (ID int, PII_Name text, PRIMARY KEY(ID));
CREATE TABLE students29 (ID int, PII_Name text, PRIMARY KEY(ID));
CREATE TABLE students30 (ID int, PII_Name text, PRIMARY KEY(ID));
CREATE TABLE students31 (ID int, PII_Name text, PRIMARY KEY(ID));
CREATE TABLE students32 (ID int, PII_Name text, PRIMARY KEY(ID));
CREATE TABLE students33 (ID int, PII_Name text, PRIMARY KEY(ID));
CREATE TABLE students34 (ID int, PII_Name text, PRIMARY KEY(ID));
CREATE TABLE students35 (ID int, PII_Name text, PRIMARY KEY(ID));
CREATE TABLE students36 (ID int, PII_Name text, PRIMARY KEY(ID));
CREATE TABLE students37 (ID int, PII_Name text, PRIMARY KEY(ID));
CREATE TABLE students38 (ID int, PII_Name text, PRIMARY KEY(ID));
CREATE TABLE students39 (ID int, PII_Name text, PRIMARY KEY(ID));
CREATE TABLE assignments19 (student_id int, assignment_id int, timestamp int, PRIMARY KEY(ID));
CREATE TABLE assignments20 (student_id int, assignment_id int, timestamp int, PRIMARY KEY(ID));
CREATE TABLE assignments21 (student_id int, assignment_id int, timestamp int, PRIMARY KEY(ID));
CREATE TABLE assignments22 (student_id int, assignment_id int, timestamp int, PRIMARY KEY(ID));
CREATE TABLE assignments23 (student_id int, assignment_id int, timestamp int, PRIMARY KEY(ID));
CREATE TABLE assignments24 (student_id int, assignment_id int, timestamp int, PRIMARY KEY(ID));
CREATE TABLE assignments25 (student_id int, assignment_id int, timestamp int, PRIMARY KEY(ID));
CREATE TABLE assignments26 (student_id int, assignment_id int, timestamp int, PRIMARY KEY(ID));
CREATE TABLE assignments27 (student_id int, assignment_id int, timestamp int, PRIMARY KEY(ID));
CREATE TABLE assignments28 (student_id int, assignment_id int, timestamp int, PRIMARY KEY(ID));
CREATE TABLE assignments29 (student_id int, assignment_id int, timestamp int, PRIMARY KEY(ID));
CREATE TABLE assignments30 (student_id int, assignment_id int, timestamp int, PRIMARY KEY(ID));
CREATE TABLE assignments31 (student_id int, assignment_id int, timestamp int, PRIMARY KEY(ID));
CREATE TABLE assignments32 (student_id int, assignment_id int, timestamp int, PRIMARY KEY(ID));
CREATE TABLE assignments33 (student_id int, assignment_id int, timestamp int, PRIMARY KEY(ID));
CREATE TABLE assignments34 (student_id int, assignment_id int, timestamp int, PRIMARY KEY(ID));
CREATE TABLE assignments35 (student_id int, assignment_id int, timestamp int, PRIMARY KEY(ID));
CREATE TABLE assignments36 (student_id int, assignment_id int, timestamp int, PRIMARY KEY(ID));
CREATE TABLE assignments37 (student_id int, assignment_id int, timestamp int, PRIMARY KEY(ID));
CREATE TABLE assignments38 (student_id int, assignment_id int, timestamp int, PRIMARY KEY(ID));
CREATE TABLE assignments39 (student_id int, assignment_id int, timestamp int, PRIMARY KEY(ID));
CREATE TABLE submissions19 (student_id int, assignment_id int, timestamp int, FOREIGN KEY (student_id) REFERENCES students(ID), FOREIGN KEY (assignment_id) REFERENCES assignments(ID));
CREATE TABLE submissions20 (student_id int, assignment_id int, timestamp int, FOREIGN KEY (student_id) REFERENCES students(ID), FOREIGN KEY (assignment_id) REFERENCES assignments(ID));
CREATE TABLE submissions21 (student_id int, assignment_id int, timestamp int, FOREIGN KEY (student_id) REFERENCES students(ID), FOREIGN KEY (assignment_id) REFERENCES assignments(ID));
CREATE TABLE submissions22 (student_id int, assignment_id int, timestamp int, FOREIGN KEY (student_id) REFERENCES students(ID), FOREIGN KEY (assignment_id) REFERENCES assignments(ID));
CREATE TABLE submissions23 (student_id int, assignment_id int, timestamp int, FOREIGN KEY (student_id) REFERENCES students(ID), FOREIGN KEY (assignment_id) REFERENCES assignments(ID));
CREATE TABLE submissions24 (student_id int, assignment_id int, timestamp int, FOREIGN KEY (student_id) REFERENCES students(ID), FOREIGN KEY (assignment_id) REFERENCES assignments(ID));
CREATE TABLE submissions25 (student_id int, assignment_id int, timestamp int, FOREIGN KEY (student_id) REFERENCES students(ID), FOREIGN KEY (assignment_id) REFERENCES assignments(ID));
CREATE TABLE submissions26 (student_id int, assignment_id int, timestamp int, FOREIGN KEY (student_id) REFERENCES students(ID), FOREIGN KEY (assignment_id) REFERENCES assignments(ID));
CREATE TABLE submissions27 (student_id int, assignment_id int, timestamp int, FOREIGN KEY (student_id) REFERENCES students(ID), FOREIGN KEY (assignment_id) REFERENCES assignments(ID));
CREATE TABLE submissions28 (student_id int, assignment_id int, timestamp int, FOREIGN KEY (student_id) REFERENCES students(ID), FOREIGN KEY (assignment_id) REFERENCES assignments(ID));
CREATE TABLE submissions29 (student_id int, assignment_id int, timestamp int, FOREIGN KEY (student_id) REFERENCES students(ID), FOREIGN KEY (assignment_id) REFERENCES assignments(ID));
CREATE TABLE submissions30 (student_id int, assignment_id int, timestamp int, FOREIGN KEY (student_id) REFERENCES students(ID), FOREIGN KEY (assignment_id) REFERENCES assignments(ID));
CREATE TABLE submissions31 (student_id int, assignment_id int, timestamp int, FOREIGN KEY (student_id) REFERENCES students(ID), FOREIGN KEY (assignment_id) REFERENCES assignments(ID));
CREATE TABLE submissions32 (student_id int, assignment_id int, timestamp int, FOREIGN KEY (student_id) REFERENCES students(ID), FOREIGN KEY (assignment_id) REFERENCES assignments(ID));
CREATE TABLE submissions33 (student_id int, assignment_id int, timestamp int, FOREIGN KEY (student_id) REFERENCES students(ID), FOREIGN KEY (assignment_id) REFERENCES assignments(ID));
CREATE TABLE submissions34 (student_id int, assignment_id int, timestamp int, FOREIGN KEY (student_id) REFERENCES students(ID), FOREIGN KEY (assignment_id) REFERENCES assignments(ID));
CREATE TABLE submissions35 (student_id int, assignment_id int, timestamp int, FOREIGN KEY (student_id) REFERENCES students(ID), FOREIGN KEY (assignment_id) REFERENCES assignments(ID));
CREATE TABLE submissions36 (student_id int, assignment_id int, timestamp int, FOREIGN KEY (student_id) REFERENCES students(ID), FOREIGN KEY (assignment_id) REFERENCES assignments(ID));
CREATE TABLE submissions37 (student_id int, assignment_id int, timestamp int, FOREIGN KEY (student_id) REFERENCES students(ID), FOREIGN KEY (assignment_id) REFERENCES assignments(ID));
CREATE TABLE submissions38 (student_id int, assignment_id int, timestamp int, FOREIGN KEY (student_id) REFERENCES students(ID), FOREIGN KEY (assignment_id) REFERENCES assignments(ID));
CREATE TABLE submissions39 (student_id int, assignment_id int, timestamp int, FOREIGN KEY (student_id) REFERENCES students(ID), FOREIGN KEY (assignment_id) REFERENCES assignments(ID));