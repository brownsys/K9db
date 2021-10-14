CREATE TABLE students0 (ID int, PII_Name text, PRIMARY KEY(ID));
CREATE TABLE assignments0 (ID int, Name text, PRIMARY KEY(ID));
CREATE TABLE submissions0 (student_id int, assignment_id int, timestamp int, FOREIGN KEY (student_id) REFERENCES students(ID), FOREIGN KEY (assignment_id) REFERENCES assignments(ID));
