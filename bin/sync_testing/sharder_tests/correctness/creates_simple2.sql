CREATE TABLE students1 (ID int, PII_Name text, PRIMARY KEY(ID));
CREATE TABLE assignments1 (ID int, Name text, PRIMARY KEY(ID));
CREATE TABLE submissions1 (student_id int, assignment_id int, timestamp int, FOREIGN KEY (student_id) REFERENCES students(ID), FOREIGN KEY (assignment_id) REFERENCES assignments(ID));
