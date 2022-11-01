CREATE TABLE students (ID int, PII_Name text, PRIMARY KEY(ID));
CREATE TABLE assignments (ID int, Name text, PRIMARY KEY(ID));
CREATE TABLE submissions (ID int, student_id int, assignment_id int, timestamp int, \
                          PRIMARY KEY(ID), FOREIGN KEY (student_id) REFERENCES students(ID), \
                          FOREIGN KEY (assignment_id) REFERENCES assignments(ID));
