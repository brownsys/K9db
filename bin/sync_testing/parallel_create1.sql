CREATE DATA_SUBJECT TABLE students0 (ID int, name text, PRIMARY KEY(ID));
CREATE TABLE assignments0 (ID int, Name text, PRIMARY KEY(ID));
CREATE TABLE submissions0 (ID int, student_id int, assignment_id int, timestamp int, \
                           PRIMARY KEY(ID), FOREIGN KEY (student_id) REFERENCES students0(ID), \
                           FOREIGN KEY (assignment_id) REFERENCES assignments0(ID));
