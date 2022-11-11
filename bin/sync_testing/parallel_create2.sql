CREATE DATA_SUBJECT TABLE students1 (ID int, name text, PRIMARY KEY(ID));
CREATE TABLE assignments1 (ID int, Name text, PRIMARY KEY(ID));
CREATE TABLE submissions1 (ID int, student_id int, assignment_id int, timestamp int, \
                           PRIMARY KEY(ID), FOREIGN KEY (student_id) REFERENCES students1(ID), \
                           FOREIGN KEY (assignment_id) REFERENCES assignments1(ID));
