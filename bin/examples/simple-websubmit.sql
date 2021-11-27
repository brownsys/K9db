SET echo;

CREATE TABLE students ( \
  ID int, \
  PII_Name text, \
  PRIMARY KEY(ID) \
);

CREATE TABLE assignments ( \
  ID int, \
  Name text, \
  PRIMARY KEY(ID) \
);

CREATE TABLE submissions ( \
  student_id int, \
  assignment_id int, \
  ts int, \
  FOREIGN KEY (student_id) REFERENCES students(ID), \
  FOREIGN KEY (assignment_id) REFERENCES assignments(ID) \
);

CREATE TABLE grades ( \
  ID int, \
  student_id int, \
  grade int, \
  FOREIGN KEY (student_id) REFERENCES students(ID) \
);

CREATE VIEW v1 AS '"SELECT students.PII_Name, assignments.Name, submissions.ts FROM students JOIN submissions ON students.ID = submissions.student_id JOIN assignments ON submissions.assignment_id = assignments.ID"';

INSERT INTO assignments VALUES (1, 'assignment 1');
INSERT INTO assignments VALUES (2, 'assignment 2');
INSERT INTO students VALUES (1, 'Jerry');
INSERT INTO students VALUES (2, 'Layne');
INSERT INTO students VALUES (3, 'Sean');
INSERT INTO submissions VALUES (1, 1, 1);
INSERT INTO submissions VALUES (2, 1, 2);
INSERT INTO submissions VALUES (3, 1, 3);
INSERT INTO submissions VALUES (1, 1, 4);
INSERT INTO submissions VALUES (1, 2, 5);
INSERT INTO submissions VALUES (2, 2, 6);
INSERT INTO submissions VALUES (3, 2, 7);
INSERT INTO grades VALUES (1, 1, 100);
INSERT INTO grades VALUES (2, 1, 50);
INSERT INTO grades VALUES (3, 2, 77);

SELECT * FROM assignments;
SELECT * FROM students;
SELECT * FROM submissions;
SELECT * FROM submissions WHERE assignment_id = 1;
SELECT * FROM submissions WHERE student_id = 1;
SELECT * FROM submissions WHERE student_id = 1 AND assignment_id = 2;
SELECT * FROM grades;
SELECT * FROM grades WHERE student_id = 1;
SELECT * FROM v1;

SHOW SHARDS;
SHOW MEMORY;
SHOW VIEW v1;

GDPR GET students 1;
GDPR FORGET students 1;

SELECT * FROM submissions;

UPDATE submissions SET ts = 20 WHERE student_id = 2 AND assignment_id = 1;
UPDATE submissions SET ts = 30 WHERE assignment_id = 2;
SELECT * FROM submissions;
