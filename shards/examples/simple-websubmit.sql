SET echo;
SET verbose;

CREATE TABLE students ( \
  ID int, \
  PII_Name varchar(100), \
  PRIMARY KEY(ID) \
);

CREATE TABLE assignments ( \
  ID int, \
  Name varchar(100), \
  PRIMARY KEY(ID) \
);

CREATE TABLE submissions ( \
  student_id int, \
  assignment_id int, \
  "timestamp" int, \
  FOREIGN KEY (student_id) REFERENCES students(ID), \
  FOREIGN KEY (assignment_id) REFERENCES assignments(ID) \
);


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

SELECT * FROM assignments;
SELECT * FROM students;
SELECT * FROM submissions;
SELECT * FROM submissions WHERE assignment_id = 1;
SELECT * FROM submissions WHERE student_id = 1;
SELECT * FROM submissions WHERE student_id = 1 AND assignment_id = 2;

DELETE FROM students WHERE ID = 1;
SELECT * FROM submissions;
