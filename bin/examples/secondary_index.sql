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
  id int, \
  student_id int, \
  assignment_id int, \
  timestamp int, \
  PRIMARY KEY(id), \
  FOREIGN KEY (student_id) REFERENCES students(ID), \
  FOREIGN KEY (assignment_id) REFERENCES assignments(ID) \
);

CREATE INDEX pk_index ON submissions (id);
CREATE INDEX ass_index ON submissions (assignment_id);


INSERT INTO assignments VALUES (1, 'assignment 1');
INSERT INTO assignments VALUES (2, 'assignment 2');
INSERT INTO students VALUES (1, 'Jerry');
INSERT INTO students VALUES (2, 'Layne');
INSERT INTO students VALUES (3, 'Sean');
INSERT INTO submissions VALUES (1, 1, 1, 1);
INSERT INTO submissions VALUES (2, 2, 1, 2);
INSERT INTO submissions VALUES (3, 3, 1, 3);
INSERT INTO submissions VALUES (4, 1, 1, 4);
INSERT INTO submissions VALUES (5, 1, 2, 5);
INSERT INTO submissions VALUES (6, 2, 2, 6);
INSERT INTO submissions VALUES (7, 3, 2, 7);

SELECT * FROM pk_index_student_id;
SELECT * FROM ass_index_student_id;
