SET echo;

CREATE TABLE students ( \
  ID int, \
  PII_Name text, \
  PRIMARY KEY(ID) \
) DATA_SUBJECT;

CREATE TABLE assignments ( \
  ID int, \
  Name text, \
  PRIMARY KEY(ID) \
);

CREATE TABLE submissions ( \
  OWNER_student_id int, \
  assignment_id int, \
  timestamp int, \
  FOREIGN KEY (OWNER_student_id) REFERENCES students(ID), \
  FOREIGN KEY (assignment_id) REFERENCES assignments(ID) \
);

CREATE TABLE grades ( \
  ID int, \
  OWNER_student_id int, \
  grade int, \
  assignment_id int, \
  FOREIGN KEY (OWNER_student_id) REFERENCES students(ID), \
  FOREIGN KEY (assignment_id) REFERENCES assignments(ID) \
);

-- CREATE VIEW avg_grade_per_student AS \
--   '"SELECT student_id,grade AS 'avg_grade' FROM grades WHERE gdpr_purpose LIKE 'avg_grade_per_student'"';
-- CREATE VIEW avg_grade_per_assignment AS \
--   '"SELECT assignments.ID,assignments.Name, grades.grade AS \
--   'avg_grade' FROM assignments JOIN grades ON assignments.ID = grades.assignment_id \
--   WHERE assignments.gdpr_purpose LIKE 'avg_grade_per_assignment'"';

CREATE VIEW v1_avg_grade_per_student AS \
  '"SELECT OWNER_student_id, grade   AS 'avg_grade' FROM grades"';

CREATE VIEW v2_avg_grade_per_assignment AS \
  '"SELECT assignments.ID,assignments.Name, grades.grade AS \
  'avg_grade' FROM assignments JOIN grades ON assignments.ID = grades.assignment_id"';


-- assignments - ID, name
INSERT INTO assignments VALUES (1, 'assignment 1', 'avg_grade_per_assignment');
INSERT INTO assignments VALUES (2, 'assignment 2', 'avg_grade_per_assignment, avg_grade_per_student');
-- INSERT INTO assignments VALUES (2, 'assignment 2', 'avg_grade_per_student');

-- students - ID, name
INSERT INTO students VALUES (1, 'Jerry', '');
INSERT INTO students VALUES (2, 'Layne', '');
INSERT INTO students VALUES (3, 'Sean', '');

-- submissions - student_id, assignment_id, timestamp
INSERT INTO submissions VALUES (1, 1, 1, '');
INSERT INTO submissions VALUES (2, 1, 2, '');
INSERT INTO submissions VALUES (3, 1, 3, '');
INSERT INTO submissions VALUES (1, 1, 4, '');
INSERT INTO submissions VALUES (1, 2, 5, '');
INSERT INTO submissions VALUES (2, 2, 6, '');
INSERT INTO submissions VALUES (3, 2, 7, '');

-- grades - ID, student_id, grade, assignment_id
INSERT INTO grades VALUES (1, 1, 100, 1, 'avg_grade_per_student, avg_grade_per_assignment');
-- INSERT INTO grades VALUES (1, 1, 100, 1, 'avg_grade_per_assignment');
INSERT INTO grades VALUES (2, 1, 50, 2, 'avg_grade_per_assignment');
-- INSERT INTO grades VALUES (3, 2, 77, 1, 'avg_grade_per_student, avg_grade_per_assignment');
-- INSERT INTO grades VALUES (3, 2, 77, 1, 'avg_grade_per _assignment');

SELECT * FROM v1_avg_grade_per_student;
SELECT * FROM v2_avg_grade_per_assignment;

-- UPDATE grades SET gdpr_purpose = '' WHERE ID = 2;
DELETE FROM grades where ID = 2;
-- VALUES (2, 1, 50, 2, 'avg_grade_per_assignment');

SELECT * FROM v1_avg_grade_per_student;
SELECT * FROM v2_avg_grade_per_assignment;

