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
  timestamp int, \
  FOREIGN KEY (student_id) REFERENCES students(ID), \
  FOREIGN KEY (assignment_id) REFERENCES assignments(ID) \
);

CREATE TABLE grades ( \
  ID int, \
  student_id int, \
  grade int, \
  assignment_id int, \
  FOREIGN KEY (student_id) REFERENCES students(ID), \
  FOREIGN KEY (assignment_id) REFERENCES assignments(ID) \
);

-- CREATE VIEW avg_grade_per_student AS \
--   '"SELECT student_id,grade AS 'avg_grade' FROM grades WHERE gdpr_purpose LIKE 'avg_grade_per_student'"';
-- CREATE VIEW avg_grade_per_assignment AS \
--   '"SELECT assignments.ID,assignments.Name, grades.grade AS \
--   'avg_grade' FROM assignments JOIN grades ON assignments.ID = grades.assignment_id \
--   WHERE assignments.gdpr_purpose LIKE 'avg_grade_per_assignment'"';

-- CREATE VIEW avg_grade_per_student AS \
--   '"SELECT student_id,   AS 'avg_grade' FROM grades"';
CREATE VIEW avg_grade_per_assignment AS \
  '"SELECT assignments.ID,assignments.Name, grades.grade AS \
  'avg_grade' FROM assignments JOIN grades ON assignments.ID = grades.assignment_id"';


INSERT INTO assignments VALUES (1, 'assignment 1', 'avg_grade_per_assignment');
-- INSERT INTO assignments VALUES (2, 'assignment 2', 'avg_grade_per_assignment, avg_grade_per_student');
INSERT INTO assignments VALUES (2, 'assignment 2', 'avg_grade_per_assignment');
INSERT INTO students VALUES (1, 'Jerry', '');
INSERT INTO students VALUES (2, 'Layne', '');
INSERT INTO students VALUES (3, 'Sean', '');
INSERT INTO submissions VALUES (1, 1, 1, '');
INSERT INTO submissions VALUES (2, 1, 2, '');
INSERT INTO submissions VALUES (3, 1, 3, '');
INSERT INTO submissions VALUES (1, 1, 4, '');
INSERT INTO submissions VALUES (1, 2, 5, '');
INSERT INTO submissions VALUES (2, 2, 6, '');
INSERT INTO submissions VALUES (3, 2, 7, '');
-- INSERT INTO grades VALUES (1, 1, 100, 1, 'avg_grade_per_student, avg_grade_per_assignment');
INSERT INTO grades VALUES (1, 1, 100, 1, 'avg_grade_per_assignment');
INSERT INTO grades VALUES (2, 1, 50, 2, '');
-- INSERT INTO grades VALUES (3, 2, 77, 1, 'avg_grade_per_student, avg_grade_per_assignment');
INSERT INTO grades VALUES (3, 2, 77, 1, 'avg_grade_per_assignment');

-- SELECT * FROM assignments;
-- SELECT * FROM students;
-- SELECT * FROM submissions;
-- SELECT * FROM submissions WHERE assignment_id = 1;
-- SELECT * FROM submissions WHERE student_id = 1;
-- SELECT * FROM submissions WHERE student_id = 1 AND assignment_id = 2;
-- SELECT * FROM grades;
-- SELECT * FROM grades WHERE student_id = 1;

-- GDPR GET students 1;
-- GDPR FORGET students 1;

-- SELECT * FROM submissions;

-- UPDATE submissions SET timestamp = 20 WHERE student_id = 2 AND assignment_id = 1;
-- UPDATE submissions SET timestamp = 30 WHERE assignment_id = 2;
-- SELECT * FROM submissions;


-- SELECT * FROM avg_grade_per_student;
SELECT * FROM avg_grade_per_assignment;
