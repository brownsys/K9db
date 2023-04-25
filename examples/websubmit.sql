SET echo;

-- Create tables.
CREATE DATA_SUBJECT TABLE students (
  ID int,
  name text,
  PRIMARY KEY(ID)
);

CREATE TABLE assignments (
  ID int,
  Name text,
  PRIMARY KEY(ID)
);

CREATE TABLE submissions (
  ID int,
  student_id int,
  assignment_id int,
  ts int,
  FOREIGN KEY (student_id) REFERENCES students(ID),
  FOREIGN KEY (assignment_id) REFERENCES assignments(ID),
  PRIMARY KEY(ID)
);

-- Create a view for caching
CREATE VIEW count_view AS '"
    SELECT students.name as name, count(submissions.ID)
    FROM students JOIN submissions ON students.ID = submissions.student_id
    GROUP BY students.name
    HAVING name = ?"';

-- Insert data.
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

-- Queries behave as expected
SELECT * FROM assignments;
SELECT * FROM students;
SELECT * FROM submissions;
SELECT * FROM submissions WHERE assignment_id = 1;
SELECT * FROM submissions WHERE student_id = 1;
SELECT * FROM submissions WHERE student_id = 1 AND assignment_id = 2;
SELECT * FROM count_view WHERE name IN ('Layne', 'Jerry');

-- List the types of data subjects, and information about views.
SHOW SHARDS;
SHOW MEMORY;
SHOW VIEW count_view;

-- Get all data for student 'Jerry'
GDPR GET students 1;

-- Delete all data owned by 'Jerry'
GDPR FORGET students 1;

-- Validate that data is indeed gone.
SELECT * FROM submissions;
SELECT * FROM count_view WHERE name IN ('Layne', 'Jerry');
