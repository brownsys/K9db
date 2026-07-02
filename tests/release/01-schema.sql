-- Schema and initial data for the release container test
-- (.github/workflows/releasetest.yml). Statements produce no output;
-- queries and their expected outputs live in the numbered files next to
-- this one.
CREATE DATA_SUBJECT TABLE patients (
  ID int,
  name text,
  PRIMARY KEY(ID)
);

CREATE TABLE chat (
  ID int,
  patient_id int,
  message text,
  PRIMARY KEY(ID),
  FOREIGN KEY (patient_id) OWNED_BY patients(ID)
);

-- Exercises the embedded JVM / calcite planner. The ? parameter makes this a
-- keyed view, so it can be queried with WHERE patient_id = <value>.
CREATE VIEW chat_counts AS '"SELECT patient_id, COUNT(ID) FROM chat GROUP BY patient_id HAVING patient_id = ?"';

INSERT INTO patients VALUES (1, 'Alice');
INSERT INTO patients VALUES (2, 'Bob');
INSERT INTO chat VALUES (1, 1, 'Hello from Alice');
INSERT INTO chat VALUES (2, 2, 'Hello from Bob');
INSERT INTO chat VALUES (3, 1, 'Alice again');
