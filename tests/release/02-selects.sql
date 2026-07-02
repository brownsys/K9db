-- Point lookups on the sharded tables. Run right after 01-schema.sql and
-- again after a container restart (the data must persist).
SELECT * FROM patients WHERE ID = 1;
SELECT * FROM patients WHERE ID = 2;
SELECT * FROM chat WHERE ID = 3;
