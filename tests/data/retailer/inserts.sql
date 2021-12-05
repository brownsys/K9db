INSERT INTO customers VALUES (1, 0, 'Alice');
INSERT INTO customers VALUES (2, 0, 'Bob');
INSERT INTO customers VALUES (3, 1, 'Charlie');
INSERT INTO customers VALUES (4, 1, 'David');
INSERT INTO customers VALUES (5, 1, 'Eve');
INSERT INTO customers VALUES (6, 1, 'Frank');


SET POLICY FOR customers 1 ALLOW ("basic_functionality");
SET POLICY FOR customers 2 ALLOW ("basic_functionality", "ads");

