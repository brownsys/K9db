INSERT INTO doctors VALUES (1, 'Alice');
INSERT INTO doctors(id, PII_name) VALUES (2, 'Bob');

INSERT INTO patients VALUES (10, 'Carl');
INSERT INTO patients VALUES (20, 'Dracula');

INSERT INTO address_doctors VALUES (1, 1, 'Boston');
INSERT INTO address_doctors VALUES (2, 1, 'Providence');
INSERT INTO address_doctors VALUES (3, 2, 'Damascus');

INSERT INTO address_patients VALUES (1, 10, 'Boston');
INSERT INTO address_patients VALUES (2, 20, 'Providence');

INSERT INTO chat VALUES (1, 10, 1, 'HELLO');
INSERT INTO chat VALUES (2, 10, 2, 'Good bye');
INSERT INTO chat VALUES (3, 20, 1, 'HELLO');
INSERT INTO chat VALUES (4, 20, 2, 'HELLO 2');
INSERT INTO chat VALUES (5, 20, 1, 'HELLO 3');
