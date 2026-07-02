-- Run against the second container: writes must work on the persisted state.
INSERT INTO chat VALUES (4, 2, 'Bob after restart');
SELECT * FROM chat WHERE ID = 4;
