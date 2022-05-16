
CREATE TABLE users_DP_budget20 (
	id int,
	PII_name text,
	age int,
	PRIMARY KEY(id)
);

INSERT INTO users_DP_budget20 VALUES (1, 'd1', 73);
INSERT INTO users_DP_budget20 VALUES (2, 'd2', 4);
INSERT INTO users_DP_budget20 VALUES (3, 'd3', 54);
INSERT INTO users_DP_budget20 VALUES (4, 'd4', 61);
INSERT INTO users_DP_budget20 VALUES (5, 'd5', 73);
INSERT INTO users_DP_budget20 VALUES (6, 'd6', 1);
INSERT INTO users_DP_budget20 VALUES (7, 'd7', 26);
INSERT INTO users_DP_budget20 VALUES (8, 'd8', 59);
INSERT INTO users_DP_budget20 VALUES (9, 'd9', 62);
INSERT INTO users_DP_budget20 VALUES (10, 'd10', 35);
	
CREATE VIEW `v1` AS '"SELECT SUM(age) FROM users_DP_budget20"';
SELECT * FROM `v1`;

CREATE VIEW `v2_DP_e1_l0_h99!5` AS '"SELECT SUM(age) FROM users_DP_budget20"';
SELECT * FROM `v2_DP_e1_l0_h99!5`;
CREATE VIEW `v3_DP_e2!8_l0_h99!5` AS '"SELECT SUM(age) FROM users_DP_budget20"';
SELECT * FROM `v3_DP_e2!8_l0_h99!5`;
CREATE VIEW `v4_DP_e4_l0_h99!5` AS '"SELECT SUM(age) FROM users_DP_budget20"';
SELECT * FROM `v4_DP_e4_l0_h99!5`;
CREATE VIEW `v5_DP_e10!2_l0_h99!5` AS '"SELECT SUM(age) FROM users_DP_budget20"';
SELECT * FROM `v5_DP_e10!2_l0_h99!5`;
SELECT * FROM users_DP_budget20_BudgetTracker;
CREATE VIEW `v6_DP_e4_l0_h99!5` AS '"SELECT SUM(age) FROM users_DP_budget20"';
