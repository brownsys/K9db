CREATE TABLE main(PII_usr VARCHAR(100) PRIMARY KEY);
CREATE TABLE usertable(YCSB_KEY VARCHAR(100) PRIMARY KEY, `DEC` VARCHAR(100), USR VARCHAR(100), SRC VARCHAR(100), OBJ VARCHAR(100), CAT VARCHAR(100), ACL VARCHAR(100), Data VARCHAR(100), PUR VARCHAR(100), SHR VARCHAR(100), TTL VARCHAR(100), FOREIGN KEY(USR) REFERENCES main(ID));
CREATE INDEX pk_index ON usertable(YCSB_KEY);