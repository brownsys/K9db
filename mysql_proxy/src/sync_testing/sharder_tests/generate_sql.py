#!/usr/bin/env python3
def read(table, **kwargs):
    """ Generates SQL for a SELECT statement matching the kwargs passed. """
    sql = list()
    sql.append("SELECT * FROM %s " % table)
    if kwargs:
        sql.append("WHERE " + " AND ".join("%s = '%s'" % (k, v) for k, v in kwargs.items()))
    sql.append(";")
    return "".join(sql)

def create(table, foreign_keys, **kwargs):
    """ Generates SQL for a CREATE statement matching the kwargs passed. """
    keys = ["%s" % k for k in kwargs]
    values = ["'%s'" % v for v in kwargs.values()]
    sql = list()
    sql.append("CREATE TABLE %s (" % table)
    sql.append(", ".join("%s %s" % (k, v) for k, v in kwargs.items()))
    if (foreign_keys):
        sql.append(", " + foreign_keys)
    sql.append(");\n")
    return "".join(sql)

def upsert(table, **kwargs):
    """ update/insert rows into objects table (update if the row already exists)
        given the key-value pairs in kwargs """
    keys = ["%s" % k for k in kwargs]
    # if key (field name) is ID, then make it int without surrounding quotes ''
    values = ["%s" % v if "ID" or "student_id" or "assignment_id" in k else "'%s'" % v for k, v in kwargs.items()]
    sql = list()
    sql.append("INSERT INTO %s" % table)
    sql.append(" VALUES (")
    sql.append(", ".join(values))
    # sql.append(") ON DUPLICATE KEY UPDATE ")
    # sql.append(", ".join("%s = '%s'" % (k, v) for k, v in kwargs.items()))
    sql.append(");\n")
    return "".join(sql)

def delete(table, **kwargs):
    """ deletes rows from table where **kwargs match """
    sql = list()
    sql.append("DELETE FROM %s " % table)
    sql.append("WHERE " + " AND ".join("%s = '%s'" % (k, v) for k, v in kwargs.items()))
    sql.append(";\n")
    return "".join(sql)

with open("creates_unique1.sql", "w") as f:
    for i in range(20):
        f.write(create("students" + str(i), "", ID="int", PII_Name="text", PRIMARY="KEY(ID)"))
    for i in range(20):
        f.write(create("assignments" + str(i), "", ID="int", Name="text", 
                       PRIMARY="KEY(ID)"))
    for i in range(20):
        f.write(create("submissions" + str(i), 
                       "FOREIGN KEY (student_id) REFERENCES students(ID), FOREIGN KEY (assignment_id) REFERENCES assignments(ID)",
                       student_id="int", assignment_id="int", 
                       timestamp="int"
                       ))

with open("creates_unique2.sql", "w") as f:
    for i in range(20, 40):
        f.write(create("students" + str(i), "", ID="int", PII_Name="text", PRIMARY="KEY(ID)"))
    for i in range(20, 40):
        f.write(create("assignments" + str(i), "", ID="int", Name="text", 
                       PRIMARY="KEY(ID)"))
    for i in range(20, 40):
        f.write(create("submissions" + str(i), 
                       "FOREIGN KEY (student_id) REFERENCES students(ID), FOREIGN KEY (assignment_id) REFERENCES assignments(ID)",
                       student_id="int", assignment_id="int", 
                       timestamp="int"
                       ))

# test correctness by having one duplicate create table statement for each of the three tables
with open("creates_duplicates1.sql", "w") as f:
    for i in range(20):
        f.write(create("students" + str(i), "", ID="int", PII_Name="text", PRIMARY="KEY(ID)"))
    for i in range(20):
        f.write(create("assignments" + str(i), "", student_id="int", assignment_id="int", 
                       timestamp="int", PRIMARY="KEY(ID)"))
    for i in range(20):
        f.write(create("submissions" + str(i), 
                       "FOREIGN KEY (student_id) REFERENCES students(ID), FOREIGN KEY (assignment_id) REFERENCES assignments(ID)",
                       student_id="int", assignment_id="int", 
                       timestamp="int"
                       ))
# test correctness by having one duplicate create table statement for each of the three tables
with open("creates_duplicates2.sql", "w") as f:
    for i in range(19, 40):
        f.write(create("students" + str(i), "", ID="int", PII_Name="text", PRIMARY="KEY(ID)"))
    for i in range(19, 40):
        f.write(create("assignments" + str(i), "", student_id="int", assignment_id="int", 
                       timestamp="int", PRIMARY="KEY(ID)"))
    for i in range(19, 40):
        f.write(create("submissions" + str(i), 
                       "FOREIGN KEY (student_id) REFERENCES students(ID), FOREIGN KEY (assignment_id) REFERENCES assignments(ID)",
                       student_id="int", assignment_id="int", 
                       timestamp="int"
                       ))

with open("inserts_unique1.sql", "w") as f:
    # INSERT INTO students VALUES (1, 'Jerry');
    for i in range(50):
        for j in range(20):
            f.write(upsert("students" + str(j), ID=i+j, PII_Name=i+j))
    # INSERT INTO assignments VALUES (1, 'assignment 1');
    for i in range(50):
        for j in range(20):
            f.write(upsert("assignments" + str(j), ID=i+j, Name=i+j))
    # INSERT INTO submissions VALUES (1, 1, 1);
    for i in range(50):
        for j in range(20):
            f.write(upsert("submissions" + str(j), student_id=i+j, assignment_id=i+j, timestamp=i+j))

with open("inserts_unique2.sql", "w") as f:
    # INSERT INTO students VALUES (1, 'Jerry');
    for i in range(50, 100):
        for j in range(20):
            f.write(upsert("students" + str(j), ID=i+j, PII_Name=i+j))
    # INSERT INTO assignments VALUES (1, 'assignment 1');
    for i in range(50, 100):
        for j in range(20):
            f.write(upsert("assignments" + str(j), ID=i+j, Name=i+j))
    # INSERT INTO submissions VALUES (1, 1, 1);
    for i in range(50, 100):
        for j in range(20):
            f.write(upsert("submissions" + str(j), student_id=i+j, assignment_id=i+j, timestamp=i+j))

# f = open("inserts_duplicates.sql", "w")
# ! TODO: generate sql file for duplicate inserts