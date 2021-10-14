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

with open("./correctness/creates_simple1.sql", "w") as f:
        f.write(create("students" + str(0), "", ID="int", PII_Name="text", PRIMARY="KEY(ID)"))
        f.write(create("assignments" + str(0), "", ID="int", Name="text", 
                       PRIMARY="KEY(ID)"))
        f.write(create("submissions" + str(0), 
                       "FOREIGN KEY (student_id) REFERENCES students(ID), FOREIGN KEY (assignment_id) REFERENCES assignments(ID)",
                       student_id="int", assignment_id="int", 
                       timestamp="int"
                       ))

with open("./correctness/creates_simple2.sql", "w") as f:
        f.write(create("students" + str(1), "", ID="int", PII_Name="text", PRIMARY="KEY(ID)"))
        f.write(create("assignments" + str(1), "", ID="int", Name="text", 
                       PRIMARY="KEY(ID)"))
        f.write(create("submissions" + str(1), 
                       "FOREIGN KEY (student_id) REFERENCES students(ID), FOREIGN KEY (assignment_id) REFERENCES assignments(ID)",
                       student_id="int", assignment_id="int", 
                       timestamp="int"
                       ))

with open("./correctness/creates_unique1.sql", "w") as f:
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

with open("./correctness/creates_unique2.sql", "w") as f:
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
with open("./correctness/creates_duplicates1.sql", "w") as f:
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
with open("./correctness/creates_duplicates2.sql", "w") as f:
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

with open("./correctness/inserts_simple1.sql", "w") as f:
    # INSERT INTO students VALUES (1, 'Jerry');
    f.write(upsert("students" + str(0), ID=0, PII_Name=0))
    # INSERT INTO assignments VALUES (1, 'assignment 1');
    f.write(upsert("assignments" + str(0), ID=0, Name=0))
    # INSERT INTO submissions VALUES (1, 1, 1);
    f.write(upsert("submissions" + str(0), student_id=0, assignment_id=0, timestamp=0))

with open("./correctness/inserts_simple2.sql", "w") as f:
    # INSERT INTO students VALUES (1, 'Jerry');
    f.write(upsert("students" + str(1), ID=0, PII_Name=0))
    # INSERT INTO assignments VALUES (1, 'assignment 1');
    f.write(upsert("assignments" + str(1), ID=0, Name=0))
    # INSERT INTO submissions VALUES (1, 1, 1);
    f.write(upsert("submissions" + str(1), student_id=0, assignment_id=0, timestamp=0))
            
with open("./correctness/inserts_unique1.sql", "w") as f:
    for i in range(50):
        for j in range(20):
            f.write(upsert("students" + str(j), ID=i+j, PII_Name=i+j))
    for i in range(50):
        for j in range(20):
            f.write(upsert("assignments" + str(j), ID=i+j, Name=i+j))
    for i in range(50):
        for j in range(20):
            f.write(upsert("submissions" + str(j), student_id=i+j, assignment_id=i+j, timestamp=i+j))

with open("./correctness/inserts_unique2.sql", "w") as f:
    for i in range(50, 100):
        for j in range(20):
            f.write(upsert("students" + str(j), ID=i+j, PII_Name=i+j))
    for i in range(50, 100):
        for j in range(20):
            f.write(upsert("assignments" + str(j), ID=i+j, Name=i+j))
    for i in range(50, 100):
        for j in range(20):
            f.write(upsert("submissions" + str(j), student_id=i+j, assignment_id=i+j, timestamp=i+j))

with open("./correctness/inserts_duplicates1.sql", "w") as f:
    for i in range(50):
        for j in range(20):
            f.write(upsert("students" + str(j), ID=i+j, PII_Name=i+j))
    for i in range(50):
        for j in range(20):
            f.write(upsert("assignments" + str(j), ID=i+j, Name=i+j))
    for i in range(50):
        for j in range(20):
            f.write(upsert("submissions" + str(j), student_id=i+j, assignment_id=i+j, timestamp=i+j))

with open("./correctness/inserts_duplicates2.sql", "w") as f:
    for i in range(49, 100):
        for j in range(20):
            f.write(upsert("students" + str(j), ID=i+j, PII_Name=i+j))
    for i in range(49, 100):
        for j in range(20):
            f.write(upsert("assignments" + str(j), ID=i+j, Name=i+j))
    for i in range(49, 100):
        for j in range(20):
            f.write(upsert("submissions" + str(j), student_id=i+j, assignment_id=i+j, timestamp=i+j))

# ----------- BENCHMARK ------------
with open("./benchmark/creates.sql", "w") as f:
    for i in range(1):
        f.write(create("students" + str(i), "", ID="int", PII_Name="text", PRIMARY="KEY(ID)"))
    for i in range(1):
        f.write(create("assignments" + str(i), "", ID="int", Name="text", 
                       PRIMARY="KEY(ID)"))
    for i in range(1):
        f.write(create("submissions" + str(i), 
                       "FOREIGN KEY (student_id) REFERENCES students(ID), FOREIGN KEY (assignment_id) REFERENCES assignments(ID)",
                       student_id="int", assignment_id="int", 
                       timestamp="int"
                       ))

with open("./benchmark/inserts_new_shard.sql", "w") as f:
    for i in range(400):
        for j in range(1):
            f.write(upsert("students" + str(j), ID=i+j, PII_Name=i+j))
    # for i in range(1000000):
    #     for j in range(0):
    #         f.write(upsert("assignments" + str(j), ID=i+j, Name=i+j))
    # for i in range(1000000):
    #     for j in range(0):
    #         f.write(upsert("submissions" + str(j), student_id=i+j, assignment_id=i+j, timestamp=i+j))

with open("./benchmark/inserts_existing_shard_all.sql", "w") as f:
    # for i in range(100000):
    #     for j in range(0):
    #         f.write(upsert("students" + str(j), ID=i+j, PII_Name=i+j))
    for i in range(400):
        for j in range(1):
            f.write(upsert("assignments" + str(j), ID=i+j, Name=i+j))
    for i in range(400):
        for j in range(1):
            f.write(upsert("submissions" + str(j), student_id=i+j, assignment_id=i+j, timestamp=i+j))

with open("./benchmark/inserts_existing_shard1.sql", "w") as f:
    # for i in range(100000):
    #     for j in range(0):
    #         f.write(upsert("students" + str(j), ID=i+j, PII_Name=i+j))
    for i in range(0, 100):
        for j in range(1):
            f.write(upsert("assignments" + str(j), ID=i+j, Name=i+j))
    for i in range(0, 100):
        for j in range(1):
            f.write(upsert("submissions" + str(j), student_id=i+j, assignment_id=i+j, timestamp=i+j))

with open("./benchmark/inserts_existing_shard2.sql", "w") as f:
    # for i in range(100000):
    #     for j in range(0):
    #         f.write(upsert("students" + str(j), ID=i+j, PII_Name=i+j))
    for i in range(100, 200):
        for j in range(1):
            f.write(upsert("assignments" + str(j), ID=i+j, Name=i+j))
    for i in range(100, 200):
        for j in range(1):
            f.write(upsert("submissions" + str(j), student_id=i+j, assignment_id=i+j, timestamp=i+j))

with open("./benchmark/inserts_existing_shard3.sql", "w") as f:
    # for i in range(100000):
    #     for j in range(0):
    #         f.write(upsert("students" + str(j), ID=i+j, PII_Name=i+j))
    for i in range(200, 300):
        for j in range(1):
            f.write(upsert("assignments" + str(j), ID=i+j, Name=i+j))
    for i in range(200, 300):
        for j in range(1):
            f.write(upsert("submissions" + str(j), student_id=i+j, assignment_id=i+j, timestamp=i+j))

with open("./benchmark/inserts_existing_shard4.sql", "w") as f:
    # for i in range(100000):
    #     for j in range(0):
    #         f.write(upsert("students" + str(j), ID=i+j, PII_Name=i+j))
    for i in range(300, 400):
        for j in range(1):
            f.write(upsert("assignments" + str(j), ID=i+j, Name=i+j))
    for i in range(300, 400):
        for j in range(1):
            f.write(upsert("submissions" + str(j), student_id=i+j, assignment_id=i+j, timestamp=i+j))