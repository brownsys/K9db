package edu.brown.pelton;
import static edu.brown.pelton.MemcachedJNI.MemcachedValue;

import java.util.HashSet;
import junit.framework.*;
import org.junit.Test;
import static org.junit.Assert.assertTrue;

public class MemcachedJNITest {
  // Schema.
  public static String[] SCHEMA = {
    "CREATE TABLE students (id INT PRIMARY KEY, name VARCHAR(50), age INT)",
    "CREATE TABLE grades (sid INT, grade INT)"
  };
  // Inserts.
  public static String[] INSERTS = {
    "INSERT INTO students VALUES(1, 'John', 25)",
    "INSERT INTO students VALUES(2, 'Smith', 35)",
    "INSERT INTO students VALUES(3, 'Kinan', 27)",
    "INSERT INTO students VALUES(4, 'Zidane', -45)",
    "INSERT INTO grades VALUES(1, 50)",
    "INSERT INTO grades VALUES(2, 60)",
    "INSERT INTO grades VALUES(2, 70)",
    "INSERT INTO grades VALUES(3, 100)",
    "INSERT INTO grades VALUES(3, 90)",
    "INSERT INTO grades VALUES(3, 96)",
    "INSERT INTO grades VALUES(4, 1000)",
    "INSERT INTO grades VALUES(4, 2000)",
    "INSERT INTO grades VALUES(4, 5000)"
  };
  // Updates.
  public static String[] UPDATES = {
    "INSERT INTO students VALUES (5, 'New', 50)",
    "UPDATE students SET age = 28 WHERE name = 'kinan'",
    "DELETE FROM grades WHERE sid = 2",
    "DELETE FROM students WHERE id = 2"
  };
  // Queries.
  public static String[] QUERIES = {
    "SELECT * FROM students;",
    "SELECT * FROM students WHERE id = ?",
    "SELECT * FROM students WHERE name = ?",
    "SELECT name, age, SUM(grades.grade) FROM students "
        + "JOIN grades ON students.id = grades.sid "
        + "GROUP BY name, age HAVING name = ? AND age = ?"
  };
  // Empty set.
  public static String[] EMPTY = new String[0];

  // Helpers.
  public static String toString(MemcachedValue[] row) {
    String str = "|";
    for (MemcachedValue v : row) {
      if (v.getType() == 1) {
        str += v.getStr();
      } else if (v.getType() == 2) {
        str += v.getInt();
      } else if (v.getType() == 3) {
        str += v.getUInt();
      }
      str += "|";
    }
    return str;
  }

  // Equality checks.
  public static boolean equals(MemcachedValue[][] result, String[] expected) {
    if (expected.length != result.length) {
      return false;
    }

    HashSet<String> set = new HashSet<String>();
    for (MemcachedValue[] row : result) {
      set.add(toString(row));
    }
    for (String e : expected) {
      if (!set.contains(e)) {
        return false;
      }
    }
    return true;
  }

  // The test.
  @Test
  public void completeTest() {
    MemcachedValue[][] actual;
    String[] expected;

    // Initialize.
    MemcachedJNI.Initialize("memcached", "root", "password", "jni");

    // Create tables and insert data.
    for (String create : SCHEMA) {
      MemcachedJNI.__ExecuteDB(create);
    }
    for (String insert : INSERTS) {
      MemcachedJNI.__ExecuteDB(insert);
    }

    // Cache the queries.
    for (String query : QUERIES) {
      MemcachedJNI.Cache(query);
    }

    // View 0.
    actual = MemcachedJNI.Read(0, new MemcachedValue[0]);    
    expected = new String[] {"|1|John|25|", "|2|Smith|35|", "|3|Kinan|27|", "|4|Zidane|-45|"};
    assertTrue(equals(actual, expected));

    // View 1.
    actual = MemcachedJNI.Read(1, new MemcachedValue[] {MemcachedValue.fromInt(1)});
    expected = new String[] {"|1|John|25|"};
    assertTrue(equals(actual, expected));

    actual = MemcachedJNI.Read(1, new MemcachedValue[] {MemcachedValue.fromInt(2)});
    expected = new String[] {"|2|Smith|35|"};
    assertTrue(equals(actual, expected));

    actual = MemcachedJNI.Read(1, new MemcachedValue[] {MemcachedValue.fromInt(3)});
    expected = new String[] {"|3|Kinan|27|"};
    assertTrue(equals(actual, expected));

    actual = MemcachedJNI.Read(1, new MemcachedValue[] {MemcachedValue.fromInt(4)});
    expected = new String[] {"|4|Zidane|-45|"};
    assertTrue(equals(actual, expected));

    actual = MemcachedJNI.Read(1, new MemcachedValue[] {MemcachedValue.fromInt(10)});
    assertTrue(equals(actual, EMPTY));

    // View 2.
    actual = MemcachedJNI.Read(2, new MemcachedValue[] {MemcachedValue.fromStr("John")});
    expected = new String[] {"|1|John|25|"};
    assertTrue(equals(actual, expected));

    actual = MemcachedJNI.Read(2, new MemcachedValue[] {MemcachedValue.fromStr("Smith")});
    expected = new String[] {"|2|Smith|35|"};
    assertTrue(equals(actual, expected));

    actual = MemcachedJNI.Read(2, new MemcachedValue[] {MemcachedValue.fromStr("Kinan")});
    expected = new String[] {"|3|Kinan|27|"};
    assertTrue(equals(actual, expected));

    actual = MemcachedJNI.Read(2, new MemcachedValue[] {MemcachedValue.fromStr("Zidane")});
    expected = new String[] {"|4|Zidane|-45|"};
    assertTrue(equals(actual, expected));

    // View 3.
    actual = MemcachedJNI.Read(3, new MemcachedValue[] {MemcachedValue.fromStr("John"), MemcachedValue.fromInt(25)});
    expected = new String[] {"|John|25|50|"};
    assertTrue(equals(actual, expected));

    actual = MemcachedJNI.Read(3, new MemcachedValue[] {MemcachedValue.fromStr("Smith"), MemcachedValue.fromInt(35)});
    expected = new String[] {"|Smith|35|130|"};
    assertTrue(equals(actual, expected));

    actual = MemcachedJNI.Read(3, new MemcachedValue[] {MemcachedValue.fromStr("Kinan"), MemcachedValue.fromInt(27)});
    expected = new String[] {"|Kinan|27|286|"};
    assertTrue(equals(actual, expected));

    actual = MemcachedJNI.Read(3, new MemcachedValue[] {MemcachedValue.fromStr("Zidane"), MemcachedValue.fromInt(-45)});
    expected = new String[] {"|Zidane|-45|8000|"};
    assertTrue(equals(actual, expected));

    // Updates.
    for (String update : UPDATES) {
      MemcachedJNI.__ExecuteDB(update);
    }
    MemcachedJNI.Update("students");

    // View 0.
    actual = MemcachedJNI.Read(0, new MemcachedValue[0]);    
    expected = new String[] {"|1|John|25|", "|3|Kinan|28|", "|4|Zidane|-45|", "|5|New|50|"};
    assertTrue(equals(actual, expected));

    // View 1.
    actual = MemcachedJNI.Read(1, new MemcachedValue[] {MemcachedValue.fromInt(1)});
    expected = new String[] {"|1|John|25|"};
    assertTrue(equals(actual, expected));

    actual = MemcachedJNI.Read(1, new MemcachedValue[] {MemcachedValue.fromInt(3)});
    expected = new String[] {"|3|Kinan|28|"};
    assertTrue(equals(actual, expected));

    actual = MemcachedJNI.Read(1, new MemcachedValue[] {MemcachedValue.fromInt(4)});
    expected = new String[] {"|4|Zidane|-45|"};
    assertTrue(equals(actual, expected));

    actual = MemcachedJNI.Read(1, new MemcachedValue[] {MemcachedValue.fromInt(5)});
    expected = new String[] {"|5|New|50|"};
    assertTrue(equals(actual, expected));

    // View 2.
    actual = MemcachedJNI.Read(2, new MemcachedValue[] {MemcachedValue.fromStr("John")});
    expected = new String[] {"|1|John|25|"};
    assertTrue(equals(actual, expected));

    actual = MemcachedJNI.Read(2, new MemcachedValue[] {MemcachedValue.fromStr("Kinan")});
    expected = new String[] {"|3|Kinan|28|"};
    assertTrue(equals(actual, expected));

    actual = MemcachedJNI.Read(2, new MemcachedValue[] {MemcachedValue.fromStr("Zidane")});
    expected = new String[] {"|4|Zidane|-45|"};
    assertTrue(equals(actual, expected));

    actual = MemcachedJNI.Read(2, new MemcachedValue[] {MemcachedValue.fromStr("New")});
    expected = new String[] {"|5|New|50|"};
    assertTrue(equals(actual, expected));

    // View 3.
    actual = MemcachedJNI.Read(3, new MemcachedValue[] {MemcachedValue.fromStr("John"), MemcachedValue.fromInt(25)});
    expected = new String[] {"|John|25|50|"};
    assertTrue(equals(actual, expected));

    actual = MemcachedJNI.Read(3, new MemcachedValue[] {MemcachedValue.fromStr("Kinan"), MemcachedValue.fromInt(28)});
    expected = new String[] {"|Kinan|28|286|"};
    assertTrue(equals(actual, expected));

    actual = MemcachedJNI.Read(3, new MemcachedValue[] {MemcachedValue.fromStr("Zidane"), MemcachedValue.fromInt(-45)});
    expected = new String[] {"|Zidane|-45|8000|"};
    assertTrue(equals(actual, expected));

    MemcachedJNI.__ExecuteDB("DROP DATABASE memcached");

    // Free.
    MemcachedJNI.Close();
  }
}
