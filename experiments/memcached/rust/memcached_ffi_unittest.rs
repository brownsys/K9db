extern crate memcached_ffi;

use memcached_ffi::memcached::*;

// Schema.
static SCHEMA: [&str; 2] = [
    "CREATE TABLE students (id INT PRIMARY KEY, name VARCHAR(50), age INT)",
    "CREATE TABLE grades (sid INT, grade INT)"];
static INSERTS: [&str; 13] = [
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
    "INSERT INTO grades VALUES(4, 5000)"];
static UPDATES: [&str; 4] = [
    "INSERT INTO students VALUES (5, 'New', 50)",
    "UPDATE students SET age = 28 WHERE name = 'kinan'",
    "DELETE FROM grades WHERE sid = 2",
    "DELETE FROM students WHERE id = 2"];

// Queries to cache.
static QUERIES: [&str; 4] = [
    "SELECT * FROM students;",
    "SELECT * FROM students WHERE id = ?",
    "SELECT * FROM students WHERE name = ?",
    "SELECT name, age, SUM(grades.grade) FROM students JOIN grades ON students.id = grades.sid GROUP BY name, age HAVING name = ? AND age = ?"];

fn eqs(actual: Vec<String>, expected: Vec<&str>) -> bool {
  if actual.len() != expected.len() {
    return false;
  }
  for e in expected {
    let mut found: bool = false;
    for a in &actual {
      if a == e {
        found = true;
        break;
      }
    }
    if !found {
      return false;
    }
  }
  return true;
}

fn eq(actual: Vec<String>, expected: &str) -> bool {
  if actual.len() != 1 {
    return false;
  }
  return actual[0] == expected;
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test1() {
    // Initialize.
    MemInitialize("memcachedrust", "pelton", "password", "rust");
    for schema in &SCHEMA {
      __MemExecuteDB(schema);
    }
    for insert in &INSERTS {
      __MemExecuteDB(insert);
    }
    for query in &QUERIES {
      MemCache(query);
    }

    // View 0.
    let key = MemCreateKey(vec![]);
    let expected = vec!["|1|John|25|", "|2|Smith|35|", "|3|Kinan|27|", "|4|Zidane|-45|"];
    assert!(eqs(MemRead(0, key), expected));

    // View 1.
    assert!(eq(MemRead(1, MemCreateKey(vec![MemSetInt(1)])), "|1|John|25|"));
    assert!(eq(MemRead(1, MemCreateKey(vec![MemSetInt(2)])), "|2|Smith|35|"));
    assert!(eq(MemRead(1, MemCreateKey(vec![MemSetInt(3)])), "|3|Kinan|27|"));
    assert!(eq(MemRead(1, MemCreateKey(vec![MemSetInt(4)])), "|4|Zidane|-45|"));
    assert!(eqs(MemRead(1, MemCreateKey(vec![MemSetInt(10)])), vec![]));

    // View 2.
    assert!(eq(MemRead(2, MemCreateKey(vec![MemSetStr("John")])), "|1|John|25|"));
    assert!(eq(MemRead(2, MemCreateKey(vec![MemSetStr("Smith")])), "|2|Smith|35|"));
    assert!(eq(MemRead(2, MemCreateKey(vec![MemSetStr("Kinan")])), "|3|Kinan|27|"));
    assert!(eq(MemRead(2, MemCreateKey(vec![MemSetStr("Zidane")])), "|4|Zidane|-45|"));

    // View 3.
    assert!(eq(MemRead(3, MemCreateKey(vec![MemSetStr("John"), MemSetInt(25)])), "|John|25|50|"));
    assert!(eq(MemRead(3, MemCreateKey(vec![MemSetStr("Smith"), MemSetInt(35)])), "|Smith|35|130|"));
    assert!(eq(MemRead(3, MemCreateKey(vec![MemSetStr("Kinan"), MemSetInt(27)])), "|Kinan|27|286|"));
    assert!(eq(MemRead(3, MemCreateKey(vec![MemSetStr("Zidane"), MemSetInt(-45)])), "|Zidane|-45|8000|"));

    // Run updates.
    for update in &UPDATES {
      __MemExecuteDB(update);
    }
    MemUpdate("students");

    // View 0.
    let key = MemCreateKey(vec![]);
    let expected = vec!["|1|John|25|", "|3|Kinan|28|", "|4|Zidane|-45|", "|5|New|50|"];
    assert!(eqs(MemRead(0, key), expected));

    // View 1.
    assert!(eq(MemRead(1, MemCreateKey(vec![MemSetInt(1)])), "|1|John|25|"));
    assert!(eq(MemRead(1, MemCreateKey(vec![MemSetInt(3)])), "|3|Kinan|28|"));
    assert!(eq(MemRead(1, MemCreateKey(vec![MemSetInt(4)])), "|4|Zidane|-45|"));
    assert!(eq(MemRead(1, MemCreateKey(vec![MemSetInt(5)])), "|5|New|50|"));

    // View 2.
    assert!(eq(MemRead(2, MemCreateKey(vec![MemSetStr("John")])), "|1|John|25|"));
    assert!(eq(MemRead(2, MemCreateKey(vec![MemSetStr("Kinan")])), "|3|Kinan|28|"));
    assert!(eq(MemRead(2, MemCreateKey(vec![MemSetStr("Zidane")])), "|4|Zidane|-45|"));
    assert!(eq(MemRead(2, MemCreateKey(vec![MemSetStr("New")])), "|5|New|50|"));

    // View 3.
    assert!(eq(MemRead(3, MemCreateKey(vec![MemSetStr("John"), MemSetInt(25)])), "|John|25|50|"));
    assert!(eq(MemRead(3, MemCreateKey(vec![MemSetStr("Kinan"), MemSetInt(28)])), "|Kinan|28|286|"));
    assert!(eq(MemRead(3, MemCreateKey(vec![MemSetStr("Zidane"), MemSetInt(-45)])), "|Zidane|-45|8000|"));

    // Done.
    __MemExecuteDB("DROP DATABASE memcachedrust");

    // Free memory.
    MemClose();
  }
}
