from random import randint, choice

STUDENTS = 200
ASSIGNMENTS = 100
SUBMISSIONS_PRIME = 250

OPERATIONS = 2500
WEIGHTS = ['INSERT' * 50] + ['DELETE'] * 25 + ['SELECT'] * 25

# Student and assignment inserts.
def make_student(id):
  return 'INSERT INTO students VALUES ({}, \'student{}\');\n'.format(id, id)
def make_assignment(id): 
  return 'INSERT INTO assignments VALUES ({}, \'assignment{}\');\n'.format(id, id)

# Submission operations.
def make_insert(id):
  sid = randint(0, STUDENTS - 1)
  aid = randint(0, ASSIGNMENTS - 1)
  ts = randint(0, 100)
  return 'INSERT INTO submissions VALUES({}, {}, {}, {});\n'.format(id, sid, aid, ts)
def make_delete(id):
  return 'DELETE FROM submissions WHERE ID = {};\n'.format(id)
def make_select(id):
  return 'SELECT * FROM submissions WHERE ID = {};\n'.format(id)

if __name__ == '__main__':
  b = [open("parallel_insert_base1.sql", "w"), open("parallel_insert_base2.sql", "w")]
  f = [open("parallel_insert_followup1.sql", "w"), open("parallel_insert_followup2.sql", "w")]

  # Create students and assignments.
  for i in range(STUDENTS):
    b[i%2].write(make_student(i))
  for i in range(ASSIGNMENTS):
    b[i%2].write(make_assignment(i))

  # Create submissions operations.
  for i in range(SUBMISSIONS_PRIME):
    f[i%2].write(make_insert(i))

  count = SUBMISSIONS_PRIME
  for i in range(OPERATIONS):
    op = choice(WEIGHTS)
    if op == 'INSERT':
      f[i%2].write(make_insert(count))
      count = count + 1
    elif op == 'DELETE':
      f[i%2].write(make_delete(randint(0, count - 1)))
    elif op == 'SELECT':
      f[i%2].write(make_select(randint(0, count - 1)))

  b[0].close()
  b[1].close()
  f[0].close()
  f[1].close()
