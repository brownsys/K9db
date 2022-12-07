import sys

USERS_FOR_DS_1 = 5800

if __name__ == "__main__":
  users = [int(arg) for arg in sys.argv[1:]]
  datascales = [u / USERS_FOR_DS_1 for u in users]
  print('Users: ', users)
  print('Datascales: ', datascales)
