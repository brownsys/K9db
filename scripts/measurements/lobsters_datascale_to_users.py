import sys

USERS_FOR_DS_1 = 5800

if __name__ == "__main__":
  datascales = [float(arg) for arg in sys.argv[1:]]
  users = [ds * USERS_FOR_DS_1 for ds in datascales]
  print('Datascales: ', datascales)
  print('Users: ', users)
