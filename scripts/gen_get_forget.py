import sys
import random

total_number = 5000
user_count = 100
users = set()
for i in range(user_count):
  user_id = None
  if sys.argv[2] == 'random':
    user_id = random.randint(user_count + 1, total_number)
    while user_id in users:
      user_id = random.randint(user_count + 1, total_number)    
  else:
    user_id = i
    
  if sys.argv[1] == 'get':
    print('GDPR GET users {}'.format(user_id))  
  else:
    print('GDPR FORGET users {}'.format(user_id))
