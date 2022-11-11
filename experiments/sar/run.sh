#!/bin/bash
IP="$1"
COUNT="$2"

python3 gen_get_forget.py $COUNT get uniform > get.sql
python3 gen_get_forget.py $COUNT forget uniform > forget.sql
python3 gen_get_forget.py $COUNT get random > getrand.sql
python3 gen_get_forget.py $COUNT forget random > forgetrand.sql

echo "GET UNIFORM"
time mariadb -P10001 --host=$IP < get.sql
echo "END GET UNIFORM"

echo "FORGET UNIFORM"
time mariadb -P10001 --host=$IP < forget.sql
echo "END FORGET UNIFORM"

echo "GET RAND"
time mariadb -P10001 --host=$IP < getrand.sql
echo "END GET RAND"

echo "FORGET RAND"
time mariadb -P10001 --host=$IP < forgetrand.sql
echo "END FORGET RAND"
