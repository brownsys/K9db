#!/bin/bash
IP="$1"
echo "GET UNIFORM"
time mariadb -P10001 --hostname=$IP < get.sql
echo "END GET UNIFORM"

echo "GET RAND"
time mariadb -P10001 --hostname=$IP < getrand.sql
echo "END GET RAND"

echo "FORGET UNIFORM"
time mariadb -P10001 --hostname=$IP < forget.sql
echo "END FORGET UNIFORM"

echo "FORGET RAND"
time mariadb -P10001 --hostname=$IP < forgetrand.sql
echo "END FORGET RAND"
