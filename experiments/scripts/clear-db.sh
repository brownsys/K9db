#!/bin/bash
mysql -u root -ppassword --execute="DROP DATABASE default_db;"
mysql -u root -ppassword --execute="DROP DATABASE gdprbench;"

# example.cc and examples/simple-websubmit.sql
mysql -u root -ppassword --execute="DROP DATABASE main_24771ec5eb3c58ff9c7877c201065c56;"
mysql -u root -ppassword --execute="DROP DATABASE main_2541acf6383da54e4044f9d3ad23f332;"
mysql -u root -ppassword --execute="DROP DATABASE main_58e42b48502f2ab9793be0e226dcf67d;"
mysql -u root -ppassword --execute="DROP DATABASE main_f8d4ae448b914fe9473e92f5d934951e;"

sleep 3

exit 0
