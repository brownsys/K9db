#!/bin/bash
mysql -u root -ppassword --execute="DROP DATABASE default_db;"
mysql -u root -ppassword --execute="DROP DATABASE gdprbench;"

# example.cc and examples/simple-websubmit.sql
mysql -u root -ppassword --execute="DROP DATABASE main_24771ec5eb3c58ff9c7877c201065c56;"
mysql -u root -ppassword --execute="DROP DATABASE main_2541acf6383da54e4044f9d3ad23f332;"
mysql -u root -ppassword --execute="DROP DATABASE main_58e42b48502f2ab9793be0e226dcf67d;"
mysql -u root -ppassword --execute="DROP DATABASE main_f8d4ae448b914fe9473e92f5d934951e;"

mysql -u root -ppassword --execute="DROP DATABASE main_1793660328a0ee16947af91bdd55e326;"
mysql -u root -ppassword --execute="DROP DATABASE main_bb02badeb43855f5478559612080fc77;"
mysql -u root -ppassword --execute="DROP DATABASE main_cdd011eeb3e087fc1471b567d538438c;"
mysql -u root -ppassword --execute="DROP DATABASE main_d9d8e79f38096c8d3ae4c1d3ce249c3a;"

sleep 3

exit 0
