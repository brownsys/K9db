#!/bin/bash
mysql -u $1 -p$2 --execute="DROP DATABASE default_db;"

# example.cc and examples/simple-websubmit.sql
mysql -u $1 -p$2 --execute="DROP DATABASE students_c4ca4238a0b923820dcc509a6f75849b;"
mysql -u $1 -p$2 --execute="DROP DATABASE students_c81e728d9d4c2f636f067f89cc14862c;"
mysql -u $1 -p$2 --execute="DROP DATABASE students_eccbc87e4b5ce2fe28308fd9f2a7baf3;"

# examples/medical_chat.sql
mysql -u $1 -p$2 --execute="DROP DATABASE doctors_c4ca4238a0b923820dcc509a6f75849b;"
mysql -u $1 -p$2 --execute="DROP DATABASE patients_98f13708210194c475687be6106a3b84;"

# examples/{social_chat.sql,social_chat.cntd.sql}
mysql -u $1 -p$2 --execute="DROP DATABASE Users_c81e728d9d4c2f636f067f89cc14862c"
mysql -u $1 -p$2 --execute="DROP DATABASE Users_c4ca4238a0b923820dcc509a6f75849b"
mysql -u $1 -p$2 --execute="DROP DATABASE Users_eccbc87e4b5ce2fe28308fd9f2a7baf3"
mysql -u $1 -p$2 --execute="DROP DATABASE users_c81e728d9d4c2f636f067f89cc14862c"
mysql -u $1 -p$2 --execute="DROP DATABASE users_c4ca4238a0b923820dcc509a6f75849b"

# Lobsters
mysql -u $1 -p$2 --execute="DROP DATABASE users_c81e728d9d4c2f636f067f89cc14862c"
mysql -u $1 -p$2 --execute="DROP DATABASE users_c4ca4238a0b923820dcc509a6f75849b"
exit 0
