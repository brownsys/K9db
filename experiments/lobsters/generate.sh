  #!/bin/bash
DBUSERNAME=$1
DBPASSWORD=$2
if [[ $DBUSERNAME == "" ]]; then
  DBUSERNAME="root"
fi
if [[ $DBPASSWORD == "" ]]; then
  DBPASSWORD="password"
fi

# Compile noria lobsters harness (once).
cd noria
if [ ! -f target/release/lobsters-mysql ]; then
  cargo b --release --bin lobsters-mysql
fi

# Clear the database.
echo "Clearing DB"
mariadb -A -u $DBUSERNAME -p$DBPASSWORD --execute="DROP DATABASE lobsters;"
mariadb -A -u $DBUSERNAME -p$DBPASSWORD --execute="DROP DATABASE gdprbench;"
mariadb -A -u $DBUSERNAME -p$DBPASSWORD --execute="DROP DATABASE peltonlobsters;"

# Prime the database.
echo "Priming..."
target/release/lobsters-mysql --runtime 0  --prime  --queries original \
    "mysql://$DBUSERNAME:$DBPASSWORD@localhost/lobsters" --scale 1 > populate.sql

# Remove comments at the end as well as useless SQL commands.
mv populate.sql tmp.sql
grep "INSERT" tmp.sql > populate.sql
sed -i 's/$/;/' populate.sql
rm tmp.sql

echo "Postprocessing..."
python3 postprocessing/post_processing.py postprocessing/equivalent_views.sql populate.sql
mv populate.sql ../mysql-populate.sql
mv transformed_trace.sql ../pelton-populate.sql

# Generate the workload.
echo "Generating Workload..."
target/release/lobsters-mysql --runtime 60  --queries original \
    "mysql://$DBUSERNAME:$DBPASSWORD@localhost/lobsters" --scale 1 > workload.sql

# Print generated load statistics
sed -n '/^#/,$ p' workload.sql 
sed -i '/^#/Q' workload.sql

echo "Postprocessing..."
python3 postprocessing/post_processing.py postprocessing/equivalent_views.sql workload.sql
mv workload.sql ../mysql-workload.sql
mv transformed_trace.sql ../pelton-workload.sql

cd ..
# Combine the schemas, population, and workload scripts.
echo "Combining Pelton..."
cat schema/schema.sql > pelton.sql
cat schema/views.sql >> pelton.sql
cat pelton-populate.sql >> pelton.sql
rm pelton-populate.sql
echo "# perf start" >> pelton.sql
cat pelton-workload.sql >> pelton.sql
rm pelton-workload.sql

echo "Combining MySQL..."
cat noria/applications/lobsters/mysql/db-schema/original_rocksdb.sql > mysql.sql
cat mysql-populate.sql >> mysql.sql
rm mysql-populate.sql
echo "# perf start" >> mysql.sql
cat mysql-workload.sql >> mysql.sql
rm mysql-workload.sql
