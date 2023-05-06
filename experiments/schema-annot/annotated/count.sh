#!/bin/bash
for f in *.sql; do
  tbls=$(grep -i -R -E "CREATE (DATA_SUBJECT )?TABLE" $f | grep -E -v "^\s*--" | wc -l)
  ds=$(grep -i -R -E "CREATE DATA_SUBJECT TABLE" $f | grep -E -v "^\s*--" | wc -l)
  own=$(grep -i -R -E " (OWNED_BY|OWNS) " $f | grep -E -v "^\s*--" | wc -l)
  acc=$(grep -i -R -E " (ACCESSED_BY|ACCESSES) " $f | grep -E -v "^\s*--" | wc -l)
  anon=$(grep -i -R -E "ON (GET|DEL) .* (ANON|DELETE_ROW)" $f | grep -E -v "^\s*--" | wc -l)
  prefix=(${f//-/ })
  prefix=${prefix[0]}
  prefix=(${prefix//_/ })
  prefix=${prefix[0]}
  printf "\\\\%-20s & %-6s & %-9s & %-3s & %-6s & %-5s \\\\\\ \\hline" $prefix $tbls $ds $own $acc $anon
  echo ""
done
