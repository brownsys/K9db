#!/usr/bin/python3
import sys, json

if len(sys.argv) < 2:
  print("usage plot-dataflow.py <INPUT_FILE>")
  sys.exit(1)

input_file = sys.argv[1]

jobj = json.load(open(input_file))

edges = []

print("digraph g {")
for node in jobj:
  print("\"node{}\" [".format(node['id']))
  print("  label = \"{ { %d | %s } | %s }\"," %
      (node['id'], node['operator'], ",\\n".join(node['output_columns'])))
  print("  shape = \"record\"")
  print("];")
  if len(node['children']) > 0:
    edges += [(node['id'], x) for x in node['children']]

for edge in edges:
  print("\"node{}\" -> \"node{}\";".format(edge[0], edge[1]))

print("}")
