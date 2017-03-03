#!/bin/python

ifname = 'all_parts.csv'
ofname = 'all_parts_cleaned.csv'

f_in = open(ifname)
f_out = open(ofname, 'w')

first = 1
for row in f_in:
  tokens = row.split(',')
  if first == 1 and tokens[0] == 'c_display_id':
    f_out.write(row)
    first = 0
  if first == 0 and tokens[0] != 'c_display_id':
    f_out.write(row)

