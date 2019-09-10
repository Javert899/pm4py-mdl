import bson
import json


gen = bson.decode_file_iter(open('table1.bson'))
json_list = []
for row in gen:
     json_list.append(row)
