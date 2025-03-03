import sys
from filter_cond import filter_cond
from model import fields

input_data = sys.stdin.read()
for data in input_data.strip().split("\n"):
    line_dict = dict(zip(fields[0:1]+fields[2:],data.split("\t")))
    if filter_cond(line_dict):
	print(data)

