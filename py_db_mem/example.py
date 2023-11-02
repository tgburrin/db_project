#!/usr/bin/env python3
import os
import sys
import time
import uuid
import datetime

import py_db_mem

print("Adding data dictionary")
a = py_db_mem.DataDictionary(os.path.join(os.environ['HOME'], "github", "db_project", "dd_client_rec.json"))
# a.display()

t = py_db_mem.DDTable(a, "test_table")
print(f"Setup {t.name}")
print(t)
print(t.open_shm_table())
print(f"Opened {t.name}")

r = py_db_mem.DDTableRecord(t)
print(r.list_fields())
r.print()

fr = t.new_record()

for i in range(25):
	r.set_value("client_uid", uuid.uuid1())

	if i == 10:
		fr.set_value('client_uid', r.get_value('client_uid'))
		print(fr)

	r.set_value("created_dt", datetime.datetime.utcnow())
	r.set_value("client_username", f"my_user_{i+1}")
	r.set_value("client_name", f"My Full Name {i+1}")

	rec_num = t.write_record(r)
	if i > 0 and i % 10000 == 0:
		print(rec_num)


l = t.find_records(fr, 'client_uid_idx_uq')
print(f"list: {l}")
for rv in l:
  print(f"repr:\n{rv}")

#t.print_index_tree()
t.print_index_tree("client_username_idx")

print(t.close_shm_table())
print("Done!")
