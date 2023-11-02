#!/usr/bin/env python3
import os
import sys
import time
import uuid
import datetime

import py_db_mem

def trace(frame, event, arg):
    print("%s, %s:%d" % (event, frame.f_code.co_filename, frame.f_lineno))
    return trace

sys.settrace(trace)

print("Adding data dictionary")
try:
  a = py_db_mem.DataDictionary(os.path.join(os.environ['HOME'], "github", "db_project", "dd_junk.json"))
  print(a)
  del a
except Exception as ex:
  print("Exception:")
  print(str(ex))

a = py_db_mem.DataDictionary(os.path.join(os.environ['HOME'], "github", "db_project", "dd_client_rec.json"))
print("Adding dd")
a.display()
# a.load_all_tables()

try:
  t = py_db_mem.DDTable(table_name="test_tables", dd=a)
except Exception as ex:
  print("Exception:")
  print(str(ex))

t = py_db_mem.DDTable(a, "test_table")
print(t.name)
print(t)
print(t.open_shm_table())
#time.sleep(30)

r = py_db_mem.DDTableRecord(t)
o = t.new_record()
fr = t.new_record()
r.print()

bs = b'deadbeef'

print(r.list_fields())
print(r.set_value("test", "that"))

for i in range(25):
	#print(r.set_value("client_uid", str(uuid.uuid1())))
	r.set_value("client_uid", uuid.uuid1())

	if i == 10:
		fr.set_value('client_uid', r.get_value('client_uid'))
		print(fr)

	#print(r.set_value("created_dt", datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)))
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

print(t.close_shm_table())
print("Done!")
