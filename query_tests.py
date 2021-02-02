from lstore.query import *
from lstore.table import *
from lstore.db import *

def test_select():
    database = Database()
    database.create_table("Students", 6, 0)
    table = database.get_table("Students")
    query = Query(table)
    ret_record = query.select(798329, 0, [0, 1, 0, 1, 0, 1])
    print(ret_record)

# test_select()

def test_insert():
    database = Database()
    database.create_table("Students", 6, 0)
    table = database.get_table("Students")
    query = Query(table)
    did_insert = query.insert(999, 1, 2, 3, 4, 5)

# test_insert()

def test_insert_and_see_if_exists():
    database = Database()
    database.create_table("Students", 6, 0)
    table = database.get_table("Students")
    query = Query(table)
    key = 888887980
    did_insert = query.insert(key, 1, 2, 3, 4, 5)
    print("did_insert:", did_insert)
    ret_value = query.table.record_does_exist(key)
    print("ret_value:", ret_value)

# test_insert_and_see_if_exists()

def test_insert_and_delete_and_see_if_exists():
    database = Database()
    database.create_table("Students", 6, 0)
    table = database.get_table("Students")
    query = Query(table)
    key = 888887980
    did_insert = query.insert(key, 1, 2, 3, 4, 5)
    print("did_insert:", did_insert)
    did_delete = query.delete(key)
    print("did_delete", did_delete)
    ret_value = query.table.record_does_exist(key)
    print("ret_value:", ret_value)

test_insert_and_delete_and_see_if_exists()
