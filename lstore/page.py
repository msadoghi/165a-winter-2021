from lstore.config import *
from datetime import datetime
import struct
import time

class Page:

    def __init__(self, column_num):
        self.num_records = 0
        self.column_num = column_num
        self.data = bytearray(PAGE_SIZE)

    def has_capacity(self):
        current_empty_space = ENTRIES_PER_PAGE - self.num_records
        if current_empty_space == 0:
            return False
        elif current_empty_space > 0:
            return True
        else:
            raise ValueError('ERROR: current_empty_space must be greater or equal to zero.')

    def write(self, value, row) -> bool:
        # if(row>self.num_records):
        #     self.num_records += 1
        # self.data[row] = value
        # TODO if value > 8 bytes raise error
        if (not self.has_capacity()):
            return False
        value_type = type(value)
        starting_point = row * PAGE_RECORD_SIZE

        if (value_type is str):
            val = value
            self.data[starting_point:(starting_point + PAGE_RECORD_SIZE - 1)] = bytes(val, 'utf-8')
            # convert string to bytearray
        elif (value_type is int):
            # start index : end index
            self.data[starting_point:(starting_point + PAGE_RECORD_SIZE - 1)] = value.to_bytes(8, 'big')
        elif (value_type is datetime):
            date = time.mktime(value.timetuple())
            # print("date", date)
            # '>' denotes big endian, f denotes float
            bin_datetime = struct.pack('>f', date)
            # print("bin_datetime", bin_datetime)
            recovered_bindate = struct.unpack('>f', bin_datetime)[0]
            # print(datetime.fromtimestamp(recovered_bindate))
            self.data[starting_point:(starting_point + PAGE_RECORD_SIZE)] = bin_datetime

        self.num_records += 1
        return

    def read(self, row):
        # TODO figure out what type of data we are supposed to be reading out of the byte array
        # can be found using column_num ->  codes can be found in config
        starting_point = row * PAGE_RECORD_SIZE
        ret_value_in_bytes = self.data[starting_point:(starting_point + PAGE_RECORD_SIZE)]
        int_val = int.from_bytes(bytes=ret_value_in_bytes, byteorder="big")
        return int_val

#     def insert_new_record(self,value):
#         # assuming value is an integer
#         if(self.num_records + 1 < PAGE_SIZE/PAGE_RECORD_SIZE):
#             raise Exception('Max records reached in this page!')
#         starting_point = self.num_records * PAGE_RECORD_SIZE
#         self.data[starting_point:(starting_point + PAGE_RECORD_SIZE - 1)] = value.to_bytes(8, 'big')
#         self.num_records += 1
#         pass

# # useful for editing metadata columns in place
#     def edit_existing_record(self, value, row):
#         starting_point = row * PAGE_RECORD_SIZE
#         existing_record = self.data[starting_point:starting_point + PAGE_RECORD_SIZE - 1]
#         if(existing_record )

# write new row
# edit existing

'''
 SID, grade1, grade2

 1, 90, 95
 2, 65, 60
 <empty>
 3, 30, 95
 <empty>
 4, 100, 98
<empty>
<empty>
56, 50, 25
<empty>
 70, 100, 98
 <empyt.....>
 '''
