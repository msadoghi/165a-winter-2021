from lstore.config import *

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

    def write(self, value, row):
        # if(row>self.num_records):
        #     self.num_records += 1
        # self.data[row] = value
        # assuming value is an integer
        starting_point = row * PAGE_RECORD_SIZE
        # start index : end index
        print(value)
        if isinstance(value, str):
            converted_value = bytes(value, 'utf-8')
        elif isinstance(value, int):
            converted_value = value.to_bytes(8, 'big')
        else:
            raise ValueError("ERROR: value should be int or string")

        self.data[starting_point:(starting_point + PAGE_RECORD_SIZE - 1)] = converted_value
        self.num_records += 1
        pass

    def read(self, row):
        return self.data[row * PAGE_RECORD_SIZE:(row * PAGE_RECORD_SIZE + PAGE_RECORD_SIZE - 1)]

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
