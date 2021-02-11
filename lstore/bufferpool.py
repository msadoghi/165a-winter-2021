from lstore.config import * 
from lstore.page import *
from lstore.table import *

class Bufferpool:

    def __init__(self):
        self.frames = []
        self.frame_directory = {}
        self.frame_count = 0

    def _add_frame_to_directory(self,page_range,base_page,column_num):
        new_frame_key = (page_range,base_page,column_num)
        self.frame_directory[new_frame_key] = self.frame_count
        self.frame_count += 1

    def at_capacity(self):
        if self.frame_count < BUFFERPOOL_FRAME_COUNT:
            return False
        else:
            return True    

    def rid_in_pool(self,rid,column):
        page_range_index = math.floor(rid / ENTRIES_PER_PAGE_RANGE)
        index = rid % ENTRIES_PER_PAGE_RANGE
        base_page_index = math.floor(index / ENTRIES_PER_PAGE)
        location_of_column = (page_range_index,base_page_index,column)

        if location_of_column in self.frame_directory:
            return True
        else:
            return False


    def evict_page(self):
        pass

    def load_page(self,base_page,page_range,column_num):
        pass

    def commit_page(self):
        pass


class Frame:

    def __init__(self,table_name):
        self.page = Page()
        self.dirty_bit = False
        self.pin = False
        self.time_in_bufferpool = 0
        self.access_count = 0 #number of times page has been accessed
        self.table_name = table_name

    
    def set_dirty_bit(self):
        self.dirty_bit = True
        return True

    def pin_frame(self):
        self.pin = True
        return True

    def unpin_frame(self):
        self.pin = False
        return True
    

