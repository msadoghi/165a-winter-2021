from datetime import datetime

class Record:
    def __init__(self, key, rid, schema_encoding, column_values):
        self.primary_key = key
        # indirection, rid, schema_encoding, timestamp
        timestamp = datetime.now()
        self.meta_data = [0, rid, schema_encoding, timestamp]
        self.user_data = column_values
        self.all_columns = self.meta_data + self.user_data
