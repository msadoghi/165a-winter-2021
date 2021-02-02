from datetime import datetime

class Record:
    def __init__(self, key, rid, schema_encoding, column_values):
        self.primary_key = key
        # indirection, rid, schema_encoding, timestamp
        timestamp = datetime.now()
        # 0 for indirection column
        self.meta_data = [0, rid, timestamp, schema_encoding]
        self.user_data = column_values
        self.all_columns = self.meta_data + self.user_data
