
# We can change this but wanted to have a starting point
class Record:
    def __init__(self, key, rid, schema_encoding, column_values):
        self.primary_key = key
        # indirection, rid, schema_encoding, timestamp
        timestamp = datetime.timestamp(datetime.now())
        self.meta_data = [None, rid, schema_encoding, timestamp]
        self.user_data = column_values
        self.all_columns = meta_data + user_data