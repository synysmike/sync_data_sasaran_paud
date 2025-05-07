# column_mapper.py

class ColumnMapper:
    def __init__(self):
        self.column_names = {}

    def add_mapping(self, index, name):
        """Maps a column index to a specific name."""
        self.column_names[index] = name

    def get_name(self, index):
        """Returns the mapped column name for a given index."""
        return self.column_names.get(index, f"{index}")  # Default naming

    def apply_mapping(self, df):
        """Applies the mapping to a pandas DataFrame."""
        df.rename(columns={i: self.get_name(i)
                  for i in df.columns}, inplace=True)
        return df
