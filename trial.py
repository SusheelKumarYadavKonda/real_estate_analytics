import os
import pandas as pd

df = pd.read_parquet('raw/combined_file.parquet')

def get_column_data_types():
    """
    Reads a CSV or Parquet file and returns a dictionary of column names and their data types.

    Parameters:
    file_path (str): The path to the file (CSV or Parquet).
    file_type (str): The type of the file ('csv' or 'parquet'). Defaults to 'csv'.

    Returns:
    dict: A dictionary where the keys are column names and the values are the data types.
    """

   

    # Get the data types of each column
    column_data_types = {}

    # Analyze integer columns
    for column in df.columns:
        if pd.api.types.is_integer_dtype(df[column]):
            min_value = df[column].min()
            max_value = df[column].max()
            
            if min_value >= -(2**31) and max_value <= 2**31 - 1:
                column_data_types[column] = 'INT'
            else:
                column_data_types[column] = 'BIGINT'

    return column_data_types

column_data_types = get_column_data_types()  # Change to 'parquet' if needed

# Print the column names and their inferred SQL data types
for column, data_type in column_data_types.items():
    print(f"{column} {data_type}")