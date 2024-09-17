import numpy as np

def is_not_null(row):
  """Check if the row value is not null. (a row is the value you get when doing df[col].apply(lambda row: ...))"""

  return (
      (row is not None) &
      (row != np.nan) &
      (str(row).strip() != "") &
      (str(row).lower() != "nan") &
      (str(row).lower() != "na")
  )

from pandas import json_normalize

def is_list_column(df, column): return df[column].apply(lambda x: isinstance(x, list)).any()

def is_dict_column(df, column): return df[column].apply(lambda x: isinstance(x, dict)).any()

def get_columns_types(df):
  
  list_columns = []
  dict_columns = []
  other_columns = []

  for c in df.columns:

    if is_dict_column(df, c):
      dict_columns.append(c)
    elif is_list_column(df, c):
      list_columns.append(c)
    else:
      other_columns.append(c)

    
  return {
    "list_columns": list_columns,
    "dict_columns": dict_columns,
    "other_columns": other_columns
  }


def recurive_flatten_and_explode(df):

  # Check
  columns_types = get_columns_types(df)

  # Explode
  for c in columns_types["list_columns"]:
    df = df.explode(c)
  
  # ReCheck
  columns_types = get_columns_types(df)

  # Flatten
  df = json_normalize(df.to_dict(orient="records"))

  # ReCheck
  columns_types = get_columns_types(df)

  # Recurse
  if len(columns_types["list_columns"]) > 0:
    df = recurive_flatten_and_explode(df)
  
  return df


def find_duplicates(df, keys):
  # Group the DataFrame by the key columns and count the occurrences
  duplicate_counts = df.groupby(keys)[keys[0]].count()

  # Filter for groups with more than one occurrence (duplicates)
  duplicates = duplicate_counts[duplicate_counts > 1]

  # Get the indices (rows) of the duplicate entries
  duplicate_indices = duplicates.index.tolist()

  # Filter the DataFrame to get the duplicate rows
  duplicate_rows = df[df.set_index(keys).index.isin(duplicate_indices)]

  return duplicate_rows

  

def get_duplicate_columns_with_different_values(df, keys):
  # Get the columns where duplicate rows have different values

  different_value_columns = []
  
  for col in df.columns:
      if df.groupby(keys)[col].nunique().max() > 1:
          different_value_columns.append(col)

  print("\nColumns with different values in duplicate rows:")
  print(different_value_columns)

  return different_value_columns


def df_to_dict(df, key_col, value_col):
  """Create a dictionary from a DataFrame where the first column value is the key and the second column value is the value"""
  return dict(zip(df[key_col], df[value_col]))
