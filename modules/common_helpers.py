import datetime
import numpy as np
import pandas as pd

def is_empty(series):
    return series.isnull() | series.isna() | (series == "")

def not_defined(cell):
    if isinstance(cell, float):
        return np.isnan(cell)
    elif isinstance(cell, str):
        return cell == ""
    else:
        return False

# def not_defined(cell):
#     return np.isnan(cell) or (cell == "")

def get_compound_segment_header(
    segment_split: list, segment_header_name: str,
    csv_col_prefix_idx: int, csv_compound_col_sep: str="_"
) -> str:
        """
        Segments in the baplie message are identified by special header names. Many times, the first three characters are enough
        to identify the header name. However, there are sometimes that the latter is not applicable. For instance, the header
        LOC is used could be used in many headers. Hence, to identify different LOC headers, we have to use another unique info
        present in the header (the location function code qualifier) to distinguish that header from other LOC headers that might
        be present in the data of the same container. This is the idea behind the compound_header: it is a combination of the
        segment header and a unique information present in the segment that is concatenated for segments idenfitication.

        Paramters
        ---------
        segment_split
            a list that is a split version of the line string (a segment from the baplie message)

        csv_header_name
            the name of the segment header (that segment_split represents)

        csv_col_prefix_idx
            the index of the column prefix, which will be the unique info present in the segment used to identify that segment

        csv_compound_col_sep, default: "_"
            the seperator that seperates the column prefix from the header name

        Returns
        -------
        csv_compound_col
            the header name and the column prefix seperated by the specified seperator.
        """
        prefix = segment_split[csv_col_prefix_idx].split(":")[0]
        csv_compound_col = segment_header_name + csv_compound_col_sep + prefix

        return csv_compound_col

def get_port_name_from_seq_num(seq_2_port_name_map_dict: dict, seq_no: int) -> str:
    return seq_2_port_name_map_dict[seq_no]

def get_seq_num_from_port_name(port_name_2_seq_map_dict: dict, port_name: str) -> int:
    return port_name_2_seq_map_dict[port_name]

def is_iso_code_old(iso_code: str) -> bool:
    # if third char in iso code is a number => old iso code
    if iso_code[2].isdigit(): return True
    else: return False

def get_str_date_as_datetime(str_date: str, date_format: str="%m/%d/%Y  %I:%M:%S %p") -> datetime.datetime:
    datetime_date = datetime.datetime.strptime(str_date, date_format)
    return datetime_date

def get_datetime_obj_as_str(datetime_obj: datetime.datetime, date_format: str="%m/%d/%Y  %I:%M:%S %p") -> str:
    date_str = datetime.datetime.strftime(datetime_obj + datetime.timedelta(hours=3), date_format)
    return date_str

def get_datetime_diff_by_unit(date: datetime.timedelta, unit: str="h") -> float:
    total_seconds = date.total_seconds()
    if unit == "s": return total_seconds
    elif unit == "m": return total_seconds / 60
    elif unit == "h": return total_seconds / 3600
    elif unit == "d": return total_seconds / 86400

# def get_str_time_as_timedelta(str_time: str) -> datetime.timedelta:
#     hours, minutes = map(int, str_time.split('.'))
#     return datetime.timedelta(hours=hours, minutes=minutes)

def get_str_time_as_timedelta(str_time: str) -> datetime.timedelta:
    parts = str_time.split('.')
    if len(parts) == 1:
        # Only hours provided
        hours = int(parts[0])
        return datetime.timedelta(hours=hours)
    elif len(parts) == 2:
        # Hours and minutes provided
        hours, minutes = map(int, parts)
        return datetime.timedelta(hours=hours, minutes=minutes)
    else:
        raise ValueError("Invalid time format. The input should be in the format 'hours' or 'hours.minutes'.")


def split_list(lst:list, num_lists:int)-> list:
    list_size = len(lst)
    sublist_size = list_size // num_lists
    remaining_elements = list_size % num_lists

    result = []
    start = 0

    for i in range(num_lists):
        sublist_length = sublist_size + (i < remaining_elements)
        end = start + sublist_length
        result.append(lst[start:end])
        start = end

    return result

def nearest_neighbor_interpolation(xy, xy_data, z_data, n_neighbors=1):
    # Calculate the Euclidean distance between xy and each element in xy_data
    dist = np.linalg.norm(xy[:, None] - xy_data, axis=2)

    # Find the indices of the n nearest neighbors for each point in xy
    nearest_indices = np.argsort(dist, axis=1)[:, :n_neighbors]

    # Use the indices to get the corresponding z_data values for the n nearest neighbors
    nearest_values = z_data[nearest_indices]

    # Calculate the approximation as the mean of the n nearest neighbors
    interpolated_values = np.mean(nearest_values, axis=1)

    return interpolated_values


def linear_interpolation_speed(x_speed, x_known, y_known):
    interpolated_values = np.interp(x_speed, x_known, y_known)
    return interpolated_values

def extract_as_dict(df, indexes=None, columns=None):
    """
    Extracts specific column values for the given indexes in the DataFrame and returns them as a dictionary.

    This function extracts the values from the DataFrame for the specified indexes (or all indexes if None)
    in the specified columns. It then returns these values as a dictionary of dictionaries, where the outer
    dictionary's keys represent the indexes, and the values are dictionaries containing the extracted column
    values for each index.

    Parameters:
        df (pandas.DataFrame): The DataFrame from which to extract the values.
        indexes (int, list, or None, optional): The indexes for which to extract the values. If None,
            all indexes in the DataFrame will be used. Defaults to None.
        columns (str or list, optional): The columns to extract values from. If None, all columns in the
            DataFrame will be used. Defaults to None.

    Returns:
        dict: A dictionary of dictionaries containing the extracted column values for the specified indexes.

    Examples:
        >>> import pandas as pd
        >>> data = {
        ...     'StdSpeed': ['12.5', '10.0'],
        ...     'MaxDraft': ['9.2', '8.7'],
        ...     'Gmdeck': ['1.8', '2.0'],
        ... }
        >>> df = pd.DataFrame(data)
        >>> extract_as_dict(df, indexes=[0, 1], columns=['StdSpeed', 'Gmdeck'])
        {0: {'StdSpeed': 12.5, 'Gmdeck': 1.8}, 1: {'StdSpeed': 10.0, 'Gmdeck': 2.0}}

        >>> extract_as_dict(df, columns='StdSpeed')
        {0: {'StdSpeed': 12.5}, 1: {'StdSpeed': 10.0}}

        >>> extract_as_dict(df)  # Extract all columns for all indexes
        {0: {'StdSpeed': 12.5, 'MaxDraft': 9.2, 'Gmdeck': 1.8}, 1: {'StdSpeed': 10.0, 'MaxDraft': 8.7, 'Gmdeck': 2.0}}
    """
    if indexes is None:
        indexes = df.index.tolist()

    if columns is None:
        columns = df.columns.tolist()
    elif isinstance(columns, str):
        columns = [columns]

    extracted_data = {}
    for index in indexes:
        extracted_data[index] = df.loc[index, columns].to_dict()

    return extracted_data

def convert_date_columns(data_frame: pd.DataFrame, columns: list, date_format: str="%d/%m/%Y %H:%M") -> pd.DataFrame:
    """
    Converts specified columns in a DataFrame from string representation of dates to datetime objects.

    Parameters:
        data_frame (pd.DataFrame): The DataFrame containing the columns to be converted.
        columns (list): A list of column names to convert.
        date_format (str, optional): The format of the date strings in the columns. Default is "%d/%m/%Y %H:%M".

    Returns:
        pd.DataFrame: The DataFrame with the specified columns converted to datetime objects.
    """
    for x in columns:
        data_frame.loc[:, x] = data_frame[x].apply(get_str_date_as_datetime, date_format=date_format)
    return data_frame
