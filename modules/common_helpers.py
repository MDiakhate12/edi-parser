import datetime

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