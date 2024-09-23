import os
import glob
import traceback

from dataclasses import dataclass

from modules.edi_parsing.utils import parsing_utils
from modules.edi_parsing.utils import python_utils

@dataclass
class EDIInputType:
    OnBoard:str = "OnBoard"
    LoadList:str = "LoadList"
    Tank:str = "Tank"

@dataclass
class EDISegmentsPattern:
    LOC_147: str = r"LOC\+147.*?CNT\+8:\d+(?::\d+)?'"
    LOC_ZZZ: str = r"LOC\+ZZZ.*?FTX\+AAI.*?'"


class EDIParser:

    @classmethod
    def parse_edi_file(
        self,
        edi_input_dir,
        edi_input_type,
        segment_parser,
        segments_pattern,
    ):
        
        print(
        "edi_input_dir =", edi_input_dir,
        "edi_input_type =", edi_input_type,
        "segment_parser =", segment_parser,
        "segments_pattern =", segments_pattern,
        )
        try:
            # Find EDI files using glob
            edi_path_wildcard = os.path.join(edi_input_dir, "**/*.edi")
            edi_files = glob.glob(edi_path_wildcard, recursive=True)

            # Check if edi_files is empty
            if not edi_files:
                raise FileNotFoundError(f"No .edi files found in directory: {edi_input_dir} using wildcard {edi_path_wildcard}")

            # Filter EDI files using the find_one function
            filter_function = lambda x: edi_input_type.lower() in x.lower()
            edi_file_path = python_utils.find_one(edi_files, filter_function, default=None)

            if edi_file_path == None:
                raise FileNotFoundError(f"{edi_input_type}.edi file not found for type: {edi_input_type} in path {edi_input_dir}")

            # Read segments from modules.edi_parsing.the EDI file
            segments = parsing_utils.read_edi_segments(
                edi_file_path=edi_file_path,
                segments_pattern=segments_pattern,
            )

            # Parse segments using the segment parser
            segments_groups = segment_parser.parse_segments_groups(segments)

            # Return the result as a dictionary
            return python_utils.as_dict(segments_groups)

        except FileNotFoundError as fnf_error:
            print(f"File not found error: {fnf_error}")
            traceback.print_exc()
            return []

        except IndexError as index_error:
            print(f"No matching data found ! Please check if your edi_input_dir contains EDI files {edi_input_dir}: {index_error}")
            traceback.print_exc()
            return []

        except ValueError as val_error:
            print(f"Value error: {val_error}")
            traceback.print_exc()
            return []

        except TypeError as type_error:
            print(f"Type error: {type_error}")
            traceback.print_exc()
            return []

        except Exception as e:
            print(f"An unexpected error occurred on EDI parsing: {e}")
            traceback.print_exc()
            return []