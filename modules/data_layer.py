import pandas as pd
import boto3
import csv
import json
import os
import io
import shutil
import logging

#TODO check unused functions

class DataLayer():
    def __init__(self, logger: logging.Logger, s3_bucket_out: str="", s3_bucket_in: str="", s3_prefix: str="") -> None:
        """
        Class Contsructor, takes the terminal arguments as a list. Think of DataLayer as a way to communicate with
        the outside world, e.g., reading, editing, and saving files, communicating with databases, etc...

        Parameters
        ----------
        
        
        Returns
        -------
        None
        """
        self.__logger = logger
        self.__s3_bucket_out = s3_bucket_out
        self.__s3_bucket_in = s3_bucket_in
        self.__s3_prefix = s3_prefix
        self.__logger = logger

        if self.__s3_bucket_out == "" and self.__s3_bucket_in == "": self.__is_local = True
        else: self.__is_local = False

    def __read_file_from_s3(self, file_path: str, s3_bucket: str="") -> str:
        s3 = boto3.resource("s3")
        key = self.__s3_prefix + file_path
        try:
            obj = s3.Object(s3_bucket, key)
            #content = obj.get()["Body"].read().decode("utf-8")
        except Exception as e:
            self.__logger.info(e)
            return ""
        else:
            #self.__logger.info("read_file_from_s3 => OK")
            #self.__logger.info(content)
            return obj.get()["Body"].read().decode("utf-8")#content
                
    def __write_file_to_s3(self, file_content: str, file_path: str, s3_bucket: str="") -> None:
        s3 = boto3.client("s3")
        key = self.__s3_prefix + file_path
        s3.put_object(Body=file_content, Bucket=s3_bucket, Key=key)

    def read_xlsx(self, xlsx_path: str, na_values: list, s3_bucket: str="", sheet: str=None) -> pd.DataFrame:
        
        if self.__is_local:
            if sheet:
                df = pd.read_excel(xlsx_path, na_values=na_values, dtype=str, sheet_name=sheet)
            else: 
                df = pd.read_excel(xlsx_path, na_values=na_values, dtype=str)
        else:
            csvStringIO = io.StringIO(self.__read_file_from_s3(xlsx_path, s3_bucket))
            if sheet:
                df = pd.read_excel(csvStringIO, na_values=na_values, dtype=str, sheet_name=sheet)
            else: 
                df = pd.read_excel(csvStringIO, na_values=na_values, dtype=str)
        
        return df  
    
    
    def read_csv(self, csv_path: str, na_values: list, sep: str=";", s3_bucket: str="") -> pd.DataFrame:
        if self.__is_local:
            df = pd.read_csv(csv_path, sep=sep, na_values=na_values, dtype=str)
        
        else:
            csvStringIO = io.StringIO(self.__read_file_from_s3(csv_path, s3_bucket))
            df = pd.read_csv(csvStringIO, sep=sep, na_values=na_values, dtype=str)
        
        return df
    
    def read_csv_lines(self, csv_path: str, s3_bucket: str="", new_line: str="", encoding: str="utf-8", skip_first_line: bool=False) -> list:
        if self.__is_local:
            lines = []
            with open(csv_path, "r", encoding=encoding) as f:
                for i, line in enumerate(f.readlines()):
                    if skip_first_line and i == 0:
                        continue

                    # else
                    lines.append(line.rstrip(new_line))
        
        else:
            lines = self.__read_file_from_s3(csv_path, s3_bucket).splitlines()
        
        return lines

    # def write_json(self, event: dict, json_path: str) -> None:
    #     self.__logger.info("try to write_json")
    #     if self.__is_local:
    #         with open(json_path, "w") as outfile:
    #             json.dump(event, outfile)
    #     else:
    #         s3 = boto3.client("s3")
    #         self.__logger.info("try to write_jso in s3")
            
    #         s3.put_object(Body=(bytes(json.dumps(event).encode('UTF-8'))), Bucket=self.__s3_bucket, Key=json_path)
    #         #self.__write_file_to_s3(event, json_path)
        
    def write_csv(self, df: pd.DataFrame, csv_path: str, s3_bucket: str="", sep: str=";", encoding: str="utf-8") -> None:
        if self.__is_local:
            df.to_csv(csv_path, index=False, sep=sep, encoding=encoding)
        
        else:
            df_csv = df.to_csv(index=False, sep=sep, encoding=encoding)
            self.__write_file_to_s3(df_csv, csv_path, s3_bucket)
    
    def write_csv_lines(self, lines: list, csv_path: str, s3_bucket: str="", sep: str=";", new_line: str="", encoding: str="utf-8") -> None:
        if self.__is_local:
            with open(csv_path, "w", newline=new_line, encoding=encoding) as csv_file:
                writer = csv.writer(csv_file, delimiter=sep)
                for sub in lines:
                    line_split = sub.split(";")
                    writer.writerow(line_split)
        
        else:
            df = pd.DataFrame([sub.split(sep) for sub in lines])
            df.columns = df.iloc[0]
            df = df.iloc[1:, :]
            df_csv = df.to_csv(index=False, sep=sep, encoding=encoding)
            self.__write_file_to_s3(df_csv, csv_path, s3_bucket)
    
    def list_files_in_path(self, path, s3_bucket: str="") -> list:
        if self.__is_local:
            #self.__logger.info("list files in path from local")
            #file_list = os.listdir(path)
            file_list = [folder for folder in os.listdir(path) if not os.path.isdir(os.path.join(path, folder))]
        
        else:
            self.__logger.info("list files in path from s3")
            s3 = boto3.resource("s3")
            bucket = s3.Bucket(s3_bucket)
            self.__logger.info(f"self.__s3_prefix + path : {self.__s3_prefix + path}")
            file_list = []
            for object_summary in bucket.objects.filter(Prefix=self.__s3_prefix + path + "/"):
                #self.__logger.info(f"key: {object_summary.key}")
                file_list.append(os.path.basename(object_summary.key))
            #file_list = file_list[1:]
        #self.__logger.info(file_list)
        return list(filter(None, file_list))
        
    def list_folders_in_path(self, path, s3_bucket: str="") -> list:
        if self.__is_local:
            self.__logger.info("list files in path from local")
            #file_list = os.listdir(path)
            folders_list = [folder for folder in os.listdir(path) if os.path.isdir(os.path.join(path, folder))]
        
        else:
            self.__logger.info("list folders in path from s3")
            
            s3 = boto3.client("s3")
            response = s3.list_objects_v2(Bucket=s3_bucket, Delimiter = '/', Prefix = path + "/")            
            
            folders_list = []
            for prefix in response['CommonPrefixes']:
                
                full_path = prefix['Prefix'][:-1]
               
                folder_from_path = full_path.split(sep="/")[-1]
                         
                folders_list.append(folder_from_path)

        return folders_list
    
    def clear_folder(self, destination_key: str, destination_bucket_name: str = ""):
        if self.__is_local:
            # For local, delete the entire folder and recreate it
            destination_folder = os.path.join(destination_bucket_name, destination_key)
            if os.path.exists(destination_folder):
                shutil.rmtree(destination_folder)
            os.makedirs(destination_folder)
        else:
            # For AWS S3, delete all objects (including objects in subfolders) in the folder
            s3 = boto3.client('s3')
            objects_to_delete = s3.list_objects_v2(Bucket=destination_bucket_name, Prefix=destination_key)
            if 'Contents' in objects_to_delete:
                delete_keys = [{'Key': obj['Key']} for obj in objects_to_delete['Contents']]
                s3.delete_objects(Bucket=destination_bucket_name, Delete={'Objects': delete_keys})

    def copy_file(self, source_key:str, destination_key:str, source_bucket_name:str="", destination_bucket_name:str="", file_name:str="LoadList.edi") -> None:
        if self.__is_local:
            # Copy the file locally
            # Create the destination directory if it doesn't exist
            os.makedirs(destination_key, exist_ok=True)

            source_file = os.path.join(source_bucket_name, source_key)
            destination_file = os.path.join(destination_bucket_name, destination_key, file_name)
            shutil.copy(source_file, destination_file)
        else:
            destination_key = os.path.join(destination_key, file_name)
            # Copy the file from S3 to S3
            s3 = boto3.client('s3')
            copy_source = {
                'Bucket': source_bucket_name,
                'Key': source_key
            }
            s3.copy_object(
                Bucket=destination_bucket_name,
                CopySource=copy_source,
                Key=destination_key
            )
            
    def read_file(self, file_path: str, s3_bucket: str="") -> str:
        if self.__is_local:
            with open(file_path, "r") as f:
                return f.read()

        else:
            return self.__read_file_from_s3(file_path, s3_bucket)

    def write_file(self, file_content: str, file_path: str, s3_bucket: str="") -> None:
        if self.__is_local:
            with open(file_path, "w") as f:
                f.write(file_content)

        else:
            self.__write_file_to_s3(file_content, file_path, s3_bucket)

    def read_json(self, file_path: str, s3_bucket: str="") -> dict:
        
        if self.__is_local:
            
            with open(file_path) as json_file:
                return json.load(json_file)
        
        else:
            
            file_content = self.__read_file_from_s3(file_path, s3_bucket)
            
            return  json.loads(file_content)

    def get_file_name_and_extension_from_path(self, file_path: str) -> 'tuple[str, str]':
        file_name_with_extension = file_path.split("/")[-1]
        l_file_name_with_extension_split = file_name_with_extension.split(".")

        file_extension = l_file_name_with_extension_split[-1]

        if len(l_file_name_with_extension_split) > 1:
            file_name = ".".join(l_file_name_with_extension_split[:-1])
        else:
            file_name = l_file_name_with_extension_split[0]

        return file_name, file_extension

    def get_baplie_delimiter(self, baplie_message: str) -> str:
        """
        Any baplie message will contain a LOC header as "LOC+---", where "-" represents a character, "LOC+---" indicates the start of the data for a new container.
        Hence, the logic is to find the index of any LOC header by searching for the index of the first occurence of the substring "LOC" inside the whole baplie message (a string),
        which will be the index of the letter "L" inside "LOC". The character right before that "L" will be the delimiter that seperates that "LOC+---" header from the header before it.
        Therefore, we select the character at the found index of "L" - 1 to get the delimiter.

        Parameters
        ----------
        baplie_message
            the input baplie message that is read

        Returns
        -------
        delimiter
            the delimiter that separates consecutive headers in the baplie messsage, the last character in a header
        """
        first_LOC_index = baplie_message.index("LOC")
        delimiter = baplie_message[first_LOC_index-1]

        return delimiter

    def read_baplie_as_list(self, baplie_path: str, s3_bucket:str) -> list:
        """
        The baplie file is read as a single string, where the end of every segment is indicated by a signle quote.
        This function splits the baplie message into segments based on the single quote, where it returns a list 
        and every element of that list is a segment from the baplie message.

        Parameters
        ----------
        None

        Returns
        -------
        segments_list
            a list where every element is a segment (str) from the input baplie message
        """
        # f_baplie = open(baplie_path, 'r')
        # baplie_message = f_baplie.read()
        baplie_message = self.read_file(baplie_path, s3_bucket)
        segment_delimiter = self.get_baplie_delimiter(baplie_message)
        segments_list = baplie_message.split(segment_delimiter)

        # if delimiter exists at the end of the baplie, the split will cause an empty string at the end of the list,
        # which will lead to an error when checking if file_name contains the correct POL
        if segments_list[-1] == "":
            segments_list = segments_list[:-1]

        # f_baplie.close()

        return segments_list
        
    def __get_baplie_list_no_header(self, segments_list: list, baplie_path: str, folder_name: str, file_name: str, s3_bucket:str) -> list:
        """
        As the header message does not contain relevant information for the desired output CSV file,
        this function takes the list of segments generated by baplie_to_list() (func in this module) and
        returns the same lists of segments but without the segments from the header of the baplie input message.
        
        Parameters
        ----------
        segments_list
            the list of segments from the baplie message

        Returns
        -------
        segments_list_no_header
            a list where every element is a segment (str) from the input baplie message but without the segments
            coming from the header of the input baplie message
        """
        segments_list = self.read_baplie_as_list(baplie_path, s3_bucket)
        # the first line after the header will always start with LOC
        # flag: the value -1 is impossible for the index of the first LOC that identifies the start of info for the first container
        first_segment_in_body_idx = -1
        for idx, segment in enumerate(segments_list):
            segment_split = segment.split("+")

            if segment_split[0] == "LOC" and segment_split[1] not in ["5", "61"]: # code identifiers for the 2 LOC headers in the message header
                    first_segment_in_body_idx = idx
                    break

            #TODO add an else statement to check if any headers from the body exists and throw an error?
        
        if first_segment_in_body_idx != -1:
            
            return segments_list[first_segment_in_body_idx:]

        else:
            self.__logger.warning(f"Failed to identify number of segments in baplie header of {file_name} in {folder_name}...")
            return None
        
    def get_new_data_flag(self, segments_list_no_header: list) -> str:
        """
        This function gets the LOC header ("LOC+---" as explained in get_baplie_delimiter()) that separates consecutive headers in the baplie message. We called it new_data_flag.

        Parameters
        ----------
        segments_list_no_header
            a list where every element represents a segment in the baplie message (without the delimiter), segments in the baplie header excluded, i.e., segments present only in the body or the tail of the baplie

        Returns
        -------
        new_data_flag
            the "LOC+---" that determines the start of the data for a new container
        """
        first_segment_of_first_container = segments_list_no_header[0]

        # The first segment of the first container will be a LOC segment, and the identifier will consist
        # of the three letters LOC and the location function code qualifier seperated by a +
        first_segment_split = first_segment_of_first_container.split("+")
        new_data_flag = f"{first_segment_split[0]}_{first_segment_split[1]}"

        return new_data_flag

    # def __handle_container_missing_serial_num(self, container_data: list, container_idx: int, baplie_filename_no_extension: str) -> list:
    #     counter = 0
    #     container_idx = 0
    #     missing_serial_numbers_indices_list = []
    #     for j, segment in enumerate(container_data):
    #         if segment[:3] == "EQD":
    #             container_idx += 1

    #             segment_split = segment.split("+")
    #             serial_number_segment_split = segment_split[2].split(":")
    #             serial_number = serial_number_segment_split[0]

    #             if not len(serial_number):
    #                 counter += 1

    #                 self.__logger.warning(f"Found container {container_idx+1} with missing serial number in {baplie_filename_no_extension}...")

    #                 serial_number = f"{baplie_filename_no_extension}_missing_CN{container_idx+1}"
    #                 serial_number_segment_split[0] = serial_number
    #                 serial_number_segment_joined = ":".join(serial_number_segment_split)
                    
    #                 segment_split[2] = serial_number_segment_joined
    #                 segment_joined = "+".join(segment_split)
    #                 container_data[j] = segment_joined
    #                 missing_serial_numbers_indices_list.append(container_idx)

    #                 break
        
    #     if counter:
    #         self.__logger.warning(f"There are {counter} containers without a serial number in {baplie_filename_no_extension} in the following positions: {*missing_serial_numbers_indices_list,}...")

    #     return container_data

    def __get_data_list_of_sublists(self, segments_list_no_header: list, new_data_flag: str, baplie_path: str, baplie_type_from_file_name: str) -> list:
        """
        As we are interested only in the body of the message since it contains the data of the containers,
        this function takes the list of segments (header excluded) and returns a list of lists (sbulists),
        where every sublist is a list of segments for a specific container and every segment in that sublist
        holds data for that specific container.

        Parameters
        ----------
        segments_list_no_header
            the list of segments without the header from the baplie message

        new_data_flag
            the "LOC+---" that determines the start of the data for a new container

        Returns
        -------
        data_list_of_sublists
            a list of lists (sbulists), where every sublist is a list of segments for a specific container
            and every segment in that sublist holds data for that specific container
        """
        # start_idx = 0 as it is assumed that the first segment will hold information about the first container
        start_idx = 0
        new_data_flag_in_baplie = new_data_flag.replace("_", "+") # example: instead of LOC_147 it becomes LOC+147 (as in baplie)
        data_list_of_sublists = []
        # container_idx = 0
        for idx, segment in enumerate(segments_list_no_header):
            if idx != 0: # to disregard the first segment as it belongs to the first container
                if segment[:7] == new_data_flag_in_baplie: # LOC+147, LOC+ZZZ, etc... represents the start of segments for a new container
                    # in the following segment of code, we slice the list from start_idx to the index
                    # just before idx (where the segments for a new container begins): the index just before
                    # idx will be the end of the segments for the old container w.r.t. the new container

                    # preprocessing step to remove duplicates:
                    # list(set(segments_list_no_header[start_idx:idx])) was first used but it is no good as it disrupts the order of the segments
                    segments_list_no_header_with_dups = segments_list_no_header[start_idx:idx]
                    segments_list_no_header_no_dups = []
                    segments_list_no_header_no_dups = [segment for segment in segments_list_no_header_with_dups if segment not in segments_list_no_header_no_dups]

                    # if baplie_type_from_file_name == "container":
                    #     container_idx += 1
                    #     csv_file_name = self.__concat_port_name_to_baplie_type(baplie_path, baplie_type_from_file_name)
                    #     segments_list_no_header_no_dups = self.__handle_container_missing_serial_num(
                    #         segments_list_no_header_no_dups, container_idx, csv_file_name
                    #     )
                    
                    data_list_of_sublists.append(segments_list_no_header_no_dups)
                    
                    start_idx = idx # let start_idx be the index where the segments of a new container start

        # when the last flag is found (the start of the data for the last container), another flag will not be found after, hence, we need to add it
        # after the loop ends.
        # PS: the benifit of leaving this part here is not iterate over the whole list of segments from the beginning to find the segments that has the first
        #     2 characters equal to "UN".
        last_data_with_dups = segments_list_no_header[start_idx:]

        # removing the tail of the message
        tail_headers_names_indices_list = [idx for idx, segment in enumerate(last_data_with_dups) if segment[:2] == "UN"]
        if len(tail_headers_names_indices_list):
            tail_start_idx = tail_headers_names_indices_list[0]
            last_data_with_dups_no_tail = last_data_with_dups[:tail_start_idx]

        else:
            last_data_with_dups_no_tail = last_data_with_dups

        last_data_no_dups = []
        last_data_no_dups = [segment for segment in last_data_with_dups_no_tail if segment not in last_data_no_dups]
        data_list_of_sublists.append(last_data_no_dups)

        return data_list_of_sublists

    def get_baplie_type_from_file_name(self, baplie_path: str) -> str:
        file_name = baplie_path.split("/")[-1]
        if file_name == "Tank.edi": return "tank"
        elif file_name in ["OnBoard.edi", "LoadList.edi"]: return "container"
        else: return None

    def get_baplie_type_from_content(self, segments_list_no_header: list) -> str:
        EQD_segments = [segment for segment in segments_list_no_header if segment[:3] == "EQD"]
        if len(EQD_segments):
            for segment in EQD_segments:
                if segment.split("+")[1] == "CN":
                    return "container"
        
        return "tank"

    def get_folder_name_from_path(self, baplie_path: str, split: bool=False) -> str:
        folder_name = baplie_path.split("/")[-2].replace("call_", "")
        if split:
            return folder_name.split("_")

        else:
            return folder_name

    def get_file_name_from_path(self, baplie_path: str) -> str:
        file_name = baplie_path.split("/")[-1].replace(".edi", "")
        return file_name

    def read_baplie_body_as_list(self, baplie_path: str, folder_name: str, file_name: str, s3_bucket:str) -> 'tuple[list, str]':
        """
        Takes no argument and reads the baplie message by calling the other functions from this class in the right order.
        This functions is like main in this class.

        Parameters
        ----------
        None

        Returns
        -------
        l_baplie_segments
            a list of lists (sbulists), where every sublist is a list of segments for a specific container
            and every segment in that sublist holds data for that specific container

        new_data_flag
            the "LOC+---" that determines the start of the data for a new container
        """
        baplie_type_from_file_name = self.get_baplie_type_from_file_name(baplie_path)
        if baplie_type_from_file_name is None: # failed to identify the baplie type
            return None, None, None, None
        
        segments_list = self.read_baplie_as_list(baplie_path, s3_bucket)
        segments_list_no_header = self.__get_baplie_list_no_header(segments_list, baplie_path, folder_name, file_name, s3_bucket)
        if segments_list_no_header is None: # failed to identify the start of the body in the baplie
            return None, None, None, None

        new_data_flag = self.get_new_data_flag(segments_list_no_header)
        if new_data_flag is None: # failed to identify the start of data segments for a certain container A
            return None, None, None, None
        
        l_baplie_segments = self.__get_data_list_of_sublists(segments_list_no_header, new_data_flag, baplie_path, baplie_type_from_file_name)

        baplie_type_from_content = self.get_baplie_type_from_content(segments_list_no_header)
        
        return l_baplie_segments, new_data_flag, baplie_type_from_file_name, baplie_type_from_content

    # def get_all_baplies_with_flags_as_dict_of_lists(self, l_baplies_paths: list) -> tuple[dict, dict, dict]:
    #     d_baplies_as_list = {"containers": [], "tanks": []}
    #     d_new_data_flags_as_list = {"containers_flags": [], "tanks_flags": []}
    #     d_baplies_by_type_paths = {"containers_paths": [], "tanks_paths": []}
    #     for baplie_path in l_baplies_paths:
    #         folder_name = self.get_folder_name_from_path(baplie_path)
    #         file_name = self.get_file_name_from_path(baplie_path)
    #         self.__logger.info(f"Reading {file_name} from {folder_name}...")
    #         l_baplie_segments, new_data_flag, baplie_type_from_file_name = self.read_baplie_body_as_list(baplie_path, folder_name, file_name)

    #         #TODO add logger
    #         if baplie_type_from_file_name is None:
    #             continue
            
    #         d_baplies_as_list[f"{baplie_type_from_file_name}s"].append(l_baplie_segments)
    #         d_new_data_flags_as_list[f"{baplie_type_from_file_name}s_flags"].append(new_data_flag)
    #         d_baplies_by_type_paths[f"{baplie_type_from_file_name}s_paths"].append(baplie_path)

    #     return d_baplies_as_list, d_new_data_flags_as_list, d_baplies_by_type_paths

    # def __concat_port_name_to_baplie_type(self, baplie_path: str, baplie_type_from_file_name: str) -> str:
    #     POL_name = self.__get_port_name_from_path(baplie_path)
    #     return f"{POL_name}_{baplie_type_from_file_name}"

    # def save_dataframe_as_csv(self, df: pd.DataFrame, csv_file_name: str, folder_name: str="") -> None:
    #     """
    #     This function takes an input python dictionary, transforms it into a pandas dataframe,
    #     and then saves it into a csv.

    #     Parameters
    #     ----------
    #     df
    #         the input dataframe to be saved as a CSV
        
    #     folder_to_save_in
    #         the desired path to save the csv, empty string by default

    #     suffix
    #         an optional suffix to add after the csv filename, empty string by default

    #     Returns
    #     -------
    #     None
    #     """
    #     if folder_name:
    #         full_path = f"{self.__output_dir}/{folder_name}_{csv_file_name}.csv" # if and else just to add the "_"
            
    #     else:
    #         full_path = f"{self.__output_dir}/{csv_file_name}.csv"

    #     df.to_csv(full_path, index=False, sep=";", encoding="utf-8")
    
    def save_dict_as_csv(self, dict_to_save: dict, path_to_save: str) -> None:
        """
        This function takes an input python dictionary and saves it into a csv.

        Parameters
        ----------
        dict_to_save
            the input dict to be saved as a CSV
        
        path_to_save
            the desired path to save the csv, empty string by default

        suffix
            an optional suffix to add after the csv filename, empty string by default

        Returns
        -------
        None
        """
        df = pd.DataFrame(dict_to_save)
        df.to_csv(path_to_save, index=False)

    def output_bayplan_edi(self, path: str, baplie_delimiter: str, l_all_segments: list, s3_bucket: str) -> None:
        baplie_message = baplie_delimiter.join(l_all_segments) + baplie_delimiter
        if self.__is_local:
            self.write_file(baplie_message, path)
        else: 
            self.__write_file_to_s3(baplie_message, path, s3_bucket)
        # f_edi_result = open(path, "w")
        # f_edi_result.write(baplie_message)
        # f_edi_result.close()

    # def write_baplie_message(
    #     self, path: str, baplie_delimiter: str, header_segments_list: list, l_baplie_segments: list, tail_segments_list: list, csv_df: pd.DataFrame
    # ) -> None:
    #     """
    #     Takes the list of segments (header, body, and tail of the message), the dataframe that contains the slot positions from the output
    #     csv file of the CPLEX model, and generates a .edi file (the baplie message) with the slots positions from the slot planning results.

    #     Parameters
    #     ----------
    #     header_segments_list
    #         a list where every element is a segment that belongs to the header of the baplie message

    #     l_baplie_segments
    #         a list of lists (sbulists), where every sublist is a list of segments for a specific container
    #         and every segment in that sublist holds data for that specific container

    #     tail_segments_list
    #         a list containing the segments that belong to the tail of the message

    #     csv_df
    #         a pandas dataframe from the output csv file of the CPLEX model, i.e., the csv file that contains the optimized slots positions

    #     Returns
    #     -------
    #     None
    #     """
    #     slots_positions_results_list = csv_df["SLOT_POSITION"].astype(str).tolist()
    #     containers_ids_list = csv_df["REAL_CONTAINER_ID"].tolist()
    #     changed_existing_slot_positions_num = 0
    #     changed_empty_slot_positions_num = 0
    #     #TODO remove the two dicts and everything related to it
    #     containers_ids_with_changed_existing_slot_positions_dict = {"ID": []}
    #     containers_ids_with_changed_empty_slot_positions_dict = {"ID": []}
    #     baplie_message = baplie_delimiter.join(header_segments_list) + baplie_delimiter # will contain the header segments
    #     #TODO ask if i could clean the baplie (weird containers already taken care of in main_layer)
    #     for i, container_data in enumerate(l_baplie_segments):
    #         first_segment_split = container_data[0].split("+")

    #         EQD_segment = [segment for segment in container_data if segment[:3] == "EQD"]
    #         if len(EQD_segment):
    #             EQD_segment = EQD_segment[0]
    #         else:
    #             continue

    #         container_id = EQD_segment.split("+")[2].split(":")[0]

    #         if container_id in containers_ids_list:
    #             slot_position_in_segment_slice_list = first_segment_split[2].split(":")
                
    #             container_id_index = containers_ids_list.index(container_id)
    #             slot_position_result = slots_positions_results_list[container_id_index]

    #             if slot_position_in_segment_slice_list[0] != slot_position_result:
    #                 if slot_position_in_segment_slice_list[0] != "":
    #                     changed_existing_slot_positions_num += 1
    #                     containers_ids_with_changed_existing_slot_positions_dict["ID"].append(container_id)

    #                 else:
    #                     changed_empty_slot_positions_num += 1
    #                     containers_ids_with_changed_empty_slot_positions_dict["ID"].append(container_id)

    #                 slot_position_in_segment_slice_list[0] = slot_position_result
    #                 slot_position_in_segment_slice_joined = ":".join(slot_position_in_segment_slice_list)

    #                 first_segment_split[2] = slot_position_in_segment_slice_joined
    #                 first_segment_joined = "+".join(first_segment_split)

    #                 l_baplie_segments[i][0] = first_segment_joined

    #         baplie_message += baplie_delimiter.join(l_baplie_segments[i]) # body of the message
    #         baplie_message += baplie_delimiter
        
    #     baplie_message += baplie_delimiter.join(tail_segments_list) # tail of the message
    #     baplie_message += baplie_delimiter

    #     self.__logger.info(f"There are {changed_existing_slot_positions_num} containers modified on board...")
    #     self.__logger.info(f"There are {changed_empty_slot_positions_num} placed containers...")
        
    #     f_edi_result = open(path, "w")
    #     f_edi_result.write(baplie_message)
    #     f_edi_result.close()


    #     path_to_save = "/".join(path.split("/")[:-1])
    #     if changed_existing_slot_positions_num:
    #         self.save_dict_as_csv(containers_ids_with_changed_existing_slot_positions_dict, f"{path_to_save}/modified_on_board.csv")
        
    #     if changed_empty_slot_positions_num:
    #         self.save_dict_as_csv(containers_ids_with_changed_empty_slot_positions_dict, f"{path_to_save}/placed.csv")
