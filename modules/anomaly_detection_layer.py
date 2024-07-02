import pandas as pd
import random
import string
from datetime import datetime
from modules import common_helpers
import logging

class AnomalyDetectionLayer():
    def __init__(self, DL:object) -> None:
        self.__edi_file_name_baplie_type_map = {"onboard": "OnBoard.edi", "container": "LoadList.edi", "tank": "Tank.edi"}
        # data_anomalies: defined as a class attribute so it would not have to be kept passing around
        self.__l_errors = []
        self.__error_num = 0

        self.__warnings_count = 0 # used in __check_if_errors to keep the order of errors

    def __brt_num_to_str(self, brt_num: int) -> str:
        brt_str = str(brt_num)
        if len(brt_str) == 1: return "0" + brt_str
        return brt_str

    def __get_full_msg(self, first_half_msg: str="", second_half_msg: str="") -> str:
        return first_half_msg + second_half_msg
    
    def __add_single_anomaly(self, criticity: str, message: str, error_value: str, call_id: str="", file_type: str="", container_id: str="") -> None:
        self.__error_num += 1

        d_anomaly = {
            "errorNo": str(self.__error_num),
            "criticity": criticity,
        }

        if len(call_id): d_anomaly["callId"] = call_id
        if len(file_type): d_anomaly["fileType"] = file_type
        if len(container_id): d_anomaly["container_id"] = container_id

        d_anomaly["message"] = message
        d_anomaly["errorValue"] = error_value

        self.__l_errors.append(d_anomaly)
        
        if criticity == "Warning": self.__warnings_count += 1

    def __add_anomalies_from_list(
            self,
            l_err_container_ids: list,
            criticity: str,
            message: str,
            error_value: str,
            call_id: str,
            file_type: str
        ) -> None:
        for container_id in l_err_container_ids:
            self.__add_single_anomaly(criticity, message, error_value, call_id, file_type, container_id)


    def check_if_errors(self) -> None:
        error_message = '\n'
        l_errors_len = len(self.__l_errors)
        if l_errors_len and l_errors_len != self.__warnings_count:
            error_message += ('\n').join(str(error) for error in self.__l_errors) + '\n'
            raise Exception(error_message)


        
    # def check_if_errors(self) -> None:
        # l_errors_len = len(self.__l_errors)
        # if l_errors_len and l_errors_len != self.__warnings_count:
        #     raise Exception(f"{self.__l_errors}")
        
        ## For testing ##
        # if l_errors_len:
        #     l_errors = [ err for err in self.__l_errors if err["criticity"] != "Warning" ]
        #     if l_errors:
        #         for err in l_errors:
        #             print(err)
        #         raise Exception(f"BAAA333")

    def check_onboard_edi(self, onboard_call: str, call_id: str) -> None:
        if not len(onboard_call):
            criticity = "Error"
            message = f"Missing {self.__edi_file_name_baplie_type_map['onboard']} "
            error_value = "TBD"
            self.__add_single_anomaly(criticity, message, error_value, call_id)
    
    def check_folder_if_exists(self, folder_list:list, keyword:str, print_message:str="")->None:
        if keyword not in folder_list[0]:
            self.__add_single_anomaly(criticity="Error", error_value= "TBD", message=f"There no {keyword} in folder_lists {print_message}..", call_id="call_00")
            
    def check_first_call_tank_edi_file(self, file_list_in_dir: list, call_id: str) -> None:
        if "Tank.edi" not in file_list_in_dir:
            criticity = "Error"
            message = f"Missing {self.__edi_file_name_baplie_type_map['tank']}, cannot initiate simulation."
            error_value = "TBD"
            self.__add_single_anomaly(criticity, message, error_value, call_id)
            

    def check_loadlist_and_tank_edi_files(self, loadlist_flag: int, tank_flag: int, call_id: str) -> None:
        # the function self.__add_single_anomaly is called more than once because two of the errors can exist at once
        criticity = "Error"
        if not(loadlist_flag + tank_flag):
            message = f"Missing {self.__edi_file_name_baplie_type_map['container']} and {self.__edi_file_name_baplie_type_map['tank']}"
            error_value = "TBD"
            self.__add_single_anomaly(criticity, message, error_value, call_id)
        
        elif not loadlist_flag:
            message = f"Missing {self.__edi_file_name_baplie_type_map['container']}"
            error_value = "TBD"
            self.__add_single_anomaly(criticity, message, error_value, call_id)

        elif not tank_flag:
            message = f"Missing {self.__edi_file_name_baplie_type_map['tank']}"
            error_value = "TBD"
            self.__add_single_anomaly(criticity, message, error_value, call_id)
            
    def check_loadlist_beyond_first_call(self, loadlist_flag: int, call_id: str) -> None:
        # the function self.__add_single_anomaly is called more than once because two of the errors can exist at once
        criticity = "Warning"
        # criticity = "Error"
        if not(loadlist_flag):
            message = f"Missing {self.__edi_file_name_baplie_type_map['container']}"
            error_value = "TBD"
            self.__add_single_anomaly(criticity, message, error_value, call_id)
        
        
    #In case of multiple EQD in one LOC is found 
    def check_missing_new_container_header(self, l_baplie_segments: list, folder_name: str, file_type:str, logger: logging.Logger) -> None:
        l_baplie_segments_updated = []

        for segment in l_baplie_segments:
            eqd_indices = [i for i, item in enumerate(segment) if item.startswith("EQD")]
            
            if len(eqd_indices) > 1:
                # raise a warning
                criticity = "Warning"
                segment_invalid_ids = [segment[idx].split("+")[2].split(":")[0] for idx in eqd_indices]
                message = f"Containers {segment_invalid_ids} are under the same new container identifier."
                error_value = "TBD"
                self.__add_single_anomaly(criticity, message, error_value, folder_name, file_type)

                # create a new segment for each EQD in the current segment, adding the slot location to the EQD information
                eqd_indices.sort()
                for i, _ in enumerate(eqd_indices):
                    try:
                        logger.debug("added segment to list: ", [segment[0]] + segment[eqd_indices[i]:eqd_indices[i+1]])
                        l_baplie_segments_updated.append([segment[0]] + segment[eqd_indices[i]:eqd_indices[i+1]])
                    except IndexError:
                        logger.debug("added segment to list: ", [segment[0]] + segment[eqd_indices[i]:])
                        l_baplie_segments_updated.append([segment[0]] + segment[eqd_indices[i]:])
            else:
                # add initial segment into new segments list
                l_baplie_segments_updated.append(segment)
        return l_baplie_segments_updated

    def check_extracted_containers_num(self, containers_in_baplie_num: int, containers_extracted_num: int, call_id: str, file_type: str) -> None:
        containers_num_diff = containers_in_baplie_num - containers_extracted_num
        if containers_num_diff:
            criticity = "Error"
            message = f"The number of extracted containers is not equal to the number of actual containers in the baplie"
            error_value = "TBD"
            self.__add_single_anomaly(criticity, message, error_value, call_id, file_type)
    
    def check_baplie_types_compatibility(self, baplie_type_from_file_name: str, baplie_type_from_content: str, call_id: str) -> None:
        if baplie_type_from_file_name != baplie_type_from_content:
            edi_file_name_from_upload = self.__edi_file_name_baplie_type_map[baplie_type_from_file_name]
            edi_file_name_from_content = self.__edi_file_name_baplie_type_map[baplie_type_from_content]
            file_type = edi_file_name_from_content.split(".")[0]

            criticity = "Error"
            message = f"Uploaded a {edi_file_name_from_upload} edi in the place of the {edi_file_name_from_content}"
            error_value = "TBD"
            self.__add_single_anomaly(criticity, message, error_value, call_id, file_type)

    def fill_containers_missing_serial_nums(self, df: pd.DataFrame, call_id: str, file_type: str, baplie_type_from_file_name: str) -> pd.DataFrame:
        l_containers_ids = df["EQD_ID"].tolist()
        no_id_container_count = 0
        for i, container_id in enumerate(l_containers_ids):
            if container_id != container_id or container_id == "":
                no_id_container_count += 1
                l_containers_ids[i] = "STOW" + ''.join(random.choices( string.digits, k=7))
                # l_containers_ids[i] = f"{call_id}_CN_{no_id_container_count}"
        if no_id_container_count:
            criticity = "Warning"
            message = self.__get_full_msg("with a missing serial number")
            error_value = "TBD"
            self.__add_single_anomaly(criticity, message, error_value, call_id, file_type)

            df["EQD_ID"] = l_containers_ids

        return df
        
    def check_containers_with_no_identifier(self, df_attributes : pd.DataFrame, call_id : str) -> pd.DataFrame:
            
        df_weird_port_containers = df_attributes[df_attributes["EQD_TYPE_CODE_QUALIFIER"] == ""]
        df_weird_port_containers_len = len(df_weird_port_containers)
        if df_weird_port_containers_len:
            criticity = "Warning"
            message = self.__get_full_msg(f"{df_weird_port_containers_len} weird containers were found in {call_id}")
            error_value = "TBD"
            self.__add_single_anomaly(criticity, message, error_value, call_id)
            df_attributes = df_attributes[~(df_attributes["EQD_TYPE_CODE_QUALIFIER"] == "")]
        return df_attributes

    def check_containers_serial_nums_dups(self, df: pd.DataFrame, call_id: str, file_type: str) -> None:
        s_container_ids_dups = df["EQD_ID"].duplicated()
        l_container_id_dups = df[s_container_ids_dups]["EQD_ID"].tolist()
 
        generated_serial_numbers = []
        for i in df.loc[df['EQD_ID'].isin(l_container_id_dups)].index:
            serial_number = "STOW" + ''.join(random.choices( string.digits, k=7))
            generated_serial_numbers.append([i, df.iloc[i]["EQD_ID"] , serial_number, df.iloc[i]["LOC_147_ID"]])
            #FIXME:A value is trying to be set on a copy of a slice from a DataFrame.
            df.loc[i, "EQD_ID"] = serial_number
        concatenated = ["{} at slot position {} was renamed to the following S/N: {} ".format(inner_list[1], inner_list[3], inner_list[2]) for inner_list in generated_serial_numbers]
       
#reactivate later            
        if s_container_ids_dups.sum():
            criticity = "Warning"
            message = self.__get_full_msg("with a duplicated serial number")
            error_value = "TBD"
            self.__add_anomalies_from_list(concatenated, criticity, message, error_value, call_id, file_type)
        
        
    def check_and_handle_POLs_names(
            self,
            df: pd.DataFrame,
            d_port_name_to_seq_num: dict,
            call_port_name: str,
            call_port_seq_num: int,
            call_id: str,
            file_type: str    
        ) -> pd.DataFrame:

        l_df_POLs_names = df["LOC_9_LOCATION_ID"].tolist()
        l_df_container_ids = df["EQD_ID"].tolist()

        l_df_unique_POLs_names_all_alpha = set([ POL_name for POL_name in l_df_POLs_names if POL_name.isalpha() ])
        d_df_1st_4_chars_to_port_name_base = { port_name[:4]: port_name[:5] for port_name in l_df_unique_POLs_names_all_alpha }

        l_rot_port_names = list(d_port_name_to_seq_num.keys())
        d_rot_1st_4_chars_to_port_name_base = { port_name[:4]: port_name[:5] for port_name in l_rot_port_names }
        l_rot_port_names_1st_4_chars = list(d_rot_1st_4_chars_to_port_name_base.keys())

        empty_port_name_err_msg = self.__get_full_msg(second_half_msg="with an empty POL name")
        empty_port_name_err_val = "TBD"

        unallowed_fictive_port_name_err_msg = self.__get_full_msg(second_half_msg="with a POL containing 1 or more non-letter characters")
        unallowed_fictive_port_name_err_val = "TBD"

        port_name_len_err_msg = self.__get_full_msg(second_half_msg="with a POL name that does not consist of 5 letters")
        port_name_len_err_val = "TBD"

        fictive_port_name_not_in_rot_err_msg = self.__get_full_msg(second_half_msg="with a POL name not in the rotation and ends with a non-letter character (this case is only allowed for POLs in the rotation)")
        fictive_port_name_not_in_rot_err_val = "TBD"

        wrong_LL_POL_name_err_msg = self.__get_full_msg(second_half_msg="with a POL different than the loadlist port")
        wrong_LL_POL_name_err_val = "TBD"

        fictive_port_name_war_msg = self.__get_full_msg(second_half_msg="with a POL name containing a non-letter character as the last character (it should be a letter)")
        fictive_port_name_war_val = "TBD"
        
        l_mapped_POLs_names = []
        for POL_name, container_id in list(zip(l_df_POLs_names, l_df_container_ids)):
            # keep pol name as default not to throw an error when setting port names in dataframe
            mapped_POL_name = POL_name
            
            # a pol name should consist of 5 chars => don't want to test if len not 5
            len_port_name = len(POL_name)
            if not len_port_name: # if port is an empty string
                self.__add_single_anomaly("Error", empty_port_name_err_msg, empty_port_name_err_val, call_id, file_type, container_id)
                l_mapped_POLs_names.append(mapped_POL_name)
                continue
            #TODO implement rest of tree map (for PODs as well)
            if len_port_name != 5: # if is is not an empty string and diff than 5 chars
                self.__add_single_anomaly("Error", port_name_len_err_msg, port_name_len_err_val, call_id, file_type, container_id)
                l_mapped_POLs_names.append(mapped_POL_name)
                continue
            
            non_alpha_chars_count = sum([ 1 if not char.isalpha() else 0 for char in mapped_POL_name ])
            is_last_char_alpha = POL_name[-1].isalpha()
            # if pol name contains at least 1 numeric character but not at the last position or at last position and other positions (more than 1 numeric char)
            if non_alpha_chars_count and (is_last_char_alpha or non_alpha_chars_count > 1):
                self.__add_single_anomaly("Error", unallowed_fictive_port_name_err_msg, unallowed_fictive_port_name_err_val, call_id, file_type, container_id)
                l_mapped_POLs_names.append(mapped_POL_name)
                continue

            if not is_last_char_alpha: # add warning whether it is an onboard or a loadlist
                self.__add_single_anomaly("Warning", fictive_port_name_war_msg, fictive_port_name_war_val, call_id, file_type, container_id)
            
            # from now on, port name is 5 chars and either 5 alpha chars or (4 alpha chars and 1 digit (NOT OTHER NUMERIC) char)
            # remember, mapped_POL_name is POL_name by default
            POL_name_1st_4_chars = POL_name[:4] # do not care if the 5th char is a numeric or an alpha to map pol name
            
            if not call_port_seq_num: # if onboard
                if not is_last_char_alpha: # last char is not a letter
                    if POL_name_1st_4_chars in l_rot_port_names_1st_4_chars: # in rotation => map pol name from rotation
                        mapped_POL_name = d_rot_1st_4_chars_to_port_name_base[POL_name_1st_4_chars]

                    elif POL_name_1st_4_chars in l_df_unique_POLs_names_all_alpha: # in edi => map from edi
                        mapped_POL_name = d_df_1st_4_chars_to_port_name_base[POL_name_1st_4_chars]

                    elif "USLA" in POL_name:
                        mapped_POL_name = "USLAX"
                    
                    else: # not in rotation => no way to find out map pol name => throw error
                        # mapped_POL_name stays as default
                        self.__add_single_anomaly("Error", fictive_port_name_not_in_rot_err_msg, fictive_port_name_not_in_rot_err_val, call_id, file_type, container_id)

                # else => all alpha chars (it's ok) => keep default pol name (no need to do anything)

            else: # if LL
                if POL_name_1st_4_chars in call_port_name: # a LL can only have a POL name as the call port => map pol name
                    mapped_POL_name = call_port_name

                else: # not allowed => throw error
                    self.__add_single_anomaly("Error", wrong_LL_POL_name_err_msg, wrong_LL_POL_name_err_val, call_id, file_type, container_id)

            l_mapped_POLs_names.append(mapped_POL_name)

        df["LOC_9_LOCATION_ID"] = l_mapped_POLs_names

        return df

    def check_and_handle_PODs_names(
            self,
            df: pd.DataFrame,
            d_port_name_to_seq_num: dict,
            l_past_POLs_names: list,
            call_port_seq_num: int,
            call_id: str,
            file_type: str    
        ) -> pd.DataFrame:
        #TODO get list of unique correct POLs and PODs in all edis to help in mapping process
        l_df_PODs_names = df["LOC_11_LOCATION_ID"].tolist()
        l_df_container_ids = df["EQD_ID"].tolist()

        l_rot_port_names = list(d_port_name_to_seq_num.keys())
        d_rot_1st_4_chars_to_port_name_base = { port_name[:4]: port_name[:5] for port_name in l_rot_port_names }
        l_rot_port_names_1st_4_chars = list(d_rot_1st_4_chars_to_port_name_base.keys())

        d_past_POLs_1st_4chars_to_port_name_bse = { port_name[:4]: port_name[:5] for port_name in l_past_POLs_names }

        empty_port_name_err_msg = self.__get_full_msg(second_half_msg="with an empty POD name")
        empty_port_name_err_val = "TBD"

        unallowed_fictive_port_name_err_msg = self.__get_full_msg(second_half_msg="with a POD containing 1 or more non-letter characters")
        unallowed_fictive_port_name_err_val = "TBD"

        port_name_len_err_msg = self.__get_full_msg(second_half_msg="with a POD name that does not consist of 5 letters")
        port_name_len_err_val = "TBD"
        
        fictive_port_name_not_in_rot_err_msg = self.__get_full_msg(second_half_msg="with a POD name not in the rotation")
        fictive_port_name_not_in_rot_err_val = "TBD"

        fictive_not_1st_LL_port_name_err_msg = self.__get_full_msg(second_half_msg="in a loadlist that is not the first, with a POD name that contains non-letter characters, and not for USLAX")
        fictive_not_1st_LL_port_name_err_val = "TBD"

        fictive_port_name_war_msg = self.__get_full_msg(second_half_msg="with a POD name containing a non-letter character as the last character (it should be a letter)")
        fictive_port_name_war_val = "TBD"
        
        l_mapped_PODs_names = []
        for POD_name, container_id in list(zip(l_df_PODs_names, l_df_container_ids)):
            mapped_POD_name = POD_name

            len_port_name = len(POD_name)
            if not len_port_name:
                self.__add_single_anomaly("Error", empty_port_name_err_msg, empty_port_name_err_val, call_id, file_type, container_id)
                l_mapped_PODs_names.append(mapped_POD_name)
                continue
            
            if len_port_name != 5:
                self.__add_single_anomaly("Error", port_name_len_err_msg, port_name_len_err_val, call_id, file_type, container_id)
                l_mapped_PODs_names.append(mapped_POD_name)
                continue
            
            non_alpha_chars_count = sum([ 1 if not char.isalpha() else 0 for char in mapped_POD_name ])
            is_last_char_alpha = POD_name[-1].isalpha()
            if non_alpha_chars_count and (is_last_char_alpha or non_alpha_chars_count > 1):
                self.__add_single_anomaly("Error", unallowed_fictive_port_name_err_msg, unallowed_fictive_port_name_err_val, call_id, file_type, container_id)
                l_mapped_PODs_names.append(mapped_POD_name)
                continue

            if not is_last_char_alpha and POD_name[:4] != "USLA": # add warning whether it is an onboard or a loadlist
                self.__add_single_anomaly("Warning", fictive_port_name_war_msg, fictive_port_name_war_val, call_id, file_type, container_id)
            
            POD_name_1st_4_chars = POD_name[:4] 
            
            if call_port_seq_num in [0, 1]: # if onboard or 1st LL
                if POD_name_1st_4_chars in l_rot_port_names_1st_4_chars:
                    mapped_POD_name = d_rot_1st_4_chars_to_port_name_base[POD_name_1st_4_chars]

                else:
                    pass
                    #reactivate Error status
                    # self.__add_single_anomaly("Error", fictive_port_name_not_in_rot_err_msg, fictive_port_name_not_in_rot_err_val, call_id, file_type, container_id)
                    # self.__add_single_anomaly("Warning", fictive_port_name_not_in_rot_err_msg, fictive_port_name_not_in_rot_err_val, call_id, file_type, container_id)

            else: # LL other than 1st LL
                if not is_last_char_alpha:
                    if "USLA" in POD_name:
                        l_mapped_PODs_names.append(POD_name)
                        continue

                    elif POD_name_1st_4_chars in l_rot_port_names_1st_4_chars:
                        mapped_POD_name = d_rot_1st_4_chars_to_port_name_base[POD_name_1st_4_chars]
                    
                    elif POD_name_1st_4_chars in l_past_POLs_names:
                        mapped_POD_name = d_past_POLs_1st_4chars_to_port_name_bse[POD_name_1st_4_chars]
                    
                    else:
                        self.__add_single_anomaly("Error", fictive_not_1st_LL_port_name_err_msg, fictive_not_1st_LL_port_name_err_val, call_id, file_type, container_id)

            l_mapped_PODs_names.append(POD_name)

        df["LOC_11_LOCATION_ID"] = l_mapped_PODs_names

        return df

    # def check_and_handle_ports_names(
    #         self,
    #         df: pd.DataFrame,
    #         d_port_name_to_seq_num: dict,
    #         l_past_POLs_names: list,
    #         call_port_name: str,
    #         call_port_seq_num: int,
    #         call_id: str,
    #         file_type: str
    #     ) -> None:

    #     #TODO what if port has identical 1st 4 chars but diff 5th char? => error
        # call_port_name_base = call_port_name[:5]

        # l_port_names = list(d_port_name_to_seq_num.keys())
        # d_1st_4_chars_to_port_name_base = { port_name[:4]: port_name[:5] for port_name in l_port_names }
        # l_port_names_1st_4_chars = list(d_1st_4_chars_to_port_name_base.keys())

        # d_1st_4_chars_to_past_POL_name_base = { port_name[:4]: port_name[:5] for port_name in l_past_POLs_names }
        # l_past_POLs_names_1st_4_chars = list(d_1st_4_chars_to_past_POL_name_base.keys())
        
        # l_container_ids = df["EDQ_ID"].tolist()

        # l_ports_types = ["POL", "POD"]
        # for ports_type in l_ports_types:

        #     empty_port_name_err_msg = self.__get_full_msg(f"with an empty {ports_type} name")
        #     empty_port_name_err_val = "TBD"

        #     unallowed_fictive_port_name_err_msg = self.__get_full_msg(f"with a {ports_type} containing numeric characters")
        #     unallowed_fictive_port_name_err_val = "TBD"

        #     len_port_name_err_msg = self.__get_full_msg(f"with a {ports_type} name that does not consist of 5 letters")
        #     len_port_name_err_val = "TBD"

        #     fictive_port_name_war_msg = self.__get_full_msg(f"with a {ports_type} name containing a numeric character as the last character (it should be a letter)")
        #     fictive_port_name_war_val = "TBD"

        #     if ports_type == "POL":
        #         l_ports_names = df["LOC_9_LOCATION_ID"].tolist()

        #         fictive_port_name_err_msg = self.__get_full_msg(f"with a {ports_type} different than the loadlist port")
        #         fictive_port_name_err_val = "TBD"
            
        #     else:
        #         l_ports_names = df["LOC_11_LOCATION_ID"].tolist()

        #         fictive_port_name_err_msg = self.__get_full_msg(f"with a {ports_type} name not in the rotation")
        #         fictive_port_name_err_val = "TBD"
            
        #     l_mapped_ports_names = []
        #     for port_name, container_id in list(zip(l_ports_names, l_container_ids)):
        #         # keep port name as default not to throw an error when setting port names in dataframe
        #         mapped_port_name_base = port_name

        #         # a port name should consist of 5 chars => don't want to test if len not 5
        #         len_port_name = len(port_name)
        #         if not len_port_name: # if port is an empty string
        #             self.__add_single_anomaly("Error", empty_port_name_err_msg, empty_port_name_err_val, call_id, file_type, container_id)
        #             continue
                
        #         if len_port_name != 5: # if is is not an empty string and diff than 5 chars
        #             self.__add_single_anomaly("Error", len_port_name_err_msg, len_port_name_err_val, call_id, file_type, container_id)
        #             continue
                
        #         numeric_chars_count = sum([ 1 if char.isnumeric() else 0 for char in mapped_port_name_base ])
        #         last_char = port_name[-1]
        #         # if port name contains a numeric character anywhere but not at the last position or at last position and other positions or at last position but not a digit (e.g., ' or _)
        #         if numeric_chars_count and (is_last_char_alpha or not last_char.isdigit() or numeric_chars_count > 1):
        #             self.__add_single_anomaly("Error", unallowed_fictive_port_name_err_msg, unallowed_fictive_port_name_err_val, call_id, file_type, container_id)
        #             continue
                
        #         # from now on, port name is 5 chars and either 5 alpha chars or (4 alpha chars and 1 digit (NOT OTHER NUMERIC) char)
        #         port_name_1st_4_chars = port_name[:4]
        #         if port_name_1st_4_chars in l_port_names_1st_4_chars: # in rotation (not an empty port name)
        #             if "USLA" in port_name:
        #                 mapped_port_name_base = port_name
        #             else:
        #                 mapped_port_name_base = d_1st_4_chars_to_port_name_base[port_name_1st_4_chars]

        #             if port_name[-1].isdigit(): # last char is a num
        #                 self.__add_single_anomaly("Warning", fictive_port_name_war_msg, fictive_port_name_war_val, call_id, file_type, container_id)

        #             elif ports_type == "POL": # POLs in loadlist must match the call port
        #                 if call_port_seq_num and mapped_port_name_base != call_port_name_base:
        #                    self.__add_single_anomaly("Error", fictive_port_name_err_msg, fictive_port_name_err_val, call_id, file_type, container_id)
                
        #         else: # not in rotation
        #             mapped_port_name_base = port_name # keep port name as it is
                    

        #             if mapped_port_name_base.isnumeric(): # not an empty string
        #                 if mapped_port_name_base[-1].isnumeric(): # if it is the last char


                    # non_alpha_chars_count = sum([ 1 if not char.isalpha() else 0 for char in mapped_port_name_base ])
                    # if non_alpha_chars_count:
                    #     if non_alpha_chars_count == 1:
                    #         self.__add_single_anomaly("Warning", fictive_port_name_war_msg, fictive_port_name_war_val, call_id, file_type, container_id)

                    # if mapped_port_name_base.isalnum(): # contains at least one number that is not at the end
                    #     self.__add_single_anomaly("Error", unallowed_fictive_port_name_err_msg, unallowed_fictive_port_name_err_val, call_id, file_type, container_id)

                    # elif mapped_port_name_base == "":
                    #     self.__add_single_anomaly("Error", empty_port_name_err_msg, empty_port_name_err_val, call_id, file_type, container_id)
                    
                    # elif ports_type == "POD": # if port_name is not empty and do not contain a number
                    #     self.__add_single_anomaly("Error", fictive_port_name_err_msg, fictive_port_name_err_val, call_id, file_type, container_id)

                # at then end, we will have a mapped_port_name_base
                
                
        #         l_mapped_ports_names.append(mapped_port_name_base)
            
        #     if ports_type == "POL":
        #         df["LOC_9_LOCATION_ID"] = l_mapped_ports_names
        #     else:
        #         df["LOC_11_LOCATION_ID"] = l_mapped_ports_names
            
        # return df
    
        # l_err_container_ids = df[df["LOC_9_LOCATION_ID"] == ""]["EQD_ID"].tolist()
        # if len(l_err_container_ids):
        #     criticity = "Error"
        #     message = self.__get_full_msg("with an empty POL")
        #     error_value = "TBD"
        #     self.__add_anomalies_from_list(l_err_container_ids, criticity, message, error_value, call_id, file_type)

        # l_err_container_ids = df[df["LOC_11_LOCATION_ID"]==""]["EQD_ID"].tolist()
        # if len(l_err_container_ids):
        #     criticity = "Error"
        #     message = self.__get_full_msg("with an empty POD")
        #     error_value = "TBD"
        #     self.__add_anomalies_from_list(l_err_container_ids, criticity, message, error_value, call_id, file_type)

    def check_ISO_codes(self, df: pd.DataFrame, d_iso_codes_map: dict, call_id: str, file_type: str) -> None:
        d_old_iso_codes_size = d_iso_codes_map["old"]["length_ft"]
        d_old_iso_codes_height = d_iso_codes_map["old"]["height"]

        d_new_iso_codes_size = d_iso_codes_map["new"]["length_ft"]
        d_new_iso_codes_height = d_iso_codes_map["new"]["height"]
        
        l_iso_codes_in_baplie = df["EQD_SIZE_AND_TYPE_DESCRIPTION_CODE"].tolist()
        l_container_ids = df["EQD_ID"].tolist()

        for iso_code, container_id in zip(l_iso_codes_in_baplie, l_container_ids):
            if not len(iso_code):
                criticity = "Error"
                message = self.__get_full_msg("with a missing ISO code")
                error_value = "TBD"
                self.__add_single_anomaly(criticity, message, error_value, call_id, file_type, container_id)

            else:
                criticity = "Warning"
                        
            is_old = common_helpers.is_iso_code_old(iso_code)

            size_code = iso_code[0]
            height_code = iso_code[1]
#reactivate late 
            if is_old:
                if size_code not in d_old_iso_codes_size.keys() or height_code not in d_old_iso_codes_height.keys():
                    message = self.__get_full_msg("with an unknown old ISO code")
                    error_value = "TBD"
                    self.__add_single_anomaly(criticity, message, error_value, call_id, file_type, container_id)
            
            else:
                if size_code not in d_new_iso_codes_size.keys() or height_code not in d_new_iso_codes_height.keys():
                    message = self.__get_full_msg("with an unknown new ISO code")
                    error_value = "TBD"
                    self.__add_single_anomaly(criticity, message, error_value, call_id, file_type, container_id)
    
    # def check_PODs_not_in_rotation(self, df_attributes: pd.DataFrame, l_PODs_names: list, call_id: str, file_type: str) -> None:
    #     l_df_PODs_names = df_attributes["LOC_11_LOCATION_ID"].tolist()
    #     l_containers_ids = df_attributes["EQD_ID"].tolist()
        
    #     criticity = "Error"
    #     for container_id, POD_name in list(zip(l_containers_ids, l_df_PODs_names)):
    #         if POD_name != "" and POD_name not in l_PODs_names:
    #             message = self.__get_full_msg("with a POD that is not in the rotation")
    #             error_value = "TBD"
    #             self.__add_single_anomaly(criticity, message, error_value, call_id, file_type, container_id)
    
    def check_weights(self, df_attributes: pd.DataFrame, call_id: str, file_type: str) -> None:
        l_weights = df_attributes["Weight"].tolist() # the list is already processed: no nans or empty strings (only zeros for no values)
        l_containers_ids = df_attributes["EQD_ID"].tolist()
        
        criticity = "Error"
        message = self.__get_full_msg("with a weight less than 1 tonne or greater 100 tonnes")
        error_value = "TBD"
        for container_id, weight in list(zip(l_containers_ids, l_weights)):
            if weight < 1 or weight > 100: #TODO ask Ioan if weights < 1 or <= 0
                self.__add_single_anomaly(criticity, message, error_value, call_id, file_type, container_id)

    def check_dup_slots(self, df: pd.DataFrame, call_id: str, file_type: str) -> None:
        pds_filled_slots = df.iloc[:, 1]
        pds_dups_bool_mask = pds_filled_slots.duplicated(keep=False)
        dups_count = pds_dups_bool_mask.sum()
        
        if dups_count:
            criticity = "Warning"
            message = self.__get_full_msg("with the same slot positions")
            error_value = "TBD"

            l_err_container_ids = df[pds_dups_bool_mask]["EQD_ID"].tolist()
            self.__add_anomalies_from_list(l_err_container_ids, criticity, message, error_value, call_id, file_type)

    def check_empty_slots_in_OnBoard(self, df: pd.DataFrame, call_id: str, file_type: str) -> None:
        df_temp = df[df["LOC_147_ID"]==""]
        if len(df_temp):
            criticity = "Error"
            message = self.__get_full_msg("with no slot position in an OnBoard list")
            error_value = "TBD"
            l_err_container_ids = df_temp["EQD_ID"].tolist()
            self.__add_anomalies_from_list(l_err_container_ids, criticity, message, error_value, call_id, file_type)        

    def check_filled_slots_in_LLs(self, df: pd.DataFrame, call_id: str, file_type: str) -> None:
        df_temp = df[df["LOC_147_ID"]!=""]
## reactivate later
        if len(df_temp):
            criticity = "Warning"
            message = self.__get_full_msg("with a slot position in a loadlist")
            error_value = "TBD"
            l_err_container_ids = df_temp["EQD_ID"].tolist()
            self.__add_anomalies_from_list(l_err_container_ids, criticity, message, error_value, call_id, file_type)        

            df["LOC_147_ID"] = ""

        return df

    # def add_PODs_anomalies(self, l_err_container_ids: list, call_id: str, file_type: str) -> None:
    #     if len(l_err_container_ids):
    #         criticity = "Error"
    #         error_value = "TBD"
    #         message = self.__get_full_msg("with a POD not in the rotation nor a past POL from the previous rotation")

    #         self.__add_anomalies_from_list(l_err_container_ids, criticity, message, error_value, call_id, file_type)

    def check_flying_containers(
            self,
            d_container_info_by_bay_row_tier: dict,
            d_stacks_rows_by_bay_row_deck: dict,
            d_type_to_size_map: dict
        ) -> None:
        # criticity = "Error"
        criticity = "Warning"
        l_container_bay_row_tier_keys = list(d_container_info_by_bay_row_tier.keys())
        for i, container_tup in enumerate(l_container_bay_row_tier_keys):
            container_id = d_container_info_by_bay_row_tier[container_tup]["container_id"]
            
            # check for odd tier nums
            container_tier_num = int(container_tup[2])
            if container_tier_num % 2 != 0:
                error_value = "TBD"
                message = self.__get_full_msg("with an odd tier number")
                self.__add_single_anomaly(criticity, message, error_value, container_id=container_id)
                continue # if odd tier number => specifc case of wrong tier number and definitely not in d_stacks_rows_by_bay_row_deck => no need to go on with the checks

            # check if valid bay, row, and deck, i.e., valid position
            tup_in_stack = (container_tup[0], container_tup[1], d_container_info_by_bay_row_tier[container_tup]["hold_or_deck"])
            if tup_in_stack not in d_stacks_rows_by_bay_row_deck.keys():
                error_value = "TBD"
                message = self.__get_full_msg("with an invalid bay or/and row numbers, or an invalid on hold or on deck status")
                self.__add_single_anomaly(criticity, message, error_value, container_id=container_id)
                continue # otherwise, it will throw an error when accessing d_stacks_rows_by_bay_row_deck[tup_in_stack] right below

            stack_row_vals = d_stacks_rows_by_bay_row_deck[tup_in_stack]

            # check if valid tier number
            stack_first_tier_num = int(stack_row_vals["FirstTier"])
            stack_max_num_of_std_cont = int(stack_row_vals["MaxNbOfStdCont"])

            if container_tier_num < stack_first_tier_num or container_tier_num > stack_first_tier_num + 2 * (stack_max_num_of_std_cont - 1): # if invalid tier number
                # text1 = f"First tier lower bound: {first_tier_num}"
                # text2 = f"First tier upper bound: {first_tier_num + 2 * (max_num_of_std_cont - 1)}"
                # text3 = f"Tier number: {tier_num}"
                # print(f"{text1}\n{text2}\n{text3}\n")
                # invalid_tiers_num += 1
                error_value = "TBD"
                message = self.__get_full_msg("with an invalid tier number")
                self.__add_single_anomaly(criticity, message, error_value, container_id=container_id)
                continue # stop testing if the container has an invalid tier number

            ### FLYING CONTAINERS ###
            container_bay, container_row, container_hold_or_deck = container_tup[0], container_tup[1], tup_in_stack[2]
            container_bay_num, container_hold_or_deck_num = int(container_bay), int(container_hold_or_deck)
            container_type = d_container_info_by_bay_row_tier[container_tup]["container_type"]
            container_size = d_type_to_size_map[container_type]

            ## ON DECK ##
            if container_hold_or_deck_num: # if deck
                if container_tier_num == stack_first_tier_num:
                    continue # not flying container: not possible to have a container below it => skip tests (should I check if there is a container below it and raise an Exception?)
                
                # else => if tier number is not first tier
                container_below_tier = self.__brt_num_to_str(container_tier_num - 2) # same bay, same row, tier below (- 2)
                below_container_tup = (container_bay, container_row, container_below_tier)

                if below_container_tup not in l_container_bay_row_tier_keys:
                    # print(i)
                    # print(stack_first_tier_num)
                    # print(below_container_tup)
                    # print(container_tup, ":", d_container_info_by_bay_row_tier[container_tup])
                    # print(below_container_tup, ":", d_container_info_by_bay_row_tier[below_container_tup])
                    error_value = "TBD"
                    message = self.__get_full_msg("on deck, not in the first tier, and without a container below it")
                    self.__add_single_anomaly(criticity, message, error_value, container_id=container_id)
                    continue # otherwise, it will throw an error when accessing d_container_info_by_bay_row_tier[below_container_tup] right below (too much belows eh? :p)

                # if all the prior conditions are not met => valid position, not first tier, and there is a container below it => check for container sizes conditions on deck
                # print(container_id)
                # print(container_tup)
                # print(below_container_tup)
                below_container_type = d_container_info_by_bay_row_tier[below_container_tup]["container_type"]
                below_container_size = d_type_to_size_map[below_container_type]
                
                # on deck, only 20 ft containers above 20 ft containers, 40 ft containers above 40 ft containers, 40 ft containers above 45 ft containers and vice versa are allowed
                not_allowed_condition = container_size != below_container_size
                allowed_condition_one = container_size == "40" and below_container_size == "45"
                allowed_condition_two = container_size == "45" and below_container_size == "40"

                if not_allowed_condition and not allowed_condition_one and not allowed_condition_two:
                    # print(container_type, below_container_type)
                    error_value = "TBD"
                    message = self.__get_full_msg("on deck, placed on top of a container with a different type")
                    self.__add_single_anomaly(criticity, message, error_value, container_id=container_id)
                    # no need for continue here: it is the last if statements won't be reached unless all if statements above are not met

            ## ON HOLD ##
            else: # if hold
                if container_size == "40":
                    if container_tier_num == stack_first_tier_num:
                        stack_odd_slot_num = int(stack_row_vals["OddSlot"])

                        if not stack_odd_slot_num: # odd slot is 0 => no problem
                            continue
                        
                        # else => odd slot
                        container_below_tier = self.__brt_num_to_str(container_tier_num - 2)
                        container_prev_bay = self.__brt_num_to_str(container_bay_num - 1)
                        below_tier_prev_bay_odd_slot_container_tup = (container_prev_bay, container_row, container_below_tier) # previous bay (- 1), same row, deck belows (- 2)

                        container_next_bay = self.__brt_num_to_str(container_bay_num + 1)
                        below_tier_next_bay_odd_slot_container_tup = (container_next_bay, container_row, container_below_tier) # next bay (+ 1), same row, deck belows (- 2)

                        if below_tier_prev_bay_odd_slot_container_tup in l_container_bay_row_tier_keys:
                            below_prev_bay_odd_slot_container_type = d_container_info_by_bay_row_tier[below_tier_prev_bay_odd_slot_container_tup]["container_type"]
                            below_prev_bay_odd_slot_container_size = d_type_to_size_map[below_prev_bay_odd_slot_container_type]

                            if below_prev_bay_odd_slot_container_size == container_size:
                                error_value = "TBD"
                                message = self.__get_full_msg("that is a 40 ft, on hold, in the first tier, and above another 40 ft container that is in an odd slot in the previous bay")
                                self.__add_single_anomaly(criticity, message, error_value, container_id=container_id)

                        elif below_tier_next_bay_odd_slot_container_tup in l_container_bay_row_tier_keys:
                            below_next_bay_odd_slot_container_type = d_container_info_by_bay_row_tier[below_tier_next_bay_odd_slot_container_tup]["container_type"]
                            below_next_bay_odd_slot_container_size = d_type_to_size_map[below_next_bay_odd_slot_container_type]

                            if below_next_bay_odd_slot_container_size == container_size:
                                error_value = "TBD"
                                message = self.__get_full_msg("that is a 40 ft, on hold, in the first tier, and above another 40 ft container that is in an odd slot in the next bay")
                                self.__add_single_anomaly(criticity, message, error_value, container_id=container_id)

                        else:
                            error_value = "TBD"
                            message = self.__get_full_msg("that is a 40 ft, on hold, in the first tier, and there is not a 20 ft container below it in the next or previous bay")
                            self.__add_single_anomaly(criticity, message, error_value, container_id=container_id)
                    
                    else: # if tier number is not first tier
                        above_container_tier = self.__brt_num_to_str(container_tier_num + 2)
                        above_container_tup = (container_bay, container_row, above_container_tier)

                        if above_container_tup in l_container_bay_row_tier_keys:
                            above_container_type = d_container_info_by_bay_row_tier[above_container_tup]["container_type"]
                            above_container_size = d_type_to_size_map[above_container_type]
                            if above_container_size != container_size:
                                # print(above_container_tup)
                                error_value = "TBD"
                                message = self.__get_full_msg("that is a 40 ft, on hold, not in the first tier, and there is a 20 ft container above it")
                                self.__add_single_anomaly(criticity, message, error_value, container_id=container_id)

                        below_container_tier = self.__brt_num_to_str(container_tier_num - 2)
                        below_container_tup = (container_bay, container_row, below_container_tier)

                        container_prev_bay = self.__brt_num_to_str(container_bay_num - 1)
                        container_next_bay = self.__brt_num_to_str(container_bay_num + 1)
                        below_tier_prev_bay_container_tup = (container_prev_bay, container_row, below_container_tier)
                        below_tier_next_bay_container_tup = (container_next_bay, container_row, below_container_tier)

                        # if 40' below a 40'
                        if below_container_tup in l_container_bay_row_tier_keys:
                            below_container_type = d_container_info_by_bay_row_tier[below_container_tup]["container_type"]
                            below_container_size = d_type_to_size_map[below_container_type]

                            if below_container_size == container_size:
                                continue
                        
                        # if two 20's below a 40'
                        elif below_tier_prev_bay_container_tup in l_container_bay_row_tier_keys and below_tier_next_bay_container_tup in l_container_bay_row_tier_keys:
                            below_tier_prev_bay_container_type = d_container_info_by_bay_row_tier[below_tier_prev_bay_container_tup]["container_type"]
                            below_tier_prev_bay_container_size = d_type_to_size_map[below_tier_prev_bay_container_type]

                            if below_tier_prev_bay_container_size == container_size:
                                error_value = "TBD"
                                message = self.__get_full_msg("that is a 40 ft, on hold, not in the first tier, and there is another 40 ft container below it in the previous bay")
                                self.__add_single_anomaly(criticity, message, error_value, container_id=container_id)

                            below_tier_next_bay_container_type = d_container_info_by_bay_row_tier[below_tier_next_bay_container_tup]["container_type"]
                            below_tier_next_bay_container_size = d_type_to_size_map[below_tier_next_bay_container_type]

                            if below_tier_next_bay_container_size == container_size:
                                error_value = "TBD"
                                message = self.__get_full_msg("that is a 40 ft, on hold, not in the first tier, and there is another 40 ft container below it in the next bay")
                                self.__add_single_anomaly(criticity, message, error_value, container_id=container_id)
                        
                        # if no 40' nor two 20's below 40'
                        else:
                            error_value = "TBD"
                            message = self.__get_full_msg("that is a 40 ft, on hold, not in the first tier, and there is not another 40 ft container nor two 20 ft containers below it")
                            self.__add_single_anomaly(criticity, message, error_value, container_id=container_id)


    def check_reefer_containers_at_non_reefer_slots(self, l_reefers_at_non_reefer: list) -> None:
        if len(l_reefers_at_non_reefer):
            criticity = "Warning"
            error_value = "TBD"
            message = self.__get_full_msg("that is a reefer and placed in a non-reefer slot")
            for container_info in l_reefers_at_non_reefer:
                container_id = container_info[1]
                self.__add_single_anomaly(criticity, message, error_value, container_id=container_id)

    def check_distance_to_next_port_is_zero(self, port_name: list) -> None:
        criticity = "Warning"
        error_value = "TBD"
        message = self.__get_full_msg(f"Distance to next port from {port_name} is 0. Default FuelCost and min/max speed will then be set to 0.")
        self.__add_single_anomaly(criticity, message, error_value)


    def check_missing_ports(self, rotation_intermediate, cranes_csv) -> None:
        missing_ports = []

        for i, ter in enumerate(rotation_intermediate['ShortName'].tolist()):
            if ter not in cranes_csv['port'].tolist():
                missing_ports.append((cranes_csv['port'].tolist()[i], ter))

        if missing_ports:
            for port, ter in missing_ports:
                message = f"Port '{port}' not found for territory '{ter}'"
                self.__add_single_anomaly("Error", message, f"Missing Port: {port} in cranes referential_file", file_type="rotations_intermediate")
    
    def check_sim_with_referentials(self, port_codes_sim:list, port_codes_ref:list, service_line:str)->None:
        for i, port in enumerate(port_codes_sim):
                if port_codes_ref.count(port) == 0:
                    message = f"Port: {port} in simulation is not found in referential for service_line {service_line}..."
                    error_value = "TBD"
                    self.__add_single_anomaly("Warning", message, error_value, file_type=f"call_{i}")
                    # self.__add_single_anomaly("Error", message, error_value, file_type=f"call_{i}")
        self.check_if_errors()
            
    def no_matching_port_check(self, port:str, index: int):
        
        message = f"could not match port {port} to a port from referential..."
        error_value = "TBD"
        self.__add_single_anomaly("Warning", message, error_value, file_type=f"call_{index}")
        # self.__add_single_anomaly("Error", message, error_value, file_type=f"call_{index}")
        
    def check_out_of_order_ports(self, port_codes_ref:list, ref_port_index_list:list, referential_folder_name:list)-> list:
        filtered_list = [elem for elem in [i for i, port in enumerate(port_codes_ref)] if min(ref_port_index_list) <= elem <= max(ref_port_index_list) and elem in ref_port_index_list]
        # Compare elements at each index and create a set of non-matching elements
        # result_set = {elem1 for elem1, elem2 in zip(ref_port_index_list, filtered_list) if elem1 != elem2}
        result_set = [{elem1, elem2} for elem1, elem2 in zip(ref_port_index_list, filtered_list) if elem1 != elem2]
        # Convert the list of sets to a set of frozensets to get unique sets
        unique_sets = set(frozenset(s) for s in result_set)
        # self._AL.__add_single_anomaly()
        mismatching_ports = []
        for mismatch_set in unique_sets:
            elem1, elem2 = mismatch_set
            index1 = ref_port_index_list.index(elem1)
            index2 = ref_port_index_list.index(elem2)
            message = f"Port {referential_folder_name[elem1][-5:]} at call_{index1} and Port {referential_folder_name[elem2][-5:]} at call_{index2} do not have a matching call sequence order between rotation and rotation referential..."
            error_value = "TBD"
            # reactivate later
            # self.__add_single_anomaly("Error", message, error_value, file_type=f"call_{index1} & call_{index2}")
            self.__add_single_anomaly("Warning", message, error_value, file_type=f"call_{index1} & call_{index2}")
            mismatching_ports.append(message)
        return mismatching_ports
    
    def check_if_no_output_postprocess(self, files_in_output:str) -> None:
        if "output.csv" not in files_in_output:
            criticity = 'ERROR'
            message = "There are no 'output.csv' result from CPLEX..."
            error_value = "TBD"
            self.__add_single_anomaly(criticity, message, error_value, file_type='output.csv')
            self.check_if_errors()

    def check_dg_loaded_in_china(self, df: pd.DataFrame) -> None:
        df_filtered = df[(df["cDG"] != "") & (df["LoadPort"].str.startswith("CN")) & (df["Stowage"] == "HOLD")]
        if df_filtered.shape[0] > 0:
            dg_containers = df_filtered[["Container"]].values.flatten()
            criticity = "Warning"
            message = self.__get_full_msg(f"Some dangerous containers loaded in China were stowed on HOLD: {', '.join(dg_containers)}. Changing Stowage column to DECK.")
            error_value = "TBD"
            self.__add_single_anomaly(criticity, message, error_value, file_type="containers.csv")
            
    def validate_data(self, data_dict: dict) -> None:
        """
        Validates the data dictionary based on specific criteria.

        This function checks if the 'StdSpeed', 'Gmdeck', and 'MaxDraft' values in the data dictionary are numeric.
        It also verifies that the 'worldwide' and 'service' values consist of alphabetic characters only and that
        the 'worldwide' value is either 'UNRESTRICTED' or 'WORLDWIDE'.

        Parameters:
            data_dict (dict): The dictionary containing the data to be validated.

        Returns:
            bool: True if the data dictionary passes all validation checks, False otherwise.
        """

        for index, data_entry in data_dict.items():
        # Check if 'StdSpeed', 'Gmdeck', and 'MaxDraft' are numeric
                for key in ['StdSpeed', 'Gmdeck', 'MaxDraft']:
                    try:
                        float(data_entry[key])
                    except:
                        criticity = "Warning"
                        message = f"key {key} = '{data_entry[key]}'is not numeric..."
                        error_value = "TBD"
                        self.__add_single_anomaly(criticity, message, error_value, call_id=data_entry['CallFolderName'])
                
                for key in ['WindowStartTime', 'WindowEndTime']:
                    try:
                        datetime.strptime(data_entry[key], "%Y-%m-%d %H:%M:%S")
                    except:
                        criticity = "Warning"
                        message = f"key {key} = '{data_entry[key]}'cannot be converted to Datetime... of format YYYY-MM-DD HH:MM:SS"
                        error_value = "TBD"
                        self.__add_single_anomaly(criticity, message, error_value, call_id=data_entry['CallFolderName'])
                                
                if index == 0:    
                    # Check if 'worldwide' and 'service' are alphabetic
                    for key in ['worldwide']:
                            if not data_entry[key].isalpha():
                                criticity = "Error"
                                message = f"key {key} = '{data_entry[key]} in rotation.csv is not alphabetic..."
                                error_value = "TBD"
                                self.__add_single_anomaly(criticity, message, error_value, call_id=data_entry['CallFolderName'])
                    
                    # Check if 'worldwide' is either 'UNRESTRICTED' or 'WORLDWIDE'
                    if data_entry['worldwide'].upper() not in ['UNRESTRICTED', 'WORLDWIDE']:
                        criticity = "Error"
                        message = f"key worldwide = '{data_entry['worldwide']}' in rotation.csv is not 'UNRESTRICTED' or 'WORLDWIDE'..."
                        error_value = "TBD"
                        self.__add_single_anomaly(criticity, message, error_value, call_id=data_entry['CallFolderName'])


    def validate_dg_data(self, dg_loadlist: pd.DataFrame) -> None:
        """
        Validates the DataFrame based on specific criteria.

        This function checks if the 'StdSpeed', 'Gmdeck', and 'MaxDraft' columns in the DataFrame are numeric.
        It also verifies that the 'worldwide' and 'service' columns consist of alphabetic characters only and that
        the 'worldwide' column is either 'UNRESTRICTED' or 'WORLDWIDE'.

        Parameters:
            df (pd.DataFrame): The DataFrame containing the data to be validated.

        Returns:
            None
        """
        criticity = "Error"
        df = dg_loadlist.copy()
        # Check if 'StdSpeed', 'Gmdeck', and 'MaxDraft' columns are numeric
        numeric_columns = ['DGS_UNDG_ID_', 'DGS_HAZARD_ID_']
        column_names = ['UN Number', 'Class']
        for index, row in df.iterrows():
            for i, column in enumerate(numeric_columns):
                try:
                    float(row[column])
                except ValueError:
                    self.__add_single_anomaly(criticity, f"{column_names[i]} = '{row[column]}' is not numeric or does not exist...",
                                            "TBD", container_id=row['EQD_ID'])
            if row['DGS_HAZARD_CODE_VERSION_ID_'] == "":
                self.__add_single_anomaly(criticity, f"Ammendment Version is empty...",
                                            "TBD", container_id=row['EQD_ID'])
        self.check_if_errors()



                