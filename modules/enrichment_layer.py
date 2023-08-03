import pandas as pd
import logging

from modules import common_helpers

#TODO fix the logic of old functions to follow the index based method (like in new functions)

class EnrichmentLayer():
    def __init__(self, csv_cols_list: list, logger: logging.Logger) -> None:
        # self.dict_of_cols_values: a dictionary having the keys identical to the output CSV headers, and the values will be the identified
        # attributes in each segment from the baplie message, i.e., the values will be the columns of the CSV
        self.csv_cols_list = csv_cols_list
        self.dict_of_cols_values = {k: [] for k in self.csv_cols_list}
        self.__logger = logger

    def __fill_dict_of_cols_values_with_nans(self, missing_csv_cols_list: list) -> None:
        """
        Takes the missing csv headers for a container, and fills the dict of attributes for those headers with empty strings (NaNs) for
        that container (a row in the csv). The csv_cols_dict is used to extract the appropriate list of columns based on the missing
        csv headers prefixes.

        Parameters
        ----------
        missing_csv_cols_list
            a list that contains csv cols that are missing for a container

        Retruns
        -------
        None
        """
        for k in missing_csv_cols_list:
            self.dict_of_cols_values[k].append("")

    def __fill_dict_of_cols_values(self, vars_list: list, this_header_csv_cols_list: list) -> None:
        """
        Takes the available csv headers for a container, the variables list which holds the extracted attributes from a baplie header
        and fills the dictionary of attributes with the extracted attributes w.r.t the appropriate columns in the csv for that container.

        Parameters
        ----------
        vars_list
            a list that holds the extracted attributes from a baplie header

        this_header_csv_cols_list
            a list containing the csv cols for that baplie header

        Returns
        -------
        None
        """
        for i in range(len(this_header_csv_cols_list)):
            self.dict_of_cols_values[this_header_csv_cols_list[i]].append(vars_list[i])

    def __get_missing_csv_cols_in_container_data(self, present_csv_cols_list: list) -> list:
        """
        Gets the missing baplie segments that could be available for a container but not for another. Let us say there is a container A
        having a header that is not available in container B, then the extracted attributes from that header in the container B will be
        filled with NaNs as they are not available for container B. This is why we are getting the missing baplie header, if missing, for
        each container. The approach is to get to get the set of all unique available headers from the baplie message, then see the missing
        headers for a specific container w.r.t to that set. Finally, the headers are mapped to the csv cols.
        
        Parameters
        ----------
        present_csv_cols_list
            the list of available csv cols for a container
        
        Returns
        -------
            list
                a list containing the missing csv cols for that container
        """
        return [csv_col for csv_col in self.csv_cols_list if csv_col not in present_csv_cols_list]

    def __check_dict_and_convert_to_df(self, containers_data_list: list) -> pd.DataFrame:
        log = "Columns having a total num of rows different than total num of containers"
        flag = 0
        err_cols_list = []
        for k in self.dict_of_cols_values.keys():
            if len(self.dict_of_cols_values[k]) != len(containers_data_list):
                flag = 1
                err_cols_list.append((k, len(self.dict_of_cols_values[k])))
                
        if flag:
            self.__logger.info(f"{log}:")
            for el in err_cols_list:
                col, col_len = el[0], el[1]
                self.__logger.info(f"- {col}: {col_len}")

        else:
            self.__logger.info((f"{log}: None"))
            attributes_df = pd.DataFrame(self.dict_of_cols_values)

        return attributes_df

    def __get_attributes_df_container(self, containers_data_list: list, new_container_data_flag: str, csv_cols_dict: dict, d_segments_map: dict) -> pd.DataFrame:
        """
        Builds a dictionary of attributes where the keys are the output csv headers and the values for a key are a list containing
        values (the extracted attributes) belonging to that key (csv header, i.e., baplie header).

        Parameters
        ----------
        containers_data_list
            a list of lists (sbulists), where every sublist is a list of lines for a specific container
            and every segment in that sublist holds data for that specific container
        
        csv_cols_dict
            the dictionary that has the keys as the csv headers prefixes and the values are the csv headers w.r.t the csv headers prefixes

        Returns
        -------
        attributes_dict
            the dictionary where a key is a csv header and the value of that key is a list of values for that csv header
        """
        
        # for key in list(csv_cols_dict.keys()):
        #     if 'EQA_NAD' in key:
        #         new_key = key.replace('EQA_NAD', 'EQA_NAD_CF')
        #         csv_cols_dict[new_key] = csv_cols_dict.pop(key)

        EQD_private_sub_headers = [ sub_header for sub_header in d_segments_map["EQD"] if sub_header not in ["FTX", "RFF"] ]
        EQD_private_sub_headers.append("EQD")

        for container_data in containers_data_list:
            # setting counters for headers that might be repeated: some headers that might be repeated will not have a count as they can
            # be distinguished using an attribute from within the header
            LOC_identifier_FTX_count = 0
            LOC_identifier_RFF_count = 0

            LOC_GROUP8_TSR_count = 0
            LOC_GROUP8_TDT_count = 0

            EQD_RFF_count = 0
            EQD_FTX_count = 0

            DGS_count = 0

            # fill present lines
            present_csv_cols_list = []
            for segment in container_data:
                segment_header_name = segment[:3]
                segment_split = segment.split("+")[1:]

                if segment_header_name == "LOC":
                    # the index passed to the func below is 0 and not 1 cz segment_split is sliced starting from 1
                    csv_main_segment_header_prefix = common_helpers.get_compound_segment_header(segment_split, segment_header_name, 0)

                elif segment_header_name == "EQD":
                    csv_main_segment_header_prefix = "EQD"

                elif segment_header_name == "TMP":
                    csv_main_segment_header_prefix = "TMP"

                elif segment_header_name == "EQA":
                    csv_main_segment_header_prefix = "EQA"

                elif segment_header_name == "DGS":
                    csv_main_segment_header_prefix = "DGS"
                    DGS_count += 1
                
                elif segment_header_name == "CTA":
                    csv_main_segment_header_prefix = "CTA"

                elif segment_header_name == "CNT":
                    csv_main_segment_header_prefix = "CNT"

                ## LOC HEADERS ##
                if "LOC" in csv_main_segment_header_prefix:
                    if csv_main_segment_header_prefix == new_container_data_flag: # e.g., if csv_main_segment_header_prefix = LOC_147
                        # the first if statement is repeated for the 2 loc groups to not fill the attributes of the segment
                        # more than once as csv_main_segment_header_prefix might stay the same accross multiple iterations
                        # if csv_main_segment_header_prefix is not in present_csv_cols_list => not filled yet
                        # if csv_main_segment_header_prefix is in present_csv_cols_list => filled and no need to fill it again
                        if not any(csv_main_segment_header_prefix in present_csv_segment_header for present_csv_segment_header in present_csv_cols_list) and csv_main_segment_header_prefix in csv_cols_dict.keys():
                            present_csv_cols_list += csv_cols_dict[csv_main_segment_header_prefix]
                            self.__fill_attributes_dict_LOC_identifier(segment_split, csv_cols_dict[csv_main_segment_header_prefix])

                        elif segment_header_name == "FTX":
                            LOC_identifier_FTX_count += 1
                            csv_sub_segment_header = f"{csv_main_segment_header_prefix}_{segment_header_name}_{LOC_identifier_FTX_count}"
                            if not any(csv_sub_segment_header in present_csv_segment_header for present_csv_segment_header in present_csv_cols_list):
                                present_csv_cols_list += csv_cols_dict[csv_sub_segment_header]
                                self.__fill_attributes_dict_FTX(segment_split, csv_cols_dict[csv_sub_segment_header])

                        elif segment_header_name == "RFF":
                            LOC_identifier_RFF_count += 1
                            csv_sub_segment_header = f"{csv_main_segment_header_prefix}_{segment_header_name}_{LOC_identifier_RFF_count}"
                            if not any(csv_sub_segment_header in present_csv_segment_header for present_csv_segment_header in present_csv_cols_list):
                                present_csv_cols_list += csv_cols_dict[csv_sub_segment_header]
                                self.__fill_attributes_dict_RFF(segment_split, csv_cols_dict[csv_sub_segment_header])

                    else:
                        if not any(csv_main_segment_header_prefix in present_csv_segment_header for present_csv_segment_header in present_csv_cols_list):
                            present_csv_cols_list += csv_cols_dict[csv_main_segment_header_prefix]
                            self.__fill_attributes_dict_LOC_GROUP8(segment_split, csv_cols_dict[csv_main_segment_header_prefix])

                        if segment_header_name == "TSR":
                            LOC_GROUP8_TSR_count += 1
                            csv_sub_segment_header = f"{csv_main_segment_header_prefix}_{segment_header_name}_{LOC_GROUP8_TSR_count}"
                            if not any(csv_sub_segment_header in present_csv_segment_header for present_csv_segment_header in present_csv_cols_list):
                                present_csv_cols_list += csv_cols_dict[csv_sub_segment_header]
                                self.__fill_attributes_dict_TSR(segment_split, csv_cols_dict[csv_sub_segment_header])

                        elif segment_header_name == "TDT":
                            LOC_GROUP8_TDT_count += 1
                            csv_sub_segment_header = f"{csv_main_segment_header_prefix}_{segment_header_name}_{LOC_GROUP8_TDT_count}"
                            if not any(csv_sub_segment_header in present_csv_segment_header for present_csv_segment_header in present_csv_cols_list):
                                present_csv_cols_list += csv_cols_dict[csv_sub_segment_header]
                                self.__fill_attributes_dict_TDT(segment_split, csv_cols_dict[csv_sub_segment_header])

                ## EQD HEADERS ##
                if "EQD" == csv_main_segment_header_prefix:
                    if not any(csv_main_segment_header_prefix in present_csv_segment_header for present_csv_segment_header in present_csv_cols_list) and csv_main_segment_header_prefix in csv_cols_dict.keys():
                        present_csv_cols_list += csv_cols_dict[csv_main_segment_header_prefix]
                        self.__fill_attributes_dict_EQD(segment_split, csv_cols_dict[csv_main_segment_header_prefix])

                    # # the index passed to the func below is 0 and not 1 cz segment_split is sliced starting from 1
                    if segment_header_name == "NAD":
                        csv_sub_segment_header_suffix = common_helpers.get_compound_segment_header(segment_split, segment_header_name, 0)
                        csv_sub_segment_header = f"{csv_main_segment_header_prefix}_{csv_sub_segment_header_suffix}"
                        if not any(csv_sub_segment_header in present_csv_segment_header for present_csv_segment_header in present_csv_cols_list):
                            present_csv_cols_list += csv_cols_dict[csv_sub_segment_header]
                            self.__fill_attributes_dict_NAD(segment_split, csv_cols_dict[csv_sub_segment_header])

                    elif segment_header_name == "MEA":
                        csv_sub_segment_header_suffix = common_helpers.get_compound_segment_header(segment_split, segment_header_name, 1)
                        csv_sub_segment_header = f"{csv_main_segment_header_prefix}_{csv_sub_segment_header_suffix}"
                        if not any(csv_sub_segment_header in present_csv_segment_header for present_csv_segment_header in present_csv_cols_list):
                            present_csv_cols_list += csv_cols_dict[csv_sub_segment_header]
                            self.__fill_attributes_dict_MEA(segment_split, csv_cols_dict[csv_sub_segment_header])

                    elif segment_header_name == "HAN":
                        csv_sub_segment_header_suffix = common_helpers.get_compound_segment_header(segment_split, segment_header_name, 0)
                        csv_sub_segment_header = f"{csv_main_segment_header_prefix}_{csv_sub_segment_header_suffix}"
                        if not any(csv_sub_segment_header in present_csv_segment_header for present_csv_segment_header in present_csv_cols_list):
                            present_csv_cols_list += csv_cols_dict[csv_sub_segment_header]
                            self.__fill_attributes_dict_HAN(segment_split, csv_cols_dict[csv_sub_segment_header])

                    elif segment_header_name == "DIM":
                        csv_sub_segment_header_suffix = common_helpers.get_compound_segment_header(segment_split, segment_header_name, 0)
                        csv_sub_segment_header = f"{csv_main_segment_header_prefix}_{csv_sub_segment_header_suffix}"
                        if not any(csv_sub_segment_header in present_csv_segment_header for present_csv_segment_header in present_csv_cols_list):
                            present_csv_cols_list += csv_cols_dict[csv_sub_segment_header]
                            self.__fill_attributes_dict_DIM(segment_split, csv_cols_dict[csv_sub_segment_header])

                    elif segment_header_name == "RFF":
                        EQD_RFF_count += 1
                        csv_sub_segment_header = f"{csv_main_segment_header_prefix}_{segment_header_name}_{EQD_RFF_count}"
                        if not any(csv_sub_segment_header in present_csv_segment_header for present_csv_segment_header in present_csv_cols_list):
                            present_csv_cols_list += csv_cols_dict[csv_sub_segment_header]
                            self.__fill_attributes_dict_RFF(segment_split, csv_cols_dict[csv_sub_segment_header])

                    elif segment_header_name == "GDS":
                        csv_sub_segment_header_suffix = common_helpers.get_compound_segment_header(segment_split, segment_header_name, 0)
                        csv_sub_segment_header = f"{csv_main_segment_header_prefix}_{csv_sub_segment_header_suffix}"
                        if not any(csv_sub_segment_header in present_csv_segment_header for present_csv_segment_header in present_csv_cols_list):
                            present_csv_cols_list += csv_cols_dict[csv_sub_segment_header]
                            self.__fill_attributes_dict_GDS(segment_split, csv_cols_dict[csv_sub_segment_header])

                    elif segment_header_name == "FTX":
                        EQD_FTX_count += 1
                        csv_sub_segment_header = f"{csv_main_segment_header_prefix}_{segment_header_name}_{EQD_FTX_count}"
                        if not any(csv_sub_segment_header in present_csv_segment_header for present_csv_segment_header in present_csv_cols_list):
                            present_csv_cols_list += csv_cols_dict[csv_sub_segment_header]
                            self.__fill_attributes_dict_FTX(segment_split, csv_cols_dict[csv_sub_segment_header])

                ## TMP HEADERS ##
                if "TMP" == csv_main_segment_header_prefix:
                    if segment_header_name == "TMP" and segment != "TMP+2":
                        if not any(csv_main_segment_header_prefix in present_csv_segment_header for present_csv_segment_header in present_csv_cols_list) and csv_main_segment_header_prefix in csv_cols_dict.keys():
                            present_csv_cols_list += csv_cols_dict[csv_main_segment_header_prefix]
                            self.__fill_attributes_dict_TMP(segment_split, csv_cols_dict[csv_main_segment_header_prefix])

                    elif segment_header_name == "RNG":
                        csv_sub_segment_header = f"{csv_main_segment_header_prefix}_{segment_header_name}"
                        if not any(csv_sub_segment_header in present_csv_segment_header for present_csv_segment_header in present_csv_cols_list):
                            present_csv_cols_list += csv_cols_dict[csv_sub_segment_header]
                            self.__fill_attributes_dict_RNG(segment_split, csv_cols_dict[csv_sub_segment_header])

                    elif segment_header_name == "DTM":
                        csv_sub_segment_header_suffix = common_helpers.get_compound_segment_header(segment_split, segment_header_name, 0)
                        csv_sub_segment_header = f"{csv_main_segment_header_prefix}_{csv_sub_segment_header_suffix}"
                        if not any(csv_sub_segment_header in present_csv_segment_header for present_csv_segment_header in present_csv_cols_list):
                            present_csv_cols_list += csv_cols_dict[csv_sub_segment_header]
                            self.__fill_attributes_dict_DTM(segment_split, csv_cols_dict[csv_sub_segment_header])

                ## EQA HEADERS ##
                if "EQA" == csv_main_segment_header_prefix:
            
                    if not any(csv_main_segment_header_prefix in present_csv_segment_header for present_csv_segment_header in present_csv_cols_list) and csv_main_segment_header_prefix in csv_cols_dict.keys():
                        present_csv_cols_list += csv_cols_dict[csv_main_segment_header_prefix]
                        self.__fill_attributes_dict_EQA(segment_split, csv_cols_dict[csv_main_segment_header_prefix])

                    elif segment_header_name == "NAD":
                        csv_sub_segment_header_suffix = common_helpers.get_compound_segment_header(segment_split, segment_header_name, 0)
                        # csv_sub_segment_header = f"{csv_main_segment_header_prefix}_{csv_sub_segment_header_suffix}"
                        csv_sub_segment_header = f"{csv_main_segment_header_prefix}_NAD"
                        
                        if not any(csv_sub_segment_header in present_csv_segment_header for present_csv_segment_header in present_csv_cols_list):

                            present_csv_cols_list += csv_cols_dict[csv_sub_segment_header]
                            
                            self.__fill_attributes_dict_NAD(segment_split, csv_cols_dict[csv_sub_segment_header])
                
                ## DGS HEADERS ##
                if "DGS" == csv_main_segment_header_prefix:
                    csv_main_segment_header_prefix_temp = f"{csv_main_segment_header_prefix}_{DGS_count}"
                    if not any(csv_main_segment_header_prefix_temp in present_csv_segment_header for present_csv_segment_header in present_csv_cols_list) and csv_main_segment_header_prefix_temp in csv_cols_dict.keys():
                        present_csv_cols_list += csv_cols_dict[csv_main_segment_header_prefix_temp]
                        self.__fill_attributes_dict_DGS(segment_split, csv_cols_dict[csv_main_segment_header_prefix_temp])
                
                    elif segment_header_name == "ATT":
                        csv_sub_segment_header_suffix = common_helpers.get_compound_segment_header(segment_split, segment_header_name, 1)
                        csv_sub_segment_header = f"{csv_main_segment_header_prefix_temp}_{csv_sub_segment_header_suffix}"
                        
                        if not any(csv_sub_segment_header in present_csv_segment_header for present_csv_segment_header in present_csv_cols_list):
                            present_csv_cols_list += csv_cols_dict[csv_sub_segment_header]
                            self.__fill_attributes_dict_ATT(segment_split, csv_cols_dict[csv_sub_segment_header])

                    elif segment_header_name == "MEA":
                        csv_sub_segment_header_suffix = common_helpers.get_compound_segment_header(segment_split, segment_header_name, 1)
                        csv_sub_segment_header = f"{csv_main_segment_header_prefix_temp}_{csv_sub_segment_header_suffix}"
                        if not any(csv_sub_segment_header in present_csv_segment_header for present_csv_segment_header in present_csv_cols_list):
                            present_csv_cols_list += csv_cols_dict[csv_sub_segment_header]
                            self.__fill_attributes_dict_MEA(segment_split, csv_cols_dict[csv_sub_segment_header])

                    elif segment_header_name == "FTX":
                        csv_sub_segment_header = f"{csv_main_segment_header_prefix_temp}_{segment_header_name}"
                        if not any(csv_sub_segment_header in present_csv_segment_header for present_csv_segment_header in present_csv_cols_list):
                            present_csv_cols_list += csv_cols_dict[csv_sub_segment_header]
                            self.__fill_attributes_dict_FTX(segment_split, csv_cols_dict[csv_sub_segment_header])

                ## CTA HEADERS ##
                if "CTA" == csv_main_segment_header_prefix:
                    if not any(csv_main_segment_header_prefix in present_csv_segment_header for present_csv_segment_header in present_csv_cols_list) and csv_main_segment_header_prefix in csv_cols_dict.keys():
                        present_csv_cols_list += csv_cols_dict[csv_main_segment_header_prefix]
                        self.__fill_attributes_dict_CTA(segment_split, csv_cols_dict[csv_main_segment_header_prefix])

                    elif segment_header_name == "COM":
                        csv_sub_segment_header = f"{csv_main_segment_header_prefix}_{segment_header_name}"
                        if not any(csv_sub_segment_header in present_csv_segment_header for present_csv_segment_header in present_csv_cols_list):
                            present_csv_cols_list += csv_cols_dict[csv_sub_segment_header]
                            self.__fill_attributes_dict_COM(segment_split, csv_cols_dict[csv_sub_segment_header])

                ## CNT HEADERS ##
                if "CNT" == csv_main_segment_header_prefix:
                    if not any(csv_main_segment_header_prefix in present_csv_segment_header for present_csv_segment_header in present_csv_cols_list):
                        present_csv_cols_list += csv_cols_dict[csv_main_segment_header_prefix]
                        self.__fill_attributes_dict_CNT(segment_split, csv_cols_dict[csv_main_segment_header_prefix])

            # for every missing segment header, populate the columns (implemented in the form of lists)
            # that belong to that row identifier with an empty string
            missing_csv_cols_list = self.__get_missing_csv_cols_in_container_data(present_csv_cols_list)

            self.__fill_dict_of_cols_values_with_nans(missing_csv_cols_list)
        
        attributes_df = self.__check_dict_and_convert_to_df(containers_data_list)

        return attributes_df

    def __get_attributes_df_tank(self, containers_data_list: list, csv_cols_dict: dict) -> pd.DataFrame:
        for container_data in containers_data_list:
            if len(container_data) > 7:
                print(container_data)
            
            i = 0
            present_csv_cols_list = []
            for segment in container_data:
                segment_header_name = segment[:3]
                segment_split = segment.split("+")[1:]

                if segment_header_name == "LOC" and not i:
                    compound_segment_header_name = common_helpers.get_compound_segment_header(segment_split, segment_header_name, 0)
                    if not any(compound_segment_header_name in present_csv_segment_header for present_csv_segment_header in present_csv_cols_list) and compound_segment_header_name in csv_cols_dict.keys():
                        present_csv_cols_list += csv_cols_dict[compound_segment_header_name]
                        self.__fill_attributes_dict_LOC_identifier(segment_split, csv_cols_dict[compound_segment_header_name])
                    
                elif segment_header_name == "MEA":
                    compound_segment_header_name = common_helpers.get_compound_segment_header(segment_split, segment_header_name, 0)
                    if not any(compound_segment_header_name in present_csv_segment_header for present_csv_segment_header in present_csv_cols_list):
                        present_csv_cols_list += csv_cols_dict[compound_segment_header_name]
                        self.__fill_attributes_dict_MEA(segment_split, csv_cols_dict[compound_segment_header_name])

                elif segment_header_name == "DIM":
                    if not any(segment_header_name in present_csv_segment_header for present_csv_segment_header in present_csv_cols_list):
                            present_csv_cols_list += csv_cols_dict[segment_header_name]
                            self.__fill_attributes_dict_DIM(segment_split, csv_cols_dict[segment_header_name])

                elif segment_header_name == "FTX":
                    if not any(segment_header_name in present_csv_segment_header for present_csv_segment_header in present_csv_cols_list):
                        present_csv_cols_list += csv_cols_dict[segment_header_name]
                        self.__fill_attributes_dict_FTX(segment_split, csv_cols_dict[segment_header_name])

            missing_csv_cols_list = self.__get_missing_csv_cols_in_container_data(present_csv_cols_list)

            self.__fill_dict_of_cols_values_with_nans(missing_csv_cols_list)
        
        attributes_df = self.__check_dict_and_convert_to_df(containers_data_list)

        return attributes_df

    def get_attributes_df(self, containers_data_list: list, new_container_data_flag: str, csv_cols_dict: dict, d_segments_map: dict, baplie_type_from_content: str) -> pd.DataFrame:
        if baplie_type_from_content == "container":
            return self.__get_attributes_df_container(containers_data_list, new_container_data_flag, csv_cols_dict, d_segments_map)

        elif baplie_type_from_content == "tank":
            return self.__get_attributes_df_tank(containers_data_list, csv_cols_dict)

    def __fill_attributes_dict_LOC_identifier(self, segment_split: list, LOC_identifier_csv_cols_list: list) -> None:
        """
        This function is specific to identifying the values of the attributes included in the lines that start
        with LOC+147, i.e., the lines that identify a stowage location (p. 46 in the implementation guide of
        baplie, Version 3.1.1, D.13B).
        Note: whenever you see p. xx, we are referring to a page in the implementation guide mentioned above.

        Parameters
        ----------
        segment_split
            a list belonging to a segment starting with LOC+147, where the list is a slice version of the segment
            by the "+" delimiter

        LOC_identifier_csv_cols_list
            a list containing the keys (str values) representing the attributes by their respective header names
            in the output CSV (which are keys in attributes_dict) for a LOC+147 segment

        Returns
        -------
        None
        """
        # This explanation is only provided in this function as the general idea will be recurrent throughout
        # the entire code. As we are splitting every segment into a list of attributes by leveraging the "+" delimiter,
        # every element will be an attribute from that segment. The attribute might come in two forms:
        # 1- an attribute containing only 1 attribute, let us call them simple attributes (does not contain ":")
        # 2- an attribute consisting of more than 1 attribute, let us call them compound attributes (contains ":" as a delimiter between different attributes)
        # One could refer to the implementation guide for further details.
        # Hence, you will see in this code a split over the ":" delimiter or some logical test on that delimiter

        # LOC+147 segment has a maximum of 3 attributes:
        # - the first and last attribute are simple attributes, resp.
        # - the second attribute is a compound attribute
        vars_list = []

        # example of segment: 'LOC+147+:9711:5'
        # example of segment_split: ['LOC', '147', ':9711:5'])        
        for i, var in enumerate(segment_split):
            # the logic in the next if statementb holds because if there is no 'LOC+147' then this function
            # would not be called in the first place, so that if statement is certain as long as such segment exists
            if i == 0:
                vars_list.append(var)

            else: # when var is the complex attribute
                vals_list_temp = var.split(":")
                vals_list_temp_len = len(vals_list_temp)

                if vals_list_temp_len > 1:
                    vars_list += vals_list_temp
                    vars_list += ["" for i in range(4-vals_list_temp_len)]

                elif vals_list_temp_len == 1:
                    val = vals_list_temp[0]
                    val_len = len(vals_list_temp[0])

                    if val_len >= 7:
                        vars_list += [val, "", "", ""]

                    elif val_len <= 2:
                        vars_list += ["", val, "", ""]

                    elif val_len <= 6:
                        vars_list += ["", "", val, ""]

                    else:
                        vars_list += ["", "", "", val]

        self.__fill_dict_of_cols_values(vars_list, LOC_identifier_csv_cols_list)

    def __fill_attributes_dict_FTX(self, segment_split: list, FTX_csv_cols_list: list) -> None:
        vars_list = []
        for i, var in enumerate(segment_split):
            
            if i == 0:
                vars_list.append(var)

            elif i == 1:
                continue
            
            elif i == 2:
                vals = var.split(":")
                vals_len = len(vals)

                if vals_len == 3:
                    var_one = vals[0]
                    var_two = vals[1]
                    var_three = vals[2]

                elif vals_len == 2:
                    var_one = vals[0]
                    var_two = vals[1]
                    var_three = ""
                
                else:
                    var_two = ""

                    if vals[0].isdigit():
                        var_one = ""
                        var_three = vals[0]
                    else:
                        var_one = vals[0]
                        var_three = ""

                vars_list += [var_one, var_two, var_three]

            elif i == 3:
                vars_list.append(var.split(":")[0])

        if len(vars_list) < 5:
            vars_list += ["" for i in range(5-len(vars_list))]
        
        self.__fill_dict_of_cols_values(vars_list, FTX_csv_cols_list)

    def __fill_attributes_dict_RFF(self, segment_split: list, RFF_csv_cols_list: list) -> None:
        vals = segment_split[0].split(":")
        if len(vals) == 2:
            var_one = vals[0]
            var_two = vals[1]

        elif len(vals) == 1:
            if vals.isalpha():
                var_one = vals[0]
                var_two = ""

            else:
                var_one = ""
                var_two = vals[0]

        else:
            var_one = var_two = ""
        
        vars_list = [var_one, var_two]

        self.__fill_dict_of_cols_values(vars_list, RFF_csv_cols_list)

    def __fill_attributes_dict_LOC_GROUP8(self, segment_split: list, LOC_GROUP8_csv_cols_list: list) -> None:
        """
        This function is specific to identifying the values of the attributes included in the lines that start
        with LOC (with LOC belonging to segment group 8: different than LOC+147), i.e., segments to identify a
        geographical location related to a unit of equipment or un-containerized cargo (p. 68).
        """
        vars_list = []
        if len(segment_split) > 3:
            segment_split = segment_split[:3] # sanity check for unwanted values
        
        for i, var in enumerate(segment_split):
            if i == 0 or i == 1:
                vars_list.append(var)
                
            else:
                if len(var):                

                    vals = var.split(":")
                    vals_len = len(vals)
                    
                    if vals_len == 3:
                        var_one = vals[0]
                        var_two = vals[1]
                        var_three = vals[2]

                    elif vals_len == 2:

                        if vals[1].isdigit():
                            if len(vals[0]) == 5:
                                var_one = ""
                                var_two = vals[0]
                                
                            else:
                                var_one = vals[0]
                                var_two = ""
                                
                            var_three = vals[1]
                        
                        else:
                            var_one = vals[0]
                            var_two = vals[1]

                    else:
                        if vals[0].isdigit():
                            var_one = ""
                            var_two = ""
                            var_three = vals[0]
                        
                        elif len(vals[0]) == 5:
                            var_one = ""
                            var_two = vals[0]
                            var_three = ""
                        
                        else:
                            var_one = vals[0]
                            var_two = ""
                            var_three = ""
                
                else:
                    var_one = ""
                    var_two = ""
                    var_three = ""
                
                vars_list.append(var_one)
                vars_list.append(var_two)
                vars_list.append(var_three)

        if len(segment_split) < 3: # if the compound attribute does not exist
            for i in range(3):
                vars_list.append("") # add 3 empty strings as the compound attribute consists of 3 simple attributes

        self.__fill_dict_of_cols_values(vars_list, LOC_GROUP8_csv_cols_list)

    def __fill_attributes_dict_TSR(self, segment_split: list, TSR_csv_cols_list: list) -> None:
        var = segment_split[2]
        if len(var):
            vals = var.split(":")
            vals_len = len(vals)
            
            if vals_len == 3:
                var_one = vals[0]
                var_two = vals[1]
                var_three = vals[2]

            elif vals_len == 2:
                if vals[0].isdigit():
                    var_one = vals[0]
                    var_two = vals[1]
                    var_three = ""

                else:
                    var_one = ""
                    var_two = vals[0]
                    var_three = vals[1]
            
            else:
                if vals[0].isdigit() and len(vals[0]) == 1:
                    var_one = vals[0]
                    var_two = ""
                    var_three = ""

                elif vals[0].isdigit():
                    var_one = ""
                    var_two = ""
                    var_three = vals[0]

                else:
                    var_one = ""
                    var_two = ""
                    var_three = ""
        
        else:
            var_one = ""
            var_two = ""
            var_three = ""

        vars_list = [var_one, var_two, var_three]

        self.__fill_dict_of_cols_values(vars_list, TSR_csv_cols_list)

    def __fill_attributes_dict_TDT(self, segment_split: list, TDT_csv_cols_list) -> None:
        vars_list = []
        for i, var in enumerate(segment_split):
            if i in [0, 1, 8]:
                vars_list.append(var)

            elif i == 2:
                vals_list_temp = var.split(":")
                vals_list_temp_len = len(vals_list_temp)
                first_val = vals_list_temp[0]

                if vals_list_temp_len == 2:
                    var_one = first_val
                    var_two = first_val

                elif vals_list_temp[0].isdigit():
                    var_one = first_val
                    var_two = ""
                
                else:
                    var_one = ""
                    var_two = first_val

                vars_list += [var_one, var_two]

            elif i in [3, 4]:
                vals_list_temp = var.split(":")
                vals_list_temp_len = len(vals_list_temp)

                if vals_list_temp_len == 4:
                    var_one = vals_list_temp[0]
                    var_two = vals_list_temp[1]
                    var_three = vals_list_temp[2]
                    var_four = vals_list_temp[3]

                elif vals_list_temp_len == 3:
                    var_one = vals_list_temp[0]
                    var_two = vals_list_temp[1]
                    var_three = vals_list_temp[2]
                    var_four = ""

                elif vals_list_temp_len == 2:
                    var_one = vals_list_temp[0]
                    var_two = vals_list_temp[1]
                    var_three = ""
                    var_four = ""
                    
                else:
                    var_one = ""
                    var_two = ""
                    var_three = ""
                    var_four = vals_list_temp[0] # supposing to be the last as it the description is the most general if only one variable

                vars_list += [var_one, var_two, var_three, var_four]

            elif i == 6:
                vals_list_temp = var.split(":")
                vals_list_temp_len = len(vals_list_temp)

                if vals_list_temp_len == 3:
                        var_one = vals_list_temp[0]
                        var_two = vals_list_temp[1]
                        var_three = vals_list_temp[2]

                elif vals_list_temp_len == 2:
                    var_one = vals_list_temp[0]
                    var_two = vals_list_temp[1]
                    var_three = ""

                else:
                    var_one = vals_list_temp[0]
                    var_two = ""
                    var_three = ""
                
                vars_list += [var_one, var_two, var_three]

            elif i == 7:
                vals_list_temp = var.split(":")
                vals_list_temp_len = len(vals_list_temp)

                if vals_list_temp_len == 5:
                    var_one = vals_list_temp[0]
                    var_two = vals_list_temp[1]
                    var_three = vals_list_temp[2]
                    var_four = vals_list_temp[3]
                    var_five = vals_list_temp[4]

                elif vals_list_temp_len == 4:
                    var_one = vals_list_temp[0]
                    var_two = vals_list_temp[1]
                    var_three = vals_list_temp[2]
                    var_four = vals_list_temp[3]
                    var_five = ""

                elif vals_list_temp_len == 3:
                    var_one = vals_list_temp[0]
                    var_two = vals_list_temp[1]
                    var_three = vals_list_temp[2]
                    var_four = ""
                    var_five = ""

                elif vals_list_temp_len == 2:
                    var_one = vals_list_temp[0]
                    var_two = vals_list_temp[1]
                    var_three = ""
                    var_four = ""
                    var_five = ""

                else:
                    val = vals_list_temp[0]
                    val_len = len(val)

                    if val.isalnum():
                        var_one = val
                        var_two = ""
                        var_three = ""
                        var_four = ""
                        var_five = ""

                    elif val.isnumeric():
                        var_two = ""
                        var_four = ""
                        var_five = ""

                        if val_len <= 3:
                            var_one = ""
                            var_three = val
                        else:
                            var_one = val
                            var_three = ""

                    else:
                        var_one = ""
                        var_three = ""

                        if val in ["CALLSIGN", "IMO"]:
                            var_two = val
                            var_four = ""
                            var_five = ""

                        elif val_len <= 3:
                            var_two = ""
                            var_four = ""
                            var_five = val

                        else:
                            var_two = ""
                            var_four = val
                            var_five = ""

                vars_list += [var_one, var_two, var_three, var_four, var_five]

        if len(vars_list) < 21:
            vars_list += ["" for i in range(21-len(vars_list))]

        self.__fill_dict_of_cols_values(vars_list, TDT_csv_cols_list)

    def __fill_attributes_dict_EQD(self, segment_split: list, EQD_csv_cols_list: list) -> None:
        """
        This function is specific to identifying the values of the attributes included in the lines that start
        with EQD, i.e., the lines that identify equipment details (p. 51).
        Parameters and returns are not mentioned becuase the functionality of this function is equivalent to
        __get_LOC_identifier_vals() but for segments starting with EQD. The same applies to similar functions where
        one could identify such functions from the naming.
        """
        vars_list = []
        for i in range(len(segment_split)):
            var = segment_split[i]

            if i == 1 or i == 2:
                vals = var.split(":")
                vals_len = len(vals)
                if vals_len == 3: # there are 2 ":"
                    var_one = vals[0]
                    var_two = vals[1]
                    var_three = vals[2]

                elif vals_len == 2: # there is 1 ":"
                    second_val_len = len(vals[1])

                    # if var_three exists -> length of second val is 1
                    # based on the implementation guide, it is assumed to come last
                    if second_val_len == 1:
                        var_three = vals[1]
                        # if the first value is for var_two cz in both complex variables the second value is composed of digits only
                        if vals[0].isdigit() or len(vals[0]) <= 4:
                            var_one = ""
                            var_two = vals[0]

                        else: # otherwise (if the first value is for var_one)
                            var_one = vals[0]
                            var_two = ""

                    else: # if var_three does not exist
                        var_one = vals[0]
                        var_two = vals[1]
                        var_three = ""

                else: # there are no ":" -> vals_len = 1 if vals (result of var.split(":")) contains one value or an empty string
                    val_len = len(vals[0])
                    # if there is a value and var_three exists
                    if val_len == 1:
                        var_one = ""
                        var_two = ""
                        var_three = vals[0]

                    # if there is a value and it is for var_two
                    elif vals[0].isdigit() or len(vals[0]) <= 4:
                        var_one = ""
                        var_two = vals[0]
                        var_three = ""

                    # if there is an empty string
                    # or if there is a value and it is for var_one
                    else:
                        var_one = vals[0] # if empty string, vals[0] will be empty string
                        var_two = ""
                        var_three = ""

                vars_list.append(var_one)
                vars_list.append(var_two)
                vars_list.append(var_three)

            else:
                vars_list.append(var)

        self.__fill_dict_of_cols_values(vars_list, EQD_csv_cols_list)

    def __fill_attributes_dict_NAD(self, segment_split: list, this_nad_csv_cols_list: list) -> None:
        """
        This function is specific to identifying the values of the attributes included in the lines that start
        with NAD, i.e., the lines that identify identify parties related to a unit of equipment or un-containerized
        cargo (including the operator) (p. 55).
        """
        vars_list = []
        for i in range(len(segment_split)):
            var = segment_split[i]

            if i == 1:
                vals = var.split(":")
                vals_len = len(vals)

                if vals_len == 3: # there are 2 ":"
                    var_one = vals[0]
                    var_two = vals[1]
                    var_three = vals[2]

                elif vals_len == 2: # there is 1 ":"

                    # if second val consists of digits
                    # based on the implementation guide, it is the only information that consists of digits
                    if vals[1].isdigit():
                        var_three = vals[1]
                        # if the first value has a length different than 3 -> it is the code list identification code and not the party identifier
                        if len(vals[0]) != 3:
                            var_one = ""
                            var_two = vals[0]

                        else: # otherwise, the first value is the party identifier
                            var_one = vals[0]
                            var_two = ""

                    else: # if var_three does not exist
                        var_one = vals[0]
                        var_two = vals[1]
                        var_three = ""

                else: # there are no ":" -> vals_len = 1 if vals (result of var.split(":")) contains one value or an empty string
                    # if there value consists of digits
                    if vals[0].isdigit():
                        var_one = ""
                        var_two = ""
                        var_three = vals[0]

                    # if there is a value and it is for var_two
                    elif len(vals[0]) != 3:
                        var_one = ""
                        var_two = vals[0]
                        var_three = ""

                    # if there is an empty string
                    # or if there is a value and it is for var_one
                    else:
                        var_one = vals[0] # if empty string, vals[0] will be empty string
                        var_two = ""
                        var_three = ""

                # the following is done to keep the every attribute at the right index in vars_list
                # if CF, then the NAD segment is for the container operator
                if vars_list[0] == "CF":
                    vars_list.append(var_one)
                    vars_list.append(var_two)
                    vars_list.append(var_three)

                    # no info about the 4 fields for the slot owner
                    for i in range(4):
                        vars_list.append("")

                # else, i.e., if GF, then the NAD segment is for the slot owner
                else:
                    # no info about the 4 fields for the container operator
                    for i in range(4):
                        vars_list.append("")

                    vars_list.append(var_one)
                    vars_list.append(var_two)
                    vars_list.append(var_three)

            else:
                vars_list.append(var)

        self.__fill_dict_of_cols_values(vars_list, this_nad_csv_cols_list)

    def __fill_attributes_dict_MEA(self, segment_split: list, this_mea_csv_cols_list: list) -> None:
        """
        This function is specific to identifying the values of the attributes included in the lines that start
        with MEA, i.e., the lines that specify weight or other measurements related to a unit of equipment
        or un-containerized cargo (p. 57).
        """
        vars_list = []
        for i in range(len(segment_split)):
            
            var = segment_split[i]

            if (i == 1 and var == "BRK") or i == 2:
                vals = var.split(":")
                vals_len = len(vals)

                if vals_len == 2: # there is 1 ":"
                    var_one = vals[0]
                    var_two = vals[1]

                elif vals[0].isdigit(): # there is no ":"
                    var_one = ""
                    var_two = vals[0]

                else:
                    var_one = vals[0] # does not matter if vals[0] holds a value or if it is an empty string, var_one will hold its value in both cases
                    var_two = ""

                vars_list.append(var_one)
                vars_list.append(var_two)

            else:
                vars_list.append(var)    

        self.__fill_dict_of_cols_values(vars_list, this_mea_csv_cols_list)

    def __fill_attributes_dict_HAN(self, segment_split: list, HAN_csv_cols_list: list) -> None:
        var = segment_split[0]
        if len(var):
            vals = var.split(":")
            vals_len = len(vals)
            
            if vals_len == 4:
                var_one = vals[0]
                var_two = vals[1]
                var_three = vals[2]
                var_four = vals[3]

            elif vals_len == 3:
                if len(vals[0]) == 3:
                    var_one = vals[0]
                    var_two = vals[1]
                    var_three = vals[2]
                    var_four = ""

                else:
                    var_one = ""
                    var_two = vals[0]
                    var_three = vals[1]
                    var_four = vals[2]

            elif vals_len == 2:
                if len(vals[0]) == 3:
                    var_one = vals[0]
                    var_two = vals[1]
                    var_three = ""
                    var_four = ""

                elif vals[0].isdigit():
                    var_one = ""
                    var_two = ""
                    var_three = vals[1]
                    var_four = vals[2]
                
                else:
                    var_one = ""
                    var_two = vals[0]
                    var_three = vals[1]
                    var_four = ""
            
            else:
                if vals[0].isdigit() and len(vals[0]) == 3:
                    var_one = ""
                    var_two = ""
                    var_three = vals[0]
                    var_four = ""

                elif len(vals[0] == 3):
                    var_one = vals[0]
                    var_two = ""
                    var_three = ""
                    var_four = ""

                else:
                    var_one = ""
                    var_two = ""
                    var_three = ""
                    var_four = ""

        
        else:
            var_one = ""
            var_two = ""
            var_three = ""
            var_four = ""

        vars_list = [var_one, var_two, var_three, var_four]

        self.__fill_dict_of_cols_values(vars_list, HAN_csv_cols_list)

    def __fill_attributes_dict_DIM(self, segment_split: list, DIM_csv_cols_list: list) -> None:
        line_split_len = len(segment_split)
        if line_split_len:
            if line_split_len == 2:
                code = [segment_split[0]]
                vals = segment_split[1].split(":")
            
            else:
                if segment_split[0].isdigit():
                    code = segment_split
                    vals = ["" for i in range(4)]

                else:
                    code = [""]
                    vals = segment_split[0].split(":")
            
            if all("" == val for val in vals):
                vars_list = code + vals

            else:
                vals_len = len(vals)

                if vals_len:
                    if vals_len == 4:
                        var_one = vals[0]
                        var_two = vals[1]
                        var_three = vals[2]
                        var_four = vals[3]

                    elif vals_len == 3:
                        if vals[0].isalpha():
                            var_one = vals[0]
                            var_two = vals[1]
                            var_three = vals[2]
                            var_four = ""

                        else:
                            var_one = ""
                            var_two = vals[0]
                            var_three = vals[1]
                            var_four = vals[2]

                    elif vals_len == 2:
                        if vals[0].isalpha():
                            var_one = vals[0]
                            var_two = ""
                            var_three = ""
                            var_four = vals[1]

                        else:
                            var_one = ""
                            var_two = ""
                            var_three = ""
                            var_four = vals[1]

                    else:
                        if vals[0].isalpha():
                            var_one = vals[0]
                            var_two = ""
                            var_three = ""
                            var_four = ""

                        else:
                            var_one = vals[0]
                            var_two = ""
                            var_three = ""
                            var_four = ""
                else:
                    var_one = ""
                    var_two = ""
                    var_three = ""
                    var_four = ""

        else:
            code = [""]
            var_one = ""
            var_two = ""
            var_three = ""
            var_four = "" 

        vars_list = code + [var_one, var_two, var_three, var_four]

        self.__fill_dict_of_cols_values(vars_list, DIM_csv_cols_list)

    def __fill_attributes_dict_GDS(self, segment_split: list, GDS_csv_cols_list: list) -> None:
        vals = segment_split[0].split(":")
        vals_len = len(vals)
        if vals_len:
            if vals_len == 3:
                var_one = vals[0]
                var_two = vals[1]
                var_three = vals[2]

            elif vals_len == 2:
                second_val_len = len(vals[1])

                if second_val_len == 1:
                    if len(vals[0]) > 6:
                        var_one = ""
                        var_two = vals[0]
                    else:
                        var_one = vals[0]
                        var_two = ""

                    var_three = vals[1]

                else:
                    var_one = vals[0]
                    var_two = vals[1]
                    var_three = ""
            
            else:
                firs_val_len = len(vals[0])
                if firs_val_len == 1:
                    var_one = ""
                    var_two = ""
                    var_three = vals[0]

                elif len(vals[0]) > 6:
                    var_one = ""
                    var_two = vals[0]
                    var_three = ""
                
                else:
                    var_one = vals[0]
                    var_two = ""
                    var_three = ""
        
        else:
            var_one = ""
            var_two = ""
            var_three = ""

        vars_list = [var_one, var_two, var_three]

        self.__fill_dict_of_cols_values(vars_list, GDS_csv_cols_list)

    def __fill_attributes_dict_TMP(self, segment_split: list, TMP_csv_cols_list: list) -> None:
        filled_flag = 0
        line_split_len = len(segment_split)
        if line_split_len:
            if line_split_len == 2:
                code = [segment_split[0]]
                vals = segment_split[1].split(":")

            else:
                if segment_split[0].isdigit():
                    code = [segment_split[0]]
                    vals = ["" for i in range(2)]

                else:
                    code = [""]
                    vals = segment_split[0].split(":")

            if all("" == val for val in vals) and len(vals) == 2:
                vars_list = code + vals
                self.__fill_dict_of_cols_values(vars_list, TMP_csv_cols_list)
                filled_flag = 1

            else:
                vals_len = len(vals)

                if vals_len == 2:
                    var_one = vals[0]
                    var_two = vals[1]

                else:
                    if vals[0].isalpha():
                        var_one = ""
                        var_two = vals[0]
                    
                    elif vals[0].isnumeric():
                        var_one = vals[0]
                        var_two = ""

                    else:
                        var_one = ""
                        var_two = ""
        
        else:
            code = [""]
            var_one = ""
            var_two = ""

        if not filled_flag:
            vars_list = code + [var_one, var_two]
            self.__fill_dict_of_cols_values(vars_list, TMP_csv_cols_list)

    def __fill_attributes_dict_ATT(self, segment_split: list, ATT_csv_cols_list: list) -> None:
        vars_list = []
        for i, var in enumerate(segment_split):
            if i == 0:
                vars_list.append(var)

            elif i == 1:
                vals_list_temp = var.split(":")
                vals_list_temp_len = len(vals_list_temp)

                if vals_list_temp_len == 3:
                    var_one = vals_list_temp[0]
                    var_two = vals_list_temp[1]
                    var_three = vals_list_temp[2]

                elif vals_list_temp_len == 2:
                    var_one = vals_list_temp[0]
                    var_two = vals_list_temp[1]
                    var_three = ""

                else:
                    val = vals_list_temp[0]

                    if val.isnumeric():
                        var_one = ""
                        var_two = val
                        var_three = ""

                    elif len(val) == 3:
                        var_one = val
                        var_two = ""
                        var_three = ""

                    else:
                        var_one =- ""
                        var_two = ""
                        var_three = val

                vars_list += [var_one, var_two, var_three]

            elif i == 2:
                vals_list_temp = var.split(":")
                vals_list_temp_len = len(vals_list_temp)

                if vals_list_temp_len == 4:
                    var_one = vals_list_temp[0]
                    var_two = vals_list_temp[1]
                    var_three = vals_list_temp[2]
                    var_four = vals_list_temp[3]

                elif vals_list_temp_len == 3:
                    var_one = vals_list_temp[0]
                    var_two = vals_list_temp[1]
                    var_three = vals_list_temp[2]
                    var_four = ""

                elif vals_list_temp_len == 2:
                    var_one = vals_list_temp[0]
                    var_two = vals_list_temp[1]
                    var_three = ""
                    var_four = ""
                    
                else:
                    var_one = ""
                    var_two = ""
                    var_three = ""
                    var_four = vals_list_temp[0]

                vars_list += [var_one, var_two, var_three, var_four]

        self.__fill_dict_of_cols_values(vars_list, ATT_csv_cols_list)   

    def __fill_attributes_dict_RNG(self, segment_split: list, RNG_csv_cols_list: list) -> None:
        filled_flag = 0
        line_split_len = len(segment_split)
        if line_split_len:
            if line_split_len == 2:
                code = [segment_split[0]]
                vals = segment_split[1].split(":")

            else:
                if segment_split[0].isdigit():
                    code = [segment_split[0]]
                    vals = ["" for i in range(2)]

                else:
                    code = [""]
                    vals = segment_split[0].split(":")

            if all("" == val for val in vals):
                vars_list = code + vals
                self.__fill_dict_of_cols_values(vars_list, RNG_csv_cols_list)
                filled_flag = 1

            else:
                vals_len = len(vals)

                if vals_len == 3:
                    var_one = vals[0]
                    var_two = vals[1]
                    var_three = vals[2]

                elif vals_len == 2:
                    if vals[0].isalpha():
                        var_one = vals[0]
                        var_two = vals[1]
                        var_three = ""
                        
                    else:
                        var_one = ""
                        var_two = vals[0]
                        var_three = vals[1]

                else:
                    if vals[0].isalpha():
                        var_one = vals[0]
                        var_two = ""
                        var_three = ""
                    
                    else:
                        var_one = ""
                        var_two = ""
                        var_three = vals[0]
        
        else:
            code = [""]
            var_one = ""
            var_two = ""
            var_three = ""

        vars_list = code + [var_one, var_two, var_three]

        if not filled_flag:
            self.__fill_dict_of_cols_values(vars_list, RNG_csv_cols_list)

    def __fill_attributes_dict_DTM(self, segment_split: list, DTM_csv_cols_list: list) -> None:
        filled_flag = 0
        line_split_len = len(segment_split)
        if line_split_len:
            if line_split_len == 2:
                code = [segment_split[0]]
                vals = segment_split[1].split(":")

            else:
                if segment_split[0].isdigit():
                    code = [segment_split[0]]
                    vals = ["" for i in range(2)]

                else:
                    code = [""]
                    vals = segment_split[0].split(":")

            if all("" == val for val in vals) and len(vals) == 2:
                vars_list = code + vals
                self.__fill_dict_of_cols_values(vars_list, DTM_csv_cols_list)
                filled_flag = 1

            else:
                vals_len = len(vals)

                if vals_len == 2:
                    var_one = vals[0]
                    var_two = vals[1]

                else:
                    if len(vals[0]) == 3:
                        var_one = ""
                        var_two = vals[0]
                    
                    elif len(vals[0]) > 3:
                        var_one = vals[0]
                        var_two = ""

                    else:
                        var_one = ""
                        var_two = ""
        
        else:
            code = [""]
            var_one = ""
            var_two = ""

        if not filled_flag:
            vars_list = code + [var_one, var_two]
            self.__fill_dict_of_cols_values(vars_list, DTM_csv_cols_list)

    def __fill_attributes_dict_EQA(self, segment_split: list, EQA_csv_cols_list: list) -> None:
        filled_flag = 0
        line_split_len = len(segment_split)
        if line_split_len == 2:
            code = [segment_split[0]]
            vals = segment_split[1].split(":")

        else:
            if ":" not in segment_split[0]:
                code = [segment_split[0]]
                vals = ["" for i in range(3)]

            else:
                code = [""]
                vals = segment_split[0].split(":")

        vals_len = len(vals)
        if all("" == val for val in vals) and vals_len == 3:
            vars_list = code + vals
            self.__fill_dict_of_cols_values(vars_list, EQA_csv_cols_list)
            filled_flag = 1

        else:
            if vals_len == 3:
                        var_one = vals[0]
                        var_two = vals[1]
                        var_three = vals[2]

            elif vals_len == 2:
                second_val_len = len(vals[1])

                if second_val_len == 1:
                    if len(vals[0]) == 4:
                        var_one = ""
                        var_two = vals[0]
                    else:
                        var_one = vals[0]
                        var_two = ""

                    var_three = vals[1]

                else:
                    var_one = vals[0]
                    var_two = vals[1]
                    var_three = ""
            
            else:
                firs_val_len = len(vals[0])
                if firs_val_len == 1:
                    var_one = ""
                    var_two = ""
                    var_three = vals[0]

                elif len(vals[0]) == 4:
                    var_one = ""
                    var_two = vals[0]
                    var_three = ""
                
                elif len(vals[0]) > 4:
                    var_one = vals[0]
                    var_two = ""
                    var_three = ""
        
                else:
                    var_one = ""
                    var_two = ""
                    var_three = ""

        if not filled_flag:
            vars_list = code + [var_one, var_two, var_three]
            self.__fill_dict_of_cols_values(vars_list, EQA_csv_cols_list)

    def __fill_attributes_dict_DGS(self, segment_split: list, DGS_csv_cols_list: list) -> None:
        vars_list = []
        for i, var in enumerate(segment_split):
            if i in [6, 7]:
                continue
            
            elif i in [0, 2, 4, 5, 8]:
                vars_list.append(var)

            elif i == 1:
                vals_list_temp = var.split(":")
                var_len = len(vals_list_temp)

                if var_len == 3:
                    var_one = vals_list_temp[0]
                    var_two = vals_list_temp[1]
                    var_three = vals_list_temp[2]

                elif var_len == 2:
                    if vals_list_temp[0].isalpha():
                        var_one = vals_list_temp[0]
                        var_two = vals_list_temp[1]
                        var_three = ""
                    
                    else:
                        var_one = vals_list_temp[0]
                        var_two = ""
                        var_three = vals_list_temp[1]

                else:
                    if vals_list_temp[0].isalpha():
                        var_one = vals_list_temp[0]
                        var_two = ""
                        var_three = ""

                    else:
                        try:
                            float_temp = float(vals_list_temp[0])
                            var_one = vals_list_temp[0]
                            var_two = ""
                            var_three = ""

                        except:
                            var_one = ""
                            var_two = ""
                            var_three = vals_list_temp[0]

                vars_list += [var_one, var_two, var_three]      

            elif i == 3:
                vals_list_temp = var.split(":")
                var_len = len(vals_list_temp)
                
                if var_len == 2:
                    var_one = vals_list_temp[0]
                    var_two = vals_list_temp[1]

                else:
                    if vals_list_temp[0].isnumeric():
                        var_one = vals_list_temp[0]
                        var_two = ""
                    
                    else:
                        var_one = ""
                        var_two = vals_list_temp[0]

                vars_list += [var_one, var_two]

            elif i == 9:
                vals_list_temp = var.split(":")
                vars_list += vals_list_temp
                vars_list += ["" for i in range(4-len(vals_list_temp))]

        if len(segment_split) < 14:
            vars_list += ["" for i in range(14-len(vars_list))]

        self.__fill_dict_of_cols_values(vars_list, DGS_csv_cols_list)

    def __fill_attributes_dict_CTA(self, segment_split: list, CTA_csv_cols_list: list) -> None:
        line_split_len = len(segment_split)
        if line_split_len == 2:
            code = [segment_split[0]]
            vals = segment_split[1].split(":")

        else:
            if ":" not in segment_split[0]:
                code = [segment_split[0]]
                vals = ["" for i in range(2)]

            else:
                code = [""]
                vals = segment_split[0].split(":")

        vals_len = len(vals)
        if vals_len == 2:
            var_one = vals[0]
            var_two = vals[1]

        else:
            var_one = vals[0]
            var_two = ""

        vars_list = code + [var_one, var_two]
        self.__fill_dict_of_cols_values(vars_list, CTA_csv_cols_list)

    def __fill_attributes_dict_COM(self, segment_split: list, COM_csv_cols_list: list) -> None:
        if len(segment_split) == 2:
            segment_split = [f"{segment_split[0]}+{segment_split[1]}"]

        vals = segment_split[0].split(":")
        vals_len = len(vals)
        
        if vals_len == 2:
            var_one = vals[0]
            var_two = vals[1]

        elif len(vals[0]) == 2:
            var_one = ""
            var_two = vals[0]
        
        else:
            var_one = vals[0]
            var_two = ""

        vars_list = [var_one, var_two]
        self.__fill_dict_of_cols_values(vars_list, COM_csv_cols_list)

    def __fill_attributes_dict_CNT(self, segment_split: list, cnt_csv_cols_list: list) -> None:
        """
        This function is specific to identifying the values of the attributes included in the lines that start
        with CNT, i.e., the lines that specify the number of units of equipment or un-containerized cargo in a
        stowage location (p. 96).
        """
        vars_list = []
        if len(segment_split):
            cnt_vars = segment_split[0].split(":")
            if cnt_vars[0].isdigit() and cnt_vars[1].isdigit():
                var_one = cnt_vars[0]
                var_two = cnt_vars[1]

            elif cnt_vars[0].isdigit():
                var_one = cnt_vars[0]
                var_two = ""

            elif cnt_vars[1].isdigit():
                var_one = ""
                var_two = cnt_vars[1]

            else:
                var_one = ""
                var_two = ""

            vars_list.append(var_one)
            vars_list.append(var_two)


        else:
            vars_list.append("")
            vars_list.append("")

        self.__fill_dict_of_cols_values(vars_list, cnt_csv_cols_list)