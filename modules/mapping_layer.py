import pandas as pd

from modules import common_helpers

class MappingLayer():
    def __init__(self, d_seq_num_to_port_name: dict, d_port_name_to_seq_num: dict) -> None:
        self.csv_cols_dict = {}
        self.__d_seq_num_to_port_name = d_seq_num_to_port_name
        self.__d_port_name_to_seq_num = d_port_name_to_seq_num
    
    def __get_segment_csv_cols(self, csv_col_prefix: str, segment_header_csv_cols_list: list) -> list:
        """
        Takes the prefix of a csv column (derived from the baplie header of a baplie segment) and concatinates it with the names of attributes present in that baplie header,
        which will be the headers in the output csv. Used to fill the classe's attribute csv_cols_dict.

        Parameters
        ----------
        csv_col_prefix
            the prefix derived from the baplie message to be concatinated, e.g., LOC_147 from LOC+147

        segment_header_csv_cols_list
            the names of the columns belonging to a segment header, with which the prefix of that derived fromt that segment header will be concatinated

        Returns
        -------
        csv_cols_list
            a list containing the names of the output csv headers for that baplie segment (starting with the baplie header from which the csv_col_prefix is derived)
        """
        csv_cols_list = []
        for col in segment_header_csv_cols_list:
            # as other attribute codes than BRK does not have MEASUREMENT_SIGNIFICANCE_CODE (the if statement is for MEA headers)
            if csv_col_prefix != "BRK" and col == "MEASUREMENT_SIGNIFICANCE_CODE":
                continue

            csv_cols_list.append(f"{csv_col_prefix}_{col}")

        return csv_cols_list

    def __get_all_csv_cols_list(self) -> list:
        """
        Uses the classe's attribute csv_cols_dict (empty at first and filled in the classe's method __get_segment_csv_cols() later on and extracts from it the output csv headers.
        __get_segment_csv_cols() is called in the other classe's mtehod  get_csv_cols_dict_and_items().

        Parameters
        ----------
        None

        Returns
        -------
        csv_cols_list
            returned in get_csv_cols_dict_and_items()
        """
        csv_cols_list = []
        for k in self.csv_cols_dict.keys():
            csv_cols_list += self.csv_cols_dict[k]

        return csv_cols_list

    def __get_all_possible_csv_cols_lists(self, d_csv_cols_to_segments_map: dict, baplie_type_from_file_content: str) -> 'tuple[list, list, list, list, list, list, list, list, list, list, list, list, list, list, list, list, list, list, list, list, list]':
        """
        This function takes the filepath of the JSON file that contains a map where the keys are the headers of different segments (by type) and the values are the csv headers
        belonging to that key, i.e., to that header, in the form of lists: each list consists of the csv headers (attributes) for each segment in the baplie message. Should return 21 lists.

        Parameters
        ----------
        None

        Returns
        -------
        tuple
            of lists, where each list consists of the csv headers for a specific segment in the baplie message
        """
        # main headers
        LOC_identifier_csv_cols_list = d_csv_cols_to_segments_map["MAIN_HEADERS"]["LOC_IDENTIFIER"].split(";")
        EQD_csv_cols_list = d_csv_cols_to_segments_map["MAIN_HEADERS"]["EQD"].split(";")
        TMP_csv_cols_list = d_csv_cols_to_segments_map["MAIN_HEADERS"]["TMP"].split(";")
        EQA_csv_cols_list = d_csv_cols_to_segments_map["MAIN_HEADERS"]["EQA"].split(";")
        DGS_csv_cols_list = d_csv_cols_to_segments_map["MAIN_HEADERS"]["DGS"].split(";")
        CTA_csv_cols_list = d_csv_cols_to_segments_map["MAIN_HEADERS"]["CTA"].split(";")
        CNT_csv_cols_list = d_csv_cols_to_segments_map["MAIN_HEADERS"]["CNT"].split(";")

        # common main headers
        LOC_GROUP8_csv_cols_list = d_csv_cols_to_segments_map["COMMON_MAIN_HEADERS"]["LOC_GROUP8"].split(";")

        # sub headers
        MEA_csv_cols_list = d_csv_cols_to_segments_map["SUB_HEADERS"]["MEA"].split(";")
        HAN_csv_cols_list = d_csv_cols_to_segments_map["SUB_HEADERS"]["HAN"].split(";")
        DIM_csv_cols_list = d_csv_cols_to_segments_map["SUB_HEADERS"]["DIM"].split(";")
        GDS_csv_cols_list = d_csv_cols_to_segments_map["SUB_HEADERS"]["GDS"].split(";")
        RNG_csv_cols_list = d_csv_cols_to_segments_map["SUB_HEADERS"]["RNG"].split(";")
        DTM_csv_cols_list = d_csv_cols_to_segments_map["SUB_HEADERS"]["DTM"].split(";")
        ATT_csv_cols_list = d_csv_cols_to_segments_map["SUB_HEADERS"]["ATT"].split(";")
        COM_csv_cols_list = d_csv_cols_to_segments_map["SUB_HEADERS"]["COM"].split(";")

        # common sub headers
        FTX_csv_cols_list = d_csv_cols_to_segments_map["COMMON_SUB_HEADERS"]["FTX"].split(";")
        RFF_csv_cols_list = d_csv_cols_to_segments_map["COMMON_SUB_HEADERS"]["RFF"].split(";")
        NAD_csv_cols_list = d_csv_cols_to_segments_map["COMMON_SUB_HEADERS"]["NAD"].split(";")
        TSR_csv_cols_list = d_csv_cols_to_segments_map["COMMON_SUB_HEADERS"]["TSR"].split(";")
        TDT_csv_cols_list = d_csv_cols_to_segments_map["COMMON_SUB_HEADERS"]["TDT"].split(";")

        if baplie_type_from_file_content == "container":
            return LOC_identifier_csv_cols_list, EQD_csv_cols_list, TMP_csv_cols_list, EQA_csv_cols_list, DGS_csv_cols_list, CTA_csv_cols_list,\
                    CNT_csv_cols_list, LOC_GROUP8_csv_cols_list, MEA_csv_cols_list, HAN_csv_cols_list, DIM_csv_cols_list, GDS_csv_cols_list,\
                    RNG_csv_cols_list, DTM_csv_cols_list, ATT_csv_cols_list, COM_csv_cols_list, FTX_csv_cols_list, RFF_csv_cols_list, NAD_csv_cols_list,\
                    TSR_csv_cols_list, TDT_csv_cols_list

        elif baplie_type_from_file_content == "tank":
            return LOC_identifier_csv_cols_list, MEA_csv_cols_list, DIM_csv_cols_list, FTX_csv_cols_list
    
    def __check_and_fill_csv_cols_dict(self, csv_main_segment_header_prefix: str, segment_csv_cols_list: list, segment_header_name: str="") -> None:
        """
        Takes the main csv header (derived from the first baplie header in a segments group), the csv header belonging to the current segment, and the segment header name
        if not a main header. Does a proper naming for the csv header for that segment depending on the case to avoid naming conflicts and loosing data: since we are using
        a dictionary, if the csv headers (keys of the dictionary) were duplicated, old data would be overwritten by new data. After renaming, it checks if the csv headers
        are already present in the dictionary or not. If not, it populates the dictionary, otherwise it does nothing.

        Parameters
        ----------
        csv_main_segment_header_prefix
            the header of the main segment (the first segment in a segments group)

        segment_csv_cols_list
            the header of the segment within the segments group which csv_main_segment_header_prefix represents (could be a main segment or a sub-segment)

        segment_header_name
            the header name of a sub-segment in the segments group, empty string  by default (if it is the main segment)

        Returns
        -------
        None
        """
        if len(segment_header_name):
            csv_col_prefix = f"{csv_main_segment_header_prefix}_{segment_header_name}"
        else:
            csv_col_prefix = csv_main_segment_header_prefix

        if csv_col_prefix not in self.csv_cols_dict.keys():
            self.csv_cols_dict[csv_col_prefix] = self.__get_segment_csv_cols(csv_col_prefix, segment_csv_cols_list)
            # example:
            # csv_cols_prefix: LOC_147
            # segment_csv_cols_list is coming from csv_cols_segments_map.json: belonging to LOC_IDENTIFIER (first LOC Header)
            # self.csv_cols_dict = { "LOC_147": ["FUNCTION_CODE_QUALIFIER" ,"ID", "CODE_LIST_ID_CODE", "CODE_LIST_RESPONSIBLE_AGENCY_CODE", "NAME"] }

    def __get_segment_header_name_temp(self, segment_header_name: str, segment_header_count: int) -> str:
        """
        In case of common sub-segments headers such as FTX and RFF, counters are initialized as there could be more than one common header within the same segments group, e.g., there could be
        2 FTX segments in one segments group. Hence, they are renamed in such a way to concatinate the segment header name with its occurence rank.

        Parameters
        ----------
        segment_header_name
            the header of the common sub-segment

        segment_header_count
            a counter representing the occurence rank of common sub-segment


        Returns
        -------
        segment_header_name_temp
            segment_header_name and segment_header_count joined by an "_"
        """
        segment_header_name_temp = f"{segment_header_name}_{segment_header_count}"
        return segment_header_name_temp
        
    def __fill_csv_cols_dict_LOC_and_return_counts(
        self, new_container_data_flag: str, csv_main_segment_header_prefix: str, segment_header_name: str, LOC_identifier_csv_cols_list: list, FTX_csv_cols_list: list,
        RFF_csv_cols_list: list, LOC_GROUP8_csv_cols_list: list, TSR_csv_cols_list: list, TDT_csv_cols_list: list,
        LOC_identifier_FTX_count: int, LOC_identifier_RFF_count: int, LOC_GROUP8_TSR_count: int, LOC_GROUP8_TDT_count: int
    ) -> 'tuple[int, int, int, int]':
        """
        Specific to LOC headers, takes new_container_data_flag (flag indicating the start of data for a new container), the current main segment header prefix, the current segment hander name, and
        the lists of main and sub-segments related to any segment that starts with LOC, along with the available counters, and fills csv_cols_dict (if not already filled with the csv header of that segment)
        with the headers belonging to the current segment header name (whether it was a LOC main header or a sub-header).

        Parameters
        ----------
        new_container_data_flag
            flag indicating the start of data for a new container

        csv_main_segment_header_prefix
            the header of the main segment (the first segment in a segments group)

        segment_header_name
            the header of the common sub-segment

        LOC_identifier_csv_cols_list
            a list of csv headers for the LOC segment that is the new_container_data_flag

        FTX_csv_cols_list
            a list of csv headers for the FTX segment that is a sub-segment for the LOC identifier segment

        RFF_csv_cols_list
            a list of csv headers for the RFF segment that is a sub-segment for the LOC identifier segment

        LOC_GROUP8_csv_cols_list
            a list of csv headers for the LOC segment other than the new_container_data_flag

        TSR_csv_cols_list
            a list of csv headers for the TSR segment that is a sub-segment for a LOC segment other than the new_container_data_flag

        TDT_csv_cols_list
            a list of csv headers for the TDT segment that is a sub-segment for a LOC segment other than the new_container_data_flag

        LOC_identifier_FTX_count
            a counter to keep track of the occurence rank if the FTX sub-segment was found

        LOC_identifier_RFF_count
            a counter to keep track of the occurence rank if the RFF sub-segment was found
        
        LOC_GROUP8_TSR_count
            a counter to keep track of the occurence rank if the TSR sub-segment was found

        LOC_GROUP8_TDT_count
            a counter to keep track of the occurence rank if the TDT sub-segment was found

        Returns
        -------
        LOC_identifier_FTX_count, LOC_identifier_RFF_count, LOC_GROUP8_TSR_count, LOC_GROUP8_TDT_count
            the same input counters but the counter that belongs to the found header will be incremented by one
        """
        if csv_main_segment_header_prefix == new_container_data_flag:
            if segment_header_name == "FTX":
                LOC_identifier_FTX_count += 1
                header_name_temp = self.__get_segment_header_name_temp(segment_header_name, LOC_identifier_FTX_count)
                self.__check_and_fill_csv_cols_dict(csv_main_segment_header_prefix, FTX_csv_cols_list, header_name_temp)

            elif segment_header_name == "RFF":
                LOC_identifier_RFF_count += 1
                header_name_temp = self.__get_segment_header_name_temp(segment_header_name, LOC_identifier_RFF_count)
                self.__check_and_fill_csv_cols_dict(csv_main_segment_header_prefix, RFF_csv_cols_list, header_name_temp)

            else:
                self.__check_and_fill_csv_cols_dict(csv_main_segment_header_prefix, LOC_identifier_csv_cols_list)

        else:
            if segment_header_name == "TSR":
                LOC_GROUP8_TSR_count += 1
                header_name_temp = self.__get_segment_header_name_temp(segment_header_name, LOC_GROUP8_TSR_count)
                self.__check_and_fill_csv_cols_dict(csv_main_segment_header_prefix, TSR_csv_cols_list, header_name_temp)

            elif segment_header_name == "TDT":
                LOC_GROUP8_TDT_count += 1
                header_name_temp = self.__get_segment_header_name_temp(segment_header_name, LOC_GROUP8_TDT_count)
                self.__check_and_fill_csv_cols_dict(csv_main_segment_header_prefix, TDT_csv_cols_list, header_name_temp)
            
            else:
                self.__check_and_fill_csv_cols_dict(csv_main_segment_header_prefix, LOC_GROUP8_csv_cols_list)

        return LOC_identifier_FTX_count, LOC_identifier_RFF_count, LOC_GROUP8_TSR_count, LOC_GROUP8_TDT_count

    def __fill_csv_cols_dict_EQD_and_return_counts(
        self, csv_main_segment_header_prefix: str, segment_header_name: str, segment_split: list, FTX_csv_cols_list: list, RFF_csv_cols_list: list,
        NAD_csv_cols_list: list, MEA_csv_cols_list: list, HAN_csv_cols_list: list, DIM_csv_cols_list: list, DGS_csv_cols_list: list,
        EQD_RFF_count: int, EQD_FTX_count: int
    ) -> 'tuple[int, int]':
        """
        Similar behavior to __fill_csv_cols_dict_LOC_and_return_counts() but for EQD sub-segments.

        Parameters
        ----------
        csv_main_segment_header_prefix
            the header of the main segment (the first segment in a segments group), "EQD" in this case, will be concatinated to get the csv cols key

        segment_header_name
            the header name of the sub-segment

        segment_split
            a list representation of the string segment split by "+", needed to get compount csv cols prefixes

        FTX_csv_cols_list
            a list of csv headers for the FTX segment that is a sub-segment for the EQD segment group

        RFF_csv_cols_list
            a list of csv headers for the RFF segment that is a sub-segment for the EQD segment group

        NAD_csv_cols_list
            a list of csv headers for the NAD segment that is a sub-segment for the EQD segment group

        MEA_csv_cols_list
            a list of csv headers for the MEA segment that is a sub-segment for the EQD segment group
        
        HAN_csv_cols_list
            a list of csv headers for the HAN segment that is a sub-segment for the EQD segment group
        
        DIM_csv_cols_list
            a list of csv headers for the DIM segment that is a sub-segment for the EQD segment group
        
        DGS_csv_cols_list
            a list of csv headers for the DGS segment that is a sub-segment for the EQD segment group
        
        EQD_RFF_count
            a counter to keep track of the occurence rank if the RFF sub-segment was found within the EQD segment group
        
        EQD_FTX_count
            a counter to keep track of the occurence rank if the FTX sub-segment was found within the EQD segment group

        Returns
        -------
        EQD_RFF_count, EQD_FTX_count
            the same input counters but the counter that belongs to the found header will be incremented by one
        """
        if segment_header_name == "FTX":
            EQD_FTX_count += 1
            header_name_temp = self.__get_segment_header_name_temp(segment_header_name, EQD_FTX_count)
            self.__check_and_fill_csv_cols_dict(csv_main_segment_header_prefix, FTX_csv_cols_list, header_name_temp)

        elif segment_header_name == "RFF":
            EQD_RFF_count += 1
            header_name_temp = self.__get_segment_header_name_temp(segment_header_name, EQD_RFF_count)
            self.__check_and_fill_csv_cols_dict(csv_main_segment_header_prefix, RFF_csv_cols_list, header_name_temp)

        elif segment_header_name == "NAD":
            compound_csv_col_prefix = common_helpers.get_compound_segment_header(segment_split, segment_header_name, 1)
            self.__check_and_fill_csv_cols_dict(csv_main_segment_header_prefix, NAD_csv_cols_list, compound_csv_col_prefix)

        elif segment_header_name in "MEA":
            compound_csv_col_prefix = common_helpers.get_compound_segment_header(segment_split, segment_header_name, 2)
            self.__check_and_fill_csv_cols_dict(csv_main_segment_header_prefix, MEA_csv_cols_list, compound_csv_col_prefix)

        elif segment_header_name == "HAN":
            compound_csv_col_prefix = common_helpers.get_compound_segment_header(segment_split, segment_header_name, 1)
            self.__check_and_fill_csv_cols_dict(csv_main_segment_header_prefix, HAN_csv_cols_list, compound_csv_col_prefix)
        
        elif segment_header_name == "DIM":
            compound_csv_col_prefix = common_helpers.get_compound_segment_header(segment_split, segment_header_name, 1)
            self.__check_and_fill_csv_cols_dict(csv_main_segment_header_prefix, DIM_csv_cols_list, compound_csv_col_prefix)

        elif segment_header_name == "GDS":
            compound_csv_col_prefix = common_helpers.get_compound_segment_header(segment_split, segment_header_name, 1)
            self.__check_and_fill_csv_cols_dict(csv_main_segment_header_prefix, DGS_csv_cols_list, compound_csv_col_prefix)

        return EQD_RFF_count, EQD_FTX_count

    def __fill_csv_cols_dict_TMP(
        self, csv_main_segment_header_prefix: str, segment_header_name: str, RNG_csv_cols_list: list, DTM_csv_cols_list: list
    ) -> None:
        """
        Similar behavior to __fill_csv_cols_dict_LOC_and_return_counts() but for the TMP sub-segments and it does not return counters as there are not any.

        Parameters
        ----------
        csv_main_segment_header_prefix
            the header of the main segment (the first segment in a segments group), "EQD" in this case, will be concatinated to get the csv cols key

        segment_header_name
            the header name of the sub-segment

        RNG_csv_cols_list
            a list of csv headers for the RNN segment that is a sub-segment for the EQD segment group

        DTM_csv_cols_list
            a list of csv headers for the DTM segment that is a sub-segment for the EQD segment group

        Returns
        -------
        None
        """
        if segment_header_name == "RNG":
            self.__check_and_fill_csv_cols_dict(csv_main_segment_header_prefix, RNG_csv_cols_list, segment_header_name)
                    
        elif segment_header_name == "DTM":
            self.__check_and_fill_csv_cols_dict(csv_main_segment_header_prefix, DTM_csv_cols_list, segment_header_name)

    def __fill_csv_cols_dict_DGS_and_return_count(
        self, csv_main_segment_header_prefix: str, segment_header_name: str, segment_split: list,
        ATT_csv_cols_list: list, MEA_csv_cols_list: list, FTX_csv_cols_list: list
    ) -> None:
        """
        Similar behavior to __fill_csv_cols_dict_LOC_and_return_counts() but for DGS sub-segments and it does not return counters as there are not any.

        Parameters
        ----------
        csv_main_segment_header_prefix
            the header of the main segment (the first segment in a segments group), "EQD" in this case, will be concatinated to get the csv cols key

        segment_header_name
            the header name of the sub-segment

        segment_split
            a list representation of the string segment split by "+", needed to get compount csv cols prefixes

        ATT_csv_cols_list
            a list of csv headers for the ATT segment that is a sub-segment for the EQD segment group

        MEA_csv_cols_list
            a list of csv headers for the MEA segment that is a sub-segment for the EQD segment group

        FTX_csv_cols_list
            a list of csv headers for the FTX segment that is a sub-segment for the EQD segment group

        Returns
        -------
        None
        """
        if segment_header_name == "ATT":
            compound_csv_col_prefix = common_helpers.get_compound_segment_header(segment_split, segment_header_name, 2)
            self.__check_and_fill_csv_cols_dict(csv_main_segment_header_prefix, ATT_csv_cols_list, compound_csv_col_prefix)

        elif segment_header_name == "MEA":
            compound_csv_col_prefix = common_helpers.get_compound_segment_header(segment_split, segment_header_name, 2)
            self.__check_and_fill_csv_cols_dict(csv_main_segment_header_prefix, MEA_csv_cols_list, compound_csv_col_prefix)

        elif segment_header_name == "FTX":
            self.__check_and_fill_csv_cols_dict(csv_main_segment_header_prefix, FTX_csv_cols_list, segment_header_name)

    def __intialize_counters(self) -> 'tuple[int, int, int, int, int, int, int, int]':
        """
        Initialize required counters to keep track of common sub-headers from different segment groups.

        Parameters
        ----------
        None

        Returns
        -------
        LOC_identifier_FTX_count, LOC_identifier_RFF_count, LOC_GROUP8_TSR_count, LOC_GROUP8_TDT_count, EQD_RFF_count, EQD_FTX_count, DGS_count
            7 counters to keep track of common sub-headers from different segment groups or headers belonging to the same segment
            group but could be repeated (holding different information)
        """
        LOC_identifier_FTX_count = 0
        LOC_identifier_RFF_count = 0

        LOC_GROUP8_TSR_count = 0
        LOC_GROUP8_TDT_count = 0

        EQD_RFF_count = 0
        EQD_FTX_count = 0

        DGS_count = 0
        
        return  LOC_identifier_FTX_count, LOC_identifier_RFF_count, LOC_GROUP8_TSR_count, LOC_GROUP8_TDT_count,\
                EQD_RFF_count, EQD_FTX_count, DGS_count

    def __get_d_csv_cols_containers(self, containers_data_list: list, new_container_data_flag: str, d_csv_cols_to_segments_map: dict, d_main_to_sub_segments_map: dict, baplie_type_from_content: str) -> 'tuple[dict, list]':
        # EQD_group_segments_headers = [ sub_header for sub_header in d_main_to_sub_segments_map["EQD"] if sub_header not in ["FTX", "RFF"] ]
        # EQD_group_segments_headers.append("EQD")

        # LOC_identifier_csv_cols_list is the list of headers that corresponds to the LOC segment that indicates the start of the data for a new container
        LOC_identifier_csv_cols_list, EQD_csv_cols_list, TMP_csv_cols_list, EQA_csv_cols_list, DGS_csv_cols_list, CTA_csv_cols_list,\
        CNT_csv_cols_list, LOC_GROUP8_csv_cols_list, MEA_csv_cols_list, HAN_csv_cols_list, DIM_csv_cols_list, GDS_csv_cols_list,\
        RNG_csv_cols_list, DTM_csv_cols_list, ATT_csv_cols_list, COM_csv_cols_list, FTX_csv_cols_list, RFF_csv_cols_list, NAD_csv_cols_list,\
        TSR_csv_cols_list, TDT_csv_cols_list = self.__get_all_possible_csv_cols_lists(d_csv_cols_to_segments_map, baplie_type_from_content)

        i = 0
        csv_main_segment_header_prefix = new_container_data_flag
        for container_data in containers_data_list:
            i += 1
            LOC_identifier_FTX_count, LOC_identifier_RFF_count, LOC_GROUP8_TSR_count, LOC_GROUP8_TDT_count, \
                EQD_RFF_count, EQD_FTX_count, DGS_count = self.__intialize_counters()

            for segment in container_data:
                segment_split = segment.split("+")
                segment_header_name = segment[:3]

                if segment_header_name == "LOC":
                    csv_main_segment_header_prefix = common_helpers.get_compound_segment_header(segment_split, segment_header_name, 1) # it will be LOC_147 for the 1st time
                
                if segment_header_name == "EQD":
                    csv_main_segment_header_prefix = "EQD"

                elif segment_header_name == "TMP":
                    csv_main_segment_header_prefix = "TMP"

                elif segment_header_name == "EQA":
                    csv_main_segment_header_prefix = "EQA"

                elif segment_header_name == "DGS":
                    csv_main_segment_header_prefix = "DGS"

                elif segment_header_name == "CTA":
                    csv_main_segment_header_prefix = "CTA"

                elif segment_header_name == "CNT":
                    csv_main_segment_header_prefix = "CNT"

                ## LOC HEADERS ##
                if "LOC" in csv_main_segment_header_prefix: # we leveraged the fact that the container data will always start with LOC
                    LOC_identifier_FTX_count, LOC_identifier_RFF_count, LOC_GROUP8_TSR_count, LOC_GROUP8_TDT_count = \
                    self.__fill_csv_cols_dict_LOC_and_return_counts(
                        new_container_data_flag, csv_main_segment_header_prefix, segment_header_name,
                        LOC_identifier_csv_cols_list, FTX_csv_cols_list, RFF_csv_cols_list,
                        LOC_GROUP8_csv_cols_list, TSR_csv_cols_list, TDT_csv_cols_list,
                        LOC_identifier_FTX_count, LOC_identifier_RFF_count,
                        LOC_GROUP8_TSR_count, LOC_GROUP8_TDT_count
                    )

                ## EQD HEADERS ##
                if "EQD" == csv_main_segment_header_prefix:
                    if segment_header_name == "EQD":
                        # print(EQD_csv_cols_list)
                        self.__check_and_fill_csv_cols_dict(csv_main_segment_header_prefix, EQD_csv_cols_list)

                    else:
                        EQD_RFF_count, EQD_FTX_count = self.__fill_csv_cols_dict_EQD_and_return_counts(
                            csv_main_segment_header_prefix, segment_header_name, segment_split, FTX_csv_cols_list, RFF_csv_cols_list, NAD_csv_cols_list,
                            MEA_csv_cols_list, HAN_csv_cols_list, DIM_csv_cols_list, GDS_csv_cols_list, EQD_RFF_count, EQD_FTX_count
                        )

                ## TMP HEADERS ##
                if "TMP" == csv_main_segment_header_prefix:
                    if segment_header_name in d_main_to_sub_segments_map["TMP"]:
                        self.__fill_csv_cols_dict_TMP(csv_main_segment_header_prefix, segment_header_name, RNG_csv_cols_list, DTM_csv_cols_list)
                    
                    elif segment != "TMP+2":
                        self.__check_and_fill_csv_cols_dict(csv_main_segment_header_prefix, TMP_csv_cols_list)

                ## EQA HEADERS ##
                if "EQA" == csv_main_segment_header_prefix:
                    if segment_header_name == "EQA":
                        self.__check_and_fill_csv_cols_dict(csv_main_segment_header_prefix, EQA_csv_cols_list)

                    else:
                        self.__check_and_fill_csv_cols_dict(csv_main_segment_header_prefix, NAD_csv_cols_list, segment_header_name)

                ## DGS HEADERS ##
                if "DGS" == csv_main_segment_header_prefix:
                    if segment_header_name == "DGS":
                        DGS_count += 1
                        csv_main_segment_header_prefix_temp = f"{csv_main_segment_header_prefix}_{DGS_count}"
                        self.__check_and_fill_csv_cols_dict(csv_main_segment_header_prefix_temp, DGS_csv_cols_list)

                    else:
                        self.__fill_csv_cols_dict_DGS_and_return_count(
                            csv_main_segment_header_prefix_temp, segment_header_name, segment_split, ATT_csv_cols_list, MEA_csv_cols_list, FTX_csv_cols_list
                        )
                
                ## CTA HEADERS ##
                if "CTA" == csv_main_segment_header_prefix:
                    if segment_header_name == "CTA":
                        self.__check_and_fill_csv_cols_dict(csv_main_segment_header_prefix, CTA_csv_cols_list)

                    else:
                        self.__check_and_fill_csv_cols_dict(csv_main_segment_header_prefix, COM_csv_cols_list, segment_header_name)

                ## CNT HEADER ##
                if "CNT" == csv_main_segment_header_prefix:
                    self.__check_and_fill_csv_cols_dict(csv_main_segment_header_prefix, CNT_csv_cols_list)
                
                # if segment_header_name in EQD_group_segments_headers: # we leveraged the fact that the container data will always start with LOC
                #     csv_main_segment_header_prefix = "EQD" # it keeps track of the segment group
                
                # # LOC HEADERS
                # if segment_header_name == "LOC":
                #     csv_main_segment_header_prefix = common_helpers.get_compound_segment_header(segment_split, segment_header_name, 1)
                #     self.__check_and_fill_csv_cols_dict(csv_main_segment_header_prefix, LOC_identifier_csv_cols_list)

                # if "LOC" in csv_main_segment_header_prefix:
                #     LOC_identifier_FTX_count, LOC_identifier_RFF_count, LOC_GROUP8_TSR_count, LOC_GROUP8_TDT_count = self.__fill_csv_cols_dict_LOC_and_return_counts(
                #         new_container_data_flag, csv_main_segment_header_prefix, segment_header_name, LOC_identifier_csv_cols_list,
                #         FTX_csv_cols_list, RFF_csv_cols_list, LOC_GROUP8_csv_cols_list, TSR_csv_cols_list, TDT_csv_cols_list,
                #         LOC_identifier_FTX_count, LOC_identifier_RFF_count, LOC_GROUP8_TSR_count, LOC_GROUP8_TDT_count
                #     )
                
                # # EQD HEADERS
                # if csv_main_segment_header_prefix == "EQD":
                #     if segment_header_name == "EQD":
                #         csv_main_segment_header_prefix = segment_header_name
                #         self.__check_and_fill_csv_cols_dict(csv_main_segment_header_prefix, EQD_csv_cols_list)
                #     else:
                #         EQD_RFF_count, EQD_FTX_count = self.__fill_csv_cols_dict_EQD_and_return_counts(
                #             csv_main_segment_header_prefix, segment_header_name, segment_split, FTX_csv_cols_list, RFF_csv_cols_list, NAD_csv_cols_list,
                #             MEA_csv_cols_list, HAN_csv_cols_list, DIM_csv_cols_list, GDS_csv_cols_list, EQD_RFF_count, EQD_FTX_count
                #         )
                
                # # TMP HEADERS
                # if segment_header_name == "TMP":
                #     csv_main_segment_header_prefix = segment_header_name
                #     if segment != "TMP+2":
                #         self.__check_and_fill_csv_cols_dict(csv_main_segment_header_prefix, TMP_csv_cols_list)

                # if segment_header_name in d_main_to_sub_segments_map["TMP"]:
                #     csv_main_segment_header_prefix = "TMP"

                # if csv_main_segment_header_prefix == "TMP":
                #     self.__fill_csv_cols_dict_TMP(csv_main_segment_header_prefix, segment_header_name, RNG_csv_cols_list, DTM_csv_cols_list)

                # # EQA HEADERS
                # if segment_header_name == "EQA":
                #     csv_main_segment_header_prefix = segment_header_name
                #     self.__check_and_fill_csv_cols_dict(csv_main_segment_header_prefix, EQA_csv_cols_list)

                # if segment_header_name in d_main_to_sub_segments_map["EQA"] and csv_main_segment_header_prefix != "EQD":
                #     csv_main_segment_header_prefix = "EQA"
                
                # if csv_main_segment_header_prefix == "EQA" and segment_header_name == "NAD":
                #     self.__check_and_fill_csv_cols_dict(csv_main_segment_header_prefix, NAD_csv_cols_list, segment_header_name)
                
                # # DGS HEADERS
                # if segment_header_name == "DGS":
                #     csv_main_segment_header_prefix = segment_header_name

                #     DGS_count += 1
                #     csv_main_segment_header_prefix_temp = f"{csv_main_segment_header_prefix}_{DGS_count}"
                #     self.__check_and_fill_csv_cols_dict(csv_main_segment_header_prefix_temp, DGS_csv_cols_list)

                # elif segment_header_name in d_main_to_sub_segments_map["DGS"] and not any(header in csv_main_segment_header_prefix for header in ["LOC", "EQD"]):
                #     csv_main_segment_header_prefix = "DGS"
                #     DGS_count += 1
                #     csv_main_segment_header_prefix_temp = f"{csv_main_segment_header_prefix}_{DGS_count}"
                
                # if csv_main_segment_header_prefix == "DGS":
                #     self.__fill_csv_cols_dict_DGS_and_return_count(
                #         csv_main_segment_header_prefix_temp, segment_header_name, segment_split, ATT_csv_cols_list, MEA_csv_cols_list, FTX_csv_cols_list
                #     )
                
                # # CTA HEADERS
                # elif segment_header_name == "CTA":
                #     csv_main_segment_header_prefix = segment_header_name
                #     self.__check_and_fill_csv_cols_dict(csv_main_segment_header_prefix, CTA_csv_cols_list)

                # elif segment_header_name == "COM":
                #     self.__check_and_fill_csv_cols_dict(csv_main_segment_header_prefix, COM_csv_cols_list, segment_header_name)
                
                # # CNT HEADERS
                # elif segment_header_name == "CNT":
                #     csv_main_segment_header_prefix = segment_header_name
                #     self.__check_and_fill_csv_cols_dict(csv_main_segment_header_prefix, CNT_csv_cols_list)

    def __get_d_csv_cols_tanks(self, containers_data_list: list, d_csv_cols_to_segments_map: dict, d_main_to_sub_segments_map: dict, baplie_type_from_content: str) -> tuple:
        # LOC_identifier_csv_cols_list is the list of headers that corresponds to the LOC segment that indicates the start of the data for a new container
        LOC_identifier_csv_cols_list, MEA_csv_cols_list, DIM_csv_cols_list, FTX_csv_cols_list = self.__get_all_possible_csv_cols_lists(d_csv_cols_to_segments_map, baplie_type_from_content)

        for container_data in containers_data_list:
            if len(container_data) > 7:
                print(container_data)
            
            i = 0 # the index for the 1st segment (new container data flag)
            for segment in container_data:
                segment_split = segment.split("+")
                segment_header_name = segment[:3]

                if segment_header_name == "LOC" and not i: # we are only interested in the 1st LOC and don't care about other LOC headers
                    compound_segment_header_name = common_helpers.get_compound_segment_header(segment_split, segment_header_name, 1)
                    self.__check_and_fill_csv_cols_dict(compound_segment_header_name, LOC_identifier_csv_cols_list)

                elif segment_header_name == "MEA":
                    compound_segment_header_name = common_helpers.get_compound_segment_header(segment_split, segment_header_name, 1)
                    self.__check_and_fill_csv_cols_dict(compound_segment_header_name, MEA_csv_cols_list)

                elif segment_header_name == "DIM":
                    self.__check_and_fill_csv_cols_dict(segment_header_name, DIM_csv_cols_list)

                elif segment_header_name == "FTX":
                    self.__check_and_fill_csv_cols_dict(segment_header_name, FTX_csv_cols_list)

    def get_csv_cols_dict_and_list(self, containers_data_list: list, new_container_data_flag: str, d_csv_cols_to_segments_map: dict, d_main_to_sub_segments_map: dict, baplie_type_from_content: str) -> tuple:
        """
        Takes the list version of the baplie message and lists of csv headers (without the added prefixes) belonging to every
        possible baplie segments headers and outputs maps linking the headers from the baplie message to the csv headers with and
        without the prefixes.

        Parameters
        ----------
        containers_data_list
            list of sublits, where every element of every sublist is a segment from the container data in the body of the baplie message

        new_container_data_flag
            flag indicating the start of data for a new container

        d_csv_cols_to_segments_map
            a python dict where the keys are all possible segments headers in a baplie message, and the values are a list of columns that belong to a specific segment

        d_main_to_sub_segments_map
            a map between a main segment's baplie header and its sub-segments' baplie headers
        
        Returns
        -------
        csv_cols_dict
            a dictionary where the keys are the csv headers derived from the Baplies segments header, e.g., LOC_9 from LOC+9 or nad froom NAD,
            and the values are lists of the csv headers with the added prefixes
        
        csv_cols_list
            a list that contains all the output csv headers
        """
        self.csv_cols_dict = {}

        if baplie_type_from_content == "tank":
            self.__get_d_csv_cols_tanks(containers_data_list, d_csv_cols_to_segments_map, d_main_to_sub_segments_map, baplie_type_from_content)

        elif baplie_type_from_content == "container":
            self.__get_d_csv_cols_containers(containers_data_list, new_container_data_flag, d_csv_cols_to_segments_map, d_main_to_sub_segments_map, baplie_type_from_content)

        csv_cols_list = self.__get_all_csv_cols_list()
        
        return self.csv_cols_dict, csv_cols_list
    
    # def map_POLs_in_df(self, df: pd.DataFrame, call_port_name: str, call_port_name_base: str) -> tuple[pd.DataFrame, list]:
    #     l_df_POLs_names = df["LOC_9_LOCATION_ID"].tolist()
    #     l_df_container_ids = df["EQD_ID"].tolist()

    #     l_mapped_POLs_names = []
    #     l_err_container_ids = []
    #     for POL_name, container_id in list(zip(l_df_POLs_names, l_df_container_ids)):
    #         if POL_name == call_port_name_base:
    #             mapped_POL_name = call_port_name
            
    #         else: # POLs different than call port not allowed in loadlists
    #             mapped_POL_name = POL_name # keep POL as it is
    #             if mapped_POL_name != "":
    #                 l_err_container_ids.append(container_id)
        
    #         l_mapped_POLs_names.append(mapped_POL_name)

    #     df["LOC_9_LOCATION_ID"] = l_mapped_POLs_names
        
    #     return df, l_err_container_ids

    def __get_d_call_port_potential_PODs_name_base_to_name(self, call_port_name_base: str, call_port_seq_num: int) -> dict:
        l_ports_names_base = []
        l_PODs_names_after_call_port = []
        for seq_num in range(call_port_seq_num+1, len(self.__d_seq_num_to_port_name)):
            port_name = self.__d_seq_num_to_port_name[seq_num]

            # port_name_base is the port name without the number at the end (if any)
            # e.g., if port_name was GBSOU2, than port_name_base would be GBSOU, if it was FRDKK, it stays FRDKK
            port_name_base = port_name[:5]

            # if we see the name base again -> we have reached the current port but in the future
            # => stop looking and exit at the last possible POD right before the current port (in the future)
            # because a POL cannot be a POD for itself
            if port_name_base == call_port_name_base: 
                break
            
            # if GBSOU is in the list, then there was already a port (GBSOU, GBSOU2, and who knows? maybe GBSOU3) that was added to the list in the step below
            # => continue: because the first occurence of GBSOU (being a POD of the current) port was already found in a previous iteration and its name base was added to the list
            # GBSOU cannot be a POD more than once for a single port
            if port_name_base in l_ports_names_base:
                continue
            
            # if GBSOU (as port name base) not in list => port GBSOU have not passed yet => add name base to list
            l_ports_names_base.append(port_name_base) # step below (mentioned in the comment above)

            # add port name to the list (depends on the POL: could be GBSOU, GBSOU2, or GBSOU3 :p)
            l_PODs_names_after_call_port.append(port_name)

        d_map = {
            port_name_base: port_name
            for (port_name_base, port_name)
            in list(zip(l_ports_names_base, l_PODs_names_after_call_port))
        }

        return d_map

    def __get_d_call_port_potential_POLs_name_base_to_name(self, call_port_name_base: str, call_port_seq_num: int) -> dict:
        l_ports_names_base = []
        l_POLs_names_before_call_port = []
        prev_seq_num = call_port_seq_num - 1
        # -1 as end to reach 0 (the first seq num) and -1 as a step to loop in the reverse order
        for seq_num in range(prev_seq_num, -1, -1):
            port_name = self.__d_seq_num_to_port_name[seq_num]
            port_name_base = port_name[:5]

            # if we see the name base again -> we have reached the current port but in the past
            # a port cannot be a POL for istelf!
            if port_name_base == call_port_name_base:
                continue

            # if two ports A and B are before past ports for port C
            # => port B is the POL and Port A cannot be a port!
            if port_name_base in l_ports_names_base:
                continue
            
            l_ports_names_base.append(port_name_base)
            l_POLs_names_before_call_port.append(port_name)

        d_map = {
            port_name_base: port_name
            for (port_name_base, port_name)
            in list(zip(l_ports_names_base, l_POLs_names_before_call_port))
        }
        
        return d_map

    def map_PODs_in_df(self, df: pd.DataFrame, l_past_POLs_names: list, call_port_name_base: str, call_port_seq_num: int) -> pd.DataFrame:
        d_call_port_potential_PODs_name_base_to_name = self.__get_d_call_port_potential_PODs_name_base_to_name(call_port_name_base, call_port_seq_num)
        l_call_port_PODs_names_base = list(d_call_port_potential_PODs_name_base_to_name.keys())

        l_df_PODs_names = df["LOC_11_LOCATION_ID"].tolist()
        # l_df_container_ids = df["EQD_ID"].tolist()

        l_mapped_PODs_names = []
        #l_err_container_ids = []
        # for POD_name, container_id in list(zip(l_df_PODs_names, l_df_container_ids)):
        for POD_name in l_df_PODs_names:
            if "USLA" in POD_name:
                l_mapped_PODs_names.append(POD_name)
                continue
            
            mapped_POD_name = POD_name # keep pod name as it is by default

            if POD_name in l_call_port_PODs_names_base: # potential POD in rotation (future port in rotation) for onboard and loadlists
                mapped_POD_name = d_call_port_potential_PODs_name_base_to_name[POD_name]

            
            elif call_port_seq_num: # if loadlist
                d_call_port_potential_POLs_name_base_to_name = self.__get_d_call_port_potential_POLs_name_base_to_name(call_port_name_base, call_port_seq_num)
                l_call_port_POLs_names_base = list(d_call_port_potential_POLs_name_base_to_name.keys())

                if POD_name in l_call_port_POLs_names_base: # previous POL in rotation (future port in rotation)
                    past_port = d_call_port_potential_POLs_name_base_to_name[POD_name]
                    len_past_port_name = len(past_port)
                    if len_past_port_name == 6 and past_port[-1].isdigit(): # has a number extension
                        past_port_extension = int(past_port[-1])
                        POD_extension = past_port_extension + 1

                    # don't have to check anymore as it is already handled by anomaly detection layer                    
                    # if len_past_port_name == 6 and past_port[-1].isalpha():
                    #     POD_extension = ""
                    #     l_err_container_ids.append(container_id)

                    if len_past_port_name == 5:
                        POD_extension = 2
                    
                    mapped_POD_name = f"{POD_name}{POD_extension}"

                elif POD_name in l_past_POLs_names: # future port not in rotation (a past POL for OnBoard)
                    mapped_POD_name = f"{POD_name}2"

                # else: port not in rotation nor a past POL => error handled in anomaly detection layer (keep pod name as it is)
                    # mapped_POD_name = POD_name
                    # if call_port_seq_num == 1 and mapped_POD_name != "": # if first loadlist (we only need to check if the first loadlist has pods not in rotation or past pols)
                    #     l_err_container_ids.append(container_id)
            
            # else: # port not in rotation nor a past POL => throw error (this is for onboard only and not applied to any loadlist)
            # => error handled in anomaly detection layer (keep pod name as it is)
                # mapped_POD_name = POD_name
                # if mapped_POD_name != "":
                #     l_err_container_ids.append(container_id)

            l_mapped_PODs_names.append(mapped_POD_name)

        df["LOC_11_LOCATION_ID"] = l_mapped_PODs_names
        
        return df #, l_err_container_ids

    def get_d_STOWING_seq_num_to_port_name(self, df_onboard_loadlist: pd.DataFrame) -> dict:
        d_port_name_to_seq_num = self.__d_port_name_to_seq_num.copy()
        l_map_keys = d_port_name_to_seq_num.keys()
        map_len = len(l_map_keys)
        port_not_in_map_counter = 0

        l_unique_POLs_PODs_names = list(set(df_onboard_loadlist["LoadPort"].tolist() + df_onboard_loadlist["DischPort"].tolist()))
        for port_name in l_unique_POLs_PODs_names:
            if port_name not in l_map_keys:
                # to start after the last seq num in d_port_name_to_seq_num (inherited from self.__d_port_name_to_seq_num)
                # the seq num is just fictional for future PODs not in rotation (to not throw an error while computing restows)
                # we only care about the order of futre ports IN THE ROTATION
                port_seq_num = port_not_in_map_counter + map_len
                d_port_name_to_seq_num[port_name] = port_seq_num
                port_not_in_map_counter += 1
        
        d_seq_num_to_port_name = { val: k for k, val in d_port_name_to_seq_num.items() }

        return d_seq_num_to_port_name

    # def __get_d_POLs_in_rotation_names_base_to_name(self, call_port_name_base: str, call_port_seq_num: int) -> dict:
    #     l_ports_names_base = []
    #     l_POLs_names_before_call_port = []
    #     prev_seq_num = call_port_seq_num - 1
    #     # -1 as end to reach 0 (the first seq num) and -1 as a step to loop in the reverse order
    #     for seq_num in range(prev_seq_num, -1, -1):
    #         port_name = self.__d_seq_num_to_port_name[seq_num]
            
    #         # port_name_base is the port name without the number at the end (if any)
    #         # e.g., if port_name was GBSOU2, than port_name_base would be GBSOU
    #         port_name_base = port_name[:5]

    #         # if we see the name base again -> we have reached the current port but in the past
    #         # => stop looking and exit at the last possible POL right after the current port (in the past)
    #         if port_name_base == call_port_name_base: 
    #             break

    #         # if GBSOU is in the list, then there was already a port (GBSOU, GBSOU2, and who knows? maybe GBSOU3) that was added to the list in the step below
    #         # => continue: because the first occurence of the port just before the current port was already found in a previous iteration and its name base was added to the list
    #         if port_name_base in l_ports_names_base:
    #             continue
            
    #         # if GBSOU not in list => port GBSOU have not passed yet => add name base to list
    #         l_ports_names_base.append(port_name_base) # step below (mentioned in the comment above)
            
    #         # add port name to the list (in the example given, GBSOU2)
    #         l_POLs_names_before_call_port.append(port_name) # when seq_num = 2 (port_name = GBSOU), port_name will not be added 

    #     d_map = {
    #         port_name_base: port_name
    #         for (port_name_base, port_name)
    #         in list(zip(l_ports_names_base, l_POLs_names_before_call_port))
    #     }
        
    #     return d_map

    # def __get_d_PODs_in_rotation_names_base_to_name(self, call_port_name_base: str, call_port_seq_num: int) -> dict:
    #     l_ports_names_base = []
    #     l_PODs_names_after_call_port = []
    #     for seq_num in range(call_port_seq_num+1, len(self.__d_seq_num_to_port_name)):
    #         port_name = self.__d_seq_num_to_port_name[seq_num]
    #         port_name_base = port_name[:5]

    #         # if we see the name base again -> we have reached the current port but in the future
    #         # => stop looking and exit at the last possible POD right before the current port (in the future)
    #         if port_name_base == call_port_name_base: 
    #             break

    #         if port_name_base in l_ports_names_base:
    #             continue
            
    #         l_ports_names_base.append(port_name_base)

    #         l_PODs_names_after_call_port.append(port_name)

    #     d_map = {
    #         port_name_base: port_name
    #         for (port_name_base, port_name)
    #         in list(zip(l_ports_names_base, l_PODs_names_after_call_port))
    #     }

    #     return d_map

    # def map_POLs_in_df(self, df: pd.DataFrame, call_port_name_base: str, call_port_seq_num: int) -> pd.DataFrame:
        
    #     l_df_POLs_names = df["LOC_9_LOCATION_ID"].tolist()
    #     d_POLs_in_rotation_name_base_to_name = self.__get_d_POLs_in_rotation_names_base_to_name(call_port_name_base, call_port_seq_num) # this map is for POLs in rotation

    #     l_POLs_in_rotation_names_base = list(d_POLs_in_rotation_name_base_to_name.keys())
    #     l_mapped_POLs_names = [
    #         d_POLs_in_rotation_name_base_to_name[POL_name] if POL_name in l_POLs_in_rotation_names_base
    #         else POL_name
    #         for POL_name in l_df_POLs_names
    #     ]
    #     df["LOC_9_LOCATION_ID"] = l_mapped_POLs_names

    #     return df, d_POLs_in_rotation_name_base_to_name, l_POLs_in_rotation_names_base
    
    # def map_PODs_in_df(
    #         self,
    #         df: pd.DataFrame,
    #         d_POLs_in_rotation_name_base_to_name: dict,
    #         l_POLs_in_rotation_names_base: list,
    #         l_past_POLs_names: list,
    #         call_port_name_base: str,
    #         call_port_seq_num: int
    #     ) -> pd.DataFrame:
    #     # print(d_POLs_in_rotation_name_base_to_name)
    #     # print(l_past_POLs_names)
    #     # print(l_POLs_in_rotation_names_base)
    #     call_port_name_base = self.__d_seq_num_to_port_name[call_port_seq_num][:5]

    #     # self.__l_ports_in_rotation_names
    #     l_df_PODs_names = df["LOC_11_LOCATION_ID"].tolist()
    #     d_PODs_in_rotation_name_base_to_name = self.__get_d_PODs_in_rotation_names_base_to_name(call_port_name_base, call_port_seq_num)
    #     # print(d_PODs_in_rotation_name_base_to_name)
    #     l_PODs_in_rotation_names_base = list(d_PODs_in_rotation_name_base_to_name.keys())
    #     # print(l_PODs_in_rotation_names_base)
    #     l_mapped_PODs_names = []
    #     for POD_name in l_df_PODs_names:
    #         # if self.__d_seq_num_to_port_name[call_port_seq_num] == "GBSOU2":
    #         #     print(POD_name)
    #         #     print(l_PODs_in_rotation_names_base)
    #         #     print(l_POLs_in_rotation_names_base)
    #         #     if POD_name in l_POLs_in_rotation_names_base:
    #         #         print(d_POLs_in_rotation_name_base_to_name[POD_name])
    #         #     print(l_past_POLs_names)

    #         # POD in rotation and already in seq map (l_PODs_in_rotation_names_base is derived from seq map)
    #         if POD_name in l_PODs_in_rotation_names_base:
    #             mapped_POD_name = d_PODs_in_rotation_name_base_to_name[POD_name]
            
    #         # POD is a past POL in the rotation
    #         elif POD_name in l_POLs_in_rotation_names_base:
    #             past_port = d_POLs_in_rotation_name_base_to_name[POD_name]
    #             if len(past_port) == 6: # has an extension
    #                 past_port_extension = int(past_port[-1])
    #                 POD_extension = past_port_extension + 1
                
    #             else:
    #                 POD_extension = 2
                
    #             mapped_POD_name = f"{POD_name}{POD_extension}" # future port

    #         # POD not in rotation but in past POLs
    #         elif POD_name in l_past_POLs_names:
    #             mapped_POD_name = f"{POD_name}2"

    #         #TODO find a way to link this to anomaly detection: we should throw an error in this case
    #         else:
    #             mapped_POD_name = POD_name
            
            
    #         l_mapped_PODs_names.append(mapped_POD_name)

    #     df["LOC_11_LOCATION_ID"] = l_mapped_PODs_names

    #     if self.__d_seq_num_to_port_name[call_port_seq_num] == "GBSOU2":
    #         print(l_mapped_PODs_names)

    #     return df

    # def map_GBSOU_POLs_PODs(self, attributes_df: pd.DataFrame, GBSOU_num: int, GBSOU2_num: int, port_num_in_folder: int) -> pd.DataFrame:
    #     if port_num_in_folder < GBSOU_num or port_num_in_folder >= GBSOU2_num:
    #         POLs_list = [ pol if pol != "GBSOU" else "GBSOU2" for pol in attributes_df["LOC_9_LOCATION_ID"].values ]
    #         attributes_df.loc[:, "LOC_9_LOCATION_ID"] = POLs_list

    #     elif port_num_in_folder > GBSOU_num and port_num_in_folder < GBSOU2_num:
    #         PODs_list = [ pod if pod != "GBSOU" else "GBSOU2" for pod in attributes_df["LOC_11_LOCATION_ID"].values ]
    #         attributes_df.loc[:, "LOC_11_LOCATION_ID"] = PODs_list

    #     return attributes_df

    def __get_bbrrtt_and_deck_from_slot_position(self, slot_position: str, with_deck: bool=False) -> tuple:
        if len(slot_position) == 5: slot_position = "0" + slot_position
        elif len(slot_position) == 7: slot_position = slot_position[1:]
        bay, row, tier = slot_position[0:2], slot_position[2:4], slot_position[4:]
        if int(tier) < 60: hold_or_deck = "0"
        if int(tier) > 60: hold_or_deck = "1"

        if with_deck:
            return bay, row, tier, hold_or_deck

        return bay, row, tier

    def get_type_to_size_map_dict(self, onboard_list_df: pd.DataFrame, POL_container_types_list: list) -> dict:
        onboard_container_types_list = onboard_list_df["Type"].tolist()
        container_sizes_list = onboard_list_df["Size"].tolist()
        type_size_map_dict = { container_type: container_size for container_type, container_size in list(zip(onboard_container_types_list, container_sizes_list)) if container_type in POL_container_types_list }
        
        return type_size_map_dict

    def get_d_container_info_by_bay_row_tier(self, slot_positions_list: list, container_types_list: list, containers_ids_list: list) -> dict:
        d_container_info_by_bay_row_tier = {}
        for slot_position, container_type, container_id in list(zip(slot_positions_list, container_types_list, containers_ids_list)):

            bay, row, tier, hold_or_deck  = self.__get_bbrrtt_and_deck_from_slot_position(slot_position, True) # bb stands for bay, rr stands for row,and tt for tier
            d_container_info_by_bay_row_tier[bay, row, tier] = {
                "container_id": container_id, "container_type": container_type, "hold_or_deck": hold_or_deck
            }
        
        return d_container_info_by_bay_row_tier

    def get_d_stacks_rows_by_bay_row_deck(self, df_stacks: pd.DataFrame) -> dict:
        df_stacks["Bay_Row_Deck"] = list(zip(df_stacks["Bay"], df_stacks["Row"], df_stacks["Tier"]))

        l_df_stacks_cols = df_stacks.columns.tolist()
        bay_row_deck_col = l_df_stacks_cols.pop(-1)

        d_stacks_rows_by_bay_row_deck = {
            df_stacks[bay_row_deck_col].tolist()[i]: \
                { col: df_stacks[col].tolist()[i] for col in l_df_stacks_cols } for i in range(len(df_stacks))
        }

        return d_stacks_rows_by_bay_row_deck

    def get_src_folder_name_from_out_csv_name(self, csv_name: str, split: bool=True) -> str:
        folder_name = csv_name.split(".")[0].replace("_container", "")
        if split:
            return folder_name.split("_")

        else:
            return folder_name
