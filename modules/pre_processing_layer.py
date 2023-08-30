import pandas as pd
import numpy as np
import re as re
from modules import common_helpers

class PreProcessingLayer():
    def __init__(self) -> None:
        #TODO hardcoded vars
        self.__port_num = 0
        self.__MAX_NB_FOR_POD_IN_SUBBAY = 3 # maximal number of containers for the POD to be considered in the subbay
    
    #region DATA TYPES PROCESSING
    def process_slot_str(self, slot: str, slot_len: int) -> str:
        if slot_len == 5:
            slot_processed = "0" + slot

        elif slot_len == 7 and slot[0] == "0":
            slot_processed = slot[1:]
        
        else:
            slot_processed = slot

        return slot_processed

    def process_slots(self, df: pd.DataFrame, slots_col_name_or_idx: str or int, is_read: bool=False) -> pd.DataFrame:
        #TODO review slots processing
        if type(slots_col_name_or_idx) == str:
            slots_col_name_or_idx = df.columns.get_loc(slots_col_name_or_idx)
        
        l_slots = df.iloc[:, slots_col_name_or_idx]
        l_slots_processed = []
        
        if not is_read:
            for slot in l_slots:
                if type(slot) == int:
                    slot_temp = str(slot)
                
                elif slot is None or slot != slot: # if None or nan (nan != nan in python)
                    slot_temp = ""
                
                elif type(slot) == float and slot == slot: # float but not a nan
                    slot_temp = str(int(slot))

                else: # if slot is str (even if empty string)
                    slot_temp = slot
                
                # now slot_temp is for sure a string
                slot_processed = self.process_slot_str(slot_temp, len(slot_temp))
                l_slots_processed.append(slot_processed)
            
            df.iloc[:, slots_col_name_or_idx] = l_slots_processed
        
        else:
            for slot in l_slots:
                if slot != slot:
                    slot == ""

                elif slot != "":
                    slot = str(int(slot))

                slot_processed = self.process_slot_str(slot, len(slot))
                l_slots_processed.append(slot_processed)

            df.iloc[:, slots_col_name_or_idx] = [ self.process_slot_str(str(slot), len(str(slot))) for slot in l_slots ]

        return df

    def process_slots_out(self):
        pass

    def get_df_as_list_of_lines(self, df: pd.DataFrame, sep: str=";") -> list:
        df_temp = df.copy()
        df_temp = df_temp.astype(str)
        l_headers = [ sep.join(df.columns) ]

        l_df_values = df_temp.values.tolist()
        l_values = [ sep.join(li) for li in l_df_values ]

        l_lines = l_headers + l_values
        
        return l_lines

    def get_list_of_lines_as_df(self, l_lines: list, dtypes_as_str: bool=True, sep: str=";") -> pd.DataFrame:
        l_headers = l_lines[0].split(sep)

        l_str_values = l_lines[1:]
        l_values = [ li.split(sep) for li in l_str_values ]

        if dtypes_as_str: df = pd.DataFrame(l_values, columns=l_headers, dtype=str)
        else: df = pd.DataFrame(l_values, columns=l_headers)

        return df
    #endregion DATA TYPES PROCESSING

    #region ROTATION CSV

    # def enrich_d_port_num_moves(self, port_name: str, d_port_name_seq_map: dict, d_port_num_moves: dict, df_port_containers: pd.DataFrame) -> dict:
    #     port_name_num = d_port_name_seq_map[port_name]

    #     if port_name_num != 1:
    #          # add number of containers in loadlist edi of port_name
    #         d_port_num_moves[port_name] += len(df_port_containers)
    #     # print(port_name)
    #     # print(port_name, ":", d_port_num_moves[port_name])
        
    #     for port in d_port_num_moves.keys():
    #         # if port is one of the a next ports (after port_name) => add number of containers in port_name having the POL as port
    #         if d_port_name_seq_map[port] > port_name_num: 
    #             d_port_num_moves[port] += len(df_port_containers[df_port_containers["LOC_11_LOCATION_ID"].str.contains(port)]) #  if port not in LOC_11_LOCATION_ID col, the result of len is 0            
    #             # print(port, ":", d_port_num_moves[port])

    #     # print("*"*100)
        
    

    #endregion ROTATION CSV

    #region CONTAINERS ONBOARD LOADLIST

    def __add_settings_to_df(self, attributes_df: pd.DataFrame) -> pd.DataFrame:
        """
        Takes two columns from attributes, the indicator on whether a container is full or empty, and the values for the temperature setting value,
        and create the Specials column and adds it to df. The logic is as follows:
        1- if the indicator is 4 => empty container and for sure it does not have a temp setting val => setting = "E" for Empty
        2- if the indicator is 5 => full container => check if temp setting val exists => if yes => setting = "R" for Reefer
                                                                                       => if no => no setting => setting = ""
        3- else => setting = ""

        Parameters
        ----------
        df
            a pandas dataframe consisting of the specific columns for the python preprocessing scripts for the CPlex model

        container_full_or_empty_series
            a pandas series (a column from attributes_df) containing indicators on whether a container is full (5) or empty (4)

        temp_vals_series
            a pandas series (a column from attributes_df) containing temperature values (digits if there is a setting value, empty string otherwise)

        Returns
        -------
        df
            the same input dataframe but after adding to it the Specials column
        """
        container_full_or_empty_series = attributes_df["EQD_FULL_OR_EMPTY_INDICATOR_CODE"].astype(str).tolist()
        temp_vals_series = attributes_df["TMP_TEMPERATURE_DEGREE"].astype(str).tolist()
        
        specials_list = [
            "E" if container_full_or_empty_series[i] == "4" \
            else "R" if container_full_or_empty_series[i] == "5" and temp_vals_series[i] != "" \
            else "" \
            for i in range(len(container_full_or_empty_series))
        ]

        return specials_list

    def __get_size_and_height_vals_from_codes(self, d_iso_codes_size: dict, d_iso_codes_height: dict, size_code: str, height_code: str) -> str:
        size_val = d_iso_codes_size[size_code]
        
        # if iso codes indicate different heights, we’ll switch to the default:
        # 20’s are standard => height_val = ""; 40s are HC
        if height_code not in d_iso_codes_height.keys():
            if size_val == "20":
                height_val = ""
            else:
                height_val = "HC"
        else:
            height_val = d_iso_codes_height[height_code]

        return size_val, height_val

    def __add_sizes_and_heights_to_df(self, df: pd.DataFrame, d_iso_codes_map: dict) -> pd.DataFrame:
        d_old_iso_codes_size = d_iso_codes_map["old"]["length_ft"]
        d_old_iso_codes_height = d_iso_codes_map["old"]["height"]

        d_new_iso_codes_size = d_iso_codes_map["new"]["length_ft"]
        d_new_iso_codes_height = d_iso_codes_map["new"]["height"]

        l_iso_codes = df["Type"].tolist()
        l_sizes = []
        l_heights = []
        for iso_code in l_iso_codes:
            is_old = common_helpers.is_iso_code_old(iso_code)
            
            size_code = iso_code[0]
            height_code = iso_code[1]

            if is_old:
                size_val, height_val = self.__get_size_and_height_vals_from_codes(d_old_iso_codes_size, d_old_iso_codes_height, size_code, height_code)

            else:
                size_val, height_val = self.__get_size_and_height_vals_from_codes(d_new_iso_codes_size, d_new_iso_codes_height, size_code, height_code)

            l_sizes.append(size_val)
            l_heights.append(height_val)

        setting_col_idx = df.columns.get_loc("Setting")
        df.insert(setting_col_idx+1, "Size", l_sizes)
        df.insert(setting_col_idx+2, "Height", l_heights)
        
        return df

    def __preprocess_l_weights(self, l_weights: list) -> list:
        """
        Takes a list of weights, replaces empty strings with 0, convert to float, and finally divide by 1000 to transform weight from Kg to Tonnes.

        Parameters
        ----------
        l_weights
            a list that contains weights of containers

        Returns
        -------
        list
            the input list but preprocessed
        """
        # no need for fillna("0") as fillna("") was already applied to whole df in prior steps
        return (pd.Series(l_weights).replace("", "0").astype(float) / 1000).tolist()
        # return (pd.Series(l_weights).fillna("0").replace("", "0").astype(float) / 1000).tolist()

    def add_weights_to_df(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Takes a dataframe and add the weight column to that dataframe and returns it.

        Parameters
        ----------
        df
            a pandas dataframe containing container information about the containers (the extracted columns for the python preprocessing scripts for the CPlex model).

        Returns
        -------
        df
            same as input df but with a new preprocessed weight column, where the old weight columns are dropped
        """
        df_cols = df.columns
        d_weights_by_method = {"EQD_MEA_VGM_MEASURE": [], "EQD_MEA_AET_MEASURE": []}
        if "EQD_MEA_VGM_MEASURE" in df_cols:
            d_weights_by_method["EQD_MEA_VGM_MEASURE"] = df["EQD_MEA_VGM_MEASURE"].tolist()
        
        if "EQD_MEA_AET_MEASURE" in df_cols:
            d_weights_by_method["EQD_MEA_AET_MEASURE"] = df["EQD_MEA_AET_MEASURE"].tolist()

        if len(d_weights_by_method["EQD_MEA_VGM_MEASURE"]) and len(d_weights_by_method["EQD_MEA_AET_MEASURE"]):
            # fill missing values in Weight column (EQD_MEA_VGM) from Weight_2 column (EQD_MEA_AET)
            l_AET_weights = d_weights_by_method["EQD_MEA_AET_MEASURE"]
            l_VGM_weights = d_weights_by_method["EQD_MEA_VGM_MEASURE"]
            
            l_weights = [ 
                l_VGM_weights[i] \
                # no need for first condition (l_VGM_weights[i] == l_VGM_weights[i]) as nans replaced with "" in previous steps
                if l_VGM_weights[i] != ""  and int(l_VGM_weights[i]) != 0 \
                # if l_VGM_weights[i] == l_VGM_weights[i] and l_VGM_weights[i] != ""  and int(l_VGM_weights[i]) != 0 \
                else l_AET_weights[i] \
                for i in range(len(l_VGM_weights))
            ]
        
        elif len(d_weights_by_method["EQD_MEA_VGM_MEASURE"]) and not len(d_weights_by_method["EQD_MEA_AET_MEASURE"]):
            l_weights = d_weights_by_method["EQD_MEA_VGM_MEASURE"]

        elif not len(d_weights_by_method["EQD_MEA_VGM_MEASURE"]) and len(d_weights_by_method["EQD_MEA_AET_MEASURE"]):
            l_weights = d_weights_by_method["EQD_MEA_AET_MEASURE"]
        
        df["Weight"] = self.__preprocess_l_weights(l_weights) # weights will no longer contain empty strings (only zeros when there is no value)
        
        return df
## DG CLASS RELATED TOPIC

    def get_DGS_suffixes(self, row: pd.Series, l_DGS_HAZARD_ID_cols: list) -> str:
        l_row = row.tolist()
        
        l_DGS_row_vals = row[row!=""].tolist()
        min_val = "99"
        str_all_DG_classes_col_suffixes = ""
        str_lowest_DG_class_col_suffix = ""
        if len(l_DGS_row_vals):
            for val in l_DGS_row_vals:
                val_row_idx = l_row.index(val)
                l_row[val_row_idx] = "" # replace val to catch the second idx if val is repeated in next iteration
                DGS_col_name_split = l_DGS_HAZARD_ID_cols[val_row_idx].split("_")
                DGS_col_id_suffix = "_" + DGS_col_name_split[-1]
                DGS_col_id_suffix_with_sep = DGS_col_id_suffix + ";"
                str_all_DG_classes_col_suffixes += DGS_col_id_suffix_with_sep

                if val < min_val:
                    min_val = val
                    str_lowest_DG_class_col_suffix = DGS_col_id_suffix

        str_all_DG_classes_col_suffixes = str_all_DG_classes_col_suffixes[:-1] # to remove the last ;

        if not len(str_lowest_DG_class_col_suffix):
            str_lowest_DG_class_col_suffix = "_1"
        #print(str_all_DG_classes_col_suffixes)
        # # lowest class
        # DGS_row_min = min(l_DGS_row_vals) if len(l_DGS_row_vals) else ""
        # DGS_row_min_idx = l_row.index(DGS_row_min)
        # DGS_col_split_with_row_min_idx = l_DGS_HAZARD_ID_cols[DGS_row_min_idx].split("_")
        # DGS_id_num = DGS_col_split_with_row_min_idx[1]
        # DGS_id_num_prefix = "_".join([DGS_col_split_with_row_min_idx[0], DGS_id_num, ""])

        return str_lowest_DG_class_col_suffix, str_all_DG_classes_col_suffixes
## LOWEST exists implied _ OLD 
    def __get_df_DG_classes_filtered(self, df_DG_LoadList: pd.DataFrame, df_DG_classes_expanded: pd.DataFrame) -> pd.DataFrame:
        l_DG_classes = df_DG_LoadList[['Class', 'SubLabel1']].values.ravel()
        l_DG_classes = list(pd.Series(l_DG_classes).dropna().unique())
        l_DG_classes = [x for x in l_DG_classes if x!='']
        l_DG_classes = [float(x) for x in l_DG_classes]
        l_indices_to_drop = [ idx for idx in df_DG_classes_expanded.index if idx not in l_DG_classes ]
    
        df_DG_classes_filtered = df_DG_classes_expanded.drop(l_indices_to_drop, axis=1, inplace=False)
        df_DG_classes_filtered.drop(l_indices_to_drop, axis=0, inplace=True)
    
        DG_columns = [str(int(x)) if x.is_integer() else str(x) for x in df_DG_classes_filtered.columns]

        df_DG_classes_filtered.index = DG_columns
        df_DG_classes_filtered.columns = DG_columns

        df_DG_classes_filtered.replace(["*", "X"], "", inplace=True)

        return df_DG_classes_filtered


    #NOTE ask about filtering the DG classes from table
    def get_df_DG_classes_grouped(self, df_DG_LoadList: pd.DataFrame, df_DG_classes_expanded: pd.DataFrame) -> pd.DataFrame:
        df_DG_classes_filtered = self.__get_df_DG_classes_filtered(df_DG_LoadList, df_DG_classes_expanded)
        d_rows_as_dicts = df_DG_classes_filtered.to_dict(orient="dict")
        l_d_rows = []
        l_rows_keys = []
        for k in d_rows_as_dicts.keys():
            d_row = d_rows_as_dicts[k]
 
            if d_row not in l_d_rows:
                l_d_rows.append(d_row)
                l_rows_keys.append(k)

            else:
                d_row_idx_in_list = l_d_rows.index(d_row)
                old_k = l_rows_keys[d_row_idx_in_list]
                new_k = f"{old_k},{k}"
                l_rows_keys[d_row_idx_in_list] = new_k

        d_rows_as_dicts_grouped = {}
        for k, row in list(zip(l_rows_keys, l_d_rows)):
            d_rows_as_dicts_grouped[k] = row

        df_grouped = pd.DataFrame(d_rows_as_dicts_grouped)
        df_grouped.drop_duplicates(inplace=True)
        df_grouped.index = df_grouped.columns

        return df_grouped

    def get_df_DG_classes_grouped_to_save(self, df_grouped: pd.DataFrame) -> pd.DataFrame:
        df_grouped_to_save = df_grouped.copy()
        df_grouped_to_save.insert(0, "CLASS", df_grouped_to_save.index.tolist())
        df_grouped_to_save.index = [ str(i) for i in range(len(df_grouped_to_save)) ]

        return df_grouped_to_save

    def __add_DG_classes_to_df(self, df_all_containers: pd.DataFrame, df_DG_classes_grouped: pd.DataFrame) -> list:
        l_DG_classes = df_all_containers["DGS_HAZARD_ID"].tolist()
        l_unique_DG_classes = df_DG_classes_grouped.columns.tolist()
        for i, DG_class in enumerate(l_DG_classes):
            for grouped_class in l_unique_DG_classes:
                if len(DG_class) and DG_class in grouped_class: # if DG_class not an empty string
                    l_DG_classes[i] = grouped_class
                    continue # no need to proceed if grouped_class already found
        
        return l_DG_classes
    
    def get_df_containers_onboard_loadlist(
            self,
            df_all_containers: pd.DataFrame,
            df_DG_classes_grouped: pd.DataFrame,
            d_cols_map: dict,
            d_iso_codes_map: dict
        ) -> pd.DataFrame:

        onboard_loadlist_attributes_df_cols = [ k for k in d_cols_map.keys() ]

        df_onboard_loadlist = df_all_containers.loc[:, onboard_loadlist_attributes_df_cols]
        df_onboard_loadlist.columns = [d_cols_map[col] for col in onboard_loadlist_attributes_df_cols]
        
        df_onboard_loadlist["Setting"] = self.__add_settings_to_df(df_all_containers)
        df_onboard_loadlist = self.__add_sizes_and_heights_to_df(df_onboard_loadlist, d_iso_codes_map)
        df_onboard_loadlist["Weight"] = df_all_containers["Weight"].tolist()
        df_onboard_loadlist["Slot"] = df_all_containers.iloc[:, 1].tolist() #TODO add new_container_data_flag instead of index 1
        df_onboard_loadlist["DG_Class"] = self.__add_DG_classes_to_df(df_all_containers, df_DG_classes_grouped)
        
        df_onboard_loadlist.fillna("", inplace=True)

        return df_onboard_loadlist

    #endregion CONTAINERS ONBOARD LOADLIST
    
    #region DG LOADLIST

    def __populate_states_list(self, states_list_to_map: list, reference_state: str, yes_no_vals_tuple: tuple) -> None:
        states_list_to_populate = []
        for states in states_list_to_map:
            if states == "" or states != states:
                states_list_to_populate.append(yes_no_vals_tuple[1])
                continue
            
            if any(reference_state == state for state in states):
                states_list_to_populate.append(yes_no_vals_tuple[0])
            else:
                states_list_to_populate.append(yes_no_vals_tuple[1])

        return states_list_to_populate

    def __get_DG_ATT_states_lists(self, attributes_df: pd.DataFrame) -> 'tuple[list, list, list, list, list]':
        #TODO revisit func
        ATT_HAZ_cols = []
        ATT_AGR_col = ""
        for col in attributes_df.columns:
            if "DETAIL_DESCRIPTION_CODE_" in col:
                if "DGS_ATT_HAZ" in col:
                    ATT_HAZ_cols.append(col)
            
                elif "DGS_ATT_AGR" in col:
                    ATT_AGR_col = col

    
        if len(ATT_HAZ_cols):
            ATT_HAZ_states_lists = [ attributes_df[ATT_HAZ_cols[i]].tolist() for i in range(len(ATT_HAZ_cols)) ]
        else:
            ATT_HAZ_states_lists = [ [("") for idx in attributes_df.index] ]
        
        if len(ATT_AGR_col):
            ATT_AGR_states_list = [ (val) for val in attributes_df[ATT_AGR_col].tolist() ]
        else:
            ATT_AGR_states_list = [ ("") for idx in attributes_df.index ]

        ATT_HAZ_states_lists_len = len(ATT_HAZ_states_lists)
        if ATT_HAZ_states_lists_len == 2:
            ATT_HAZ_cols_states = [ (state_one, state_two) for (state_one, state_two) in list(zip(ATT_HAZ_states_lists[0], ATT_HAZ_states_lists[1])) ]
        else:
            ATT_HAZ_cols_states = [ (state) for state in ATT_HAZ_states_lists[0] ]

        marine_pollutant_list = self.__populate_states_list(ATT_HAZ_cols_states, "P", ("yes", "no")) # polmar
        flammable_list = self.__populate_states_list(ATT_HAZ_cols_states, "FLVAP", ("x", ""))
        liquid_list = self.__populate_states_list(ATT_AGR_states_list, "L", ("x", ""))
        solid_list = self.__populate_states_list(ATT_AGR_states_list, "S", ("x", ""))

        return marine_pollutant_list, flammable_list, liquid_list, solid_list

    def __add_DG_ATT_states_to_df(self, df: pd.DataFrame, attributes_df: pd.DataFrame) -> pd.DataFrame:
        marine_pollutant_list, flammable_list, liquid_list, solid_list = self.__get_DG_ATT_states_lists(attributes_df)
        df["Marine Pollutant"] = marine_pollutant_list
        df["Liquid"] = liquid_list
        df["Solid"] = solid_list
        df["Flammable"] = flammable_list
        df["Non-Flammable"] = ""

        return df


    def _add_DG_missing_cols_to_df(self, df: pd.DataFrame) -> pd.DataFrame:
        missing_cols = [
            "Closed Freight Container", "Loading remarks",
            "SegregationGroup", "Stowage and segregation", "Package Goods", "Stowage Category", "not permitted bay 74", "Zone"
        ]

        for col in missing_cols:
            df[col] = ""

        return df

    def __reorder_df_DG_loadlist_cols(self, df_DG_loadlist: pd.DataFrame) -> pd.DataFrame:
        DG_cols_ordered_list = ("Serial Number;Operator;POL;POD;Type;Closed Freight Container;Weight;Regulation Body;Ammendmant Version;UN;Class;SubLabel1;SubLabel2;" +\
                                "DG-Remark (SW5 = Mecanical Ventilated Space if U/D par.A DOC);FlashPoints;Loading remarks;" +\
                                "Limited Quantity;Marine Pollutant;PGr;Liquid;Solid;Flammable;Non-Flammable;Proper Shipping Name (Paragraph B of DOC);" +\
                                "SegregationGroup;SetPoint;Stowage and segregation;Package Goods;Stowage Category;not permitted bay 74;Zone").split(";")

        df_DG_loadlist = df_DG_loadlist[DG_cols_ordered_list]

        return df_DG_loadlist
    #old code
    def get_df_DG_loadlist(self, df_onboard_loadlist: pd.DataFrame, df_all_containers: pd.DataFrame, d_cols_map: dict) -> pd.DataFrame:
        df_DG_containers = df_all_containers[df_onboard_loadlist["DG_Class"]!=""]
        df_DG_loadlist_cols = [k for k in d_cols_map.keys() if k in df_DG_containers.columns]
        df_DG_loadlist = df_DG_containers.loc[:,df_DG_loadlist_cols]
        df_DG_loadlist.columns = [d_cols_map[col] for col in df_DG_loadlist_cols]

        df_DG_loadlist = self.__add_DG_ATT_states_to_df(df_DG_loadlist, df_DG_containers)
        df_DG_loadlist = self._add_DG_missing_cols_to_df(df_DG_loadlist)
        df_DG_loadlist = self.__reorder_df_DG_loadlist_cols(df_DG_loadlist)
        
        df_DG_loadlist.fillna("", inplace=True) # just in case
               
        return df_DG_loadlist

    def get_df_DG_loadlist_exhaustive(self, df_all_containers: pd.DataFrame, d_cols_names: dict) -> pd.DataFrame:
        
        df_DG_containers = df_all_containers[df_all_containers['DGS_HAZARD_ID_1']!=""]
        df_DG_containers = pd.wide_to_long(df_DG_containers,
                        stubnames=[
                                    'DGS_REGULATIONS_CODE_',
                                    'DGS_HAZARD_CODE_VERSION_ID_',
                                    'DGS_HAZARD_ID_',
                                    'DGS_DGS_SUB_LABEL1_',
                                    'DGS_DGS_SUB_LABEL2_',
                                    'DGS_UNDG_ID_',
                                    'DGS_SHIPMENT_FLASHPOINT_DEGREE_',
                                    'DGS_ATT_AGR_DETAIL_DESCRIPTION_CODE_',
                                    'DGS_MEASUREMENT_UNIT_CODE_',
                                    'DGS_PACKAGING_DANGER_LEVEL_CODE_',
                                    'DGS_ATT_PSN_DETAIL_DESCRIPTION_',
                                    'DGS_ATT_QTY_DETAIL_DESCRIPTION_CODE_',
                                    'DGS_MEA_AAA_MEASURE_',
                                    'DGS_MEA_AAA_MEASUREMENT_UNIT_CODE_',
                                    'DGS_FTX_FREE_TEXT_DESCRIPTION_CODE_',
                                    'DGS_ATT_HAZ_DETAIL_DESCRIPTION_CODE_'
                                ], 
                        i=['EQD_ID','LOC_9_LOCATION_ID','LOC_11_LOCATION_ID'],
                        j='variable'
                        )
        
        df_DG_containers = df_DG_containers[df_DG_containers['DGS_HAZARD_ID_']!=""]
        df_DG_containers = df_DG_containers.reset_index()

    
        df_DG_loadlist = df_DG_containers[['EQD_ID','EQD_NAD_CF_PARTY_ID','LOC_9_LOCATION_ID',
              'LOC_11_LOCATION_ID','EQD_SIZE_AND_TYPE_DESCRIPTION_CODE',
              'DGS_REGULATIONS_CODE_','DGS_HAZARD_CODE_VERSION_ID_',
              'DGS_HAZARD_ID_','DGS_DGS_SUB_LABEL1_','DGS_DGS_SUB_LABEL2_',
              'DGS_UNDG_ID_','DGS_SHIPMENT_FLASHPOINT_DEGREE_','DGS_ATT_AGR_DETAIL_DESCRIPTION_CODE_',
              'DGS_MEASUREMENT_UNIT_CODE_', 'DGS_PACKAGING_DANGER_LEVEL_CODE_',
              'DGS_ATT_PSN_DETAIL_DESCRIPTION_','DGS_ATT_QTY_DETAIL_DESCRIPTION_CODE_',
              'DGS_MEA_AAA_MEASURE_','DGS_MEA_AAA_MEASUREMENT_UNIT_CODE_',
              'DGS_FTX_FREE_TEXT_DESCRIPTION_CODE_','DGS_ATT_HAZ_DETAIL_DESCRIPTION_CODE_',
              'TMP_TEMPERATURE_DEGREE','TMP_TEMPERATURE_MEASUREMENT_UNIT_CODE'
              ]].copy()
        

        df_DG_loadlist = self.__add_DG_ATT_states_to_df(df_DG_loadlist, df_DG_containers)
        df_DG_loadlist = self._add_DG_missing_cols_to_df(df_DG_loadlist)
        df_DG_loadlist.rename(columns = d_cols_names, inplace=True)  
        df_DG_loadlist = self.__reorder_df_DG_loadlist_cols(df_DG_loadlist)
        
        df_DG_loadlist.fillna("", inplace=True) # just in case
    

        return df_DG_loadlist
    #endregion DG LOADLIST

    #region FILLED TANKS

    # def __read_tanks_basic_infos(self):
    #     tanks_dir = f"{self.__input_dir_vessels}/Tanks"
    #     d_tanks_basic_infos = {}

    #     # only relevant files are in this directory
    #     for fn_tank in os.listdir(tanks_dir):
    #         filepath = f"{tanks_dir}/{fn_tank}" 
    #         # sort between raw text (extension .txt) and new text (extension .csv)
    #         f_name, f_extension = os.path.splitext(filepath)
    #         if f_extension != ".csv":
    #             continue
        
    #         f_tank = open(filepath, "r")
        
    #         for no_line, line in enumerate(f_tank):
            
    #             # no header
    #             l_items = line.split(";")
            
    #             # just read first line
    #             if no_line == 0:
    #                 tank_name = l_items[0]
            
    #                 capacity = float(l_items[1])
    #                 first_frame = int(l_items[2])
    #                 last_frame = int(l_items[3])
                
    #                 d_tanks_basic_infos[tank_name] = (capacity, first_frame, last_frame)
    #             else:
    #                 break
                
    #         f_tank.close()
        
    #     # complete manually for some (scrubbing) tanks.
    #     d_tanks_basic_infos["SCRUBBER HOLDING"] = (152.10, 28, 30)
    #     d_tanks_basic_infos["SCRUBBER RESIDUE"] = (200.32, 27, 30)
    #     d_tanks_basic_infos["SCRUBBER SILO 1"] = (58.35, 35, 39)
    #     d_tanks_basic_infos["SCRUBBER SILO 2"] = (46.54, 35, 39)
    #     d_tanks_basic_infos["SCRUBBER M/E PROC"] = (51.49, 40, 43)
    #     d_tanks_basic_infos["SCRUBBER G/E PROC"] = (15.54, 41, 43)

    #     # and override for 2 others
    #     d_tanks_basic_infos["NO.2 M/E CYL.O STOR.TK(S)"] = (55.48, 43, 45)
    #     d_tanks_basic_infos["M/E SYS.O SETT.TK(S)"] = (111.42, 45, 49)
        
    #     return d_tanks_basic_infos

    def __read_frames(self, l_frames_lines: list) -> 'tuple[dict, list]':
        
        # reading frames
        # keeping start and end positions is enough
        # in two structures
        d_frames = {}
        l_frames = []

        for no_line, line in enumerate(l_frames_lines):
            if no_line == 0: continue
            l_items = line.split(";")
            frame_no = int(l_items[0])
            frame_start = float(l_items[2])
            frame_end = float(l_items[3])
            d_frames[frame_no] = (frame_start, frame_end)
            l_frames.append((frame_no, frame_start, frame_end))

        # just to be sure (re)sort the list
        l_frames.sort(key=lambda x: x[0])
        
        return d_frames, l_frames

    def __read_blocks(self, d_frames: dict, l_blocks_lines: list) -> 'tuple[list, dict]':
        l_blocks = []
        # no need for a dictionary, no_block starts at 1, add 1 to index if necessary
        for no_line, line in enumerate(l_blocks_lines):
            if no_line == 0: continue
            l_items = line.split(";")
            block_no = int(l_items[0])
            first_frame = int(l_items[1])
            last_frame = int(l_items[2])
            pos_start = d_frames[first_frame][0]
            pos_end = d_frames[last_frame][1]
            # get also, if making sense, the geometrical middle of the container stacks
            # i.e. the gravity center for the container bays
            # either (and rarely) directly in the file
            pos_bay_xcg = None
            if l_items[5] != "":
                pos_bay_xcg = float(l_items[5])
            # or from the bay frames    
            else:
                if l_items[6] != "" and l_items[7] != "":
                    first_bay_frame = int(l_items[6])
                    last_bay_frame = int(l_items[7])
                    pos_bay_xcg = 0.5 * (d_frames[first_bay_frame][0] + d_frames[last_bay_frame][1])
            no_bay = l_items[3]
            coeff_bay = float(l_items[4])
        
            l_blocks.append((block_no, pos_start, pos_end, first_frame, last_frame, pos_bay_xcg, no_bay, coeff_bay))

        # just to be sure (re)sort the list
        l_blocks.sort(key=lambda x: x[0])

        # we need also the reverse, from a frame, give to which (unique) block it belongs
        d_frames_block = {}
        for (block_no, pos_start, pos_end, first_frame, last_frame, pos_bay_xcg, no_bay, coeff_bay) in l_blocks:
            for no_frame in range(first_frame, last_frame+1):
                d_frames_block[no_frame] = block_no
                
        return l_blocks, d_frames_block

    # ugly, but keep things in the same pattern
    def read_tank_elems(self) -> 'tuple[list, str, str, list]':
        
        # list of tank types such as read in the edi file and to be selected
        # no water ballast, no void
        l_sel_tank_types = [
            "HEAVY FUEL O..",
            "LUBRIC.OIL.",
            "DIESEL OIL.",
            "FRESH WATER.",
            "MISCELLANEOUS."
        ]

        void_tank_type = "VOID SPACES."
        wb_tank_type = "WATERBALLAST."
        #l_unknown_tanks = ["SCRUBBER HOLDING", "SCRUBBER RESIDUE",
        #                   "SCRUBBER SILO 1", "SCRUBBER SILO 2", 
        #                   "SCRUBBER M/E PROC", "SCRUBBER G/E PROC"]
        l_unknown_tanks = []
        
        return l_sel_tank_types, void_tank_type, wb_tank_type, l_unknown_tanks

    def get_run_parameters(self):
        BV_condition = None
        WB_compensating_trim = True
        edi_tank_format = None
        filter_out_wb = None

        if edi_tank_format is None:
            if BV_condition is None: 
                edi_tank_format = "EDI_CRLF"
            else:
                if WB_compensating_trim == False:
                    edi_tank_format = "EDI_QUOTE"
                else:
                    edi_tank_format = "EDI_CRLF"
        if filter_out_wb is None:
            if BV_condition is None: 
                filter_out_wb = True
            else:
                filter_out_wb = False

        return BV_condition, WB_compensating_trim, edi_tank_format, filter_out_wb

    def l_get_filled_tanks_port_infos(self, l_rows, port_name, port_name_extension, 
                                  void_tank_type, l_unknown_tanks, l_sel_tank_types,
                                  filter_out_wb=True):
        l_filled_tanks_port = []
        
        for no_row, row in enumerate(l_rows):
            
            # useless header
            if no_row in [0, 1, 2, 3, 4, 6, 7, 8]:
                continue
            
            # in header, get the port if to be read there
            if no_row == 5:
                if port_name == "":
                    port = row[6:11] + port_name_extension
                else:
                    port = port_name + port_name_extension
                continue
            
            # useless tail
            if row[0:3] in ["UNT", "UNZ"]:
                continue
            
            # tanker rows
            #print(row)
            
            # tank name
            if row[0:3] == "LOC":
                s_l_name = row.split(":")
                name = s_l_name[3]
            # tank weight
            if row[0:6] == "MEA+WT":
                weight = float(row[12:])
            if row[0:7]  == "MEA+VOL":
                volume = float(row[13:])
            if row[0:3] == "DIM":
                s_l_cg = row[10:]
                l_s_cg = s_l_cg.split(":")
                l_cg = float(l_s_cg[0])
                # potential sign inconsistency...
                t_cg = -1 * float(l_s_cg[1])
                v_cg = float(l_s_cg[2])
                
            # most importantely, and at the end..., if wb to be filtered
            if row[0:3] == "FTX":
                tank_to_be_kept = True
                tank_type = row[10:]
                # no void
                if tank_type == void_tank_type:
                    tank_to_be_kept = False
                # no "unknown" tanks (scrubber in addendum)
                if name in l_unknown_tanks:
                    tank_to_be_kept = False
                # no water ballast if filter...
                if filter_out_wb == True and tank_type not in l_sel_tank_types:
                    tank_to_be_kept = False
                # if OK, store what had been saved
                if tank_to_be_kept == True:
                    l_filled_tanks_port.append((port, name, volume, weight, l_cg, t_cg, v_cg))
                        
        return l_filled_tanks_port
    
    def __get_elem_weight_in_segment(self, seg_start, seg_end, elem_start, elem_end, height_start, delta_height):
        # not relevant cases
        if seg_end <= seg_start: return 0.0
        if seg_start > elem_end or seg_end < elem_start: return 0.0
        
        elem_length = elem_end - elem_start
        # just in case...
        if elem_length <= 0: return 0.0
        seg_length = seg_end - seg_start
        seg_height = height_start 
        seg_height += (delta_height / elem_length) * ((seg_start - elem_start) + (seg_length / 2))
        seg_weight = seg_length * seg_height
        
        return seg_weight

    def __get_elem_cg_weight_in_segment(self, seg_start, seg_end, elem_start, elem_end, height_start, delta_height):
        # not relevant cases
        if seg_end <= seg_start: return 0.0, 0.0
        if seg_start > elem_end or seg_end < elem_start: return 0.0, 0.0
        
        elem_length = elem_end - elem_start
        seg_length = seg_end - seg_start
        
        # apply the trapeze formula
        height_seg_start = height_start + (delta_height / elem_length) * (seg_start - elem_start)
        height_seg_end = height_start + (delta_height / elem_length) * (seg_end - elem_start)
        
        # in case of...
        if height_seg_start + height_seg_end <= 0:
            cg = seg_start + seg_length / 2
            weight = 0
            return cg, weight
            
        cg = seg_start
        cg += (seg_length / 3) * (height_seg_start + 2 * height_seg_end) / (height_seg_start + height_seg_end)
        
        weight = self.__get_elem_weight_in_segment(seg_start, seg_end, elem_start, elem_end, height_start, delta_height)
        
        return cg, weight

    def __get_height_start_delta(self, pos_start, pos_end, pos_lcg, weight):
        # supposing first a uniform weight = 1
        
        # ls: pos_start
        # le: pos_end
        # L = le - ls
        # lg: pos_lcg
        # Lg: lg - ls
        # gamma = Lg / L
        # W = S: assuming density = 1, whole weight taken up by "heights" 
        # hs: height_pos_start
        # he: height_end_start
        # delta_h = he - hs
        # alpha = (he - hs) / hs
        # then:
        # 1) S = L . hs . (1 + alpha/2)
        # 2) gamma = (hs/2 + delta_h/3) / (hs + delta_h/2) = (1/2 + alpha/3) + (1 + alpha/2)
        # or: alpha = (gamma - 1/2) / (1/3 - gamma/2)
        # hence, using alpha: 
        # hs = S / L(1 + alpha/2)
        # delta_h = alpha . S / L(1 + alpha/2)
        
        # we can use both values to compute the mass m of a segment l1, l2 (between ls and le)
        # m = s = (l2 - l1) . (hs + ((l1 - ls) + 1/2 .(l2 -l1)) . delta_h / L)
        
        length = (pos_end - pos_start)
        gamma = (pos_lcg - pos_start) / (pos_end - pos_start)
        # if gamma = 2/3, infinite slop, take alpha = 1000
        if gamma == 2/3:
            alpha = 100000000
        else:
            alpha = (gamma - (1/2)) / ((1/3) - (gamma/2))
        height_start = weight / (length * (1 + (alpha/2)))
        delta_height = alpha * height_start
        
        return height_start, delta_height   

    def __get_l_filled_tank_subtanks(self, first_frame, last_frame, volume, weight, l_cg, t_cg, v_cg,
                               d_frames, d_frames_block):
        
        # list of subtanks
        l_filled_tank_subtanks = []
        
        # division along x-axis, get the tank blocks with its frame (inside the tank)
        # note : range(first_frame, last_frame) means that the last frame considered is last_frame - 1 !
        # the frames given in the data are the frames as positions not as segments...
        d_tank_blocks = {}
        for no_frame in range(first_frame, last_frame):
            id_block = d_frames_block[no_frame]
            if id_block not in d_tank_blocks:
                d_tank_blocks[id_block] = []
            d_tank_blocks[id_block].append(no_frame)
        
        # sorting
        l_tank_blocks = [(id_block, l_no_frames) for id_block, l_no_frames in d_tank_blocks.items()]
        l_tank_blocks.sort(key=lambda x: x[0])
        for (id_block, l_no_frames) in l_tank_blocks:
            l_no_frames.sort()
        
        # getting block id (+1 already in d_frames_block), x positions (start and end)
        l_tank_blocks_x = [(id_block, d_frames[l_no_frames[0]][0], d_frames[l_no_frames[-1]][1])\
                        for (id_block, l_no_frames) in l_tank_blocks]
        #print(l_tank_blocks_x)
        
        # determine the volumes, weights, x_cg, y_cg, z_cg for each subtank
        x_start_tank = l_tank_blocks_x[0][1]
        x_end_tank = l_tank_blocks_x[-1][2]
        x_len_tank = x_end_tank - x_start_tank
        # for the x_cg (100 useless, we are only interested by the cg)
        height_start, delta_height = self.__get_height_start_delta(x_start_tank, x_end_tank, l_cg, weight)
        
        # looping on x (volumes, weights, x_cg)
        l_infos_sub_blocks_x = []
        for no_block_x, (id_block_x, x_start_block_x, x_end_block_x) in enumerate(l_tank_blocks_x):
            x_len_sub_tank = x_end_block_x - x_start_block_x
            #volume_sub_tank = volume * x_len_sub_tank / x_len_tank
            #weight_sub_tank = weight * x_len_sub_tank / x_len_tank
            # instead of simple proportion...
            
            # not forgetting empty tanks
            if weight != 0.0:
                x_cg_sub_tank, weight_sub_tank = self.__get_elem_cg_weight_in_segment(x_start_block_x, x_end_block_x,
                                                                            x_start_tank, x_end_tank, 
                                                                            height_start, delta_height)
                volume_sub_tank = volume * (weight_sub_tank / weight)
            else:
                x_cg_sub_tank = 0.5 * (x_start_block_x + x_end_block_x)
                weight_sub_tank = 0.0
                volume_sub_tank = 0.0
            
            l_infos_sub_blocks_x.append((volume_sub_tank, weight_sub_tank, x_cg_sub_tank))
        
        # the easy part
        y_cg = t_cg
        z_cg = v_cg
        
        for no_block_x, (id_block_x, x_start_block_x, x_end_block_x) in enumerate(l_tank_blocks_x):
            # volume_st = l_infos_sub_blocks_x[no_block_x][0]
            weight_st = l_infos_sub_blocks_x[no_block_x][1]
            x_cg_st = l_infos_sub_blocks_x[no_block_x][2]
            l_filled_tank_subtanks.append((id_block_x, weight_st, x_cg_st, y_cg, z_cg)) # volume_st was the second element in the tuple
        
        return l_filled_tank_subtanks

    def __get_l_filled_subtanks(
            self,
            d_tanks_basic_infos: dict,
            d_tank_names_edi_2_bv: dict,
            l_frames_lines: list,
            l_blocks_lines: list,
            l_filled_tanks_ports: list
        ) -> list:
        # Identification, aligner noms edi TANKSTA sur noms doc BV...
        
        # d_tanks_basic_infos = self.__read_tanks_basic_infos()
        d_frames, l_frames = self.__read_frames(l_frames_lines)
        l_blocks, d_frames_block = self.__read_blocks(d_frames, l_blocks_lines)
        l_filled_subtanks = []
        for (port_name, tank_edi_name, volume, weight, l_cg, t_cg, v_cg) in l_filled_tanks_ports:
            
            # get first and last frame of the tank
            tank_name = d_tank_names_edi_2_bv[tank_edi_name]
            first_frame = d_tanks_basic_infos[tank_name][1]
            last_frame = d_tanks_basic_infos[tank_name][2]
            
            l_filled_tank_subtanks = self.__get_l_filled_tank_subtanks(first_frame, last_frame, volume, weight, l_cg, t_cg, v_cg,
                                                                    d_frames, d_frames_block)
            # complete with port and tank name
            # volume_st 4th element in the tuple
            l_filled_tank_subtanks = [
                (port_name, f"{tank_name}_{id_block_x}", tank_name, id_block_x, weight_st, x_cg_st, y_cg, z_cg) \
                for (id_block_x, weight_st, x_cg_st, y_cg, z_cg) in l_filled_tank_subtanks # volume_st 2nd element in the tuple
            ]
            
            l_filled_subtanks.extend(l_filled_tank_subtanks)

        return l_filled_subtanks

    def get_df_filled_subtanks(
            self,
            d_tanks_basic_infos: dict,
            d_tank_names_edi_2_bv: dict,
            l_frames_lines: list,
            l_blocks_lines: list,
            l_filled_tanks_ports: list,
            l_filled_subtanks_csv_cols: list
        ) -> pd.DataFrame:

        l_filled_subtanks = self.__get_l_filled_subtanks(
                d_tanks_basic_infos,
                d_tank_names_edi_2_bv,
                l_frames_lines,
                l_blocks_lines,
                l_filled_tanks_ports
            )
        
        filled_subtanks_df = pd.DataFrame(l_filled_subtanks, columns=l_filled_subtanks_csv_cols).round(decimals=3)

        return filled_subtanks_df
    #endregion FILLED TANKS

    #region STOWING INFO

    def __get_d_stacks(self, l_stacks_lines: list) -> dict:
        d_stacks = {}
        for no_line, line in enumerate(l_stacks_lines):
            
            if no_line == 0: continue

            l_items = line.split(";")
            bay = l_items[0] # sur 2 caractères
            row = l_items[1] # sur 2 caractères
            macro_tier = l_items[2]
            subbay = l_items[3]
            #if subbay[0] == "0": subbay = subbay[1:]
            first_tier = l_items[4]
            max_nb_std_cont = int(l_items[5])
            odd_slot = int(l_items[6])
            max_nb_45 = int(l_items[7])
            min_40_sub_45 = int(l_items[8])
            nb_reefer = int(l_items[9])
            max_weight = float(l_items[10])
            stack_height = float(l_items[11])
            max_nb_HC_at_max_stack = int(l_items[12])
            
            
            stack = (bay, row, macro_tier)

            d_stacks[stack] = {
                "subbay": subbay,
                "first_tier": first_tier, 
                "max_nb_std_cont": max_nb_std_cont,
                "odd_slot": odd_slot,
                "nb_reefer": nb_reefer,
                "max_nb_45": max_nb_45,
                "min_40_sub_45": min_40_sub_45,
                "max_nb_HC_at_max_stack": max_nb_HC_at_max_stack,
                "stack_height": stack_height,
                "max_weight": max_weight
            }

        return d_stacks

    # get the index (starting at 0) of a tier in a stack
    def __get_ix_tier(self, tier, stack, d_stacks):
        tier_no = int(tier)
        first_tier_no = int(d_stacks[stack]["first_tier"])
        return int((tier_no - first_tier_no) / 2)

    # conversely
    def __get_tier(self, ix_tier, stack, d_stacks):
        first_tier_no = int(d_stacks[stack]["first_tier"])
        tier_no = first_tier_no + 2 * ix_tier
        return "%02d" % tier_no

    # from a slot position, get its stack and its tier index
    def __get_slot_stack_ix_tier(self, slot_address, d_stacks):    
        if len(slot_address) == 5: slot_address = "0" + slot_address
        bay = slot_address[0:2]
        row = slot_address[2:4]
        tier = slot_address[4:6]
        if int(tier) < 60: macro_tier = "0"
        if int(tier) > 60: macro_tier = "1"
        stack = (bay, row, macro_tier)
        ix_tier = self.__get_ix_tier(tier, stack, d_stacks)
        
        return stack, ix_tier

    def __get_d_sb_capacities(self, l_subbays_lines: list) -> dict:
        # récupérer aussi les capacités totales des sous-baies (au cas où)
        d_sb_capacities = {}
        for no_line, line in enumerate(l_subbays_lines):
                
            if no_line == 0: continue
            
            l_items = line.split(";")
            subbay = l_items[0] 
            #if subbay[0] == "0": subbay = subbay[1:]
            cap_20_or_40 = int(l_items[4])
            cap_only_20 = int(l_items[5])
            cap_only_40 = int(l_items[6])
            capacity = 2 * cap_20_or_40 + cap_only_20 + 2 * cap_only_40
            
            d_sb_capacities[subbay] = capacity

        return d_sb_capacities

    # get the list of all stacks to consider when looking below (overstower)
    # in some cases, one or several of those stack may not exist really in the vessel, existence to be checked when used
    def __get_stacks_4_below(self, stack, only_hc_move=False):
        
        bay_ref = stack[0]
        row_ref = stack[1]
        macro_tier_ref = stack[2]
        
        bay_no = int(bay_ref)
        if bay_no % 2 == 0: l_bays = ["%02d" % (bay_no-1), "%02d" % (bay_no), "%02d" % (bay_no+1)]
        if bay_no % 4 == 1: l_bays = ["%02d" % (bay_no), "%02d" % (bay_no+1)]
        if bay_no % 4 == 3: l_bays = ["%02d" % (bay_no), "%02d" % (bay_no-1)]
    
        l_stacks_4_below = []
        for bay in l_bays:
            # at present macro tier
            if only_hc_move == False:
                l_stacks_4_below.append((bay, row_ref, macro_tier_ref))
            # plus the stack in the hold if we are on the deck
            if macro_tier_ref == "1":
                l_stacks_4_below.append((bay, row_ref, "0"))
        
        return l_stacks_4_below
        
    # get the list of all stacks to consider when looking upper (overstowed)
    # in some cases, one or several of those stack may not exist really in the vessel, existence to be checked when used
    def __get_stacks_4_above(self, stack, only_hc_move=False):
        
        bay_ref = stack[0]
        row_ref = stack[1]
        macro_tier_ref = stack[2]
        
        bay_no = int(bay_ref)
        if bay_no % 2 == 0: l_bays = ["%02d" % (bay_no-1), "%02d" % (bay_no), "%02d" % (bay_no+1)]
        if bay_no % 4 == 1: l_bays = ["%02d" % (bay_no), "%02d" % (bay_no+1)]
        if bay_no % 4 == 3: l_bays = ["%02d" % (bay_no), "%02d" % (bay_no-1)]
    
        l_stacks_4_above = []
        for bay in l_bays:
            # at present macro tier
            if only_hc_move == False:
                l_stacks_4_above.append((bay, row_ref, macro_tier_ref))
            # plus the stack in the deck if we are in the hold
            if macro_tier_ref == "0":
                l_stacks_4_above.append((bay, row_ref, "1"))
        
        return l_stacks_4_above

    # get the list of all stacks to consider when looking upper but to assess capacity impact
    # in some cases, one or several of those stack may not exist really in the vessel, existence to be checked when used
    def __get_stacks_4_capacity_above(self, stack):
        
        bay_ref = stack[0]
        row_ref = stack[1]
        macro_tier_ref = stack[2]
        
        bay_no = int(bay_ref)
        # just look at the 20" bays (except for 74 and 94!!)
        if bay_no % 2 == 0 and bay_no not in [74, 94]: l_bays = ["%02d" % (bay_no-1), "%02d" % (bay_no+1)]
        if bay_no % 2 == 0 and bay_no in [74, 94]: l_bays = ["%02d" % (bay_no)]
        if bay_no % 4 == 1: l_bays = ["%02d" % (bay_no), "%02d" % (bay_no+2)]
        if bay_no % 4 == 3: l_bays = ["%02d" % (bay_no), "%02d" % (bay_no-2)]
    
        l_stacks_4_capacity_above = []
        for bay in l_bays:
            # only at present macro tier
            l_stacks_4_capacity_above.append((bay, row_ref, macro_tier_ref))
        
        return l_stacks_4_capacity_above

    # for any slot, get the total capacity above
    def __total_capacity_above(self, slot, d_stacks, d_sb_capacities, only_hc_move=False):
        
        bay = slot[0:2]
        row = slot[2:4]
        tier = slot[4:6]
        
        bay_no = int(bay)
        row_no = int(row)
        tier_no = int(tier)
        if tier_no < 60: macro_tier = "0"
        if tier_no > 60: macro_tier = "1"
        
        origin_stack = (bay, row, macro_tier)
        # get the stacks for above (around in fact)
        l_stacks_4_above = self.__get_stacks_4_capacity_above(origin_stack)
        
        # get the number of slots above the current slot in one stack
        capacity_above = 0
        # only if looking also current
        if only_hc_move == False:
            for stack in l_stacks_4_above:
                # the generated stack may not exist (for instance "73")
                if stack in d_stacks:
                    ix_tier = self.__get_ix_tier(tier, stack, d_stacks)
                    nb_slots_above_in_stack = d_stacks[stack]["max_nb_std_cont"] - ix_tier - 1
                    # specific case of 40" only bays (74, 94)
                    if bay_no in [74, 94]:
                        nb_slots_above_in_stack *= 2
                    capacity_above += nb_slots_above_in_stack
                
        # if in the hold, add the total capacity of the deck sub-bay above
        if macro_tier == "0":
            stack_above = (bay, row, "1")
            if stack_above in d_stacks: # should always be the case
                subbay_above = d_stacks[stack_above]["subbay"]
                if subbay_above in d_sb_capacities: # should always be the case
                    capacity_above += d_sb_capacities[subbay_above]
        
        return capacity_above

    # is a slot a reefer slot 
    # slot as (bay, row, tier)
    def __is_reefer_slot(self, slot, d_stacks):

        if len(slot) == 7: slot = slot[1:]
        
        bay = slot[0:2]
        row = slot[2:4]
        tier = slot[4:6]
        
        bay_no = int(bay)
        row_no = int(row)
        tier_no = int(tier)
        if tier_no < 60: macro_tier = "0"
        if tier_no > 60: macro_tier = "1"

        # most of the cases, where no reefer at all
        if macro_tier == "0": return False
        if bay_no % 4 == 1: return False
        if row in ["19", "20"]: return False
        if bay in ["01", "02", "03", "94"]: return False
        
        # for other rows, use the tier
        reefer_low_tier = int(d_stacks[(bay, row, macro_tier)]["first_tier"])
        if bay in ["06", "07"] or row in ["17", "18"] or (bay in ["82", "83", "86", "87", "90", "91"] and row == "00"):
            nb_reefers_in_stack = 2
        else:
            nb_reefers_in_stack = 3
        reefer_high_tier = reefer_low_tier + (2 * (nb_reefers_in_stack-1))
        if tier_no >= reefer_low_tier and tier_no <= reefer_high_tier:
            return True
        
        return False

    ################################################################################
    # Liste de dictionnaires qui à chaque port associe un dictionnaire qui à chaque pile 
    # associe de bas en haut les caractéristiques des conteneurs (None si pas de conteneur)

    # Charger les résultats de slot planning ou onboard

    # initialiser tous les slots à None
    def __initialize_slots(self, d_stacks):
        
        d_stacks_slots = {}
        for stack, d_stack in d_stacks.items():
            nb_slots = d_stack["max_nb_std_cont"]
            d_stacks_slots[stack] = [None] * nb_slots
            
        return d_stacks_slots

    # Chargement On-board, maintenant on le fait uniquement à partir d'un format "CONSO"
    def __load_onboard_stacks(self, d_stacks: dict, l_onboard_lines: list):

        # same structure than results, but with port 0 as only port
        d_ports_slots = {}
                
        for no_line, line in enumerate(l_onboard_lines):
            
            if no_line == 0: continue
            l_items = line.split(";")
            
            # first read slot, if no slot, that's the loading list part, to be skipped
            slot = l_items[8].strip() # sur 6 caractères ou 0...
            slot_len = len(slot)
            slot = self.process_slot_str(slot, slot_len)
            if slot_len == 0: continue
        
            # subbay
            if slot_len == 5: slot = "0" + slot # just in case
            if slot_len == 7 and slot[0] == "0": slot = slot[1:]
            garbage = False # just in case
            if slot_len == 0: garbage = True
            if garbage == False:
                stack, ix_tier = self.__get_slot_stack_ix_tier(slot, d_stacks)
            
            # POL and POD
            # La gestion GBSOU / GBSOU2 a été effectuée lors de la constitution du
            # fichier onboard - loadlist, pas nécessaire de faire cela aussi
            # par contre la gestion de la séquence doit se faire d'après le contexte
            # testing the lengths just to make sure: handled POLs in enrichment layer and PODs in anomaly detection layer
            pol_name = l_items[1].strip()
            if len(pol_name):
                pol_seq = common_helpers.get_seq_num_from_port_name(self.__d_STOWING_port_name_to_seq_num, pol_name)
                
            pod_name = l_items[2].strip()
            if len(pod_name):
                pod_seq = common_helpers.get_seq_num_from_port_name(self.__d_STOWING_port_name_to_seq_num, pod_name)
        
            # container id 
            container_id = l_items[0].strip()

            # type
            #iso_type = l_items[3].strip()
            c_size = l_items[5].strip()
            hc = l_items[6].strip()
        
            # weight
            weight = float(l_items[7].strip()) # already in tons
        
            # specials, either "", "E" (empty), or "R" (effective reefer)
            # we must also determine if reefer or not
            specials = l_items[4].strip()
            if specials == "R": c_type = "RE"
            else: c_type = "GP"
            
            if garbage == False:
                
                # here, only one (first) port
                # (some files may contain several ports)
                #TODO ask Ioan: the above will handle right here (it should :p)
                port_seq = 0
                if port_seq not in d_ports_slots:
                    d_ports_slots[port_seq] = self.__initialize_slots(d_stacks)
                d_ports_slots[port_seq][stack][ix_tier] = (container_id, pol_seq, pod_seq, 
                                                        c_size, c_type, weight, hc)    
        
        l_ports_slots = [(stack, l_slots) for stack, l_slots in d_ports_slots[self.__port_num].items()]
        
        return d_ports_slots, l_ports_slots

    def __get_l_overstow(self, d_stacks: dict, d_ports_slots: dict, l_ports_slots: list, only_hc_moves: bool) -> list:
        # POUR POUVOIR UTILISER LES 2 VALEURS DE PARAMETRE 
        # only_hc_moves = False POUR ECRIRE "9454450 Containers Stowing Info"
        # only_hc_moves = True POUR ECRIRE "9454450 Overstowing Subbays"
        # TRANSFORMER LIGNES SUIVANTES EN FONCTION, A APPELER 2 FOIS...
        # EN ATTENDANT EXECUTER 2 FOIS AVEC LES VALEURS DE PARAMETRE DIFFERENT SI BESOIN

        l_overstow = []

        for (stack, l_slots) in l_ports_slots:
            bay = stack[0]
            row = stack[1]
            macro_tier = stack[2]
            # get the stacks of interest related to the current stack
            l_stacks_4_below = self.__get_stacks_4_below(stack, only_hc_moves)
            # look at each container (slot) of the current stack
            for ix, container in enumerate(l_slots):
                if container is None: continue
                container_id = container[0]
                # this dictionnary at the level of the (slot, container) contains for each POD < POD of the container,
                # the number of containers overstowed by the current container
                d_container_overstow = {}
                pod_no = container[2]
                tier_no = int(self.__get_tier(ix, stack, d_stacks))
                for stack_4_below in l_stacks_4_below:
                    if stack_4_below not in d_ports_slots[self.__port_num]: continue
                    l_slots_4_below = d_ports_slots[self.__port_num][stack_4_below]
                    for ix_4_below, container_4_below in enumerate(l_slots_4_below):
                        # selection on filled slot 
                        if container_4_below is None: continue
                        # being strictly below the current slot and with a pod strictly before the current pod
                        pod_no_4_below = container_4_below[2]
                        tier_no_4_below = int(self.__get_tier(ix_4_below, stack_4_below, d_stacks))
                        # add it to the dictionnary
                        if pod_no_4_below < pod_no and tier_no_4_below < tier_no:
                            if pod_no_4_below not in d_container_overstow:
                                d_container_overstow[pod_no_4_below] = 0
                            d_container_overstow[pod_no_4_below] += 1
                
                # now, append the dictionary to the list of overstows, 
                # with 3 elements, slot, container, dictionary of overstow, only if there are overstos
                if len(d_container_overstow) > 0:
                    l_overstow.append(((bay, row, "%02d" % tier_no), container_id, d_container_overstow))

        # sorting by the slot
        l_overstow.sort(key=lambda x: x[0])

        return l_overstow

    def __get_d_subbay_pods(self, d_stacks: dict, l_ports_slots: list) -> dict:

        # Isolated containers in a sub-bay
        # If no more than N (3) containers of the same POD in any given subbay

        # for each subbay, get the distribution of POL / POD
        d_subbay_pods = {}

        for (stack, l_slots) in l_ports_slots:
            
            subbay = d_stacks[stack]["subbay"]
            
            if subbay not in d_subbay_pods:
                d_subbay_pods[subbay] = {}
                
            for container in l_slots:
                if container is None: continue
                    
                pod_no = container[2]
                if pod_no not in d_subbay_pods[subbay]:
                    d_subbay_pods[subbay][pod_no] = 0
                d_subbay_pods[subbay][pod_no] += 1

        return d_subbay_pods

    def __get_l_potential_restows(self, d_stacks: dict, d_subbay_pods: dict, d_sb_capacities: dict, d_ports_slots: dict, l_ports_slots: list, only_hc_moves: bool) -> list:
        l_potential_restows = []

        for (stack, l_slots) in l_ports_slots:
            bay = stack[0]
            row = stack[1]
            macro_tier = stack[2]
            subbay = d_stacks[stack]["subbay"]
            # get the stacks of interest related to the current stack
            l_stacks_4_above = self.__get_stacks_4_above(stack, only_hc_moves)
            #print("stack:", stack)
            # look at each container (slot) of the current stack
            for ix, container in enumerate(l_slots):
                if container is None: continue
                container_id = container[0]
                # only consider containers from POD with less than N containers in the sub_bay
                pod_no = container[2]
                if d_subbay_pods[subbay][pod_no] > self.__MAX_NB_FOR_POD_IN_SUBBAY: continue
                tier_no = int(self.__get_tier(ix, stack, d_stacks))
                
                # set of PODS above the current container
                s_pod_above = set()
                
                for stack_4_above in l_stacks_4_above:
                    if stack_4_above not in d_ports_slots[self.__port_num]: continue
                    #print("stack 4 above:", stack_4_above)
                    l_slots_4_above = d_ports_slots[self.__port_num][stack_4_above]
                    for ix_4_above, container_4_above in enumerate(l_slots_4_above):
                        # selection on filled slot 
                        if container_4_above is None: continue
                        # being strictly above the current slot and with a pod strictly before the current pod
                        pod_no_4_above = container_4_above[2]
                        tier_no_4_above = int(self.__get_tier(ix_4_above, stack_4_above, d_stacks))
                        # add it to the set
                        if pod_no_4_above < pod_no and tier_no_4_above > tier_no:
                            if pod_no_4_above not in s_pod_above:
                                s_pod_above.add(pod_no_4_above)
                
                # now, append the dictionary to the list of overstows, 
                # with 4 elements, slot, container, dictionary of overstow, only if there are overstow
                # and also impact in terms of capacity 
                if len(s_pod_above) > 0:
                    slot = "%s%s%02d" % (bay, row, tier_no)
                    capacity_above = self.__total_capacity_above(slot, d_stacks, d_sb_capacities, only_hc_moves)
                    l_potential_restows.append(((bay, row, "%02d" % tier_no), container_id, 
                                                s_pod_above, capacity_above))

        # sorting by the slot
        l_potential_restows.sort(key=lambda x: x[0])

        return l_potential_restows

    def __get_l_non_reefers_at_reefer_and_vice_versa(self, d_stacks: dict, l_ports_slots: list) -> list:
        l_non_reefers_at_reefer = []
        l_reefers_at_non_reefer = []

        for (stack, l_slots) in l_ports_slots:
            bay = stack[0]
            row = stack[1]
            macro_tier = stack[2]
            # look at each container (slot) of the current stack
            for ix, container in enumerate(l_slots):
                if container is None: continue
                container_id = container[0]
                c_type = container[4]
                # is the slot a reefer slot ?
                tier_no = int(self.__get_tier(ix, stack, d_stacks))
                slot = "%s%s%02d" % (bay, row, tier_no)
                placed_in_reefer = self.__is_reefer_slot(slot, d_stacks)
                # is it a (real) reefer
                is_reefer = True if c_type == "RE" else False
                if placed_in_reefer == True and is_reefer == False:
                    l_non_reefers_at_reefer.append(((bay, row, "%02d" % tier_no), container_id))

                #TODO according to the last discussion with Ioan, this could be non-problematic for some containers => check with Ioan and check scripts
                if placed_in_reefer == False and is_reefer == True:
                    l_reefers_at_non_reefer.append((((bay, row, "%02d" % tier_no), container_id)))
                    
        # sorting by the slot
        l_non_reefers_at_reefer.sort(key=lambda x: x[0])
        l_reefers_at_non_reefer.sort(key=lambda x: x[0])

        return l_non_reefers_at_reefer, l_reefers_at_non_reefer

    def __get_dicts_from_lists(self, l_overstow: list, l_potential_restows: list, l_non_reefers_at_reefer: list) -> 'tuple[dict, dict, dict]':
        d_overstows = {(bay, row, tier_no): d_container_overstow for ((bay, row, tier_no), container_id, d_container_overstow) in l_overstow}
        d_potential_restows = {(bay, row, tier_no): (s_pod_above, capacity_above) for ((bay, row, tier_no), container_id, s_pod_above, capacity_above) in l_potential_restows}
        d_non_reefers_at_reefer = {(bay, row, tier_no): True for ((bay, row, tier_no), container_id) in l_non_reefers_at_reefer}

        return d_overstows, d_potential_restows, d_non_reefers_at_reefer

    def __compute_detailed_stowing_info(
            self,
            l_stacks_lines: list,
            l_subbays_lines: list,
            l_onboard_lines: list,
            recompute_for_overstowing_subbays: bool
        ) -> 'tuple[dict, dict, dict, list, dict, dict]':
        
        only_hc_moves = recompute_for_overstowing_subbays # false to output stowing info and true for overstowing subbays (by IBM)

        d_stacks = self.__get_d_stacks(l_stacks_lines)
        d_ports_slots, l_ports_slots = self.__load_onboard_stacks(d_stacks, l_onboard_lines)
        l_overstow = self.__get_l_overstow(d_stacks, d_ports_slots, l_ports_slots, only_hc_moves)
        d_subbay_pods = self.__get_d_subbay_pods(d_stacks, l_ports_slots)
        d_sb_capacities = self.__get_d_sb_capacities(l_subbays_lines)
        l_potential_restows = self.__get_l_potential_restows(d_stacks, d_subbay_pods, d_sb_capacities, d_ports_slots, l_ports_slots, only_hc_moves)
        l_non_reefers_at_reefer, l_reefers_at_non_reefer = self.__get_l_non_reefers_at_reefer_and_vice_versa(d_stacks, l_ports_slots)
        d_overstows, d_potential_restows, d_non_reefers_at_reefer = self.__get_dicts_from_lists(l_overstow, l_potential_restows, l_non_reefers_at_reefer)

        return d_overstows, d_potential_restows, d_non_reefers_at_reefer, l_reefers_at_non_reefer, l_overstow, d_stacks, d_sb_capacities

    def get_l_stowing_info_lines_and_reefers_at_non_reefers(
            self,
            l_stacks_lines: list,
            l_subbays_lines: list,
            l_onboard_lines: list,
            recompute_for_overstowing_subbays: bool
        ) -> list:

        d_overstows, d_potential_restows, d_non_reefers_at_reefer, l_reefers_at_non_reefer, l_overstow, d_stacks, d_sb_capacities \
        = self.__compute_detailed_stowing_info(
                                                l_stacks_lines,
                                                l_subbays_lines,
                                                l_onboard_lines,
                                                recompute_for_overstowing_subbays
                                            )

        l_stowing_info_lines = []
        for no_line, line in enumerate(l_onboard_lines):
                
            if no_line == 0:
                s_header = "Slot;LoadPort;DischPort;ContId;" +\
                        "Overstow;OverstowPOD;"+\
                        "PotentialRestow;PotentialRestowPOD;PotentialRestowImpact;"+\
                        "NonReeferAtReefer"
                
                l_stowing_info_lines.append(s_header)
                continue
            
            l_items = line.split(';')
            
            # first read slot, if no slot, that's the loading list part, to be skipped
            slot = l_items[8].strip() # sur 6 caractères ou 0...
            if len(slot) == 0: continue
            
            # subbay
            if len(slot) == 5: slot = '0'+slot # just in case
            elif len(slot) == 7 and slot[0] == "0": slot = slot[1:] # just in case
            
            # POL and POD
            pol_name = l_items[1].strip()
            pod_name = l_items[2].strip()
            # container id 
            container_id = l_items[0].strip()
            # other items are useless

            # transform slot into tuple, key to dictionary
            t_slot = (slot[0:2], slot[2:4], slot[4:6])
            
            # container to be added in the additional file
            to_be_added = False
            
            # overstow and potential restow
            overstow = ""
            overstow_pod_name = ""
            if t_slot in d_overstows:
                to_be_added = True
                overstow = "X"
                d_container_overstow = d_overstows[t_slot]
                overstow_pod_no = 99
                for pod_no, nb_overstow in d_container_overstow.items():
                    if pod_no < overstow_pod_no:
                        overstow_pod_no = pod_no
                overstow_pod_name = common_helpers.get_port_name_from_seq_num(self.__d_STOWING_seq_num_to_port_name, overstow_pod_no)

                if "USLA" in pod_name and "USLA" in overstow_pod_name:
                    continue
                
            potential_restow = ""
            potential_restow_pod_name = ""
            s_capacity_above = ""
            if t_slot in d_potential_restows:
                to_be_added = True
                potential_restow = "X"
                s_pod_above = d_potential_restows[t_slot][0]
                potential_restow_pod_no = -1
                for pod_no in s_pod_above:
                    if pod_no > potential_restow_pod_no:
                        potential_restow_pod_no = pod_no
                potential_restow_pod_name = common_helpers.get_port_name_from_seq_num(self.__d_STOWING_seq_num_to_port_name, potential_restow_pod_no)
                s_capacity_above = "%d" % d_potential_restows[t_slot][1]
            
            non_reefer_at_reefer = ""
            if t_slot in d_non_reefers_at_reefer:
                to_be_added = True
                non_reefer_at_reefer = "X"
            
            # writing, only if any kind of overstow or other information
            if to_be_added == True:
                s_line = "%s;%s;%s;%s;%s;%s;%s;%s;%s;%s" %\
                        (slot, pol_name, pod_name, container_id,\
                        overstow, overstow_pod_name,\
                        potential_restow, potential_restow_pod_name, s_capacity_above,\
                        non_reefer_at_reefer)
                
                l_stowing_info_lines.append(s_line)
    
        return l_stowing_info_lines, l_reefers_at_non_reefer

    def get_l_overstowing_subbays_lines(
            self,
            l_stacks_lines: list,
            l_subbays_lines: list,
            l_onboard_lines: list,
            recompute_for_overstowing_subbays: bool
        ) -> list:
        d_overstows, d_potential_restows, d_non_reefers_at_reefer, l_reefers_at_non_reefer, l_overstow, d_stacks, d_sb_capacities \
        = self.__compute_detailed_stowing_info(
                                                l_stacks_lines,
                                                l_subbays_lines,
                                                l_onboard_lines,
                                                recompute_for_overstowing_subbays
                                            )

        # first obtain the set of overstowing subbays 
        s_overstowing_subbays = set()

        for ((bay, row, tier_no), container_id, d_container_overstow) in l_overstow:
            if int(tier_no) >= 60: stack = (bay, row, "1")
            else: stack = (bay, row, "0")
            subbay = d_stacks[stack]['subbay']

            if subbay not in s_overstowing_subbays:
                s_overstowing_subbays.add(subbay)

        s_header = "Subbay;Overstowing"
        l_overstowing_subbays_lines = [s_header]

        l_subbays = [subbay for subbay in d_sb_capacities]
        l_subbays.sort()
        for subbay in l_subbays:
            if subbay in s_overstowing_subbays: overstowing = "1"
            else: overstowing = "0"

            s_line = "%s;%s" % (subbay, overstowing)
            l_overstowing_subbays_lines.append(s_line)
            
        return l_overstowing_subbays_lines

    def add_STOWING_maps_to_class_attributes(self, d_stowing_map: dict) -> None:
        self.__d_STOWING_seq_num_to_port_name = d_stowing_map
        self.__d_STOWING_port_name_to_seq_num = { val: k for (k, val) in self.__d_STOWING_seq_num_to_port_name.items() }

    #endregion STOWING INFO

    #region GROUP CONTAINERS

    def __get_d_stack_2_subbay_dict(self, l_stacks_lines: list) -> dict:
        d_stack_2_subbay = {}
        for no_line, line in enumerate(l_stacks_lines):
                
            if no_line == 0: continue
            
            l_items = line.split(';')
            bay = l_items[0] # sur 2 caractères
            row = l_items[1] # sur 2 caractères
            macro_tier = l_items[2]
            subbay = l_items[3] # sur 4 caractères, alors que pour cg, avec bay sur 1 caractère
            #if subbay[0] == '0': subbay = subbay[1:] ici les positions sont lues sur 6
                
            stack = (bay, row, macro_tier)
            
            d_stack_2_subbay[stack] = subbay
        
        return d_stack_2_subbay
    
    def __get_d_revenues_by_pol_pod_size_type(self, l_POL_POD_revenues_lines: list):
        d_pol_pod_revenues = {}        
        for no_line, line in enumerate(l_POL_POD_revenues_lines):
            
            if no_line == 0: continue
            
            l_items = line.split(';')
            
            pol_name = l_items[1]
            pod_name = l_items[2]
            size_type = l_items[0]
            revenue = float(l_items[3])
            
            if (pol_name, pod_name) not in d_pol_pod_revenues:
                d_pol_pod_revenues[(pol_name, pod_name)] = {}
            d_pol_pod_revenues[(pol_name, pod_name)][size_type] = revenue
                
        return d_pol_pod_revenues

    def __do_regroup_overstows(self, l_stowing_info_lines: list):        
        d_container_overstows = {}
        for no_line, line in enumerate(l_stowing_info_lines):
            
            if no_line == 0: continue

            l_items = line. split(';')
            
            slot = l_items[0].strip()
            load_port = l_items[1].strip()
            disch_port = l_items[2].strip()
            container_id = l_items[3].strip()
            overstow = l_items[4].strip()
            overstow_port = l_items[5].strip()
            if overstow == 'X':
                # note pod_name for unicity of container traject 
                # (if the same container is used twice in the same voyage, that must be distinguished)
                d_container_overstows[(container_id, disch_port)] = common_helpers.get_seq_num_from_port_name(self.__d_STOWING_port_name_to_seq_num, overstow_port)
        
        return d_container_overstows

    def __lh_split_container_group(self, l_weights: list, weight_threshold):
        # trier la liste, au cas où
        l_weights.sort()
        
        nb_weights = len(l_weights)
        # list of first indices in sub-groups
        l_ix_weight_ssgrps = []
        prev_weight = 0.0
        for ix, weight in enumerate(l_weights):
            if weight > prev_weight:
                l_ix_weight_ssgrps.append(ix)
                prev_weight = weight
        nb_ssgrps = len(l_ix_weight_ssgrps)

        # making into a list of min & max indices min et max of each sub-groups
        l_ix12_weight_ssgrps = []
        for ii, ix_weight_ssgrp in enumerate(l_ix_weight_ssgrps):
            ix_start = ix_weight_ssgrp
            if ii == nb_ssgrps - 1:
                ix_end = nb_weights - 1
            else:
                ix_end = l_ix_weight_ssgrps[ii+1] - 1
            l_ix12_weight_ssgrps.append((ix_start, ix_end))
            

        # testing average of weights for each sum from the beginning
        limit_l = -1
        weight_limit_l = 0.0
        for ii, (ix_start, ix_end) in enumerate(l_ix12_weight_ssgrps):
        
            l_weights_below = l_weights[0:ix_end+1]
            nb_weights_below = ix_end+1
            #avg_weight = sum(l_weights_below) / nb_weights_below
        
    #        if avg_weight <= weight_threshold:
            if sum(l_weights_below) <= nb_weights_below * weight_threshold:
                limit_l = ix_end
                weight_limit_l = l_weights[ix_end]

        
        return limit_l, weight_limit_l

    def __load_onboard_loadlist_container_groups(
        self,
        l_onboard_loadlist: list,
        d_stack_2_subbay: dict,
        d_container_overstows: dict,
        regroup_overstows: bool
    ):
        # while reading the file, aggregate the containers into sub-groups
        # ignoring the distinction L and H, and keep track of container weights
        # at the level of each subbay
        # L/H will be computed in a secend time
        d_subbays_macro_container_groups = {}
        for no_line, line in enumerate(l_onboard_loadlist):
            
            if no_line == 0: continue
            
            l_items = line.split(';')
            # subbay
            slot = l_items[8].strip() # on 6 characters or empty
            slot = self.process_slot_str(slot, len(slot))
            if len(slot):
                bay = slot[0:2] 
                row = slot[2:4]
                tier = slot[4:6]
                if int(tier) < 60: macro_tier = "0"
                if int(tier) > 60: macro_tier = "1"
                stack = (bay, row, macro_tier)
                subbay = d_stack_2_subbay[stack]
            else:
                subbay = ""
            
            # container id 
            container = l_items[0].strip()

            # POL and POD
            
            # starting with POD
            pod_name = l_items[2].strip()
            pod_seq = common_helpers.get_seq_num_from_port_name(self.__d_STOWING_port_name_to_seq_num, pod_name)
        
            # only POL now
            pol_name = l_items[1].strip()
            # if an overstow, the POL can be overriden with the port where loading is done
            # note pod_name for unicity of container traject 
            # (if the same container is used twice in the same voyage, that must be distinguished)
            overstow_port_seq = -1
            if (container, pod_name) in d_container_overstows:
                overstow_port_seq = d_container_overstows[(container, pod_name)]   
            
            # No need to change the name for Southampton,
            # allready done in the input file
            pol_seq = common_helpers.get_seq_num_from_port_name(self.__d_STOWING_port_name_to_seq_num, pol_name)
            
            # type and hc, 20 already deprived of HC...
            #iso_type = l_items[3].strip()
            c_size = l_items[5].strip()
            hc = l_items[6].strip()
            
            # weight
            # keep real (float) weight, and for rounding issues, have the weight
            # as a integer (in hundred of kilos)
            s_weight = l_items[7].strip()
            l_weight_elems = s_weight.split(".")
            i_weight = int(l_weight_elems[0]) * 10
            if len(l_weight_elems) == 2:
                i_weight += int(l_weight_elems[1])
                        
            # specials, either '', 'E' (empty), or 'R' (effective reefer)
            # we must also determine if reefer or not
            setting = l_items[4]
            if setting == "R": c_type = "RE"
            else: c_type = "GP"
            # NOTE: we keep also for later use the distinction empty / not empty
            # should be stored as a 3rd category for weight...
            empty = ""
            if setting == "E": empty = "E"

            # dangerous cargo
            c_dg = l_items[9].strip()
                
            # all elements have been collected
            # 3 possibilities
            # no overstowing from container, keep things are there are
            # there is overstowing :
            # create 2 occurrences
            # 1) current subbay, original pol, but pod is changed into overstow_port
            # 2) '' (load), overstow_port for pol, pod is inchanged
            
            l_container_sb_cg = []
            if overstow_port_seq == -1:
                subbay_macro_container_group = (subbay, (pol_seq, pod_seq, c_size, c_type, hc, c_dg))
                l_container_sb_cg.append(subbay_macro_container_group)
            # but change pol_name only when overstowed must be done on next port
            else:
                # EVENTUELLEMENT, GARDER pod_seq si overstow_port_seq > 1 ?????
                if regroup_overstows:
                    subbay_macro_container_group = (subbay, (pol_seq, overstow_port_seq, c_size, c_type, hc, c_dg)) 
                    l_container_sb_cg.append(subbay_macro_container_group)

                else:
                    subbay_macro_container_group = (subbay, (pol_seq, overstow_port_seq, c_size, c_type, hc, c_dg)) 
                    l_container_sb_cg.append(subbay_macro_container_group)
                    subbay_macro_container_group = ('', (overstow_port_seq, pod_seq, c_size, c_type, hc, c_dg)) 
                    l_container_sb_cg.append(subbay_macro_container_group)
            
            # integrating
            # sb and not subbay so that original subbay is not lost
            for (sb, macro_container_group) in l_container_sb_cg:
                if (sb, macro_container_group) not in d_subbays_macro_container_groups:
                    d_subbays_macro_container_groups[(sb, macro_container_group)] = []
                # add the container's weight in the list associated to the subbay X macro_container_group
                # and also the fact that it is in overstow, but for time being only if subbay != '',
                # because it will be computed later on
                # SOURCE TO BE REORGANIZED LATER ON...
                d_subbays_macro_container_groups[(sb, macro_container_group)]\
                .append((container, empty, i_weight, 
                        overstow_port_seq if sb != '' else -1,
                        i_weight if overstow_port_seq > 0 else 0,
                        subbay if sb == '' else '',
                        pol_seq if sb == '' else -1))
                
        # second step, determine the weight limits for each macro_container_groups
        # such as seen on the set of all sub-baies
        # INCLUDING SUBBAYS NOT FILLED IN A SET ONBOARD + LOADLIST
        d_macro_container_groups_l_weights = {}
        for (subbay, macro_container_group), l_items_in_subbay in d_subbays_macro_container_groups.items():
            if macro_container_group not in d_macro_container_groups_l_weights:
                d_macro_container_groups_l_weights[macro_container_group] = []
            l_weights_in_subbay = [i_weight \
                                for (container, empty, i_weight, 
                                        overstow_port_seq, i_overstow_weight, 
                                        overstow_sce_sb, overstow_sce_pol) in l_items_in_subbay]
            d_macro_container_groups_l_weights[macro_container_group].extend(l_weights_in_subbay)
        
        d_macro_container_groups_weight_limit_l = {}
        for macro_container_group, l_weights in d_macro_container_groups_l_weights.items():
            if macro_container_group[2] == '20':
                weight_threshold = 10.0
                i_weight_threshold = 100.0
            else:
                weight_threshold = 15.0
                i_weight_threshold = 150.0
            # l_weights will be sorted inside next function
            #limit_l, weight_limit_l = self.__lh_split_container_group(l_weights, weight_threshold)
            limit_l, weight_limit_l = self.__lh_split_container_group(l_weights, i_weight_threshold)
            d_macro_container_groups_weight_limit_l[macro_container_group] = weight_limit_l
                
        # it enable creating definitive container groups in sub-bays
        d_subbays_container_groups = {}
        # as well as the lists of individual containers associated to container groups
        d_container_groups_containers = {}
        for (subbay, macro_container_group), l_items_in_subbay in d_subbays_macro_container_groups.items():
            for (container, empty, i_weight, overstow_port_seq, i_overstow_weight, 
                overstow_sce_sb, overstow_sce_pol) in l_items_in_subbay:
                weight_limit_l = d_macro_container_groups_weight_limit_l[macro_container_group]
                if i_weight <= weight_limit_l:
                    c_weight = "L"
                else:
                    c_weight = "H"
                
                container_group = (macro_container_group[0], macro_container_group[1],
                                macro_container_group[2], macro_container_group[3],
                                c_weight, macro_container_group[4], macro_container_group[5])
                if (subbay, container_group) not in d_subbays_container_groups:
                    # quantity, weight, and overstow in subbay
                    d_subbays_container_groups[(subbay, container_group)] = (0, 0.0, -1, 0, 0.0, {})
                sb_cg_quantity = d_subbays_container_groups[(subbay, container_group)][0] + 1
                # back to the real weight !!!
                sb_cg_weight = d_subbays_container_groups[(subbay, container_group)][1] + (i_weight/10)
                # overstowing in subbay
                sb_cg_overstow_port_seq = d_subbays_container_groups[(subbay, container_group)][2]
                if overstow_port_seq > 0: 
                    sb_cg_overstow_port_seq = overstow_port_seq

                overstow_quantity = 1 if i_overstow_weight > 0 else 0

                sb_cg_overstow_quantity = d_subbays_container_groups[(subbay, container_group)][3]\
                                        + overstow_quantity

                sb_cg_overstow_weight = d_subbays_container_groups[(subbay, container_group)][4]\
                                        + (i_overstow_weight/10)
                                        
                sb_cg_overstow_sources = d_subbays_container_groups[(subbay, container_group)][5]
                if overstow_sce_sb != "":
                    if (overstow_sce_sb, overstow_sce_pol) not in sb_cg_overstow_sources:
                        sb_cg_overstow_sources[(overstow_sce_sb, overstow_sce_pol)]\
                        = (overstow_quantity, i_overstow_weight/10)
                    else:
                        sb_cg_overstow_sources[(overstow_sce_sb, overstow_sce_pol)]\
                        = (sb_cg_overstow_sources[(overstow_sce_sb, overstow_sce_pol)][0]\
                                                +overstow_quantity,
                        sb_cg_overstow_sources[(overstow_sce_sb, overstow_sce_pol)][1]\
                                                +(i_overstow_weight/10))

                d_subbays_container_groups[(subbay, container_group)] = (sb_cg_quantity, sb_cg_weight, 
                                                                        sb_cg_overstow_port_seq,
                                                                        sb_cg_overstow_quantity,
                                                                        sb_cg_overstow_weight,
                                                                        sb_cg_overstow_sources)
                # and then, processing the container lists
                # NOTE, we must handle the empty information which had been lost
                if container_group not in d_container_groups_containers:
                    d_container_groups_containers[container_group] = []
                d_container_groups_containers[container_group].append((container, empty))
        
        return d_subbays_container_groups, d_container_groups_containers

    def __get_four_onboard_dicts(self, d_subbays_container_groups):
        d_onboard_loadlist_cg_2_sb = {}
        d_onboard_loadlist_sb_2_cg = {}
        d_onboard_loadlist_cg_total_quantity_weight = {}
        d_onboard_loadlist_cg_avg_weight = {}

        for (sb, cg), (quantity, weight, overstow_port_seq,\
                    overstow_quantity, overstow_weight, overstow_sources)\
            in d_subbays_container_groups.items():
            
            if cg not in d_onboard_loadlist_cg_2_sb:
                d_onboard_loadlist_cg_2_sb[cg] = {}
            d_onboard_loadlist_cg_2_sb[cg][sb] = (quantity, weight,
                                                overstow_port_seq, overstow_quantity, overstow_weight, overstow_sources)
            
            if sb not in d_onboard_loadlist_sb_2_cg:
                d_onboard_loadlist_sb_2_cg[sb] = {}
            d_onboard_loadlist_sb_2_cg[sb][cg] = (quantity, weight,
                                                overstow_port_seq, overstow_quantity, overstow_weight, overstow_sources)
            
            if cg not in d_onboard_loadlist_cg_total_quantity_weight:
                d_onboard_loadlist_cg_total_quantity_weight[cg] = (0, 0.0)
            total_quantity = d_onboard_loadlist_cg_total_quantity_weight[cg][0] + quantity
            total_weight = d_onboard_loadlist_cg_total_quantity_weight[cg][1] + weight
            d_onboard_loadlist_cg_total_quantity_weight[cg] = (total_quantity, total_weight)
            
        for cg, (total_quantity, total_weight) in d_onboard_loadlist_cg_total_quantity_weight.items():
            d_onboard_loadlist_cg_avg_weight[cg] = total_weight / total_quantity

        return d_onboard_loadlist_cg_2_sb, d_onboard_loadlist_sb_2_cg, d_onboard_loadlist_cg_total_quantity_weight, d_onboard_loadlist_cg_avg_weight

    def __get_sb_2_cg_list(self, d_onboard_loadlist_sb_2_cg):
        l_onboard_loadlist_sb_2_cg = []
        for sb, d_cg in d_onboard_loadlist_sb_2_cg.items():
            l_cg = []
            for cg, (quantity, weight, 
                    overstow_port_seq, overstow_quantity, overstow_weight, overstow_sources) in d_cg.items():
                
                # with data on overstowing
                with_overstow = False
                osw_quantity = 0
                osw_weight = 0.0
                osw_sources = {}
                if overstow_quantity > 0: # dans tous les cas, loadlist ou onboard
                    with_overstow = True
                    osw_quantity = overstow_quantity
                    osw_weight = overstow_weight
                    osw_sources = overstow_sources
                
                cg_items = (cg, quantity, weight,  
                            overstow_port_seq, with_overstow, osw_quantity, osw_weight, osw_sources)
                
                l_cg.append(cg_items)
            l_onboard_loadlist_sb_2_cg.append((sb, l_cg))

        l_onboard_loadlist_sb_2_cg.sort(key=lambda x:x[0])

        return l_onboard_loadlist_sb_2_cg

    def __get_l_container_groups_completed_lines(self, l_onboard_loadlist_sb_2_cg: list) -> list:
        l_container_groups_completed_lines = []

        s_header = "Subbay;LoadPort;DischPort;" +\
                "Size;cType;cWeight;Height;cDG;"+\
                "AvgWeightInSubbay;QuantityInSubbay;WeightInSubbay;"+\
                "Overstow;OverstowPod;QuantityOverstow;WeightOverstow;SourcesOverstow"
        l_container_groups_completed_lines.append(s_header)
            
        for (subbay, l_container_groups) in l_onboard_loadlist_sb_2_cg:
                
            for (cg, quantity_in_sb, weight_in_sb, 
                overstow_port_seq, with_overstow, osw_quantity, osw_weight, osw_sources) in l_container_groups:
                
                pol_seq = cg[0]
                pol_name = common_helpers.get_port_name_from_seq_num(self.__d_STOWING_seq_num_to_port_name, pol_seq)
                pod_seq = cg[1]
                pod_name = common_helpers.get_port_name_from_seq_num(self.__d_STOWING_seq_num_to_port_name, pod_seq)
                size = cg[2]
                c_type = cg[3]
                c_weight = cg[4]
                height = cg[5]
                c_dg = cg[6]
                #avg_weight = d_onboard_cg_avg_weight[cg]
                avg_weight_in_sb = 0.0
                if quantity_in_sb != 0:
                    avg_weight_in_sb = weight_in_sb / quantity_in_sb
                    
                # overstow items, depending on loadlist (old or new) or onboard
                s_overstow = ''
                s_overstow_pod = ''
                s_quantity_overstow = ''
                s_weight_overstow = ''
                s_sources_overstow = ""
                if with_overstow == True: # loadlist ou subbay
                    s_overstow = 'X'
                    if subbay != '':
                        s_overstow_pod = common_helpers.get_port_name_from_seq_num(self.__d_STOWING_seq_num_to_port_name, overstow_port_seq)
                    s_quantity_overstow = "%d" % osw_quantity
                    s_weight_overstow = "%.1f" % osw_weight
                    if subbay == '':
                        for (sb_source, pol_source), (q_source, w_source) in osw_sources.items():
                            s_sources_overstow += "%s-%s-%d-%.3f" %\
                            (sb_source, common_helpers.get_port_name_from_seq_num(self.__d_STOWING_seq_num_to_port_name, pol_source), q_source, w_source)
                            s_sources_overstow += "|"
                        if len(s_sources_overstow) > 0:
                            s_sources_overstow = s_sources_overstow[:-1]
                
                # writing
                s_line = "%s;%s;%s;%s;%s;%s;%s;%s;%.3f;%d;%.1f;%s;%s;%s;%s;%s" %\
                        (subbay, pol_name, pod_name,\
                        size, c_type, c_weight, height, c_dg,\
                        avg_weight_in_sb, quantity_in_sb, weight_in_sb, 
                        s_overstow, s_overstow_pod, s_quantity_overstow, s_weight_overstow,
                        s_sources_overstow)
                
                l_container_groups_completed_lines.append(s_line)
        
        return l_container_groups_completed_lines

    def __get_container_revenue(self, d_pol_pod_revenues, pol_name, pod_name, size, c_type, height, empty):    
        # if no predefined revenue, return 0.0
        
        if (pol_name, pod_name) not in d_pol_pod_revenues:
            
            #TODO uncomment print
            # print("POL %s POD %s NOT IN REVENUES" % (pol_name, pod_name))
            return 0.0
            
        # empty with a very low revenue
        #if empty == 'E': return 10.0
        
        # reefers
        if c_type == 'RE':
            return d_pol_pod_revenues[(pol_name, pod_name)]['Reefer']
        
        # 20'
        if size == '20':
            return d_pol_pod_revenues[(pol_name, pod_name)]['20']
        
        # remaining are 40, HC or not
        if height == 'HC':
            return d_pol_pod_revenues[(pol_name, pod_name)]['40 HC']
        
        # ordinaries 40'
        return d_pol_pod_revenues[(pol_name, pod_name)]['40']

    def __get_l_container_groups_containers_lines(
            self,
            d_container_groups_containers: dict,
            d_pol_pod_revenues: dict
        ) -> list:

        l_container_groups_containers_lines = []
        s_header = "Container;LoadPort;DischPort;" +\
                "Size;cType;cWeight;Height;cDG;Empty;Revenue"
        l_container_groups_containers_lines.append(s_header)

        for cg, l_containers in d_container_groups_containers.items():
            
            # container groups
            pol_seq = cg[0]
            pol_name = common_helpers.get_port_name_from_seq_num(self.__d_STOWING_seq_num_to_port_name, pol_seq)
            pod_seq = cg[1]
            pod_name = common_helpers.get_port_name_from_seq_num(self.__d_STOWING_seq_num_to_port_name, pod_seq)
            size = cg[2]
            c_type = cg[3]
            c_weight = cg[4]
            height = cg[5]
            c_dg = cg[6]
            
            # each container
            for (container, empty) in l_containers:
                
                # get the revenue
                revenue = self.__get_container_revenue(
                    d_pol_pod_revenues, 
                    pol_name,
                    pod_name,
                    size,
                    c_type,
                    height,
                    empty
                )
            
                # writing
                s_line = "%s;%s;%s;%s;%s;%s;%s;%s;%s;%.2f" %\
                        (container, pol_name, pod_name,\
                        size, c_type, c_weight, height, c_dg, empty, revenue)
                l_container_groups_containers_lines.append(s_line)
        
        return l_container_groups_containers_lines

    def get_two_list_for_container_groups(
            self,
            l_stacks_lines: list,
            l_stowing_info_lines: list,
            l_onboard_loadlist: list,
            l_POL_POD_revenues_lines: list
        ) -> 'tuple[list, list]':

        d_stack_2_subbay = self.__get_d_stack_2_subbay_dict(l_stacks_lines)
        d_pol_pod_revenues = self.__get_d_revenues_by_pol_pod_size_type(l_POL_POD_revenues_lines)

        d_container_overstows = {}
        regroup_overstows = False #TODO hardcoded var
        if regroup_overstows:
            d_container_overstows = self.__do_regroup_overstows(l_stowing_info_lines)

        d_subbays_container_groups, d_container_groups_containers \
        = self.__load_onboard_loadlist_container_groups(
                l_onboard_loadlist,
                d_stack_2_subbay,
                d_container_overstows,
                regroup_overstows
            )
        
        d_onboard_loadlist_cg_2_sb, d_onboard_loadlist_sb_2_cg, d_onboard_loadlist_cg_total_quantity_weight, d_onboard_loadlist_cg_avg_weight \
        = self.__get_four_onboard_dicts(d_subbays_container_groups)

        l_onboard_loadlist_sb_2_cg = self.__get_sb_2_cg_list(d_onboard_loadlist_sb_2_cg)

        l_container_groups_completed_lines = self.__get_l_container_groups_completed_lines(l_onboard_loadlist_sb_2_cg)
        l_container_groups_containers_lines = self.__get_l_container_groups_containers_lines(d_container_groups_containers, d_pol_pod_revenues)

        return l_container_groups_completed_lines, l_container_groups_containers_lines

    #endregion GROUP CONTAINERS

    def do_nothing(self):
        """
        This function is just defined to be able to toggle (open/close) the region GROUP CONTAINERS
        """
        pass
    # start of Final container.csv file
    def __add_weights(self, df_containers:pd.DataFrame) -> pd.DataFrame:
        # We use the VGM value by default
        # if it is undefined, empty or zero, we use the AET value
        df_containers["Weight"] = df_containers["Weight_VGM"]
        df_containers["Weight"] = np.where(df_containers["Weight"].isnull(), df_containers["Weight_AET"], df_containers["Weight"])
        df_containers["Weight"] = np.where(df_containers["Weight"] == "", df_containers["Weight_AET"], df_containers["Weight"])
        df_containers["Weight"] = np.where(df_containers["Weight"].astype(int) == 0, df_containers["Weight_AET"], df_containers["Weight"])
        # In case we were to use AET value and it was also empty ("").
        # We also need to convert these values to float and convert them to metric tons
        df_containers["Weight"] = df_containers["Weight"].replace("", "0").astype(float) / 1000
        
        df_containers["cWeight"] = np.where(df_containers["Weight"] <= 15, "L", "H")
        df_containers.drop(columns=["Weight_VGM", "Weight_AET"], inplace=True)

        return df_containers
    
        
    def __extract_columns_combined_containers(self, df_all_containers:pd.DataFrame, containers_final_dict:dict) -> pd.DataFrame:
        
        static_columns = [
            "EQD_ID", "LOC_9_LOCATION_ID", "LOC_11_LOCATION_ID", # ID, POL, POD
            "EQD_MEA_VGM_MEASURE", "EQD_MEA_AET_MEASURE", # Weights
            "EQD_SIZE_AND_TYPE_DESCRIPTION_CODE", # Size, Heigh and Type
            "EQD_FULL_OR_EMPTY_INDICATOR_CODE", # Empty
            "TMP_TEMPERATURE_DEGREE", # Reefer
            "LOC_147_ID"
        ]
        
        oog_dynamic_cols = [
            "EQD_DIM_5_LENGTH_MEASURE", "EQD_DIM_6_LENGTH_MEASURE", 
            "EQD_DIM_7_WIDTH_MEASURE", "EQD_DIM_8_WIDTH_MEASURE", "EQD_DIM_13_HEIGHT_MEASURE"
        ]
        
        
        oog_dynamic_cols_present = [col for col in oog_dynamic_cols if col in df_all_containers.columns]
        
        handling_cols = [col for col in df_all_containers.columns if "HANDLING" in col or "HAN" in col]
        
        columns_selected = static_columns + oog_dynamic_cols_present + handling_cols
        
        df_combined_containers_filtered = df_all_containers[columns_selected]
        df_combined_containers_filtered = df_combined_containers_filtered.copy()
        df_combined_containers_filtered.rename(columns= containers_final_dict["column_names"], inplace=True)
        
        
        return df_combined_containers_filtered
    
    def __add_oog_combined_containers(self, df_combined_containers: pd.DataFrame) -> pd.DataFrame:
        # Handle OOG_TOP_MEASURE
        if "OOG_TOP" in df_combined_containers.columns:
            df_combined_containers["OOG_TOP"].replace('', np.nan, inplace=True)
            df_combined_containers["OOG_TOP"] = pd.to_numeric(df_combined_containers["OOG_TOP"], errors='coerce')
            df_combined_containers["OOG_TOP_MEASURE"] = np.where(
                df_combined_containers["OOG_TOP"].isnull(), 
                0.0, 
                df_combined_containers["OOG_TOP"] / 100
            ).astype(float)
        else:
            df_combined_containers["OOG_TOP_MEASURE"] = 0.0

        # Handle OOG columns and convert them to integer
        oog_cols = ["OOG_FORWARD", "OOG_AFTWARDS", "OOG_RIGHT", "OOG_LEFT", "OOG_TOP"]
        for col in oog_cols:
            # Now perform the integer conversion and comparison
            if col in df_combined_containers.columns:
                # Replace empty strings with np.nan
                df_combined_containers[col].replace('', np.nan, inplace=True)

                # Convert NaNs to 0 before conversion to integers
                df_combined_containers[col].fillna(0, inplace=True)
                
                df_combined_containers[col] = (df_combined_containers[col].astype(int) > 0).astype(int)
            else:
                df_combined_containers[col] = 0
                
        return df_combined_containers
    
    def __add_stowage_handling(self, df_containers: pd.DataFrame) -> pd.DataFrame:
        
        handling_cols = [col for col in df_containers.columns if "HANDLING" in col or "HAN" in col]

        def get_stowage_handling(row):
            for cell in row[handling_cols]:
                scell = str(cell)
                if "OND" in scell or "DECK" in scell:
                    return "DECK"
                if "UND" in scell or "HOLD" in scell:
                    return "HOLD"
            return ""

        df_containers["Stowage"] = df_containers.apply(get_stowage_handling, axis=1)

        return df_containers   
    
    def __add_slot_position(self, df_containers:pd.DataFrame) -> pd.DataFrame:
        df_containers["Slot"].replace('', np.nan, inplace=True)
        df_containers["Slot"] = np.where(df_containers["Slot"].isnull(), 0, df_containers["Slot"]).astype(int)
        df_containers["Slot"] = df_containers["Slot"].replace(0, "").astype(str)

        return df_containers

    def __add_uslax_priorities(self, df:pd.DataFrame, df_uslax:pd.DataFrame)-> pd.DataFrame:
        
        df = pd.merge(df, df_uslax, how='left',
                    left_on = ["DischPort"], right_on = ["subPort"])
        df["Subport"] = np.where(df["port"].isnull(), "", df["DischPort"])
        df["port"] = np.where(df["port"].isnull(), df["DischPort"], df["port"])
        df.rename(columns={"DischPort": "OriginalPOD", "port": "DischPort"}, inplace=True)
        df["priorityID"] = np.where(df["priorityID"] == "", -1, df["priorityID"])
        df["priorityID"] = df["priorityID"].fillna(-1).astype(int)
        df["priorityLevel"] = np.where(df["priorityLevel"]=="", -1, df["priorityLevel"])
        df["priorityLevel"] = df["priorityLevel"].fillna(-1).astype(int)
        
        return df
    
    def __add_characteristics(self, df:pd.DataFrame)->pd.DataFrame:
        # A container is empty if its "full or empty" indicator equals 4, else it should equal to 5.
        df["Empty"] = np.where(df["FULL_OR_EMPTY"].astype(str) == "4", "E", "")
        df.drop(columns=["FULL_OR_EMPTY"], inplace=True)
        
        # A container is refeer if a temperature is set.
        # However if the container is empty we ignore the temperature.
        df["cType"] = np.where(df["TEMPERATURE"]=="", "GP", "RE")
        #df["cType"] = np.where(df["TEMPERATURE"].astype(str) != "", "RE", "GP")
        df["cType"] = np.where(df["Empty"] == "E", "GP", df["cType"])
        df.drop(columns=["TEMPERATURE"], inplace=True)
        
        df["Setting"] = np.where(df["cType"] == "RE", "R", df["Empty"])

        return df
    
    def __add_revenues(self, df:pd.DataFrame, df_revenues:pd.DataFrame)-> pd.DataFrame:
        # Get revenue from an external file.
        # The revenue depends on the (POL,POD,Size,Height,Reefer) combination
        expanded = []
        cols = ["POL", "POD", "Size_fictive", "Height_fictive", "cType", "Revenue"]
        for (i, row) in df_revenues.iterrows():
            pol = row["POL"]
            pod = row["POD"]
            revenue = row["Revenue"]

            if "Reefer" in row["Size-Type"]:
                expanded.append(pd.DataFrame([[pol, pod, "20", "", "RE", revenue]], columns=cols))
                expanded.append(pd.DataFrame([[pol, pod, "40", "", "RE", revenue]], columns=cols))
                expanded.append(pd.DataFrame([[pol, pod, "40", "HC", "RE", revenue]], columns=cols))
            else:
                height = "HC" if "HC" in row["Size-Type"] else ""
                size = "20" if "20" in row["Size-Type"] else "40"
                expanded.append(pd.DataFrame([[pol, pod, size, height, "GP", revenue]], columns=cols))

        expanded_revenues = pd.concat(expanded, ignore_index=True)
        
        # In order to get the correct revenues, because of port codes followed by a number to differentiate rotations
        df['POL'] = [ row[:5] for row in df['LoadPort'] ]
        df['POD'] = [ row[:5] for row in df['DischPort'] ]
        
        # Regarding the revenues we do not differentiate 45' containers from 40'
        df['Size_fictive'] = np.where(df["Size"] == "20", "20", "40")
        
        df['Height_fictive'] = np.where(df["Size"] == "20", "", df["Height"])      

        df = pd.merge(df, expanded_revenues, how='left',
                    left_on=['POL', 'POD', 'Size_fictive', 'Height_fictive', 'cType'],
                    right_on=['POL', 'POD', 'Size_fictive', 'Height_fictive', 'cType'])
        
        df.drop(columns=["POL", "POD", "Size_fictive", "Height_fictive"], inplace=True)
        
        # And finally we keep reefer price for reefer containers, and GP price for the others
        #df["Revenue"] = np.where(df["Revenue_y"].isnull(), df["Revenue_x"], df["Revenue_y"])
        #df.drop(columns=["Revenue_x", "Revenue_y"], inplace=True)
        
        # Unavailable revenues are set to 0
        df["Revenue"] = np.where(df["Revenue"].isnull(), 0, df["Revenue"])

        return df

    def __add_pol_pod_nb(self, df:pd.DataFrame, df_rotation: pd.DataFrame) -> pd.DataFrame:
        
        df_rotation = df_rotation[["ShortName", "Sequence"]]

        df["POLOrig"] = [ port if port in df_rotation["ShortName"] else port[:5] for port in df["LoadPort"] ]
        df["PODOrig"] = [ port if port in df_rotation["ShortName"] else port[:5] for port in df["DischPort"] ]

        df = pd.merge(df, df_rotation, how='left',
                    left_on=["POLOrig"], right_on=["ShortName"])
        df.rename(columns={"Sequence": "POL_nb"}, inplace=True)
        df.drop(columns=["ShortName"], inplace=True)

        df = pd.merge(df, df_rotation, how='left',
                    left_on=["PODOrig"], right_on=["ShortName"])
        df.rename(columns={"Sequence": "POD_nb"}, inplace=True)
        df.drop(columns=["ShortName"], inplace=True)

        nbPorts = len(set(df_rotation["ShortName"]))

        df["POL_nb"] = np.where(common_helpers.is_empty(df["POL_nb"]) & ~common_helpers.is_empty(df["Slot"]), -nbPorts, df["POL_nb"])
        df["POL_nb"] = np.where(common_helpers.is_empty(df["POL_nb"]) & common_helpers.is_empty(df["Slot"]), 2*nbPorts, df["POL_nb"])
        df["POD_nb"] = np.where(common_helpers.is_empty(df["POD_nb"]), 2*nbPorts, df["POD_nb"])

        df["POL_nb"] = np.where(common_helpers.is_empty(df["Slot"]), df["POL_nb"], df["POL_nb"] - nbPorts).astype(int)
        df["POD_nb"] = np.where(df["POL_nb"] > df["POD_nb"], df["POL_nb"] + nbPorts, df["POD_nb"]).astype(int)

        return df
    
    
    def __add_slot_stack_informations(self, df: pd.DataFrame, df_stacks: pd.DataFrame)-> pd.DataFrame:
        
        df_stacks["MacroRow"] = [ int(str(sb)[-2:-1]) for sb in df_stacks["SubBay"] ]
        df_stacks = df_stacks[["Bay", "Row", "Tier", "MacroRow", "OddSlot", "FirstTier"]].rename(columns={"Tier": "MacroTier"})
        df_stacks[["Bay", "Row", "MacroTier"]] = df_stacks[["Bay", "Row", "MacroTier"]].astype(int)
        df["Tier"] = [ int(row[-2:]) for row in df["Slot"] ]
        df["Row"] = [ int(row[-4:-2]) for row in df["Slot"] ]
        df["Bay"] = [ int(row[:-4]) for row in df["Slot"] ]
        df["MacroBay"] = [ 2+round((row-2)/4)*4 for row in df["Bay"] ]
        df["MacroTier"] = df["Tier"] >= 50

        df = pd.merge(df, df_stacks, how='left', left_on=["Bay", "Row", "MacroTier"], right_on=["Bay", "Row", "MacroTier"])

        return df

    def __get_on_board_df(self, df: pd.DataFrame, df_stacks, first_port: int)-> pd.DataFrame:
        df_ob = df[~common_helpers.is_empty(df["Slot"])].copy()
        df_ob = df_ob[df_ob["POD_nb"] >= first_port]
        df_ob = self.__add_slot_stack_informations(df_ob, df_stacks)

        return df_ob
    
    def __add_overstows(self, df: pd.DataFrame, df_stacks: pd.DataFrame)-> pd.DataFrame:
        df_ob = self.__get_on_board_df(df, df_stacks, 0)

        overstow = {}
        for container in df_ob["Container"]:
            overstow[container] = np.inf

        # Stack restows
        for (name, gr_df_ob) in df_ob.groupby(["MacroBay", "Row", "MacroTier"]):
            for (i1, r1) in gr_df_ob.iterrows():
                for (i2, r2) in gr_df_ob.iterrows():
                    if r2["POL_nb"] < r1["POD_nb"] and r1["POD_nb"] < r2["POD_nb"]:
                        if r1["Tier"] < r2["Tier"] and abs(r1["Bay"] - r2["Bay"]) <= 1: #(r1["Bay"] == r2["Bay"] or (r1["Size"] == 20 and r2["Size"] != 20)):
                            overstow[r2["Container"]] = min(overstow[r2["Container"]], r1["POD_nb"])
                                
        # Subbay restows
        for (name, gr_df_ob) in df_ob.groupby(["MacroBay", "MacroRow"]):
            for (i1, r1) in gr_df_ob.iterrows():
                for (i2, r2) in gr_df_ob.iterrows():
                    if r2["POL_nb"] < r1["POD_nb"] and r1["POD_nb"] < r2["POD_nb"]:
                            if r2["MacroTier"] == r1["MacroTier"] + 1:
                                overstow[r2["Container"]] = min(overstow[r2["Container"]], r1["POD_nb"])
        
        df_ob["overstowPort"] = [ overstow[container] for container in df_ob["Container"] ]

        df = pd.merge(df, df_ob[["Container", "overstowPort", "POD_nb"]], how='left',
                    left_on=["Container", "POD_nb"], right_on=["Container", "POD_nb"])
        df["overstowPort"] = np.where(df["overstowPort"].isnull(), np.inf, df["overstowPort"])
        df["overstowPort"] = [ str(int(row)) if row != np.inf else "" for row in df["overstowPort"] ]

        return df
    
    
    def __add_overstows_20_isolated(self, df:pd.DataFrame, df_stacks:pd.DataFrame, df_rotation:pd.DataFrame) -> pd.DataFrame:
    
        df_ob = self.__get_on_board_df(df, df_stacks, 1)

        nbPorts = len(set(df_rotation["ShortName"]))

        overstow = {}

        for sequence in sorted(set(df_rotation["Sequence"]))[:-2]:
            df_ob = df_ob[df_ob["POD_nb"] > sequence]

            for (name, gr_df_ob) in df_ob[df_ob["MacroTier"] == 0].groupby(["MacroBay", "Row"]):
                for (i, r) in gr_df_ob.iterrows():
                    if r["Container"] not in overstow and r["Size"] == "20" and r["Tier"] == r["FirstTier"] and len(gr_df_ob) == 1 and r["POD_nb"] <= nbPorts-2:
                        overstow[r["Container"]] = sequence

        for key in overstow.keys():
            initialOverstowPort = df.loc[df["Container"] == key]["overstowPort"].tolist()
            assert(len(initialOverstowPort) == 1)
            initialOverstowPort = initialOverstowPort[0]
            if initialOverstowPort != "":
                overstow[key] = min(overstow[key], int(initialOverstowPort))
            df.loc[df["Container"]==key, "overstowPort"] = str(overstow[key])
            
        return df
    
    def __add_non_reefer_at_reefer_slot(self, df: pd.DataFrame, df_stacks: pd.DataFrame) -> pd.DataFrame:
        
        df_stacks["MacroRow"] = [ int(str(sb)[-2:-1]) for sb in df_stacks["SubBay"] ]
        df_stacks = df_stacks[["Bay", "Row", "Tier", "MacroRow", "FirstTier", "NbReefer"]].rename(columns={"Tier": "MacroTier"})
        df_stacks[["Bay", "Row", "MacroTier"]] = df_stacks[["Bay", "Row", "MacroTier"]].astype(int)
        df_ob = df[~common_helpers.is_empty(df["Slot"])].copy()

        df_ob = df_ob[df_ob["POD_nb"] >= 0]

        df_ob["Tier"] = [ int(row[-2:]) for row in df_ob["Slot"] ]
        df_ob["Row"] = [ int(row[-4:-2]) for row in df_ob["Slot"] ]
        df_ob["Bay"] = [ int(row[:-4]) for row in df_ob["Slot"] ]
        
        df_ob["MacroTier"] = df_ob["Tier"] >= 50

        df_ob = pd.merge(df_ob, df_stacks, how='left', left_on=["Bay", "Row", "MacroTier"], right_on=["Bay", "Row", "MacroTier"])

        df_ob["NbReefer"] = df_ob["NbReefer"].astype(int)
        def is_reefer_slot(tier: int, first_tier: int, nb_reefers: int):
            if first_tier is not None and tier is not None:
                first_tier = int(first_tier)
                tier = int(tier)
                return first_tier <= tier and tier <= first_tier + 2 * (nb_reefers - 1)
            else:
                # Handle the case where either is None or some other non-numeric type
                return False  # or whatever is appropriate
            # return first_tier <= tier and tier <= first_tier + 2 * (nb_reefers - 1)
        
        df_ob["NonReeferAtReefer"] = [ "X" if is_reefer_slot(row["Tier"], row["FirstTier"], row["NbReefer"]) and row["cType"] != "RE" else ""
                                for (_, row) in df_ob.iterrows()]

        df = pd.merge(df, df_ob[["Container", "NonReeferAtReefer", "POD_nb"]], how='left', left_on=["Container", "POD_nb"], right_on=["Container", "POD_nb"])

        return df
    
    def __add_dg_class(self, df: pd.DataFrame, df_combined:pd.DataFrame, df_dg_loadlist:pd.DataFrame)-> pd.DataFrame:
        dg_hazard_id_cols = [col for col in df_combined.columns if "DGS" in col and "HAZARD_ID" in col]
        dg_free_txt_cols = [col for col in df_combined.columns if "DGS_FTX_FREE_TEXT_DESCRIPTION" in col]
        dg_kco_col = "EQD_HAN_KCO_HANDLING_INSTRUCTION_DESCRIPTION_CODE"

        dg_cols = ["EQD_ID", "LOC_9_LOCATION_ID"] + dg_hazard_id_cols + dg_free_txt_cols
        if (dg_kco_col in df_combined.columns):
            dg_cols.append(dg_kco_col)

        
        df_copy = df_dg_loadlist[["Serial Number", "POL", "POD", "DG-Remark (SW5 = Mecanical Ventilated Space if U/D par.A DOC)"]]
        df_dg_ll = df_copy.copy()
        df_dg_ll.rename(columns={"DG-Remark (SW5 = Mecanical Ventilated Space if U/D par.A DOC)": "DG-Remark"}, inplace = True)

        dg_remark_dict = {}
        for index, row in df_dg_ll.iterrows():
            rowid = (row["Serial Number"], row["POL"])
            if rowid in dg_remark_dict: dg_remark_dict[rowid] += str(row["DG-Remark"]) + " "
            else:                       dg_remark_dict[rowid]  = str(row["DG-Remark"]) + " "

        def get_dg_class(row):
            list_of_dg_codes = [ row[col] for col in dg_hazard_id_cols if not common_helpers.not_defined(row[col]) ] #np.isnan(row[col]) ]
            if len(list_of_dg_codes) == 0: return "" # If no dg code found, return ""
            min_dg_code = min(list_of_dg_codes)
            # Try converting code to an integer to prevent 8.0 instead of 8 when converting to a string
            try:
                if min_dg_code == int(min_dg_code): return str(int(min_dg_code))
            except(ValueError, TypeError):
                pass
            
            return str(min_dg_code)
        
        df_dg = df_combined[dg_cols].copy()
        #df_dg.drop_duplicates(subset=["EQD_ID", "LOC_9_LOCATION_ID"], inplace=True)
        df_dg["cDG"] = [ get_dg_class(row) for (_, row) in df_dg.iterrows() ]

        reg_sw_1 = re.compile("SW1\ ")
        reg_sw_2 = re.compile("SW1$")
        def match_sw1(s): return reg_sw_1.match(s) or reg_sw_2.match(s)

        def match_sw1_cols(r): return bool(any([match_sw1(str(r[col])) for col in dg_free_txt_cols]))

        def match_sw1_dict(r):
            rowid = (r['EQD_ID'], r['LOC_9_LOCATION_ID'])
            return (rowid in dg_remark_dict) and bool(match_sw1(dg_remark_dict[rowid]))
        
        reg_kc = re.compile("KC")
        reg_kco = re.compile("KCO")
        def match_kco(r):
            if dg_kco_col in r:
                s = str(r[dg_kco_col])
                return bool(any([reg_kc.match(s), reg_kco.match(s)]))
            return False

        df_dg["DGheated"] = [ int(any([match_sw1_cols(row), match_kco(row), match_sw1_dict(row)])) for (_, row) in df_dg.iterrows() ]

        df_dg = df_dg[["EQD_ID", "LOC_9_LOCATION_ID", "cDG", "DGheated"]]#.rename({"EQD_ID":"Container", "LOC_9_LOCATION_ID":"LoadPort"})

        df = pd.merge(df, df_dg, how='left', left_on=['Container', 'LoadPort'], right_on=["EQD_ID", "LOC_9_LOCATION_ID"]) #right_on=['Container', 'LoadPort'])

        return df
    
    def __add_dg_exclusion(self, df:pd.DataFrame, df_dg_exclusion:pd.DataFrame, df_stacks:pd.DataFrame)->pd.DataFrame:
        
        df_stacks = df_stacks[["Bay", "Tier", "SubBay"]].rename(columns={"Tier": "MacroTier"}).drop_duplicates(inplace=False)

        df_exclusion = pd.merge(df_dg_exclusion, df_stacks, how='left', left_on=["Bay", "MacroTier"], right_on=["Bay", "MacroTier"])
        df_exclusion.drop(columns=["Bay", "MacroTier"], inplace=True)
        #print(df_exclusion)

        df_exclusion["SubBay"] = df_exclusion["SubBay"].astype(str)
        df_exclusion = df_exclusion.groupby(["ContId", "LoadPort"]).agg({"SubBay" : ','.join}).reset_index()
        df_exclusion.rename(columns={"SubBay" : "Exclusion"}, inplace=True)

        df = pd.merge(df, df_exclusion, how='left', left_on=["Container", "LoadPort"], right_on=["ContId", "LoadPort"])

        return df    
    
    def __arrange_columns(self, df:pd.DataFrame)->pd.DataFrame:
        return df[[
            "Container", "LoadPort", "POL_nb", "DischPort", "POD_nb",
            "Size", "cType", "cWeight", "Height",
            "cDG", "Empty", "Revenue",
            "Type", "Setting", "Weight", "Slot",
            "priorityID", "priorityLevel",
            "overstowPort", "NonReeferAtReefer",
            "Subport", "Stowage", "DGheated", "Exclusion",
            "OOG_FORWARD", "OOG_AFTWARDS", "OOG_RIGHT", "OOG_LEFT", "OOG_TOP", "OOG_TOP_MEASURE"
            ]]
        
    def get_df_containers_final(self, df_all_containers:pd.DataFrame, containers_final_dict:dict, d_iso_codes_map:dict, df_uslax:pd.DataFrame, df_revenues:pd.DataFrame, df_rotations:pd.DataFrame, df_stacks:pd.DataFrame, df_dg_loadlist:pd.DataFrame, df_dg_exclusions:pd.DataFrame):
        df_copy = df_all_containers.copy()

        df_combined_containers_filtered = self.__extract_columns_combined_containers(df_copy, containers_final_dict)
        df_combined_containers_filtered = self.__add_characteristics(df_combined_containers_filtered)
        df_combined_containers_filtered = self.__add_sizes_and_heights_to_df(df_combined_containers_filtered, d_iso_codes_map)
        df_combined_containers_filtered = self.__add_weights(df_combined_containers_filtered)
        df_combined_containers_filtered = self.__add_oog_combined_containers(df_combined_containers_filtered)
        df_combined_containers_filtered = self.__add_stowage_handling(df_combined_containers_filtered)
        df_combined_containers_filtered = self.__add_dg_class(df_combined_containers_filtered, df_all_containers, df_dg_loadlist)
        df_combined_containers_filtered = self.__add_dg_exclusion(df_combined_containers_filtered, df_dg_exclusions, df_stacks)
        df_combined_containers_filtered = self.__add_slot_position(df_combined_containers_filtered)
        df_combined_containers_filtered = self.__add_uslax_priorities(df_combined_containers_filtered, df_uslax)
        df_combined_containers_filtered = self.__add_revenues(df_combined_containers_filtered, df_revenues)
        df_combined_containers_filtered = self.__add_pol_pod_nb(df_combined_containers_filtered, df_rotations)
        df_combined_containers_filtered = self.__add_overstows(df_combined_containers_filtered, df_stacks)
        df_combined_containers_filtered = self.__add_overstows_20_isolated(df_combined_containers_filtered, df_stacks, df_rotations)
        df_combined_containers_filtered = self.__add_non_reefer_at_reefer_slot(df_combined_containers_filtered, df_stacks)
        df_combined_containers_final = self.__arrange_columns(df_combined_containers_filtered)
    
        return df_combined_containers_final
