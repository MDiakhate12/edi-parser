import pandas as pd 
import numpy as np
import json 
import logging
import os 

class DG:
    def __init__(self, logger: logging.Logger, vessel: object, df_all_containers: pd.DataFrame, DG_loadlist_enrichment_map: dict, DG_rules: dict, imdg_haz_exis: pd.DataFrame):
        self.logger = logger
        self._vessel = vessel
        self._df_all_containers = df_all_containers
        self._DG_loadlist_enrichment_map = DG_loadlist_enrichment_map
        self._imdg_haz_exis = imdg_haz_exis
        self.__DG_rules = DG_rules
        


    def __populate_states_list(self, states_list_to_map: list, reference_state: str, yes_no_vals_tuple: tuple) -> None:
        """
        Populates a list with values based on mapping rules using a reference state.

        Args:
            states_list_to_map (list): The list to be populated with mapped values.
            reference_state (str): The reference state used for mapping.
            yes_no_vals_tuple (tuple): A tuple containing two values representing 'yes' and 'no' states.

        Returns:
            list: A new list containing mapped values based on the given mapping rules.

        Raises:
            None.

        Notes:
            - The function populates the 'states_list_to_map' list based on mapping rules.
            - If an element in the 'states_list_to_map' is empty or not equal to itself, it is mapped to 'no' (yes_no_vals_tuple[1]).
            - If an element in the 'states_list_to_map' matches the 'reference_state', it is mapped to 'yes' (yes_no_vals_tuple[0]).
            - If an element in the 'states_list_to_map' does not match the 'reference_state', it is also mapped to 'no' (yes_no_vals_tuple[1]).

        Example:
            ATT_HAZ_cols_states = ["P", "", "", "P", ""]
            reference_state = "P"
            yes_no_values = ("yes", "no")

            marine_pollutant_list = self.__populate_states_list(ATT_HAZ_cols_states, reference_state, yes_no_values)
            print(marine_pollutant_list)
            # Output: ['yes', 'no', 'no', 'yes', 'no']
        """
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
        """
        Extracts and processes DG (Dangerous Goods) attribute states from a DataFrame.

        Args:
            attributes_df (pd.DataFrame): The DataFrame containing the attributes.

        Returns:
            tuple[list, list, list, list, list]: A tuple of lists representing different attribute states.

        Raises:
            None.

        Notes:
            - The method iterates over the columns of the 'attributes_df' DataFrame.
            - It identifies the columns related to hazardous goods ('DGS_ATT_HAZ') and agricultural goods ('DGS_ATT_AGR').
            - If hazardous goods columns are found, it constructs 'ATT_HAZ_states_lists' as a list of lists, where each inner list contains the attribute states for the respective hazardous goods column.
            - If no hazardous goods columns are found, a single empty string ("") is used to create 'ATT_HAZ_states_lists'.
            - If an agricultural goods column is found, 'ATT_AGR_states_list' is created as a list of attribute states for the agricultural goods column.
            - If no agricultural goods column is found, a single empty string ("") is used to create 'ATT_AGR_states_list'.
            - 'ATT_HAZ_cols_states' is created based on 'ATT_HAZ_states_lists'. If 'ATT_HAZ_states_lists' has two inner lists, 'ATT_HAZ_cols_states' is a list of tuples containing corresponding attribute states from both lists. Otherwise, it is a list of attribute states from the single inner list.
            - 'marine_pollutant_list' is populated using 'ATT_HAZ_cols_states' to determine whether each state contains the reference state "P" or not.
            - 'flammable_list' is populated using 'ATT_HAZ_cols_states' to determine whether each state contains the reference state "FLVAP" or not.
            - 'liquid_list' is populated using 'ATT_AGR_states_list' to determine whether each state contains the reference state "L" or not.
            - 'solid_list' is populated using 'ATT_AGR_states_list' to determine whether each state contains the reference state "S" or not.

        Example:
            attributes_df = pd.DataFrame({
                'DETAIL_DESCRIPTION_CODE_1': ['DGS_ATT_HAZ_1', 'DGS_ATT_HAZ_2', 'DGS_ATT_AGR'],
                'DETAIL_DESCRIPTION_CODE_2': ['P', 'FLVAP', 'L'],
                'DETAIL_DESCRIPTION_CODE_3': ['Q', 'XYZ', 'S']
            })

            result = __get_DG_ATT_states_lists(attributes_df)
            print(result)
            # Output: (['yes', 'yes', ''], ['x', '', ''], ['x', '', 'L'], ['x', '', 'S'])
        """
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
        """
        Adds DG (Dangerous Goods) attribute states to the given DataFrame.

        Args:
            df (pd.DataFrame): The DataFrame to which the attribute states will be added.
            attributes_df (pd.DataFrame): The DataFrame containing the attributes.

        Returns:
            pd.DataFrame: The modified DataFrame with added attribute states.

        Raises:
            None.

        Notes:
            - The method calls the '__get_DG_ATT_states_lists' method to extract attribute states from the 'attributes_df'.
            - It assigns the extracted states to respective columns in the 'df' DataFrame: 'Marine Pollutant', 'Liquid', 'Solid', 'Flammable', and 'Non-Flammable'.
            - The 'Marine Pollutant' column is populated with the 'marine_pollutant_list' obtained from '__get_DG_ATT_states_lists'.
            - The 'Liquid' column is populated with the 'liquid_list' obtained from '__get_DG_ATT_states_lists'.
            - The 'Solid' column is populated with the 'solid_list' obtained from '__get_DG_ATT_states_lists'.
            - The 'Flammable' column is populated with the 'flammable_list' obtained from '__get_DG_ATT_states_lists'.
            - The 'Non-Flammable' column is assigned an empty string ("").
            - The modified DataFrame is returned.

        Example:
            df = pd.DataFrame({
                'Item': ['Item1', 'Item2', 'Item3'],
                'Value': [10, 20, 30]
            })

            attributes_df = pd.DataFrame({
                'DETAIL_DESCRIPTION_CODE_1': ['DGS_ATT_HAZ_1', 'DGS_ATT_HAZ_2', 'DGS_ATT_AGR'],
                'DETAIL_DESCRIPTION_CODE_2': ['P', 'FLVAP', 'L'],
                'DETAIL_DESCRIPTION_CODE_3': ['Q', 'XYZ', 'S']
            })

            result = __add_DG_ATT_states_to_df(df, attributes_df)
            print(result)
            # Output:
            #     Item  Value Marine Pollutant Liquid Solid Flammable Non-Flammable
            # 0  Item1     10               yes         L                x
            # 1  Item2     20               yes                    x
            # 2  Item3     30
        """
        marine_pollutant_list, flammable_list, liquid_list, solid_list = self.__get_DG_ATT_states_lists(attributes_df)
        df["Marine Pollutant"] = marine_pollutant_list
        df["Liquid"] = liquid_list
        df["Solid"] = solid_list
        df["Flammable"] = flammable_list
        df["Non-Flammable"] = ""

        return df
    
    
    def _add_DG_missing_cols_to_df(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Adds missing DG (Dangerous Goods) columns to the given DataFrame.

        Args:
            df (pd.DataFrame): The DataFrame to which the missing columns will be added.

        Returns:
            pd.DataFrame: The modified DataFrame with added columns.

        Raises:
            None.

        Notes:
            - The method takes a DataFrame 'df' and adds missing DG-related columns to it.
            - The list 'missing_cols' contains the names of the missing columns.
            - For each column name in 'missing_cols', a new column is added to the DataFrame 'df' with empty values.
            - The modified DataFrame is returned.

        Example:
            df = pd.DataFrame({
                'Item': ['Item1', 'Item2', 'Item3'],
                'Value': [10, 20, 30]
            })

            result = _add_DG_missing_cols_to_df(df)
            print(result)
            # Output:
            #     Item  Value Closed Freight Container Loading remarks SegregationGroup Stowage and segregation Package Goods Stowage Category not permitted bay 74 Zone
            # 0  Item1     10                                                                                                                            
            # 1  Item2     20                                                                                                                            
            # 2  Item3     30

        """
        missing_cols = [
            "Closed Freight Container", "Loading remarks",
            "SegregationGroup", "Stowage and segregation", "Package Goods", "Stowage Category", "not permitted bay 74", "Zone"
        ]

        for col in missing_cols:
            df[col] = ""

        return df

    def __reorder_df_DG_loadlist_cols(self, df_DG_loadlist: pd.DataFrame) -> pd.DataFrame:
        """
        Reorders the columns of the DG loadlist DataFrame.

        Args:
            df_DG_loadlist (pd.DataFrame): The DataFrame representing the DG loadlist.

        Returns:
            pd.DataFrame: The modified DataFrame with reordered columns.

        Raises:
            None.

        Notes:
            - The method takes a DataFrame 'df_DG_loadlist' representing the DG loadlist and reorders its columns.
            - The column order is defined in the 'DG_cols_ordered_list' variable, which contains the desired column order as a semicolon-separated string.
            - The string is split into a list of column names.
            - The DataFrame 'df_DG_loadlist' is modified by reordering its columns according to the defined order.
            - The modified DataFrame is returned.

        Example:
            df_DG_loadlist = pd.DataFrame({
                'Serial Number': [1, 2, 3],
                'Operator': ['ABC', 'DEF', 'GHI'],
                'Type': ['TypeA', 'TypeB', 'TypeC'],
                'Weight': [100, 200, 300],
                'POD': ['POD1', 'POD2', 'POD3']
            })

            result = __reorder_df_DG_loadlist_cols(df_DG_loadlist)
            print(result)
            # Output:
            #    Serial Number Operator  POL  POD   Type  Closed Freight Container  Weight  Regulation Body  Ammendmant Version  UN  Class  SubLabel1  SubLabel2  DG-Remark (SW5 = Mecanical Ventilated Space if U/D par.A DOC)  FlashPoints  Loading remarks  Limited Quantity  Marine Pollutant  PGr  Liquid  Solid  Flammable  Non-Flammable  Proper Shipping Name (Paragraph B of DOC)  SegregationGroup  SetPoint  Stowage and segregation  Package Goods  Stowage Category  not permitted bay 74  Zone
            # 0              1      ABC  NaN  POD1  TypeA                      NaN     100              NaN                 NaN NaN    NaN        NaN        NaN                                                NaN           NaN              NaN               NaN               NaN  NaN     NaN    NaN        NaN            NaN                                                NaN               NaN       NaN                      NaN             NaN               NaN                   NaN   NaN
            # 1              2      DEF  NaN  POD2  TypeB                      NaN     200              NaN                 NaN NaN    NaN        NaN        NaN                                                NaN           NaN              NaN               NaN               NaN  NaN     NaN    NaN        NaN            NaN                                                NaN               NaN       NaN                      NaN             NaN               NaN                   NaN   NaN
            # 2              3      GHI  NaN  POD3  TypeC                      NaN     300              NaN                 NaN NaN    NaN        NaN        NaN                                                NaN           NaN              NaN               NaN               NaN  NaN     NaN    NaN        NaN            NaN                                                NaN               NaN       NaN                      NaN             NaN               NaN                   NaN   NaN
        """
        DG_cols_ordered_list = ("Serial Number;Operator;POL;POD;Type;Closed Freight Container;Weight;Regulation Body;Ammendmant Version;UN;Class;SubLabel1;SubLabel2;" +\
                                "DG-Remark (SW5 = Mecanical Ventilated Space if U/D par.A DOC);FlashPoints;Loading remarks;" +\
                                "Limited Quantity;Marine Pollutant;PGr;Liquid;Solid;Flammable;Non-Flammable;Proper Shipping Name (Paragraph B of DOC);" +\
                                "SegregationGroup;SetPoint;Stowage and segregation;Package Goods;Stowage Category;not permitted bay 74;Zone").split(";")

        df_DG_loadlist = df_DG_loadlist[DG_cols_ordered_list]

        return df_DG_loadlist
    
    
    def get_df_DG_loadlist(self, df_onboard_loadlist: pd.DataFrame, df_all_containers: pd.DataFrame, d_cols_map: dict) -> pd.DataFrame:
        """
        Generates the DG (Dangerous Goods) loadlist DataFrame based on the onboard loadlist and container data.

        Args:
            df_onboard_loadlist (pd.DataFrame): The DataFrame representing the onboard loadlist.
            df_all_containers (pd.DataFrame): The DataFrame representing all containers.
            d_cols_map (dict): A dictionary mapping column names.

        Returns:
            pd.DataFrame: The generated DG loadlist DataFrame.

        Raises:
            None.

        Notes:
            - The method takes the onboard loadlist DataFrame 'df_onboard_loadlist', the DataFrame containing all containers 'df_all_containers', and a dictionary 'd_cols_map' mapping column names.
            - 'df_DG_containers' is obtained by filtering 'df_all_containers' to only include rows where the 'DG_Class' column is not empty.
            - 'df_DG_loadlist_cols' is created as a list of column names from 'd_cols_map' that exist in 'df_DG_containers' columns.
            - 'df_DG_loadlist' is obtained by selecting columns from 'df_DG_containers' based on 'df_DG_loadlist_cols'.
            - The column names in 'df_DG_loadlist' are updated using the values from 'd_cols_map' dictionary.
            - '__add_DG_ATT_states_to_df' method is called to add DG attribute states to 'df_DG_loadlist' DataFrame.
            - '_add_DG_missing_cols_to_df' method is called to add any missing DG columns to 'df_DG_loadlist' DataFrame.
            - '__reorder_df_DG_loadlist_cols' method is called to reorder the columns of 'df_DG_loadlist' DataFrame.
            - 'df_DG_loadlist' DataFrame is filled with empty string values to replace any NaN values.
            - The generated DG loadlist DataFrame is returned.

        Example:
            df_onboard_loadlist = pd.DataFrame({
                'ContainerID': ['C1', 'C2', 'C3'],
                'DG_Class': ['ClassA', '', 'ClassB'],
                'Weight': [100, 200, 300]
            })

            df_all_containers = pd.DataFrame({
                'ContainerID': ['C1', 'C2', 'C3'],
                'Description': ['Container1', 'Container2', 'Container3'],
                'DG_Class': ['ClassA', '', 'ClassB'],
                'Volume': [10, 20, 30]
            })

            d_cols_map = {
                'DG_Class': 'Dangerous Goods Class',
                'Weight': 'Weight (kg)',
                'Volume': 'Volume (m^3)'
            }

            result = get_df_DG_loadlist(df_onboard_loadlist, df_all_containers, d_cols_map)
            print(result)
            # Output:
            #   ContainerID  Dangerous Goods Class  Weight (kg) Description  Volume (m^3)
            # 0          C1                 ClassA          100   Container1            10
            # 1          C3                 ClassB          300   Container3            30
        """
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
        """
        Generates the exhaustive DG (Dangerous Goods) loadlist DataFrame based on container data.

        Args:
            df_all_containers (pd.DataFrame): The DataFrame representing all containers.
            d_cols_names (dict): A dictionary mapping column names.

        Returns:
            pd.DataFrame: The generated exhaustive DG loadlist DataFrame.

        Raises:
            None.

        Notes:
            - The method takes the DataFrame containing all containers 'df_all_containers' and a dictionary 'd_cols_names' mapping column names.
            - 'df_DG_containers' is obtained by filtering 'df_all_containers' to only include rows where the 'DGS_HAZARD_ID_1' column is not empty.
            - 'df_DG_containers' DataFrame is reshaped from wide to long format using 'pd.wide_to_long' function.
            - After reshaping, additional filtering is performed to remove rows where 'DGS_HAZARD_ID_' column is empty.
            - 'df_DG_containers' DataFrame is reset with a new index.
            - 'df_DG_loadlist' DataFrame is created by selecting specific columns from 'df_DG_containers'.
            - '__add_DG_ATT_states_to_df' method is called to add DG attribute states to 'df_DG_loadlist' DataFrame.
            - '_add_DG_missing_cols_to_df' method is called to add any missing DG columns to 'df_DG_loadlist' DataFrame.
            - The column names in 'df_DG_loadlist' are updated using the values from 'd_cols_names' dictionary.
            - '__reorder_df_DG_loadlist_cols' method is called to reorder the columns of 'df_DG_loadlist' DataFrame.
            - 'df_DG_loadlist' DataFrame is filled with empty string values to replace any NaN values.
            - The generated exhaustive DG loadlist DataFrame is returned.

        Example:
            df_all_containers = pd.DataFrame({
                'EQD_ID': ['C1', 'C2', 'C3'],
                'DGS_HAZARD_ID_1': ['Hazard1', '', 'Hazard2'],
                'Weight': [100, 200, 300]
            })

            d_cols_names = {
                'EQD_ID': 'Container ID',
                'DGS_HAZARD_ID_1': 'Hazardous Goods',
                'Weight': 'Weight (kg)'
            }

            result = get_df_DG_loadlist_exhaustive(df_all_containers, d_cols_names)
            print(result)
            # Output:
            #   Container ID  Hazardous Goods  Weight (kg)
            # 0           C1         Hazard1          100
            # 2           C3         Hazard2          300
        """
        
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
    
    def __process_stowage_and_segregation(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Processes the 'DGIES_STOW' and 'DGIES_SEG' columns of the DataFrame to generate 'Stowage and segregation' and 'SegregationGroup' columns.

        Args:
            df (pd.DataFrame): The DataFrame containing 'DGIES_STOW' and 'DGIES_SEG' columns.

        Returns:
            pd.DataFrame: The modified DataFrame with added 'Stowage and segregation' and 'SegregationGroup' columns.

        Raises:
            None.
        """

        stow = []
        seg = []

        df.fillna('', inplace=True)
        df = df.reset_index()

        for idx, row in df.iterrows():
            stow_list = df["DGIES_STOW"].iloc[idx].split()
            seg_list = df["DGIES_SEG"].iloc[idx].split()
            merge_list = stow_list + seg_list

            merge_list = [x for x in merge_list if x != "nan"]

            seg_list = []
            stow_list = []

            for i, val in enumerate(merge_list):
                if "SGG" in val:
                    seg_list.append(val)
                else:
                    stow_list.append(val.rstrip("abcde"))

            stow.append(', '.join(stow_list))
            seg.append(', '.join(seg_list))

        df['Stowage and segregation'] = stow
        df['SegregationGroup'] = seg
        df.set_index('index', inplace=True)

        return df
    
    def __filter_df_DG_loadlist(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Filters the DataFrame based on specific conditions and drops rows that do not meet the conditions.

        Args:
            df (pd.DataFrame): The DataFrame to be filtered.

        Returns:
            pd.DataFrame: The filtered DataFrame.

        Raises:
            None.
        """

        df.loc[
            (df['UNNO'] == '1950') & ('MAX' in df['Proper Shipping Name (Paragraph B of DOC)'].str.upper()) &
            (~df['Limited Quantity'].str.contains('TLQ', case=False, na=False)),
            'Limited Quantity'
        ] = 'TLQ'

        df.drop(
            df[
                (df['UNNO'] == '1950') & ('WASTE' in df['PSN']) &
                (~df['Proper Shipping Name (Paragraph B of DOC)'].str.upper().str.contains('WASTE', na=False))
            ].index,
            inplace=True
        )

        df.drop(
            df[
                (df['UNNO'] == '1950') & (df['Limited Quantity'].str.contains('TLQ', case=False, na=False)) &
                (df['LQ'].str.contains('None', na=False))
            ].index,
            inplace=True
        )

        df.drop(
            df[
                (df['UNNO'] == '1950') & (~df['Limited Quantity'].str.contains('TLQ', case=False, na=False)) &
                (~df['LQ'].str.contains('None', na=False))
            ].index,
            inplace=True
        )

        df.drop(
            df[
                (df['UNNO'] == '3528') & ('BELOW' in df['VARIATION'].str.upper()) &
                (df['FlashPoints'] >= 23)
            ].index,
            inplace=True
        )

        df.drop(
            df[
                (df['UNNO'] == '3528') & ('ABOVE' in df['VARIATION'].str.upper()) &
                (df['FlashPoints'] < 23)
            ].index,
            inplace=True
        )

        df.drop(
            df[
                (df['UNNO'] == '2037') & ('WASTE' in df['PSN'].str.upper()) &
                (~df['Proper Shipping Name (Paragraph B of DOC)'].str.upper().str.contains('WASTE', na=False))
            ].index,
            inplace=True
        )

        df.drop(
            df[
                (df['UNNO'] == '2037') & (~df['PSN'].str.upper().str.contains('WASTE', na=False)) &
                (df['Proper Shipping Name (Paragraph B of DOC)'].str.upper().str.contains('WASTE', na=False))
            ].index,
            inplace=True
        )

        df.drop(
            df[
                (df['UNNO'].isin(['3322', '3325', '3327'])) & ('4.2' in df['SubLabel1'].str.upper()) &
                ('4.2' not in df['SUBLABEL1'].str.upper())
            ].index,
            inplace=True
        )

        df.drop(
            df[
                (df['UNNO'].isin(['3322', '3325', '3327'])) & ('4.2' not in df['SubLabel1'].str.upper()) &
                ('4.2' in df['SUBLABEL1'].str.upper())
            ].index,
            inplace=True
        )

        df.drop(
            df[
                (df['UNNO'].isin(['2215', '2280', '1381', '2870'])) &
                (~df['PSN'].str.upper().isin(df['Proper Shipping Name (Paragraph B of DOC)'].str.upper()))
            ].index,
            inplace=True
        )

        df.drop(
            df[
                (df['UNNO'] == '3373') & ('P650' in df['VARIATION']) &
                (~df['Proper Shipping Name (Paragraph B of DOC)'].str.upper().str.contains('P650', na=False))
            ].index,
            inplace=True
        )

        df.drop(
            df[
                (df['UNNO'] == '3373') & ('P650' not in df['VARIATION']) &
                (df['Proper Shipping Name (Paragraph B of DOC)'].str.upper().str.contains('P650', na=False))
            ].index,
            inplace=True
        )

        df.drop(
            df[
                (df['UNNO'] == '2794') & ('x' in df['Closed Freight Container']) &
                ('B' in df['STOWCAT'].str.upper())
            ].index,
            inplace=True
        )

        df.drop(
            df[
                (df['UNNO'] == '2794') & ('x' not in df['Closed Freight Container']) &
                ('A' in df['STOWCAT'].str.upper())
            ].index,
            inplace=True
        )

        return df

    def __add_not_permitted_bay_column(self, df_DG_loadlist: pd.DataFrame) -> pd.DataFrame:
        """
        Adds the "not permitted bay" column to the DataFrame based on specific conditions and mappings.

        Args:
            df_DG_loadlist (pd.DataFrame): The DataFrame to which the column will be added.
            d_DG_enrichment_map (dict): The dictionary containing the mappings and thresholds.

        Returns:
            pd.DataFrame: The DataFrame with the "not permitted bay" column added.

        Raises:
            None.
        """
        #Not Permitted Bay 
        not_permitted_bay_col_name = self.__DG_rules["Not_permitted_bay_column_name"]
        Not_Permitted_Bay_list = self.__DG_rules["UNNO_not_permitted_bay"]
        df_DG_loadlist[not_permitted_bay_col_name] = df_DG_loadlist.apply(lambda x: "x" if x['UN'] in Not_Permitted_Bay_list  else "" , axis=1)
        Dynamic_Not_Permitted_Bay_dict = self.__DG_rules["Dynamic_not_permitted_bay"]
    
        df_copy = df_DG_loadlist.copy(deep=False)
        df_copy['FlashPoints'].fillna("-1000", inplace=True)
        df_copy['FlashPoints'] = pd.to_numeric(df_copy['FlashPoints'], inplace=True)

        # # to be set later in function for modularity 
        # #def __classify_flashpoint(self, fp_low_threshold, fp_high_threshold, dg_loadlist: pd.DataFrame) -> pd.DataFrame:

        fp_low_threshold = float(self._DG_loadlist_enrichment_map ["FP_Threshold_Low"]) 
        fp_high_theshold = float(self._DG_loadlist_enrichment_map ["FP_Threshold_High"])  
        
        PE_Conditions = [
            (df_copy['FlashPoints'] == -1000.0),
            (df_copy['FlashPoints'] < fp_low_threshold),
            (df_copy['FlashPoints']  >= fp_low_threshold) & (df_copy['FlashPoints']  <= fp_high_theshold),
            (df_copy['FlashPoints']  >  fp_high_theshold)
        ]
        PE_Categories = ['0', '1', '2', '3']
        temp_list= []
        temp_list = np.select(PE_Conditions, PE_Categories)
        df_copy['FlashPoint_Category'] = temp_list   

        df_copy["new"] = df_copy.apply(lambda x: ','.join([str(x['Class']), str(x['Liquid']),str(x['Solid']), str(x['FlashPoint_Category'])]), axis=1)   
        df_copy["not permitted bay dynamic"]= df_copy["new"].map(Dynamic_Not_Permitted_Bay_dict)  
        df_copy[not_permitted_bay_col_name] = df_copy.apply(lambda x: "x" if ((x["not permitted bay dynamic"] == "x" and x[not_permitted_bay_col_name] != "x") or (x[not_permitted_bay_col_name] == "x")) else "" , axis=1)
        
        return df_DG_loadlist
    
    def output_DG_loadlist(self) -> pd.DataFrame:
        self.logger.info("Extracting and saving DG LoadList...")
        
        df_copy = self._df_all_containers.copy()
    
        if "DGS_ATT_AGR_DETAIL_DESCRIPTION_CODE_1" not in df_copy.columns:
            df_copy["DGS_ATT_AGR_DETAIL_DESCRIPTION_CODE_1"] = ""
    
        naming_schema = self._DG_loadlist_enrichment_map["DG_LOADLIST_SCHEMA"]
        df_DG_loadlist = self.get_df_DG_loadlist_exhaustive(self._df_all_containers, naming_schema)
        
        # fill for now as all "x"
        # Need to explore type of for any indicator to other than closed freight container 
        # Assumption is all containers are closed and and DGs' are in packing 
        df_DG_loadlist["Closed Freight Container"] = "x"
        df_DG_loadlist["Package Goods"] = "x"

        # Mapping for Packaging Group (1,2,3) -> (I,II,III)
        PGr_map = self._DG_loadlist_enrichment_map["PACKAGING_GROUP"]
        df_DG_loadlist["PGr"] = df_DG_loadlist["PGr"].map(PGr_map)

        # Stowage Category 
        # fix data type and try 
        imdg_codes_df = self._imdg_haz_exis
        df_DG_loadlist.fillna('', inplace=True)
        imdg_codes_df.fillna('', inplace=True)
        df_DG_loadlist = df_DG_loadlist.applymap(str)
        imdg_codes_df = imdg_codes_df.applymap(str)
        df_DG_loadlist['UN'] = df_DG_loadlist['UN'].astype(int).apply(lambda x: '{:04d}'.format(x))
        imdg_codes_df['UNNO'] = imdg_codes_df['UNNO'].astype(int).apply(lambda x: '{:04d}'.format(x))
        #Set index to a column as Join resets index 
        df_DG_loadlist["Index"] = df_DG_loadlist.index 

        #Joining DG loadlist to Imdg code 
        ### later step --> match to ammendment version as well in case any beside 40-20 is used in Baplie message by user 
    
        df_DG_loadlist["Ammendmant Version"] = df_DG_loadlist.apply(lambda x:  x['Ammendmant Version'][:2] , axis=1)
        
        df_DG_loadlist = df_DG_loadlist.merge(imdg_codes_df[['PSN','STATE','LQ','CLASS','SUBLABEL1','SUBLABEL2','UNNO', 'IMDG_AMENDMENT','PG', 'STOWCAT','DGIES_STOW','DGIES_SEG','VARIATION']],
                        how='left' ,left_on=['Ammendmant Version', 'Class', 'SubLabel1', 'SubLabel2','UN', 'PGr'], right_on= ['IMDG_AMENDMENT','CLASS','SUBLABEL1', 'SUBLABEL2','UNNO','PG'])

        df_DG_loadlist['Stowage Category'] = df_DG_loadlist['STOWCAT']

        # loop over rows of the data frame
        print(df_DG_loadlist.columns)
        df_DG_loadlist = self.__filter_df_DG_loadlist(df_DG_loadlist)

        df_DG_loadlist.drop_duplicates(subset=["Serial Number", "POL", "POD", "Class", "SubLabel1",
                                                "SubLabel2", "Proper Shipping Name (Paragraph B of DOC)", "Weight",
                                                "PGr", "UN", "Stowage Category", "Index"], inplace=True)
        

        df_matched = df_DG_loadlist.drop(df_DG_loadlist[df_DG_loadlist['Stowage Category'].isnull()].index.to_list())
        df_matched = df_matched.reset_index(drop=True)
        
        # check and handle duplicates of same stowage category for DG matching a category from imdg code
        df_matched = df_DG_loadlist.drop(df_DG_loadlist[df_DG_loadlist['Stowage Category'].isnull()].index.to_list())
        
        # get DG Goods that did not match to any category from imdg code 
        df_not_matched = df_DG_loadlist[df_DG_loadlist['Stowage Category'].isnull()]
        df_not_matched = df_not_matched[['Serial Number', 'Operator', 'POL', 'POD', 'Type', 'Closed Freight Container', 'Weight', 'Regulation Body', 'Ammendmant Version', 'UN',
        'Class', 'SubLabel1', 'SubLabel2', 'DG-Remark (SW5 = Mecanical Ventilated Space if U/D par.A DOC)', 'FlashPoints', 'Loading remarks', 'Limited Quantity',
        'Marine Pollutant', 'PGr', 'Liquid', 'Solid', 'Flammable', 'Non-Flammable', 'Proper Shipping Name (Paragraph B of DOC)', 'SegregationGroup', 'SetPoint',
        'Stowage and segregation', 'Package Goods', 'Stowage Category', 'not permitted bay 74', 'Zone']]
        
        

        # try matching on UN Number without class and Subclass, as to handle class 1 in particular or dg with incorrect class, sublabel or generic subsidiary risk (eg. 2 instead of 2.1)

        df_not_matched = df_not_matched.merge(imdg_codes_df[['PSN','STATE','LQ','CLASS','SUBLABEL1','SUBLABEL2','UNNO', 'IMDG_AMENDMENT','PG', 'STOWCAT','DGIES_STOW','DGIES_SEG','VARIATION']],
                        how='left' ,left_on=['Ammendmant Version','UN', 'PGr'], right_on= ['IMDG_AMENDMENT','UNNO','PG'])
        df_not_matched['Stowage Category'] = df_not_matched['STOWCAT']
        df_not_matched.drop_duplicates(subset=["Serial Number", "POL", "POD", "Class", "SubLabel1",
                                                "SubLabel2", "Proper Shipping Name (Paragraph B of DOC)", "Weight",
                                                "PGr", "UN", "Stowage Category"], inplace=True)
        
        df_not_matched.reset_index(drop=True)
        
        # Concat both dataframes
        df_DG_loadlist = pd.concat([df_matched, df_not_matched],ignore_index=True) 
        df_copy1 = df_DG_loadlist.copy()
        df_copy1['Stowage Category'] = df_copy1['Stowage Category'].map(lambda x: str(x))
        df_copy1.reset_index(inplace= True, drop=True)
        
        #Stowage Category order from most to least critical 
        sort_order= ["1", "2", "3", "4", "5", "A", "B", "C", "D", "E"]
        # Group By Serial Number and order in decreasing criticality
        df_copy1= df_copy1.groupby(["Serial Number", "POL", "POD", "Class", "SubLabel1", "SubLabel2", "Proper Shipping Name (Paragraph B of DOC)", "Weight", "PGr", "UN", "Stowage Category"],group_keys= False).apply(lambda x: x.sort_values(by="Stowage Category", key=lambda y: [sort_order.index(v) for v in y]))
        df_copy1.reset_index( drop=True)
        
        # df_copy1.set_index("level_0", inplace=True)
        # Keep first row which is most critical
        df_copy1.drop_duplicates(subset = ["Serial Number", "POL", "POD", "Class", "SubLabel1", "PGr", "UN", "Stowage Category"], inplace=True)
        
        # Group Back both DataFrames 
        df_DG_loadlist = df_copy1.copy()
        df_DG_loadlist = self.__process_stowage_and_segregation(df_DG_loadlist)

        # filling up LQ, State, Flammability and Zone  
        df_DG_loadlist["Limited Quantity"] = df_DG_loadlist.apply(lambda x: "yes" if x['Limited Quantity'] == "TLQ" else "" , axis=1)
        df_DG_loadlist["Liquid"] = df_DG_loadlist.apply(lambda x: "x" if (x['STATE'] == "L" or "LIQUID" in x['Proper Shipping Name (Paragraph B of DOC)'].upper()
                                                                            or "SOLUTION" in x['Proper Shipping Name (Paragraph B of DOC)'].upper()) 
                                                                            else "" , axis=1)
        
        df_DG_loadlist["Solid"] = df_DG_loadlist.apply(lambda x: "x" if (x['STATE'] == "S" or "SOLID" in x['Proper Shipping Name (Paragraph B of DOC)'].upper() 
                                                                        or "DRY" in x['Proper Shipping Name (Paragraph B of DOC)'].upper() 
                                                                        or "POWDER" in x['Proper Shipping Name (Paragraph B of DOC)'].upper()) 
                                                                        else "" , axis=1)
        ## Check criteria for flammability 
        df_DG_loadlist["Flammable"] = df_DG_loadlist.apply(lambda x: "x" if (x['Class'] in ['1.3','2.1','3','4.1','4.2','4.3'] 
                                                                    or x['SubLabel1'] in ['1.3','2.1','3','4.1','4.2','4.3'] 
                                                                    or x['SubLabel2'] in ['1.3','2.1','3','4.1','4.2','4.3'] 
                                                                    or "FLAMMABLE" in x['Proper Shipping Name (Paragraph B of DOC)'].upper() 
                                                                    or x['Flammable'] == "x" ) else "" , axis=1)

        df_DG_loadlist["Non-Flammable"] = df_DG_loadlist.apply(lambda x: "" if x['Flammable'] == "x" else "x" , axis=1)
        # apply zone mapping  
        Zone_map = self._DG_loadlist_enrichment_map["PORT_ZONE"]
        df_DG_loadlist["Zone"] = df_DG_loadlist["POL"].str[:5].map(Zone_map)
        
        #Not Permitted Bay 
        df_DG_loadlist = self.__add_not_permitted_bay_column(df_DG_loadlist, self._DG_loadlist_enrichment_map, self.__DG_rules)
        # Loading Remarks 
        segregation_group_map = self._DG_loadlist_enrichment_map["SEGREGATION_GROUP"]
        df_copy["Loading remarks"] = df_copy["SegregationGroup"].apply(lambda row: "" if row == "" else ", ".join([segregation_group_map[value] for value in row.split(", ")]))


        df_DG_loadlist = df_copy.copy()        

        df_DG_loadlist = df_DG_loadlist[["Serial Number","Operator","POL","POD","Type","Closed Freight Container",
                            "Weight","UN","Class","SubLabel1",
                            "DG-Remark (SW5 = Mecanical Ventilated Space if U/D par.A DOC)","FlashPoints",
                            "Loading remarks","Limited Quantity","Marine Pollutant","PGr","Liquid","Solid",
                            "Flammable","Non-Flammable","Proper Shipping Name (Paragraph B of DOC)","SegregationGroup",
                            "SetPoint","Stowage and segregation","Package Goods","Stowage Category","not permitted bay 74",
                            "Zone"]]
                
        return df_DG_loadlist
    
    
    def __get_DG_category(self, un_no: str, imdg_class: str, sub_label: str,
                        as_closed: str, liquid: str, solid: str, flammable: str, flash_point: float, l_explosion_protect_IIB_T4: list, l_IMDG_Class_1_S: list) -> str:
        
        if ( un_no in l_IMDG_Class_1_S)\
        or (imdg_class == '6.1' and solid == True and as_closed == True)\
        or (imdg_class == '8' and liquid == True and flash_point is None)\
        or (imdg_class == '8' and solid == True)\
        or (imdg_class == '9' and as_closed == True):
            return 'PPP'
        
        if (imdg_class == '2.2')\
        or (imdg_class == '2.3' and flammable == False and as_closed == True)\
        or (imdg_class == '3' and flash_point >= 23 )\
        or (imdg_class == '4.1' and as_closed == True)\
        or (imdg_class == '4.2' and as_closed == True)\
        or (imdg_class == '4.3' and liquid == True and as_closed == True and (flash_point >= 23 or flash_point is None))\
        or (imdg_class == '4.3' and solid == True and as_closed == True)\
        or (imdg_class == '5.1' and as_closed == True)\
        or (imdg_class == '8' and liquid == True and as_closed == True and flash_point >= 23 and flash_point <= 60):
            return 'PP'
        
        if (imdg_class == '6.1' and liquid == True and flash_point is None):
            return 'PX'
        
        if (imdg_class == '6.1' and liquid == True and flash_point >= 23 and flash_point <= 60 and as_closed == True):
            return 'PX+'
        
        if (imdg_class == '2.1' and as_closed == True and un_no not in l_explosion_protect_IIB_T4)\
        or (imdg_class == '2.3' and flammable == True and as_closed == True and un_no not in l_explosion_protect_IIB_T4 and sub_label != '2.1')\
        or (imdg_class == '3' and flash_point < 23 and as_closed == True and un_no not in l_explosion_protect_IIB_T4)\
        or (imdg_class == '6.1' and liquid == True and flash_point < 23 and as_closed == True and un_no not in l_explosion_protect_IIB_T4)\
        or (imdg_class == '8' and liquid == True and flash_point < 23 and as_closed == True and un_no not in l_explosion_protect_IIB_T4):
            return 'XP'
        
        if (imdg_class == '6.1' and solid == True and as_closed == False)\
        or (imdg_class == '9' and as_closed == False):
            return 'XX-'
        
        if (imdg_class == '2.1' and (as_closed == False or un_no in l_explosion_protect_IIB_T4))\
        or (imdg_class == '2.3' and flammable == True and (as_closed == False or un_no in l_explosion_protect_IIB_T4 or sub_label == '2.1'))\
        or (imdg_class == '2.3' and flammable == False and as_closed == False)\
        or (imdg_class == '3' and flash_point < 23 and (as_closed == False or un_no in l_explosion_protect_IIB_T4))\
        or (imdg_class == '4.1' and as_closed == False)\
        or (imdg_class == '4.2' and as_closed == False)\
        or (imdg_class == '4.3' and liquid == True and (as_closed == False or flash_point < 23))\
        or (imdg_class == '4.3' and solid == True and as_closed == False)\
        or (imdg_class == '5.1' and as_closed == False)\
        or (imdg_class == '5.2')\
        or (imdg_class == '6.1' and liquid == True and flash_point < 23 and (as_closed == False or un_no in l_explosion_protect_IIB_T4))\
        or (imdg_class == '6.1' and liquid == True and flash_point >= 23 and flash_point <= 60 and as_closed == False)\
        or (imdg_class == '8' and liquid == True and flash_point < 23 and (as_closed == False or un_no in l_explosion_protect_IIB_T4))\
        or (imdg_class == '8' and liquid == True and flash_point >= 23 and flash_point <= 60 and as_closed == False):
            return 'XX'
            
        if (imdg_class[0] == '1' and un_no not in l_IMDG_Class_1_S):
            return 'XX+'
            
        if (imdg_class == '6.2')\
        or (imdg_class == '7'):
            return 'XXX'
        
        return '?'

    

    def __expand_exclusion(self, set_exclusions: set, l_updates: list) -> set:
        
        # taking update elements one by one
        for update in l_updates:
            
            # if update is a simple string, it is the string bbbT, bbb bay number, T macro-tier 0 or 1
            if type(update) == str:
                # look if there is already an element concerning this macro combination in the set, if yes remove it
                bay = update[0:3]
                macro_tier = update[3]
                for exclusion in set_exclusions:
                    if exclusion[0] == bay and exclusion[2][0] == macro_tier:
                        set_exclusions.remove(exclusion)
                        break
                # and in any case create it, the exclusion is on the totality of the bay + macro-tier
                set_exclusions.add((bay, None, (macro_tier, None)))
                
            # if update is a triplet, before adding it, verify it is not already covered by an existing one
            if type(update) == tuple:
                bay = update[0]
                l_rows = update[1]
                macro_tier = update[2][0]
                l_tiers = update[2][1]
                # we should refine, but it is useless, at least for the time being
                # so integrate if the total coverage of the bay + macro tier is not already existing
                integration = True
                for exclusion in set_exclusions:
                    if exclusion[0] == bay and exclusion[2][0] == macro_tier:
                        if exclusion[1] is None and exclusion[2][1] is None:
                            integration = False
                            break
                if integration == True:
                    set_exclusions.add(update)
        
        return set_exclusions
# ==============================================================================================================================================

# DG by CG Related Functions 
    def __get_stacks_capacities(self, fn_stacks: pd.DataFrame) -> dict:
        
        d_stacks = {}

        #f_stacks = open(fn_stacks, 'r')
        
        for idx, row in fn_stacks.iterrows():

            bay = fn_stacks["Bay"].iloc[idx] # sur 2 caractères
            row = fn_stacks["Row"].iloc[idx] # sur 2 caractères
            macro_tier = fn_stacks["Tier"].iloc[idx]
            subbay = fn_stacks["SubBay"].iloc[idx] 
            first_tier = fn_stacks["FirstTier"].iloc[idx]
            max_nb_std_cont = int(fn_stacks["MaxNbOfStdCont"].iloc[idx])
            odd_slot = int(fn_stacks["OddSlot"].iloc[idx])
            max_nb_45 = int(fn_stacks["MaxNb45"].iloc[idx])
            min_40_sub_45 = int(fn_stacks["Min40sub45"].iloc[idx])
            nb_reefer = int(fn_stacks["NbReefer"].iloc[idx])
            max_weight = float(fn_stacks["MaxWeight"].iloc[idx])
            stack_height = float(fn_stacks["StackHeight"].iloc[idx])
            max_nb_HC_at_max_stack = int(fn_stacks["MaxNbHCAtMaxStack"].iloc[idx])
        
            stack = (bay, row, macro_tier)
        
            d_stacks[stack] = {'subbay': subbay, 'first_tier': first_tier, 
                            'max_nb_std_cont': max_nb_std_cont, 'odd_slot': odd_slot, 'nb_reefer': nb_reefer,
                            'max_nb_45': max_nb_45, 'min_40_sub_45': min_40_sub_45,
                            'max_nb_HC_at_max_stack': max_nb_HC_at_max_stack,
                            'stack_height': stack_height, 'max_weight': max_weight}
        
        return d_stacks


    def __get_bays_macro_tiers_l_subbays(self, d_stacks: dict) -> dict:
        
        d_bay_macro_tier_l_subbays = {}
        
        for (bay, row, macro_tier), d_stack_items in d_stacks.items():
            
            bay_macro_tier = (bay, macro_tier)
            subbay = d_stack_items['subbay']
            
            if bay_macro_tier not in d_bay_macro_tier_l_subbays:
                d_bay_macro_tier_l_subbays[bay_macro_tier] = set()
            d_bay_macro_tier_l_subbays[bay_macro_tier].add(subbay)
        
        return d_bay_macro_tier_l_subbays   

    def __list_areas_for_zone_intersections(self, d_ix_zones: dict, l_zones: list) -> dict:
        
        l_ix_zones = [(ix_zone, nb_containers) for ix_zone, nb_containers in d_ix_zones.items()]
        
        d_combi_zones = {}
        # for N zones, 2 ** N combinations
        N = len(d_ix_zones)
        nb_combi = 2 ** N
        for cx in range(nb_combi):
            # eliminate the empty combination
            if cx == 0: continue
            # binary string of 0 or 1, left-padded with 0
            # 2: because of prefix '0b'
            cx_bin = bin(cx)[2:].zfill(N)
            s_combi_area = set()
            nb_containers = 0
            first_zone = True
            for ix, cix in enumerate(cx_bin):
                if cix == '0': continue
                if first_zone == True:
                    s_combi_area = l_zones[l_ix_zones[ix][0]].copy()
                    first_zone = False
                else:
                    s_combi_area = s_combi_area.intersection(l_zones[l_ix_zones[ix][0]].copy())
                nb_containers += l_ix_zones[ix][1]
            if len(s_combi_area) > 0:
                #for ix, cix in enumerate(cx_bin): print(ix, cix)
                #print(nb_containers)
                if frozenset(s_combi_area) not in d_combi_zones:
                    d_combi_zones[frozenset(s_combi_area)] = 0
                # important point, we take the maximum of sums, and not sum of sums
                # that is the number of the most complete configuration
                # in case of having more of one combination for the same final zone, 
                # we don't sum up, and just take the maximum of container numbers, 
                # which represents the maximal covering
                d_combi_zones[frozenset(s_combi_area)] = max(nb_containers, d_combi_zones[frozenset(s_combi_area)])
                
        return d_combi_zones

    def __get_zone_list_subbays(self, s_area: frozenset, d_bay_macro_tier_l_subbays: dict) -> set:
        
        s_area_subbays = set()
        for area in s_area:
            for subbay in d_bay_macro_tier_l_subbays[area]:
                if subbay not in s_area_subbays:
                    s_area_subbays.add(subbay)
                    
        return s_area_subbays
    
    
    
    def __update_table_7_2_4(self, row, table, stowage_code, class_sublabel, new_value, unique_list) -> None:
        segregation_list = str(row['Stowage and segregation']).split(',')
        if stowage_code in segregation_list and stowage_code not in [code.strip() for code in segregation_list if code != stowage_code] and class_sublabel in unique_list:
            if table.loc[row['Class'], class_sublabel] in {'X', ''} or float(table.loc[row['Class'], class_sublabel]) < new_value:
                table.at[(row['Class'], class_sublabel)] = new_value
                
            if table.loc[class_sublabel, row['Class']] in {'X', ''} or float(table.loc[class_sublabel, row['Class']]) < new_value:
                table.at[(class_sublabel, row['Class'])] = new_value

    
    def __update_table_7_2_4_sgg(self, row, dg_loadlist, table, stowage_code, sgg, new_value, unique_list):
        segregation_list = str(row['Stowage and segregation']).split(',')
        if stowage_code in segregation_list and stowage_code not in [code.strip() for code in segregation_list if code != stowage_code] and sgg in unique_list:
    
            for indx, ro in dg_loadlist.iterrows():
                
                if sgg in ro['SegregationGroup']: 
    
                    if table.loc[row['Class'], ro['Class']] in {'X', ''} or float(table.loc[row['Class'], ro['Class']]) < new_value:
                        table.at[(row['Class'], ro['Class'])] = new_value
                
                    if table.loc[ro['Class'], row['Class']] in {'X', ''} or float(table.loc[ro['Class'], row['Class']]) < new_value:
                        table.at[(ro['Class'], row['Class'])] = new_value



    def __compare_lists_and_replace(self, row, table_7_2_4, table, stowage_code, seg_class):   
        segregation_list = str(row['Stowage and segregation']).split(',')
        if stowage_code in segregation_list and stowage_code not in [code.strip() for code in segregation_list if code != stowage_code]:
            result = []
            replacement_value = table[seg_class].to_list()
            compare_list_v = table_7_2_4[row['Class']].to_list()

            for i in range(len(compare_list_v)):
                if replacement_value[i] == 'X' or replacement_value[i] == '*':
                    result.append(compare_list_v[i])
                elif compare_list_v[i] == 'X' or compare_list_v[i] == '*':
                    result.append(replacement_value[i])
                elif int(replacement_value[i]) > int(compare_list_v[i]) and str(replacement_value[i]).isnumeric() and str(compare_list_v[i]).isnumeric():
                    result.append(replacement_value[i]) 
                else:
                    result.append(compare_list_v[i])
            table_7_2_4[row['Class']] = result  

            result = [] 
            compare_list_v = table_7_2_4.loc[row['Class']].to_list()
            for i in range(len(compare_list_v)):
                if replacement_value[i] == 'X' or replacement_value[i] == '*':
                    result.append(compare_list_v[i])
                elif compare_list_v[i] == 'X' or compare_list_v[i] == '*':
                    result.append(replacement_value[i])
                elif int(replacement_value[i]) > int(compare_list_v[i]) and str(replacement_value[i]).isnumeric() and str(compare_list_v[i]).isnumeric():
                    result.append(replacement_value[i]) 
                else:
                    result.append(compare_list_v[i])       
            table_7_2_4.loc[row['Class']] = result
            
    def __output_adjusted_table_7_2_4(self, table_7_2_4:pd.DataFrame, df_DG_loadlist: pd.DataFrame) -> pd.DataFrame:
        self.logger.info("Extracting and saving Adjusted IMDG table_7_2_4...")
        df_DG_classes_expanded = table_7_2_4.copy()
        df_DG_classes_expanded.columns = df_DG_classes_expanded.columns.astype(float)
        df_DG_classes_expanded.index = df_DG_classes_expanded.index.astype(float)
        df_DG_classes_expanded_reference = df_DG_classes_expanded.copy()
            
            

        df_temp = df_DG_loadlist
        df_temp = df_temp[['Serial Number','UN','Class','SubLabel1','Proper Shipping Name (Paragraph B of DOC)','SegregationGroup','Stowage and segregation','FlashPoints', 'Flammable', 'Non-Flammable', 'Stowage Category']]
        df_temp = df_temp.reset_index()
        # Change Class & SubLabel  Columns to float type 
        DG_Loadlist = df_temp.copy()
        DG_Loadlist[['Class', 'SubLabel1']] = DG_Loadlist[['Class', 'SubLabel1']].apply(lambda x: x.replace('', np.nan))
        DG_Loadlist = DG_Loadlist.astype({'Class': float, 'SubLabel1': float})
        
        # extract the "Stowage and segregation" column as a list of lists
        sg_list = DG_Loadlist['Stowage and segregation'].tolist()

        # split each string in the list of lists by commas and remove non-SG indexes
        sg_list = [x.split(',') for x in sg_list if 'SG' in x]
        sg_list = [[y.strip() for y in x if y.strip().startswith('SG')] for x in sg_list]
        sg_str = [', '.join(x) for x in sg_list]
        DG_Loadlist['Stowage and segregation'] = pd.Series(sg_str)

        unique_class_sublabel = DG_Loadlist[['Class', 'SubLabel1']].values.ravel()
        unique_class_sublabel = pd.Series(unique_class_sublabel).dropna().unique()
        unique_class_sublabel = [x for x in unique_class_sublabel if x != '']
            
        unique_sgg = DG_Loadlist[['SegregationGroup']].values.ravel()
        new_list = [ss.strip() for s in unique_sgg for ss in s.split(',')]
        unique_sgg = pd.Series(new_list).dropna().unique()
        unique_sgg = [x for x in unique_sgg if x != '']
            
        for index, row in DG_Loadlist.iterrows():
                
            if row['SubLabel1'] in [1.1, 1.2, 1.5, 1.3, 1.6, 1.4]: 
                    keep_list = df_DG_classes_expanded[row['Class']].to_list()[:6]
                    self.__compare_lists_and_replace(row, df_DG_classes_expanded, df_DG_classes_expanded_reference, 'SG1', 1.3)
                    row_list = df_DG_classes_expanded[row['Class']].to_list()
                    row_list[:len(keep_list)] = keep_list
                    df_DG_classes_expanded[row['Class']] = row_list


            self.__compare_lists_and_replace(row, df_DG_classes_expanded, df_DG_classes_expanded_reference, 'SG2', 1.2)
            self.__compare_lists_and_replace(row, df_DG_classes_expanded, df_DG_classes_expanded_reference, 'SG3', 1.3)
            self.__compare_lists_and_replace(row, df_DG_classes_expanded, df_DG_classes_expanded_reference, 'SG4', 2.1)
            self.__compare_lists_and_replace(row, df_DG_classes_expanded, df_DG_classes_expanded_reference, 'SG5', 3.0)
            self.__compare_lists_and_replace(row, df_DG_classes_expanded, df_DG_classes_expanded_reference, 'SG6', 5.1)
                
            #Stow Away from Class
            self.__update_table_7_2_4(row, df_DG_classes_expanded, 'SG7', 3.0, 1, unique_class_sublabel)
            self.__update_table_7_2_4(row, df_DG_classes_expanded, 'SG8', 4.1, 1, unique_class_sublabel)
            self.__update_table_7_2_4(row, df_DG_classes_expanded, 'SG9', 4.3, 1, unique_class_sublabel)
            self.__update_table_7_2_4(row, df_DG_classes_expanded, 'SG10', 5.1, 1, unique_class_sublabel)
            self.__update_table_7_2_4(row, df_DG_classes_expanded, 'SG11', 6.2, 1, unique_class_sublabel)
            self.__update_table_7_2_4(row, df_DG_classes_expanded, 'SG12', 7.0, 1, unique_class_sublabel)
            self.__update_table_7_2_4(row, df_DG_classes_expanded, 'SG13', 8.0, 1, unique_class_sublabel)
                #SG14
            if 'SG14' in str(row['Stowage and segregation']):
                elements = [1.1, 1.2, 1.5, 1.3, 1.6, 1.4]
                for e in elements:
                    if str(e) in unique_class_sublabel:
                        cols_rows = [float(x) for x in str(e).split('.')]
                        if df_DG_classes_expanded.loc[row.Class, cols_rows[0]] == 'X' or df_DG_classes_expanded.loc[row.Class, cols_rows[0]] < 2:
                            df_DG_classes_expanded.at[(row.Class, cols_rows[0]), (cols_rows[1], row.Class)] = 2

            #SG15 - SG19 --> seperated from class
            self.__update_table_7_2_4(row, df_DG_classes_expanded, 'SG15', 3.0, 2, unique_class_sublabel)
            self.__update_table_7_2_4(row, df_DG_classes_expanded, 'SG16', 4.1, 2, unique_class_sublabel)
            self.__update_table_7_2_4(row, df_DG_classes_expanded, 'SG17', 5.1, 2, unique_class_sublabel)
            self.__update_table_7_2_4(row, df_DG_classes_expanded, 'SG18', 6.2, 2, unique_class_sublabel)
            self.__update_table_7_2_4(row, df_DG_classes_expanded, 'SG19', 7.0, 2, unique_class_sublabel)
                
            #SG20  --> Stow away from SGG1- acids
            self.__update_table_7_2_4_sgg(row, DG_Loadlist, df_DG_classes_expanded, 'SG20','SGG1', 1, unique_sgg)
            #SG21  --> Stow away from SGG18- alkalis
            self.__update_table_7_2_4_sgg(row, DG_Loadlist, df_DG_classes_expanded, 'SG21','SGG18', 1, unique_sgg)
            #SG22  --> Stow away from ammonium salsts --> considered as SGG2- ammonium compounds
            self.__update_table_7_2_4_sgg(row, DG_Loadlist, df_DG_classes_expanded, 'SG22','SGG2', 1, unique_sgg)
            #SG23 --> away from animal & vegetable oils
            ############
            #SG24 --. Stow Away from SGG17- azides
            self.__update_table_7_2_4_sgg(row, DG_Loadlist, df_DG_classes_expanded, 'SG24','SGG17', 1, unique_sgg)
            #SG25 --> seperated from goods of class 2.1 and 3
            self.__update_table_7_2_4(row, df_DG_classes_expanded, 'SG25', 2.1, 2, unique_class_sublabel)
            self.__update_table_7_2_4(row, df_DG_classes_expanded, 'SG25', 3.0, 2, unique_class_sublabel)
            #SG26 --> seperation category 4 from class 2.1 and 3 (complementary to SG25) when on deck 
            self.__update_table_7_2_4(row, df_DG_classes_expanded, 'SG26', 2.1, 4, unique_class_sublabel)
            self.__update_table_7_2_4(row, df_DG_classes_expanded, 'SG26', 3.0, 4, unique_class_sublabel)
            #SG27 --> Stow seperated from explosives containing chlorates or perchlorates

            if any(x in list(DG_Loadlist['UN'].unique()) for x in ['0075', '0160', '0224', '0451', '0465', '0511']):
                self.__update_table_7_2_4(row, df_DG_classes_expanded, 'SG27', 1.1, 1, unique_class_sublabel)
            if any(x in list(DG_Loadlist['UN'].unique()) for x in ['0245', '0395', '0466', '0467']):
                self.__update_table_7_2_4(row, df_DG_classes_expanded, 'SG27', 1.2, 1, unique_class_sublabel)
            if any(x in list(DG_Loadlist['UN'].unique()) for x in ['0158', '0159', '0161', '0183', '0246', '0335', '0396', '0508']):
                self.__update_table_7_2_4(row, df_DG_classes_expanded, 'SG27', 1.3, 1, unique_class_sublabel)
            if any(x in list(DG_Loadlist['UN'].unique()) for x in ['0312', '0454', '0505', '0506', '0507', '0509']):
                self.__update_table_7_2_4(row, df_DG_classes_expanded, 'SG27', 1.4, 1, unique_class_sublabel)
            if any(x in list(DG_Loadlist['UN'].unique()) for x in ['0332']):
                self.__update_table_7_2_4(row, df_DG_classes_expanded, 'SG27', 1.5, 1, unique_class_sublabel)
            #################
            #SG28 --> Stow seperated form SGG2 - ammonium compoinds and explosives containing amonium compounds or salts 
            self.__update_table_7_2_4_sgg(row, DG_Loadlist, df_DG_classes_expanded, 'SG28','SGG2', 2, unique_sgg)
            ## add explosives 
            #SG29 --> Segregation from foodstuff 
            ##### --> not in scope
            #SG30 --> Stow away from SGG7 - heavy metals and their salts 
            self.__update_table_7_2_4_sgg(row, DG_Loadlist, df_DG_classes_expanded, 'SG30','SGG7', 1, unique_sgg)
            #SG31 --> Stow away from SGG9 - lead and its compunds 
            self.__update_table_7_2_4_sgg(row, DG_Loadlist, df_DG_classes_expanded, 'SG31','SGG9', 1, unique_sgg)
            #SG32 --> Stow away from SGG10 - liquid halogenated hydrcarbons 
            self.__update_table_7_2_4_sgg(row, DG_Loadlist, df_DG_classes_expanded, 'SG32','SGG10', 1, unique_sgg)
            #SG33 --> Stow away from SGG15 - powdered metals 
            self.__update_table_7_2_4_sgg(row, DG_Loadlist, df_DG_classes_expanded, 'SG33','SGG15', 1, unique_sgg)
            #SG34 -->  when containing ammonium compunds, Stow seperated from SGG4 - Chlorates or SGG13 - perchlrates 
            if 'SGG2' in str(row['SegregationGroup']): 
                self.__update_table_7_2_4_sgg(row, DG_Loadlist, df_DG_classes_expanded, 'SG34','SGG4', 2, unique_sgg)
                self.__update_table_7_2_4_sgg(row, DG_Loadlist, df_DG_classes_expanded, 'SG34','SGG13', 2, unique_sgg)
                ## add Stow seperated from explosives containing chlorates or perchlorates
            #SG35 -->  Stow seperated from SGG1 - Acids  
            self.__update_table_7_2_4_sgg(row, DG_Loadlist, df_DG_classes_expanded, 'SG35','SGG1', 2, unique_sgg)
            #SG35 -->  Stow seperated from SGG18 - Alkalis  
            self.__update_table_7_2_4_sgg(row, DG_Loadlist, df_DG_classes_expanded, 'SG36','SGG18', 2, unique_sgg)
            #SG37
            if any(x in list(DG_Loadlist['UN'].unique()) for x in ['1043','2073']):
                    self.__update_table_7_2_4(row, df_DG_classes_expanded, 'SG37', 2.2, 2, unique_class_sublabel)
                        
            if any(x in list(DG_Loadlist['UN'].unique()) for x in ['1005', '3318']):
                    self.__update_table_7_2_4(row, df_DG_classes_expanded, 'SG37', 2.3, 2, unique_class_sublabel)
                        
            if any(x in list(DG_Loadlist['UN'].unique()) for x in ['2672']):
                    self.__update_table_7_2_4(row, df_DG_classes_expanded, 'SG37', 8.0, 2, unique_class_sublabel)   
                
            if any(x in list(DG_Loadlist['UN'].unique()) for x in ['1841']):
                    self.__update_table_7_2_4(row, df_DG_classes_expanded, 'SG37', 9.0, 2, unique_class_sublabel)
            #SG38
            self.__update_table_7_2_4_sgg(row, DG_Loadlist, df_DG_classes_expanded, 'SG38','SGG2', 2, unique_sgg)
            #SG39 
            self.__update_table_7_2_4_sgg(row, DG_Loadlist, df_DG_classes_expanded, 'SG39','SGG2', 2, unique_sgg)
            #SG40
            self.__update_table_7_2_4_sgg(row, DG_Loadlist, df_DG_classes_expanded, 'SG40','SGG2', 2, unique_sgg)
            #SG41 --> away from animal & vegetable oils
            ############
            #SG42
            self.__update_table_7_2_4_sgg(row, DG_Loadlist, df_DG_classes_expanded, 'SG42','SGG2', 3, unique_sgg)
            #SG43
            if any(x in list(DG_Loadlist['UN'].unique()) for x in ['2901']):
                    self.__update_table_7_2_4(row, df_DG_classes_expanded, 'SG43', 2.3, 2, unique_class_sublabel)
                        
            if any(x in list(DG_Loadlist['UN'].unique()) for x in ['1745', '1746']):
                    self.__update_table_7_2_4(row, df_DG_classes_expanded, 'SG43', 5.1, 2, unique_class_sublabel)
                        
            if any(x in list(DG_Loadlist['UN'].unique()) for x in ['1744']):
                    self.__update_table_7_2_4(row, df_DG_classes_expanded, 'SG43', 8.0, 2, unique_class_sublabel)   
            #SG44 
            if any(x in list(DG_Loadlist['UN'].unique()) for x in ['1444']):
                    self.__update_table_7_2_4(row, df_DG_classes_expanded, 'SG44', 5.1, 2, unique_class_sublabel) 
            #SG45
            self.__update_table_7_2_4_sgg(row, DG_Loadlist, df_DG_classes_expanded, 'SG45','SGG4', 2, unique_sgg)
            #SG46
            if any(x in list(DG_Loadlist['UN'].unique()) for x in ['2548', '1749', '1017']):
                    self.__update_table_7_2_4(row, df_DG_classes_expanded, 'SG46', 2.3, 2, unique_class_sublabel)
                        
            if any(x in list(DG_Loadlist['UN'].unique()) for x in ['2995', '2996']):
                    self.__update_table_7_2_4(row, df_DG_classes_expanded, 'SG46', 6.1, 2, unique_class_sublabel)
            #SG47
            self.__update_table_7_2_4_sgg(row, DG_Loadlist, df_DG_classes_expanded, 'SG47','SGG5', 2, unique_sgg)
            #SG48 
            #from combustible material
            #SG49
            self.__update_table_7_2_4_sgg(row, DG_Loadlist, df_DG_classes_expanded, 'SG49','SGG6', 2, unique_sgg)
            #SG50 --> Segregation from foodstuff 
            ##### --> not in scope
            #SG51
            self.__update_table_7_2_4_sgg(row, DG_Loadlist, df_DG_classes_expanded, 'SG51','SGG8', 2, unique_sgg)
            #SG52
            if any(x in list(DG_Loadlist['UN'].unique()) for x in ['1376']):
                    self.__update_table_7_2_4(row, df_DG_classes_expanded, 'SG52', 4.2, 2, unique_class_sublabel)
            #SG53 --> out of scope
            #SG54
            self.__update_table_7_2_4_sgg(row, DG_Loadlist, df_DG_classes_expanded, 'SG54','SGG11', 2, unique_sgg)
            #SG55
            self.__update_table_7_2_4_sgg(row, DG_Loadlist, df_DG_classes_expanded, 'SG55','SGG11', 2, unique_sgg)
            #SG56
            self.__update_table_7_2_4_sgg(row, DG_Loadlist, df_DG_classes_expanded, 'SG56','SGG12', 2, unique_sgg)
            #SG57  odour absorbing cargo 
            #SG58
            self.__update_table_7_2_4_sgg(row, DG_Loadlist, df_DG_classes_expanded, 'SG58','SGG13', 2, unique_sgg)
            #SG59
            self.__update_table_7_2_4_sgg(row, DG_Loadlist, df_DG_classes_expanded, 'SG59','SGG14', 2, unique_sgg)
            #SG60
            self.__update_table_7_2_4_sgg(row, DG_Loadlist, df_DG_classes_expanded, 'SG60','SGG16', 2, unique_sgg)
            #SG61
            self.__update_table_7_2_4_sgg(row, DG_Loadlist, df_DG_classes_expanded, 'SG61','SGG15', 2, unique_sgg)
            #SG62
            if any(x in list(DG_Loadlist['UN'].unique()) for x in ['1350', '2448']):
                    self.__update_table_7_2_4(row, df_DG_classes_expanded, 'SG62', 4.1, 2, unique_class_sublabel)
            #SG63 
            if 'SG63' in str(row['Stowage and segregation']):
                elements = [1.1, 1.2, 1.5, 1.3, 1.6, 1.4]
                for e in elements:
                    if str(e) in unique_class_sublabel:
                        cols_rows = [float(x) for x in str(e).split('.')]
                        if df_DG_classes_expanded.loc[row.Class, cols_rows[0]] == 'X' or df_DG_classes_expanded.loc[row.Class, cols_rows[0]] < 4:
                            df_DG_classes_expanded.at[(row.Class, cols_rows[0]), (cols_rows[1], row.Class)] = 4
                
            #SG65
            if 'SG65' in str(row['Stowage and segregation']):
                elements = [1.1, 1.2, 1.5, 1.3, 1.6, 1.4]
                for e in elements:
                    if str(e) in unique_class_sublabel:
                        cols_rows = [float(x) for x in str(e).split('.')]
                        if df_DG_classes_expanded.loc[row.Class, cols_rows[0]] == 'X' or df_DG_classes_expanded.loc[row.Class, cols_rows[0]] < 3:
                            df_DG_classes_expanded.at[(row.Class, cols_rows[0]), (cols_rows[1], row.Class)] = 3
            #SG67
            self.__update_table_7_2_4(row, df_DG_classes_expanded, 'SG67', 1.4, 2, unique_class_sublabel)
            if 'SG67' in str(row['Stowage and segregation']):
                elements = [1.1, 1.2, 1.5, 1.3, 1.6, 1.4]
                for e in elements:
                    if str(e) in unique_class_sublabel:
                        cols_rows = [float(x) for x in str(e).split('.')]
                        if df_DG_classes_expanded.loc[row.Class, cols_rows[0]] == 'X' or df_DG_classes_expanded.loc[row.Class, cols_rows[0]] < 4:
                            df_DG_classes_expanded.at[(row.Class, cols_rows[0]), (cols_rows[1], row.Class)] = 4
            #SG68
            if row['FlashPoints'] is not None and float(row['FlashPoints']) < 60.0:
                self.__compare_lists_and_replace(row, df_DG_classes_expanded, df_DG_classes_expanded_reference, 'S68', 3.0)
                self.__update_table_7_2_4(row, df_DG_classes_expanded, 'SG68', 4.1, 1, unique_class_sublabel)
            #SG69
            if str(row['UN']) == '1950': 
                if str(row['Stowage Category']) == 'A':
                    self.__compare_lists_and_replace(row, df_DG_classes_expanded, df_DG_classes_expanded_reference, 'SG69', 9.0)
                    elements = [1.1, 1.2, 1.5, 1.3, 1.6, 1.4]
                    for e in elements:
                        if str(e) in unique_class_sublabel:
                            cols_rows = [float(x) for x in str(e).split('.')]
                            if df_DG_classes_expanded.loc[row.Class, cols_rows[0]] == 'X' or df_DG_classes_expanded.loc[row.Class, cols_rows[0]] < 1:
                                df_DG_classes_expanded.at[(row.Class, cols_rows[0]), (cols_rows[1], row.Class)] = 1
                            
                if str(row['Stowage Category']) == 'B': 
                    if row['Flammable'] == 'x': 
                        self.__compare_lists_and_replace(row, df_DG_classes_expanded, df_DG_classes_expanded_reference, 'SG69', 2.1)
                    if row['Non-Flammable'] == 'x' and str(row['SubLabel1']) != '6.1':
                        self.__compare_lists_and_replace(row, df_DG_classes_expanded, df_DG_classes_expanded_reference, 'SG69', 2.2)
                    if str(row['SubLabel1']) == '6.1':
                        self.__compare_lists_and_replace(row, df_DG_classes_expanded, df_DG_classes_expanded_reference, 'SG69', 2.3)

                if str(row['Stowage Category']) == 'C': 
                    if row['Flammable'] == 'x': 
                        self.__compare_lists_and_replace(row, df_DG_classes_expanded, df_DG_classes_expanded_reference, 'SG69', 2.1)
                    if row['Non-Flammable'] == 'x' and str(row['SubLabel1']) != '6.1':
                        self.__compare_lists_and_replace(row, df_DG_classes_expanded, df_DG_classes_expanded_reference, 'SG69', 2.2)
                    if str(row['SubLabel1']) == '6.1':
                        self.__compare_lists_and_replace(row, df_DG_classes_expanded, df_DG_classes_expanded_reference, 'SG69', 2.3)

            #SG70
            self.__update_table_7_2_4_sgg(row, DG_Loadlist, df_DG_classes_expanded, 'SG70','SGG1', 2, unique_sgg)
            #SG74
            self.__compare_lists_and_replace(row, df_DG_classes_expanded, df_DG_classes_expanded_reference, 'SG74', 1.4)
            #SG75
            self.__update_table_7_2_4_sgg(row, DG_Loadlist, df_DG_classes_expanded, 'SG75','SGG1a', 2, unique_sgg)
            #SG76
            self.__compare_lists_and_replace(row, df_DG_classes_expanded, df_DG_classes_expanded_reference, 'SG76', 7.0)
            #SG77
            self.__compare_lists_and_replace(row, df_DG_classes_expanded, df_DG_classes_expanded_reference, 'SG77', 8.0)
            #SG78
            if 'SG78' in str(row['Stowage and segregation']):
                elements = [1.1, 1.2, 1.5]
                for e in elements:
                    if str(e) in unique_class_sublabel:
                        cols_rows = [float(x) for x in str(e).split('.')]
                        if df_DG_classes_expanded.loc[row.Class, cols_rows[0]] == 'X' or df_DG_classes_expanded.loc[row.Class, cols_rows[0]] < 4:
                            df_DG_classes_expanded.at[(row.Class, cols_rows[0]), (cols_rows[1], row.Class)] = 4

        return df_DG_classes_expanded

def main():
    # Set up the logger
    logger = logging.getLogger('example_logger')
    logger.setLevel(logging.DEBUG)
    # Create a file handler and set its level to DEBUG
    file_handler = logging.FileHandler('example.log')
    file_handler.setLevel(logging.DEBUG)
    # Create a formatter and add it to the file handler
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    # Add the file handler to the logger
    logger.addHandler(file_handler)
    def read_json_from_path(file_path):
        """
        Read JSON data from a file path.

        Args:
            file_path (str): Path to the JSON file.

        Returns:
            dict: JSON data loaded as a dictionary.

        Raises:
            FileNotFoundError: If the file at the specified path does not exist.
            json.JSONDecodeError: If the file does not contain valid JSON data.
        """
        try:
            with open(file_path, 'r') as file:
                json_data = json.load(file)
            return json_data
        except FileNotFoundError:
            raise FileNotFoundError(f"File not found at path: {file_path}")
        except json.JSONDecodeError:
            raise json.JSONDecodeError("Invalid JSON data in file", file_path, 0)
    
    # Create objects and data needed for DG class initialization
    vessel_profile = read_json_from_path(r"C:\Users\BRT.AFARHAT12\OneDrive - CMA CGM\Desktop\git_repos\Optistow_new_repo\vas-data\service_input_output\data\referential\vessels\9454450\Vessel Profile.Json")
    DG_rules = read_json_from_path(r"C:\Users\BRT.AFARHAT12\OneDrive - CMA CGM\Desktop\git_repos\Optistow_new_repo\vas-data\service_input_output\data\referential\vessels\9454450\DG_rules.JSON")
    vessel = Vessel(logger, 14, 20, 14.5, vessel_profile, DG_rules)
    df_all_containers = pd.read_csv(r"C:\Users\BRT.AFARHAT12\OneDrive - CMA CGM\Desktop\git_repos\Optistow_new_repo\autostow-processing\df_all_containers.csv")
    DG_loadlist_enrichment_map = read_json_from_path(r"C:\Users\BRT.AFARHAT12\OneDrive - CMA CGM\Desktop\git_repos\Optistow_new_repo\vas-data\service_input_output\data\referential\config_jsons\DG_loadlist_enrichment_map.json")
    imdg_haz_exis = pd.read_csv(r"C:\Users\BRT.AFARHAT12\OneDrive - CMA CGM\Desktop\git_repos\Optistow_new_repo\vas-data\service_input_output\data\referential\hz_imdg_exis_subs.csv")

    naming_schema = DG_loadlist_enrichment_map["DG_LOADLIST_SCHEMA"]
    # Instantiate the DG class
    dg_instance = DG(logger, vessel, df_all_containers, DG_loadlist_enrichment_map, DG_rules, imdg_haz_exis)
    df = dg_instance.get_df_DG_loadlist_exhaustive(df_all_containers,naming_schema)
    print(df.shape[0])
if __name__ == "__main__":
    main()