import pandas as pd
import numpy as np
import json
import logging
from modules.vessel import Vessel

class DG:
    def __init__(self, logger: logging.Logger, vessel: object, DG_loadlist_config: dict, imdg_haz_exis: pd.DataFrame, DG_rule: str):
        self.logger = logger
        self._vessel = vessel
        self._imdg_haz_exis = imdg_haz_exis
        self.__DG_loadlist_config = DG_loadlist_config
        self.__DG_rules = vessel.get_DG_rules()
        self.__dg_exclusions = vessel.get_DG_exclusions()
        self.__vessel_profile = vessel.get_vessel_profile()
        self.__DG_rule = DG_rule
## =======================================================================================================================        
## DG Loadlist Zone
## =======================================================================================================================
    def __populate_states_list(self, states_list_to_map: list, reference_state: str, yes_no_vals_tuple: tuple) -> list:
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
            >>> ATT_HAZ_cols_states = ["P", "", "", "P", ""]
            >>> reference_state = "P"
            >>> yes_no_values = ("yes", "no")
            >>> marine_pollutant_list = self.__populate_states_list(ATT_HAZ_cols_states, reference_state, yes_no_values)
            # Output: ['yes', 'no', 'no', 'yes', 'no']
        """
        populated_states_list = []
        
        for states in states_list_to_map:
            if not states or states != states:  # Check for empty string or NaN
                populated_states_list.append(yes_no_vals_tuple[1])
                continue

            # Check if reference_state matches any state in the 'states' list or if it matches 'states' directly
            if any(reference_state == state for state in states) or (reference_state == states):
                populated_states_list.append(yes_no_vals_tuple[0])
            else:
                populated_states_list.append(yes_no_vals_tuple[1])

        return populated_states_list 
    
    def __extract_ATT_state_columns(self, df: pd.DataFrame) -> 'tuple[list, str, str]':
        all_cols = df.columns.tolist()
    
        ATT_HAZ_cols = list(filter(lambda col: "DETAIL_DESCRIPTION_CODE_" in col and "DGS_ATT_HAZ" in col, all_cols))
        ATT_AGR_col = next(filter(lambda col: "DETAIL_DESCRIPTION_CODE_" in col and "DGS_ATT_AGR" in col, all_cols), "")
        LIM_QUA_col = next(filter(lambda col: "DETAIL_DESCRIPTION_CODE_" in col and "DGS_ATT_QTY" in col, all_cols), "")
        return ATT_HAZ_cols, ATT_AGR_col, LIM_QUA_col

    def __get_DG_ATT_states_lists(self, df_DG_loadlist: pd.DataFrame) -> 'tuple[list, list, list, list, list]':
        """
        Extracts various states lists from a DataFrame that includes specific columns
        for ATT_HAZ, ATT_AGR, and LIM_QUA.

        Parameters:
            df_DG_loadlist (pd.DataFrame): DataFrame containing columns for different ATT states.

        Returns:
            tuple[list, list, list, list, list]: Tuple containing five lists corresponding to 
            marine pollutants, flammables, liquids, solids, and limited quantities.

        Examples:
        >>> import logging 
        >>> logger = logging.Logger
        >>> df = pd.DataFrame({'ATT_HAZ_col1': ['P', 'TLQ'], 'ATT_HAZ_col2': ['FLVAP', 'TLQ'], 'ATT_AGR_col': ['L', 'S'], 'LIM_QUA_col': ['TLQ', 'TLQ']})
        >>> DG_loadlist_config_dict = {}
        >>> imdg_haz_exis = pd.DataFrame()
        >>> dg = DG(logger, None, DG_loadlist_config_dict, imdg_haz_exis)
        >>> dg._DG__get_DG_ATT_states_lists(df)
        (['yes', 'no'], ['x', ''], ['x', ''], ['x', ''], ['yes', 'no'])

        Note: The above doctest is purely illustrative. Replace it with real data and expected results.
        """

        ATT_HAZ_cols, ATT_AGR_col, LIM_QUA_col = self.__extract_ATT_state_columns(df_DG_loadlist)

        ATT_HAZ_states_lists = [df_DG_loadlist[col].tolist() if ATT_HAZ_cols else [""] * len(df_DG_loadlist.index) for col in ATT_HAZ_cols]
        ATT_AGR_states_list = df_DG_loadlist[ATT_AGR_col].tolist() if ATT_AGR_col else [""] * len(df_DG_loadlist.index)
        LIM_QUA_list = df_DG_loadlist[LIM_QUA_col].tolist() if LIM_QUA_col else [""] * len(df_DG_loadlist.index)

        if len(ATT_HAZ_states_lists) == 2:
            ATT_HAZ_cols_states = list(zip(ATT_HAZ_states_lists[0], ATT_HAZ_states_lists[1]))
        else:
            ATT_HAZ_cols_states = [(state) for state in ATT_HAZ_states_lists[0]]
            
        limited_quantity_list = self.__populate_states_list(LIM_QUA_list, 'TLQ', ("yes", "no"))
        marine_pollutant_list = self.__populate_states_list(ATT_HAZ_cols_states, "P", ("yes", "no")) 
        flammable_list = self.__populate_states_list(ATT_HAZ_cols_states, "FLVAP", ("x", ""))
        liquid_list = self.__populate_states_list(ATT_AGR_states_list, "L", ("x", ""))
        solid_list = self.__populate_states_list(ATT_AGR_states_list, "S", ("x", ""))
        
        return marine_pollutant_list, flammable_list, liquid_list, solid_list, limited_quantity_list

    def __add_DG_ATT_states_to_df(self, df: pd.DataFrame) -> pd.DataFrame:
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
        marine_pollutant_list, flammable_list, liquid_list, solid_list, limited_quantity_list = self.__get_DG_ATT_states_lists(df)
        df["Marine Pollutant"] = marine_pollutant_list
        df["Liquid"] = liquid_list
        df["Solid"] = solid_list
        df["Flammable"] = flammable_list
        df["Non-Flammable"] = ["x" if is_flammable == "" else "" for is_flammable in flammable_list]
        df["Limited Quantity"] = limited_quantity_list

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
        df_out = df.copy()
        for col in missing_cols:
            df_out[col] = ""

        return df_out

    def __get_df_DG_loadlist_exhaustive(self, df_all_containers: pd.DataFrame) -> pd.DataFrame:
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
        df_all_containers = df_all_containers.copy()
        if "DGS_ATT_AGR_DETAIL_DESCRIPTION_CODE_1" not in df_all_containers.columns:
            df_all_containers["DGS_ATT_AGR_DETAIL_DESCRIPTION_CODE_1"] = ""
            
        # filter out containers without any Dangerous Cargo
        df_DG_containers = df_all_containers[(df_all_containers['DGS_HAZARD_ID_1'] != "") & ~(df_all_containers['DGS_HAZARD_ID_1'].isna())]

        # filter and keep only necessary columns
        static_column_names = self.__DG_loadlist_config['static_df_all_containers_cols']
        dynamic__column_prefixes = self.__DG_loadlist_config['dynamic_df_all_containers_col_prefixes']
        dynamic_columns = [col for col in df_DG_containers.columns if any(col.startswith(prefix) for prefix in dynamic__column_prefixes)]
        columns_to_keep = static_column_names + dynamic_columns
        filtered_df = df_DG_containers[columns_to_keep]

        # Stack each DG separately into its own record
        df_DG_containers_long = pd.wide_to_long(
                        filtered_df,
                        stubnames= dynamic__column_prefixes, 
                        i=['EQD_ID','LOC_9_LOCATION_ID','LOC_11_LOCATION_ID'],
                        j='variable'
                    )
        
        df_DG_containers_long = df_DG_containers_long[(df_DG_containers_long['DGS_HAZARD_ID_']!="") & ~(df_DG_containers_long['DGS_HAZARD_ID_'].isna())]
        df_DG_containers_long = df_DG_containers_long.reset_index()
        
        return df_DG_containers_long
    
    def __filter_columns_df_DG_loadlist(self, df_DG_loadlist:pd.DataFrame)-> pd.DataFrame:
        
        columns_to_keep = [*self.__DG_loadlist_config['DG_LOADLIST_SCHEMA'].keys()]
        columns_to_keep.extend(['Marine Pollutant' , 'Liquid', 'Solid', 'Flammable', 'Non-Flammable', 'Limited Quantity'])
        df_DG_loadlist = df_DG_loadlist[columns_to_keep]

        return df_DG_loadlist

    def __rename_df_DG_columns(self, df_DG_loadlist:pd.DataFrame)-> pd.DataFrame:
        d_column_names = self.__DG_loadlist_config['DG_LOADLIST_SCHEMA']

        df_DG_loadlist = df_DG_loadlist.copy()
        df_DG_loadlist.rename(columns = d_column_names, inplace=True)  
        # just in case
        df_DG_loadlist.fillna("", inplace=True)
        
        return df_DG_loadlist

    def __get_closed_freight_containers(self, df_DG_loadlist:pd.DataFrame)->None:

        # Closed freight containerClick to open paragraph tools   
        # means a freight container which totally encloses its contents by permanent structures. 
        # A freight container formed partly by a tarpaulin, plastic sheet,
        # or similar material is not a closed freight container. 
        # - Flat Rack Containers
        # - Open-Top Containers
        # - Platform Containers
        # - Ventilated Containers
        # - Half-Height Containers
        # - Side-Open Containers
        # - Tank containers are not technically closed but are sealed 
        # new ISO: 3rd code of the Type ISO code is in {'P', 'T'(do not include), 'U', 'V', 'S'}
        # old ISO: ???
        # cases of nan or not iso container ??                                                                                                                                                                                                                                                                                                          
        openFreightContainers = {
            "new_iso_code": ['P', 'U', 'V', 'S'],
            "old_iso_code": ['1', '5', '6']
        }
        df_DG_loadlist['Closed Freight Container'] = df_DG_loadlist['Type'].apply(lambda x: '' if x[2] in openFreightContainers['new_iso_code'] or x[2] in openFreightContainers['old_iso_code'] else 'x')

    def __get_DGC_packaged_goods(self, df_DG_loadlist: pd.DataFrame)-> None:
        
        df_DG_loadlist['Package Goods'] = "x"

    def __map_packaging_group(self, df_DG_loadlist:pd.DataFrame)->None:
        PGr_map = self.__DG_loadlist_config["PACKAGING_GROUP"]
        df_DG_loadlist["PGr"] = df_DG_loadlist["PGr"].apply(lambda x: PGr_map.get(x, x))

    def __filter_df_DG_loadlist(self, df_DG_loadlist: pd.DataFrame) -> pd.DataFrame:
        
        
        """
        Filters the DataFrame based on specific conditions and drops rows that do not meet the conditions.

        Args:
            df (pd.DataFrame): The DataFrame to be filtered.

        Returns:
            pd.DataFrame: The filtered DataFrame.

        Raises:
            None.
        """
        df_DG_loadlist['FP'] = df_DG_loadlist['FlashPoints'].apply(lambda x: np.nan if x == '' else float(x))
        # UNNO : 1950
        
        for idx, row in df_DG_loadlist.iterrows():

            if row['UNNO'] == '1950':

                if 'MAX' in row['Proper Shipping Name (Paragraph B of DOC)'].upper() and 'yes' not in row['Limited Quantity']: 
                    row['Limited Quantity'] = 'yes'

                if 'WASTE' in row['PSN'] and 'WASTE' not in row['Proper Shipping Name (Paragraph B of DOC)'].upper():
                    df_DG_loadlist.drop(idx, inplace=True)
                
                if 'yes' in row['Limited Quantity'] and 'None|$' in row['LQ']: 
                    df_DG_loadlist.drop(idx, inplace=True)
                
                if 'yes' not in row['Limited Quantity'] and 'None|$' not in row['LQ']:
                    df_DG_loadlist.drop(idx, inplace=True)
            
            # assumption for now take lowest CATEGORY
            # if row['UNNO'] == ['3480', '3481']:
            #     if row['PSN'] not in row['Proper Shipping Name (Paragraph B of DOC)'].upper():
            #         df_DG_loadlist.drop(idx, inplace=True)
                
            if row['UNNO'] == '3528':
                if 'BELOW' in row['VARIATION'].upper() and row['FP'] >= 23:
                    df_DG_loadlist.drop(idx, inplace=True)
                
                if 'ABOVE' in row['VARIATION'].upper() and row['FP'] < 23:
                    df_DG_loadlist.drop(idx, inplace=True)


            if row['UNNO'] == '2037':
                if 'WASTE' in row['PSN'].upper() and 'WASTE' not in row['Proper Shipping Name (Paragraph B of DOC)'].upper():
                    df_DG_loadlist.drop(idx, inplace=True)
                
                if 'WASTE' not in row['PSN'].upper() and 'WASTE' in row['Proper Shipping Name (Paragraph B of DOC)'].upper():
                    df_DG_loadlist.drop(idx, inplace=True)

            if row['UNNO'] == ['3322', '3325', '3327']:
                if '4.2' in row['SubLabel1'].upper() and '4.2' not in row['SUBLABEL1'].upper():
                    df_DG_loadlist.drop(idx, inplace=True)
                
                if '4.2' not in row['SubLabel1'].upper() and '4.2' in row['SUBLABEL1'].upper():
                    df_DG_loadlist.drop(idx, inplace=True)

            if row['UNNO'] == ['2215', '2280', '1381', '2870']:
                if row['PSN'].upper() not in row['Proper Shipping Name (Paragraph B of DOC)'].upper():
                    df_DG_loadlist.drop(idx, inplace=True)
                
    
            if row['UNNO'] == ['3373']:
                if 'P650' in row['VARIATION'] and 'P650' not in row['Proper Shipping Name (Paragraph B of DOC)'].upper():
                    df_DG_loadlist.drop(idx, inplace=True)

                if 'P650' not in row['VARIATION'] and 'P650' in row['Proper Shipping Name (Paragraph B of DOC)'].upper():
                    df_DG_loadlist.drop(idx, inplace=True)
                    
            if row['UNNO'] == ['2794']:
                if 'x' in row['Closed Freight Container'] and 'B' in row['STOWCAT'].upper():
                    df_DG_loadlist.drop(idx, inplace=True)
                
                if 'x' not in row['Closed Freight Container'] and 'A' in row['STOWCAT'].upper():
                    df_DG_loadlist.drop(idx, inplace=True)

        return df_DG_loadlist

    def __merge_DG_loadlist_with_imdg_code(self, df_DG_loadlist:pd.DataFrame)->pd.DataFrame:
        
        imdg_codes_df = self._imdg_haz_exis
        df_DG_loadlist['UN'] = df_DG_loadlist['UN'].astype(int).apply(lambda x: '{:04d}'.format(x))
        imdg_codes_df['UNNO'] = imdg_codes_df['UNNO'].astype(int).apply(lambda x: '{:04d}'.format(x))
        df_DG_loadlist["Ammendmant Version"] = df_DG_loadlist.apply(lambda x:x['Ammendmant Version'][:2], axis=1)
        
        # Replace Nan values with ""
        imdg_codes_df.fillna("", inplace=True)
        # sort so merge is faster 
        df_DG_loadlist.sort_values(by=['Ammendmant Version', 'Class', 'SubLabel1', 'SubLabel2', 'UN', 'PGr'], inplace=True)
        imdg_codes_df.sort_values(by=['IMDG_AMENDMENT', 'CLASS', 'SUBLABEL1', 'SUBLABEL2', 'UNNO', 'PG'], inplace=True)

        # 1st merge
        df_DG_loadlist = df_DG_loadlist.merge(imdg_codes_df[['PSN','STATE','LQ','CLASS','SUBLABEL1','SUBLABEL2','UNNO', 'IMDG_AMENDMENT','PG', 'STOWCAT','DGIES_STOW','DGIES_SEG','VARIATION']],
                        how='left' ,left_on=['Ammendmant Version', 'Class', 'SubLabel1', 'SubLabel2','UN', 'PGr'], right_on= ['IMDG_AMENDMENT','CLASS','SUBLABEL1', 'SUBLABEL2','UNNO','PG'])

        df_DG_loadlist['Stowage Category'] = df_DG_loadlist['STOWCAT']
        
        
        df_DG_loadlist = self.__filter_df_DG_loadlist(df_DG_loadlist)
        
        
        df_DG_loadlist.drop_duplicates(subset=["Serial Number", "POL", "POD", "Class", "SubLabel1",
                                                "SubLabel2", "Proper Shipping Name (Paragraph B of DOC)", "Weight",
                                                "PGr", "UN", "Stowage Category"], keep='first', inplace=True)
        
        
        # check and handle duplicates of same stowage category for DG matching a category from imdg code
        df_matched = df_DG_loadlist.drop(df_DG_loadlist[df_DG_loadlist['Stowage Category'].isnull()].index.to_list())
        df_matched = df_matched.reset_index(drop=True)
        
        
        # get DG Goods that did not match to any category from imdg code 
        df_not_matched = df_DG_loadlist[df_DG_loadlist['Stowage Category'].isnull()]
        df_not_matched = df_not_matched[['Serial Number', 'Operator', 'POL', 'POD', 'Type', 'Closed Freight Container', 'Weight', 'Regulation Body', 'Ammendmant Version', 'UN',
        'Class', 'SubLabel1', 'SubLabel2', 'DG-Remark (SW5 = Mecanical Ventilated Space if U/D par.A DOC)', 'FlashPoints', 'Limited Quantity',
        'Marine Pollutant', 'PGr', 'Liquid', 'Solid', 'Flammable', 'Non-Flammable', 'Proper Shipping Name (Paragraph B of DOC)', 'SetPoint', 'Package Goods', 'Stowage Category']]
        
        # try matching on UN Number without class and Subclass, as to handle class 1 in particular or dg with incorrect class, sublabel or generic subsidiary risk (eg. 2 instead of 2.1)

        df_not_matched = df_not_matched.merge(imdg_codes_df[['PSN','STATE','LQ','CLASS','SUBLABEL1','SUBLABEL2','UNNO', 'IMDG_AMENDMENT','PG', 'STOWCAT','DGIES_STOW','DGIES_SEG','VARIATION']],
                        how='left' ,left_on=['Ammendmant Version','UN', 'PGr'], right_on= ['IMDG_AMENDMENT','UNNO','PG'])
        df_not_matched['Stowage Category'] = df_not_matched['STOWCAT']
        
        df_not_matched.drop_duplicates(subset=["Serial Number", "POL", "POD", "Class", "SubLabel1",
                                                "SubLabel2", "Proper Shipping Name (Paragraph B of DOC)", "Weight",
                                                "PGr", "UN", "Stowage Category"], keep='first', inplace=True)
        
        df_not_matched.reset_index(drop=True)
        
        # Concat both dataframes
        df_DG_loadlist = pd.concat([df_matched, df_not_matched],ignore_index=True) 
        df_copy1 = df_DG_loadlist.copy()
        df_copy1['Stowage Category'] = df_copy1['Stowage Category'].map(lambda x: str(x))
        df_copy1.reset_index(inplace= True, drop=True)
        #Stowage Category order from most to least critical 
        sort_order= ["1", "2", "3", "4", "5", "A", "B", "C", "D", "E"]
        # Group By Serial Number and order in decreasing criticality
        df_copy1= df_copy1.groupby(["Serial Number", "POL", "POD", "Class", "SubLabel1", "SubLabel2", "Proper Shipping Name (Paragraph B of DOC)", "Weight", "PGr", "UN"],group_keys= False).apply(lambda x: x.sort_values(by="Stowage Category", key=lambda y: [sort_order.index(v) for v in y], ascending=True))
        # df_copy1.set_index("level_0", inplace=True)
        # Keep first row which is most critical
        df_copy1.drop_duplicates(subset = ["Serial Number", "POL", "POD", "Class", "SubLabel1", "PGr", "UN"], keep='first', inplace=True)
        df_DG_loadlist = df_copy1
        df_DG_loadlist.reset_index(inplace=True)
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
    
    def __get_liquid_state(self, df_DG_loadlist:pd.DataFrame):
        df_DG_loadlist["Liquid"] = df_DG_loadlist.apply(lambda x: "x" if (x['STATE'] == "L" or "LIQUID" in x['Proper Shipping Name (Paragraph B of DOC)'].upper()
                                                                            or "SOLUTION" in x['Proper Shipping Name (Paragraph B of DOC)'].upper()) 
                                                                            else "" , axis=1)
    
    def __get_solid_state(self, df_DG_loadlist:pd.DataFrame):
        df_DG_loadlist["Solid"] = df_DG_loadlist.apply(lambda x: "x" if (x['STATE'] == "S" or "SOLID" in x['Proper Shipping Name (Paragraph B of DOC)'].upper() 
                                                                        or "DRY" in x['Proper Shipping Name (Paragraph B of DOC)'].upper() 
                                                                        or "POWDER" in x['Proper Shipping Name (Paragraph B of DOC)'].upper()) 
                                                                        else "" , axis=1)
        
    def __get_flammable_state(self, df_DG_loadlist:pd.DataFrame):
        df_DG_loadlist["Flammable"] = df_DG_loadlist.apply(lambda x: "x" if (x['Class'] in ['1.3','2.1','3','4.1','4.2','4.3'] 
                                                                    or x['SubLabel1'] in ['1.3','2.1','3','4.1','4.2','4.3'] 
                                                                    or x['SubLabel2'] in ['1.3','2.1','3','4.1','4.2','4.3'] 
                                                                    or "FLAMMABLE" in x['Proper Shipping Name (Paragraph B of DOC)'].upper() 
                                                                    or x['Flammable'] == "x" ) else "" , axis=1)    

    def __get_non_flammable_state(self, df_DG_loadlist:pd.DataFrame):
        df_DG_loadlist["Non-Flammable"] = df_DG_loadlist.apply(lambda x: "" if x['Flammable'] == "x" else "x" , axis=1)
    
    def __get_zone_port(self, df_DG_loadlist:pd.DataFrame):
        # apply zone mapping  
        Zone_map = self.__DG_loadlist_config["PORT_ZONE"]
        df_DG_loadlist["Zone"] = df_DG_loadlist["POL"].str[:5].map(Zone_map)    
    
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
        not_permitted_bay_col_name = self.__DG_rules["Not_permitted_bay_column_name"]
        Not_Permitted_Bay_list = self.__DG_rules["UNNO_not_permitted_bay"]
        df_DG_loadlist[not_permitted_bay_col_name] = df_DG_loadlist.apply(lambda x: "x" if x['UN'] in Not_Permitted_Bay_list  else "" , axis=1)
        Dynamic_Not_Permitted_Bay_dict = self.__DG_rules["Dynamic_not_permitted_bay"]
    
        df_copy = df_DG_loadlist.copy(deep=False)
        df_copy['FlashPoints'].fillna(np.nan, inplace=True)
        df_copy['FlashPoints'] = pd.to_numeric(df_copy['FlashPoints'])

        fp_low_threshold = float(self.__DG_loadlist_config ["FP_Threshold_Low"]) 
        fp_high_theshold = float(self.__DG_loadlist_config ["FP_Threshold_High"])  
        
        PE_Conditions = [
            (df_copy['FlashPoints'] == np.nan),
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
        df_DG_loadlist = df_copy.copy()
        
        return df_DG_loadlist
    
    def __get_loading_remarks(self, df_DG_loadlist:pd.DataFrame):
        segregation_group_map = self.__DG_loadlist_config ["SEGREGATION_GROUP"]
        df_DG_loadlist["Loading remarks"] = df_DG_loadlist["SegregationGroup"].apply(lambda row: "" if row == "" else ", ".join([segregation_group_map[value] for value in row.split(", ")]))

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
                                "SegregationGroup;SetPoint;Stowage and segregation;Package Goods;Stowage Category").split(";")

        DG_cols_ordered_list.append(self.__DG_rules["Not_permitted_bay_column_name"])
        DG_cols_ordered_list.append("Zone")

        df_DG_loadlist = df_DG_loadlist[DG_cols_ordered_list]
        df_DG_loadlist.reset_index(inplace=True, drop=True)
        return df_DG_loadlist

    def __handle_missing_sw1_flammable_class(self, df_DG_loadlist: pd.DataFrame): 
        def update_stowage(row):
            # Check the conditions
            if (row['Class'] in ["3"] or row['SubLabel1'] in ["3"] or row['SubLabel2'] in ["3"]) and (row['PGr'] in ['I', 'II']) and ('SW1' not in row['Stowage and segregation']) or (row['Class'] in ["4.1", "4.2", "4.3"] or row['SubLabel1'] in ["4.1", "4.2", "4.3"] or row['SubLabel2'] in ["4.1", "4.2", "4.3"]) and ('SW1' not in row['Stowage and segregation']):
                if row['Stowage and segregation'] == "":
                    return 'SW1'
                else:
                    return 'SW1' + ", " + row['Stowage and segregation']
            else:
                return row['Stowage and segregation']


        df_DG_loadlist['Stowage and segregation'] = df_DG_loadlist.apply(update_stowage, axis=1)    
        return None

    def get_df_dg_loadlist(self, df_all_containers:pd.DataFrame) -> pd.DataFrame:
        df_DG_loadlist = self.__get_df_DG_loadlist_exhaustive(df_all_containers)
        if df_DG_loadlist.shape[0] != 0:
            df_DG_loadlist = self.__add_DG_ATT_states_to_df(df_DG_loadlist)
            df_DG_loadlist = self.__filter_columns_df_DG_loadlist(df_DG_loadlist)
            df_DG_loadlist = self.__rename_df_DG_columns(df_DG_loadlist)
            self.__get_closed_freight_containers(df_DG_loadlist)
            self.__get_DGC_packaged_goods(df_DG_loadlist)
            self.__map_packaging_group(df_DG_loadlist)
            df_DG_loadlist = self.__merge_DG_loadlist_with_imdg_code(df_DG_loadlist)
            df_DG_loadlist = self.__process_stowage_and_segregation(df_DG_loadlist)
            self.__get_liquid_state(df_DG_loadlist)
            self.__get_solid_state(df_DG_loadlist)
            self.__get_flammable_state(df_DG_loadlist)
            self.__get_non_flammable_state(df_DG_loadlist)
            self.__get_zone_port(df_DG_loadlist)
            self.__get_loading_remarks(df_DG_loadlist)
            self.__handle_missing_sw1_flammable_class(df_DG_loadlist)
            df_DG_loadlist = self.__add_not_permitted_bay_column(df_DG_loadlist)
            df_DG_loadlist = self.__reorder_df_DG_loadlist_cols(df_DG_loadlist)
        else:
            DG_cols = ("Serial Number;Operator;POL;POD;Type;Closed Freight Container;Weight;Regulation Body;Ammendmant Version;UN;Class;SubLabel1;SubLabel2;" +\
                        "DG-Remark (SW5 = Mecanical Ventilated Space if U/D par.A DOC);FlashPoints;Loading remarks;" +\
                        "Limited Quantity;Marine Pollutant;PGr;Liquid;Solid;Flammable;Non-Flammable;Proper Shipping Name (Paragraph B of DOC);" +\
                        "SegregationGroup;SetPoint;Stowage and segregation;Package Goods;Stowage Category").split(";")

            # Create an empty DataFrame with the specified columns
            df_DG_loadlist = pd.DataFrame(columns=DG_cols)
            
        return df_DG_loadlist

## =======================================================================================================================        
## Update table 7_2_4
## =======================================================================================================================
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
            
    def output_adjusted_table_7_2_4(self, table_7_2_4:pd.DataFrame, df_DG_loadlist: pd.DataFrame) -> pd.DataFrame:
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
        DG_Loadlist[['Class', 'SubLabel1', 'FlashPoints']] = DG_Loadlist[['Class', 'SubLabel1', 'FlashPoints']].apply(lambda x: x.replace('', np.nan))
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

## =======================================================================================================================        
## DG Loadlist Exclusion
## =======================================================================================================================
    def __extract_columns_for_exclusions(self, df_DG_loadlist:pd.DataFrame):
        column_list = ['Serial Number', 'POL', 'POD', 'Closed Freight Container',
                       'UN', 'Class', 'SubLabel1', 'DG-Remark (SW5 = Mecanical Ventilated Space if U/D par.A DOC)',
                       'FlashPoints', 'Marine Pollutant', 'PGr', 'Liquid', 'Solid',
                       'Flammable', 'Non-Flammable', 'Proper Shipping Name (Paragraph B of DOC)',
                       'Stowage and segregation','Package Goods', 'Stowage Category']

        return df_DG_loadlist[column_list]
    
    def __rename_df_exclusion_columns(self,df_exclusion:pd.DataFrame)-> pd.DataFrame:

        exclusion_column_names = {
            'Serial Number': 'container_id',
            'POL': 'pol',
            'POD': 'pod',
            'Closed Freight Container': 's_closed_freight_container',
            'UN': 'un_no',
            'Class': 'imdg_class',
            'SubLabel1': 'sub_label',
            'DG-Remark (SW5 = Mecanical Ventilated Space if U/D par.A DOC)': 'dg_remark',
            'FlashPoints': 's_flash_point',
            'Marine Pollutant': 's_polmar',
            'PGr': 'pgr',
            'Liquid': 's_liquid',
            'Solid': 's_solid',
            'Flammable': 's_flammable',
            'Non-Flammable': 's_non_flammable',
            'Proper Shipping Name (Paragraph B of DOC)': 'shipping_name',
            'Stowage and segregation': 's_stowage_segregation',
            'Package Goods': 's_package_goods',
            'Stowage Category': 'stowage_category'
        }
        df = df_exclusion.copy()
        df.rename(columns=exclusion_column_names, inplace=True)
        
        return df

    def __get_d_containers_exclusions(self, df:pd.DataFrame)->dict:
        # Drop duplicates and convert to a list of tuples
        d_containers_exclusions = {}
        for idx, row in df.iterrows():
            if (row['container_id'], row['pol']) not in d_containers_exclusions:
                        d_containers_exclusions[(row['container_id'], row['pol'])] = set()

        return d_containers_exclusions

    def __transform_dg_exclusion_data(self, df_dg_exclusions:pd.DataFrame)-> pd.DataFrame:
        df = df_dg_exclusions.copy()
        df['as_closed'] = df['s_closed_freight_container'].apply(lambda x: x == 'x')
        df['liquid'] = df['s_liquid'].apply(lambda x: x == 'x')
        df['solid'] = df['s_solid'].apply(lambda x: x == 'x')
        df['solid'] = df.apply(lambda row: True if not row['solid'] and not row['liquid'] else row['solid'], axis=1)
        df['flammable'] = df['s_flammable'].apply(lambda x: x == 'x')
        df['polmar'] = df['s_polmar'].apply(lambda x: x == 'yes')
        df['sw_1'] = df['s_stowage_segregation'].apply(lambda x: "SW1" in str(x))
        df['sw_2'] = df['s_stowage_segregation'].apply(lambda x: "SW2" in str(x))
        df['flash_point'] = df['s_flash_point'].apply(lambda x: float(x) if pd.notna(x) and len(str(x)) > 0 else None)
        df.loc[(df['imdg_class'] == '3') & (df['s_flash_point'].apply(lambda x: len(str(x)) == 0)), 'flash_point'] = 23.0
        
        return df

    def __get_DG_category(self, row) -> str:
        if pd.isna(row['flash_point']):
            row['flash_point'] = None
        if ( row['un_no'] in self.__DG_loadlist_config['l_IMDG_Class_1_S'])\
        or (row['imdg_class'] == '6.1' and row['solid'] == True and row['as_closed'] == True)\
        or (row['imdg_class'] == '8' and row['liquid'] == True and row['flash_point'] is None)\
        or (row['imdg_class'] == '8' and row['solid'] == True)\
        or (row['imdg_class'] == '9' and row['as_closed'] == True):
            return 'PPP'
        
        if (row['imdg_class'] == '2.2')\
        or (row['imdg_class'] == '2.3' and row['flammable'] == False and row['as_closed'] == True)\
        or (row['imdg_class'] == '3' and row['flash_point'] >= 23 )\
        or (row['imdg_class'] == '4.1' and row['as_closed'] == True)\
        or (row['imdg_class'] == '4.2' and row['as_closed'] == True)\
        or (row['imdg_class'] == '4.3' and row['liquid'] == True and row['as_closed'] == True and (row['flash_point'] >= 23 or row['flash_point'] is None))\
        or (row['imdg_class'] == '4.3' and row['solid'] == True and row['as_closed'] == True)\
        or (row['imdg_class'] == '5.1' and row['as_closed'] == True)\
        or (row['imdg_class'] == '8' and row['liquid'] == True and row['as_closed'] == True and row['flash_point'] >= 23 and row['flash_point'] <= 60):
            return 'PP'
        
        if (row['imdg_class'] == '6.1' and row['liquid'] == True and row['flash_point'] is None):
            return 'PX'
        
        if (row['imdg_class'] == '6.1' and row['liquid'] == True and row['flash_point'] >= 23 and row['flash_point'] <= 60 and row['as_closed'] == True):
            return 'PX+'
        
        if (row['imdg_class'] == '2.1' and row['as_closed'] == True and row['un_no'] not in self.__DG_loadlist_config['l_explosion_protect_IIB_T4'])\
        or (row['imdg_class'] == '2.3' and row['flammable'] == True and row['as_closed'] == True and row['un_no'] not in self.__DG_loadlist_config['l_explosion_protect_IIB_T4'] and row['sub_label'] != '2.1')\
        or (row['imdg_class'] == '3' and row['flash_point'] < 23 and row['as_closed'] == True and row['un_no'] not in self.__DG_loadlist_config['l_explosion_protect_IIB_T4'])\
        or (row['imdg_class'] == '6.1' and row['liquid'] == True and row['flash_point'] < 23 and row['as_closed'] == True and row['un_no'] not in self.__DG_loadlist_config['l_explosion_protect_IIB_T4'])\
        or (row['imdg_class'] == '8' and row['liquid'] == True and row['flash_point'] < 23 and row['as_closed'] == True and row['un_no'] not in self.__DG_loadlist_config['l_explosion_protect_IIB_T4']):
            return 'XP'
        
        if (row['imdg_class'] == '6.1' and row['solid'] == True and row['as_closed'] == False)\
        or (row['imdg_class'] == '9' and row['as_closed'] == False):
            return 'XX-'
        
        if (row['imdg_class'] == '2.1' and (row['as_closed'] == False or row['un_no'] in self.__DG_loadlist_config['l_explosion_protect_IIB_T4']))\
        or (row['imdg_class'] == '2.3' and row['flammable'] == True and (row['as_closed'] == False or row['un_no'] in self.__DG_loadlist_config['l_explosion_protect_IIB_T4'] or row['sub_label'] == '2.1'))\
        or (row['imdg_class'] == '2.3' and row['flammable'] == False and row['as_closed'] == False)\
        or (row['imdg_class'] == '3' and row['flash_point'] < 23 and (row['as_closed'] == False or row['un_no'] in self.__DG_loadlist_config['l_explosion_protect_IIB_T4']))\
        or (row['imdg_class'] == '4.1' and row['as_closed'] == False)\
        or (row['imdg_class'] == '4.2' and row['as_closed'] == False)\
        or (row['imdg_class'] == '4.3' and row['liquid'] == True and (row['as_closed'] == False or row['flash_point'] < 23))\
        or (row['imdg_class'] == '4.3' and row['solid'] == True and row['as_closed'] == False)\
        or (row['imdg_class'] == '5.1' and row['as_closed'] == False)\
        or (row['imdg_class'] == '5.2')\
        or (row['imdg_class'] == '6.1' and row['liquid'] == True and row['flash_point'] < 23 and (row['as_closed'] == False or row['un_no'] in self.__DG_loadlist_config['l_explosion_protect_IIB_T4']))\
        or (row['imdg_class'] == '6.1' and row['liquid'] == True and row['flash_point'] >= 23 and row['flash_point'] <= 60 and row['as_closed'] == False)\
        or (row['imdg_class'] == '8' and row['liquid'] == True and row['flash_point'] < 23 and (row['as_closed'] == False or row['un_no'] in self.__DG_loadlist_config['l_explosion_protect_IIB_T4']))\
        or (row['imdg_class'] == '8' and row['liquid'] == True and row['flash_point'] >= 23 and row['flash_point'] <= 60 and row['as_closed'] == False):
            return 'XX'
            
        if (row['imdg_class'][0] == '1' and row['un_no'] not in self.__DG_loadlist_config['l_IMDG_Class_1_S']):
            return 'XX+'
            
        if (row['imdg_class'] == '6.2')\
        or (row['imdg_class'] == '7'):
            return 'XXX'
        
        return '?'    
    
    def __get_dg_exclusions_ref_dict(self)-> dict:
        dg_exclusions_df = self.__dg_exclusions
        dg_exclusions_df.set_index('DG category', inplace=True)

        # Transforming dg_exclusions_df to dictionary 
        ### Reading the Exclusions zones depending on the (macro-)category
        ##### Getting for each (macro-)category the list of exclusion zones
        dg_exclusions_by_category = {}
        # Use boolean indexing to filter columns where value is "X" and get the column names
        indices = dg_exclusions_df.columns[dg_exclusions_df.eq('X').any()].tolist()
        # Create a dictionary where the keys are the row indices and the values are the indices list
        dg_exclusions_by_category = dict(zip(dg_exclusions_df.index, indices))
        # iterate over the rows
        for i, row in dg_exclusions_df.iterrows():
            # create empty list to hold column indices where value is "X"
            indices = []
            # iterate over the columns
            for j, col in row.items():
                # if value is "X", append column index to indices list
                if col == 'X':
                    indices.append(j)
            # populate index and values in dictionary 
                dg_exclusions_by_category[i] = indices 
        return dg_exclusions_by_category
    
    def __get_deck_hold_bays(self):
        
        l_macro_bays = self.__vessel_profile['fourty_foot_bays']
        l_deck_bays = l_macro_bays + self.__vessel_profile['twenty_foot_bays']
        l_deck_bays.sort()
        
        l_hold_bays = self.__vessel_profile['fourty_foot_hold_bays']
        l_hold_bays.extend(self.__vessel_profile['twenty_foot_hold_bays'])
        l_hold_bays.sort()
        
        l_hold_zones = ["%03d0" % n for n in l_hold_bays]
        
        return l_deck_bays, l_hold_bays, l_hold_zones
    
    def deserialize_config_sw1(self,serialized_l_sw_1):
        deserialized_list = []
        for item in serialized_l_sw_1:
            a, b, c = item
            b = frozenset(b) if b!="None" else None
            c1, c2 = c
            deserialized_list.append((a, b, (c1, frozenset(c2))))
        return deserialized_list
    
    def __get_sw1_exclusion_list(self)-> list:
        
        serialized_l_sw_1 = self.__DG_rules['l_sw1']
        l_sw1 = self.deserialize_config_sw1(serialized_l_sw_1)
        
        return l_sw1
    
    def __get_sw2_exclusion_list(self)-> list:
        
        l_sw2 = self.__DG_rules['l_sw2']
        
        return l_sw2   
    
    def __get_polmar_exclusion_zones(self, l_deck_bays:list)-> tuple:
        
        l_polmar_bays_all_positions = self.__DG_rules['l_polmar_bays_exclusion_all_positions']
        row_tiers_per_bay = self.__vessel_profile['Rows_Tiers_per_Bay']
        
        row_end_per_bay = {item['bay']: item['row_end'] for item in row_tiers_per_bay}
            
        l_decks_polmar_extension = ["%03d" % n for n in l_deck_bays if n not in l_polmar_bays_all_positions]
        l_rows_polmar_extension = [frozenset({str(row_end_per_bay[n] - 1), str(row_end_per_bay[n])}) for n in l_deck_bays if n not in l_polmar_bays_all_positions]
    
        l_polmar_master = ["{:03d}1".format(x) for x in l_polmar_bays_all_positions]
        l_polmar = []
        l_polmar.extend([(x[0], x[1], ('1', None)) for x in zip(l_decks_polmar_extension, l_rows_polmar_extension)])
        return l_polmar, l_polmar_master
    
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

    def __apply_exclusion_expansions(self, d_containers_exclusions:dict, df_dg_exclusions:pd.DataFrame):
        dg_exclusions_by_category = self.__get_dg_exclusions_ref_dict()
        l_deck_bays, l_hold_bays, l_hold_zones = self.__get_deck_hold_bays()
        l_sw_1 = self.__get_sw1_exclusion_list()
        l_polmar, l_polmar_master = self.__get_polmar_exclusion_zones(l_deck_bays)
        l_sw_2 = self.__get_sw2_exclusion_list()
        for idx, row in df_dg_exclusions.iterrows():
            d_containers_exclusions[(row['container_id'], row['pol'])] = self.__expand_exclusion(d_containers_exclusions[(row['container_id'], row['pol'])], 
                                                                                dg_exclusions_by_category[row['DG_category']])
            if row['stowage_category'] in ['C', 'D']:
                d_containers_exclusions[(row['container_id'], row['pol'])] = self.__expand_exclusion(d_containers_exclusions[(row['container_id'], row['pol'])],
                                                                                    l_hold_zones)
            if row['sw_2'] == True:
                d_containers_exclusions[(row['container_id'], row['pol'])] = self.__expand_exclusion(d_containers_exclusions[(row['container_id'], row['pol'])],
                                                                                    l_sw_2)
            if self.__DG_rule == "master":
                if row['polmar'] == True:
                    # change to l_polmar_master in case you want to only include exhaustive bays
                    d_containers_exclusions[(row['container_id'], row['pol'])] = self.__expand_exclusion(d_containers_exclusions[(row['container_id'], row['pol'])],
                                                                                            l_polmar_master)

                    d_containers_exclusions[(row['container_id'], row['pol'])] = self.__expand_exclusion(d_containers_exclusions[(row['container_id'], row['pol'])],
                                                                                            l_polmar)
                    
            if self.__DG_rule == "slot":
                if row['sw_1'] == True:
                    d_containers_exclusions[(row['container_id'], row['pol'])] = self.__expand_exclusion(d_containers_exclusions[(row['container_id'], row['pol'])],
                                                                                        l_sw_1)
                if row['polmar'] == True:
                
                    d_containers_exclusions[(row['container_id'], row['pol'])] = self.__expand_exclusion(d_containers_exclusions[(row['container_id'], row['pol'])],
                                                                                        l_polmar)
        return d_containers_exclusions
    
    def __construct_final_dg_exclusion(self, d_containers_exclusions:dict)->pd.DataFrame:
        f_loadlist_exclusions_list = []
        if self.__DG_rule == "master":
            header_list = ["ContId", "LoadPort", "Bay", "MacroTier"]

            # ordinary rows
            for ((container_id, pol), s_exclusions) in d_containers_exclusions.items():
                for (bay, l_rows, (macro_tier, l_tiers)) in s_exclusions:
                    row = []
                    row.extend([container_id, pol, bay[1:3], macro_tier])
                    f_loadlist_exclusions_list.append(row)
                    
        if self.__DG_rule == "slot":
            # header
            header_list = ["ContId", "LoadPort", "Bay", "Row", "MacroTier", "Tier"]

            # ordinary rows
            for ((container_id, pol), s_exclusions) in d_containers_exclusions.items():
                for (bay, l_rows, (macro_tier, l_tiers)) in s_exclusions:

                    if l_rows is None:
                        if l_tiers is None:
                            f_loadlist_exclusions_list.append([container_id, pol, bay[1:3], "", macro_tier,""])
                                
                        else:
                            for tier in l_tiers:
                                f_loadlist_exclusions_list.append([container_id, pol, bay[1:3], "", macro_tier, tier])
                
                    else:
                        for row in l_rows:
                            if l_tiers is None:
                                
                                f_loadlist_exclusions_list.append([container_id, pol, bay[1:3], row, macro_tier,""])
                        
                            else:
                                for tier in l_tiers:
                                    
                                    f_loadlist_exclusions_list.append([container_id, pol, bay[1:3], row, macro_tier, tier])
                        
                                        
                    

        f_loadlist_exclusions = pd.DataFrame(f_loadlist_exclusions_list, columns= header_list)
        return f_loadlist_exclusions
    
    def get_dg_exclusions(self, df_DG_loadlist:pd.DataFrame)->pd.DataFrame:
        if df_DG_loadlist.shape[0] != 0:
            df_dg_exclusions = self.__extract_columns_for_exclusions(df_DG_loadlist)
            df_dg_exclusions = self.__rename_df_exclusion_columns(df_dg_exclusions)
            d_containers_exclusions = self.__get_d_containers_exclusions(df_dg_exclusions)
            df_dg_exclusions = self.__transform_dg_exclusion_data(df_dg_exclusions)
            df_dg_exclusions['DG_category'] = df_dg_exclusions.apply(self.__get_DG_category, axis=1)
            d_containers_exclusions= self.__apply_exclusion_expansions(d_containers_exclusions, df_dg_exclusions)
            df_exclusions = self.__construct_final_dg_exclusion(d_containers_exclusions)
        else: 
            DG_exclusion_cols = ['ContId', 'LoadPort', 'Bay', 'MacroTier']
            df_exclusions = pd.DataFrame(columns=DG_exclusion_cols)
        return df_exclusions

## =======================================================================================================================        
## DG Loadlist Exclusion by Zone & nb DG
## =======================================================================================================================    
    def __get_stacks_capacities(self) -> dict:
        fn_stacks = self._vessel.get_fn_stacks()
        # Create a new column that merges Bay, Row, and Tier as a tuple
        fn_stacks['stack'] = list(zip(fn_stacks['Bay'], fn_stacks['Row'], fn_stacks['Tier']))
        
        # Create a dictionary with the necessary information
        d_stacks = fn_stacks.set_index('stack').apply(lambda row: {
            'subbay': row['SubBay'],
            'first_tier': row['FirstTier'],
            'max_nb_std_cont': int(row['MaxNbOfStdCont']),
            'odd_slot': int(row['OddSlot']),
            'nb_reefer': int(row['NbReefer']),
            'max_nb_45': int(row['MaxNb45']),
            'min_40_sub_45': int(row['Min40sub45']),
            'max_nb_HC_at_max_stack': int(row['MaxNbHCAtMaxStack']),
            'stack_height': float(row['StackHeight']),
            'max_weight': float(row['MaxWeight'])
        }, axis=1).to_dict()
        
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

    def __get_container_2_container_groups(self, df_containers_group:pd.DataFrame)-> dict:
            # Create a new column for the container group (cg)
        df_containers_group['cg'] = list(zip(
            df_containers_group['LoadPort'], 
            df_containers_group['DischPort'], 
            df_containers_group['Size'], 
            df_containers_group['cType'], 
            df_containers_group['cWeight'], 
            df_containers_group['Height']
        ))
    
        # Create a new column for the container key (container, load_port_name)
        df_containers_group['container_key'] = list(zip(
            df_containers_group['Container'], 
            df_containers_group['LoadPort']
        ))

        # Create the dictionary
        d_container_2_container_group = df_containers_group.set_index('container_key')['cg'].to_dict()

        return d_container_2_container_group

    def __create_exclusion_zone_mapping(self, df_loadlist_exclusions: pd.DataFrame) -> dict:
        # Create a key column that represents the tuple (container, load_port_name)
        df_loadlist_exclusions['key'] = list(zip(
            df_loadlist_exclusions['ContId'],
            df_loadlist_exclusions['LoadPort']
        ))

        # Define a function that will convert each group to a set of (bay, macro_tier) tuples
        def to_set(group):
            return set(zip(group['Bay'], group['MacroTier']))

        # Group by the key and apply the to_set function to each group, then convert to a dictionary
        d_container_2_exclusion_zone = df_loadlist_exclusions.groupby('key').apply(to_set).to_dict()

        return d_container_2_exclusion_zone   
    
    def __get_l_zones(self,d_container_2_exclusion_zone:dict)-> list:
        s_zones = set()
        for (container, load_port_name), zone in d_container_2_exclusion_zone.items():
            s_zones.add(frozenset(zone))
        l_zones = list(s_zones)
        return l_zones
    
    def __get_d_container_2_ix_exclusion_zone(self, d_container_2_exclusion_zone:dict, l_zones:list)->dict:
        d_container_2_ix_exclusion_zone = {}
        for (container, load_port_name), container_zone in d_container_2_exclusion_zone.items():
            ix_zone = -1
            for ix, zone in enumerate(l_zones):
                if zone == container_zone:
                    ix_zone = ix
                    break
            d_container_2_ix_exclusion_zone[(container, load_port_name)] = ix_zone
            
        return d_container_2_ix_exclusion_zone
    
    def __get_d_cg_2_ix_exclusion_zones(self, d_container_2_ix_exclusion_zone:dict,d_container_2_container_group:dict)->dict:
        # now, list exclusion zones for each container group, and count corresponding containers 
        d_cg_2_ix_exclusion_zones = {}

        for (container, load_port_name), ix_zone in d_container_2_ix_exclusion_zone.items():
        # contrôle de cohérence
            cg = d_container_2_container_group[(container, load_port_name)]
            if cg not in d_cg_2_ix_exclusion_zones:
                d_cg_2_ix_exclusion_zones[cg] = {}
            if ix_zone not in d_cg_2_ix_exclusion_zones[cg]:
                d_cg_2_ix_exclusion_zones[cg][ix_zone] = 0
            d_cg_2_ix_exclusion_zones[cg][ix_zone] += 1 
            
        return d_cg_2_ix_exclusion_zones
    
    def __get_d_cg_2_combi_zones(self, d_cg_2_ix_exclusion_zones:dict, l_zones:list)->dict:
        # creation of the list of exclusion zones (including combinations) for each container group
        d_cg_2_combi_zones = {}
        for cg, d_ix_zones in d_cg_2_ix_exclusion_zones.items():
            d_combi_zones = self.__list_areas_for_zone_intersections(d_ix_zones, l_zones)
            d_cg_2_combi_zones[cg] = d_combi_zones
        
        return d_cg_2_combi_zones
    
    def __get_d_cg_combi_subbays(self, d_cg_2_combi_zones:dict, d_bay_macro_tier_l_subbays:dict)-> dict: 
        # at last, split bay x macro_tier area into subbays, while keeping the nb of containers data
        d_cg_combi_subbays = {}

        for cg, d_combi_zones in d_cg_2_combi_zones.items():
            d_combi_subbays = {}
            for s_combi_area, nb_containers in d_combi_zones.items():
                s_combi_subbays = self.__get_zone_list_subbays(s_combi_area, d_bay_macro_tier_l_subbays)
                d_combi_subbays[frozenset(s_combi_subbays)] = nb_containers
            d_cg_combi_subbays[cg] = d_combi_subbays   
            
        return d_cg_combi_subbays
    
    def __get_f_cg_exclusion_zones(self, d_cg_combi_subbays:dict)->pd.DataFrame:
        l_cg_exclusion_zones = []
        s_header_zones_list = ["LoadPort", "DischPort", "Size", "cType", "cWeight", "Height", "idZone", "Subbay"]

        for (load_port_name, disch_port_name, size, c_type, c_weight, height), d_combi_subbays in d_cg_combi_subbays.items():
                
            for ix, (s_combi_subbays, nb_containers) in enumerate(d_combi_subbays.items()):
                l_combi_subbays = list(s_combi_subbays)
                l_combi_subbays.sort()
                # writing zones
                for subbay in l_combi_subbays:
                    row = []
                    row.extend([load_port_name, disch_port_name, size, c_type, c_weight, height, ix, subbay])
                    l_cg_exclusion_zones.append(row)
        f_cg_exclusion_zones = pd.DataFrame(l_cg_exclusion_zones, columns=s_header_zones_list)
        return f_cg_exclusion_zones
    
    def __get_f_cg_exclusion_zones_nb_dg(self, d_cg_combi_subbays:dict)->pd.DataFrame: 
        l_cg_exclusion_zones_nb_dg = []
        s_header_nb_dg_list = ["LoadPort", "DischPort", "Size", "cType", "cWeight", "Height", "idZone", "NbDG"]
        for (load_port_name, disch_port_name, size, c_type, c_weight, height), d_combi_subbays in d_cg_combi_subbays.items():
            
            for ix, (s_combi_subbays, nb_containers) in enumerate(d_combi_subbays.items()):
                l_combi_subbays = list(s_combi_subbays)
                l_combi_subbays.sort()
                # writing nb of containers
                row_nb = []
                row_nb.extend([load_port_name, disch_port_name, size, c_type, c_weight, height, ix, nb_containers])
                l_cg_exclusion_zones_nb_dg.append(row_nb)
                
        f_cg_exclusion_zones_nb_dg = pd.DataFrame(l_cg_exclusion_zones_nb_dg, columns=s_header_nb_dg_list)
        
        return f_cg_exclusion_zones_nb_dg
    
    def get_exclusion_zones(self, df_grouped_containers:pd.DataFrame, df_loadlist_exclusions:pd.DataFrame):
        if df_loadlist_exclusions.shape[0] != 0:
            d_container_2_container_group = self.__get_container_2_container_groups(df_grouped_containers)
            d_stacks = self.__get_stacks_capacities()
            d_bay_macro_tier_l_subbays = self.__get_bays_macro_tiers_l_subbays(d_stacks)
            d_container_2_exclusion_zone = self.__create_exclusion_zone_mapping(df_loadlist_exclusions)
            l_zones = self.__get_l_zones(d_container_2_exclusion_zone)
            d_container_2_ix_exclusion_zone = self.__get_d_container_2_ix_exclusion_zone(d_container_2_exclusion_zone, l_zones)
            d_cg_2_ix_exclusion_zones = self.__get_d_cg_2_ix_exclusion_zones(d_container_2_ix_exclusion_zone, d_container_2_container_group)
            d_cg_2_combi_zones = self.__get_d_cg_2_combi_zones(d_cg_2_ix_exclusion_zones, l_zones)
            d_cg_combi_subbays = self.__get_d_cg_combi_subbays(d_cg_2_combi_zones,d_bay_macro_tier_l_subbays)
            df_cg_exclusion_zones = self.__get_f_cg_exclusion_zones(d_cg_combi_subbays)
            df_cg_exclusion_zones_nb_dg = self.__get_f_cg_exclusion_zones_nb_dg(d_cg_combi_subbays)
        else: 
            df_cg_exclusion_zones = pd.DataFrame(columns=['LoadPort','DischPort','Size','cType','cWeight','Height','idZone','Subbay'])
            df_cg_exclusion_zones_nb_dg = pd.DataFrame(columns=['LoadPort','DischPort','Size','cType','cWeight','Height','idZone','NbDG'])
        return df_cg_exclusion_zones, df_cg_exclusion_zones_nb_dg




