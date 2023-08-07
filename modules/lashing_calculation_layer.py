import math
import pandas as pd
import numpy as np
import logging

class LashingGetter():
    def __init__(self, speed: float, gm: float, draft: float, lashing_config:str, lashing_parameters:dict=None):

        # Initialization of variables
        self.speed = speed
        self.gm = gm
        self.__T = draft
        self.lashing_config = lashing_config
        # self.__T = 14.00                           # Moulded draught NR467 PtB, Ch1, Sec2, 3.7.1
        self.__D = 29.9                            # Depth NR467 PtB, Ch1, Sec2, 3.5.1
        self.__Bw = 53.6                           # Moulded breadth
        self.__Lpp = 378.4                         # Length between perpendiculars (bpp), [m]
        self.__hw = 200 / math.sqrt(self.__Lpp)     # Wave parameter hw=10.281 m (Pt B, Ch 5, Sec 3)
        self.__C = 10.75 - ((self.__Lpp - 350) / 150) ** 1.5
        self.__W = 53.6                            # Width, [m]
        DS = 25.1                                   # Design speed, [Kn]
        Rlt = 150                                   # Permissible racking load (transverse force), [kN]
        Rll = 100                                   # Permissible racking load (longitudinal force), [kN]
        Pt = 250                                    # Permissible tension for non-bottom layer, [kN]
        Pcp = 848                                   # Permissible corner post, [kN]
        self.__Tftd = 160                           # Thickness of fitting between tiers (on-deck bays), [mm]
        Tfth = 0                                    # Thickness of fitting between tiers (in-hold bays), [mm]
        Tbfd = 0                                    # Thickness of bottom fitting (on-deck bays), [mm]
        Tbfh = 0                                    # Thickness of bottom fitting (in-hold bays), [mm]
        self.__Gv = 12                              # Twistlock bottom clearance, [mm]
        Gh = 10                                     # Twistlock clearance between tiers, [mm]
        self.__twistlockhorizgap = -0               # Twistlock horizontal gap used in deformations calculus [mm]
        self.__Kridoor = 3.764                      # Resilience of 8'6 container door side, [kN/mm]
        self.__Krifront = 15.058                    # Resilience of 8'6 container front side, [kN/mm]
        Bld = 25                                    # Basic lashing diameter, [mm]
        Ea = 160000                                 # Young Modulus in MPA or N/mm2
        Al = math.pow(Bld / 10, 2) * math.pi / 4     # Cross section in cm2
        n = 1                                       # Navigation Coefficient n=1.000  (Pt B, Ch 5, Sec 1, 2.6.1) value taken for unrestricted navigation
        self.__n = n


    def getValues(self):
        # Calculation of various values based on inputs
        self.__F = 0.164 * self.speed / math.sqrt(self.__Lpp)  # Froude's number F=0.212 (Pt B, Ch 5, Symbols)
        ab = self.__n * (0.76 * self.__F + 1.875 * self.__hw / self.__Lpp)  # Motion and acceleration parameter ab=0.212 (Pt B, Ch 5, Sec 3)
        self.__ab = ab
        asu = 0.5  # Surge acceleration asu=0.500 m/s2 (Pt B, Ch 5, Sec 3, 2.1.1)
        asw = 0.775 * ab * 9.81  # Sway acceleration asw=1.610 m/s2 (Pt B, Ch 5, Sec 3, 2.2.1)
        Tsw = 0.8 * math.sqrt(self.__Lpp) / (1.22 * self.__F + 1)  # Sway period Tsw=12.369 s (Pt B, Ch 5, Sec 3, 2.2.1)
        ah = ab * 9.81  # Heave acceleration ah=2.077 m/s2 (Pt B, Ch 5, Sec 3, 2.3.1)
        delta = 0.35 * self.__W  # Roll radius of gyration Delta=18.760 m (Pt B, Ch 5, Sec 3, 2.4.1)
        Eorig = 1.39 * self.gm * self.__W / (delta * delta)   # Roll parameter E used for calculation E=6.536 (Pt B, Ch 5, Sec 3, 2.4.1)
        E = max (1, Eorig)  
        ar = min(0.35, ab * math.sqrt(E))  # Roll angle amplitude ar=0.350000 rad (Pt B, Ch 5, Sec 3, 2.4.1)
        ardeg = 180 * ar / math.pi  # Roll angle amplitude ar=20.054 deg (Pt B, Ch 5, Sec 3, 2.4.1)
        Tr = min(((2.2 * delta) / (math.sqrt(self.gm))), 27)  # Roll period Tr=7.428 s (Pt B, Ch 5, Sec 3, 2.4.1)
        alphaR = ar * math.pow((2 * math.pi / Tr), 2)  # Roll acceleration alphaR=0.250446 rad/s2 (Pt B, Ch 5, Sec 3, 2.4.1)
        cb = 0.65  # cb = (190968 + 53834) / (Lpp * W * 16)
        self.__cb = cb
        ap = 0.328 * ab * (1.32 - self.__hw / self.__Lpp) * math.pow(0.6 / cb, 0.75)  # Pitch angle amplitude ap=0.058726 rad (Pt B, Ch 5, Sec 3, 2.5.1)
        apdeg = 180 * ap / math.pi  # Pitch angle amplitude ap=3.365 deg (Pt B, Ch 5, Sec 3, 2.5.1)
        Tp = 0.575 * math.sqrt(self.__Lpp)  # Pitch Period Tp=11.185 s (Pt B, Ch 5, Sec 3, 2.5.1)
        alphap = ap * math.pow(2 * math.pi / Tp, 2)  # Pitch acceleration alphaP=0.018531 rad/s2 (Pt B, Ch 5, Sec 3, 2.5.1)
        ay = 1.581 * ab * 9.81 / self.__Lpp  # Yaw acceleration ay=0.006027 rad/s2 (Pt B, Ch 5, Sec 3, 2.6.1)
        alphaY = ay
        BV_rules = '2014'  # BV 2014 specific rules
        wind_side = 'out_in'
        #Lashing Unrestricted
        if self.lashing_config == 'UNRESTRICTED':
            nw1 = nw2 = 1.000
        # Lashing WORLDWIDE
        elif self.lashing_config == 'WORLDWIDE':
            nw1 = 0.95 - self.__Lpp * cb / 3000  
            nw2 = 0.8 + math.pow((27 - Tr) / 50, 2)
            
        
        h1M = 0.42 * nw1 * self.__C * (self.__cb + 0.7)  # BV calculus of h1M,h1AE,h1FE
        h1AE = 0.7 * ((4.35 / math.sqrt(self.__cb) - 3.25)) * h1M
        h1FE = ((4.35 / math.sqrt(self.__cb) - 3.25)) * h1M
        
        return {'wind_side':wind_side,
                'BV_rules':BV_rules,
                'Gv': self.__Gv,
                'hw':self.__hw,
                'cb': self.__cb,
                'n' : self.__n,
                'F' : self.__F,
                'ab' : self.__ab,
                'Tftd': self.__Tftd,
                'Lpp': self.__Lpp,
                'T': self.__T,
                'D': self.__D,
                'Bw': self.__Bw,
                'nw1': nw1,
                'nw2': nw2,
                'h1M':h1M,
                'h1AE':h1AE,
                'h1FE':h1FE,
                'C':self.__C,
                'asu': asu,
                'Tsw' : Tsw,
                'ap': ap,
                'ar': ar,
                'ah': ah,
                'Eorig' :Eorig,
                'E' : E,
                'alphap': alphap,
                'alphaR': alphaR,
                'alphaY': alphaY,
                'asw': asw,
                'delta' : delta,
                'Tr' : Tr,
                'Tp' : Tp,
                'Krifront': self.__Krifront,
                'Kridoor': self.__Kridoor,
                'twistlockhorizgap': self.__twistlockhorizgap}


class Lashing():
    def __init__(self, logger: logging.Logger, vessel: object, lashing_calculations:str) -> None:  
        self._logger = logger
        self._vessel = vessel
        self._vessel_imo = vessel.get_imo()
        self._speed = vessel.get_speed()
        self._gm = vessel.get_gm()
        self._draft = vessel.get_draft()
        self._vessel_profile = vessel.get_vessel_profile()
        self._lashing_calculations = lashing_calculations
        

    def __extract_unique_combinations(self, array: np.array):
        """
        Extracts unique combinations of elements from the given array.

        Args:
            array (np.array): The input array.

        Returns:
            set: A set containing the unique combinations.

        Example:
            unique_array = np.array([[1, 2, 3], [4, 5, 6], [1, 2, 3]])
            unique_combinations = self.__extract_unique_combinations(unique_array)
            print(unique_combinations)
            # Output: {(1, 2, 3), (4, 5, 6)}
        """
        unique_combinations = set((row[0], row[1], row[2]) for row in array)
        return unique_combinations
        
    def __extract_rows_tiers_for_unique_bays(self, unique_bays: np.array, vessel: object):
        """
        Extracts rows and tiers combinations for the unique bay values.

        Args:
            unique_bays (np.array): An array of unique bay values.
            vessel (object): The vessel object to retrieve rows and tiers from.

        Returns:
            list: A list of combinations containing (bay, row, tier).

        Example:
            unique_bays = np.array([1, 2, 3])
            vessel = Vessel()  # Create a vessel object
            combinations = self.__extract_rows_tiers_for_unique_bays(unique_bays, vessel)
            print(combinations)
            # Output: [(1, row1, tier1), (1, row1, tier2), (1, row2, tier1), ...]
        """
        combinations = []

        for bay in unique_bays:  # Iterate over unique bay values
            rows, tiers = vessel.getRowsTiers(bay)  # Get rows and tiers for the current bay

            for row in rows:  # Iterate over possible row values
                for tier in tiers:  # Iterate over possible tier values
                    combination = (bay, row, tier)
                    combinations.append(combination)

        return combinations
        
    def __filter_array_by_calculated_combinations(self, array, calculated_combinations):
        """
        Filters the array by the calculated combinations.

        Args:
            array (np.array): The input array.
            calculated_combinations (list): A list of calculated combinations.

        Returns:
            np.array: The filtered array containing rows with combinations in the calculated combinations list.

        Example:
            input_array = np.array([[1, 2, 3, 'data1'], [4, 5, 6, 'data2'], [1, 2, 3, 'data3']])
            calculated_combinations = [(1, 2, 3), (4, 5, 6)]
            filtered_array = self.__filter_array_by_calculated_combinations(input_array, calculated_combinations)
            print(filtered_array)
            # Output: [[1, 2, 3, 'data1'], [4, 5, 6, 'data2']]
        """
        calculated_combinations_set = set(calculated_combinations)

        filtered_array = np.array([row for row in array if tuple(row[:3]) in calculated_combinations_set])
        return filtered_array
        
    def __convert_iso_codes_to_dimensions(self, codes):
        """
        Converts ISO codes to corresponding dimensions.

        Args:
            codes (list): A list of ISO codes.

        Returns:
            list: A list of dimensions corresponding to the ISO codes.

        Raises:
            ValueError: If an invalid length code or height code for the old ISO format is encountered.

        Example:
            iso_codes = ["2100"]
            dimensions = self.__convert_iso_codes_to_dimensions(iso_codes)
            print(dimensions)
            # Output: [[2.991, 2.591]]
        """
        length_codes = {
                "1": 2.991,
                "2": 6.058,
                "3": 9.125,
                "4": 12.192,
                "L": 13.716,
                "9": 13.716,
                "M": 14.63
            }
        height_codes = {
                "0": 2.591,
                "2": 2.591,
                "C": 2.591,
                "L": 2.591,
                "3": 2.591,
                "4": 2.743,
                "D": 2.743,
                "M": 2.743,
                "5": 2.895,
                "E": 2.895,
                "N": 2.895, 
                "8": 1.295,
                "9":1.219
            }
        
        dimensions = []

        for code in codes:
            length_code = code[0]
            height_code = code[1]

            if length_code in length_codes and height_code in height_codes:
                length = length_codes[length_code]
                height = height_codes[height_code]
                dimensions.append([length, height])
            else:
                self._logger.error(f"could not match {code} container type to height or length....")
                raise ValueError("Invalid length code or height code for old ISO format")
            
        return dimensions

    def __extract_max_min_diff(self, split_array:np.array)->np.array:
        """
        Extracts the maximum difference between z values for each unique x value.

        Args:
            split_array (np.array): The input array containing x, y, and z values.

        Returns:
            np.array: The array of maximum differences between z values for each unique x value.

        Example:
            input_array = np.array([[1, 2, 3], [1, 3, 5], [2, 2, 4], [2, 4, 8], [3, 1, 2]])
            max_diff_array = self.__extract_max_min_diff(input_array)
            print(max_diff_array)
            # Output: [2, 4, 6]
        """
        # Find the maximum and minimum y for each unique x
        x = split_array[:, 0]
        y = split_array[:, 1]
        unique_x = np.unique(x)
        # Find the maximum and minimum y for each unique x
        max_y = []
        min_y = []
        for val in unique_x:
            if np.any(x == val):
                max_y.append(np.max(y[x == val]))
                min_y.append(np.min(y[x == val]))
            else:
                max_y.append(np.nan)
                min_y.append(np.nan)


        min_max_array = np.column_stack((unique_x, max_y,  min_y))

            
        max_diff_array = []
        for row in split_array:
            x, y, z = row  # Unpack x, y, and z from the row
            y_min = min_max_array[min_max_array[:, 0] == x][:, [2]]
            y_max = min_max_array[min_max_array[:, 0] == x][:, [1]]
            left_diff = 0
            right_diff = 0
            if y > y_min:
                left_diff = max(z - max(split_array[(split_array[:, 0] == x) & (split_array[:, 1] < y), 2], default=z), 0)
            
            else:
                left_diff = max(z, 0)
                    
            if y < y_max:
                right_diff = max(z - max(split_array[(split_array[:, 0] == x) & (split_array[:, 1] > y), 2], default=z), 0)

            else:
                right_diff = max(z, 0)

            max_diff = max(left_diff, right_diff)
            max_diff_array.append(max_diff)
        return max_diff_array
        
    def __calculate_coverage(self, row):
        """
        Calculates the coverage values based on the height list and maximum height difference.

        Args:
            row (dict): A dictionary containing the 'Height List' and 'Max_Height_Diff' values.

        Returns:
            list: The coverage values for each height in the height list.

        Example:
            input_row = {'Height List': [10, 8, 6, 4], 'Max_Height_Diff': 20}
            coverage_list = self.__calculate_coverage(input_row)
            print(coverage_list)
            # Output: [0.5, 1.0, 1.0, 1.0]
        """
        height_list = row['Height List'][::-1]
        height = row['Max_Height_Diff']
        remaining_height = round(height, 3)
        coverage_list = []

        for h in height_list:
            if remaining_height >= h:
                remaining_height -= h
                coverage = 1.0
            else:
                coverage = remaining_height / h
                remaining_height = 0

            coverage_list.append(coverage)

        return coverage_list[::-1]
        
    def _get_delta_z(self, length: np.array) -> np.array:
        """
        Calculates the delta z values based on the length values.

        Args:
            length (np.array): An array containing the length values.

        Returns:
            np.array: An array containing the bay and delta z values for each length.

        Example:
            lengths = np.array([6.058, 12.192, 6.058])
            delta_z_array = self._get_delta_z(lengths)
            print(delta_z_array)
            # Output: [[20, 0.558], [40, 0.762], [20, 0.558]]
        """
        bay = np.where(length == 6.058, 20, 40)
        deltaz = np.where(length == 6.058, (13716 - 12600) / (2 * 1000), (13716 - 12192) / (2 * 1000))
        return np.column_stack((bay, deltaz))

    def __apply_word_transform(self, word, num_chars, pattern, transformation):
        """
        Applies a word transformation based on a pattern match.

        Args:
            word (str): The input word to apply the transformation to.
            num_chars (int): The number of characters to match from the start of the word.
            pattern (str): The pattern to match against the start of the word.
            transformation (str): The transformation to apply if the pattern matches.

        Returns:
            str: The transformed word if the pattern matches, otherwise the original word.

        Example:
            input_word = 'USLA5'
            transformed_word = self.__apply_word_transform(input_word, 4, 'USLA', 'USLAX')
            print(transformed_word)
            # Output: 'USLAX'
        """
        if len(word) >= num_chars and word[:num_chars] == pattern :
            return transformation
        else:
            return word
    
    def __get_absolute_max(self, dataframe:pd.DataFrame, columns:list) -> np.array:
        """
        Calculates the absolute maximum values for each row in the specified columns of a DataFrame.

        Args:
            dataframe (pd.DataFrame): The input DataFrame.
            columns (list): The list of column names to calculate the absolute maximum values from.

        Returns:
            np.array: An array containing the absolute maximum values for each row.

        Example:
            input_df = pd.DataFrame({'A': [1, -2, 3], 'B': [-4, 5, -6], 'C': [7, 8, -9]})
            max_values = self.__get_absolute_max(input_df, ['A', 'C'])
            print(max_values)
            # Output: [7, 9]
        """
        subset = dataframe[columns]  # Select the desired columns from the DataFrame
        def calculate_max(row):
            absolute_values = [abs(val) if not isinstance(val, list) else max(abs(x) for x in val) for val in row]

            return max(absolute_values)

        absolute_max = subset.apply(calculate_max, axis=1)  # Calculate the absolute maximum row-wise using custom function

        return absolute_max

    def __lashing_calculations(self, call_0_dataframe:pd.DataFrame, POD_0:str) -> pd.DataFrame:    
        
        
        # vessel.display_info()
        # open containers file call_00_00 
        # path = r'C:\Users\BRT.AFARHAT12\OneDrive - CMA CGM\Desktop\git_repos\OptiStow_lambda_func_local\vas-data\service_input_output\data\simulations\testscript_03-04-2023-Baptisite-02\intermediate\00_THLCH_container.csv'
        self._logger.info(f"Extracting Onboard Containers Dataframe...")
        
        containers = call_0_dataframe
        
        columns_to_add = ['LOC_147_ID','EQD_ID', 'EQD_SIZE_AND_TYPE_DESCRIPTION_CODE', 'EQD_FULL_OR_EMPTY_INDICATOR_CODE','EQD_MEA_AET_MEASURE', 'EQD_MEA_AET_MEASUREMENT_UNIT_CODE', 'EQD_MEA_VGM_MEASURED_ATTRIBUTE_CODE', 'EQD_MEA_VGM_MEASURE', 'EQD_MEA_VGM_MEASUREMENT_UNIT_CODE','LOC_9_LOCATION_ID', 'LOC_11_LOCATION_ID']
        for column in columns_to_add:
            try:
                containers[column]
            except KeyError:
                containers[column] = ''

        containers = containers[columns_to_add]
        
        containers = containers.iloc[containers['LOC_147_ID'].notnull().values ,:]
        self._logger.info(f"There is {containers.shape[0]} containers are Onboard with a slot position...")
        #POD
        containers['LOC_9_LOCATION_ID'] = containers['LOC_9_LOCATION_ID'].apply(lambda word: self.__apply_word_transform(word, 4, 'USLA', 'USLAX'))
        pol_id = containers['LOC_9_LOCATION_ID'].values
        #POL 
        containers['LOC_11_LOCATION_ID'] = containers['LOC_11_LOCATION_ID'].apply(lambda word: self.__apply_word_transform(word, 4, 'USLA', 'USLAX'))
        pod_id = containers['LOC_11_LOCATION_ID'].values 
        
        #transform weight to metric tonnes if unit is KGM
        
        containers['EQD_MEA_AET_MEASURE'] = pd.to_numeric(containers['EQD_MEA_AET_MEASURE'], errors='coerce')
        containers['EQD_MEA_VGM_MEASURE'] = pd.to_numeric(containers['EQD_MEA_VGM_MEASURE'], errors='coerce')
        containers.loc[containers['EQD_MEA_AET_MEASUREMENT_UNIT_CODE'] == 'KGM', 'EQD_MEA_AET_MEASURE'] /= 1000
        containers.loc[containers['EQD_MEA_VGM_MEASUREMENT_UNIT_CODE'] == 'KGM', 'EQD_MEA_VGM_MEASURE'] /= 1000
        
        # Extract columns for weight measurement 
        aet_measure = np.nan_to_num(containers['EQD_MEA_AET_MEASURE'].values.astype(float), nan=0)
        vgm_measure = np.nan_to_num(containers['EQD_MEA_VGM_MEASURE'].values.astype(float), nan=0)
        total_weight = (aet_measure + vgm_measure)
        
        containers_slots =  containers['LOC_147_ID'].values.astype(int)
        split_array = np.array([(num // 10000, (num // 100) % 100, num % 100) for num in containers_slots]) 
        
        containers_types = containers['EQD_SIZE_AND_TYPE_DESCRIPTION_CODE'].values.astype(str)
        length_height_containers = np.array(self.__convert_iso_codes_to_dimensions(containers_types))
        tcg_results = self._vessel.getTCG_vectorized(split_array)
        lcg_results = self._vessel.getLCG_vectorized(split_array)
        delta_z_results = self._get_delta_z(length_height_containers[:,0])
        result_array = np.column_stack((split_array, pol_id, pod_id, tcg_results, lcg_results, length_height_containers, total_weight, delta_z_results))
        
        unique_bays = set(row[0] for row in result_array)
        bay_row_tiers_comb = self.__extract_rows_tiers_for_unique_bays(unique_bays, self._vessel)
        containers_bay = self.__filter_array_by_calculated_combinations(result_array, bay_row_tiers_comb)
        self._logger.info(f"There is {containers_bay.shape[0]} containers that are on deck...")

        lashing_df = pd.DataFrame(containers_bay, columns=['BAY', 'ROW', 'TIER', 'POL', 'POD', 'TCG', 'LCG', 'LENGTH', 'HEIGHT', 'WEIGHT', 'bayType', 'Delta_Z'])
        # Find the minimum tier for each bay and row combination
        lashing_df['Minimum Tier'] = lashing_df.groupby(['BAY', 'ROW'])['TIER'].transform('min')
        # # Remove rows where the 'POD' value matches the given value
        # lashing_df = lashing_df[lashing_df['POD'] != POD_0]
        # Get the rows to remove based on condition
        rows_to_remove = lashing_df[lashing_df['POD'] == POD_0][['BAY', 'ROW', 'TIER']].values
        self._logger.info(f"There is {rows_to_remove.shape[0]} Containers to be Discharged at {POD_0} (disregarded from calculation with containers on top as well)...")
        # Create a mask to identify the rows with higher tiers
        if rows_to_remove.shape[0] > 0:
            mask = ~lashing_df.apply(lambda row: ((rows_to_remove[:, 0] == row['BAY']) & (rows_to_remove[:, 1] == row['ROW']) & (row['TIER'] >= rows_to_remove[:, 2])).any(), axis=1)
            # Filter the DataFrame based on the condition
            lashing_df = lashing_df[mask]
            self._logger.info(f"Removing {containers_bay.shape[0] - lashing_df.shape[0]} Containers...")
        # =======================================================================================================================================================
        #Split 40ft containers into 2 20ft sections with odd bays before computing wind coverage 
        # Create a new dataframe to store the removed rows
        removed_rows = []

        # Create a new dataframe to store the updated rows
        updated_rows = []

        # Iterate over the rows of the original dataframe
        for index, row in lashing_df.iterrows():
            if (row['BAY'] % 2 == 0 and row['BAY'] not in self._vessel.get_exclusive_fourty_bays()) :
                new_row = row.copy()  # Create a copy of the current row
                new_row['BAY'] = row['BAY'] - 1  # Set the 'bay' value to 'bay-1'
                new_row['LENGTH'] = row['LENGTH'] / 2  # Divide the 'length' by 2
                updated_rows.append(new_row)  # Add the new row to the updated dataframe

                new_row = row.copy()  # Create another copy of the current row
                new_row['BAY'] = row['BAY'] + 1  # Set the 'bay' value to 'bay+1'
                new_row['LENGTH'] = row['LENGTH'] / 2  # Divide the 'length' by 2
                updated_rows.append(new_row)  # Add the new row to the updated dataframe

                removed_rows.append(row)  # Add the current row to the removed rows
            else:
                updated_rows.append(row)  # Add the current row as it is to the updated dataframe

        # Create a new dataframe from the updated rows
        updated_df = pd.DataFrame(updated_rows)
        # Reset the index of the updated dataframe
        updated_df.reset_index(drop=True, inplace=True)
        # Create a new dataframe from the removed rows
        removed_df = pd.DataFrame(removed_rows)
        # Reset the index of the removed dataframe
        removed_df.reset_index(drop=True, inplace=True)
        
        # updated_df['ROW'] = updated_df['ROW'].apply(lambda x: -x if x % 2 == 0 else x)
        updated_df = updated_df.sort_values(['BAY', 'ROW', 'TIER'], ascending=True)
        # Group by 'BAY', 'ROW', 'TCG', and 'LCG' and aggregate the rest of the columns as a list
        grouped_df = updated_df.groupby(['BAY', 'ROW', 'TCG', 'LCG']).agg({
            'TIER': [('Tiers in Stack', list), ('Maximum Tier', 'max'),('Minimum Tier Stack', 'min'), ('Nb in Stack', lambda x: ((max(x) - min(x))  / 2) + 1)],
            'HEIGHT': [('Height List', list), ('Stack Height', 'sum')],
            'WEIGHT': [('Weight List', list), ('Total Weight', 'sum')],
            'LENGTH': [('Length List', list)],
            'bayType': [('Bay Type', max)],
            'Delta_Z': [('Delta_Z', max)],
            'Minimum Tier': [('Minimum Tier', min)]
        })

        grouped_df.columns = grouped_df.columns.droplevel(0)
        
        # #keep only rows that have a minimum tier in list equal to the minimum tier
        # grouped_df = grouped_df[grouped_df['Minimum Tier Stack'] == grouped_df['Minimum Tier']]

        # Set 'Max_Tier' column as part of the index
        grouped_df = grouped_df.reset_index()
        grouped_df['Bay Type'] = grouped_df['Bay Type'].replace({20: 'bay20', 40: 'bay40'})

        # Extract specific columns into a new array
        split_array = grouped_df[['BAY', 'TCG', 'Stack Height']].values
        grouped_df['Max_Height_Diff'] = self.__extract_max_min_diff(split_array)
        # stack calculation
        split_array = grouped_df[['BAY', 'TCG', 'Nb in Stack']].values
        grouped_df['Max_Stack_Diff'] = self.__extract_max_min_diff(split_array)
        
        grouped_df['Coverage'] = grouped_df.apply(self.__calculate_coverage, axis=1)
        grouped_df['Coverage'] = grouped_df['Coverage'].apply(lambda x: [round(num, 3) for num in x])
        # Perform element-wise multiplication on 'Coverage' and 'Length List' lists
        grouped_df['Coverage_Area'] = grouped_df.apply(lambda row: [a * b * c for a, b, c in zip(row['Coverage'], row['Length List'], row['Height List'])], axis=1)
        #get start of Stack Height
        grouped_df['Start_Stack_Height'] = grouped_df.apply(self._vessel.getStackHeight, axis=1)

        # Apply the coverage calculation on the DataFrame column
        
        grouped_df = grouped_df.groupby(['TCG', 'LCG', 'ROW', 'Maximum Tier', 'Nb in Stack', 'Stack Height', 'Total Weight', 'Bay Type', 'Delta_Z', 'Start_Stack_Height' ]).agg({
        'BAY': [('BAY', 'mean')],
        'Max_Stack_Diff': [('Max_Stack_Diff',list)],
        'Max_Height_Diff': [('Max_Height_Diff' ,list)],
        'Tiers in Stack': [('Tiers in Stack','max')],
        'Height List': [('Height List' ,'max')],
        'Weight List': [('Weight List' ,'max')],
        'Coverage': [('Coverage', lambda x: x.tolist() if isinstance(x.iloc[0], list) else [x.tolist()])],
        'Coverage_Area' : [('Coverage_Area', lambda x: [sum(col) for col in zip(*x.tolist())] if isinstance(x.iloc[0], list) else [[x.tolist()]])],
        'Length List': [('Length List', lambda x: [sum(col) for col in zip(*x.tolist())] if isinstance(x.iloc[0], list) else [[x.tolist()]])]
        
    })
        grouped_df.columns = grouped_df.columns.droplevel(0)
        grouped_df = grouped_df.reset_index()
        self._logger.info(f" Calculating lashing for {grouped_df.shape[0]} Container Stacks...")
        column_order = ['BAY'] + [col for col in grouped_df.columns if col != 'BAY']
        grouped_df = grouped_df[column_order]
        
        lashing = LashingGetter(self._speed, self._gm, self._draft, self._lashing_calculations)
        lashing_dict = lashing.getValues()
        # self._logger.info(f" Lashing Calculation Parameters: {lashing_dict}")
        # get exterior rows per bay
        exterior_rows_per_bay = self._vessel.get_max_rows_per_bay()
        
        #start of lashing calculations 
        VCG_list, lash_bridge_aft_list, lash_bridge_fore_list, longitudinal_wind_forces_list, traversal_wind_forces_list, roll_acc_horiz_list, roll_acc_vert_list, pitch_acc_horiz_list, pitch_acc_vert_list,\
        traversal_forces_list, longitudinal_forces_list, racking_forces_aft_list, racking_forces_fore_list, racking_pitch_forces_list, deformation_aft_list, deformation_fore_list, emod_aft_list, emod_fore_list,\
            h_displacement_aft_list, h_displacement_fore_list, lashing_length_aft_list, lashing_lenght_fore_list, internal_lashing_forces_aft_list, internal_lashing_forces_fore_list, twist_shear_aft_list,\
                twist_shear_fore_list, pressure_aft_list, pressure_fore_list, lifting_aft_list, lifting_fore_list = [[] for _ in range(30)]
        
        for index, row in grouped_df.iterrows():
            tiers = row['Tiers in Stack']
            heights = row['Height List']
            masses = row['Weight List']
            STACKH=[]
            FFVCGslotold = row['Start_Stack_Height']
            deltaz = row['Delta_Z']
            #container width 
            width = 2.438
            
            VCGtierold = oldheight = mi_vcg_old = Fsiold = Fwxiold = Fxwindiold = Fwzi1old = Fwzi2old = 0
            
            FWXI, FXWINDI, FYWINDI, FZR1, FZR2, FZL1, FZL2, FY2I, FY2IM1  = [[] for i in range(9)]
            
            # Lashing diameter(mm)
            d = 25
            # Cross-section of the lashing device, in cm2
            Al = math.pi * math.pow(d / 10, 2) / 4
            
            #Input parameter definitions
            rg = row['Nb in Stack']
            # Create the Masses cumulative list 
            Mitot_list = [sum(masses[:i+1]) for i in range(len(masses))]
            # Create the Stack_height list 
            STACKH = [sum(heights[:i+1]) for i in range(len(heights))]
            
            FFVCGslot_list, FFVCGslotold_list, VCGtier_list = [[] for i in range(3)]

            for i, height in enumerate(heights):
                FFVCGslot = FFVCGslotold + 0.45 * height + i * lashing_dict['Gv'] / 1000
                # Append the calculated values to the respective lists
                FFVCGslot_list.append(FFVCGslot)
                FFVCGslotold_list.append(FFVCGslotold)
                FFVCGslotold = FFVCGslot + 0.55 * height

                # Perform other calculations and operations as needed
                VCGtier = (masses[i] * FFVCGslot + Mitot_list[i-1] * VCGtierold) / Mitot_list[i]
                VCGtier_list.append(VCGtier)
                VCGtierold = VCGtier
                
            m3vcg_list = [0.45 * height + row['Start_Stack_Height'] if idx == 0 else 0.40 * heighttot + row['Start_Stack_Height'] for idx, (height, heighttot) in enumerate(zip(heights, STACKH))]   
            VCGtier_list = [min(x, y) for x, y in zip(VCGtier_list, m3vcg_list)]
            mi_vcg_list = [Mi * FFVCGslot for Mi, FFVCGslot in zip(masses, FFVCGslot_list)]
            mivcgtot_list = []
            
            for mi_vcg in mi_vcg_list:
                mivcgtot = mi_vcg + mi_vcg_old
                mivcgtot_list.append(mivcgtot)
                mi_vcg_old = mivcgtot
        # LASHING PATTERNS
        # LASHING FOR 20s and 40s
        # internal rows, lashing from hatchcover, except extreme rows    
            if rg > 1:
                
                dlb_1, llength1_1, llength2_1, Klhh1_1, Klhh2_1, Klvv1_1, Klvv2_1, Klvh1_1, Klvh2_1, deltaZ1_1, deltaZ2_1, deltaY1_1, deltaY2_1 = [[0] * (2 * int(rg)) for _ in range(13)]   

                l1_1 = math.sqrt(math.pow(STACKH[0], 2) + math.pow(width, 2) + math.pow(deltaz, 2))*1.015/1.015
                e1_1 = 180000 if l1_1 > 3.94 else 140000
                e2_1 = l2_1 = Kl2_1 = 0
                
                llength1_1[2] = l1_1    
                Kl1_1 = (Al * e1_1) / (10000 * l1_1)        
                Klhh1_1[2] = math.pow(width / l1_1, 2) * Kl1_1            
                Klvv1_1[2] = math.pow((STACKH[0] + lashing_dict['Tftd'] / 1000) / l1_1, 2) * Kl1_1              
                Klvh1_1[2] = (width * (STACKH[0] + lashing_dict['Tftd'] / 1000)) / math.pow(l1_1, 2) * Kl1_1     
                deltaZ1_1[2] = heights[0] + lashing_dict['Tftd'] / 1000 
                deltaY1_1[2] = width

            else:
                e1_1 = e2_1 = l1_1 = l2_1 = Kl1_1 = Kl2_1 = 0
                dlb_1, llength1_1, llength2_1, Klhh1_1, Klhh2_1, Klvv1_1, Klvv2_1, Klvh1_1, Klvh2_1, deltaZ1_1, deltaZ2_1, deltaY1_1, deltaY2_1 = [[0] * (2 * int(rg)) for _ in range(13)] 
                
            if rg > 2:
                
                dlb_1, llength2_1, Klhh2_1, Klvv2_1, Klvh2_1, deltaZ2_1, deltaY2_1 = [[0] * (2 * int(rg)) for _ in range(7)] 
                llength2_1[4] = l2_1

                e2_1 = 180000
                l2_1 = math.sqrt(math.pow(STACKH[1], 2) + math.pow(width, 2) + math.pow(deltaz, 2))*1.012/1.012
                llength2_1[4] = l2_1
                Kl2_1 = (Al * e2_1) / (10000 * l2_1)
                Klhh2_1[4] = math.pow(width / l2_1, 2) * Kl2_1
                Klvv2_1[4] = math.pow((STACKH[1] + lashing_dict['Tftd'] / 1000) / l2_1, 2) * Kl2_1
                Klvh2_1[4] = (width * (STACKH[1] + lashing_dict['Tftd'] / 1000)) / math.pow(l2_1, 2) * Kl2_1
                deltaZ2_1[4] = STACKH[1] + lashing_dict['Tftd'] / 1000
                deltaY2_1[4]=  width
                
            # LASHING FOR 20s
            # external rows, lashing from hatchcover, wind applied from seaside towards hatchcover center

            if rg > 1:
                
                dlb_0, llength1_0, Klhh1_0, Klvv1_0, Klvh1_0, deltaZ1_0, deltaY1_0 = [[0] * (2 * int(rg)) for _ in range(7)] 

                l1_0 = math.sqrt(math.pow(STACKH[0], 2) + math.pow(width, 2) + math.pow(deltaz, 2))*1.015/1.015
                e1_0 = 180000 if l1_0 > 3.94 else 140000
        
                e2_0 = 0
                llength1_0[2] = l1_0
                Kl1_0 = (Al * e1_0) / (10000 * l1_0)
                Klhh1_0[2] = math.pow(width / l1_0, 2) * Kl1_0
                Klvv1_0[2] = math.pow((STACKH[0] + lashing_dict['Tftd'] / 1000) / l1_0, 2) * Kl1_0
                Klvh1_0[2] = (width * (STACKH[0] + lashing_dict['Tftd'] / 1000)) / math.pow(l1_0, 2) * Kl1_0
                deltaZ1_0[2] = STACKH[0] + lashing_dict['Tftd'] / 1000
                deltaY1_0[2] = width

            #vertical bar
                l2_0 = STACKH[1]
                e2_0 = 180000 if l2_0 > 3.94 else 140000
                e3_0 = 0
                llength2_0, llength3_0, Klhh2_0, Klhh3_0, Klvv2_0, Klvv3_0, Klvh2_0, Klvh3_0, deltaZ2_0, deltaZ3_0, deltaY2_0, deltaY3_0 = [[0] * (2 * int(rg)) for _ in range(12)]
                llength2_0[3] = l2_0
                
                Kl2_0 = (Al * e2_0) / (10000 * l2_0)
                Klhh2_0[3] = math.pow(0 / l2_0, 2) * Kl2_0
                Klvv2_0[3] = math.pow((STACKH[0] + lashing_dict['Tftd'] / 1000) / l2_0, 2) * Kl2_0
                Klvh2_0[3] = (0 * (STACKH[0] + lashing_dict['Tftd'] / 1000)) / math.pow(l2_0, 2) * Kl2_0
                deltaZ2_0[3] = STACKH[1] + lashing_dict['Tftd'] / 1000

            if rg > 2:
                
                l3_0 = math.sqrt(math.pow(STACKH[1], 2) + math.pow(width, 2) + math.pow(deltaz, 2))*1.015/1.015
                e3_0 = 180000 if l3_0 > 3.94 else 140000
                llength3_0, Klhh3_0, Klvv3_0, Klvh3_0, deltaZ3_0, deltaY3_0 = [[0] * (2 * int(rg)) for _ in range(6)]
                llength3_0[4] = l3_0
                Kl3_0 = (Al * e3_0) / (10000 * l3_0)
                Klhh3_0[4] = math.pow(width / l3_0, 2) * Kl3_0
                Klvv3_0[4] = math.pow((STACKH[1] + lashing_dict['Tftd'] / 1000) / l3_0, 2) * Kl3_0
                Klvh3_0[4] = (width * (STACKH[1] + lashing_dict['Tftd'] / 1000)) / math.pow(l3_0, 2) * Kl3_0
                deltaZ3_0[4] = STACKH[1] + lashing_dict['Tftd'] / 1000
                deltaY3_0[4] = width

            if rg <= 1:
                e1_0 = e2_0 = e3_0 = l1_0 = l2_2 = Kl1_0 = Kl2_0 = Kl3_0 = 0
                dlb_0, llength1_0, llength2_0, llength3_0, Klhh1_0, Klhh2_0, Klhh3_0, Klvv1_0, Klvv2_0, Klvv3_0, Klvh1_0, Klvh2_0, Klvh3_0, deltaZ1_0, deltaZ2_0, deltaZ3_0, deltaY1_0, deltaY2_0, deltaY3_0 = [[0] * (2 * int(rg)) for _ in range(19)]
                
            # LASHING FOR 20s
            # external rows, lashing from hatchcover, wind applied from hatchcover center towards seaside
            if rg > 1:
                
                dlb_2, llength1_2, Klhh1_2, Klvv1_2, Klvh1_2, deltaZ1_2, deltaY1_2, llength2_2, Klhh2_2, Klvv2_2, Klvh2_2, deltaZ2_2, deltaY2_2 = [[0] * (2 * int(rg)) for _ in range(13)]
                l1_2 = math.sqrt(math.pow(STACKH[0], 2) + math.pow(width, 2) + math.pow(deltaz, 2))*1.015/1.015
                e1_2 = 180000 if l1_2 > 3.94 else 140000
                
                e2_2 = e2_4 = l2_2 = Kl2_2 = 0
                llength1_2[2] = l1_2
                Kl1_2 = (Al * e1_2) / (10000 * l1_2)
                Klhh1_2[2] =  math.pow(width / l1_2, 2) * Kl1_2
                Klvv1_2[2] =  math.pow((STACKH[0] + lashing_dict['Tftd'] / 1000) / l1_2, 2) * Kl1_2
                Klvh1_2[2] = (width * (STACKH[0] + lashing_dict['Tftd'] / 1000)) / math.pow(l1_2, 2) * Kl1_2
                deltaZ1_2[2] = STACKH[0] + lashing_dict['Tftd'] / 1000
                deltaY1_2[2] = width
                
            else:
                e1_2 = e2_2 = e2_4 = l1_2 = l2_2 = Kl1_2 = Kl2_2 = 0
                dlb_2, llength1_2, llength2_2, Klhh1_2, Klhh2_2, Klvv1_2, Klvv2_2, Klvh1_2, Klvh2_2, deltaZ1_2, deltaZ2_2, deltaY1_2, deltaY2_2 = [[0] * (2 * int(rg)) for _ in range(13)]

            # LASHING FOR 20s
            # internal rows, lashing from lashing bridge(from 2nd tier)
            if rg > 2:

                dlb_3 = [10] * (2 * int(rg))
                l1_3 = math.sqrt(math.pow(row['Start_Stack_Height'] +STACKH[1]-35.2, 2) + math.pow(width, 2) + math.pow(deltaz, 2))*1.02/1.02
                e1_3 = 180000 if l1_3 > 3.94 else 140000
                e2_3 = l2_3 = Kl2_3 = 0

                #LAshing Bridge Stifness BV2018 NR625 Ch14 Sec1 5.5
                #alphaLB=0.5
                #e = alphaLB * Al * e / (10000 * l)
                llength1_3, Klhh1_3, Klvv1_3, Klvh1_3, deltaZ1_3, deltaY1_3, llength2_3, Klhh2_3, Klvv2_3, Klvh2_3, deltaZ2_3, deltaY2_3 = [[0] * (2 * int(rg)) for _ in range(12)]
                llength1_3[4] = l1_3
                Kl1_3 = (Al * e1_3) / (10000 * l1_3)
                Klhh1_3[4] = math.pow(width / l1_3, 2) * Kl1_3
                Klvv1_3[4] = math.pow((row['Start_Stack_Height']+STACKH[1]-35.2 + lashing_dict['Tftd'] / 1000) / l1_3, 2) * Kl1_3
                Klvh1_3[4] = (width * (row['Start_Stack_Height']+STACKH[1]-35.2 + lashing_dict['Tftd'] / 1000)) / math.pow(l1_3, 2) * Kl1_3
                deltaZ1_3[4] = row['Stack Height']+STACKH[1]-35.2 + lashing_dict['Tftd'] / 1000
                deltaY1_3[4] = width

            else:
                e1_3 = l1_3 = l2_3 = Kl1_3 = Kl2_3 = 0
                dlb_3 = [10] * (2 * int(rg))
                llength1_3, Klhh1_3, Klvv1_3, Klvh1_3, deltaZ1_3, deltaY1_3, llength2_3, Klhh2_3, Klvv2_3, Klvh2_3, deltaZ2_3, deltaY2_3 = [[0] * (2 * int(rg)) for _ in range(12)]

            # LASHING FOR 20s
            # external rows, lashing from lashing bridge(from 2nd tier) (pattern 6)

            if rg > 3:

                dlb_8 = dlb_3
                l2_8 = math.sqrt(math.pow(row['Start_Stack_Height']+STACKH[2]-35.2, 2) + math.pow(width, 2) + math.pow(deltaz, 2))*1.028/1.028
                e2_8 = 180000 if l2_8 > 3.94 else 140000
                
                llength2_8, Klhh2_8, Klvv2_8, Klvh2_8, deltaZ2_8, deltaY2_8 = [[0] * (2 * int(rg)) for _ in range(6)]
                llength2_8[6] = l2_8
                Kl2_8 = (Al * e2_8) / (10000 * l2_8)
                Klhh2_8[6] = math.pow(width / l2_8, 2) * Kl2_8
                Klvv2_8[6] = math.pow((row['Start_Stack_Height']+STACKH[2]-35.2 + lashing_dict['Tftd'] / 1000) / l2_8, 2) * Kl2_8
                Klvh2_8[6] = (width * (row['Start_Stack_Height']+STACKH[2]-35.2 + lashing_dict['Tftd']  / 1000)) / math.pow(l2_8, 2) * Kl2_8
                deltaZ2_8[6] = row['Start_Stack_Height']+STACKH[2]-35.2 + lashing_dict['Tftd']  / 1000
                deltaY2_8[6] = width

            else:
                e2_8 = l2_8 = Kl2_8 = 0
                dlb_8 = [10] * (2 * int(rg))
                
                llength2_8, Klhh2_8, Klvv2_8, Klvh2_8, deltaZ2_8, deltaY2_8 = [[0] * (2 * int(rg)) for _ in range(6)]
            # LASHING FOR 20s
            # internal rows, lashing from lashing bridge(from 3rd tier)
            if rg > 3:
                dlb_4 = [25] * (2 * int(rg))
                l1_4 = math.sqrt(math.pow(row['Start_Stack_Height']+STACKH[2]-37.8, 2) + math.pow(width, 2) + math.pow(deltaz, 2))*1.028/1.028
                e1_4 = 180000 if l1_4 > 3.94 else 140000
                e2_4 = l2_4 = Kl2_4 = 0
                # LAshing Bridge Stiffness BV2018 NR625 Ch14 Sec1 5.5
                # alphaLB=0.3
                # e = alphaLB * Al * e / (10000 * l)
                llength1_4, Klhh1_4, Klvv1_4, Klvh1_4, deltaZ1_4, deltaY1_4, llength2_4, Klhh2_4, Klvv2_4, Klvh2_4, deltaZ2_4, deltaY2_4 = [[0] * (2 * int(rg)) for _ in range(12)]
                llength1_4[6] = l1_4
                Kl1_4 = (Al * e1_4) / (10000 * l1_4)
                Klhh1_4[6] = math.pow(width / l1_4, 2) * Kl1_4
                Klvv1_4[6] = math.pow((row['Start_Stack_Height']+STACKH[2]-37.8 + lashing_dict['Tftd'] / 1000) / l1_4, 2) * Kl1_4
                Klvh1_4[6] = (width * (row['Start_Stack_Height']+STACKH[2]-37.8 + lashing_dict['Tftd'] / 1000)) / math.pow(l1_4, 2) * Kl1_4
                deltaZ1_4[6] = row['Start_Stack_Height']+STACKH[2]-37.8 + lashing_dict['Tftd'] / 1000
                deltaY1_4[6] = width

            else:
                e1_4 = l1_4 = l2_4 = Kl1_4 = Kl2_4 = 0
                dlb_4 = [25] * (2 * int(rg))
                llength1_4, llength2_4, Klhh1_4, Klhh2_4, Klvv1_4, Klvv2_4, Klvh1_4, Klvh2_4, deltaZ1_4, deltaZ2_4, deltaY1_4, deltaY2_4 = [[0] * (2 * int(rg)) for _ in range(12)]
            # LASHING FOR 20s
            # external rows, lashing from lashing bridge(from 3rd tier)

            if rg > 4:

                dlb_9 = dlb_4
                l2_9 = math.sqrt(math.pow(row['Start_Stack_Height']+STACKH[3]-37.8, 2) + math.pow(width, 2) + math.pow(deltaz, 2))
                e2_9 = 180000 if l2_9 > 3.94 else 140000
                llength2_9, Klhh2_9, Klvv2_9, Klvh2_9, deltaZ2_9, deltaY2_9 = [[0] * (2 * int(rg)) for _ in range(6)]
                llength2_9[8] = l2_9
                Kl2_9 = (Al * e2_9) / (10000 * l2_9)
                Klhh2_9[8] = math.pow(width / l2_9, 2) * Kl2_9
                Klvv2_9[8] = math.pow((row['Start_Stack_Height']+STACKH[3]-37.8 + lashing_dict['Tftd'] / 1000) / l2_9, 2) * Kl2_9
                Klvh2_9[8] = (width * (row['Start_Stack_Height']+STACKH[3]-37.8 + lashing_dict['Tftd'] / 1000)) / math.pow(l2_9, 2) * Kl2_9
                deltaZ2_9[8] = row['Start_Stack_Height']+STACKH[3]-37.8 + lashing_dict['Tftd'] / 1000
                deltaY2_9[8] = width

            else:
                e2_9 = l2_9 = Kl2_9 = 0
                dlb_9, llength2_9, Klhh2_9, Klvv2_9, Klvh2_9, deltaZ2_9, deltaY2_9 = [[0] * (2 * int(rg)) for _ in range(7)]

            # LASHING FOR 40s
            # internal rows, lashing from lashing bridge(from 2nd tier) (pattern 3)
            if rg > 1:

                dlb_5 = [10] * (2 * int(rg))
                l1_5 = math.sqrt(math.pow(row['Start_Stack_Height']+STACKH[1]-35.2, 2) + math.pow(width, 2) + math.pow(deltaz, 2))
                e1_5 = 180000 if l1_5 > 3.94 else 140000
                e2_5=0

                # LAshing Bridge Stiffness BV2018 NR625 Ch14 Sec1 5.5
                # alphaLB=0.5
                # e = alphaLB * Al * e / (10000 * l)
                llength1_5, Klhh1_5, Klvv1_5, Klvh1_5, deltaZ1_5, deltaY1_5, llength2_5, Klhh2_5, Klvv2_5, Klvh2_5, deltaZ2_5, deltaY2_5 = [[0] * (2 * int(rg)) for _ in range(12)]
                
                llength1_5[3] = l1_5
                Kl1_5 = (Al * e1_5) / (10000 * l1_5)
                Klhh1_5[3] = math.pow(width / l1_5, 2) * Kl1_5
                Klvv1_5[3] = math.pow((row['Start_Stack_Height']+STACKH[1]-35.2 + lashing_dict['Tftd'] / 1000) / l1_5, 2) * Kl1_5
                Klvh1_5[3] = (width * (row['Start_Stack_Height']+STACKH[1]-35.2 + lashing_dict['Tftd'] / 1000)) / math.pow(l1_5, 2) * Kl1_5
                deltaZ1_5[3] = row['Start_Stack_Height']+STACKH[1]-35.2 + lashing_dict['Tftd'] / 1000
                deltaY1_5[3] = width

                l2_5 = Kl2_5 = 0

            else:
                e1_5 = e2_5 = l1_5 = l2_5 = Kl1_5 = Kl2_5 = 0            
                dlb_5 = [10] * (2 * int(rg))
                llength1_5, llength2_5, Klhh1_5, Klhh2_5, Klvv1_5, Klvv2_5, Klvh1_5, Klvh2_5, deltaZ1_5, deltaZ2_5, deltaY1_5, deltaY2_5 = [[0] * (2 * int(rg)) for _ in range(12)]

            if rg > 2:
                #BV2014 fix for summing the short bars
                dlb_5 = [10] * (2 * int(rg))

                l2_5 = math.sqrt(math.pow(row['Start_Stack_Height']+STACKH[1]-35.2, 2) + math.pow(width, 2) + math.pow(deltaz, 2))*1.02/1.02
                e2_5 = 180000 if l2_5 > 3.9 else 140000
                llength2_5, Klhh2_5, Klvv2_5, Klvh2_5, deltaZ2_5, deltaY2_5 = [[0] * (2 * int(rg)) for _ in range(6)]
                llength2_5[4] = l2_5
                Kl2_5 = (Al * e2_5) / (10000 * l2_5)
                #The below lines will split the klhh and klvh as per BV2018

                Klhh2_5[4] =  math.pow(width / l2_5, 2) * Kl2_5
                Klvh2_5[4] = (width * (row['Start_Stack_Height'] + STACKH[1] - 35.2 + lashing_dict['Tftd'] / 1000)) / math.pow(l2_5, 2) * Kl2_5

                #The below lines will summup the klhh as per BV2014
                if lashing_dict['BV_rules']=='2014':
                    Klhh1_5_2014, Klvh1_5_2014, Klhh2_5_2014, Klvh2_5_2014 = [[0] * (2 * int(rg)) for _ in range(4)]

                    Klhh2_5_2014[4] = math.pow(width / l2_5, 2) * Kl2_5+math.pow(width / l1_5, 2) * Kl1_5
                    Klvh2_5_2014[4] = (width * (row['Start_Stack_Height'] + STACKH[1] - 35.2 + lashing_dict['Tftd'] / 1000)) / math.pow(l1_5, 2) * Kl1_5+(width * (row['Start_Stack_Height'] + STACKH[1] - 35.2 + lashing_dict['Tftd'] / 1000)) / math.pow(l2_5, 2) * Kl2_5
                
                Klvv2_5[4] = math.pow((row['Start_Stack_Height']+STACKH[1]-35.2 + lashing_dict['Tftd'] / 1000) / l2_5, 2) * Kl2_5
                deltaZ2_5[4] = row['Start_Stack_Height']+STACKH[1]-35.2 + lashing_dict['Tftd'] / 1000
                deltaY2_5[4] = width

            # LASHING FOR 40s
            # external rows, lashing from lashing bridge(from 2nd tier) (pattern 1)
            if rg > 2:

                dlb_10 = dlb_5
                l3_10 = math.sqrt(math.pow(row['Start_Stack_Height']+STACKH[2]-35.2, 2) + math.pow(width, 2) + math.pow(deltaz, 2))
                e3_10 = 180000 if l3_10 > 3.95 else 140000
                # LAshing Bridge Stiffness BV2018 NR625 Ch14 Sec1 5.5
                # alphaLB=0.5
                # e = alphaLB * Al * e / (10000 * l)
                llength3_10, Klhh3_10, Klvv3_10, Klvh3_10, deltaZ3_10, deltaY3_10 = [[0] * (2 * int(rg)) for _ in range(6)]
                llength3_10[5] = l3_10
                Kl3_10 = (Al * e3_10) / (10000 * l3_10)
                Klhh3_10[5] = math.pow(width / l3_10, 2) * Kl3_10
                Klvv3_10[5] = math.pow((row['Start_Stack_Height']+STACKH[2]-35.2 + lashing_dict['Tftd'] / 1000) / l3_10, 2) * Kl3_10
                Klvh3_10[5] = (width * (row['Start_Stack_Height']+STACKH[2]-35.2 + lashing_dict['Tftd'] / 1000)) / math.pow(l3_10, 2) * Kl3_10
                deltaZ3_10[5] = row['Start_Stack_Height']+STACKH[2]-35.2 + lashing_dict['Tftd'] / 1000
                deltaY3_10[5] = width

            else:
                e3_10 = l3_10 = Kl3_10 = 0
                dlb_10, llength3_10, Klhh3_10, Klvv3_10, Klvh3_10, deltaZ3_10, deltaY3_10 = [[0] * (2 * int(rg)) for _ in range(7)]

            # LASHING FOR 40s
            # internal rows, lashing from lashing bridge(from 3rd tier)

            if rg > 2:

                dlb_6 = [25] * (2 * int(rg))

                l1_6 = math.sqrt(math.pow(row['Start_Stack_Height']+STACKH[2]-37.8, 2) + math.pow(width, 2) + math.pow(deltaz, 2))*1/1
            
        
                e1_6 = 180000 if l1_6 > 3.9 else 140000
                e2_6 = l2_6 = Kl2_6 = 0
                # LAshing Bridge Stifness BV2018 NR625 Ch14 Sec1 5.5
                # alphaLB=0.3
                # e = alphaLB * Al * e / (10000 * l)
                llength1_6, Klhh1_6, Klvv1_6, Klvh1_6, deltaZ1_6, deltaY1_6, llength2_6, Klhh2_6, Klvv2_6, Klvh2_6, deltaZ2_6, deltaY2_6 = [[0] * (2 * int(rg)) for _ in range(12)]
                llength1_6[5] = l1_6
                Kl1_6 = (Al * e1_6) / (10000 * l1_6)
                Klhh1_6[5] = math.pow(width / l1_6, 2) * Kl1_6
                Klvv1_6[5] = math.pow((row['Start_Stack_Height']+STACKH[2]-37.8 + lashing_dict['Tftd'] / 1000) / l1_6, 2) * Kl1_6
                Klvh1_6[5] = (width * (row['Start_Stack_Height']+STACKH[2]-37.8 + lashing_dict['Tftd'] / 1000)) / math.pow(l1_6, 2) * Kl1_6
                deltaZ1_6[5] = row['Start_Stack_Height']+STACKH[2]-37.8 + lashing_dict['Tftd'] / 1000
                deltaY1_6[5] = width

            else:
                e1_6 = e2_6 = l1_6 = l2_6 = Kl1_6 = Kl2_6 = 0
                dlb_6 = [25] * (2 * int(rg))
                llength1_6, llength2_6, Klhh1_6, Klhh2_6, Klvv1_6, Klvv2_6, Klvh1_6, Klvh2_6, deltaZ1_6, deltaZ2_6, deltaY1_6, deltaY2_6 = [[0] * (2 * int(rg)) for _ in range(12)]
                
            if rg > 3:
                # BV2014 fix for summing the short bars
                dlb_6 = [25] * (2 * int(rg))
                l2_6 = math.sqrt(math.pow(row['Start_Stack_Height']+STACKH[2]-37.8, 2) + math.pow(width, 2) + math.pow(deltaz, 2))*1.028/1.028
                e2_6 = 180000 if l2_6 > 4 else 140000
                llength2_6, Klhh2_6, Klvv2_6, Klvh2_6, deltaZ2_6, deltaY2_6 = [[0] * (2 * int(rg)) for _ in range(6)]
                llength2_6[6] = l2_6
                Kl2_6 = (Al * e2_6) / (10000 * l2_6)
                # The below lines will split the klhh as per BV2018
                Klhh2_6[6] = math.pow(width / l2_6, 2) * Kl2_6
                Klvh2_6[6] = (width * (row['Start_Stack_Height'] + STACKH[2] - 37.8 + lashing_dict['Tftd'] / 1000)) / math.pow(l2_6, 2) * Kl2_6

                if lashing_dict['BV_rules']=='2014':
                # The below lines will summup the klhh and klvh as per BV2014
                    Klhh1_6_2014, Klvh1_6_2014, Klhh2_6_2014, Klvh2_6_2014 = [[0] * (2 * int(rg)) for _ in range(4)]
                    Klhh2_6_2014[6] = math.pow(width / l1_6, 2) * Kl1_6 + math.pow(width / l2_6, 2) * Kl2_6
                    Klvh2_6_2014[6] = (width * (row['Start_Stack_Height'] + STACKH[2] - 37.8 + lashing_dict['Tftd'] / 1000)) / math.pow(l1_6, 2) * Kl1_6+(width * (row['Start_Stack_Height'] + STACKH[2] - 37.8 + lashing_dict['Tftd'] / 1000)) / math.pow(l2_6, 2) * Kl2_6

                Klvv2_6[6] =math.pow((row['Start_Stack_Height']+STACKH[2]-37.8 + lashing_dict['Tftd'] / 1000) / l2_6, 2) * Kl2_6
                deltaZ2_6[6] = row['Start_Stack_Height']+STACKH[2]-37.8 + lashing_dict['Tftd'] / 1000
                deltaY2_6[6] = width

            # LASHING FOR 40s
            # external rows, lashing from lashing bridge(from 3rd tier) (pattern 1)

            if rg > 3:

                dlb_11 = dlb_6
                l3_11 = math.sqrt(math.pow(row['Start_Stack_Height']+STACKH[3]-37.8, 2) + math.pow(width, 2) + math.pow(deltaz, 2))
                e3_11 = 180000 if l3_11 > 3.95 else 140000

                # LAshing Bridge Stiffness BV2018 NR625 Ch14 Sec1 5.5
                # alphaLB=0.5
                # e = alphaLB * Al * e / (10000 * l)
                llength3_11, Klhh3_11, Klvv3_11, Klvh3_11, deltaZ3_11, deltaY3_11 = [[0] * (2 * int(rg)) for _ in range(6)]
                llength3_11[7] = l3_11
                Kl3_11 = (Al * e3_11) / (10000 * l3_11)
                Klhh3_11[7] = math.pow(width / l3_11, 2) * Kl3_11
                Klvv3_11[7] = math.pow((row['Start_Stack_Height']+STACKH[3]-37.8 + lashing_dict['Tftd'] / 1000) / l3_11, 2) * Kl3_11
                Klvh3_11[7] = (width * (row['Start_Stack_Height']+STACKH[3]-37.8 + lashing_dict['Tftd'] / 1000)) / math.pow(l3_11, 2) * Kl3_11
                deltaZ3_11[7] = row['Start_Stack_Height']+STACKH[3]-37.8 + lashing_dict['Tftd'] / 1000
                deltaY3_11[7] = width

            else:
                e3_11 = l3_11 = Kl3_11 = 0
                dlb_11, llength3_11, Klhh3_11, Klvv3_11, Klvh3_11, deltaZ3_11, deltaY3_11 = [[0] * (2 * int(rg)) for _ in range(7)]
                

            # LASHING FOR 40s
            # internal rows, lashing from lashing bridge(from 4th tier)
            if rg > 3:
                
                dlb_7 = [35] * (2 * int(rg))
                l1_7 = math.sqrt(math.pow(row['Start_Stack_Height']+STACKH[3]-38.1, 2) + math.pow(width, 2) + math.pow(deltaz, 2))*1.005/1.005
                e1_7 = 180000 if l1_7 > 3.95 else 140000

                e2_7 = Kl2_7 = 0
                # LAshing Bridge Stifness BV2018 NR625 Ch14 Sec1 5.5
                # alphaLB=0.2
                # e = alphaLB * Al * e / (10000 * l)
                llength1_7, Klhh1_7, Klvv1_7, Klvh1_7, deltaZ1_7, deltaY1_7, llength2_7, Klhh2_7, Klvv2_7, Klvh2_7, deltaZ2_7, deltaY2_7  = [[0] * (2 * int(rg)) for _ in range(12)]
                llength1_7[7] = l1_7
                Kl1_7 = (Al * e1_7) / (10000 * l1_7)
                Klhh1_7[7] = math.pow(width / l1_7, 2) * Kl1_7
                Klvv1_7[7] = math.pow((row['Start_Stack_Height']+STACKH[3]-38.1 + lashing_dict['Tftd'] / 1000) / l1_7, 2) * Kl1_7
                Klvh1_7[7] = (width * (row['Start_Stack_Height']+STACKH[3]-38.1 + lashing_dict['Tftd'] / 1000)) / math.pow(l1_7, 2) * Kl1_7
                deltaZ1_7[7] = row['Start_Stack_Height']+STACKH[3]-38.1 + lashing_dict['Tftd'] / 1000
                deltaY1_7[7] = width

            else:
                e1_7 = e2_7 = l1_7 = l2_7 = Kl1_7 = Kl2_7 = 0
                
                dlb_7 = [35] * (2 * int(rg))
                llength1_7, Klhh1_7, Klvv1_7, Klvh1_7, deltaZ1_7, deltaY1_7, llength2_7, Klhh2_7, Klvv2_7, Klvh2_7, deltaZ2_7, deltaY2_7 = [[0] * (2 * int(rg)) for _ in range(12)]

            if rg > 4:
                # BV2014 fix for summing the short bars
                dlb_7 = [35] * (2 * int(rg))

                l2_7 = math.sqrt(math.pow(row['Start_Stack_Height'] + STACKH[3]-38.1, 2) + math.pow(width, 2) + math.pow(deltaz, 2))*1.032/1.032
                e2_7 = 180000 if l2_7 > 4 else 140000
                llength2_7, Klhh2_7, Klvv2_7, Klvh2_7, deltaZ2_7, deltaY2_7 = [[0] * (2 * int(rg)) for _ in range(6)]
                llength2_7[8] = l2_7
                Kl2_7 = (Al * e2_7) / (10000 * l2_7)
                # The below lines will split the klhh and klvh as per BV2018
                Klhh2_7[8] = math.pow(width / l2_7, 2) * Kl2_7
                Klvh2_7[8] = (width * (row['Start_Stack_Height'] + STACKH[3] - 38.1 + lashing_dict['Tftd'] / 1000)) / math.pow(l2_7, 2) * Kl2_7

                if lashing_dict['BV_rules']=='2014':
                # The below lines will summup the klhh and klvh as per BV2014
                    Klhh1_7_2014, Klvh1_7_2014, Klhh2_7_2014, Klvh2_7_2014 = [[0] * (2 * int(rg)) for _ in range(4)]

                    Klhh2_7_2014[8] = math.pow(width / l1_7, 2) * Kl1_7+ math.pow(width / l2_7, 2) * Kl2_7
                    Klvh2_7_2014[8] = (width*  (row['Start_Stack_Height'] + STACKH[3]-38.1 + lashing_dict['Tftd']/ 1000))/ math.pow(l1_7, 2) * Kl1_7 
                    + (width * (row['Start_Stack_Height'] + STACKH[3] - 38.1 + lashing_dict['Tftd'] / 1000)) / math.pow(l2_7, 2) * Kl2_7

                Klvv2_7[8] = math.pow((row['Start_Stack_Height']+STACKH[3]-38.1 + lashing_dict['Tftd'] / 1000) / l2_7, 2) * Kl2_7
                deltaZ2_7[8] = row['Start_Stack_Height']+STACKH[3]-38.1 + lashing_dict['Tftd'] / 1000
                deltaY2_7[8] = width

            else:
                e1_7 = e2_7 = l1_7 = l2_7 = Kl1_7 = Kl2_7 = 0
                
                dlb_7 = [35] * (2 * int(rg))
                llength1_7, Klhh1_7, Klvv1_7, Klvh1_7, deltaZ1_7, deltaY1_7, llength2_7, Klhh2_7, Klvv2_7, Klvh2_7, deltaZ2_7, deltaY2_7 = [[0] * (2 * int(rg)) for _ in range(12)]

            # LASHING FOR 40s
            # external rows, lashing from lashing bridge(from 4th tier) (pattern 1)

            if rg > 2:

                dlb_14 = dlb_7
                l3_14 = math.sqrt(math.pow(row['Start_Stack_Height']+STACKH[2]-38.1, 2) + math.pow(width, 2) + math.pow(deltaz, 2))
                e3_14 = 180000 if l3_14 > 4 else 140000

                # LAshing Bridge Stiffness BV2018 NR625 Ch14 Sec1 5.5
                # alphaLB=0.5
                # e = alphaLB * Al * e / (10000 * l)
                llength3_14, Klhh3_14, Klvv3_14, Klvh3_14, deltaZ3_14, deltaY3_14 = [[0] * (2 * int(rg)) for _ in range(6)]
                llength3_14[5] = l3_14
                Kl3_14 = (Al * e3_14) / (10000 * l3_14)
                Klhh3_14[5] =  math.pow(width / l3_14, 2) * Kl3_14
                Klvv3_14[5] = math.pow((row['Start_Stack_Height']+STACKH[2]-38.1 + lashing_dict['Tftd'] / 1000) / l3_14, 2) * Kl3_14
                Klvh3_14[5] = (width * (row['Start_Stack_Height']+STACKH[2]-38.1 + lashing_dict['Tftd'] / 1000)) / math.pow(l3_14, 2) * Kl3_14
                deltaZ3_14[5] =  row['Start_Stack_Height']+STACKH[2]-38.1 + lashing_dict['Tftd'] / 1000
                deltaY3_14[5] = width

            else:
                e3_14 = l3_14 = Kl3_14 = 0
                dlb_14, llength3_14, Klhh3_14, Klvv3_14, Klvh3_14, deltaZ3_14, deltaY3_14 = [[0] * (2 * int(rg)) for _ in range(7)]
                dlb_14 = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]


            if rg > 3:

                dlb_12 = dlb_7
                l3_12 = math.sqrt(math.pow(row['Start_Stack_Height'] + STACKH[3] - 38.1, 2) + math.pow(width, 2) + math.pow(deltaz, 2))
                l3_15 = math.sqrt(math.pow(row['Start_Stack_Height'] + STACKH[2] - 38.1, 2) + math.pow(width, 2) + math.pow(deltaz, 2))
                e3_12 = 180000 if l3_12 > 4 else 140000
                e3_15 = 180000 if l3_15 > 4 else 140000

                # LAshing Bridge Stiffness BV2018 NR625 Ch14 Sec1 5.5
                # alphaLB=0.5
                # e = alphaLB * Al * e / (10000 * l)
                llength3_12, llength3_15, Klhh3_12, Klhh3_15, Klvv3_12, Klvv3_15, Klvh3_12, Klvh3_15, deltaZ3_12, deltaZ3_15, deltaY3_12, deltaY3_15 = [[0] * (2 * int(rg)) for _ in range(12)]
                llength3_12[7] = l3_12
                llength3_15[7] = l3_15
                Kl3_12 = (Al * e3_12) / (10000 * l3_12)
                Kl3_15 = (Al * e3_15) / (10000 * l3_15)
                Klhh3_12[7] = math.pow(width / l3_12, 2) * Kl3_12
                Klhh3_15[7] = math.pow(width / l3_15, 2) * Kl3_15
                Klvv3_12[7] = math.pow((row['Start_Stack_Height']+STACKH[3]-38.1 + lashing_dict['Tftd'] / 1000) / l3_12, 2) * Kl3_12
                Klvv3_15[7] =  math.pow((row['Start_Stack_Height']+STACKH[2]-38.1 + lashing_dict['Tftd'] / 1000) / l3_15, 2) * Kl3_15
                Klvh3_12[7] = (width * (row['Start_Stack_Height']+STACKH[3]-38.1 + lashing_dict['Tftd'] / 1000)) / math.pow(l3_12, 2) * Kl3_12
                Klvh3_15[7] = (width * (row['Start_Stack_Height']+STACKH[2]-38.1 + lashing_dict['Tftd'] / 1000)) / math.pow(l3_15, 2) * Kl3_15
                deltaZ3_12[7] = row['Start_Stack_Height']+STACKH[3]-38.1 + lashing_dict['Tftd'] / 1000
                deltaZ3_15[7] = row['Start_Stack_Height']+STACKH[2]-38.1 + lashing_dict['Tftd'] / 1000
                deltaY3_12[7] = width
                deltaY3_15[7] = width

            else:
                e3_12 = e3_15 = l3_12 = l3_15 = Kl3_12 = Kl3_15 = 0
                dlb_12, dlb_15, llength3_12, llength3_15, Klhh3_12, Klhh3_15, Klvv3_12, Klvv3_15, Klvh3_12, Klvh3_15, deltaZ3_12, deltaZ3_15, deltaY3_12, deltaY3_15 = [[0] * (2 * int(rg)) for _ in range(14)]
                dlb_12 = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
                dlb_15 = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]

            if rg > 4:

                dlb_13 = dlb_7
                l3_13 = math.sqrt(math.pow(row['Start_Stack_Height'] + STACKH[3]-38.1, 2) + math.pow(width, 2) + math.pow(deltaz, 2))
                e3_13 = 180000 if l3_13 > 4 else 140000

                # LAshing Bridge Stiffness BV2018 NR625 Ch14 Sec1 5.5
                # alphaLB=0.5
                # e = alphaLB * Al * e / (10000 * l)
                llength3_13, Klhh3_13, Klvv3_13, Klvh3_13, deltaZ3_13, deltaY3_13 = [[0] * (2 * int(rg)) for _ in range(6)]
                llength3_13[7] = l3_13
                Kl3_13 = (Al * e3_13) / (10000 * l3_13)
                Klhh3_13[7] = math.pow(width / l3_13, 2) * Kl3_13
                Klvv3_13[7] = math.pow((row['Start_Stack_Height'] + STACKH[3]-38.1 + lashing_dict['Tftd'] / 1000) / l3_13, 2) * Kl3_13
                Klvh3_13[7] = (width * (row['Start_Stack_Height']+STACKH[3]-38.1 + lashing_dict['Tftd'] / 1000)) / math.pow(l3_13, 2) * Kl3_13
                deltaZ3_13[7] = row['Start_Stack_Height'] + STACKH[3]-38.1 + lashing_dict['Tftd'] / 1000
                deltaY3_13[7] = width

            else:
                e3_13 = l3_13 = Kl3_13 = 0
                dlb_13, llength3_13, Klhh3_13, Klvv3_13, Klvh3_13, deltaZ3_13, deltaY3_13 = [[0] * (2 * int(rg)) for _ in range(7)]

            # Reference lashing_values of the accelerations aX, aY and aZ

            Kx = max(1.2 * math.pow((row['LCG'] / lashing_dict['Lpp']), 2) - 1.1 * (row['LCG'] / lashing_dict['Lpp']) + 0.2, 0.018)
            
            # Upright condition
            ax1 = math.sqrt(math.pow(lashing_dict['asu'], 2) + math.pow((lashing_dict['ap'] * 9.81 + lashing_dict['alphap'] * (VCGtier_list[-1] - self._draft)), 2))
            ay1 = 0
            az1 = math.sqrt(math.pow(lashing_dict['ah'], 2) + math.pow(lashing_dict['alphap'] * lashing_dict['Lpp'], 2) * Kx)
            AX = 1.4 * lashing_dict['nw1'] * ax1
            AZ1 = 1.4 * lashing_dict['nw1'] * az1        

            # Inclined condition
            ax2 = 0
            ay2 = math.sqrt(math.pow(lashing_dict['asw'], 2) + math.pow((lashing_dict['ar'] * 9.81 + lashing_dict['alphaR'] * (VCGtier_list[-1] - self._draft)), 2) + math.pow(lashing_dict['alphaY'] * lashing_dict['Lpp'], 2)\
                * Kx)
            az2 = math.sqrt(0.25 * math.pow(lashing_dict['ah'], 2) + math.pow(lashing_dict['alphaR'] * row['TCG'], 2))  

            nw2ay2 = lashing_dict['nw2'] * ay2
            if nw2ay2 < 0.35 * 9.81:
                if row['BAY'] in [37,38,39,41,42,43,45,46,47,49,50,51,53,54,55,57,58,59,61,62,63,65,66,67,69,70,71,74] and lashing_dict['nw1']==lashing_dict['nw2']==1:
                    nw2ay2 = 4.65/1.4
                # below not ok with BV (as we apply it on "Worldwide" which we should not) , results ok for MACS3
                elif row['BAY'] in [37,38,39,41,42,43,45,46,47,49,50,51,53,54,55,57,58,59,61,62,63,65,66,67,69,70,71,74]:
                    nw2ay2 = lashing_dict['nw2'] * ay2
                # below ok with BV not ok with MACS3
                else:
                    nw2ay2=0.35 * 9.81
        
            AY = 1.4 * nw2ay2
            AZ2 = 1.4 * lashing_dict['nw2'] * az2
            
    #########################################################################################################        
            # FXWINDI = [1.2 * element for element in row['5ge_Area_x']]
            FYWINDI = [1.2 * element for element in row['Coverage_Area']]
            Fsi = [mass * 9.81 for mass in masses]
            # 4.3.3 Inertial forces in upright condition
            FWXI = [mass * AX for mass in masses]
            #debug
            Fwzi1 = [mass * AZ1 for mass in masses]
            # 4.3.4 Inertial forces in inclined condition
            Fwyi = [mass * AY for mass in masses]
            Fwzi2 = [mass * AZ2 for mass in masses]
            # 5 Resulting loads in lashing equipment and container frames
            # 5.2.2 Longitudinal force
            # FXI = [FWXI[i] + FXWINDI[i] for i in range(len(FWXI))]
            
            # NR467 Pt D, Ch 2, Sec 2,Table 2 : Containers - Still water, inertial and wind forces - forces acting on the stack
            Fsiold = 0  # Initial value of Fsiold
            Fs = []  # List to store Fs values
            Rs = []  # List to store Rs values

            # Iterate over the Fsi values
            for value in Fsi:
                Fs.append(value + Fsiold)
                Fsiold = Fs[-1]
                Rs.append(Fs[-1] / 4)
            
            Fwxiold = 0  # Initial value of Fwxiold
            Fxwindiold = 0  # Initial value of Fxwindiold
            Fwzi1old = 0  # Initial value of Fwzi1old

            Fwx = []
            Fwz1 = []

            # Iterate over the lists
            for i in range(len(Fwzi1)):
                # Fwx_value = FWXI[i] + FXWINDI[i] + Fwxiold + Fxwindiold
                # Fwxiold = Fwx_value
                # Fxwindiold = FXWINDI[i]
                Fwz1_value = Fwzi1[i] + Fwzi1old
                Fwzi1old = Fwz1_value
                # Fwx.append(Fwx_value)
                Fwz1.append(Fwz1_value)    
                
            # Rw11 = [(Fwz / 4) + ((i * h * Fw) / (4 * l)) for i, (Fwz, h, Fw, l) in enumerate(zip(Fwz1, row['Height List'], Fwx, row['Length List']))]    
            Rw11 = [(Fwz / 4) for Fwz in Fwz1] 
            # Rw21 = [(Fwz / 4) - ((i * h * Fw) / (4 * l)) for i, (Fwz, h, Fw, l) in enumerate(zip(Fwz1, row['Height List'], Fwx, row['Length List']))]    
            Rw21 = [(Fwz / 4) for Fwz in Fwz1] 
            
            # Inclined condition

            Fwy = [Fwyi[i] + FYWINDI[i] for i in range(len(Fwyi))]
            Fwz2 = [sum(Fwzi2[:i+1]) for i in range(len(Fwzi2))]
            Rw12 = [(fwz/4) + ((i * row['Height List'] [i] * Fwy[i]) / (4 * width)) for i, fwz in enumerate(Fwz2)]
            Rw22 = [(fwz/4) - ((i * row['Height List'] [i] * Fwy[i]) / (4 * width)) for i, fwz in enumerate(Fwz2)]  

            # 5.2.3 Transverse forces
            Fywind2i = Fywind2im1 = [0.25 * val for val in FYWINDI]
            Fwy2im1 = [0.5 * val for val in Fwyi]
            FY2I, FY2IM1 = [], []
            for i in range(len(row['Tiers in Stack'])):
                for n in range(2 * i - 1, 2 * i + 1, 1):
                    Fwy2i = 0
                    if n == 2 * i:
                        # for the top corners of the exposed side:
                        Fy2i = Fwy2i + Fywind2i[i]
                        Fy2im1 = 0
                        FY2I.append(Fy2i)
                    
                    else: 
                        # for the bottom corners of the exposed side:
                        Fy2i = 0
                        Fy2im1 = Fwy2im1[i] + Fywind2im1[i]
                        FY2IM1.append(Fy2im1)
                    # 5.2.4 Vertical forces
                    Rwzi = 0.45 * row['Height List'][i] * Fwyi[i] / (2 * width)
                    Mr = width * Rwzi
                    Mf = M = 0.45 * row['Height List'][i] * Fwyi[i] / 2
                    
                    if n == 2 * i:
                        # for the top corner:
                        Fwz2i = 0
                        Fwz2im1 = 0
                        Fs2i = 0
                        Fs2im1 = 0
                        Fz2iL = Fz2iR = Fs2i + Fwz2i
                        Fz2im1L = Fz2im1R = 0
                        
                    else:
                        Fwz2i = 0
                        Fs2i = 0
                        Fz2iL = Fz2iR = 0
                        # for the bottom corner:

                        Fs2im1 = 0.25 * Fsi[i]
                        # for the bottom corner, in upright condition:
                        Fwz2im1 = 0.25 * Fwzi1[i]
                        # for the bottom corner, in inclined condition:
                        Fwz2im11 = 0.25 * Fwzi2[i]
                        # for the bottom corner, right side (upright and inclined)
                        Fz2im1R1 = Fs2im1 + Fwz2im1 + Rwzi
                        Fz2im1R2 = Fs2im1 + Fwz2im11 + Rwzi
                        # for the bottom corner, left side (upright and inclined)
                        Fz2im1L1 = Fs2im1 + Fwz2im1 - Rwzi
                        Fz2im1L2 = Fs2im1 + Fwz2im11 - Rwzi

                        FZR1.append(Fz2im1R1)
                        FZR2.append(Fz2im1R2)
                        FZL1.append(Fz2im1L1)
                        FZL2.append(Fz2im1L2)


            lash_bridge_fore, lash_bridge_aft = self._vessel.getLBFA(row['BAY'])
            # Apply the lashing for the particular case       
            # LASHING FOR 20s and 40s FORE (pattern 4)
        # internal rows, lashing from hatchcover, except extreme rows

            if lash_bridge_fore == 32.689 and row['ROW'] not in exterior_rows_per_bay[row['BAY']]:
                KLHHFORE = KLHHFORE_2014 = np.array(Klhh1_1[:(2 * int(rg))]) + np.array(Klhh2_1[:(2 * int(rg))])
                KLVHFORE = KLVHFORE_2014 = np.array(Klvh1_1[:(2 * int(rg))]) + np.array(Klvh2_1[:(2 * int(rg))])
                KLVVFORE = np.array(Klvv1_1[:(2 * int(rg))]) + np.array(Klvv2_1[:(2 * int(rg))])
                LLENGTHFORE = np.array(llength1_1[:(2 * int(rg))]) + np.array(llength2_1[:(2 * int(rg))])
                DELTAZFORE = np.array(deltaZ1_1[:(2 * int(rg))]) + np.array(deltaZ2_1[:(2 * int(rg))])
                DELTAYFORE = np.array(deltaY1_1[:(2 * int(rg))]) + np.array(deltaY2_1[:(2 * int(rg))])

        #         DELTALBFORE = dlb_1
                EMODULEFORE = [e1_1,e2_1]
            
                # LASHING FOR 20s and 40s FORE (pattern 2 in_out)
        # external rows, lashing from hatchcover, wind applied from hatchcover center towards seaside
            if lash_bridge_fore == 32.689 and row['ROW'] in exterior_rows_per_bay[row['BAY']]:
                KLHHFORE = KLHHFORE_2014 = np.array(Klhh1_2[:(2 * int(rg))]) + np.array(Klhh2_2[:(2 * int(rg))])
                KLVHFORE = KLVHFORE_2014 =  np.array(Klvh1_2[:(2 * int(rg))]) + np.array(Klvh2_2[:(2 * int(rg))])
                KLVVFORE = np.array(Klvv1_2) + np.array(Klvv2_2)
                LLENGTHFORE = np.array(llength1_2[:(2 * int(rg))]) + np.array(llength2_2[:(2 * int(rg))])
                DELTAZFORE = np.array(deltaZ1_2[:(2 * int(rg))]) + np.array(deltaZ2_2[:(2 * int(rg))])
                DELTAYFORE = np.array(deltaY1_2[:(2 * int(rg))]) + np.array(deltaY2_2[:(2 * int(rg))])

        #         DELTALBFORE = dlb_2
                EMODULEFORE = [e1_2,e2_2]

            # LASHING FOR 20s and 40s FORE (pattern 2 out_in)
            # external rows, lashing from hatchcover, wind applied from seaside towards hatchcover center
            if lash_bridge_fore == 32.689 and row['ROW'] in exterior_rows_per_bay[row['BAY']] and lashing_dict['wind_side']=='out_in':
                KLHHFORE = KLHHFORE_2014 = np.array(Klhh1_0[:(2 * int(rg))]) + np.array(Klhh2_0[:(2 * int(rg))]) + np.array(Klhh3_0[:(2 * int(rg))])
                KLVHFORE = KLVHFORE_2014 = np.array(Klvh1_0[:(2 * int(rg))]) + np.array(Klvh2_0[:(2 * int(rg))]) + np.array(Klvh3_0[:(2 * int(rg))])
                KLVVFORE = np.array(Klvv1_0[:(2 * int(rg))]) + np.array(Klvv2_0[:(2 * int(rg))]) + np.array(Klvv3_0[:(2 * int(rg))])
                LLENGTHFORE = np.array(llength1_0[:(2 * int(rg))]) + np.array(llength2_0[:(2 * int(rg))]) + np.array(llength3_0[:(2 * int(rg))])
                DELTAZFORE = np.array(deltaZ1_0[:(2 * int(rg))]) + np.array(deltaZ2_0[:(2 * int(rg))]) + np.array(deltaZ3_0[:(2 * int(rg))])
                DELTAYFORE = np.array(deltaY1_0[:(2 * int(rg))]) + np.array(deltaY2_0[:(2 * int(rg))]) + np.array(deltaY3_0[:(2 * int(rg))])

                #         DELTALBFORE = dlb_3
                EMODULEFORE = [e1_0,e2_0,e3_0]

            # LASHING FOR 20s FORE (pattern 5)
            # internal rows, lashing from lashing bridge(from 2nd tier).For external rows wind applied from hatchcover center towards seaside.
            if lash_bridge_fore == 35.2 and row['Bay Type'] == 'bay20':
                KLHHFORE = KLHHFORE_2014 = np.array(Klhh1_3[:(2 * int(rg))]) + np.array(Klhh2_3[:(2 * int(rg))])
                KLVHFORE = KLVHFORE_2014 = np.array(Klvh1_3[:(2 * int(rg))]) + np.array(Klvh2_3[:(2 * int(rg))])
                KLVVFORE = np.array(Klvv1_3[:(2 * int(rg))]) + np.array(Klvv2_3[:(2 * int(rg))])
                LLENGTHFORE = np.array(llength1_3[:(2 * int(rg))]) + np.array(llength2_3[:(2 * int(rg))])
                DELTAZFORE = np.array(deltaZ1_3[:(2 * int(rg))]) + np.array(deltaZ2_3[:(2 * int(rg))])
                DELTAYFORE = np.array(deltaY1_3[:(2 * int(rg))]) + np.array(deltaY2_3[:(2 * int(rg))])

        #         DELTALBFORE = dlb_3
                EMODULEFORE = [e1_3]

            # LASHING FOR 20s FORE (pattern 6)
            # external rows, lashing from lashing bridge(from 2nd tier).For external rows wind applied from seaside to hatch cover centre.
            if lash_bridge_fore == 35.2 and row['Bay Type'] == 'bay20' and row['ROW'] in exterior_rows_per_bay[row['BAY']] and lashing_dict['wind_side']=='out_in':
                KLHHFORE = KLHHFORE_2014 = np.array(Klhh1_3[:(2 * int(rg))]) + np.array(Klhh2_8[:(2 * int(rg))])
                KLVHFORE = KLVHFORE_2014 = np.array(Klvh1_3[:(2 * int(rg))]) + np.array(Klvh2_8[:(2 * int(rg))])
                KLVVFORE = np.array(Klvv1_3[:(2 * int(rg))]) + np.array(Klvv2_8[:(2 * int(rg))])
                LLENGTHFORE = np.array(llength1_3[:(2 * int(rg))]) + np.array(llength2_8[:(2 * int(rg))])
                DELTAZFORE = np.array(deltaZ1_3[:(2 * int(rg))]) + np.array(deltaZ2_8[:(2 * int(rg))])
                DELTAYFORE = np.array(deltaY1_3[:(2 * int(rg))]) + np.array(deltaY2_8[:(2 * int(rg))])

                EMODULEFORE = [e1_3, e2_8]

            # LASHING FOR 20s FORE (pattern 5)
            # internal rows, lashing from lashing bridge(from 3rd tier).For external rows wind applied from hatchcover center towards seaside.
            if lash_bridge_fore == 37.8 and row['Bay Type'] == 'bay20':
                KLHHFORE = KLHHFORE_2014 = np.array(Klhh1_4[:(2 * int(rg))]) + np.array(Klhh2_4[:(2 * int(rg))])
                KLVHFORE = KLVHFORE_2014 = np.array(Klvh1_4[:(2 * int(rg))]) + np.array(Klvh2_4[:(2 * int(rg))])
                KLVVFORE = np.array(Klvv1_4[:(2 * int(rg))]) + np.array(Klvv2_4[:(2 * int(rg))])
                LLENGTHFORE = np.array(llength1_4[:(2 * int(rg))]) + np.array(llength2_4[:(2 * int(rg))])
                DELTAZFORE = np.array(deltaZ1_4[:(2 * int(rg))]) + np.array(deltaZ2_4[:(2 * int(rg))])
                DELTAYFORE = np.array(deltaY1_4[:(2 * int(rg))]) + np.array(deltaY2_4[:(2 * int(rg))])

            # LASHING FOR 20s FORE (pattern 6)
            # external rows, lashing from lashing bridge(from 3rd tier).For external rows wind applied from seaside to hatch cover centre.
            if lash_bridge_fore == 37.8 and row['Bay Type'] == 'bay20' and row['ROW'] in exterior_rows_per_bay[row['BAY']] and lashing_dict['wind_side']=='out_in':
                KLHHFORE = KLHHFORE_2014 = np.array(Klhh1_4[:(2 * int(rg))]) + np.array(Klhh2_9[:(2 * int(rg))])
                KLVHFORE = KLVHFORE_2014 = np.array(Klvh1_4[:(2 * int(rg))]) + np.array(Klvh2_9)
                KLVVFORE = np.array(Klvv1_4[:(2 * int(rg))]) + np.array(Klvv2_9)
                LLENGTHFORE = np.array(llength1_4[:(2 * int(rg))]) + np.array(llength2_9[:(2 * int(rg))])
                DELTAZFORE = np.array(deltaZ1_4[:(2 * int(rg))]) + np.array(deltaZ2_9[:(2 * int(rg))])
                DELTAYFORE = np.array(deltaY1_4[:(2 * int(rg))]) + np.array(deltaY2_9[:(2 * int(rg))])

                #         DELTALBFORE = dlb_4
                EMODULEFORE = [e1_4,e2_9]

            # LASHING FOR 40s (2018) FORE (pattern 3)
            # internal rows, lashing from lashing bridge(from 2nd tier). For external rows wind applied from hatchcover center towards seaside.
            if lash_bridge_fore == 35.2 and row['Bay Type'] == 'bay40':
                KLHHFORE = KLHHFORE_2014 = np.array(Klhh1_5[:(2 * int(rg))]) + np.array(Klhh2_5[:(2 * int(rg))])
                KLVHFORE = KLVHFORE_2014 = np.array(Klvh1_5[:(2 * int(rg))]) + np.array(Klvh2_5[:(2 * int(rg))])
                KLVVFORE = np.array(Klvv1_5[:(2 * int(rg))]) + np.array(Klvv2_5[:(2 * int(rg))])
                LLENGTHFORE = np.array(llength1_5[:(2 * int(rg))]) + np.array(llength2_5[:(2 * int(rg))])
                DELTAZFORE = np.array(deltaZ1_5[:(2 * int(rg))]) + np.array(deltaZ2_5[:(2 * int(rg))])
                DELTAYFORE = np.array(deltaY1_5[:(2 * int(rg))]) + np.array(deltaY2_5[:(2 * int(rg))])

        #         DELTALBFORE = dlb_5
                EMODULEFORE = [e1_5,e2_5]


            # LASHING FOR 40s (2014) FORE (pattern 3)
            # internal rows, lashing from lashing bridge(from 2nd tier). For external rows wind applied from hatchcover center towards seaside.
            if lash_bridge_fore == 35.2 and row['Bay Type'] == 'bay40' and lashing_dict['BV_rules']=='2014' and row['ROW'] not in exterior_rows_per_bay[row['BAY']] :
                KLHHFORE = KLHHFORE_2014 = np.array(Klhh1_5_2014[:(2 * int(rg))]) + np.array(Klhh2_5_2014[:(2 * int(rg))])
                KLVHFORE = KLVHFORE_2014 = np.array(Klvh1_5_2014[:(2 * int(rg))]) + np.array(Klvh2_5_2014[:(2 * int(rg))])
                KLVVFORE = np.array(Klvv1_5[:(2 * int(rg))]) + np.array(Klvv2_5[:(2 * int(rg))])
                LLENGTHFORE = np.array(llength1_5[:(2 * int(rg))]) + np.array(llength2_5[:(2 * int(rg))])
                DELTAZFORE = np.array(deltaZ1_5[:(2 * int(rg))]) + np.array(deltaZ2_5[:(2 * int(rg))])
                DELTAYFORE = np.array(deltaY1_5[:(2 * int(rg))]) + np.array(deltaY2_5[:(2 * int(rg))])

                #  DELTALBFORE = dlb_5
                EMODULEFORE = [e1_5, e2_5]

            # LASHING FOR 40s (2018) FORE (pattern 1)
            # external rows, lashing from lashing bridge(from 2nd tier). For external rows wind applied from seaside to hatch cover centre.
            if lash_bridge_fore == 35.2 and row['Bay Type'] == 'bay40' and row['ROW'] in exterior_rows_per_bay[row['BAY']] and lashing_dict['wind_side']=='out_in':
                KLHHFORE = KLHHFORE_2014 = np.array(Klhh1_5[:(2 * int(rg))]) + np.array(Klhh2_5[:(2 * int(rg))]) + np.array(Klhh2_8[:(2 * int(rg))]) + np.array(Klhh3_10[:(2 * int(rg))])
                KLVHFORE = KLVHFORE_2014 = np.array(Klvh1_5[:(2 * int(rg))]) + np.array(Klvh2_5[:(2 * int(rg))]) + np.array(Klvh2_8[:(2 * int(rg))]) + np.array(Klvh3_10[:(2 * int(rg))])
                KLVVFORE = np.array(Klvv1_5[:(2 * int(rg))]) + np.array(Klvv2_5[:(2 * int(rg))]) + np.array(Klvv2_8[:(2 * int(rg))]) + np.array(Klvv3_10[:(2 * int(rg))])
                LLENGTHFORE = np.array(llength1_5[:(2 * int(rg))]) + np.array(llength2_5[:(2 * int(rg))]) + np.array(llength2_8[:(2 * int(rg))]) + np.array(llength3_10[:(2 * int(rg))])
                DELTAZFORE = np.array(deltaZ1_5[:(2 * int(rg))]) + np.array(deltaZ2_5[:(2 * int(rg))]) + np.array(deltaZ2_8[:(2 * int(rg))]) + np.array(deltaZ3_10[:(2 * int(rg))])
                DELTAYFORE = np.array(deltaY1_5[:(2 * int(rg))]) + np.array(deltaY2_5[:(2 * int(rg))]) + np.array(deltaY2_8[:(2 * int(rg))]) + np.array(deltaY3_10[:(2 * int(rg))])

            #         DELTALBFORE = dlb_5
                EMODULEFORE = [e1_5, e2_5, e2_8, e3_10]

            # LASHING FOR 40s (2018) FORE (pattern 3)
            # internal rows, lashing from lashing bridge(from 3rd tier). For external rows wind applied from hatchcover center towards seaside.
            if lash_bridge_fore == 37.8 and row['Bay Type'] == 'bay40':
                KLHHFORE = KLHHFORE_2014 = np.array(Klhh1_6[:(2 * int(rg))]) + np.array(Klhh2_6[:(2 * int(rg))])
                KLVHFORE = KLVHFORE_2014 = np.array(Klvh1_6[:(2 * int(rg))]) + np.array(Klvh2_6[:(2 * int(rg))])
                KLVVFORE = np.array(Klvv1_6[:(2 * int(rg))]) + np.array(Klvv2_6[:(2 * int(rg))])
                LLENGTHFORE = np.array(llength1_6[:(2 * int(rg))]) + np.array(llength2_6[:(2 * int(rg))])
                DELTAZFORE = np.array(deltaZ1_6[:(2 * int(rg))]) + np.array(deltaZ2_6[:(2 * int(rg))])
                DELTAYFORE = np.array(deltaY1_6[:(2 * int(rg))]) + np.array(deltaY2_6[:(2 * int(rg))])

        #         DELTALBFORE = dlb_6
                EMODULEFORE = [e1_6,e2_6]

            # LASHING FOR 40s (2018) FORE (pattern 1)
            # external rows, lashing from lashing bridge(from 3rd tier). For external rows wind applied from seaside to hatch cover centre.

            if lash_bridge_fore == 37.8 and row['Bay Type'] == 'bay40' and row['ROW'] in exterior_rows_per_bay[row['BAY']] and lashing_dict['wind_side']=='out_in':

                KLHHFORE = KLHHFORE_2014 = np.array(Klhh1_6[:(2 * int(rg))]) + np.array(Klhh2_6[:(2 * int(rg))]) + np.array(Klhh2_9[:(2 * int(rg))]) + np.array(Klhh3_11[:(2 * int(rg))])
                KLVHFORE = KLVHFORE_2014 = np.array(Klvh1_6[:(2 * int(rg))]) + np.array(Klvh2_6[:(2 * int(rg))]) + np.array(Klvh2_9[:(2 * int(rg))]) + np.array(Klvh3_11[:(2 * int(rg))])
                KLVVFORE = np.array(Klvv1_6[:(2 * int(rg))]) + np.array(Klvv2_6[:(2 * int(rg))]) + np.array(Klvv2_9[:(2 * int(rg))]) + np.array(Klvv3_11[:(2 * int(rg))])
                LLENGTHFORE = np.array(llength1_6[:(2 * int(rg))]) + np.array(llength2_6[:(2 * int(rg))]) + np.array(llength2_9[:(2 * int(rg))]) + np.array(llength3_11[:(2 * int(rg))])
                DELTAZFORE = np.array(deltaZ1_6[:(2 * int(rg))]) + np.array(deltaZ2_6[:(2 * int(rg))]) + np.array(deltaZ2_9[:(2 * int(rg))]) + np.array(deltaZ3_11[:(2 * int(rg))])
                DELTAYFORE = np.array(deltaY1_6[:(2 * int(rg))]) + np.array(deltaY2_6[:(2 * int(rg))]) + np.array(deltaY2_9[:(2 * int(rg))]) + np.array(deltaY3_11[:(2 * int(rg))])

                EMODULEFORE = [e1_6, e2_6, e2_9, e3_11]


            # LASHING FOR 40s (2014) FORE (pattern 3)
            # internal rows, lashing from lashing bridge(from 3rd tier).For external rows wind applied from hatchcover center towards seaside.
            if lash_bridge_fore == 37.8 and row['Bay Type'] == 'bay40' and row['ROW'] not in exterior_rows_per_bay[row['BAY']] and lashing_dict['BV_rules'] =='2014':
                KLHHFORE = KLHHFORE_2014 = np.array(Klhh1_6_2014[:(2 * int(rg))]) + np.array(Klhh2_6_2014[:(2 * int(rg))])
                KLVHFORE = KLVHFORE_2014 = np.array(Klvh1_6_2014[:(2 * int(rg))]) + np.array(Klvh2_6_2014[:(2 * int(rg))])
                KLVVFORE = np.array(Klvv1_6[:(2 * int(rg))]) + np.array(Klvv2_6[:(2 * int(rg))])
                LLENGTHFORE = np.array(llength1_6[:(2 * int(rg))]) + np.array(llength2_6[:(2 * int(rg))])
                DELTAZFORE = np.array(deltaZ1_6[:(2 * int(rg))]) + np.array(deltaZ2_6[:(2 * int(rg))])
                DELTAYFORE = np.array(deltaY1_6[:(2 * int(rg))]) + np.array(deltaY2_6[:(2 * int(rg))])

                #         DELTALBFORE = dlb_6
                EMODULEFORE = [e1_6, e2_6]

            # LASHING FOR 40s (2018) FORE (pattern 3)
            # internal rows, lashing from lashing bridge(from 4th tier).For external rows wind applied from hatchcover center towards seaside.
            if lash_bridge_fore == 38.1 and row['Bay Type'] == 'bay40' :
                KLHHFORE = KLHHFORE_2014 = np.array(Klhh1_7[:(2 * int(rg))]) + np.array(Klhh2_7[:(2 * int(rg))])
                KLVHFORE = KLVHFORE_2014 = np.array(Klvh1_7[:(2 * int(rg))]) + np.array(Klvh2_7[:(2 * int(rg))])
                KLVVFORE = np.array(Klvv1_7[:(2 * int(rg))]) + np.array(Klvv2_7[:(2 * int(rg))])
                LLENGTHFORE = np.array(llength1_7[:(2 * int(rg))]) + np.array(llength2_7[:(2 * int(rg))])
                DELTAZFORE = np.array(deltaZ1_7[:(2 * int(rg))]) + np.array(deltaZ2_7[:(2 * int(rg))])
                DELTAYFORE = np.array(deltaY1_7[:(2 * int(rg))]) + np.array(deltaY2_7[:(2 * int(rg))])

        #         DELTALBFORE = dlb_7
                EMODULEFORE = [e1_7,e2_7]

            # LASHING FOR 40s (2014) FORE (pattern 3)
            # internal rows, lashing from lashing bridge(from 4th tier).For external rows wind applied from hatchcover center towards seaside.
            if lash_bridge_fore == 38.1 and row['Bay Type'] == 'bay40' and lashing_dict['BV_rules']=='2014':
                KLHHFORE = KLHHFORE_2014 = np.array(Klhh1_7_2014[:(2 * int(rg))]) + np.array(Klhh2_7_2014[:(2 * int(rg))])
                KLVHFORE = KLVHFORE_2014 = np.array(Klvh1_7_2014[:(2 * int(rg))]) + np.array(Klvh2_7_2014[:(2 * int(rg))])
                KLVVFORE = np.array(Klvv1_7[:(2 * int(rg))]) + np.array(Klvv2_7[:(2 * int(rg))])
                LLENGTHFORE = np.array(llength1_7[:(2 * int(rg))]) + np.array(llength2_7[:(2 * int(rg))])
                DELTAZFORE = np.array(deltaZ1_7[:(2 * int(rg))]) + np.array(deltaZ2_7[:(2 * int(rg))])
                DELTAYFORE = np.array(deltaY1_7[:(2 * int(rg))]) + np.array(deltaY2_7[:(2 * int(rg))])

                #         DELTALBFORE = dlb_7
                EMODULEFORE = [e1_7, e2_7]


            # LASHING FOR 40s (2018) FORE (pattern 1)
            # external rows, lashing from lashing bridge(from 4th tier). For external rows wind applied from seaside to hatch cover centre.

            if lash_bridge_fore == 38.1 and row['Bay Type'] == 'bay40' and row['ROW'] in exterior_rows_per_bay[row['BAY']] and lashing_dict['wind_side']=='out_in':
                KLHHFORE = KLHHFORE_2014 = np.array(Klhh3_14[:(2 * int(rg))]) + np.array(Klhh3_15[:(2 * int(rg))]) + np.array(Klhh3_12[:(2 * int(rg))]) + np.array(Klhh3_13[:(2 * int(rg))])
                KLVHFORE = KLVHFORE_2014 = np.array(Klvh3_14[:(2 * int(rg))]) + np.array(Klvh3_15[:(2 * int(rg))]) + np.array(Klvh3_12[:(2 * int(rg))]) + np.array(Klvh3_13[:(2 * int(rg))])
                KLVVFORE = np.array(Klvv3_14[:(2 * int(rg))]) + np.array(Klvv3_15[:(2 * int(rg))]) + np.array(Klvv3_12[:(2 * int(rg))]) + np.array(Klvv3_13[:(2 * int(rg))])
                LLENGTHFORE = np.array(llength3_14[:(2 * int(rg))]) + np.array(llength3_15[:(2 * int(rg))]) + np.array(llength3_12[:(2 * int(rg))]) + np.array(llength3_13[:(2 * int(rg))])
                DELTAZFORE = np.array(deltaZ3_14[:(2 * int(rg))]) + np.array(deltaZ3_15[:(2 * int(rg))]) + np.array(deltaZ3_12[:(2 * int(rg))]) + np.array(deltaZ3_13[:(2 * int(rg))])
                DELTAYFORE = np.array(deltaY3_14[:(2 * int(rg))]) + np.array(deltaY3_15[:(2 * int(rg))]) + np.array(deltaY3_12[:(2 * int(rg))]) + np.array(deltaY3_13[:(2 * int(rg))])

                #         DELTALBFORE = dlb_7
                EMODULEFORE = [e3_14, e3_15, e3_12, e3_13]

            # LASHING FOR 20s and 40s AFT (pattern 4)
            # internal rows, lashing from hatchcover, except extreme rows
            if lash_bridge_aft == 32.689 and row['ROW'] not in exterior_rows_per_bay[row['BAY']]:
                KLHHAFT = KLHHAFT_2014 = np.array(Klhh1_1[:(2 * int(rg))]) + np.array(Klhh2_1[:(2 * int(rg))])
                KLVHAFT = KLVHAFT_2014 = np.array(Klvh1_1[:(2 * int(rg))]) + np.array(Klvh2_1[:(2 * int(rg))])
                KLVVAFT = np.array(Klvv1_1[:(2 * int(rg))]) + np.array(Klvv2_1[:(2 * int(rg))])
                LLENGTHAFT = np.array(llength1_1[:(2 * int(rg))]) + np.array(llength2_1[:(2 * int(rg))])
                DELTAZAFT = np.array(deltaZ1_1[:(2 * int(rg))]) + np.array(deltaZ2_1[:(2 * int(rg))])
                DELTAYAFT = np.array(deltaY1_1[:(2 * int(rg))]) + np.array(deltaY2_1[:(2 * int(rg))])

        #         DELTALBAFT = dlb_1
                EMODULEAFT = [e1_1,e2_1]

            # LASHING FOR 20s AFT (pattern 2 in_out)
            # external rows, lashing from hatchcover, wind applied from hatchcover center towards seaside
            if lash_bridge_aft == 32.689 and row['ROW'] in exterior_rows_per_bay[row['BAY']]:
                KLHHAFT = KLHHAFT_2014 = np.array(Klhh1_2[:(2 * int(rg))]) + np.array(Klhh2_2[:(2 * int(rg))])
                KLVHAFT = KLVHAFT_2014 = np.array(Klvh1_2[:(2 * int(rg))]) + np.array(Klvh2_2[:(2 * int(rg))])
                KLVVAFT = np.array(Klvv1_2[:(2 * int(rg))]) + np.array(Klvv2_2[:(2 * int(rg))])
                LLENGTHAFT = np.array(llength1_2[:(2 * int(rg))]) + np.array(llength2_2[:(2 * int(rg))])
                DELTAZAFT = np.array(deltaZ1_2[:(2 * int(rg))]) + np.array(deltaZ2_2[:(2 * int(rg))])
                DELTAYAFT = np.array(deltaY1_2[:(2 * int(rg))]) + np.array(deltaY2_2[:(2 * int(rg))])

        #         DELTALBAFT = dlb_2
                EMODULEAFT = [e1_2,e2_2]

            # LASHING FOR 20s AFT (pattern 2 out_in)
            # external rows, lashing from hatchcover, wind applied from seaside towards hatchcover center
            if lash_bridge_aft == 32.689 and row['ROW'] in exterior_rows_per_bay[row['BAY']] and row['Bay Type'] == 'bay20' and lashing_dict['wind_side']=='out_in':
                KLHHAFT = KLHHAFT_2014 = np.array(Klhh1_0[:(2 * int(rg))]) + np.array(Klhh2_0[:(2 * int(rg))]) + np.array(Klhh3_0[:(2 * int(rg))])
                KLVHAFT = KLVHAFT_2014 = np.array(Klvh1_0[:(2 * int(rg))]) + np.array(Klvh2_0[:(2 * int(rg))]) + np.array(Klvh3_0[:(2 * int(rg))])
                KLVVAFT = np.array(Klvv1_0[:(2 * int(rg))]) + np.array(Klvv2_0[:(2 * int(rg))]) + np.array(Klvv3_0[:(2 * int(rg))])
                LLENGTHAFT = np.array(llength1_0[:(2 * int(rg))]) + np.array(llength2_0[:(2 * int(rg))]) + np.array(llength3_0[:(2 * int(rg))])
                DELTAZAFT = np.array(deltaZ1_0[:(2 * int(rg))]) + np.array(deltaZ2_0[:(2 * int(rg))]) + np.array(deltaZ3_0[:(2 * int(rg))])
                DELTAYAFT = np.array(deltaY1_0[:(2 * int(rg))]) + np.array(deltaY2_0[:(2 * int(rg))]) + np.array(deltaY3_0[:(2 * int(rg))])

                #         DELTALBAFT = dlb_3
                EMODULEAFT = [e1_0,e2_0,e3_0]

            # LASHING FOR 20s AFT (pattern 5)
            # internal rows, lashing from lashing bridge(from 2nd tier).For external rows wind applied from hatchcover center towards seaside.
            if lash_bridge_aft == 35.2 and row['Bay Type'] == 'bay20':
                KLHHAFT = KLHHAFT_2014 = np.array(Klhh1_3[:(2 * int(rg))]) + np.array(Klhh2_3[:(2 * int(rg))])
                KLVHAFT = KLVHAFT_2014 = np.array(Klvh1_3[:(2 * int(rg))]) + np.array(Klvh2_3[:(2 * int(rg))])
                KLVVAFT = np.array(Klvv1_3[:(2 * int(rg))]) + np.array(Klvv2_3[:(2 * int(rg))])
                LLENGTHAFT = np.array(llength1_3[:(2 * int(rg))]) + np.array(llength2_3[:(2 * int(rg))])
                DELTAZAFT = np.array(deltaZ1_3[:(2 * int(rg))]) + np.array(deltaZ2_3[:(2 * int(rg))])
                DELTAYAFT = np.array(deltaY1_3[:(2 * int(rg))]) + np.array(deltaY2_3[:(2 * int(rg))])

        #         DELTALBFT = dlb_3
                EMODULEAFT = [e1_3]
            # LASHING FOR 20s AFT (pattern 6)
            # external rows, lashing from lashing bridge(from 2nd tier).For external rows wind applied from seaside to hatch cover centre.
            if lash_bridge_aft == 35.2 and row['Bay Type'] == 'bay20' and row['ROW'] in exterior_rows_per_bay[row['BAY']] and lashing_dict['wind_side']=='out_in':
                KLHHAFT = KLHHAFT_2014 = np.array(Klhh1_3[:(2 * int(rg))]) + np.array(Klhh2_8[:(2 * int(rg))])
                KLVHAFT = KLVHAFT_2014 = np.array(Klvh1_3[:(2 * int(rg))]) + np.array(Klvh2_8[:(2 * int(rg))])
                KLVVAFT = np.array(Klvv1_3[:(2 * int(rg))]) + np.array(Klvv2_8[:(2 * int(rg))])
                LLENGTHAFT = np.array(llength1_3[:(2 * int(rg))]) + np.array(llength2_8[:(2 * int(rg))])
                DELTAZAFT = np.array(deltaZ1_3[:(2 * int(rg))]) + np.array(deltaZ2_8[:(2 * int(rg))])
                DELTAYAFT = np.array(deltaY1_3[:(2 * int(rg))]) + np.array(deltaY2_8[:(2 * int(rg))])

                EMODULEAFT = [e1_3, e2_8]

            # LASHING FOR 20s AFT (pattern 5)
            # internal rows, lashing from lashing bridge(from 3rd tier).For external rows wind applied from hatchcover center towards seaside.
            if lash_bridge_aft == 37.8 and row['Bay Type'] == 'bay20':
                KLHHAFT = KLHHAFT_2014 = np.array(Klhh1_4[:(2 * int(rg))]) + np.array(Klhh2_4[:(2 * int(rg))])
                KLVHAFT = KLVHAFT_2014 =  np.array(Klvh1_4[:(2 * int(rg))]) + np.array(Klvh2_4[:(2 * int(rg))])
                KLVVAFT = np.array(Klvv1_4[:(2 * int(rg))]) + np.array(Klvv2_4[:(2 * int(rg))])
                LLENGTHAFT = np.array(llength1_4[:(2 * int(rg))]) + np.array(llength2_4[:(2 * int(rg))])
                DELTAZAFT = np.array(deltaZ1_4[:(2 * int(rg))]) + np.array(deltaZ2_4[:(2 * int(rg))])
                DELTAYAFT = np.array(deltaY1_4[:(2 * int(rg))]) + np.array(deltaY2_4[:(2 * int(rg))])

        #         DELTALBAFT = dlb_4
                EMODULEAFT = [e1_4,e2_4]

                # LASHING FOR 20s AFT (pattern 6)
                # external rows, lashing from lashing bridge(from 3rd tier).For external rows wind applied from seaside to hatch cover centre.
            if lash_bridge_aft == 37.8 and row['Bay Type'] == 'bay20' and row['ROW'] in exterior_rows_per_bay[row['BAY']] and lashing_dict['wind_side']=='out_in':
                KLHHAFT = KLHHAFT_2014 = np.array(Klhh1_4[:(2 * int(rg))]) + np.array(Klhh2_9[:(2 * int(rg))])
                KLVHAFT = KLVHAFT_2014 = np.array(Klvh1_4[:(2 * int(rg))]) + np.array(Klvh2_9[:(2 * int(rg))])
                KLVVAFT = np.array(Klvv1_4[:(2 * int(rg))]) + np.array(Klvv2_9[:(2 * int(rg))])
                LLENGTHAFT = np.array(llength1_4[:(2 * int(rg))]) + np.array(llength2_9[:(2 * int(rg))])
                DELTAZAFT = np.array(deltaZ1_4[:(2 * int(rg))]) + np.array(deltaZ2_9[:(2 * int(rg))])
                DELTAYAFT = np.array(deltaY1_4[:(2 * int(rg))]) + np.array(deltaY2_9[:(2 * int(rg))])

                #         DELTALBFORE = dlb_4
                EMODULEAFT = [e1_4, e2_9]

            # LASHING FOR 40s (2018) AFT
            # internal rows, lashing from lashing bridge(from 2nd tier). For external rows wind applied from hatchcover center towards seaside.
            if lash_bridge_aft == 35.2 and row['Bay Type'] == 'bay40':
                KLHHAFT = KLHHAFT_2014 = np.array(Klhh1_5[:(2 * int(rg))]) + np.array(Klhh2_5[:(2 * int(rg))])
                KLVHAFT = KLVHAFT_2014 = np.array(Klvh1_5[:(2 * int(rg))]) + np.array(Klvh2_5[:(2 * int(rg))])
                KLVVAFT = np.array(Klvv1_5[:(2 * int(rg))]) + np.array(Klvv2_5[:(2 * int(rg))])
                LLENGTHAFT = np.array(llength1_5[:(2 * int(rg))]) + np.array(llength2_5[:(2 * int(rg))])
                DELTAZAFT = np.array(deltaZ1_5[:(2 * int(rg))]) + np.array(deltaZ2_5[:(2 * int(rg))])
                DELTAYAFT = np.array(deltaY1_5[:(2 * int(rg))]) + np.array(deltaY2_5[:(2 * int(rg))])

        #        DELTALBAFT = dlb_5
                EMODULEAFT = [e1_5,e2_5]

            # LASHING FOR 40s (2018) AFT (pattern 1)
            # external rows, lashing from lashing bridge(from 2nd tier). For external rows wind applied from seaside to hatch cover centre.
            if lash_bridge_aft == 35.2 and row['Bay Type'] == 'bay40' and row['ROW'] in exterior_rows_per_bay[row['BAY']] and lashing_dict['wind_side']=='out_in':
                KLHHAFT = KLHHAFT_2014 =  np.array(Klhh1_5[:(2 * int(rg))]) + np.array(Klhh2_5[:(2 * int(rg))]) + np.array(Klhh2_8[:(2 * int(rg))]) + np.array(Klhh3_10[:(2 * int(rg))])
                KLVHAFT = KLVHAFT_2014 = np.array(Klvh1_5[:(2 * int(rg))]) + np.array(Klvh2_5[:(2 * int(rg))]) + np.array(Klvh2_8[:(2 * int(rg))]) + np.array(Klvh3_10[:(2 * int(rg))])
                KLVVAFT = np.array(Klvv1_5[:(2 * int(rg))]) + np.array(Klvv2_5[:(2 * int(rg))]) + np.array(Klvv2_8[:(2 * int(rg))]) + np.array(Klvv3_10[:(2 * int(rg))])
                LLENGTHAFT = np.array(llength1_5[:(2 * int(rg))]) + np.array(llength2_5[:(2 * int(rg))]) + np.array(llength2_8[:(2 * int(rg))]) + np.array(llength3_10[:(2 * int(rg))])
                DELTAZAFT = np.array(deltaZ1_5[:(2 * int(rg))]) + np.array(deltaZ2_5[:(2 * int(rg))]) + np.array(deltaZ2_8[:(2 * int(rg))]) + np.array(deltaZ3_10[:(2 * int(rg))])
                DELTAYAFT = np.array(deltaY1_5[:(2 * int(rg))]) + np.array(deltaY2_5[:(2 * int(rg))]) + np.array(deltaY2_8[:(2 * int(rg))]) + np.array(deltaY3_10[:(2 * int(rg))])

                #         DELTALBFORE = dlb_5
                EMODULEAFT = [e1_5, e2_5, e2_8, e3_10]


            # LASHING FOR 40s (2014) AFT
            # internal rows, lashing from lashing bridge(from 2nd tier). For external rows wind applied from hatchcover center towards seaside.
            if lash_bridge_aft == 35.2 and row['Bay Type'] == 'bay40' and lashing_dict['BV_rules']=='2014':
                KLHHAFT = KLHHAFT_2014 =  np.array(Klhh1_5_2014[:(2 * int(rg))]) + np.array(Klhh2_5_2014[:(2 * int(rg))])
                KLVHAFT = KLVHAFT_2014 = np.array(Klvh1_5_2014[:(2 * int(rg))]) + np.array(Klvh2_5_2014[:(2 * int(rg))])
                KLVVAFT = np.array(Klvv1_5[:(2 * int(rg))]) + np.array(Klvv2_5[:(2 * int(rg))])
                LLENGTHAFT = np.array(llength1_5[:(2 * int(rg))]) + np.array(llength2_5[:(2 * int(rg))])
                DELTAZAFT = np.array(deltaZ1_5[:(2 * int(rg))]) + np.array(deltaZ2_5[:(2 * int(rg))])
                DELTAYAFT = np.array(deltaY1_5[:(2 * int(rg))]) + np.array(deltaY2_5[:(2 * int(rg))])

                #        DELTALBAFT = dlb_5
                EMODULEAFT = [e1_5, e2_5]

            # LASHING FOR 40s (2018) AFT
            # internal rows, lashing from lashing bridge(from 3rd tier). For external rows wind applied from hatchcover center towards seaside.
            if lash_bridge_aft == 37.8 and row['Bay Type'] == 'bay40':
                KLHHAFT = KLHHAFT_2014 = np.array(Klhh1_6[:(2 * int(rg))]) + np.array(Klhh2_6[:(2 * int(rg))])
                KLVHAFT = KLVHAFT_2014 = np.array(Klvh1_6[:(2 * int(rg))]) + np.array(Klvh2_6[:(2 * int(rg))])
                KLVVAFT = np.array(Klvv1_6[:(2 * int(rg))]) + np.array(Klvv2_6[:(2 * int(rg))])
                LLENGTHAFT = np.array(llength1_6[:(2 * int(rg))]) + np.array(llength2_6[:(2 * int(rg))])
                DELTAZAFT = np.array(deltaZ1_6[:(2 * int(rg))]) + np.array(deltaZ2_6[:(2 * int(rg))])
                DELTAYAFT = np.array(deltaY1_6[:(2 * int(rg))]) + np.array(deltaY2_6[:(2 * int(rg))])

        #         DELTALBAFT = dlb_6
                EMODULEAFT = [e1_6,e2_6]

            # LASHING FOR 40s (2018) AFT (pattern 1)
            # external rows, lashing from lashing bridge(from 3rd tier). For external rows wind applied from seaside to hatch cover centre.

            if lash_bridge_aft == 37.8 and row['Bay Type'] == 'bay40' and row['ROW'] in exterior_rows_per_bay[row['BAY']] and lashing_dict['wind_side']=='out_in':
                KLHHAFT = KLHHAFT_2014 = np.array(Klhh1_6[:(2 * int(rg))]) + np.array(Klhh2_6[:(2 * int(rg))]) + np.array(Klhh2_9[:(2 * int(rg))]) + np.array(Klhh3_11[:(2 * int(rg))])
                KLVHAFT = KLVHAFT_2014 = np.array(Klvh1_6[:(2 * int(rg))]) + np.array(Klvh2_6[:(2 * int(rg))]) + np.array(Klvh2_9[:(2 * int(rg))]) + np.array(Klvh3_11[:(2 * int(rg))])
                KLVVAFT = np.array(Klvv1_6[:(2 * int(rg))]) + np.array(Klvv2_6[:(2 * int(rg))]) + np.array(Klvv2_9[:(2 * int(rg))]) + np.array(Klvv3_11[:(2 * int(rg))])
                LLENGTHAFT = np.array(llength1_6[:(2 * int(rg))]) + np.array(llength2_6[:(2 * int(rg))]) + np.array(llength2_9[:(2 * int(rg))]) + np.array(llength3_11[:(2 * int(rg))])
                DELTAZAFT = np.array(deltaZ1_6[:(2 * int(rg))]) + np.array(deltaZ2_6[:(2 * int(rg))]) + np.array(deltaZ2_9[:(2 * int(rg))]) + np.array(deltaZ3_11[:(2 * int(rg))])
                DELTAYAFT = np.array(deltaY1_6[:(2 * int(rg))]) + np.array(deltaY2_6[:(2 * int(rg))]) + np.array(deltaY2_9[:(2 * int(rg))]) + np.array(deltaY3_11[:(2 * int(rg))])

                EMODULEAFT = [e1_6, e2_6, e2_9, e3_11]

            # LASHING FOR 40s (2014) AFT
            # internal rows, lashing from lashing bridge(from 3rd tier).For external rows wind applied from hatchcover center towards seaside.
            if lash_bridge_aft == 37.8 and row['Bay Type'] == 'bay40' and row['ROW'] not in exterior_rows_per_bay[row['BAY']]  and lashing_dict['BV_rules']=='2014':
                KLHHAFT = KLHHAFT_2014 = np.array(Klhh1_6_2014[:(2 * int(rg))]) + np.array(Klhh2_6_2014[:(2 * int(rg))])
                KLVHAFT = KLVHAFT_2014 = np.array(Klvh1_6_2014[:(2 * int(rg))]) + np.array(Klvh2_6_2014[:(2 * int(rg))])
                KLVVAFT = np.array(Klvv1_6[:(2 * int(rg))]) + np.array(Klvv2_6[:(2 * int(rg))])
                LLENGTHAFT = np.array(llength1_6[:(2 * int(rg))]) + np.array(llength2_6[:(2 * int(rg))])
                DELTAZAFT = np.array(deltaZ1_6[:(2 * int(rg))]) + np.array(deltaZ2_6[:(2 * int(rg))])
                DELTAYAFT = np.array(deltaY1_6[:(2 * int(rg))]) + np.array(deltaY2_6[:(2 * int(rg))])

                #         DELTALBAFT = dlb_6
                EMODULEAFT = [e1_6, e2_6]

            # LASHING FOR 40s (2018) AFT
            # internal rows, lashing from lashing bridge(from 4th tier).For external rows wind applied from hatchcover center towards seaside.
            if lash_bridge_aft == 38.1 and row['Bay Type'] == 'bay40':
                KLHHAFT = KLHHAFT_2014 = np.array(Klhh1_7[:(2 * int(rg))]) + np.array(Klhh2_7[:(2 * int(rg))])
                KLVHAFT = KLVHAFT_2014 = np.array(Klvh1_7[:(2 * int(rg))]) + np.array(Klvh2_7[:(2 * int(rg))])
                KLVVAFT = np.array(Klvv1_7[:(2 * int(rg))]) + np.array(Klvv2_7[:(2 * int(rg))])
                LLENGTHAFT = np.array(llength1_7[:(2 * int(rg))]) + np.array(llength2_7[:(2 * int(rg))])
                DELTAZAFT = np.array(deltaZ1_7[:(2 * int(rg))]) + np.array(deltaZ2_7)[:(2 * int(rg))]
                DELTAYAFT = np.array(deltaY1_7[:(2 * int(rg))]) + np.array(deltaY2_7[:(2 * int(rg))])

        #         DELTALBAFT = dlb_7
                EMODULEAFT = [e1_7,e2_7]
            # LASHING FOR 40s (2014) AFT
            # internal rows, lashing from lashing bridge(from 4th tier).For external rows wind applied from hatchcover center towards seaside.
            if lash_bridge_aft == 38.1 and row['Bay Type'] == 'bay40' and lashing_dict['BV_rules']=='2014':
                KLHHAFT = KLHHAFT_2014 = np.array(Klhh1_7_2014[:(2 * int(rg))]) + np.array(Klhh2_7_2014[:(2 * int(rg))])
                KLVHAFT = KLVHAFT_2014 = np.array(Klvh1_7_2014[:(2 * int(rg))]) + np.array(Klvh2_7_2014[:(2 * int(rg))])
                KLVVAFT = np.array(Klvv1_7[:(2 * int(rg))]) + np.array(Klvv2_7[:(2 * int(rg))])
                LLENGTHAFT = np.array(llength1_7[:(2 * int(rg))]) + np.array(llength2_7[:(2 * int(rg))])
                DELTAZAFT = np.array(deltaZ1_7[:(2 * int(rg))]) + np.array(deltaZ2_7[:(2 * int(rg))])
                DELTAYAFT = np.array(deltaY1_7[:(2 * int(rg))]) + np.array(deltaY2_7[:(2 * int(rg))])

                #         DELTALBAFT = dlb_7
                EMODULEAFT = [e1_7, e2_7]


            DELTALBFORE = DELTALBAFT = dlb_1[:(2 * int(rg))]

            # LASHING FOR 40s (2018) AFT (pattern 1)
            # external rows, lashing from lashing bridge(from 4th tier). For external rows wind applied from seaside to hatch cover centre.

            if lash_bridge_aft == 38.1 and row['Bay Type'] == 'bay40' and row['ROW'] in exterior_rows_per_bay[row['BAY']] and lashing_dict['wind_side']=='out_in':
                KLHHAFT = np.array(Klhh3_14[:(2 * int(rg))]) + np.array(Klhh3_15[:(2 * int(rg))]) + np.array(Klhh3_12[:(2 * int(rg))]) + np.array(Klhh3_13[:(2 * int(rg))])
                KLVHAFT = np.array(Klvh3_14[:(2 * int(rg))]) + np.array(Klvh3_15[:(2 * int(rg))]) + np.array(Klvh3_12[:(2 * int(rg))]) + np.array(Klvh3_13[:(2 * int(rg))])
                KLVVAFT = np.array(Klvv3_14[:(2 * int(rg))]) + np.array(Klvv3_15[:(2 * int(rg))]) + np.array(Klvv3_12[:(2 * int(rg))]) + np.array(Klvv3_13[:(2 * int(rg))])
                LLENGTHAFT = np.array(llength3_14[:(2 * int(rg))]) + np.array(llength3_15[:(2 * int(rg))]) + np.array(llength3_12[:(2 * int(rg))]) + np.array(llength3_13[:(2 * int(rg))])
                DELTAZAFT = np.array(deltaZ3_14[:(2 * int(rg))]) + np.array(deltaZ3_15[:(2 * int(rg))]) + np.array(deltaZ3_12[:(2 * int(rg))]) + np.array(deltaZ3_13[:(2 * int(rg))])
                DELTAYAFT = np.array(deltaY3_14[:(2 * int(rg))]) + np.array(deltaY3_15[:(2 * int(rg))]) + np.array(deltaY3_12[:(2 * int(rg))]) + np.array(deltaY3_13[:(2 * int(rg))])
                #         DELTALBFORE = dlb_7
                EMODULEAFT = [e3_14, e3_15, e3_12, e3_13]

            # Computing forces on each axis, at each level, individually
            # Vertical forces, right corner, upright, bottom to top
            FZR1 = np.array(FZR1) * (-1)
            # Vertical forces, right corner, inclined, bottom to top
            FZR2 = np.array(FZR2) * (-1)
            # Vertical forces, left corner, upright, bottom to top
            FZL1 = np.array(FZL1) * (-1)
            # Vertical forces, left corner, inclined, bottom to top
            FZL2 = np.array(FZL2) * (-1)
            
            # Longitudinal forces, top to bottom
            FXFORCES = []

            # Transversal forces, top to bottom
            FYFORCES = []
            FYold = 0
            FXold = 0
            for k in range(int(rg) - 1, -1, -1):
                if k == int(rg) - 1:
                    FY = (FY2I[k] + FY2IM1[k]) / 2
                    FYold = FY + FYold

                    FX = (FWXI[k]) / 4
                    FXold = FX + FXold

                else:
                    FY = (FY2I[k] + FY2IM1[k]) / 2 + 2 * FYold
                    FYold = (FY2I[k] + FY2IM1[k]) / 2 + FYold

                    FX = (FWXI[k]) / 4 + 2 * FXold
                    FXold = (FWXI[k]) / 4 + FXold

                FXFORCES.append(FX)
                FYFORCES.append(FY)
                
            VALS = []
            ROW = []
            MATRIX = []
            for rws in range(2 * int(rg)):
                for cols in range(2 * int(rg)):
                    if rws % 2 == 0:
                        if rws == cols:
                            val = 1
                        elif cols == rws - 1:
                            val = -1
                        else:
                            val = 0
                    elif rws % 2 != 0:
                        if cols == rws - 1:
                            val = -1 * lashing_dict['Krifront']
                        elif cols == rws:
                            val = lashing_dict['Krifront']
                        elif cols > rws:
                            val = KLHHFORE[cols]
                            if lashing_dict['BV_rules']=='2014' and row['ROW'] not in exterior_rows_per_bay[row['BAY']]:
                                val = KLHHFORE_2014[cols]
                        else:
                            val = 0

                    VALS.append(val)
                if cols == 2 * rg - 1:
                    ROW = [VALS[s] for s in range(rws * (cols + 1), ((rws + 1) * (cols + 1)))]
                    MATRIX.append(ROW)

            RESULTS = []
            for rws in range(2 * int(rg), 0, -1):
                if rws % 2 == 0 or rws == 2 * int(rg):
                    val = lashing_dict['twistlockhorizgap']
                else:
                    val = -FYFORCES[int((rws) / 2)]

                RESULTS.append(val)
            
            VALSAFT = []
            ROWAFT = []
            MATRIXAFT = []
            for rws in range(2 * int(rg)):
                for cols in range(2 * int(rg)):
                    if rws % 2 == 0:
                        if rws == cols:
                            val = 1
                        elif cols == rws - 1:
                            val = -1
                        else:
                            val = 0
                    elif rws % 2 != 0:
                        if cols == rws - 1:
                            val = -1 * lashing_dict['Kridoor']
                        elif cols == rws:
                            val = lashing_dict['Kridoor']
                        elif cols > rws:
                            val = KLHHAFT[cols]
                            if lashing_dict['BV_rules']=='2014':
                                val = KLHHAFT_2014[cols]
                        else:
                            val = 0

                    VALSAFT.append(val)
                if cols == 2 * rg - 1:
                    ROWAFT = [VALSAFT[s] for s in range(rws * (cols + 1), ((rws + 1) * (cols + 1)))]
                    MATRIXAFT.append(ROWAFT)
            RESULTSAFT = []

            for rws in range(2 * int(rg), 0, -1):
                if rws % 2 == 0 or rws == 2 * int(rg):
                    val = lashing_dict['twistlockhorizgap']
                else:

                    val = -FYFORCES[int((rws) / 2)]

                RESULTSAFT.append(val)

            # Deformations in mm, front - bottom to top
            d = np.linalg.solve(MATRIX, RESULTS)

            # Deformations in mm, aft - bottom to top
            daft = np.linalg.solve(MATRIXAFT, RESULTSAFT)
            # Horizontal lashing reactions in KN, front - bottom to top
            R = -np.array(KLHHFORE) * (np.array(d) - np.array(DELTALBFORE))
            #R = -(numpy.array(KLHHFORE) * numpy.array(d) + numpy.array(KLVHFORE)*numpy.array(d))
            # Horizontal lashing reactions in KN, aft - bottom to top
            RAFT = -np.array(KLHHAFT) * (np.array(daft) - np.array(DELTALBAFT))
            #RAFT = -(numpy.array(KLHHAFT) * numpy.array(daft) + numpy.array(KLVHAFT)*numpy.array(daft))

            # Vertical lashing reactions in KN, front - bottom to top
            V = -np.array(KLVHFORE) * (np.array(d) - np.array(DELTALBFORE))
            if lashing_dict['BV_rules']=='2014':
                V = -np.array(KLVHFORE_2014) * (np.array(d) - np.array(DELTALBFORE))
            #V = -(-numpy.array(KLVVFORE) * numpy.array(d) + numpy.array(KLVHFORE)*numpy.array(d))
            # Vertical lashing reactions in KN, aft - bottom to top
            
            VAFT = -np.array(KLVHAFT) * (np.array(daft) - np.array(DELTALBAFT))
            if lashing_dict['BV_rules'] == '2014':
                VAFT = -np.array(KLVHAFT_2014) * (np.array(daft) - np.array(DELTALBAFT))
            #VAFT = -(-numpy.array(KLVVAFT) * numpy.array(daft) + numpy.array(KLVHAFT) * numpy.array(daft))
            # Total lashing reactions in KN, front - bottom to top
            L = []
            for k in range(2 * int(rg)):
                if LLENGTHFORE[k] != 0:
                    vall = (np.array(R[k]) * np.array(DELTAYFORE[k]) + np.array(V[k]) * np.array(DELTAZFORE[k])) / np.array(LLENGTHFORE[k])
                else:
                    vall = 0
                L.append(vall)
            # Total lashing reactions in KN, aft - bottom to top
            LAFT = []
            for k in range(2 * int(rg)):
                if LLENGTHAFT[k] != 0:
                    vall = (np.array(RAFT[k]) * np.array(DELTAYAFT[k]) + np.array(VAFT[k]) * np.array(DELTAZAFT[k])) / np.array(LLENGTHAFT[k])
                else:
                    vall = 0
                LAFT.append(vall)
                
            # Container racking forces in KN, fore - bottom to top

            T2i = []
            for k in range(2 * int(rg)):
                if k != 0:
                    vall = -1 * lashing_dict['Krifront'] * (d[k] - d[k - 1])
                else:
                    vall = 0
                T2i.append(vall)

            # Container racking forces in KN, aft - bottom to top

            T2iAFT = []
            for k in range(2 * int(rg)):
                if k != 0:
                    vall = -1 * lashing_dict['Kridoor'] * (daft[k] - daft[k - 1])
                else:
                    vall = 0
                T2iAFT.append(vall)

            #   Twistlock reactions (shear forces between containers) in KN, fore - top to bottom
            #   Reversing FYFORCES to be bottom to top)
            FYFORCES = FYFORCES[::-1]
            T2im1 = []
            vallold = 0
            for k in range(2 * int(rg)-1,0,-1):
                if k % 2 == 0:
                    vall = -((FYFORCES[int((k) / 2)]) + R[k]) -(0 + R[k+1])+ vallold
                    vallold = vall
                else:
                    vall = 0
                    vallold=0
                T2im1.append(vall)

            # Twistlock reactions (shear forces between containers) in KN, aft - top to bottom

            T2im1AFT = []
            vallold=0
            for k in range(2 * int(rg)-1,0,-1):
                if k % 2 == 0:
                    vall = -((FYFORCES[int(k / 2)]) + RAFT[k]) - (0 + RAFT[k+1]) + vallold
                    vallold=vall
                else:
                    vall=0
                    vallold=0
                T2im1AFT.append(vall)
            # Vertical Forces acting on container corners fore

            PLUP2i, PRUP2i, PLUP2iM1, PRUP2iM1, PLINCL2i, PRINCL2i, PLINCL2iM1, PRINCL2iM1= [[] for i in range(8)] 
            Plup2im1 = Prup2im1 = Plincl2im1 = Princl2im1 = Plup2i = Prup2i = Plincl2i = Princl2i = 0

            #Reversing lengths and deltay to be top to bottom
            #LLENGTHAFT=LLENGTHAFT[::-1]
            #LLENGTHFORE=LLENGTHFORE[::-1]
            #DELTAYFORE=DELTAYFORE[::-1]
            a = 'na'
            for k in range(2 * int(rg), 0, -1):
                prev_a=a
                a = (90 - math.acos(DELTAYFORE[k - 1] / LLENGTHFORE[k - 1]) * 180 / math.pi if LLENGTHFORE[k - 1] > 0 else 'na')
                tc = (1 if a == 0 else 0)
                # top level of container - top container to bottom container
                if k % 2 == 0:
                    Plup2i = tc*V[k-1] + Plup2im1
                    Prup2i = -(1-tc)*V[k-1] + Prup2im1

        #           Plincl2i = V[k-1] + Plincl2im1
        #           Princl2i = Princl2im1

                # bottom level of container - top container to bottom container
                elif k % 2 != 0:
                    Plup2im1 = Plup2i + tc*V[k-1] + FZL1[int((k-1) / 2)] + (T2i[k] * heights[int((k) / 2)]) / width
        #            Plincl2im1 = Plincl2i + V[k-1]*0 + FZL2[int((k - 1) / 2)] + (T2i[k] * heights[int((k) / 2)] / width)
                    Prup2im1 = Prup2i - (1-tc)*V[k-1] + FZL1[int((k - 1) / 2)] - (T2i[k] * heights[int((k) / 2)] / width)
        #            Princl2im1 = Princl2i + FZR2[int((k - 1) / 2)] - T2i[k - 1] * heights[int((k - 1) / 2)] / width
                    if prev_a==0:
                        L[k]=Plup2im1
                        Plup2im1=0

                PLUP2i.append(Plup2i)
                PRUP2i.append(Prup2i)
                PLUP2iM1.append(Plup2im1)
                PRUP2iM1.append(Prup2im1)
                PLINCL2i.append(Plincl2i)
                PRINCL2i.append(Princl2i)
                PLINCL2iM1.append(Plincl2im1)
                PRINCL2iM1.append(Princl2im1)

                # Vertical Forces acting on container corners aft

                PLUP2iAFT, PRUP2iAFT, PLUP2iM1AFT, PRUP2iM1AFT, PLINCL2iAFT, PRINCL2iAFT, PLINCL2iM1AFT, PRINCL2iM1AFT = [[] for i in range(8)] 
                Plup2im1aft = Prup2im1aft = Plincl2im1aft = Princl2im1aft = Plup2iaft = Prup2iaft = Plincl2iaft = Princl2iaft = 0

            a=='na'
            for k in range(2 * int(rg), 0, -1):
                prev_a=a
                a = (90 - math.acos(DELTAYAFT[k - 1] / LLENGTHAFT[k - 1]) * 180 / math.pi if LLENGTHAFT[k - 1] > 0 else 'na')
                tc = (1 if a == 0 else 0)

                # top level of container
                if k % 2 == 0:

                    Plup2iaft = tc*VAFT[k-1] + Plup2im1aft
                    Prup2iaft = -(1-tc)*VAFT[k-1] + Prup2im1aft

        #            Plincl2iaft = VAFT[k-1] + Plincl2im1aft
        #            Princl2iaft = Princl2im1aft
                # bottom level of container
                elif k % 2 != 0 and k < 2 * int(rg) + 1:
                    Plup2im1aft = Plup2iaft + tc*VAFT[k-1] + FZL1[int((k - 1) / 2)] + (T2iAFT[k] * heights[int((k) / 2)] / width)
        #         Plincl2im1aft = Plincl2iaft + VAFT[k-1] + FZL2[int((k - 1) / 2)] + (T2iAFT[k] * heights[int((k - 1) / 2)] / width)
        #           Formula bellow is different from BV, but results match MACS3. FZR1 should be added i/O FZL1
                    Prup2im1aft = Prup2iaft - (1-tc)*VAFT[k-1] + FZL1[int((k - 1) / 2)] - (T2iAFT[k] * heights[int((k) / 2)] / width)
        #         Princl2im1aft = Princl2iaft + FZR2[int((k - 1) / 2)] - T2iAFT[k - 1] * heights[int((k - 1) / 2)] / width
                if prev_a==0:
                    LAFT[k]=Plup2im1aft
                    Plup2im1aft=0

                PLUP2iAFT.append(Plup2iaft)
                PRUP2iAFT.append(Prup2iaft)
                PLUP2iM1AFT.append(Plup2im1aft)
                PRUP2iM1AFT.append(Prup2im1aft)
                PLINCL2iAFT.append(Plincl2iaft)
                PRINCL2iAFT.append(Princl2iaft)
                PLINCL2iM1AFT.append(Plincl2im1aft)
                PRINCL2iM1AFT.append(Princl2im1aft)

                # Reverse V,T2i,and vertical forces: to be top to bottom
            V = V[::-1]
            VAFT = VAFT[::-1]
            T2i = T2i[::-1]
            T2iAFT = T2iAFT[::-1]
            #T2im1 = T2im1[::-1]
            #T2im1AFT = T2im1AFT[::-1]
            FZR1 = FZR1[::-1]
            FZR2 = FZR2[::-1]
            FZL1 = FZL1[::-1]
            FZL2 = FZL2[::-1]
            FYFORCES = FYFORCES[::-1]

            masses = masses[::-1]
            heights = heights[::-1]
            LAFT = LAFT[::-1]
            L = L[::-1]
            d = d[::-1]
            daft = daft[::-1]
            FXWINDI=FXWINDI[::-1]
            FYWINDI=FYWINDI[::-1]
            LLENGTHAFT=LLENGTHAFT[::-1]
            LLENGTHFORE=LLENGTHFORE[::-1]
            RAFT=RAFT[::-1]
            R=R[::-1]
            KLHHFORE=KLHHFORE[::-1]
            KLHHAFT=KLHHAFT[::-1]
            EMODULEAFT=EMODULEAFT[::-1]
            EMODULEFORE=EMODULEFORE[::-1]

            VCG_list.append(VCGtier_list)
            lash_bridge_aft_list.append(lash_bridge_aft)
            lash_bridge_fore_list.append(lash_bridge_fore)
            longitudinal_wind_forces_list.append([*np.around(np.array(FXWINDI), 2)])
            traversal_wind_forces_list.append([*np.around(np.array(FYWINDI), 2)])
            roll_acc_horiz_list.append(AY)
            roll_acc_vert_list.append(AZ2 + 9.81)
            pitch_acc_horiz_list.append(lashing_dict['nw1']*ax1)
            pitch_acc_vert_list.append(lashing_dict['nw1']*az1)
            traversal_forces_list.append([*np.around(np.array(FYFORCES), 2)])
            longitudinal_forces_list.append([*np.around(np.array(FXFORCES)/1.4, 2)])
            racking_forces_aft_list.append([*np.around(np.array(T2iAFT), 2)])
            racking_forces_fore_list.append([*np.around(np.array(T2i)/1.4,2)])
            racking_pitch_forces_list.append([*np.around(np.array(FXFORCES)/1.4, 2)])
            deformation_aft_list.append([*np.around(np.array(daft), 2)])
            deformation_fore_list.append([*np.around(np.array(d), 2)])
            emod_aft_list.append([*np.around(np.array(EMODULEAFT), 2)])
            emod_fore_list.append([*np.around(np.array(EMODULEFORE), 2)])
            h_displacement_aft_list.append(DELTALBAFT)
            h_displacement_fore_list.append(DELTALBFORE)
            lashing_length_aft_list.append([*np.around(np.array(LLENGTHAFT), 2)])
            lashing_lenght_fore_list.append([*np.around(np.array(LLENGTHFORE), 2)])
            internal_lashing_forces_aft_list.append([*np.around(np.array(LAFT), 2)])
            internal_lashing_forces_fore_list.append([*np.around(np.array(L), 2)])
            twist_shear_aft_list.append([*np.around(np.array(T2im1AFT), 2)])
            twist_shear_fore_list.append([*np.around(np.array(T2im1), 2)])
            pressure_aft_list.append([*np.around(np.array(PRUP2iM1AFT), 2)])
            pressure_fore_list.append([*np.around(np.array(PRUP2iM1), 2)])
            lifting_aft_list.append([*np.around(np.array(PLUP2iM1AFT),2)])
            lifting_fore_list.append([*np.around(np.array(PLUP2iM1),2)])
        grouped_df['VCG tier (m)'] = VCG_list
        grouped_df['Lashing bridge level aft (m)'] = lash_bridge_aft_list
        grouped_df['Lashing bridge level fore (m)'] = lash_bridge_fore_list
        grouped_df['Windforces, longitudinal (KN), top to bottom'] = longitudinal_wind_forces_list
        grouped_df['Windforces, transversal (KN), top to bottom'] = traversal_wind_forces_list
        grouped_df['Roll acc horiz (m/s2)'] = roll_acc_horiz_list
        grouped_df['Roll acc vert (m/s2)'] = roll_acc_vert_list
        grouped_df['Pitch acc horiz (m/s2)'] = pitch_acc_horiz_list
        grouped_df['Pitch acc vert (m/s2)'] = pitch_acc_vert_list
        grouped_df['Traversal forces (N), top to bottom'] = traversal_forces_list
        grouped_df['Longitudinal forces (N), top to bottom'] = longitudinal_forces_list
        grouped_df['Racking forces (KN<150), aft (N), top to bottom'] = racking_forces_aft_list
        grouped_df['Racking forces (KN<150), fore (N), top to bottom'] = racking_forces_fore_list
        grouped_df['Racking pitch forces (KN<100), top to bottom'] = racking_pitch_forces_list
        grouped_df['Deformation aft (mm), top to bottom'] = deformation_aft_list
        grouped_df['Deformation fore (mm), top to bottom'] = deformation_fore_list
        grouped_df['Emodule aft (N/mm2), top to bottom'] = emod_aft_list
        grouped_df['Emodule fore (N/mm2), top to bottom'] = emod_fore_list
        grouped_df['Horizontal displacement of lashing bridge aft at anchor point, dLB (mm)'] = h_displacement_aft_list
        grouped_df['Horizontal displacement of lashing bridge fore at anchor point, dLB (mm)'] = h_displacement_fore_list
        grouped_df['Lashing length aft (mm), top to bottom'] = lashing_length_aft_list
        grouped_df['Lashing length fore (mm), top to bottom'] = lashing_lenght_fore_list
        grouped_df['Internal lashing forces aft (N), top to bottom'] = internal_lashing_forces_aft_list
        grouped_df['Internal lashing forces fore (N), top to bottom'] = internal_lashing_forces_fore_list
        grouped_df['Twistlock shear forces aft (KN<210), top to bottom'] = twist_shear_aft_list
        grouped_df['Twistlock shear forces fore (KN<210), top to bottom'] = twist_shear_fore_list
        grouped_df['Pressure aft,(KN, < 848 KN), top to bottom'] = pressure_aft_list
        grouped_df['Pressure fore,(KN, < 848 KN), top to bottom'] = pressure_fore_list
        grouped_df['Lifting aft,(KN, < 250 KN), top to bottom'] = lifting_aft_list
        grouped_df['Lifting fore,(KN, < 250 KN), top to bottom'] = lifting_fore_list
        # grouped_df.to_csv('lashing_details_out.csv')
        # CPLEX needed calculations
        grouped_df['Racking Forces Absolute Max(<150 KN)'] = self.__get_absolute_max(grouped_df, ['Racking forces (KN<150), aft (N), top to bottom', 'Racking forces (KN<150), fore (N), top to bottom'])
        grouped_df['Racking Pitch Forces Absolute Max(<100 KN)'] = self.__get_absolute_max(grouped_df, ['Racking pitch forces (KN<100), top to bottom'])
        grouped_df['Internal lashing force Absolute Max'] = self.__get_absolute_max(grouped_df, ['Internal lashing forces aft (N), top to bottom', 'Internal lashing forces fore (N), top to bottom'])
        grouped_df['Twistlock shear forces Absolute Max(<210 KN)'] = self.__get_absolute_max(grouped_df, ['Twistlock shear forces aft (KN<210), top to bottom', 'Twistlock shear forces fore (KN<210), top to bottom'])
        grouped_df['Pressure Absolute Max(<848 KN)'] = self.__get_absolute_max(grouped_df, ['Pressure aft,(KN, < 848 KN), top to bottom', 'Pressure fore,(KN, < 848 KN), top to bottom'])
        grouped_df['Lifting Absolute Max(<250 KN)'] = self.__get_absolute_max(grouped_df, ['Lifting aft,(KN, < 250 KN), top to bottom', 'Lifting fore,(KN, < 250 KN), top to bottom'])
        grouped_df['BAY'] = grouped_df['BAY'].astype(int)
        return grouped_df[['BAY', 'ROW', 'Racking Forces Absolute Max(<150 KN)', 'Racking Pitch Forces Absolute Max(<100 KN)', 'Internal lashing force Absolute Max', 'Twistlock shear forces Absolute Max(<210 KN)', 'Pressure Absolute Max(<848 KN)', 'Lifting Absolute Max(<250 KN)']]
    
    
    def perform_lashing_calculations(self, l_dfs_containers, POD):
        """
        Performs lashing calculations based on the given containers and Port of Discharge (POD).

        Args:
            l_dfs_containers: The containers data used for lashing calculations.
            POD: The Port of Discharge (POD) value.

        Returns:
            The result of the lashing calculations as a DataFrame.

        Example:
            containers_data = ...
            pod_value = "POD_0"
            result_df = self.perform_lashing_calculations(containers_data, pod_value)
            print(result_df)
        """
        # Call the __lashing_calculations function and return the result
        lashing_df = self.__lashing_calculations(l_dfs_containers, POD)
        return lashing_df
