import numpy as np
import logging
import pandas as pd

class Vessel:
    def __init__(self, logger: logging.Logger, speed:float, gm:float, draft:float, vessel_profile:dict, DG_rules:dict, dg_exclusions:pd.DataFrame, fn_stacks:pd.DataFrame):
        self.logger = logger
        self._imo = vessel_profile["imo"]
        self._name = vessel_profile["name"]
        self._v_class = vessel_profile["v_class"]
        self._capacity = vessel_profile["capacity"]
        self._operator = vessel_profile["operator"]
        self._max_speed = vessel_profile["max_speed"]
        self._lara_code = vessel_profile["lara_code"]
        self._model = vessel_profile["model"]
        self._speed = speed
        self._gm = gm
        self._draft = draft
        self._vessel_profile = vessel_profile
        self._DG_rules = DG_rules
        self._dg_exclusions = dg_exclusions
        self._fn_stacks = fn_stacks
        
    def display_info(self):
        print(f"Vessel Name: {self._name}")
        print(f"Vessel Class: {self._v_class}")
        print(f"Capacity: {self._capacity} TEUs")
        print(f"Operator: {self._operator}")
        print(f"Max Speed: {self._max_speed} kts")
        print(f"Lara Code: {self._lara_code}")
        print(f"Model: {self._model}")
        print(f"Speed: {self._speed} kts")
        print(f"Gm: {self._gm} meters")
        print(f"Draft: {self._draft} meters")

    def get_imo(self):
        """
        Retrieves the IMO number of the vessel.

        Returns:
            str: The IMO number of the vessel.
        """
        return self._imo

    def get_speed(self):
        """
        Retrieves the speed of the vessel.

        Returns:
            float: The speed of the vessel.
        """
        return self._speed

    def get_gm(self):
        """
        Retrieves the GM (metacentric height) of the vessel.

        Returns:
            float: The GM of the vessel.
        """
        return self._gm

    def get_draft(self):
        """
        Retrieves the draft of the vessel.

        Returns:
            float: The draft of the vessel.
        """
        return self._draft

    def get_vessel_profile(self):
        """
        Retrieves the vessel profile.

        Returns:
            dict: The vessel profile as a dictionary.
        """
        return self._vessel_profile

    def get_DG_rules(self):
        """
        Retrieves the DG rules.

        Returns:
            dict: The DG rules as a dictionary.
        """
        return self._DG_rules
    
    def get_fn_stacks(self):
        return self._fn_stacks 
    
    def get_DG_exclusions(self):
        return self._dg_exclusions
    
    def get_Fuel_types(self):
        """
        Get the available fuel types for the vessel.

        Returns:
            list: A list of strings representing the available fuel types for the vessel.
        """
        return self._vessel_profile["fuel_types_available"]
    
    def getRowsTiers(self, bay: int) -> set:
        """
        Retrieves the rows and tiers associated with the specified bay.

        Args:
            bay (int): The bay number for which to retrieve the rows and tiers.

        Returns:
            tuple: A tuple containing two sets: the rows and tiers associated with the bay.

        Example:
            vessel = Vessel()
            bay = 3
            rows, tiers = vessel.getRowsTiers(bay)
            print(rows)
            # Output: {0, 1, 2, 3, ..., 20}
            print(tiers)
            # Output: {72, 74, 76, 78, ..., 86}
        """
        rows = set()
        tiers = set()
        for condition in self._vessel_profile["Rows_Tiers_per_Bay"]:
            
            bay_condition = condition.get('bay')
            if bay_condition is None or bay == bay_condition:
                rows.update(range(condition['row_start'], condition['row_end']+1))
                tiers.update(range(condition['tier_start'], condition['tier_end']+1))

        return rows, tiers
    
    def get_max_rows_per_bay(self) -> dict:
        """
        Returns a dictionary mapping each bay to its maximum 2 rows.

        The method retrieves the twenty-foot bays and forty-foot bays from the vessel's profile.
        For each bay, it calculates the rows and tiers using the `getRowsTiers` method.
        It then selects the maximum two rows for each bay and adds them to the `max_rows_per_bay` dictionary.

        Returns:
            dict: A dictionary mapping each bay to its maximum two rows.

        Example:
            vessel = Vessel()
            max_rows_per_bay = vessel.get_max_rows_per_bay()
            print(max_rows_per_bay)
            # Output: {1: [18, 17], 2: [20, 19], 3: [20, 19], ...}
        """
        max_rows_per_bay = {}

        # get tw_bays and fo_bays from vessel_profile
        tw_bays = self._vessel_profile["twenty_foot_bays"]
        fo_bays = self._vessel_profile["fourty_foot_bays"]
            
        for bay in tw_bays + fo_bays:
            rows, tiers = self.getRowsTiers(bay)
            
            # Select the maximum 2 rows
            selected_rows = sorted(rows, reverse=True)[:2]
            
            max_rows_per_bay[bay] = selected_rows
        
        return max_rows_per_bay

    def getTCG(self, bay: int, row: int) -> float:
        """For a given bay and row of a stack, the centroid of the stack is
        extracted based on the vessel properties.

        Args:
            bay (int): Bay value of stack.
            row (int): Row value of stack.

        Returns:
            float: TCG (Transversal Centroid).
        """
        bay_mapping = self._vessel_profile["TCG_bay_mapping"]
        # Change bay name to either "bay??", or "general"
        bay_name = bay_mapping.get(str(int(bay)), "general")
        # Get tcg from vessel_profile["TCG_bay_mapping"]
        tcg = self._vessel_profile["TCG"].get(bay_name, {}).get(str(int(row)), 0.0)

        return tcg
    
    def getTCG_vectorized(self, bay_row_array: np.array) -> np.array:
        """
        Retrieves the TCG (Transversal Centroid) values for an array of bay-row pairs.

        Args:
            bay_row_array (np.array): Array of bay-row pairs.

        Returns:
            np.array: Array of TCG values corresponding to each bay-row pair.
        """    
        bay_mapping = self._vessel_profile["TCG_bay_mapping"]
        bay_names = np.array([bay_mapping.get(str(int(bay)), "general") for bay in bay_row_array[:, 0]])
        rows = bay_row_array[:, 1]
        tcg_values = np.zeros_like(rows, dtype=float)

        for i in range(len(bay_names)):
            bay_name = bay_names[i]
            row = rows[i]
            tcg = self._vessel_profile["TCG"].get(bay_name, {}).get(str(int(row)), 0.0)
            tcg_values[i] = tcg

        return tcg_values
    
    def getLCG(self, bay:int)->float:
        """for a given bay and row of a stack, the centroid of the stack is 
            extracted based on the vessel properties  

        Args:
            bay (int): bay value of stack

        Returns:
            float: LCG -> Transversal Centroid 
        """        
        #get lcg 
        lcg = self._vessel_profile["LCG"].get(str(int(bay)), {})
    
        return lcg
    
    def getLCG_vectorized(self, bay_array:np.array) -> np.array:
        """
        Retrieves the LCG (Longitudinal Centroid) values for an array of bays.

        Args:
            bay_array (np.array): Array of bays.

        Returns:
            np.array: Array of LCG values corresponding to each bay.
        """
        lcg_values = np.array([self._vessel_profile["LCG"].get(str(int(bay)), None) for bay in bay_array[:, 0]])
        
        return lcg_values
    
    def getLBFA(self, bay: int) -> tuple:
        """
        Retrieves the lash_bridge_fore and lash_bridge_aft values for a given bay.

        Args:
            bay (int): The bay number.

        Returns:
            tuple: A tuple containing the lash_bridge_fore and lash_bridge_aft values. If the bay is not found, (0.0, 0.0) is returned.
        """
        bay_data = self._vessel_profile["LBFA"].get(str(int(bay)), {})
        lash_bridge_fore = bay_data.get("lash_bridge_fore", 0.0)
        lash_bridge_aft = bay_data.get("lash_bridge_aft", 0.0)
    
        return lash_bridge_fore, lash_bridge_aft    
    
    def getStackHeight(self, row, default_height=0)-> float:
        """
        Calculates the stack height for a given row based on the vessel's profile.

        Args:
            row (dict): A dictionary representing the row data, including the 'BAY' and 'ROW' values.
            default_height (float, optional): The default stack height to use when no matching condition is found. Defaults to 0.

        Returns:
            float: The calculated stack height for the given row.

        Raises:
            None

        Example:
            row = {'BAY': 1, 'ROW': 1}
            default_height = 0

            vessel = Vessel()
            stack_height = vessel.getStackHeight(row, default_height)
            print(stack_height)
            # Output: The calculated stack height for the given row.
        """
        bay = row['BAY']
        row = row['ROW']
        stack_height = None
        
        for condition in self._vessel_profile["Stack_Height"]:
            bay_condition = condition.get('bay')
            row_condition = condition.get('row')
            height = condition.get('height')

            if (bay_condition is None or str(int(bay)) == bay_condition) and (row_condition is None or str(int(row)) == row_condition):
                stack_height = height
                break
            
        if stack_height is None:
            stack_height = default_height
        
        return stack_height

    def get_exclusive_fourty_bays(self)->list:
        """
        Retrieves the list of exclusive forty-foot bays from the vessel profile.

        Returns:
            list: The list of exclusive forty-foot bays.

        Example:
            vessel = Vessel()
            exclusive_bays = vessel.get_exclusive_fourty_bays()
            print(exclusive_bays)
            # Output: [74, 94]
        """
        return self._vessel_profile["fourty_foot_exclusive_bays"]
    