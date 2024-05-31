import pandas as pd
import logging 
import numpy as np
from modules.common_helpers import nearest_neighbor_interpolation, get_datetime_diff_by_unit, get_str_date_as_datetime, get_str_time_as_timedelta, linear_interpolation_speed
import datetime 

class rotation():

    def __init__(self, logger: logging.Logger, vessel: object, rotation_intermediate: pd.DataFrame, l_dfs_containers_POL_POD: pd.DataFrame, l_containers_folder_names:list, d_seq_num_to_port_name: dict, rotation_csv_map: dict, df_shifting_rates:pd.DataFrame, consumption_df:pd.DataFrame, fuel_data_dict: dict) -> None:
        self.logger = logger
        self.__vessel_id = vessel.get_imo()
        self.rotations_intermediate = rotation_intermediate
        self.consumption_df = consumption_df
        self.vessel_profile = vessel.get_vessel_profile()
        self.fuel_data_dict = fuel_data_dict
        self.df_shifting_rates = df_shifting_rates
        self.l_dfs_containers_POL_POD = l_dfs_containers_POL_POD
        self.l_containers_folder_names = l_containers_folder_names
        self.d_seq_num_to_port_name = d_seq_num_to_port_name
        self.d_port_name_to_seq_num = {value: key for key, value in self.d_seq_num_to_port_name.items()}
        self.ports_count = len(self.d_seq_num_to_port_name)
        self.__rotation_csv_map = rotation_csv_map
        self.l_ports_names = [self.d_seq_num_to_port_name[i] for i in range(self.ports_count)]
 
    def __increment_port_number(self, rotation_intermediate_df: pd.DataFrame)->None:
        port_counts = {}
        
        def helper(port):
            if port not in port_counts:
                port_counts[port] = 1
                return port
            else:
                port_counts[port] += 1
                return f"{port}{port_counts[port]}"
        
        rotation_intermediate_df['ShortName'] = rotation_intermediate_df['ShortName'].apply(helper)

    def __process_df_intermediate_rotation(self) -> dict:

        self.rotations_intermediate['worldwide'].fillna(self.rotations_intermediate['worldwide'][0], inplace=True)
        self.rotations_intermediate['Gmhold'].fillna(self.rotations_intermediate['Gmhold'][0], inplace=True)
        self.rotations_intermediate['Gmdeck'].fillna(self.rotations_intermediate['Gmdeck'][0], inplace=True)
        self.rotations_intermediate['MaxDraft'].fillna(self.rotations_intermediate['MaxDraft'][0], inplace=True)
        float_cols = self.__rotation_csv_map["cols_intermediate_dtypes"]["float_cols"]
        int_cols = self.__rotation_csv_map["cols_intermediate_dtypes"]["int_cols"]

        self.rotations_intermediate[float_cols] = self.rotations_intermediate[float_cols].astype(float)
        self.rotations_intermediate[int_cols] = self.rotations_intermediate[int_cols].astype(int)

        self.rotations_intermediate["nb_moves_calc"] = 0 # this is for calculating the number of moves required for each port
        
        self.__increment_port_number(self.rotations_intermediate)

        l_records = self.rotations_intermediate.to_dict(orient="records")
        d_rotation = { record["ShortName"]: record for record in l_records }
        
        return d_rotation
    
    def __map_d_rotation_to_seq_num_port_name(self, d_rotation:dict) -> dict: 
            updated_dict2 = {}

            # Iterate through the keys of the first dictionary
            for key in self.d_seq_num_to_port_name:
                sequence_number = key
                port_name = self.d_seq_num_to_port_name[key]
                
                # Check if the sequence_number exists in dict2
                for port_key in d_rotation:
                    if d_rotation[port_key]['Sequence'] == sequence_number:
                        # Update the 'ShortName' field in the second dictionary with the port_name
                        d_rotation[port_key]['ShortName'] = port_name
                        
                        # Add the updated entry to the new dictionary
                        updated_dict2[port_name] = d_rotation[port_key]
                        break

            return updated_dict2
        
    def __add_num_moves_to_d_rotation_pol(self, df_port_containers: pd.DataFrame, d_rotation: dict, port_name: str, port_num: int) -> dict:

        if port_num != 0:
            try:
                d_rotation[port_name]["nb_moves_calc"] += len(df_port_containers[df_port_containers["LOC_9_LOCATION_ID"].str.contains(port_name)]) # add number of containers in loadlist edi of port_name
            except: 
                d_rotation[port_name]["nb_moves_calc"] += 0

            return d_rotation
        
        else : 
            return d_rotation

    def __add_num_moves_to_d_rotation_pod(self, df_port_containers: pd.DataFrame, d_rotation: dict, port_name: str, port_num: int) -> dict:

        if port_num == 0: # if loadlist
            try: 
                d_rotation[port_name]["nb_moves_calc"] += len(df_port_containers[df_port_containers["LOC_11_LOCATION_ID"].str.contains(port_name)]) # add number of containers in loadlist edi of port_name
            except: 
                None
        # for next ports
        for next_seq_num in range(port_num+1, self.ports_count):
            
            next_port_name = self.d_seq_num_to_port_name[next_seq_num]

            try:
                d_rotation[next_port_name]["nb_moves_calc"] += len(df_port_containers[df_port_containers["LOC_11_LOCATION_ID"].str.contains(next_port_name)])
            except: 
                d_rotation[next_port_name]["nb_moves_calc"] += 0
        return d_rotation
    
    def __add_num_moves_to_d_rotation_from_proforma(self, d_rotation: dict, port_name: str, port_num: int) -> None:
        if port_num != 0:
            single_speed = d_rotation[port_name]["SpeedSingle"]
            nb_cranes_proforma = d_rotation[port_name]["NbCranesProforma"]
            std_time_berth = d_rotation[port_name]["StdTimeAtBerth"]
            d_rotation[port_name]["nb_moves_calc"] = single_speed * nb_cranes_proforma * std_time_berth

    def __add_RW_costs_to_d_rotation(self, d_rotation: dict, port_name: str) -> None:

        terminal_code = d_rotation[port_name]["Terminal"]

        port_name_base = port_name[:5] # cz the ports names in the booklet are without number extensions
        df_temp_1 = df_temp = self.df_shifting_rates[
                    ( self.df_shifting_rates["POINT_CODE"] == port_name_base ) &
                    ( [ terminal_code in code for code in self.df_shifting_rates["TERMINAL_CODE"] ] )
                ]
        if not len(df_temp):
            df_temp_1 = df_temp = self.df_shifting_rates[
                    ( self.df_shifting_rates["POINT_CODE"] == port_name_base ) 
            ]
            
            
        df_temp = df_temp[
                        ( [ "0001" in code for code in df_temp["CARRIER_CODE"] ] ) &
                        (
                            ( df_temp["SERVICE_CODES"] == "ALL" ) |
                            ( df_temp["SERVICE_CODES"] == "COLSUEZ")
                        )
                ]


        if not len(df_temp) and not len(df_temp_1):
            
            d_rotation[port_name]["LongName"] = ""
            d_rotation[port_name]["CostRw20"] = ""
            d_rotation[port_name]["CostRw40"] = ""
            d_rotation[port_name]["CostRw45"] = ""
        
        elif not len(df_temp) and len(df_temp_1):
            d_rotation[port_name]["LongName"] = df_temp_1["POINT_NAME"].iloc[0]
            d_rotation[port_name]["CostRw20"] = df_temp_1["TARIFF_20_FULL_SQS"].iloc[0]
            d_rotation[port_name]["CostRw40"] = df_temp_1["TARIFF_40_FULL_SQS"].iloc[0]
            d_rotation[port_name]["CostRw45"] = df_temp_1["TARIFF_45_FULL_SQS"].iloc[0]
                
        else:
            d_rotation[port_name]["LongName"] = df_temp["POINT_NAME"].iloc[0]
            d_rotation[port_name]["CostRw20"] = df_temp["TARIFF_20_FULL_SQS"].iloc[0]
            d_rotation[port_name]["CostRw40"] = df_temp["TARIFF_40_FULL_SQS"].iloc[0]
            d_rotation[port_name]["CostRw45"] = df_temp["TARIFF_45_FULL_SQS"].iloc[0]
    
    def __add_fuel_cons_to_d_rotation(self, d_rotation: dict, port_name: str, key: str, target_key: str):
        
        # extracted from rotation 
        draught_array = np.array(d_rotation[port_name]["MaxDraft"]).astype(float)
        std_speed_array = np.array(d_rotation[port_name][key]).astype(float)

        # draught_array = np.array([port_data["MaxDraft"] for port_data in d_rotation.values()]).astype(float)
        # std_speed_array = np.array([port_data[key] for port_data in d_rotation.values()]).astype(float)
        
        #extracted from consumption 
        consumption_column = f"{self.__vessel_id}_PROD_Conso"
        mean_draft = self.consumption_df['Mean Draft (m)'].values.astype(float)
        speed = self.consumption_df['Avg Speed STW'].values.astype(float)
        consumption = self.consumption_df[consumption_column].values.astype(float)
        
        
        points = np.column_stack((mean_draft, speed))
        sample_points = np.column_stack((draught_array, std_speed_array))
        
        interpolated_consumption = nearest_neighbor_interpolation(sample_points, points, consumption, 3)
        d_rotation[port_name][target_key] = interpolated_consumption[0]
    
    def __get_time_vars_for_d_rotation(
        self,
        d_rotation: dict,
        port_name: str
    ) -> 'tuple[float, float, float, float]':

        Std_time_at_sea = get_str_time_as_timedelta(d_rotation[port_name]["StdTimeAtSea"])

        # Std_time_at_sea = float(d_rotation[port_name]["DistToNext"]) / float(d_rotation[port_name]["StdSpeed"])
        # hours = int(Std_time_at_sea)  # Get the integer part of the float
        # minutes = int((Std_time_at_sea - hours) * 60)  # Calculate the minutes

        # Std_time_at_sea = datetime.timedelta(hours=hours, minutes=minutes) 
        Time_in =  get_str_time_as_timedelta(d_rotation[port_name]["TimeIn"])
        Time_out = get_str_time_as_timedelta(d_rotation[port_name]["TimeOut"])
        time_diff_days = Std_time_at_sea + Time_in + Time_out
        time_diff_hours = get_datetime_diff_by_unit(time_diff_days, unit="h")

        opt_cranes_num = float(d_rotation[port_name]["NbCranes"])
        speed_single = float(d_rotation[port_name]["SpeedSingle"])
        nb_moves = float(d_rotation[port_name]["nb_moves_calc"])
        proforma_cranes_num = float(d_rotation[port_name]["NbCranesProforma"])
        if proforma_cranes_num == opt_cranes_num:
            opt_cranes_num += 1
            d_rotation[port_name]["NbCranes"] = opt_cranes_num

        time_max = nb_moves / (proforma_cranes_num * speed_single)
        time_max_rounded = round(time_max, 2)
        time_min = nb_moves / (opt_cranes_num * speed_single)
        time_min_rounded = round(time_min, 2)
        
        return time_diff_days, time_diff_hours, time_min_rounded, time_max_rounded
    
    def __get_speed_min_and_max(
            self,
            port_name: str,
            d_rotation: dict,
            time_diff_hours: float,
            time_min: datetime.datetime,
            time_max: datetime.datetime
        ):

        distance_to_next = d_rotation[port_name]["DistToNext"]
        time_max_min_diff_hours = time_max - time_min
        if (time_diff_hours + time_max_min_diff_hours) == 0:
            speed_min_temp = speed_max_temp = 0.0 
        else:
            speed_min_temp = distance_to_next / (time_diff_hours + time_max_min_diff_hours)
            speed_max_temp = distance_to_next / time_diff_hours
        speed_min = max([8, speed_min_temp])
        speed_min_rounded = round(speed_min, 2)
        speed_max = max([8, speed_max_temp])
        speed_max_rounded = round(speed_max, 2)

        d_rotation[port_name]["speed_max"] = speed_max_rounded
        d_rotation[port_name]["speed_min"] = speed_min_rounded
       
    def __calculate_fuel_consumption_plan_and_price(self, d_rotation: dict, port_name: str)->None:
    # Define the distances for maneuvering and long leg (in Nm)
        maneuvering_distance = min(0.05 * d_rotation[port_name]["DistToNext"], 100)
        long_leg_distance = 2000
        
        # Determine the preferred fuel type for maneuvering
        preferred_fuel_type_maneuvering = self.vessel_profile["fuel_type_maneuvering"]
        # preferred_fuel_type_maneuvering = 'LSHFO' if 'LSHFO' in self.available_fuel_types else 'MDO'

        # Determine the preferred fuel type for short legs
        preferred_fuel_type_short_leg = self.vessel_profile["fuel_type_short_leg"]
        # preferred_fuel_type_short_leg = 'LNG' if 'LNG' in self.available_fuel_types else 'LSHFO'
        
        # Determine the preferred fuel type for the long leg
        preferred_fuel_type_long_leg = self.vessel_profile["fuel_type_long_leg"]
        # preferred_fuel_type_long_leg = 'LNG' if 'LNG' in self.available_fuel_types else 'HFO'

        # Initialize the fuel plan dictionary with zero consumption for all fuel types
        fuel_plan = {fuel_type: 0 for fuel_type in self.vessel_profile["fuel_types_available"]}
        
        distance_nm = d_rotation[port_name]['DistToNext']
        
        if distance_nm <= maneuvering_distance:
            # Case 1: Coastal - Distance is less than or equal to 100 Nm (maneuvering)
            fuel_plan[preferred_fuel_type_maneuvering] += distance_nm
        
        else: 
            # Case 2: Coastal or long - Distance is more than or equal to 100 Nm (maneuvering)
            fuel_plan[preferred_fuel_type_maneuvering] += maneuvering_distance
            remaining_distance = distance_nm - maneuvering_distance
            # Case 2.a: Coastal - Distance is less than or equal to 2000 Nm (short_leg Distance)
            if distance_nm <= long_leg_distance:
                fuel_plan[preferred_fuel_type_short_leg] += remaining_distance
            # Case 2.b: Non Coastal - Distance is more than 2000 Nm (long_leg Distance)   
            else: 
                fuel_plan[preferred_fuel_type_long_leg] += remaining_distance
        
        if distance_nm != 0:
            # Calculate the percentage of each fuel type against the total distance
            percentage_fuel_plan = {fuel_type: (fuel_distance / distance_nm) for fuel_type, fuel_distance in fuel_plan.items()}
            # Convert Fuel Cost in dictionary into floats
            fuel_cost_float = {fuel: float(cost) for fuel, cost in self.fuel_data_dict.items()}
            # Filter fuel_cost_float to remove 'LNG' key
            fuel_cost_float = {fuel: cost for fuel, cost in fuel_cost_float.items() if fuel in percentage_fuel_plan}
            # Calculate the weighted cost for each fuel type
            weighted_costs = {fuel: cost * percentage_fuel_plan[fuel] for fuel, cost in fuel_cost_float.items()}
            # Calculate the total weighted cost
            total_weighted_cost = sum(weighted_costs.values())
            # Calculate the weighted average cost
            weighted_average_cost = total_weighted_cost / sum(percentage_fuel_plan.values())
            
            d_rotation[port_name]['FuelCost'] = weighted_average_cost
        else:
            self.logger.warn(f"Distance to next port from {port_name} is 0. Default FuelCost and min/max speed will then be set to 0.")
            d_rotation[port_name]['FuelCost'] = 0.0
                   
    def __add_hourly_cost_to_d_rotation(
            self,
            d_rotation: dict,
            port_name: str
        ):

        time_diff_days, time_diff_hours, time_min, time_max = self.__get_time_vars_for_d_rotation(d_rotation, port_name)
        self.__get_speed_min_and_max(port_name, d_rotation, time_diff_hours, time_min, time_max)
        self.__calculate_fuel_consumption_plan_and_price(d_rotation, port_name)
        self.__add_fuel_cons_to_d_rotation(d_rotation, port_name, "speed_min", "cons_per_hour_at_v_min")
        self.__add_fuel_cons_to_d_rotation(d_rotation, port_name, "speed_max", "cons_per_hour_at_v_max")
        
        cons_per_hour_at_v_min = d_rotation[port_name]["cons_per_hour_at_v_min"]
        cons_per_hour_at_v_max = d_rotation[port_name]["cons_per_hour_at_v_max"]
              
        total_potential_fuel_gain = (cons_per_hour_at_v_max - cons_per_hour_at_v_min) * (time_diff_days.total_seconds() / 86400)
        total_potential_fuel_gain_rounded = round(total_potential_fuel_gain, 2)
        fuel_cost_per_ton = d_rotation[port_name]["FuelCost"]
        try:
            hourly_cost = (total_potential_fuel_gain_rounded / ((time_max - time_min)) * fuel_cost_per_ton) if time_min != time_max else 0
            hourly_cost_rounded = round(hourly_cost, 2)
        except:
            hourly_cost_rounded = 0
        d_rotation[port_name]["HourlyCost"] = hourly_cost_rounded   
          
    def get_rotations_final(self) -> None:
        
        d_rotation = self.__process_df_intermediate_rotation()
        #temp solution until webapp has these 2 values 
        for key, subdict in d_rotation.items():

                    subdict['speed_max'] = 0
                    subdict['speed_min'] = 0 
                    subdict["cons_per_hour_at_v_min"] = 0
                    subdict["cons_per_hour_at_v_max"] = 0 

        
        # d_rotation = self.__map_d_rotation_to_seq_num_port_name(d_rotation)

        # port_nums = [int(folder_name.split('_')[0]) for folder_name in self.l_containers_folder_names]
        # port_names = [self.d_seq_num_to_port_name[num] for num in port_nums]
        
        # for df, port_name in list(zip(self.l_dfs_containers_POL_POD, port_names)):
            
        #     port_num = self.d_port_name_to_seq_num[port_name]
            
        #     # self.__add_num_moves_to_d_rotation_pol(df, d_rotation, port_name, port_num)
        #     # self.__add_num_moves_to_d_rotation_pod(df, d_rotation, port_name, port_num)
        for key, port_name in self.d_seq_num_to_port_name.items():
            # Check if the sequence_number exists in dict2
            for port_key, port_info in d_rotation.items():
                if port_info['Sequence'] == key:
                    # Update the 'ShortName' field in the second dictionary with the port_name
                    d_rotation[port_key]['ShortName'] = port_name
                    break    
                      
        port_names = list(d_rotation.keys())
        for port_name in port_names:
            
            port_num = d_rotation[port_name]['Sequence']
        
            if port_num:
                self.__add_num_moves_to_d_rotation_from_proforma(d_rotation, port_name, port_num)
                self.__add_fuel_cons_to_d_rotation(d_rotation, port_name, "StdSpeed", "StdFuelCons")
                
                self.__add_RW_costs_to_d_rotation(d_rotation, port_name)
                self.__add_hourly_cost_to_d_rotation(d_rotation, port_name)
        
        # Add missing keys
        for port_data in d_rotation.values():
            port_data['EstimContWeight'] = ""

            port_data['worldwide'] = '0' if port_data['worldwide'] == "UNRESTRICTED" else '1'
        
        column_order = ['Sequence', 'ShortName', 'NbCranes', 'LongName', 'CostRw20', 'CostRw40', 'CostRw45', 'SpeedSingle', 'SpeedTwin', 'DistToNext',\
            'StdFuelCons', 'StdSpeed', 'worldwide', 'Gmhold', 'Gmdeck', 'StdTimeAtBerth', 'StdTimeAtSea', 'FuelCost', 'Berth Side', 'MaxTierHeight',\
            'MaxRowWidth', 'EstimContWeight', 'MaxDraft', 'HourlyCost', 'WindowStartTime', 'WindowEndTime', 'TimeIn', 'TimeOut', 'WorkDistBtwCranes']    
        # Create the DataFrame with the specified column order
        rotation_final_df = pd.DataFrame.from_dict(d_rotation, orient='index', columns=column_order)
        rotation_final_df.reset_index(drop=True, inplace=True)
        return rotation_final_df
