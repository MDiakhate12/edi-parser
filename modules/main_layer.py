import pandas as pd
import numpy as np 
import re

DEFAULT_MISSING = pd._libs.parsers.STR_NA_VALUES
if "" in DEFAULT_MISSING:
    DEFAULT_MISSING = DEFAULT_MISSING.remove("")

import logging

# # Create a console handler
# console_handler = logging.StreamHandler()
# console_handler.setLevel(logging.DEBUG)
# logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

# # Set the formatter for the console handler
# console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
# console_handler.setFormatter(console_formatter)


from modules.anomaly_detection_layer import AnomalyDetectionLayer as AL
from modules.data_layer import DataLayer as DL
from modules.mapping_layer import MappingLayer as ML
from modules.enrichment_layer import EnrichmentLayer as EL
from modules.pre_processing_layer import PreProcessingLayer as PL
from modules.lashing_calculation_layer import Lashing 
from modules.vessel import Vessel
from modules.worst_case_edi_layer import worst_case_baplies
from modules.rotation_layer import rotation
from modules.dg_layer import DG
from modules.common_helpers import extract_as_dict

class MainLayer():
    def __init__(self, logger: logging.Logger, event: dict, reusePreviousResults: bool, s3_bucket_out: str="", s3_bucket_in: str="") -> None:
        self.logger = logger
        logger.info(event)

        # initialize data layer 
        self.__DL = DL(logger, s3_bucket_out, s3_bucket_in)
        d_event_json = event
        # other params
        path = d_event_json.get("path", "")
        self.__simulation_id = d_event_json["simulation_id"]
        self.__vessel_id = d_event_json.get("vesselImo", "")
        self.__target_port = d_event_json.get("port", "")
        self.__reuse_previous_results = reusePreviousResults
        self.__s3_bucket_out = s3_bucket_out
        self.__s3_bucket_in = s3_bucket_in
        logger.info(f"self.__reuse_previous_results : {self.__reuse_previous_results}")
        ## DG RULES if Master or Slot
        self.__DG_Rules = d_event_json.get("dg_exception_rules", "")
        ## directories
        self.__init_file_paths_from_event(path, self.__simulation_id, self.__s3_bucket_out)
        ## initialize anomaly detection
        self.__AL = AL(self.__DL)
        # for reading, writing, and mapping functionalities
        self.__l_baplies_filepaths = []
        self.__d_seq_num_to_port_name = {}
        self.__l_POL_POD_containers_baplies_paths = []
        self.__l_POL_POD_csvs_paths = []
        # folder name params
        #check for correctness of files 
        logger.info("before __init_params_from_folders_names")
        # clear previous simulation folders
        self.__DL.clear_folder(self.__dynamic_in_dir, self.__s3_bucket_out)
        self.__DL.clear_folder(self.__py_scripts_out_dir, self.__s3_bucket_out)
        self.__DL.clear_folder(self.__cplex_out_dir, self.__s3_bucket_out)
        # copy files from origin to in_preprocessing folder
        self.__copy_webapp_input_into_in_preprocessing()
        self.__after_first_call_loadlist = self.__init_params_from_folders_names()
    
        # intialize mapping layer
        self.__ML = ML(self.__d_seq_num_to_port_name, self.__d_port_name_to_seq_num)
        
        # initialize processing layer
        self.__PL = PL()

    def __init_file_paths_from_event(self, path: str, simulation_id: str, s3_bucket: str="") -> None:
        """
        Constructs the input paths to the static data (csvs, JSON, ...) necessary for the preprocessing and postprocessing steps,
        and to the simulation folder (in that contains the edi's, intermediate that will contain the py scripts output: csv files
        of the input edi's and the necessary input csvs to CPLEX, and out that will contain the CPLEX csv output and the BayPlan.edi).
        The constructed paths will be class attributes for ease of access and as they do not change during run time.

        Parameters
        ----------
        path
            the path for the data folder in the S3 bucket (on AWS), or the local path to the data folder

        simulation_id
            the id of the simulation: the folder name of the simulation

        Returns
        -------
        None
        """
        if s3_bucket == "":
            simulation_dir = f"{path}/simulations/{simulation_id}"
            self.__static_in_dir = f"{path}/referential"
            self.__vessels_static_in_dir = f"{self.__static_in_dir}/vessels/{self.__vessel_id}"
            self.__jsons_static_in_dir = f"{self.__static_in_dir}/config_jsons"
            self.__Edi_static_in_dir = f"{self.__static_in_dir}/EDI_referential"
            self.__service_static_in_dir = f"{self.__static_in_dir}/Service"
            self.__stevedoring_RW_costs_path = f"{self.__static_in_dir}/costs/booklet_stevedoring.csv"
            self.__fuel_costs_path =  f"{self.__static_in_dir}/costs/fuelCosts.csv"
            self.__consumption = f"{self.__static_in_dir}/schedules_temp/conso_apiPreVsProdVsInterp.csv"
            
        else:
            bucket_path = f"s3://{s3_bucket}"
            simulation_dir = f"{simulation_id}"
            self.__static_in_dir = ""
            self.__vessels_static_in_dir = f"vessels/{self.__vessel_id}"
            self.__jsons_static_in_dir = "config_jsons"
            self.__Edi_static_in_dir = f"EDI_referential"
            self.__service_static_in_dir = "Service"
            self.__stevedoring_RW_costs_path = "costs/booklet_stevedoring.csv"
            self.__fuel_costs_path =  "costs/fuelCosts.csv"
            self.__consumption = "schedules_temp/conso_apiPreVsProdVsInterp.csv"
        
        self.__dynamic_in_origin_dir = f"{simulation_dir}/in"    
        self.__dynamic_in_dir = f"{simulation_dir}/in_preprocessing"   
        # self.__dynamic_in_dir = f"{simulation_dir}/in"
        self.__py_scripts_out_dir = f"{simulation_dir}/intermediate"
        
        self.__all_containers_csv_path = f"{self.__py_scripts_out_dir}/csv_combined_containers.csv"
        self.__rotation_intermediate_path = f"{self.__dynamic_in_dir}/rotation.csv"
        
        self.__cplex_out_dir = f"{simulation_dir}/out"
        self.__error_log_path = f"{self.__cplex_out_dir}/error.txt"
       
    def __get_first_two_ports_baplie_and_csv_paths(self, baplies_dir: str, file_name: str, folder_name: str) -> None:
        """
        Gets the baplies paths of the first and second ports and get the csv (converted from the baplie) paths for those two ports.
        These paths will be used later on to modify the slot positions in the two csvs following CPLEX output and create the BayPlan.edi
        using the two baplies and the two modified csvs.

        Parameters
        ----------
        baplies_dir
            the path to the baplies directory for the first and second port

        file_name
            the name of the edi file

        folder_name
            the name of the folder that contains the edi file, it is used to map the name of the two csv files

        Returns
        -------
        None
        """
        baplie_path = f"{baplies_dir}/{file_name}"
        self.__l_POL_POD_containers_baplies_paths.append(baplie_path)

        csv_name = folder_name.replace("call_","") + "_container.csv"
        self.__l_POL_POD_csvs_paths.append(f"{self.__py_scripts_out_dir}/{csv_name}")
    
    def __get_port_name_and_seq_num_maps(self):
        l_seq_nums_sorted = sorted(list(self.__d_seq_num_to_port_name.keys())) # extra safe: to ensure that sequences are in the right order
        self.__d_port_name_to_seq_num = {}
        l_past_ports = []
        for seq_num in l_seq_nums_sorted:
            port_name = self.__d_seq_num_to_port_name[seq_num]
            
            port_name_count = l_past_ports.count(port_name) # count will be 0 when a port name occurs for the first time cz port name will not be in the list
            l_past_ports.append(port_name) # append after count
            if port_name_count: # if count is not 0, i.e., if port name is already in the list at least one time (the count will at least be 1)
                port_name = f"{port_name}{port_name_count+1}" # we distinguish repeated ports names from 2 as a number extension, e.g., CNTXG, CNTXG2, CNTXG3...
            
            self.__d_port_name_to_seq_num[port_name] = seq_num # port_name as is for 1st time, and will be with the number extension if repeated

        self.__d_seq_num_to_port_name = { val: k for k, val in self.__d_port_name_to_seq_num.items() } # substitue values in map as ports names might change

    def __init_params_from_folders_names(self) -> int:
        """
        Enriches class attributes from the folder names available in the simulation folder. These attibutes and their content are:
        - l_baplies_filepaths: contains the paths to all the baplies
        - l_POL_POD_containers_baplies_paths: populated by __get_first_two_ports_baplie_and_csv_paths()
        - l_POL_POD_csvs_paths: populated by __get_first_two_ports_baplie_and_csv_paths()
        - d_port_name_to_seq_num: will hold a map between a port's name (embedded in the folder name) and its sequence number in the rotation
        - d_seq_num_to_port_name: will hold a map between a port's sequence number in the rotation and its name
        
        Also, it performs checks on the following:
        - if OnBoard.edi exists in the call 0 folder, if not => error
        - if both LoadList.edi and Tank.edi exist in other calls folders, if not => error

        This is stage I for error checks: if something is wrong with the files requirements => stop pre-processing and throw an error

        Parameters
        ----------
        None

        Returns
        -------
        None
        """
        onboard_call = ""
        self.logger.info(self.__dynamic_in_dir)
        after_first_call_loadlist = 0
        folder_lists = sorted(self.__DL.list_folders_in_path(self.__dynamic_in_dir, self.__s3_bucket_out))

        self.__AL.check_folder_if_exists(folder_lists, keyword="call_00", print_message= "-> no OnBoard.edi file uploaded...")
        
        for i, folder_name in enumerate(folder_lists):
            
            baplies_dir = f"{self.__dynamic_in_dir}/{folder_name}"
            folder_name_split = folder_name.split("_")
            
            call_id = "_".join(folder_name_split[-2:])
            seq_num = int(folder_name_split[-2])
            loadlist_flag, tank_flag  = 0, 0
            
            for file_name in self.__DL.list_files_in_path(baplies_dir, self.__s3_bucket_out):
                if file_name.split(".")[-1] == "edi":
                    self.__l_baplies_filepaths.append(f"{baplies_dir}/{file_name}")

                    if file_name == "OnBoard.edi":
                        onboard_call = folder_name

                        self.__get_first_two_ports_baplie_and_csv_paths(baplies_dir, file_name, folder_name)
                    
                    if i >= 2:
                        if file_name == "Tank.edi" :
                            tank_flag = 1
                            loadlist_flag = 1
                        elif file_name == "LoadList.edi": 
                            loadlist_flag = 1
                            after_first_call_loadlist += 1
                        else: 
                            loadlist_flag = 1
                            
                    elif file_name == "Tank.edi" :
                        tank_flag = 1
                        
                    elif file_name == "LoadList.edi":
                        loadlist_flag = 1
                        
                        
                        if seq_num == 1:
                            self.__get_first_two_ports_baplie_and_csv_paths(baplies_dir, file_name, folder_name)   

            # error in case of wrong edi files
            # first folder with no OnBoard.edi
            
            if not seq_num:
                
                self.__AL.check_onboard_edi(onboard_call, call_id)
            
            # not first folder but LoadList.edi without Tank.edi or Tank.edi without LoadList.edi
            else:
                self.__AL.check_loadlist_and_tank_edi_files(loadlist_flag, tank_flag, call_id)
            
            port_name = folder_name_split[-1]
            self.__d_seq_num_to_port_name[seq_num] = port_name
        
        
        self.__get_port_name_and_seq_num_maps()
        return after_first_call_loadlist

    def __output_onboard_loadlist(
            self,
            df_all_containers: pd.DataFrame,
            df_filled_slots: pd.DataFrame,
            df_DG_classes_grouped: pd.DataFrame,
            l_stacks_lines: list,
            d_iso_codes_map: dict
        ) -> pd.DataFrame:

        self.logger.info("Extracting and saving containers onboard LoadList...")
        d_containers_to_onboard_ll_cols_map = self.__DL.read_json(f"{self.__jsons_static_in_dir}/containers_onboard_loadlist_cols_map.json", self.__s3_bucket_in)
        
        df_onboard_loadlist = self.__PL.get_df_containers_onboard_loadlist(
            df_all_containers,
            df_DG_classes_grouped,
            d_containers_to_onboard_ll_cols_map,
            d_iso_codes_map
        )
        
        l_container_ids = df_filled_slots["EQD_ID"].tolist()
        l_slots = df_filled_slots.iloc[:, 1].tolist()

        l_container_types = df_filled_slots["EQD_SIZE_AND_TYPE_DESCRIPTION_CODE"].tolist()
        
        d_type_to_size_map = self.__ML.get_type_to_size_map_dict(df_onboard_loadlist, l_container_types)
        d_container_info_by_bay_row_tier = self.__ML.get_d_container_info_by_bay_row_tier(l_slots, l_container_types, l_container_ids)

        df_stacks = self.__PL.get_list_of_lines_as_df(l_stacks_lines)
        
        d_stacks_rows_by_bay_row_deck = self.__ML.get_d_stacks_rows_by_bay_row_deck(df_stacks)
        
        self.__AL.check_flying_containers(d_container_info_by_bay_row_tier, d_stacks_rows_by_bay_row_deck, d_type_to_size_map)
        self.__AL.check_if_errors()
        
        onboard_csv_name = "Containers OnBoard Loadlist 0.csv"
        onboard_csv_path = f"{self.__py_scripts_out_dir}/{onboard_csv_name}"
        self.__DL.write_csv(df_onboard_loadlist, onboard_csv_path, self.__s3_bucket_out)

        return df_onboard_loadlist
    
    def __output_stowing_info(self, l_onboard_loadlist_lines: list, l_stacks_lines: list) -> None:
        subbays_csv_name = "SubBays Capacities Extrait Prototype MP_IN.csv"
        subbays_csv_path = f"{self.__vessels_static_in_dir}/{subbays_csv_name}"
        l_subbays_lines = self.__DL.read_csv_lines(subbays_csv_path, self.__s3_bucket_in)
                
        ## STOWING INFO ##
        l_stowing_info_lines, l_reefers_at_non_reefer \
        = self.__PL.get_l_stowing_info_lines_and_reefers_at_non_reefers(
                l_stacks_lines,
                l_subbays_lines,
                l_onboard_loadlist_lines,
                False
            )

        self.__AL.check_reefer_containers_at_non_reefer_slots(l_reefers_at_non_reefer)
        self.__AL.check_if_errors()

        stowing_info_csv_name = "Containers Stowing Info 0.csv"
        stowing_info_csv_path = f"{self.__py_scripts_out_dir}/{stowing_info_csv_name}"
        self.__DL.write_csv_lines(l_stowing_info_lines, stowing_info_csv_path, self.__s3_bucket_out)
        
        ## OVERSTOWING SUBBAYS ##
        #what if a reefer was placed in a non reefer and it is legit? how will we know?
        l_overstowing_subbays_lines \
        = self.__PL.get_l_overstowing_subbays_lines(
                l_stacks_lines,
                l_subbays_lines,
                l_onboard_loadlist_lines,
                True
            )
        
        overstowing_subbays_csv_name = "Overstowing Subbays 0.csv"
        overstowing_subbays_csv_path = f"{self.__py_scripts_out_dir}/{overstowing_subbays_csv_name}"
        self.__DL.write_csv_lines(l_overstowing_subbays_lines, overstowing_subbays_csv_path, self.__s3_bucket_out)

        return l_stowing_info_lines

    def __output_grouped_containers(
            self,
            l_stacks_lines: list,
            l_stowing_info_lines: list,
            l_onboard_loadlist: list
        ) -> None:
        POL_POD_revenues_csv_name = "Revenues by Size Type POL POD.csv"
        POL_POD_revenues_csv_path = f"{self.__service_static_in_dir}/{self.__service_code}/{POL_POD_revenues_csv_name}"
        l_POL_POD_revenues_lines = self.__DL.read_csv_lines(POL_POD_revenues_csv_path, self.__s3_bucket_in)

        l_container_groups_completed_lines, l_container_groups_containers_lines \
        = self.__PL.get_two_list_for_container_groups(
                l_stacks_lines,
                l_stowing_info_lines,
                l_onboard_loadlist,
                l_POL_POD_revenues_lines
            )
        # update to include Baptiste's work
        groups_completed_csv_name = "Container Groups Completed 0.csv"
        groups_completed_csv_path = f"{self.__py_scripts_out_dir}/{groups_completed_csv_name}"
        self.__DL.write_csv_lines(l_container_groups_completed_lines, groups_completed_csv_path, self.__s3_bucket_out)
        
        groups_containers_csv_name = "Container Groups Containers.csv"
        groups_containers_csv_path = f"{self.__py_scripts_out_dir}/{groups_containers_csv_name}"
        self.__DL.write_csv_lines(l_container_groups_containers_lines, groups_containers_csv_path, self.__s3_bucket_out)
        return l_container_groups_containers_lines
    
    def __output_CPLEX_input_container_csvs(self, df_all_containers: pd.DataFrame, df_filled_slots: pd.DataFrame, df_DG_classes_expanded: pd.DataFrame, d_iso_codes_map: dict) :
        # loading stacks
        stacks_csv_name = "Stacks Extrait Prototype MP_IN.csv"
        stacks_csv_path = f"{self.__vessels_static_in_dir}/{stacks_csv_name}"
        l_stacks_lines = self.__DL.read_csv_lines(stacks_csv_path, self.__s3_bucket_in)
        
        ## Onboard Loadlist ##
        df_onboard_loadlist = self.__output_onboard_loadlist(
            df_all_containers,
            df_filled_slots,
            df_DG_classes_expanded,
            l_stacks_lines,
            d_iso_codes_map
        )

        # load referential data 
        if not self.__reuse_previous_results:

            # d_STOWING_seq_num_to_port_name: this map is only for stowing info
            d_STOWING_seq_num_to_port_name = self.__ML.get_d_STOWING_seq_num_to_port_name(df_onboard_loadlist)
            self.__PL.add_STOWING_maps_to_class_attributes(d_STOWING_seq_num_to_port_name)

            l_onboard_loadlist_lines = self.__PL.get_df_as_list_of_lines(df_onboard_loadlist)
            l_stowing_info_lines = self.__output_stowing_info(l_onboard_loadlist_lines, l_stacks_lines)

            ## Grouped containers ##
            self.logger.info("Extracting and saving Container Groups Containers...")
            l_container_groups_containers_lines = self.__output_grouped_containers(l_stacks_lines, l_stowing_info_lines, l_onboard_loadlist_lines)

        return l_container_groups_containers_lines if not self.__reuse_previous_results else None
        #======================================================================================================================================       
        ## Stowing and overstowing ##
    
    def __get_df_from_baplie_and_return_types(self, baplie_path: str, call_id: str, file_type: str, d_csv_cols_to_segments_map: dict, d_main_to_sub_segments_map: dict, s3_bucket:str):
       
        l_baplie_segments, new_data_flag, baplie_type_from_file_name, baplie_type_from_content = \
            self.__DL.read_baplie_body_as_list(baplie_path, call_id, file_type, s3_bucket)
        if file_type in ['OnBoard', 'LoadList']:
            self.__AL.check_missing_new_container_header(l_baplie_segments, call_id, file_type)
            
        if baplie_type_from_file_name is None:
            return None, None, None

        new_data_flag_in_baplie = new_data_flag.replace("_", "+")
        self.logger.info(f"New data for a {baplie_type_from_file_name} starts at: {new_data_flag_in_baplie}...")
        self.logger.info(f"There are {len(l_baplie_segments)} {baplie_type_from_file_name}s in this file...")

        csv_cols_dict, csv_cols_list = self.__ML.get_csv_cols_dict_and_list(l_baplie_segments, new_data_flag, d_csv_cols_to_segments_map, d_main_to_sub_segments_map, baplie_type_from_content)
        if any("LOC_12" in csv_col for csv_col in csv_cols_list):
            self.logger.warning("WARNING: A LOC+12 segment was found...") #TODO add to anoamlies dict
        
        # intialize enrichment layer
        self.__EL = EL(csv_cols_list, self.logger)

        # getting the dataframe of extracted attributes from the Baplie message
        df_attributes = self.__EL.get_attributes_df(l_baplie_segments, new_data_flag, csv_cols_dict, d_main_to_sub_segments_map, baplie_type_from_content)
        df_attributes.fillna("", inplace=True)
        if "EQD_TYPE_CODE_QUALIFIER" in df_attributes.columns:
            df_attributes["EQD_TYPE_CODE_QUALIFIER"] = df_attributes["EQD_TYPE_CODE_QUALIFIER"].astype(str)

        # check mismatch between the total rows num for a column and the total num of containers
        self.__AL.check_extracted_containers_num(len(l_baplie_segments), len(df_attributes), call_id, file_type)

        return df_attributes, baplie_type_from_file_name, baplie_type_from_content

    def __add_lowest_DGS_cols_to_df(self, df: pd.DataFrame, d_csv_cols_to_segments_map: dict) -> pd.DataFrame:
        df_copy = df.copy()

        l_DGS_cols = [
            col for col in d_csv_cols_to_segments_map["MAIN_HEADERS"]["DGS"].split(";")
        ]
        l_static_DG_cols = ["ATT_PSN_DETAIL_DESCRIPTION_CODE", "ATT_PSN_DETAIL_DESCRIPTION",
                            "FTX_FREE_TEXT_DESCRIPTION_CODE","PACKAGING_DANGER_LEVEL_CODE",
                            "DGS_SUB_LABEL1"]

        l_DGS_cols += l_static_DG_cols
        # prints occuppied DGS Hazards in Columns if not null 
        l_DGS_HAZARD_ID_cols = [
            col for col in df_copy.columns
            if "DGS" in col and "HAZARD_ID" in col # and "DGS_HAZARD_ID" not in col
        ]

        df_temp = df_copy[l_DGS_HAZARD_ID_cols]
        df_copy["DG_classes_suffixes"] = df_temp.apply(lambda row: self.__PL.get_DGS_suffixes(row.astype(str), l_DGS_HAZARD_ID_cols), axis=1)
        df_copy["lowest_DG_class_suffix"], df_copy["all_DG_class_suffixes"] = zip(*df_copy["DG_classes_suffixes"])
        df_copy.drop("DG_classes_suffixes", axis=1, inplace=True)

        for col in l_DGS_cols:
            col_name = "DGS_" + col
            df_copy[col_name] = df_copy.apply(lambda row: row.get(col_name + row["lowest_DG_class_suffix"], np.nan), axis=1)
            
        return df_copy

    def __get_df_DG_classes_expanded(self) -> pd.DataFrame:
        DG_classes_expanded_csv_path = f"{self.__vessels_static_in_dir}/table_7_2_4_expanded.csv"
        df = self.__DL.read_csv(DG_classes_expanded_csv_path, DEFAULT_MISSING, s3_bucket=self.__s3_bucket_in).astype(str)
        df.drop("CLASS", axis=1, inplace=True)
        df.index = df.columns

        return df
    
    def __get_df_filled_slots(self, df_attributes: pd.DataFrame) -> pd.DataFrame:
        pds_slots = df_attributes.iloc[:, 1]
        filled_slots_count = len(pds_slots) - len(pds_slots[pds_slots == ""])
        if filled_slots_count:
            s_filled_slots = pds_slots[pds_slots != ""]
            
            df_filled_slots = df_attributes.iloc[s_filled_slots.index, :]
            df_filled_slots.reset_index(inplace=True, drop=True)
            
            return df_filled_slots

    def __get_tanks_basic_infos(self) -> dict:
        tanks_dir = f"{self.__vessels_static_in_dir}/Tanks"
        d_tanks_basic_infos = {}
        for fn_tank in self.__DL.list_files_in_path(tanks_dir, s3_bucket=self.__s3_bucket_in):
            tanks_csv_path = f"{tanks_dir}/{fn_tank}"
            # sort between raw text (extension .txt) and new text (extension .csv)
            f_name, f_extension = self.__DL.get_file_name_and_extension_from_path(tanks_csv_path)
            if f_extension != "csv":
                continue 

            l_tanks_lines = self.__DL.read_csv_lines(tanks_csv_path, s3_bucket=self.__s3_bucket_in, new_line="\n")
            
            for no_line, line in enumerate(l_tanks_lines):
            
                # no header
                l_items = line.split(";")
            
                # just read first line
                if no_line == 0:
                    tank_name = l_items[0]
            
                    capacity = float(l_items[1])
                    first_frame = int(l_items[2])
                    last_frame = int(l_items[3])
                
                    d_tanks_basic_infos[tank_name] = (capacity, first_frame, last_frame)
                else:
                    break
        
        # complete manually for some (scrubbing) tanks.
        d_tanks_basic_infos["SCRUBBER HOLDING"] = (152.10, 28, 30)
        d_tanks_basic_infos["SCRUBBER RESIDUE"] = (200.32, 27, 30)
        d_tanks_basic_infos["SCRUBBER SILO 1"] = (58.35, 35, 39)
        d_tanks_basic_infos["SCRUBBER SILO 2"] = (46.54, 35, 39)
        d_tanks_basic_infos["SCRUBBER M/E PROC"] = (51.49, 40, 43)
        d_tanks_basic_infos["SCRUBBER G/E PROC"] = (15.54, 41, 43)

        # and override for 2 others
        d_tanks_basic_infos["NO.2 M/E CYL.O STOR.TK(S)"] = (55.48, 43, 45)
        d_tanks_basic_infos["M/E SYS.O SETT.TK(S)"] = (111.42, 45, 49)
        
        return d_tanks_basic_infos

    def __get_l_filled_tanks_ports(self, l_tanks_baplies_paths: list):
        l_sel_tank_types, void_tank_type, wb_tank_type, l_unknown_tanks = self.__PL.read_tank_elems()
        BV_condition, WB_compensating_trim, edi_tank_format, filter_out_wb = self.__PL.get_run_parameters()

        # in case 1 passed as an empty string => will not affect output csv
        #TODO investigate the impact BV_condition_tank_port to know how to change it dynamically
        BV_condition_tank_port = self.__target_port

        l_filled_tanks_ports = []
    
        #### Case 1: files provided by CMA, one file by port (No BV condition)
        ###### We are in the context of only fuel and miscellaneous tanks are considered (no water ballast)
        GBSOU_flag = 0
        if BV_condition is None:

            # only relevant files are in this directory
            for filepath in sorted(l_tanks_baplies_paths):
                # sort between baplie edi (extension .edi) and new text (extension .csv)
                f_name, f_extension = self.__DL.get_file_name_and_extension_from_path(filepath)
                
                #TODO take care of the f_name and f_extension problem: port_name_extension is added to l_filled_tanks_port

                if f_extension != "edi":
                    continue
                
                # how to distinguish between GBSOU and GBSOU2 can only be done using the file name 
                port_name_extension = ""
                if f_name[-1] in ["2", "3", "4"]:
                    port_name_extension = f_name[-1]
                
                s_tanks_port = self.__DL.read_file(filepath, s3_bucket=self.__s3_bucket_out)
                delimiter = self.__DL.get_baplie_delimiter(s_tanks_port)
                l_rows = s_tanks_port.split(delimiter)

                # f_tanks_port = open(filepath, "r")
                # l_tanks_port = self.__DL.read_csv_lines(filepath)

                # # get the file rows as a list, so to have common treatment with format without row separator
                # if edi_tank_format == "EDI_CRLF":
                #     s_tanks_port = self.__DL.read_file(filepath)
                #     delimiter = self.__DL.get_baplie_delimiter(s_tanks_port)
                #     l_tanks_port = s_tanks_port.split(delimiter)
                #     l_rows = []
                #     for no_row, row in enumerate(f_tanks_port):
                #         # remove " and \n
                #         if '"' in row and "\n" in row: l_rows.append(row[0:-2])
                #         else: l_rows.append(row)
                #     f_tanks_port.close()

                #     if len(l_rows) == 1:
                #         f_tanks_port = open(filepath, "r")
                #         #TODO investigate issue
                #         l_line = f_tanks_port.readlines()
                #         # "real" rows
                #         l_rows = l_line[0].split("'")
                    
                #         f_tanks_port.close()

                #     print(l_rows == l_tanks_port)
                
                # if edi_tank_format == "EDI_QUOTE":
                #     l_line = f_tanks_port.readlines()
                #     # "real" rows
                #     l_rows = l_line[0].split("'")
                #     f_tanks_port.close()
                    
                # f_tanks_port.close()
                
                # work on the list of rows as a whole
                l_filled_tanks_port = self.__PL.l_get_filled_tanks_port_infos(
                    l_rows, "", port_name_extension, void_tank_type, l_unknown_tanks, l_sel_tank_types, filter_out_wb
                )
                
                l_filled_tanks_ports.extend(l_filled_tanks_port)

        # case 2: not used so far
        #### case 2) Reading TANKSTA edi files coming from macS3 (no WB compensation)
        ###### we keep water ballast
        
        if BV_condition is not None and BV_condition != "01" and WB_compensating_trim == False:

            fn_edi_tank_list = "lc%s-tanks.edi" % BV_condition

            f_edi_tank_list = open(fn_edi_tank_list, "r")
            
            # un seul enregistrement
            if edi_tank_format == "EDI_QUOTE":
                l_line = f_edi_tank_list.readlines()
                # "real" rows
                l_rows = l_line[0].split("'")
            #print(len(l_line)) 
            # ou plusieurs
            if edi_tank_format == "EDI_CRLF":
                l_rows = []
                for no_row, row in enumerate(f_edi_tank_list):
                    # remove " and \n
                    l_rows.append(row[0:-2])
            
            f_edi_tank_list.close()

            # simply useless
            port_name_extension = ""

            # work on the list of rows as a whole
            l_filled_tanks_port = self.__PL.l_get_filled_tanks_port_infos(l_rows, BV_condition_tank_port, port_name_extension, 
                                                                void_tank_type, l_unknown_tanks, l_sel_tank_types,
                                                                filter_out_wb=False)
            l_filled_tanks_ports.extend(l_filled_tanks_port)
            
        #### case 3) Reading TANKSTA edi files coming from CMA (WB are compensating trim)
        ###### we keep water ballast
        # not our case right now
        if BV_condition is not None and WB_compensating_trim == True:
            
            port_name_extension = ""

            fn_edi_tank_list = "lc%s-tanks_WB_4_trim.edi" % BV_condition

            f_tanks_port = open(fn_edi_tank_list, "r")
        
            # get the file rows as a list, so to have common treatment with format without row separator
            if edi_tank_format == "EDI_CRLF":
                l_rows = []
                for no_row, row in enumerate(f_tanks_port):
                    # remove " and \n
                    l_rows.append(row[0:-2])
            # or one unique row
            if edi_tank_format == "EDI_QUOTE":
                l_line = f_tanks_port.readlines()
                # "real" rows
                l_rows = l_line[0].split("'")
        
            f_tanks_port.close()
        
            # work on the list of rows as a whole, no filter on WB !!
            l_filled_tanks_port = self.__PL.l_get_filled_tanks_port_infos(l_rows, BV_condition_tank_port, port_name_extension, 
                                                                void_tank_type, l_unknown_tanks, l_sel_tank_types,
                                                                filter_out_wb=False)
            l_filled_tanks_ports.extend(l_filled_tanks_port)
            
            
        return l_filled_tanks_ports
    
    def __output_filled_subtanks(self, l_tanks_baplies_paths: list) -> None:
        d_tanks_basic_infos = self.__get_tanks_basic_infos()

        frames_csv_path = f"{self.__vessels_static_in_dir}/Frames.csv"
        l_frames_lines = self.__DL.read_csv_lines(frames_csv_path, s3_bucket=self.__s3_bucket_in, new_line="\n")

        blocks_csv_path = f"{self.__vessels_static_in_dir}/Blocks.csv"
        l_blocks_lines = self.__DL.read_csv_lines(blocks_csv_path, s3_bucket=self.__s3_bucket_in, new_line="\n")

        l_filled_tanks_ports = self.__get_l_filled_tanks_ports(l_tanks_baplies_paths)

        d_tank_names_edi_2_bv = self.__DL.read_json(f"{self.__jsons_static_in_dir}/tanks_names_edi_to_bv_map.json", s3_bucket=self.__s3_bucket_in)
        l_filled_subtanks_csv_cols = self.__DL.read_json(f"{self.__jsons_static_in_dir}/filled_tanks_cols.json", s3_bucket=self.__s3_bucket_in)["filled_tanks_cols"]
        df_filled_subtanks = self.__PL.get_df_filled_subtanks(
                d_tanks_basic_infos,
                d_tank_names_edi_2_bv,
                l_frames_lines,
                l_blocks_lines,
                l_filled_tanks_ports,
                l_filled_subtanks_csv_cols
            )
        
        filled_tanks_csv_name  = "Preprocessed Filled Tanks Ports.csv"
        filled_tanks_csv_path  = f"{self.__py_scripts_out_dir}/{filled_tanks_csv_name}"
        self.__DL.write_csv(df_filled_subtanks, filled_tanks_csv_path, s3_bucket=self.__s3_bucket_out)

    def __copy_webapp_input_into_in_preprocessing(self):
        # read baplies already existing from webapp
        for i, folder_name in enumerate(sorted(self.__DL.list_folders_in_path(self.__dynamic_in_origin_dir, self.__s3_bucket_out))):
            baplies_dir = f"{self.__dynamic_in_origin_dir}/{folder_name}/"
            baplies_destination_dir = f"{self.__dynamic_in_dir}/{folder_name}/"
            for file_name in self.__DL.list_files_in_path(baplies_dir, self.__s3_bucket_out):
                source_key = f"{baplies_dir}{file_name}"
                self.__DL.copy_file(source_key, baplies_destination_dir, self.__s3_bucket_out, self.__s3_bucket_out, file_name)
        # also copy rotation.csv
        rotation_csv_dir = f"{self.__dynamic_in_origin_dir}/rotation.csv"
        rotation_csv_destination_dir = f"{self.__dynamic_in_dir}"
        self.__DL.copy_file(rotation_csv_dir, rotation_csv_destination_dir, self.__s3_bucket_out, self.__s3_bucket_out, "rotation.csv")

    def __get_pod_from_baplies_uploaded(self, d_csv_cols_to_segments_map:dict, d_main_to_sub_segments_map:dict)-> list:
        # read baplies already existing from webapp
        l_baplies_webapp_filepaths, l_POD_profile = [], []
        for i, folder_name in enumerate(sorted(self.__DL.list_folders_in_path(self.__dynamic_in_dir, self.__s3_bucket_out))): 
            baplies_dir = f"{self.__dynamic_in_dir}/{folder_name}"
            for file_name in self.__DL.list_files_in_path(baplies_dir, self.__s3_bucket_out):
                if file_name in ['LoadList.edi', 'OnBoard.edi']:
                    file_name_path = f"{baplies_dir}/{file_name}"
                    l_baplies_webapp_filepaths.append(file_name_path)     

        for baplie_path in l_baplies_webapp_filepaths:
            folder_name = self.__DL.get_folder_name_from_path(baplie_path)
            file_name = self.__DL.get_file_name_from_path(baplie_path)
            folder_name_split = folder_name.split("_")
            call_id = "_".join(folder_name_split[-2:])

            df_attributes, baplie_type_from_file_name, baplie_type_from_content = self.__get_df_from_baplie_and_return_types(baplie_path, call_id, file_name, d_csv_cols_to_segments_map, d_main_to_sub_segments_map, self.__s3_bucket_out)
            self.__AL.check_baplie_types_compatibility(baplie_type_from_file_name, baplie_type_from_content, call_id)
            l_POD_in_edi = df_attributes['LOC_11_LOCATION_ID'].unique()
            l_POD_profile.extend([attribute for attribute in l_POD_in_edi if attribute not in l_POD_profile and attribute != ''])
        return l_POD_profile
    
    def __run_first_execution(self) -> None:

        # get csv headers dict, list, and prefixes list present in the Baplie message
        d_csv_cols_to_segments_map = self.__DL.read_json(f"{self.__jsons_static_in_dir}/csv_cols_segments_map.json", s3_bucket=self.__s3_bucket_in)
        d_main_to_sub_segments_map = self.__DL.read_json(f"{self.__jsons_static_in_dir}/main_sub_segments_map.json", s3_bucket=self.__s3_bucket_in)
        # iso codes sizes and heights map (to check iso codes)
        self.logger.info("Reading iso_code_map from referential configuration folder...")
        d_iso_codes_map = self.__DL.read_json(f"{self.__jsons_static_in_dir}/ISO_size_height_map.json", s3_bucket=self.__s3_bucket_in)

        # Add Rotations before (identify (gm, std speed , draft and service line from rotation intermediate ))
        self.logger.info("*" * 80)
        self.logger.info("Reading rotation_csv column mapping from referential configuration folder...")
        rotation_csv_maps = self.__DL.read_json(f"{self.__jsons_static_in_dir}/rotation_csv_maps.json", s3_bucket=self.__s3_bucket_in)
        self.logger.info("Reading stevedoring cost from referential costs folder...")
        RW_costs = self.__DL.read_csv(self.__stevedoring_RW_costs_path, na_values=DEFAULT_MISSING, s3_bucket=self.__s3_bucket_in, sep=";")
        self.logger.info("Reading Rotations intermediate csv file from simulation in folder...")
        rotation_intermediate = self.__DL.read_csv(self.__rotation_intermediate_path,  na_values=DEFAULT_MISSING, s3_bucket=self.__s3_bucket_out)

        l_POD_profile = self.__get_pod_from_baplies_uploaded(d_csv_cols_to_segments_map, d_main_to_sub_segments_map)  
        last_index_in_rotations = rotation_intermediate[rotation_intermediate['ShortName'].isin(l_POD_profile)].index.max()
        rotation_intermediate = rotation_intermediate.iloc[:(last_index_in_rotations + 1)]
        self.logger.info("Reading consumption csv file from referential vessels folder...")
        consumption_df = self.__DL.read_csv(self.__consumption, na_values=DEFAULT_MISSING, sep=',', s3_bucket=self.__s3_bucket_in)
        self.logger.info("Extracting StdSpeed, GmDeck, MaxDraft, lashing calculation configuration and service line for call_01 from rotation intermediate...")
        lashing_parameters_dict = extract_as_dict(rotation_intermediate, indexes=None, columns=['CallFolderName', 'StdSpeed', 'Gmdeck', 'MaxDraft', 'worldwide', 'service', 'WindowStartTime', 'WindowEndTime'])
        self.__AL.validate_data(lashing_parameters_dict)
        self.__AL.check_if_errors()
        self.logger.info("*"*80)
        
        # # Intialize worst cast files generation 
        self.__service_code = lashing_parameters_dict[0]['service']
        # # Check if service code exists in EDI referentials 
        self.logger.info(f"Checking if {self.__service_code} is in service line EDI's referential folder...")
        if self.__service_code in self.__DL.list_folders_in_path(f"{self.__Edi_static_in_dir}", self.__s3_bucket_in):
            self.logger.info("Checking if all calls beyond call_01 have a LoadList.edi (if provided no need to generate worst case baplies for future ports)...")
            if  self.__after_first_call_loadlist != max(self.__d_seq_num_to_port_name.keys()) -1:
                self.logger.info(f"Generating Worst case scenario Baplie messages for future ports in rotation for service: {self.__service_code}...")
                EDI_referential_path = f"{self.__Edi_static_in_dir}/{self.__service_code}"
                self.__worst_case_baplies = worst_case_baplies(self.logger, self.__AL, self.__vessel_id, self.__simulation_id, EDI_referential_path, self.__dynamic_in_dir, self.__error_log_path, self.__s3_bucket_in, self.__s3_bucket_out)
                self.__worst_case_baplies.generate_worst_case_baplie_loadlist()

                # Check if all LoadLists exist after generation
                for i, folder_name in enumerate(sorted(self.__DL.list_folders_in_path(self.__dynamic_in_dir, self.__s3_bucket_out))): 
                    baplies_dir = f"{self.__dynamic_in_dir}/{folder_name}"
                    folder_name_split = folder_name.split("_")
                    call_id = "_".join(folder_name_split[-2:])
                    loadlist_flag = 0 
                    for file_name in self.__DL.list_files_in_path(baplies_dir, self.__s3_bucket_out):
                        if i <2:
                            loadlist_flag = 1
                        if i >= 2:
                                if file_name == "LoadList.edi": 
                                    loadlist_flag = 1
                    self.__AL.check_loadlist_beyond_first_call(loadlist_flag, call_id)
                    
                self.__AL.check_if_errors()
            else: 
                self.logger.info("LoadList.edi exists for all port calls in sumulation folder...")
            self.logger.info("*" * 80)   
        else: 
            self.logger.info(f"Service line {self.__service_code} not found in EDI referentials...")
            self.logger.info("*" * 80)  
            
        # Intialize Vessel
        self.logger.info(f"Creating Vessel instance for vessel: {self.__vessel_id}")
        # vessel profile file in referentials
        self.logger.info(f"Reading vessel_profile.json & DG_rules.json configuration file from referential vessel folder for vessel: {self.__vessel_id}...")
        vessel_profile = self.__DL.read_json(f"{self.__vessels_static_in_dir}/vessel_profile.json", s3_bucket=self.__s3_bucket_in)
        
        # DG_rules config JSON for Vessel
        DG_rules = self.__DL.read_json(f"{self.__vessels_static_in_dir}/DG_rules.json", s3_bucket=self.__s3_bucket_in)
        
        # DG Exclusions 
        dg_exclusions_csv_path = f"{self.__vessels_static_in_dir}/DG Exclusions.csv"
        dg_exclusions_df =  self.__DL.read_csv(dg_exclusions_csv_path, DEFAULT_MISSING, ";", self.__s3_bucket_in).astype(str)
        
        # Vessel Stacks 
        stacks_csv_name = "Stacks Extrait Prototype MP_IN.csv"
        stacks_csv_path = f"{self.__vessels_static_in_dir}/{stacks_csv_name}"
        l_stacks_lines = self.__DL.read_csv_lines(stacks_csv_path, s3_bucket=self.__s3_bucket_in, new_line="\n")
        fn_stacks = self.__PL.get_list_of_lines_as_df(l_stacks_lines)
        
        std_speed = float(lashing_parameters_dict[0]['StdSpeed'])
        draft = float(lashing_parameters_dict[0]['MaxDraft'])
        gm_deck = float(lashing_parameters_dict[0]['Gmdeck'])
        
        vessel = Vessel(self.logger, std_speed, gm_deck, draft, vessel_profile, DG_rules, dg_exclusions_df, fn_stacks)
        
        #Iniatlize Lashing 
        lashing_conditions = lashing_parameters_dict[0]['worldwide']
        lashing = Lashing(self.logger, vessel, lashing_conditions)
        self.logger.info("*"*80)
        
        # empty lists for dataframes that are going to be saved as csvs and their folder names (used in the names of the csvs)
        l_dfs_containers, l_containers_folder_names, l_dfs_rotation_containers = [], [], []
        l_dfs_tanks, l_tanks_baplies_paths, l_tanks_folder_names = [], [], []
        
        l_baplies_filepaths = []
        for i, folder_name in enumerate(sorted(self.__DL.list_folders_in_path(self.__dynamic_in_dir, self.__s3_bucket_out))): 
            baplies_dir = f"{self.__dynamic_in_dir}/{folder_name}"
            for file_name in self.__DL.list_files_in_path(baplies_dir, self.__s3_bucket_out):
                    if file_name.split(".")[-1] == "edi":
                        file_name_path = f"{baplies_dir}/{file_name}"
                        l_baplies_filepaths.append(file_name_path)     

        for baplie_path in l_baplies_filepaths:
            folder_name = self.__DL.get_folder_name_from_path(baplie_path)
            file_name = self.__DL.get_file_name_from_path(baplie_path)
            folder_name_split = folder_name.split("_")
            call_id = "_".join(folder_name_split[-2:])
            file_type = file_name.split(".")[0]
            self.logger.info(f"Reading {file_name} from {folder_name}...")

            # get csv headers dict, list, and prefixes list present in the Baplie message
            d_csv_cols_to_segments_map = self.__DL.read_json(f"{self.__jsons_static_in_dir}/csv_cols_segments_map.json", s3_bucket=self.__s3_bucket_in)
            d_main_to_sub_segments_map = self.__DL.read_json(f"{self.__jsons_static_in_dir}/main_sub_segments_map.json", s3_bucket=self.__s3_bucket_in)

            df_attributes, baplie_type_from_file_name, baplie_type_from_content = self.__get_df_from_baplie_and_return_types(baplie_path, call_id, file_name, d_csv_cols_to_segments_map, d_main_to_sub_segments_map, self.__s3_bucket_out)
        
            self.__AL.check_baplie_types_compatibility(baplie_type_from_file_name, baplie_type_from_content, call_id)

            if baplie_type_from_file_name == baplie_type_from_content:

                if baplie_type_from_file_name == "container":
                    df_attributes = self.__AL.check_containers_with_no_identifier(df_attributes, call_id) 

                    # in case any container has no EQD_ID
                    df_attributes = self.__AL.fill_containers_missing_serial_nums(df_attributes, call_id, file_type, baplie_type_from_file_name)
                    
                    call_port_seq_num = int(folder_name_split[-2])
                    call_port_name = self.__d_seq_num_to_port_name[call_port_seq_num]
                    call_port_name_base = call_port_name[:5]
                    
                    # check whether duplicate EQD_ID exists
                    self.__AL.check_containers_serial_nums_dups(df_attributes, call_id, file_type)
                    #check POL of all containers
                    df_attributes = self.__AL.check_and_handle_POLs_names(
                                                                    df_attributes,
                                                                    self.__d_port_name_to_seq_num,
                                                                    call_port_name,
                                                                    call_port_seq_num,
                                                                    call_id,
                                                                    file_type
                                                                )
                    
                    # df_attributes preprocessing for used cols
                    # DGS_HAZARD_ID will be processed in a future step in df_all_containers.fillna("", inplace=True)
                    df_attributes = self.__PL.process_slots(df_attributes, 1)
                    df_attributes = self.__PL.add_weights_to_df(df_attributes)
                    df_attributes["EQD_SIZE_AND_TYPE_DESCRIPTION_CODE"] = df_attributes["EQD_SIZE_AND_TYPE_DESCRIPTION_CODE"].astype(str) # iso codes

                    if not call_port_seq_num:
                        df_filled_slots = self.__get_df_filled_slots(df_attributes)
                        self.__AL.check_dup_slots(df_filled_slots, call_id, file_type)
                        
                        l_past_POLs_names = list(set([ POL_name for POL_name in df_attributes["LOC_9_LOCATION_ID"].tolist() if POL_name.isalpha() ]))

                    df_attributes = self.__AL.check_and_handle_PODs_names(
                                                                    df_attributes,
                                                                    self.__d_port_name_to_seq_num,
                                                                    l_past_POLs_names,
                                                                    call_port_seq_num,
                                                                    call_id,
                                                                    file_type
                                                                )
                    
                    if call_port_seq_num: # if a LoadList
                        # clear potential filleds slots for all LLs
                        df_attributes = self.__AL.check_filled_slots_in_LLs(df_attributes, call_id, file_type)
                    
                    # mapping PODs
                    df_attributes = self.__ML.map_PODs_in_df(df_attributes, l_past_POLs_names, call_port_name_base, call_port_seq_num)
                    # self.__AL.add_PODs_anomalies(l_err_container_ids, call_id, file_type)
                    
                    self.__AL.check_ISO_codes(df_attributes, d_iso_codes_map, call_id, file_type)
                    self.__AL.check_weights(df_attributes, call_id, file_type)

                    #TODO write code in section in a function
                    ## START OF SECTION ##
                    l_cols = []
                    for col in df_attributes.columns:
                        if re.match(r"DGS_\d+_", col):
                            col_split = col.split("_")
                            col_name = "_".join([col_split[0]] + col_split[2:] + [col_split[1]])
                            l_cols.append(col_name)
                        
                        else:
                            l_cols.append(col)

                    df_attributes.columns = l_cols
                    list_vals = ['DGS_REGULATIONS_CODE_1','DGS_HAZARD_ID_1','DGS_ADDITIONAL_HAZARD_CLASS_ID_1','DGS_HAZARD_CODE_VERSION_ID_1','DGS_UNDG_ID_1',
                     'DGS_SHIPMENT_FLASHPOINT_DEGREE_1','DGS_MEASUREMENT_UNIT_CODE_1','DGS_PACKAGING_DANGER_LEVEL_CODE_1','DGS_EMS_CODE_1','DGS_HAZARD_MFAG_ID_1',
                     'DGS_DGS_PRIM_LABEL1_1','DGS_DGS_SUB_LABEL1_1','DGS_DGS_SUB_LABEL2_1','DGS_DGS_SUB_LABEL3_1','DGS_ATT_PSN_FUNCTION_CODE_QUALIFIER_1',
                     'DGS_ATT_PSN_TYPE_DESCRIPTION_CODE_1','DGS_ATT_PSN_TYPE_CODE_LIST_ID_CODE_1','DGS_ATT_PSN_TYPE_CODE_LIST_RESPONSIBLE_AGENCY_CODE_1',
                     'DGS_ATT_PSN_DETAIL_DESCRIPTION_CODE_1','DGS_ATT_PSN_DETAIL_CODE_LIST_ID_CODE_1','DGS_ATT_PSN_DETAIL_CODE_LIST_RESPONSIBLE_AGENCY_CODE_1',
                     'DGS_ATT_PSN_DETAIL_DESCRIPTION_1','DGS_ATT_TNM_FUNCTION_CODE_QUALIFIER_1','DGS_ATT_TNM_TYPE_DESCRIPTION_CODE_1',
                     'DGS_ATT_TNM_TYPE_CODE_LIST_ID_CODE_1','DGS_ATT_TNM_TYPE_CODE_LIST_RESPONSIBLE_AGENCY_CODE_1','DGS_ATT_TNM_DETAIL_DESCRIPTION_CODE_1',
                     'DGS_ATT_TNM_DETAIL_CODE_LIST_ID_CODE_1','DGS_ATT_TNM_DETAIL_CODE_LIST_RESPONSIBLE_AGENCY_CODE_1','DGS_ATT_TNM_DETAIL_DESCRIPTION_1',
                     'DGS_MEA_AAA_MEASUREMENT_PURPOSE_CODE_QUALIFIER_1','DGS_MEA_AAA_MEASURED_ATTRIBUTE_CODE_1','DGS_MEA_AAA_MEASUREMENT_UNIT_CODE_1',
                     'DGS_MEA_AAA_MEASURE_1','DGS_FTX_TEXT_SUBJECT_CODE_QUALIFIER_1','DGS_FTX_FREE_TEXT_DESCRIPTION_CODE_1','DGS_FTX_CODE_LIST_ID_CODE_1',
                     'DGS_FTX_CODE_LIST_RESPONSIBLE_AGENCY_CODE_1','DGS_FTX_FREE_TEXT_1']
                    for col in list_vals:
                         if col not in df_attributes.columns:
                               df_attributes[col] = ""

                    df_attributes = self.__add_lowest_DGS_cols_to_df(df_attributes, d_csv_cols_to_segments_map)
                    
                    df_rotation = df_attributes[['EQD_ID', 'LOC_9_LOCATION_ID', 'LOC_11_LOCATION_ID']]

                    ## END OF SECTION ##

                    l_dfs_containers.append(df_attributes)
                    l_dfs_rotation_containers.append(df_rotation)
                    l_containers_folder_names.append(folder_name)

                elif baplie_type_from_file_name == "tank":
                    l_dfs_tanks.append(df_attributes)
                    l_tanks_baplies_paths.append(baplie_path)
                    l_tanks_folder_names.append(folder_name)
                
                else:
                    self.logger.info("*"*80)
                    continue

                self.logger.info("*"*80)

            else: continue
            
        self.__AL.check_if_errors()
        
        self.logger.info("Reading fuel_costs.csv file from referential cost folder...")
        fuel_costs_df = self.__DL.read_csv(self.__fuel_costs_path,  na_values=DEFAULT_MISSING, s3_bucket=self.__s3_bucket_in)
        fuel_data_dict = fuel_costs_df.set_index('FUEL_TYPE')['COST_USD'].to_dict()
        self.logger.info("Generating final rotation.csv...")
        rotations = rotation(self.logger, vessel, rotation_intermediate, l_dfs_rotation_containers, l_containers_folder_names, self.__d_seq_num_to_port_name, rotation_csv_maps, RW_costs, consumption_df, fuel_data_dict)
        df_rotation_final = rotations.get_rotations_final()
        rotation_csv_name = "rotation.csv"
        rotation_csv_path = f"{self.__py_scripts_out_dir}/{rotation_csv_name}"
        self.__DL.write_csv(df_rotation_final, rotation_csv_path, s3_bucket=self.__s3_bucket_out)
        
        # saving after to save time as an error might be thrown before
        for i, df in enumerate(l_dfs_containers):
            folder_name = l_containers_folder_names[i]
            port_containers_csv_name = f"{folder_name}_container.csv"
            port_containers_csv_path = f"{self.__py_scripts_out_dir}/{port_containers_csv_name}"
            self.__DL.write_csv(df, port_containers_csv_path, s3_bucket=self.__s3_bucket_out)

        #lashing
        self.logger.info(f"Calculating Lashing forces for Onboard Containers...")
        lashing_df = lashing.perform_lashing_calculations(l_dfs_containers[0], l_containers_folder_names[1][-5:])
        lashing_csv_name = f"on_board_lashing.csv"
        self.logger.info("*"*80)
        
        self.logger.info(f"Saving OnBoard lashing Calculation...")
        lashing_csv_path = f"{self.__py_scripts_out_dir}/{lashing_csv_name}"
        self.__DL.write_csv(lashing_df, lashing_csv_path, s3_bucket=self.__s3_bucket_out)
        
        for i, df in enumerate(l_dfs_tanks):
            port_tanks_csv_name = f"{l_tanks_folder_names[i]}_tank.csv"
            port_tanks_csv_path = f"{self.__py_scripts_out_dir}/{port_tanks_csv_name}"
            self.__DL.write_csv(df, port_tanks_csv_path, s3_bucket=self.__s3_bucket_out)
        
        # combined csv from all containers csvs
        self.logger.info(f"Saving extracted info from {baplie_type_from_file_name}s baplies...")
        df_all_containers = pd.concat(l_dfs_containers, axis=0, ignore_index=True)
        df_all_containers.fillna("", inplace=True)
        self.__DL.write_csv(df_all_containers, self.__all_containers_csv_path, s3_bucket=self.__s3_bucket_out)

        df_DG_classes_expanded = self.__get_df_DG_classes_expanded()
        imdg_codes_list_csv_path = f"{self.__static_in_dir}/hz_imdg_exis_subs.csv" if self.__static_in_dir else "hz_imdg_exis_subs.csv"
        imdg_codes_df = self.__DL.read_csv(imdg_codes_list_csv_path, DEFAULT_MISSING, ",", self.__s3_bucket_in).astype(str) 
        d_DG_loadlist_config = self.__DL.read_json(f"{self.__jsons_static_in_dir}/DG_loadlist_config.json", self.__s3_bucket_in)
        
        dg_instance = DG(self.logger, vessel, d_DG_loadlist_config, imdg_codes_df, self.__DG_Rules)
        
        self.logger.info("Extracting and saving DG LoadList...")
        df_DG_loadlist = dg_instance.get_df_dg_loadlist(df_all_containers)
        DG_csv_name =  "DG Loadlist.csv"
        DG_csv_path = f"{self.__py_scripts_out_dir}/{DG_csv_name}"
        self.__DL.write_csv(df_DG_loadlist, DG_csv_path, self.__s3_bucket_out)

        self.logger.info("Extracting and saving DG LoadList Exclusion...")
        df_loadlist_exclusions = dg_instance.get_dg_exclusions(df_DG_loadlist)
        DG_csv_name =  "DG Loadlist Exclusions.csv"
        DG_csv_path = f"{self.__py_scripts_out_dir}/{DG_csv_name}"
        self.__DL.write_csv(df_loadlist_exclusions, DG_csv_path, self.__s3_bucket_out)
        
    
        df_DG_classes_expanded_updated = dg_instance.output_adjusted_table_7_2_4(df_DG_classes_expanded, df_DG_loadlist)
        df_DG_classes_grouped = self.__PL.get_df_DG_classes_grouped(df_DG_loadlist, df_DG_classes_expanded_updated)
        df_DG_classes_grouped_to_save = self.__PL.get_df_DG_classes_grouped_to_save(df_DG_classes_grouped)
        df_DG_classes_grouped_to_save_csv_path = f"{self.__py_scripts_out_dir}/table_7_2_4_grouped.csv"
        self.__DL.write_csv(df_DG_classes_grouped_to_save, df_DG_classes_grouped_to_save_csv_path, self.__s3_bucket_out)
        
        l_container_groups_containers_lines = self.__output_CPLEX_input_container_csvs(df_all_containers, df_filled_slots, df_DG_classes_expanded, d_iso_codes_map)
        self.logger.info("Extracting and saving DG LoadList Exclusion Zones & Nb DG ...")
        df_grouped_containers = self.__PL.get_list_of_lines_as_df(l_container_groups_containers_lines)
        df_cg_exclusion_zones, df_cg_exclusion_zones_nb_dg = dg_instance.get_exclusion_zones(df_grouped_containers, df_loadlist_exclusions)

        f_cg_exclusion_zones_name =  "DG Container Groups Exclusion Zones.csv"
        f_cg_exclusion_zones_csv_path = f"{self.__py_scripts_out_dir}/{f_cg_exclusion_zones_name}"
        self.__DL.write_csv(df_cg_exclusion_zones, f_cg_exclusion_zones_csv_path, self.__s3_bucket_out)

        f_cg_exclusion_zones_nb_dg_name =  "DG Container Groups Exclusion Zones Nb DG.csv"
        f_cg_exclusion_zones_nb_dg_csv_path = f"{self.__py_scripts_out_dir}/{f_cg_exclusion_zones_nb_dg_name}"
        self.__DL.write_csv(df_cg_exclusion_zones_nb_dg, f_cg_exclusion_zones_nb_dg_csv_path, self.__s3_bucket_out)

        if len(l_dfs_tanks):                    
            df_tanks_final = pd.concat(l_dfs_tanks, axis=0, ignore_index=True)
            df_tanks_final.fillna("", inplace=True)
            all_tanks_csv_name = "csv_combined_tanks.csv"
            all_tanks_csv_path = f"{self.__py_scripts_out_dir}/{all_tanks_csv_name}"
            self.__DL.write_csv(df_tanks_final, all_tanks_csv_path, s3_bucket=self.__s3_bucket_out)

        #containers final 
        self.logger.info("Generating final containers.csv file...")
        df_containers_config = self.__DL.read_json(f"{self.__jsons_static_in_dir}/csv_combined_containers_final.json", self.__s3_bucket_in)
        #POL POD revenue
        POL_POD_revenues_csv_name = "Revenues by Size Type POL POD.csv"
        POL_POD_revenues_csv_path = f"{self.__service_static_in_dir}/{self.__service_code}/{POL_POD_revenues_csv_name}"
        df_POL_POD_revenues = self.__DL.read_csv(POL_POD_revenues_csv_path, DEFAULT_MISSING, ";", self.__s3_bucket_in).astype(str)
        
        df_uslax_path =  f"{self.__static_in_dir}/los_angeles.csv" if self.__static_in_dir else "los_angeles.csv"
        df_uslax = self.__DL.read_csv(df_uslax_path, DEFAULT_MISSING, ";", self.__s3_bucket_in).astype(str)
        
        # loading stacks
        stacks_csv_name = "Stacks Extrait Prototype MP_IN.csv"
        stacks_csv_path = f"{self.__vessels_static_in_dir}/{stacks_csv_name}"
        df_stacks = self.__DL.read_csv(stacks_csv_path, DEFAULT_MISSING, ";", self.__s3_bucket_in).astype(str)
        
        df_final_containers_csv_name = "containers.csv"
        df_final_containers = self.__PL.get_df_containers_final(df_all_containers, df_containers_config, d_iso_codes_map, df_uslax, df_POL_POD_revenues, df_rotation_final, df_stacks, df_DG_loadlist, df_loadlist_exclusions)
        df_final_containers_csv_path = f"{self.__py_scripts_out_dir}/{df_final_containers_csv_name}"
        self.__DL.write_csv(df_final_containers, df_final_containers_csv_path, s3_bucket=self.__s3_bucket_out)
        
        self.__output_filled_subtanks(l_tanks_baplies_paths)
        self.logger.info("Preprocessing first Execution: Done...")
        self.logger.info("*"*80)

    def __get_CPLEX_output(self) -> 'tuple[pd.DataFrame, dict, dict]':
        df_cplex_out = self.__DL.read_csv(f"{self.__cplex_out_dir}/output.csv", na_values=DEFAULT_MISSING, s3_bucket=self.__s3_bucket_out)
        df_cplex_out = self.__PL.process_slots(df_cplex_out, "SLOT_POSITION", True)
        l_cplex_containers_ids = df_cplex_out["REAL_CONTAINER_ID"].tolist()
        l_cplex_slots = []
        for pos in df_cplex_out["SLOT_POSITION"].tolist():
            if pos != pos:
                l_cplex_slots.append("")
            else:
                l_cplex_slots.append(pos)
        d_cplex_containers_slot_by_id = {
            container_id: slot_position for container_id, slot_position in list(zip(l_cplex_containers_ids, l_cplex_slots))
        }

        d_cplex_containers_slot_by_id_keys = list(d_cplex_containers_slot_by_id.keys())

        return df_cplex_out, d_cplex_containers_slot_by_id, d_cplex_containers_slot_by_id_keys

    def __update_csvs_with_CPLEX_output(self, d_cplex_containers_slot_by_id: dict, d_cplex_containers_slot_by_id_keys: dict) -> 'tuple[pd.DataFrame, pd.DataFrame]':
        for i, csv_path in enumerate(self.__l_POL_POD_csvs_paths):
            df = self.__DL.read_csv(csv_path, na_values=DEFAULT_MISSING, s3_bucket=self.__s3_bucket_out)
            
            df.fillna("", inplace=True)
            df = self.__PL.process_slots(df, 1, True)

            l_containers_ids = df["EQD_ID"].tolist()
            l_slot_positions = df.iloc[:, 1].tolist()

            l_slot_positions = []
            for pos in df.iloc[:, 1].tolist():
                if pos != pos:
                    l_slot_positions.append("")
                else:
                    l_slot_positions.append(pos)
            
            l_containers_to_drop_indices = []
            for j, container_id in enumerate(l_containers_ids):
                if container_id in d_cplex_containers_slot_by_id_keys:
                    l_slot_positions[j] = d_cplex_containers_slot_by_id[container_id]

                elif i == 0:
                    l_containers_to_drop_indices.append(j)

            df.iloc[:, 1] = l_slot_positions
            
            if i == 0:
                df.drop(l_containers_to_drop_indices, inplace=True)
                df.reset_index(inplace=True, drop=True)
            
            self.__DL.write_csv(df, csv_path, s3_bucket=self.__s3_bucket_out)

        l_dfs_containers, l_dfs_filled_slots = [], []
        for file_name in self.__DL.list_files_in_path(self.__py_scripts_out_dir, s3_bucket=self.__s3_bucket_out):
            if "_container.csv" not in file_name:
                continue
            
            csv_path = f"{self.__py_scripts_out_dir}/{file_name}"
            df_containers = self.__DL.read_csv(csv_path, na_values=DEFAULT_MISSING, s3_bucket=self.__s3_bucket_out)
            df_containers.fillna("", inplace=True)
            df_containers = self.__PL.process_slots(df_containers, 1, True)

            if csv_path in self.__l_POL_POD_csvs_paths:
                df_filled_slots_temp = self.__get_df_filled_slots(df_containers)
                l_dfs_filled_slots.append(df_filled_slots_temp)
            
            l_dfs_containers.append(df_containers)
        
        df_filled_slots = pd.concat(l_dfs_filled_slots, axis=0, ignore_index=True)
        df_filled_slots.fillna("", inplace=True)

        df_all_containers = pd.concat(l_dfs_containers, axis=0, ignore_index=True)
        df_all_containers.fillna("", inplace=True)
        self.__DL.write_csv(df_all_containers, self.__all_containers_csv_path, s3_bucket=self.__s3_bucket_out)
        return df_all_containers, df_filled_slots

    def __output_bayplan(self, df_cplex_out: pd.DataFrame, d_cplex_containers_slot_by_id: dict) -> None:
        d_cplex_containers_slot_by_id_keys = list(d_cplex_containers_slot_by_id.keys())
        l_containers_ids = []
        containers_count = 0 # for logging
        for i, csv_path in enumerate(self.__l_POL_POD_csvs_paths):
            
            df  = self.__DL.read_csv(csv_path, na_values=DEFAULT_MISSING, s3_bucket=self.__s3_bucket_out)
            df = self.__PL.process_slots(df, 1, True)
            l_containers_ids_temp = df["EQD_ID"].tolist()   
            l_containers_ids += l_containers_ids_temp
   
        containers_data_list = []
        for i, path in enumerate(self.__l_POL_POD_containers_baplies_paths):
            folder_name = self.__DL.get_folder_name_from_path(path)
            # f_baplie = open(path, 'r')
            # baplie_message = f_baplie.read()
            baplie_message = self.__DL.read_file(path, s3_bucket=self.__s3_bucket_out)
            baplie_delimiter = self.__DL.get_baplie_delimiter(baplie_message)
            segments_list = baplie_message.split(baplie_delimiter)
            # f_baplie.close()
            for idx, segment in enumerate(segments_list):
                segment_split = segment.split("+")
                first_el = segment_split[0]
                second_el = segment_split[1]

                if first_el == "LOC":
                    if second_el not in ["5", "61"]: # code identifiers for the 2 LOC headers in the message header
                        first_segment_in_body_idx = idx
                        break

            header_segments_list = segments_list[:first_segment_in_body_idx]
            segments_list_no_header = segments_list[first_segment_in_body_idx:]
            new_data_flag = self.__DL.get_new_data_flag(segments_list_no_header)
            
            start_idx = 0
            new_container_data_flag_in_baplie = new_data_flag.replace("_", "+")
            for idx, segment in enumerate(segments_list_no_header):
                if idx != 0: # to disregards the first segment as it belongs to the first container
                    if segment[:7] == new_container_data_flag_in_baplie: # LOC+147, LOC+ZZZ, etc... represents the start of segments for a new container
                        # in the following segment of code, we slice the list from start_idx to the index
                        # just before idx (where the segments for a new container begins): the index just before
                        # idx will be the end of the segments for the old container w.r.t. the new container

                        # preprocessing step to remove duplicates
                        segments_list_no_header_with_dups = segments_list_no_header[start_idx:idx]
                        
                        segments_list_no_header_no_dups = []
                        segments_list_no_header_no_dups = [segment for segment in segments_list_no_header_with_dups if segment not in segments_list_no_header_no_dups]
                        first_segment_split = segments_list_no_header_no_dups[0].split("+")
                        container_id, POD_name = None, None
                        for current_segment in segments_list_no_header_no_dups:

                            current_segment_split = current_segment.split("+")
                            header = current_segment_split[0]
                            if header == "EQD":
                                container_id = current_segment_split[2].split(":")[0]
                                EQD_segment = current_segment_split

                            if header == "LOC" and current_segment_split[1] == "11":
                                POD_name = current_segment_split[2]
                            
                            if container_id and POD_name:
                                if container_id in d_cplex_containers_slot_by_id_keys:
                                    l_slot = EQD_segment[2].split(":")
                                    slot = d_cplex_containers_slot_by_id[container_id]
                                    
                                    if len(slot):
                                        slot = "0" + self.__PL.process_slot_str(slot, len(slot))
                                    
                                    l_slot[0] = slot
                                    slot_slice = ":".join(l_slot)
                                    first_segment_split[2] = slot_slice
                                    first_segment_joined = "+".join(first_segment_split)
                                    segments_list_no_header_no_dups[0] = first_segment_joined

                        if container_id in l_containers_ids and POD_name != self.__target_port:
                            containers_data_list += segments_list_no_header_no_dups
                            containers_count += 1
                        start_idx = idx # let start_idx be the index where the segments of a new container start                        

            # when the last flag is found (the start of the data for the last container), another flag will not be found after, hence, we need to add it
            # after the loop ends.
            # PS: the benifit of leaving this part here is not iterate over the whole list of segments from the beginning to find the segments that has the first
            #     2 characters equal to "UN".
            last_container_data_with_dups = segments_list_no_header[start_idx:]

            # removing the tail of the message
            tail_headers_names_indices_list = [idx for idx, segment in enumerate(last_container_data_with_dups) if segment[:2] == "UN"]
            if len(tail_headers_names_indices_list):
                tail_start_idx = tail_headers_names_indices_list[0]
                tail_segments_list = last_container_data_with_dups[tail_start_idx:]
                last_container_data_with_dups_no_tail = last_container_data_with_dups[:tail_start_idx]


            else:
                last_container_data_with_dups_no_tail = last_container_data_with_dups
                tail_segments_list = []

            if last_container_data_with_dups_no_tail[-1] == "":
                last_container_data_with_dups_no_tail = last_container_data_with_dups_no_tail[:-1]

            last_container_data = []
            last_container_data_without_dups = [segment for segment in last_container_data_with_dups_no_tail if segment not in last_container_data]

            container_id, POD_name = None, None
            for current_segment in last_container_data_without_dups:
                current_segment_split = current_segment.split("+")
                header = current_segment_split[0]
                if header == "EQD":
                    container_id = current_segment_split[2].split(":")[0]

                if header == "LOC" and current_segment_split[1] == "11":
                    POD_name = current_segment_split[2]
            
            if container_id and POD_name:
                if container_id in d_cplex_containers_slot_by_id_keys:
                    l_slot = EQD_segment[2].split(":")
                    slot = d_cplex_containers_slot_by_id[container_id]
                    
                    if len(slot):
                        slot = "0" + self.__PL.process_slot_str(slot, len(slot))
                    
                    l_slot[0] = slot
                    slot_slice = ":".join(l_slot)
                    first_segment_split[2] = slot_slice
                    first_segment_joined = "+".join(first_segment_split)
                    last_container_data_without_dups[0] = first_segment_joined
            #TODO investigate issue
            if container_id in l_containers_ids and POD_name != self.__target_port:
                containers_data_list += last_container_data_without_dups
                containers_count += 1
            
            self.logger.info(f"There are {len(containers_data_list)} containers in the baplie message...")

            self.logger.info(f"There are {len(df_cplex_out)} containers in the slot planning results...")

        all_semgents_count = len(header_segments_list) + len(containers_data_list) + len(tail_segments_list)
        for i, segment in enumerate(tail_segments_list):
            if segment[:3] == "UNT":
                segment_split = segment.split("+")
                segment_split[1] = str(all_semgents_count)
                tail_segments_list[i] = "+".join(segment_split)

        l_all_semgents = header_segments_list + containers_data_list + tail_segments_list
        path_to_save = f"{self.__cplex_out_dir}/Bayplan.edi"
        self.__DL.output_bayplan_edi(path_to_save, baplie_delimiter, l_all_semgents, self.__s3_bucket_out)

    def __run_reexecution(self) -> None:
        
        files_in_output = self.__DL.list_files_in_path(self.__cplex_out_dir, self.__s3_bucket_out)
        self.__AL.check_if_no_output_postprocess(files_in_output)
        
        df_cplex_out, d_cplex_containers_slot_by_id, d_cplex_containers_slot_by_id_keys = self.__get_CPLEX_output()
            
        df_all_containers, df_filled_slots = self.__update_csvs_with_CPLEX_output(d_cplex_containers_slot_by_id, d_cplex_containers_slot_by_id_keys)
            # df_DG_classes_expanded = self.__get_df_DG_classes_expanded()
            # # df_DG_classes_grouped = self.__PL.get_df_DG_classes_grouped(df_all_containers, df_DG_classes_expanded)
            # self.__output_CPLEX_input_container_csvs(df_all_containers, df_filled_slots, df_DG_classes_expanded, d_iso_codes_map)

        self.__output_bayplan(df_cplex_out, d_cplex_containers_slot_by_id)

    def run_main(self) -> None:
        if not self.__reuse_previous_results:
            self.logger.info("Run first execution")
            self.__run_first_execution()

        else:
            self.logger.info("Run reexecution")
            self.__run_reexecution()
