import pandas as pd
import numpy as np 
import re

DEFAULT_MISSING = pd._libs.parsers.STR_NA_VALUES
if "" in DEFAULT_MISSING:
    DEFAULT_MISSING = DEFAULT_MISSING.remove("")

import logging
# Set up the root logger with the desired log level and format


# Disable the default stream handler of the root logger
root_logger = logging.getLogger()
root_logger.handlers = []

# # Create a file handler with 'w' filemode to truncate the file
# file_handler = logging.FileHandler('log_file.log', mode='w')
# file_handler.setLevel(logging.DEBUG)

# # Set the formatter for the file handler
# file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
# file_handler.setFormatter(file_formatter)

# # # Add the file handler to the root logger
# root_logger.addHandler(file_handler)

# # Create a console handler
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)
# logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

# # # Set the formatter for the console handler
console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console_handler.setFormatter(console_formatter)

# # # # Add the console handler to the root logger
root_logger.addHandler(console_handler)

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
            
        self.__dynamic_in_dir = f"{simulation_dir}/in"
        self.__py_scripts_out_dir = f"{simulation_dir}/intermediate"
        
        self.__all_containers_csv_path = f"{self.__py_scripts_out_dir}/csv_combined_containers.csv"
        self.__rotation_intermediate_path = f"{self.__dynamic_in_dir}/rotation.csv"
        
        self.__cplex_out_dir = f"{simulation_dir}/out"
        self.__error_log_path = f"{self.__cplex_out_dir}/error.csv"
       
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
        for i, folder_name in enumerate(sorted(self.__DL.list_folders_in_path(self.__dynamic_in_dir, self.__s3_bucket_out))): 
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
                            
                    elif file_name == "Tank.edi" :
                        tank_flag = 1
                        
                    elif file_name == "LoadList.edi":
                        loadlist_flag = 1
                        
                        
                        if seq_num == 1:
                            self.__get_first_two_ports_baplie_and_csv_paths(baplies_dir, file_name, folder_name)   
    
                    
                
            # error in case of wrong edi files
            # first folder with no OnBoard.edi
            if not i:
                self.__AL.check_onboard_edi(onboard_call, call_id)
            
            # not first folder but LoadList.edi without Tank.edi or Tank.edi without LoadList.edi
            else:
                self.__AL.check_loadlist_and_tank_edi_files(loadlist_flag, tank_flag, call_id)
            
            port_name = folder_name_split[-1]
            self.__d_seq_num_to_port_name[seq_num] = port_name
        
        
        # self.__AL.check_if_errors()
        
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
        self.__AL.check_if_errors(self.__error_log_path, self.__s3_bucket_out)
        
        onboard_csv_name = f"{self.__vessel_id} Containers OnBoard Loadlist 0.csv"
        onboard_csv_path = f"{self.__py_scripts_out_dir}/{onboard_csv_name}"
        self.__DL.write_csv(df_onboard_loadlist, onboard_csv_path, self.__s3_bucket_out)

        return df_onboard_loadlist
    
    # all dg in one container listed separately not the lowest as agreed.
    # Extract more variable into df_all_containers, aka. (sublabel2 if exists, TLQ(limited quantity), etc. ) 
    def __output_DG_loadlist(self, df_all_containers: pd.DataFrame,d_DG_enrichment_map: dict, imdg_codes_df:pd.DataFrame) -> pd.DataFrame:
        self.logger.info("Extracting and saving DG LoadList...")
        
        # d_DG_loadlist_cols_map = self.__DL.read_json(f"{self.__jsons_static_in_dir}/DG_loadlist_cols_map.json", self.__s3_bucket_in)
        df_copy = df_all_containers.copy()
    
        if "DGS_ATT_AGR_DETAIL_DESCRIPTION_CODE_1" not in df_copy.columns:
            df_copy["DGS_ATT_AGR_DETAIL_DESCRIPTION_CODE_1"] = ""
    
    
        naming_schema = d_DG_enrichment_map["DG_LOADLIST_SCHEMA"]
        
        df_DG_loadlist = self.__PL.get_df_DG_loadlist_exhaustive(df_all_containers, naming_schema)
        # df_DG_loadlist.to_csv("output_old.csv")
        # fill for now as all "x"
        # Need to explore type of for any indicator to other than closed freight container 
        # Assumption is all containers are closed and and DGs' are in packing 
        df_DG_loadlist["Closed Freight Container"] = "x"
        df_DG_loadlist["Package Goods"] = "x"

        # Mapping for Packaging Group (1,2,3) -> (I,II,III)
        PGr_map = d_DG_enrichment_map["PACKAGING_GROUP"]
        df_DG_loadlist["PGr"] = df_DG_loadlist["PGr"].map(PGr_map)

        # Stowage Category 
        # fix data type and try 
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
        for idx, row in df_DG_loadlist.iterrows():

            if row['UNNO'] == '1950':

                if 'MAX' in row['Proper Shipping Name (Paragraph B of DOC)'].upper() and 'TLQ' not in row['Limited Quantity']: 
                    row['Limited Quantity'] = 'TLQ'

                if 'WASTE' in row['PSN'] and 'WASTE' not in row['Proper Shipping Name (Paragraph B of DOC)'].upper():
                    df_DG_loadlist.drop(idx, inplace=True)
                
                if 'TLQ' in row['Limited Quantity'] and 'None' in row['LQ']: 
                    df_DG_loadlist.drop(idx, inplace=True)
                
                if 'TLQ' not in row['Limited Quantity'] and 'None' not in row['LQ']:
                    df_DG_loadlist.drop(idx, inplace=True)
            
            # assumption for now take lowest CATEGORY
            # if row['UNNO'] == ['3480', '3481']:
            #     if row['PSN'] not in row['Proper Shipping Name (Paragraph B of DOC)'].upper():
            #         df_DG_loadlist.drop(idx, inplace=True)
                
            if row['UNNO'] == '3528':
                if 'BELOW' in row['VARIATION'].upper() and row['flashpoints'] >= 23:
                    df_DG_loadlist.drop(idx, inplace=True)
                
                if 'ABOVE' in row['VARIATION'].upper() and row['flashpoints'] < 23:
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
        stow = []
        seg = []
        df_DG_loadlist.fillna('', inplace=True)
        df_DG_loadlist = df_DG_loadlist.reset_index()
        for idx, row in df_DG_loadlist.iterrows():
            stow_list = df_DG_loadlist["DGIES_STOW"].iloc[idx].split()
            seg_list = df_DG_loadlist["DGIES_SEG"].iloc[idx].split()
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

        df_DG_loadlist['Stowage and segregation'] = stow
        df_DG_loadlist['SegregationGroup'] = seg
        df_DG_loadlist.set_index('index',inplace=True)

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

        Zone_map = d_DG_enrichment_map["PORT_ZONE"]
        df_DG_loadlist["Zone"] = df_DG_loadlist["POL"].str[:5].map(Zone_map)
        
        #Not Permitted Bay 74 
        Not_Permitted_Bay_74_list = d_DG_enrichment_map["UNNO_not_permitted_bay_74"]
        df_DG_loadlist["not permitted bay 74"] = df_DG_loadlist.apply(lambda x: "x" if x['UN'] in Not_Permitted_Bay_74_list  else "" , axis=1)
        # Class_Only_Not_Permitted_Bay_74_dict = d_DG_enrichment_map["Not_permitted_Bay74_Class_only"]
        Dynamic_Not_Permitted_Bay_74_dict = d_DG_enrichment_map["Dynamic_Not_Permitted_Bay74"]
    
        df_copy = df_DG_loadlist.copy(deep=False)
        df_copy['FlashPoints'].fillna("-1000", inplace=True)
        df_copy['FlashPoints'] = pd.to_numeric(df_copy['FlashPoints'])

        # # to be set later in function for modularity 
        # #def __classify_flashpoint(self, fp_low_threshold, fp_high_threshold, dg_loadlist: pd.DataFrame) -> pd.DataFrame:

        fp_low_threshold = float(d_DG_enrichment_map["FP_Threshold_Low"]) 
        fp_high_theshold = float(d_DG_enrichment_map["FP_Threshold_High"])  
        
        PE_Conditions = [
            (df_copy['FlashPoints'] == -1000.0),
            (df_copy['FlashPoints'] < fp_low_threshold),
            (df_copy['FlashPoints']  >= fp_low_threshold) & (df_copy['FlashPoints']  <= fp_high_theshold),
            (df_copy['FlashPoints']  >  fp_high_theshold)
        ]
        PE_Categories = ['0', '1', '2','3']
        temp_list= []
        temp_list = np.select(PE_Conditions, PE_Categories)
        df_copy['FlashPoint_Category'] = temp_list   

        df_copy["new"] = df_copy.apply(lambda x: ','.join([str(x['Class']), str(x['Liquid']),str(x['Solid']), str(x['FlashPoint_Category'])]), axis=1)   
        df_copy["not permitted bay 74_dynamic"]= df_copy["new"].map(Dynamic_Not_Permitted_Bay_74_dict)  
        df_copy["not permitted bay 74"] = df_copy.apply(lambda x: "x" if ((x["not permitted bay 74_dynamic"] == "x" and x["not permitted bay 74"] != "x") or (x["not permitted bay 74"] == "x")) else "" , axis=1)
        # Loading Remarks 
        segregation_group_map = d_DG_enrichment_map["SEGREGATION_GROUP"]
        df_copy["Loading remarks"] = df_copy["SegregationGroup"].apply(lambda row: "" if row == "" else ", ".join([segregation_group_map[value] for value in row.split(", ")]))


        df_DG_loadlist = df_copy.copy()        

        df_DG_loadlist = df_DG_loadlist[["Serial Number","Operator","POL","POD","Type","Closed Freight Container",
                           "Weight","UN","Class","SubLabel1",
                           "DG-Remark (SW5 = Mecanical Ventilated Space if U/D par.A DOC)","FlashPoints",
                           "Loading remarks","Limited Quantity","Marine Pollutant","PGr","Liquid","Solid",
                           "Flammable","Non-Flammable","Proper Shipping Name (Paragraph B of DOC)","SegregationGroup",
                           "SetPoint","Stowage and segregation","Package Goods","Stowage Category","not permitted bay 74",
                           "Zone"]]
                
        DG_csv_name =  f"{self.__vessel_id} DG Loadlist.csv"
        DG_csv_path = f"{self.__py_scripts_out_dir}/{DG_csv_name}"
        self.__DL.write_csv(df_DG_loadlist, DG_csv_path, self.__s3_bucket_out)
        
        return df_DG_loadlist
## DG Exclusion LoadList Related Functions 
#==============================================================================================================================================
    # getting the DG macro category depending on elements provided by the load list
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

#===============================================================================================================================================

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
        self.__AL.check_if_errors(self.__error_log_path, self.__s3_bucket_out)

        stowing_info_csv_name = f"{self.__vessel_id} Containers Stowing Info 0.csv"
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
        
        overstowing_subbays_csv_name = f"{self.__vessel_id} Overstowing Subbays 0.csv"
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

        groups_completed_csv_name = f"{self.__vessel_id} Container Groups Completed 0.csv"
        groups_completed_csv_path = f"{self.__py_scripts_out_dir}/{groups_completed_csv_name}"
        self.__DL.write_csv_lines(l_container_groups_completed_lines, groups_completed_csv_path, self.__s3_bucket_out)
        
        groups_containers_csv_name = f"{self.__vessel_id} Container Groups Containers.csv"
        groups_containers_csv_path = f"{self.__py_scripts_out_dir}/{groups_containers_csv_name}"
        self.__DL.write_csv_lines(l_container_groups_containers_lines, groups_containers_csv_path, self.__s3_bucket_out)

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

    #TODO: check all case scenarios / animal oil category implementation    
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
    
    def __output_CPLEX_input_container_csvs(self, df_all_containers: pd.DataFrame, df_filled_slots: pd.DataFrame, df_DG_classes_expanded: pd.DataFrame, d_iso_codes_map: dict) -> None:
        
        
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

        ## DG LoadList ##
        # load referential data 
        if not self.__reuse_previous_results:
            d_DG_enrichment_map = self.__DL.read_json(f"{self.__jsons_static_in_dir}/DG_loadlist_enrichment_map.json", self.__s3_bucket_in)
            
            imdg_codes_list_csv_path = f"{self.__static_in_dir}/hz_imdg_exis_subs.csv" if self.__static_in_dir else "hz_imdg_exis_subs.csv"
            # imdg_codes_list_csv_path = f"{self.__static_in_dir}/hz_imdg_exis_subs.csv" 
            
            imdg_codes_df = self.__DL.read_csv(imdg_codes_list_csv_path, DEFAULT_MISSING, ",", self.__s3_bucket_in).astype(str)
        
        
            df_DG_loadlist = self.__output_DG_loadlist(df_all_containers, d_DG_enrichment_map, imdg_codes_df)

            df_DG_classes_expanded_updated = self.__output_adjusted_table_7_2_4( df_DG_classes_expanded, df_DG_loadlist)
            df_DG_classes_grouped = self.__PL.get_df_DG_classes_grouped(df_DG_loadlist, df_DG_classes_expanded_updated)
            df_DG_classes_grouped_to_save = self.__PL.get_df_DG_classes_grouped_to_save(df_DG_classes_grouped)
            df_DG_classes_grouped_to_save_csv_path = f"{self.__py_scripts_out_dir}/table_7_2_4_grouped.csv"
            self.__DL.write_csv(df_DG_classes_grouped_to_save, df_DG_classes_grouped_to_save_csv_path, self.__s3_bucket_out)

        #======================================================================================================================================  
        #DG Exclusions
        #======================================================================================================================================
            self.logger.info("Extracting and saving DG LoadList Exclusion Zones...")
            dg_exclusions_csv_path = f"{self.__vessels_static_in_dir}/DG Exclusions.csv"
            dg_exclusions_df =  self.__DL.read_csv(dg_exclusions_csv_path, DEFAULT_MISSING, ";", self.__s3_bucket_in).astype(str)
            dg_exclusions_df.set_index('DG category', inplace=True)


            # def get_dg_exclusion_category(self, dg_exclusion_csv_path) -> dict:
            
            # return dg_exclusion_by_category       
        

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

            #### Using the (macro-)categories defined by the vessel doc

            l_explosion_protect_IIB_T4  = d_DG_enrichment_map["l_explosion_protect_IIB_T4"]
            l_IMDG_Class_1_S = d_DG_enrichment_map["40-20_Class_1.4S"]
            #### In addition to the (macro-)categories derived from the compliance doc, we have special exclusions zones linked to stowage categories
        
            #### The general exclusion data takes the form:
            #- bay (differentiating 20' and 40')
            #- rows (most of the time it will be all rows, storing None, but it can be a list of rows)
            #- tiers (first part, 0 (hold) or 1 (deck), second part, all tiers inside, storing None if all tiers, but it can be a list of tiers)
            #- In order to include those in sets, lists of rows and of tiers are stored as frozensets
                
            l_macro_bays = [2 + 4 * n for n in range(0, 24)]
            # list of all bays on deck
            l_deck_bays = l_macro_bays.copy()
            l_deck_bays.extend([n-1 for n in l_macro_bays if n not in [74, 94]])
            l_deck_bays.extend([n+1 for n in l_macro_bays if n not in [74, 94]])
            l_deck_bays.sort()
            # list of all bays in hold
            l_hold_bays = [n for n in l_macro_bays if n not in [74, 94]]
            l_hold_bays.extend([n-1 for n in l_macro_bays if n not in [74, 94]])
            l_hold_bays.extend([n+1 for n in l_macro_bays if n not in [74, 94]])
            l_hold_bays.sort()

            # list of all zones under the deck, those zones are forbidden for stowage categories C and D
            l_hold_zones = ["%03d0" % n for n in l_hold_bays]

            l_sw_1 = [
                ('034', None, ('0', frozenset({'02', '04', '06', '08', '10', '12', '14', '16'}))),
                ('035', None, ('0', frozenset({'02', '04', '06', '08', '10', '12', '14', '16'}))),
                ('037', None, ('0', frozenset({'02', '04', '06', '08', '10', '12', '14', '16'}))),
                ('038', None, ('0', frozenset({'02', '04', '06', '08', '10', '12', '14', '16'}))),
                ('077', frozenset({'08', '09', '10', '11', '12', '13', '14', '15', '16'}), ('0', frozenset({'10'}))),
                ('078', frozenset({'08', '09', '10', '11', '12', '13', '14', '15', '16'}), ('0', frozenset({'10'}))),
                ('079', frozenset({'08', '09', '10', '11', '12', '13', '14', '15', '16'}), ('0', frozenset({'10'})))
            ]

            # SW2, forbid a simple list
            l_sw_2 = ['0341', '0351', '0371', '0381']

            # polmar,
            # exclude all positions for bays 1, 2, 94 on the dock
            l_polmar = ['0011', '0021', '0941']
            # for other bays, from 3 to 91, only external rows
            l_decks_polmar_extension = ["%03d" % n for n in l_deck_bays if n not in [1,2,94]]
            # bay 3 is a special case, external rows are 17 and 18 instead of 19 and 20
            # and it is the first in the list from 3 to 91
            l_rows_polmar_extension = [frozenset({'17', '18'})]
            # all other bays
            l_rows_polmar_extension.extend([frozenset({'19', '20'}) for n in l_deck_bays if n not in [1,2,3,94]])
                                        
            l_polmar.extend([(x[0], x[1], ('1', None)) for x in zip(l_decks_polmar_extension, l_rows_polmar_extension)])

            ### Creating a dictionnary for each container x loading port, containing the set of excluded zones
                
            ##### Depending on creating for master planning, only doc, and only at bay level, or for slot planning, with all exclusions
            #- Master = Simple rules (by bay) = Compliance Doc + Simple DG rules
            #- Slot = Complex rules (by row and tier) = Complex DG Rules

            d_containers_exclusions = {}  
            f_dg_loadlist = df_DG_loadlist.copy()
            f_dg_loadlist.fillna("",inplace=True)
            f_dg_loadlist = f_dg_loadlist.reset_index()
            for idx, row in f_dg_loadlist.iterrows():
        
                # getting columns of interest
                container_id = f_dg_loadlist['Serial Number'].iloc[idx]
                pol = f_dg_loadlist['POL'].iloc[idx]
                pod = f_dg_loadlist['POD'].iloc[idx]
                # adapt pol to SOU2 if needed
                
                s_closed_freight_container = f_dg_loadlist['Closed Freight Container'].iloc[idx]
                un_no = f_dg_loadlist['UN'].iloc[idx]
                imdg_class = f_dg_loadlist['Class'].iloc[idx]
                sub_label = f_dg_loadlist['SubLabel1'].iloc[idx]
                dg_remark = f_dg_loadlist['DG-Remark (SW5 = Mecanical Ventilated Space if U/D par.A DOC)'].iloc[idx]
                s_flash_point = f_dg_loadlist['FlashPoints'].iloc[idx]
                s_polmar = f_dg_loadlist['Marine Pollutant'].iloc[idx]
                pgr = f_dg_loadlist['PGr'].iloc[idx]
                s_liquid = f_dg_loadlist['Liquid'].iloc[idx]
                s_solid = f_dg_loadlist['Solid'].iloc[idx]
                s_flammable = f_dg_loadlist['Flammable'].iloc[idx]
                s_non_flammable = f_dg_loadlist['Non-Flammable'].iloc[idx]
                shipping_name = f_dg_loadlist['Proper Shipping Name (Paragraph B of DOC)'].iloc[idx]
                s_stowage_segregation = f_dg_loadlist['Stowage and segregation'].iloc[idx]
                s_package_goods = f_dg_loadlist['Package Goods'].iloc[idx]
                stowage_category = f_dg_loadlist['Stowage Category'].iloc[idx]
                
                # container identification
                if (container_id, pol) not in d_containers_exclusions:
                    d_containers_exclusions[(container_id, pol)] = set()
                
                # getting the corresponding macro-category
                
                # transforming and combining some items
                
                # for remark a), dg_remark could have been used as well
                as_closed = True
                # but maybe useless
                #if s_stowage_segregation.find("SW5") >= 0 and s_closed_freight_container != 'x':
                if s_closed_freight_container != 'x':
                    as_closed = False
                
                # for remark b)
                # ...
                
                liquid = True if s_liquid == 'x' else False
                solid = True if s_solid == 'x' else False
                if solid == False and liquid == False: solid = True
                flammable = True if s_flammable == 'x' else False
                polmar = True if s_polmar == 'yes' else False
                sw_1 = True if "SW1" in str(s_stowage_segregation) else False
                sw_2 = True if "SW2" in str(s_stowage_segregation) else False
                
                flash_point = None
                if len(str(s_flash_point)) > 0:
                    s_flash_point = s_flash_point
                    flash_point = float(s_flash_point)

                if imdg_class == '3' and len(str(s_flash_point)) == 0: 
                    flash_point = float(23) 
            
                
                
                category = self.__get_DG_category(un_no, imdg_class, sub_label,
                                        as_closed, liquid, solid, flammable, flash_point,l_explosion_protect_IIB_T4, l_IMDG_Class_1_S)
                # print(container_id, pol, category)
                # if category == '?':h
                #    print("un_no, imdg, sub_label, as_closed, liquid, solid, flammable, flash_point")
                #    print(un_no, imdg_class, sub_label,
                #          as_closed, liquid, solid, flammable, flash_point,dg_remark,pgr,shipping_name, stowage_category, s_stowage_segregation)
                
                # and use the macro-category to expand the set of forbidden zones

                d_containers_exclusions[(container_id, pol)] = self.__expand_exclusion(d_containers_exclusions[(container_id, pol)], 
                                                                                dg_exclusions_by_category[category])
            
                # plus, if necessary, the stowage conditions (no C or D on hold, whatever the circumstances)
                # and SW1, SW2 and polmar
                # depending on if for slot planning or not
                
                if stowage_category in ['C', 'D']:
                    d_containers_exclusions[(container_id, pol)] = self.__expand_exclusion(d_containers_exclusions[(container_id, pol)],
                                                                                    l_hold_zones)
                if sw_2 == True:
                    d_containers_exclusions[(container_id, pol)] = self.__expand_exclusion(d_containers_exclusions[(container_id, pol)],
                                                                                    l_sw_2)
                
                if self.__DG_Rules == "slot":       
                    if sw_1 == True:
                        d_containers_exclusions[(container_id, pol)] = self.__expand_exclusion(d_containers_exclusions[(container_id, pol)],
                                                                                        l_sw_1)     
                    if polmar == True:
                        d_containers_exclusions[(container_id, pol)] = self.__expand_exclusion(d_containers_exclusions[(container_id, pol)],
                                                                                        l_polmar)
            f_loadlist_exclusions_list = []
            if self.__DG_Rules == "master":
                header_list = ["ContId", "LoadPort", "Bay", "MacroTier"]

                # ordinary rows
                for ((container_id, pol), s_exclusions) in d_containers_exclusions.items():
                    for (bay, l_rows, (macro_tier, l_tiers)) in s_exclusions:
                        row = []
                        row.extend([container_id, pol, bay[1:3], macro_tier])
                        f_loadlist_exclusions_list.append(row)
                    
            if self.__DG_Rules == "slot":
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

            DG_csv_name =  f"{self.__vessel_id} DG Loadlist Exclusions.csv"
            DG_csv_path = f"{self.__py_scripts_out_dir}/{DG_csv_name}"
            self.__DL.write_csv(f_loadlist_exclusions, DG_csv_path, self.__s3_bucket_out)

        
            # d_STOWING_seq_num_to_port_name: this map is only for stowing info
            d_STOWING_seq_num_to_port_name = self.__ML.get_d_STOWING_seq_num_to_port_name(df_onboard_loadlist)
            self.__PL.add_STOWING_maps_to_class_attributes(d_STOWING_seq_num_to_port_name)

            l_onboard_loadlist_lines = self.__PL.get_df_as_list_of_lines(df_onboard_loadlist)
            l_stowing_info_lines = self.__output_stowing_info(l_onboard_loadlist_lines, l_stacks_lines)

            ## Grouped containers ##
            self.logger.info("Extracting and saving Container Groups Containers...")
            self.__output_grouped_containers(l_stacks_lines, l_stowing_info_lines, l_onboard_loadlist_lines)

        #======================================================================================================================================
            
            groups_containers_csv_name = f"{self.__vessel_id} Container Groups Containers.csv"
            groups_containers_csv_path = f"{self.__py_scripts_out_dir}/{groups_containers_csv_name}"
            f_containers = self.__DL.read_csv(groups_containers_csv_path, DEFAULT_MISSING, ";", self.__s3_bucket_out).astype(str)

            stacks_csv_name = "Stacks Extrait Prototype MP_IN.csv"
            stacks_csv_path = f"{self.__vessels_static_in_dir}/{stacks_csv_name}"
            l_stacks_lines = self.__DL.read_csv_lines(stacks_csv_path, s3_bucket=self.__s3_bucket_in, new_line="\n")
            fn_stacks = self.__PL.get_list_of_lines_as_df(l_stacks_lines)
            
            # Preliminaries : get for each bay x macro-tier combination the set of relevant subbays

            d_stacks = self.__get_stacks_capacities(fn_stacks)
            d_bay_macro_tier_l_subbays = self.__get_bays_macro_tiers_l_subbays(d_stacks)

            # Reading the exclusions by containers and their membership in CG

            # Create exclusion zones by container groups

            #### The individual container is a couple (container, POL) :
            # - we first get their container group in the container group file
            # - we get the individual exclusion area (bay x macro-tier) 
            # in the containers exclusion zones file (with a parameter set to 'master', not 'slot')
            # - the unions of those indivicual areas create a set of exclusion zones, 
            # - for each container group, we list the relevant exclusion zones

            # get container group for each container
            d_container_2_container_group = {}

            for idx, row in f_containers.iterrows():
                container = f_containers["Container"].iloc[idx]
                load_port_name = f_containers["LoadPort"].iloc[idx]
                disch_port_name = f_containers["DischPort"].iloc[idx]
                size = f_containers["Size"].iloc[idx]
                c_type = f_containers["cType"].iloc[idx]
                c_weight = f_containers["cWeight"].iloc[idx]
                height = f_containers["Height"].iloc[idx]
                cg = (load_port_name, disch_port_name, size, c_type, c_weight, height)
                d_container_2_container_group[(container, load_port_name)] = cg

            # get exclusion zones (set of areas) for each container
            d_container_2_exclusion_zone = {}

            for idx, row in f_loadlist_exclusions.iterrows():
                container = f_loadlist_exclusions["ContId"].iloc[idx]
                load_port_name = f_loadlist_exclusions["LoadPort"].iloc[idx]
                bay = f_loadlist_exclusions["Bay"].iloc[idx]
                macro_tier = f_loadlist_exclusions["MacroTier"].iloc[idx]
                # beware, in that file, some GBSOU refer to GBSOU2 !! (handled)
            #     if load_port_name == 'GBSOU' and (container, load_port_name) not in d_container_2_container_group:
            #         load_port_name = 'GBSOU2'
                # normal process
                if (container, load_port_name) not in d_container_2_exclusion_zone:
                    d_container_2_exclusion_zone[(container, load_port_name)] = set()
                d_container_2_exclusion_zone[(container, load_port_name)].add((bay, macro_tier))

            # get set of zones as such, all container groups considered together
            s_zones = set()
            for (container, load_port_name), zone in d_container_2_exclusion_zone.items():
                s_zones.add(frozenset(zone))
            l_zones = list(s_zones)

            # get for each container the exclusion zone index in that list
            d_container_2_ix_exclusion_zone = {}
            for (container, load_port_name), container_zone in d_container_2_exclusion_zone.items():
                ix_zone = -1
                for ix, zone in enumerate(l_zones):
                    if zone == container_zone:
                        ix_zone = ix
                        break
                d_container_2_ix_exclusion_zone[(container, load_port_name)] = ix_zone
                
                
            # now, list exclusion zones for each container group, and count corresponding containers 
            d_cg_2_ix_exclusion_zones = {}

            for (container, load_port_name), ix_zone in d_container_2_ix_exclusion_zone.items():
            #     # contrôle de cohérence
            #     if (container, load_port_name) not in d_container_2_container_group:
            #         print((container, load_port_name))

                cg = d_container_2_container_group[(container, load_port_name)]
                if cg not in d_cg_2_ix_exclusion_zones:
                    d_cg_2_ix_exclusion_zones[cg] = {}
                if ix_zone not in d_cg_2_ix_exclusion_zones[cg]:
                    d_cg_2_ix_exclusion_zones[cg][ix_zone] = 0
                d_cg_2_ix_exclusion_zones[cg][ix_zone] += 1     

            # creation of the list of exclusion zones (including combinations) for each container group
            d_cg_2_combi_zones = {}
            for cg, d_ix_zones in d_cg_2_ix_exclusion_zones.items():
                d_combi_zones = self.__list_areas_for_zone_intersections(d_ix_zones, l_zones)
                d_cg_2_combi_zones[cg] = d_combi_zones

            # at last, split bay x macro_tier area into subbays, while keeping the nb of containers data
            d_cg_combi_subbays = {}

            for cg, d_combi_zones in d_cg_2_combi_zones.items():
                d_combi_subbays = {}
                for s_combi_area, nb_containers in d_combi_zones.items():
                    s_combi_subbays = self.__get_zone_list_subbays(s_combi_area, d_bay_macro_tier_l_subbays)
                    d_combi_subbays[frozenset(s_combi_subbays)] = nb_containers
                d_cg_combi_subbays[cg] = d_combi_subbays               

            l_cg_exclusion_zones = []
            s_header_zones_list = ["LoadPort", "DischPort", "Size", "cType", "cWeight", "Height", "idZone", "Subbay"]

            l_cg_exclusion_zones_nb_dg = []
            s_header_nb_dg_list = ["LoadPort", "DischPort", "Size", "cType", "cWeight", "Height", "idZone", "NbDG"]

            for (load_port_name, disch_port_name, size, c_type, c_weight, height), d_combi_subbays in d_cg_combi_subbays.items():
                
                for ix, (s_combi_subbays, nb_containers) in enumerate(d_combi_subbays.items()):
                    l_combi_subbays = list(s_combi_subbays)
                    l_combi_subbays.sort()
                    # writing zones
                    for subbay in l_combi_subbays:
                        row = []
                        row.extend([load_port_name, disch_port_name, size, c_type, c_weight, height, ix, subbay])
                        l_cg_exclusion_zones.append(row)

                    # writing nb of containers
                    row_nb = []
                    row_nb.extend([load_port_name, disch_port_name, size, c_type, c_weight, height, ix, nb_containers])
                    l_cg_exclusion_zones_nb_dg.append(row_nb)

            f_cg_exclusion_zones = pd.DataFrame(l_cg_exclusion_zones, columns=s_header_zones_list)
            f_cg_exclusion_zones_nb_dg = pd.DataFrame(l_cg_exclusion_zones_nb_dg, columns=s_header_nb_dg_list)

            f_cg_exclusion_zones_name =  f"{self.__vessel_id} DG Container Groups Exclusion Zones.csv"
            f_cg_exclusion_zones_csv_path = f"{self.__py_scripts_out_dir}/{f_cg_exclusion_zones_name}"
            self.__DL.write_csv(f_cg_exclusion_zones, f_cg_exclusion_zones_csv_path, self.__s3_bucket_out)

            f_cg_exclusion_zones_nb_dg_name =  f"{self.__vessel_id} DG Container Groups Exclusion Zones Nb DG.csv"
            f_cg_exclusion_zones_nb_dg_csv_path = f"{self.__py_scripts_out_dir}/{f_cg_exclusion_zones_nb_dg_name}"
            self.__DL.write_csv(f_cg_exclusion_zones_nb_dg, f_cg_exclusion_zones_nb_dg_csv_path, self.__s3_bucket_out)

        #======================================================================================================================================       
        ## Stowing and overstowing ##
    def __get_df_from_baplie_and_return_types(self, baplie_path: str, call_id: str, file_type: str, d_csv_cols_to_segments_map: dict, d_main_to_sub_segments_map: dict, s3_bucket:str):
       
        l_baplie_segments, new_data_flag, baplie_type_from_file_name, baplie_type_from_content = \
            self.__DL.read_baplie_body_as_list(baplie_path, call_id, file_type, s3_bucket)
        # if file_type in ['OnBoard', 'LoadList']:
        #     self.__AL.check_missing_new_container_header(l_baplie_segments, call_id, file_type)
            
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
            # df_copy[col_name] = df_copy.apply(lambda row: row[col_name+row["lowest_DG_class_suffix"]], axis=1)
      
        # for col in l_DGS_cols:
        #     col_name = "DGS_" + col
        #     new_col_name = col_name + df_copy["lowest_DG_class_suffix"].iloc[0]
        #     if new_col_name not in df_copy.columns:
        #         df_copy[new_col_name] = pd.Series(np.nan, index=df_copy.index)
        #     df_copy[col_name] = df_copy[new_col_name]
            
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
        
        filled_tanks_csv_name  = f"{self.__vessel_id} Preprocessed Filled Tanks Ports.csv"
        filled_tanks_csv_path  = f"{self.__py_scripts_out_dir}/{filled_tanks_csv_name}"
        self.__DL.write_csv(df_filled_subtanks, filled_tanks_csv_path, s3_bucket=self.__s3_bucket_out)

    def __run_first_execution(self) -> None:
        
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
        rotation_intermediate = rotation_intermediate.iloc[:len(self.__d_seq_num_to_port_name)-1]
        self.logger.info("Reading consumption csv file from referential vessels folder...")
        consumption_df = self.__DL.read_csv(self.__consumption, na_values=DEFAULT_MISSING, sep=',', s3_bucket=self.__s3_bucket_in)
        self.logger.info("Extracting StdSpeed, GmDeck, MaxDraft, lashing calculation configuration and service line for call_01 from rotation intermediate...")
        lashing_parameters_dict = extract_as_dict(rotation_intermediate, indexes=None, columns=['CallFolderName', 'StdSpeed', 'Gmdeck', 'MaxDraft', 'worldwide', 'service', 'WindowStartTime', 'WindowEndTime'])
        self.__AL.validate_data(lashing_parameters_dict)
        self.__AL.check_if_errors(self.__error_log_path, self.__s3_bucket_out)
        self.logger.info("*"*80)
        
        # # Intialize worst cast files generation 
        self.__service_code = lashing_parameters_dict[0]['service']
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
                
            self.__AL.check_if_errors(self.__error_log_path, self.__s3_bucket_out)
        else: 
            self.logger.info("LoadList.edi exists for all port calls in sumulation folder...")
        self.logger.info("*" * 80)    
        # Intialize Vessel
        self.logger.info(f"Creating Vessel instance for vessel: {self.__vessel_id}")
        # vessel profile file in referentials
        self.logger.info(f"Reading vessel_profile.json & DG_rules.json configuration file from referential vessel folder for vessel: {self.__vessel_id}...")
        vessel_profile = self.__DL.read_json(f"{self.__vessels_static_in_dir}/vessel_profile.json", s3_bucket=self.__s3_bucket_in)
        
        # DG_rules config JSON for Vessel
        DG_rules = self.__DL.read_json(f"{self.__vessels_static_in_dir}/DG_rules.json", s3_bucket=self.__s3_bucket_in)
        
        std_speed = float(lashing_parameters_dict[0]['StdSpeed'])
        draft = float(lashing_parameters_dict[0]['MaxDraft'])
        gm_deck = float(lashing_parameters_dict[0]['Gmdeck'])
        vessel = Vessel(self.logger, std_speed, gm_deck, draft, vessel_profile, DG_rules)
        
        #Iniatlize Lashing 
        lashing_conditions = lashing_parameters_dict[0]['worldwide']
        lashing = Lashing(self.logger, vessel, lashing_conditions)
        self.logger.info("*"*80)
        
        # empty lists for dataframes that are going to be saved as csvs and their folder names (used in the names of the csvs)
        l_dfs_containers, l_containers_folder_names, l_dfs_rotation_containers = [], [], []
        l_dfs_tanks, l_tanks_baplies_paths, l_tanks_folder_names = [], [], []
        for baplie_path in self.__l_baplies_filepaths:
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
                    # path_name = "output_" + call_id +".csv"
                    # df_attributes.to_csv(path_name)
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

    
                    #NOTE consider these 2 lines
                    # l_non_empty_cols = [ col for col in df_all_containers.columns if sum(df_all_containers[col].astype(bool)) ]
                    # df_all_containers = df_all_containers[l_non_empty_cols]

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
            
        self.__AL.check_if_errors(self.__error_log_path, self.__s3_bucket_out)
        
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
        # start here
        # d_DG_enrichment_map = self.__DL.read_json(f"{self.__jsons_static_in_dir}/DG_loadlist_enrichment_map.json", self.__s3_bucket_in)
        # imdg_codes_list_csv_path = f"{self.__static_in_dir}/hz_imdg_exis_subs.csv" if self.__static_in_dir else "hz_imdg_exis_subs.csv"
        # imdg_codes_df = self.__DL.read_csv(imdg_codes_list_csv_path, DEFAULT_MISSING, ",", self.__s3_bucket_in).astype(str) 
        # naming_schema = d_DG_enrichment_map["DG_LOADLIST_SCHEMA"] 
        # dg_instance = DG(self.logger, vessel, df_all_containers, d_DG_enrichment_map, DG_rules, imdg_codes_df)
        # df = dg_instance.output_DG_loadlist()
        # df.to_csv("output.csv")
        
        self.__output_CPLEX_input_container_csvs(df_all_containers, df_filled_slots, df_DG_classes_expanded, d_iso_codes_map)
        if len(l_dfs_tanks):                    
            df_tanks_final = pd.concat(l_dfs_tanks, axis=0, ignore_index=True)
            df_tanks_final.fillna("", inplace=True)
            all_tanks_csv_name = "csv_combined_tanks.csv"
            all_tanks_csv_path = f"{self.__py_scripts_out_dir}/{all_tanks_csv_name}"
            self.__DL.write_csv(df_tanks_final, all_tanks_csv_path, s3_bucket=self.__s3_bucket_out)

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

                        #TODO investigate issue
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
        path_to_save = f"{self.__cplex_out_dir}/BayPlan.edi"
        self.__DL.output_bayplan_edi(path_to_save, baplie_delimiter, l_all_semgents, self.__s3_bucket_out)
        

    def __run_reexecution(self) -> None:
        
        files_in_output = self.__DL.list_files_in_path(self.__cplex_out_dir, self.__s3_bucket_out)
        self.__AL.check_if_no_output_postprocess(files_in_output, self.__error_log_path, self.__s3_bucket_out)
        
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
            