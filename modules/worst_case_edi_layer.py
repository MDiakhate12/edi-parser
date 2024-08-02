import os
import re
import logging

from modules.common_helpers import split_list 
from modules.data_layer import DataLayer as DL


class worst_case_baplies():
    def __init__(self, logger: logging.Logger, AL: object, vessel_imo : str, simulation_id: str, referential_input_path: str, simulation_input_path: str, simulation_output_path: str, s3_bucket_ref: str="", s3_bucket_out:str="") -> None:
        self.logger = logger
        self.__vessel_imo = vessel_imo
        self.__simulation_id = simulation_id
        self.__simulation_output_path = simulation_output_path
        self.__referential_input = referential_input_path
        self.__service_line = self.__referential_input.split("/")[-1]
        self.__simulation_input = simulation_input_path
        self.__s3_bucket_ref = s3_bucket_ref
        self.__s3_bucket_out = s3_bucket_out
        self._DL = DL(self.logger, self.__s3_bucket_out, self.__s3_bucket_ref)
        self._AL = AL


    def _perspective_approach_POD(self, i: int, port:str, port_codes_sim:list)-> list:
        l_PODs_Perspective = []
        for j in range(len(port_codes_sim[i+1:])):
            if j == len(port_codes_sim[i+1:]) - 1:  # if we're at the end of the list
                l_PODs_Perspective.append(port_codes_sim[i+1:][j])
                break
            current_value = port_codes_sim[i+1:][j]
            if current_value != port:
                l_PODs_Perspective.append(current_value)
            else: 
                break
        return l_PODs_Perspective

    def _retrospective_approach_POD(self, i:int, port:str, port_codes_sim:list)-> list:
        l_PODs_Retrospective = []
        for j in range(i, 0, -1):
            #if we are at the start of the list
            if i == 0:  # if we're at the end of the list
                break
            current_value = port_codes_sim[j-1]
            if current_value != port:
                l_PODs_Retrospective.append(current_value)
            else:
                break
        return l_PODs_Retrospective

    def _check_matching_perspective_approach(self, port:str, port_codes_ref:list, sublists:list, l_PODs_Perspective: list)->list: 
        matching_port_index_per = []     
        for m, lst in enumerate(sublists):
            if all(elem in lst for elem in l_PODs_Perspective):
                index = [i for i, x in enumerate(port_codes_ref) if x == port][m]
                matching_port_index_per.append(index)
        return matching_port_index_per

    def _check_matching_retrospective_approach(self, port:str, indices:list, port_codes_ref:list, sublists:list, l_PODs_Retrospective:list)->list:
        matching_port_index_retro = []     
        for m, lst in enumerate(sublists):
            if all(elem in lst for elem in l_PODs_Retrospective):
                index = [i for i, x in enumerate(port_codes_ref) if x == port][m]
                indices_index = indices.index(index)
                if indices_index == len(sublists) - 1:
                    selected_value = indices[0]  # If matching index is the last index, select the first value
                    matching_port_index_retro.append(selected_value)
                else:
                    selected_value = indices[indices_index + 1]  # If matching index is not the last index, select
                    matching_port_index_retro.append(selected_value)
        return matching_port_index_retro

    def _get_matching_port_index(self, port:str, index: int, matching_port_index_per:list, matching_port_index_retro:list)-> int: 
        if len(matching_port_index_per) == 1 and not len(matching_port_index_retro) and port == port:
            return matching_port_index_per[0]
        
        elif not len(matching_port_index_per) and len(matching_port_index_retro) == 1:
            return matching_port_index_retro[0]
        
        else:
            matching_value = set(matching_port_index_per).intersection(set(matching_port_index_retro))
            if matching_value:
                matching_value = int(matching_value.pop())
                return matching_value   
            else: 
                self.logger.info(f"No matching port index found for port: {port} at call: {index} ...")
                self.logger.info("*" * 80)
                
        
    # find missing port calls
    def _find_missing_port_calls(self, ref_port_index_list:list, referential_folder_name:list):
        missing_port_calls = []
        ref_port_index_list = [x for x in ref_port_index_list if x is not None]
        if max(ref_port_index_list) == ref_port_index_list[-1]:
            # find the range of numbers to check
            start, end = ref_port_index_list[0], ref_port_index_list[-1]

        # iterate through the range and check for missing numbers
            for i in range(start, end+1):
                if i not in ref_port_index_list:
                    missing_port_calls.append(i)
            missing_port_folder_list = [referential_folder_name[i] for i in missing_port_calls]
            self.logger.info("*" * 80)
            self.logger.info("missing port calls: %s", missing_port_calls)
            self.logger.info("missing port folder list: %s", missing_port_folder_list)
            return missing_port_calls, missing_port_folder_list
        else: 
            max_elem = max(ref_port_index_list)
            int_range = range(ref_port_index_list[0], max_elem+1)
            missing_ints_end = [i for i in int_range if i not in ref_port_index_list]

            int_range = range(0, ref_port_index_list[-1]+1)
            missing_ints_start = [i for i in int_range if i not in ref_port_index_list]
            missing_port_calls = missing_ints_end + missing_ints_start
            missing_port_folder_list = [referential_folder_name[i] for i in missing_port_calls]
            self.logger.info("*" * 80)
            self.logger.info("missing port calls: %s", missing_port_calls)
            self.logger.info("missing port folder list: %s", missing_port_folder_list)
            return missing_port_calls, missing_port_folder_list

    def _get_port_calls_in_rotation(self, path:str, s3_bucket:str):
        """
        Get a list of port codes in the rotation from the specified  path and S3 bucket.

        Parameters:
            path (str): Path to the simulation input folder.
            s3_bucket (str): S3 bucket name for the output.

        Returns:
            list: A list of port codes in the rotation.
        """
        # Import folders from  path
        folder_names = self._DL.list_folders_in_path(path, s3_bucket)
        # sort folder names for consistency 
        sorted_folder_names = sorted(folder_names, key=lambda x: int(x.split('_')[1])) 
        # Extract port codes from folder names that start with "call_"
        port_codes= [folder_name[-5:] for folder_name in sorted_folder_names if folder_name.startswith("call_")]

        return sorted_folder_names, port_codes
    
    def _get_port_lists(self):
        # import folders from simulation in 
        simulation_folder_names, port_codes_sim = self._get_port_calls_in_rotation(self.__simulation_input, self.__s3_bucket_out)
        self.logger.info("Port Calls in Rotation: %s", port_codes_sim)
        # referential folder names 
        referential_folder_name, port_codes_ref = self._get_port_calls_in_rotation(self.__referential_input, self.__s3_bucket_ref)
        self.logger.info("Port Calls in Referential: %s", port_codes_ref)
        
        return simulation_folder_names, port_codes_sim, referential_folder_name, port_codes_ref
    
    def _process_port_codes(self, port_codes_sim:list, port_codes_ref:list, referential_folder_name:list):
        """
        Process port codes and return the matching reference port indices list.

        Parameters:
            port_codes_sim (list): List of port codes from the simulation.
            port_codes_ref (list): List of port codes from the reference data.
            referential_folder_name (list): List of folder names from the reference data.

        Returns:
            list: A list of matching reference port indices.
        """
        # do first check to see if all ports exist in referentials
        self._AL.check_sim_with_referentials(port_codes_sim, port_codes_ref, self.__service_line)
        
        sim_port_index_list, ref_port_index_list = [], []
        
        for i, port in enumerate(port_codes_sim):

                self.logger.info(f"Port: {port}")
                
                if port not in port_codes_ref: 
                    self.logger.info(f"Port {port} is not found in referential files for service line: {self.__service_line}...")
                    self.logger.info("*" * 80)
                    
                else:
                    l_PODs_Perspective = self._perspective_approach_POD(i, port, port_codes_sim)
                    self.logger.info("POD Portfolio Ahead: %s", l_PODs_Perspective)

                    l_PODs_Retrospective = self._retrospective_approach_POD(i, port, port_codes_sim)
                    self.logger.info("POD Portfolio Behind: %s", l_PODs_Retrospective)
                    
                    
                    indices = []
                    sublists = []
                    match_tag = False
                    for j, port_ref in enumerate(port_codes_ref):
                        if port == port_ref and port_codes_ref.count(port_ref) == 1:
                            ref_port_index_list.append(j)
                            sim_port_index_list.append(i)
                            self.logger.info("will_be_matching to: %s from referential.", referential_folder_name[j])
                            self.logger.info("*" * 80)

                        elif port == port_ref and port_codes_ref.count(port_ref) != 1:
                            # Find the indices of all occurrences of port
                            indices = [k for k, x in enumerate(port_codes_ref) if x == port]
                            match_tag = True
                            # Iterate over the indices and create sublists
                            sublists = []
                            for k in range(len(indices)):
                                start = indices[k]
                                end = indices[(k + 1) % len(indices)]
                                if len(port_codes_ref[start:end + 1]) != 0:
                                    sublists.append(port_codes_ref[start + 1:end])
                            # Find the index of the last occurrence of port
                            sublists.append(port_codes_ref[indices[-1] + 1:] + port_codes_ref[0:indices[0]])
                        
        

                    if len(sublists):
                        self.logger.info("port calls between occurrences: %s", sublists)
                        self.logger.info("at indices = %s", indices)

                        matching_port_index_per = self._check_matching_perspective_approach(port, port_codes_ref, sublists, l_PODs_Perspective)
                        matching_port_index_retro = self._check_matching_retrospective_approach(port, indices, port_codes_ref, sublists, l_PODs_Retrospective)
                        try:
                            matching_value = self._get_matching_port_index(port, i, matching_port_index_per, matching_port_index_retro)
                            if matching_value:
                                ref_port_index_list.append(matching_value)
                                sim_port_index_list.append(i)
                                self.logger.info("will_be_matching to: %s", referential_folder_name[matching_value])
                                self.logger.info("*" * 80)
                            
                        except: 
                            self.logger.info("No Matching port found...")
                            self.logger.info("*" * 80)
                            self._AL.no_matching_port_check(port,i)

                    elif not len(sublists) and match_tag == True: 
                        self.logger.info("No Matching port found...")
                        self.logger.info("*" * 80)
                        self._AL.no_matching_port_check(port,i)
        
        self._AL.check_if_errors()
        # check
        # mismatching_ports = self._AL.check_out_of_order_ports(port_codes_ref, ref_port_index_list, referential_folder_name)
        # for mismatch in mismatching_ports: 
        #     self.logger.warning(mismatch)
        # self._AL.check_if_errors()
        
        self.logger.info("matched ref_port_index_list: %s", ref_port_index_list)
        self.logger.info("matched sim_port_index_list: %s", sim_port_index_list)
        return ref_port_index_list, sim_port_index_list
    
        
    def check_numbers_in_order(self, lst, max_value):
        n = len(lst)
        result = []

        for i in range(1, n - 1):
            current_element = lst[i]
            next_element = lst[(i + 1) % n]
            prev_element = lst[i - 1]

            # Check if the next and previous elements are not in the right order
            if (current_element >= next_element) or (current_element <= prev_element):
                result.append(i)

        return result


    def _copy_loadlist_files(self, ref_port_index_list:list, referential_folder_name:list, sim_port_index_list:list, folder_names:list) -> None:
        """
        Copy LoadList files from referential to the corresponding simulation folders.

        Parameters:
            ref_port_index_list (list): List of matching reference port indices.
            referential_input_path (str): Path to the referential input folder.
            s3_bucket_in (str): S3 bucket name for the referential input.
            folder_names (list): List of folder names from the simulation input.
            simulation_input_path (str): Path to the simulation input folder.
            s3_bucket_out (str): S3 bucket name for the simulation input.
        """
        for i, call_number in enumerate(ref_port_index_list):
            if call_number is not None and sim_port_index_list[i] not in [0, 1]:
                source_key = os.path.join(self.__referential_input, referential_folder_name[call_number])
                destination_folder_key = [int(folder_name.split('_')[1]) for folder_name in folder_names]
                folder_name = folder_names[destination_folder_key.index(sim_port_index_list[i])]
                destination_key = os.path.join(self.__simulation_input, folder_name)
                files_in_ref_path = self._DL.list_files_in_path(source_key, self.__s3_bucket_ref)
                files_in_simulation_call = self._DL.list_files_in_path(destination_key, self.__s3_bucket_out)
                if "LoadList.edi" not in files_in_simulation_call:
                    self.logger.info(f"LoadList Baplie file will be copied from folder: {source_key} to {destination_key}...")
                    if 'LoadList.edi' not in files_in_ref_path:
                        self.logger.error("no Loadlist file found in referential path: %s ..." % filename)
                        raise ValueError("no Loadlist found in referential path...")
                    
                    for filename in files_in_ref_path:
                        if filename in ["LoadList.edi"]:
                            file_dir = os.path.join(source_key, filename)
                            
                            self._DL.copy_file(file_dir, destination_key, self.__s3_bucket_ref, self.__s3_bucket_out)

    def replace_location(self, data: list, replacements: list) -> None:
        """
        Replace occurrences of 'LOC+9+' followed by any characters in the given data list with the provided replacements.

        Args:
            data (list): The nested list containing the data to be processed.
            replacements (list): The list of replacements to be used.

        Returns:
            None: The function modifies the input data in-place and does not return anything.
        """
        for i, sublist in enumerate(data):
            if not sublist:  # Check if the sublist is empty
                continue

            for j, inner_list in enumerate(sublist):
                for k, sublist_element in enumerate(inner_list):
                    if i < len(replacements):
                        replacement = replacements[i]
                    else:
                        break

                    pattern = r'LOC\+9\+[A-Z]+'
                    if isinstance(sublist_element, str):  # Check if the element is a string
                        sublist[j][k] = re.sub(pattern, f'LOC+9+{replacement}', sublist_element)
    
    def _process_missing_port_folders(self, missing_port_folder_list:list, ref_port_index_list:list, folder_names:list, port_codes_sim:list, missing_port_calls:list) -> None:
        """
        Process missing port folders and divide the LoadList.edi file into corresponding simulation folders.

        Parameters:
            missing_port_folder_list (list): List of missing port folders.
            referential_input_path (str): Path to the referential input folder.
            s3_bucket_in (str): S3 bucket name for the referential input.
            folder_names (list): List of folder names from the simulation input.
            simulation_input_path (str): Path to the simulation input folder.
            s3_bucket_out (str): S3 bucket name for the simulation input.
            port_codes_sim (list): List of port codes from the simulation.
            missing_port_calls (list): List of missing port calls.
        """
        for k, missing_port in enumerate(missing_port_folder_list): 
            self.logger.info("missing port: %s",missing_port)
            folder_path = os.path.join(self.__referential_input, missing_port_folder_list[k])
            self.logger.info("folder location: %s", folder_path)
            files_in_missing_port_folder = self._DL.list_files_in_path(folder_path, self.__s3_bucket_ref)
            for filename in files_in_missing_port_folder:
                if filename in ["LoadList.edi"]:
                            file_path = os.path.join(folder_path, filename)
                            file = self._DL.read_file(file_path, self.__s3_bucket_ref)
                            l_segments = re.split("(?='LOC\+147)", file)
                            del l_segments[0]
                            if len(l_segments):
                                list_end = l_segments[-1].split("'UNT")
                                l_segments[-1] = list_end[0]
                                l_segments = [seg[1:] for seg in l_segments]
                                l_segments = [seg.split("'") for seg in l_segments]
                                
                                POD_file, EQD_POD_list, EQD_POD_dict = [], [], {}
                                for i, segment in enumerate(l_segments):
                                    # detecting empty or full
                                    if segment[1][-1] == '5':
                                        Empty_flag = False
                                    else: 
                                        Empty_flag = True
                                    # initial tag for Reefer    
                                    Reefer_flag = False
                                    #initial tag for DGS container
                                    DG_flag = False
                                    #OOG Flag  
                                    OOG_flag = False
                                    
                                    if segment[1] == "LOC+11":
                                        loc_11_id = segment[2]
                                        if loc_11_id.startswith("USLA"):
                                            loc_11_id = "USLAX"
                                    for sub_segment in segment:
                                        segment_header = sub_segment[:6]
                                        for value in ['5', '6', '7', '8', '13']:
                                            if "DIM+"+value in sub_segment:            
                                                OOG_flag = True
                                        
                                        if "TMP" in sub_segment:
                                            Reefer_flag = True
                                        
                                        if "DGS" in sub_segment:
                                            DG_flag = True

                                        if segment_header == "LOC+11":
                                            loc_11_id = sub_segment.split("+")[2]
                                            
                                            if loc_11_id.startswith("USLA"):
                                                loc_11_id = "USLAX"

                                            if loc_11_id not in POD_file:
                                                POD_file.append(loc_11_id)
                                    if OOG_flag and not Empty_flag:
                                        list_name = "EQD_" + loc_11_id + "_OOG"   
                                    elif OOG_flag and Empty_flag:  
                                        list_name = "EQD_" + loc_11_id + "_EMPTY_OOG"       
                                    elif Reefer_flag and DG_flag and not Empty_flag:
                                        list_name = "EQD_" + loc_11_id + "_DG_REEFER"
                                    elif Reefer_flag and not DG_flag and Empty_flag:
                                        list_name = "EQD_" + loc_11_id + "_EMPTY_REEFER"
                                    elif Reefer_flag and not DG_flag and not Empty_flag:
                                        list_name = "EQD_" + loc_11_id + "_REEFER"
                                    elif DG_flag and  not Empty_flag:
                                        list_name = "EQD_" + loc_11_id + "_DG"
                                    else:
                                        if Empty_flag:
                                            list_name = "EQD_" + loc_11_id + "_EMPTY"
                                        elif not Empty_flag: 
                                            list_name = "EQD_" + loc_11_id 

                                    if list_name not in EQD_POD_list:
                                        EQD_POD_list.append(list_name)
                                        EQD_POD_dict[list_name] = []
                                    EQD_POD_dict[list_name].append(segment)
                            else: 
                                EQD_POD_list = None
            if EQD_POD_list:
                self.logger.info("*" * 80)                     
                self.logger.info("EQD Categories: %s", EQD_POD_list)
                self.logger.info("POD_Portfolio: %s", POD_file)
                self.logger.info("*" * 80)  
                for EQD_POD in EQD_POD_list:
                    self.logger.info(f"Distributing Containers for: {EQD_POD}...")
                    POD = EQD_POD[4:9]
                    self.logger.info("POD: %s", POD)

                    max_element = max(ref_port_index_list)

                    for index, value in enumerate(ref_port_index_list):
                        if value > missing_port_calls[k]:
                            index = ref_port_index_list.index(value)
                            break
                        elif missing_port_calls[k] == max_element and missing_port_calls[k] != ref_port_index_list[-1]:
                            index = ref_port_index_list.index(0)
                            break
                        elif missing_port_calls[k] == max_element and missing_port_calls[k] == ref_port_index_list[-1]:
                            index = ref_port_index_list.index(missing_port_calls[k])
                            break

                    POD_index_start = max(index, 2)
                    try:
                        POD_index_end = port_codes_sim.index(POD, POD_index_start + 1) 
                    except ValueError:
                        POD_index_end = len(port_codes_sim)  
                    if POD_index_start == POD_index_end: 
                        POD_index_end = len(port_codes_sim) 
                    self.logger.info("POD_index_start: %s", POD_index_start)
                    self.logger.info("POD_index_end: %s", POD_index_end)
                    
                    #remove folders without loadlists 
                    folders_to_update = folder_names[POD_index_start:POD_index_end]
                    port_codes_sim_to_replace = port_codes_sim[POD_index_start:POD_index_end]

                    for j, folder_name in enumerate(folders_to_update):
                        folders = os.path.join(self.__simulation_input, folder_name)
                        files_in_path = self._DL.list_files_in_path(folders,self.__s3_bucket_out)
                        if "LoadList.edi" not in files_in_path:
                            del folders_to_update[j]
                            del port_codes_sim_to_replace[j]
                            
                    self.logger.info("POL: %s", port_codes_sim_to_replace)
                    self.logger.info("number of Containers to be divided is = %s", len(EQD_POD_dict[EQD_POD]))
                    self.logger.info("*" * 80)  
                    result = split_list(EQD_POD_dict[EQD_POD],len(folders_to_update))
                    self.replace_location(result, port_codes_sim_to_replace)
                    for j, folder_name in enumerate(folders_to_update):
                        folders = os.path.join(self.__simulation_input, folder_name,"LoadList.edi")
                        flattened_list = []
                        for inner_list in result[j]:
                            for element in inner_list:
                                flattened_list.append(element)
                        file = self._DL.read_file(folders, self.__s3_bucket_out)
                        l_segments = file.split("'")
                        for index, segment in enumerate(l_segments):
                            segment_header = segment[:7]
                            if segment_header == "LOC+147":
                                EQD_index = index
                                break
                        result_list = l_segments[:index] + flattened_list + l_segments[index:]
                        file = "'".join(result_list)  
                        self._DL.write_file(file, folders, self.__s3_bucket_out) 

    def generate_worst_case_baplie_loadlist(self):
        """
        Generate the worst-case Baplie LoadList by performing the following steps:
        1. Get the port lists from simulation and referential data.
        2. Process port codes to find matching reference port indices.
        3. Copy LoadList files from referential to the corresponding simulation folders.
        4. Find missing port calls.
        5. Process missing port folders and divide the LoadList.edi file into corresponding simulation folders.
        """
        folder_names, port_codes_sim, referential_folder_name, port_codes_ref = self._get_port_lists()
        
        ref_port_index_list, sim_port_index_list = self._process_port_codes(port_codes_sim, port_codes_ref, referential_folder_name)
        
        self._copy_loadlist_files(ref_port_index_list, referential_folder_name, sim_port_index_list, folder_names)

        # find missing port calls
        missing_port_calls, missing_port_folder_list = self._find_missing_port_calls(ref_port_index_list, referential_folder_name)

        self._process_missing_port_folders(missing_port_folder_list,ref_port_index_list, folder_names, port_codes_sim, missing_port_calls)
                    
