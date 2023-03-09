#!/usr/bin/env python
# coding: utf-8

# # Preprocessing for Constraints

#########################################################################
# ### Run Parameters
# Context (loading a BV condition, or ordinary preparation)

# Not for a BV condition or not
# BV_condition = '04' # a string,here for condition 4
BV_condition = None

#specific edi file without BV condition
#specific_edi_filename = "bay21-22-23-rows02_04_06_08.edi"
# A RECOMMENTARISER ENSUITE
specific_edi_filename = None
edi_container_nb_header_lines = 9
edi_structure = 'EQD'

# specific name for result
#specific_result_filename = None
specific_result_filename = "Loaded Lists IBM 2021-08-23"

# with WB filled for compensing trim
#WB_compensating_trim = True
# condition trim, used as a parameter to buoyancies calculations
#condition_trim = 0.0

# with BV condition loading ports and discharge ports, for BV conditions cargo file obtained from macS3
BV_condition_load_port = None
BV_condition_disch_port = None
# as well as for tank files
#BV_condition_tank_port = 'CNTXG'


##########################################################################
# ###### Files and parameters

# local directory
REP_DATA = "c:/Projets/CCS/Vessel Stowage/Python Prétraitement"

if BV_condition is None:
    REP_LOADLIST_DATA = REP_DATA + "/9454450 Rotation"
else:
    REP_LOADLIST_DATA = REP_DATA + "/9454450 BV Conditions"

###################################################################
###################################################################

# libraries
import os

import vessel_stow_preproc as vsp


# ## Loading static data files

os.chdir(REP_DATA)


# ### LoadLists : reading .csv files

os.chdir(REP_LOADLIST_DATA)

l_containers = vsp.get_l_containers(BV_condition, BV_condition_load_port, BV_condition_disch_port,
                                    specific_edi_filename, edi_container_nb_header_lines,
                                    edi_structure)

# #### Back to common track, write file

os.chdir(REP_DATA)

PREPROCESSED_LOADLIST_BASENAME = "9454450 Preprocessed Loadlist"
result_filename = PREPROCESSED_LOADLIST_BASENAME
if specific_result_filename is not None:
    result_filename = specific_result_filename
if BV_condition is not None:
    fn_prepro_loadlist = "%s C%s.csv"% (result_filename, BV_condition)
else:
    fn_prepro_loadlist = "%s.csv"% (result_filename)

f_prepro_loadlist = open(fn_prepro_loadlist, 'w')

# output formatted loadlist
s_header = "ContId;LoadPort;DischPort;Type;Setting;Size;Height;Weight;Slot\n"
f_prepro_loadlist.write(s_header)

for (cont_id, load_port, disch_port, c_type, setting, size, height, weight, slot) in l_containers:

        s_line = "%s;%s;%s;%s;%s;%s;%s;%.3f;%s\n" % (cont_id, load_port, disch_port, c_type, setting, size, height, weight, slot)
        f_prepro_loadlist.write(s_line)
        
f_prepro_loadlist.close()


