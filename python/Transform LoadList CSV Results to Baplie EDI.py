
# Transform LoadLists CSV Results into a BAPLI EDI File

# libraries
import os
import csv
import re
import datetime as dt

# directory and file names
##################
# TO BE MODIFIED #
#REP_DATA = "c:/Projets/CCS/Vessel Stowage/Python Posttraitement"
REP_DATA="C:/Projets/CCS/Vessel Stowage/Python C++ Modèles/C++/MasterPlanningV11/msvc9/data/ver102810"

#Test8_V11_out_stab_hz6_warm_start_with_objectives
#Test8_V11_out_stab_precise_hz6_warm_start_with_objectives
#Test8_V12_out_stab_hz6_warm_start_with_objectives
#Test8_V12_out_stab_hz7_warm_start_with_objectives
#Test8_V12_out_stab_precise_hz6_warm_start_with_objectives
#Test8_V12_out_stab_precise_hz7_warm_start_with_objectives
#RESULT_FILENAME_BASE = "results_0825_CNTXG_1"
#RESULT_FILENAME_BASE = "OnBoard_0823_CNTXG"
#RESULT_FILENAME_BASE = "results_0901_light_HC_handling_SGSIN"
#RESULT_FILENAME_BASE = "results_0903_portHorizon_5"
#RESULT_FILENAME_BASE = "results_0716_01"
#RESULT_FILENAME_BASE = "1022_resultatSlotPlanning"
RESULT_FILENAME_BASE = "resultatSlotPlanning"
EDI_RESULT_FILENAME_HEAD = "X"
##################

os.chdir(REP_DATA)

# for some (useless ?) complements to the baplie records
record_tail = {
    'pol_pod': ':139:6', 
    'slot_position': '::5', 
    'carrier': ':172:20'
}
# if no such tail...
#record_tail = {'pol_pod': '', 'slot_position': '', 'carrier': ''}

# header data
person_name = "xxx+XXX"
date_hour = "2021-07-01-010000"
org_name = "tedivo.com"
vessel_name = "JULES VERNES"
port_1 = "FRLEH"
port_2 = "DEHAM"
date_hour_bis = "2107020200"
date_hour_ter = "2107030300"

# file names
source_result_ll_filename = RESULT_FILENAME_BASE + ".csv"
edi_result_ll_filename = EDI_RESULT_FILENAME_HEAD + date_hour + "_" + RESULT_FILENAME_BASE + "_22.edi"
edi_result_ll_filename = EDI_RESULT_FILENAME_HEAD + "_" + RESULT_FILENAME_BASE + ".edi"

# WRITING FUNCTIONS

# writing the header's several rows
def write_edi_header(f_edi_result_ll):
    
    row_1 = "UNB+UNOA:2+" + person_name + "+" + date_hour[2:4] + date_hour[5:7] + date_hour[8:10]          + ":" + date_hour[11:15] + "+" + date_hour[2:4] + date_hour[5:7]          + "++" + org_name + "+++" + "'\n"
    f_edi_result_ll.write(row_1)
    
    row_2 = "UNH+" + date_hour[11:15] + "00001+" + "BAPLIE" + ":D:95B:UN:SMDG22" + "'\n"
    f_edi_result_ll.write(row_2)
    
    row_3 = "BGM++1+9" + "'\n"
    f_edi_result_ll.write(row_3)
    
    row_4 = "DTM+137:"          + date_hour[2:4] + date_hour[5:7] + date_hour[8:10] + date_hour[11:15]          + "PDT:301" + "'\n"
    f_edi_result_ll.write(row_4)
    
    row_5 = "TDT+20++++:172:20+++:103:11:" + vessel_name + "'\n"
    f_edi_result_ll.write(row_5)
    
    row_6 = "LOC+5+" + port_1 + record_tail['pol_pod'] + "'\n"
    f_edi_result_ll.write(row_6)
    
    row_7 = "LOC+61+" + port_2 + record_tail['pol_pod'] + "'\n"
    f_edi_result_ll.write(row_7)
    
    row_8 = "DTM+178:" + date_hour_bis + ":201" + "'\n"
    f_edi_result_ll.write(row_8)
    
    row_9 = "DTM+132:" + date_hour_bis[0:6] + ":101" + "'\n"
    f_edi_result_ll.write(row_9)
    
    row_10 = "DTM+133:" + date_hour_ter[0:6] + ":101" + "'\n"
    f_edi_result_ll.write(row_10)
    
    row_11 = "DTM+136:" + date_hour_ter + ":201" + "'\n"
    f_edi_result_ll.write(row_11) 

# write the current container line
def write_edi_container(f_edi_result_ll, 
                        container_id, pol, pod, 
                        container_type, weight_kg, settings, slot_position, carrier):
    
    # for counting the rows
    nb_rows_add = 0
    
    # the slot position
    row_slot = "LOC+147+" + slot_position + record_tail['slot_position'] + "'\n"
    f_edi_result_ll.write(row_slot)
    
    # weight (bis)
    row_weight = "MEA+WT++KGM:" + "%d" % weight_kg + "'\n"
    f_edi_result_ll.write(row_weight)
    f_edi_result_ll.write(row_weight)
    
    # temperature (if for a reefer, arbitrarily set at -10°C)
    # TMP+2 : transport temperature
    if settings == "R":
        row_temperature = "TMP+2+-10.0:CEL" + "'\n"
        f_edi_result_ll.write(row_temperature)
        nb_rows_add += 1
    
    # POL
    row_pol = "LOC+9+" + pol + record_tail['pol_pod'] + "'\n"
    f_edi_result_ll.write(row_pol)
    
    # POD
    row_pod = "LOC+11+" + pod + record_tail['pol_pod'] + "'\n"
    f_edi_result_ll.write(row_pod)
    
    # dummy line
    row_dummy = "RFF+BM:1" + "'\n"
    f_edi_result_ll.write(row_dummy)
    
    # container
    s_empty_full = "4" if settings == "E" else "5"
    row_container = "EQD+CN+" + container_id + "+" + container_type + "+++" + s_empty_full + "'\n"
    f_edi_result_ll.write(row_container)
    
    # carrier
    row_carrier = "NAD+CA+" + carrier + record_tail['carrier'] + "'\n"
    f_edi_result_ll.write(row_carrier)
    
    nb_rows = 8 + nb_rows_add
    return nb_rows
    
# write the tail
def write_edi_tail(f_edi_result_ll, nb_lines):
    
    # nb of lines, including this one (+1, but not the following one, not +2)
    row_1 = "UNT+" + str(nb_lines+1) + "+" + date_hour[11:15] + "00001" + "'\n"
    f_edi_result_ll.write(row_1) 
    
    # the end seems to have no new line!!!!
    row_2 = "UNZ+1+" + date_hour[2:4] + date_hour[5:7] + "'"
    f_edi_result_ll.write(row_2) 

# CREATING THE BAPLIE FILE
    
# opening
f_source_result_ll = open(source_result_ll_filename, 'r') 
f_edi_result_ll = open(edi_result_ll_filename, 'w') 

# nb of (relevant) rows for last segment
nb_lines = 0

# main loop
for no_line, line in enumerate(f_source_result_ll):
    
    # passing the source header but writing it 
    if no_line == 0:
        write_edi_header(f_edi_result_ll)
        # 11 lines in the header, but the UNB one is not relevant
        nb_lines += 10
        continue

    # if too short...
    if len(line) < 2: continue
    
    # reading the source and writing the edi
    l_items = line.split(';')
       
    # containers 
    container_id = l_items[0]
    #print(container_id)
    # test container id no longer needed
    # POL POD
    # les 5 premiers caractères pour couper les 2 comme GBSOU2
    pol = l_items[2]
    if len(pol) > 5: pol = pol[0:5]
    pod = l_items[3]
    if len(pod) > 5: pod = pod[0:5]
    # type, weight, and empty or full
    container_type = l_items[4]
    weight_kg = int(l_items[5])
    settings = l_items[6]
    # slot and carrier (slot, ensure a 0 and 7 digits is there in any case)
    # if not, we don't write the container
    #if l_items[7] == '':
    #    continue
    # writing also garbage with no explicit slot, no selection on slot empty or not
    if l_items[7] == '':
        slot_position = ''
    else:
        slot_position = int(l_items[7])
        slot_position = "%07d" % slot_position
    # remove the end of line...
    carrier = l_items[8][:-1]
    
    nb_rows_container = write_edi_container(f_edi_result_ll, 
                                            container_id, pol, pod, 
                                            container_type, weight_kg, settings, 
                                            slot_position, carrier)
    # 8 lines by container
    nb_lines += nb_rows_container
    
# closing
f_source_result_ll.close()

# tail
write_edi_tail(f_edi_result_ll, nb_lines)
f_edi_result_ll.close()

