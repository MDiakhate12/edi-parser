
# Transform Tanks Results into a TANSTA File

# libraries
import os

import vessel_stow_preproc as vsp

# directory and file names
##################
REF_DATA = "c:/Projets/CCS/Vessel Stowage/Python Prétraitement/9454450 Tanks"
#REP_DATA = "c:/Projets/CCS/Vessel Stowage/Python Posttraitement"
REP_DATA="C:/Projets/CCS/Vessel Stowage/Python C++ Modèles/C++/MasterPlanningV11/msvc9/data/ver102810"

#Test8_V11_out_stab_hz6_warm_start_with_objectives
#Test8_V11_out_stab_precise_hz6_warm_start_with_objectives
#Test8_V12_out_stab_hz6_warm_start_with_objectives
#Test8_V12_out_stab_hz7_warm_start_with_objectives
#Test8_V12_out_stab_precise_hz6_warm_start_with_objectives
#Test8_V12_out_stab_precise_hz7_warm_start_with_objectives
# for file names
#RESULT_FILENAME_BASE = "1022_results_AllPorts_AllTANKS"
RESULT_FILENAME_BASE = "results_AllPorts_AllTANKS"
EDI_RESULT_FILENAME_HEAD = "X"
##################

os.chdir(REP_DATA)

### Eléments d'information préalable des tankers

d_tank_names_bv_2_edi = {
    'AFT VOID': ('AFT VOID', 'AFVOI', 'VOID SPACES'),
    'BILGE HOLDING TK(P)': ('BILGE HOLDING TK(P)', 'BIHOL', 'MISCELLANEOUS'),
    'BILGE SETT.TK(P)': ('BILGE SETT.TK(P)', 'BISET', 'MISCELLANEOUS'),
    'BOILER FEED WATER TK': ('BOILER FW TK', 'BOIFW', 'MISCELLANEOUS'),
    'DISPOSAL WATER TK UNDER ACCOM.': ('DISP WT TK ACC.', 'DIWTA', 'MISCELLANEOUS'),
    'DISPOSAL WATER TK(S)': ('DISP WT TK(S)', 'DIWT', 'MISCELLANEOUS'),
    'F.O OVERF.TK(P)': ('FO OVERF.TK(P)', 'FOOVE', 'MISCELLANEOUS'),
    'F.O.SLUDGE TK(P)': ('FO.SLUDGE TK(P)', 'FOSLU', 'MISCELLANEOUS'),
    'F.W.TK(P)': ('FW.TK(P)', 'FWTKP', 'FRESH WATER'),
    'F.W.TK(S)': ('FW.TK(S)', 'FWTKS', 'FRESH WATER'),
    'FWD LOW VOID': ('FWD LOW VOID', 'FWVOI', 'VOID SPACES'),
    'H.F.O SERV.TK(P)': ('HFO SERV.TK(P)', 'HFSER', 'HEAVY FUEL O.'),
    'H.F.O SETT.TK(P)': ('HFO SETT.TK(P)', 'HFSET', 'HEAVY FUEL O.'),
    'L.O.SLUDGE TK(P)': ('LO.SLUDGE TK(P)', 'LOSLU', 'MISCELLANEOUS'),
    'L.S.H.F.O SERV.TK(P)': ('LSHFO SERV.TK(P)', 'LSSER', 'HEAVY FUEL O.'),
    'L.S.H.F.O SETT.TK(P)': ('LSHFO SETT.TK(P)', 'LSSET', 'HEAVY FUEL O.'),
    'M.D.O. SERV.TK(P)': ('MDO SERV.TK(P)', 'DOSER', 'DIESEL OIL'),
    'M.D.O. STOR.TK(S)': ('MDO STOR.TK(S)', 'DOSTO', 'DIESEL OIL'),
    'M/E CYL.O SERV.TK(P)': ('ME CYL.O.SERV(P)', 'COSER', 'LUBRIC.OIL'),
    'M/E J.C.W.DRAIN TK(S)': ('ME JCW.DRAIN TK(S)', 'JCWDR', 'MISCELLANEOUS'),
    'M/E SYS.O SETT.TK(S)': ('ME SYS.O.SETT(S)', 'LOSET', 'LUBRIC.OIL'),
    'M/E SYS.O STOR.TK(S)': ('ME SYS.O.STOR(S)', 'LOSTO', 'LUBRIC.OIL'),
    'M/E SYS.O.SUMP TK': ('ME SYS.O.SUMP TK', 'SUMPT', 'MISCELLANEOUS'),
    'NO.1 M/E CYL.O STOR.TK(S)': ('NO.1 CYLO.STOR(S)', '1COST', 'LUBRIC.OIL'),
    'NO.1 G/E L.O.STOR.TK(P)': ('NO.1 GE.LO.STOR(P)', '1LOST', 'LUBRIC.OIL'),
    'NO.1 H.F.O.TK(CS)': ('NO.1 HFO.TK(CS)', '1HFCS', 'HEAVY FUEL O.'),
    'NO.1 UPP.VOID(P)': ('NO.1 UPP.VOID(P)', '1UVOP', 'VOID SPACES'),
    'NO.1 UPP.VOID(S)': ('NO.1 UPP.VOID(S)', '1UVOS', 'VOID SPACES'),
    'NO.1 VOID(C)': ('NO.1 VOID(C)', '1VOIC', 'VOID SPACES'),
    'NO.2 M/E CYL.O STOR.TK(S)': ('NO.2 CYLO.STOR(S)', '2COST', 'LUBRIC.OIL'),
    'NO.2 DB.W.B.TK(P)': ('NO.2 DB.W.B.TK(P)', '2DBP', 'WATERBALLAST'),
    'NO.2 DB.W.B.TK(S)': ('NO.2 DB.W.B.TK(S)', '2DBS', 'WATERBALLAST'),
    'NO.2 G/E L.O.STOR.TK(P)': ('NO.2 GE.LO.STOR(P)', '2LOST', 'LUBRIC.OIL'),
    'NO.2 H.F.O.TK(CP)': ('NO.2 HFO.TK(CP)', '2HFCP', 'HEAVY FUEL O.'),
    'NO.2 W.W.B.TK(P)': ('NO.2 W.W.B.TK(P)', '2WWP', 'WATERBALLAST'),
    'NO.2 W.W.B.TK(S)': ('NO.2 W.W.B.TK(S)', '2WWS', 'WATERBALLAST'),
    'NO.3 DB.W.B.TK(P)': ('NO.3 DB.W.B.TK(P)', '3DBP', 'WATERBALLAST'),
    'NO.3 DB.W.B.TK(S)': ('NO.3 DB.W.B.TK(S)', '3DBS', 'WATERBALLAST'),
    'NO.3 H.F.O.TK(MS)': ('NO.3 HFO.TK(MS)', '3HFMS', 'HEAVY FUEL O.'),
    'NO.3 W.W.B.TK(P)': ('NO.3 W.W.B.TK(P)', '3WWP', 'WATERBALLAST'),
    'NO.3 W.W.B.TK(S)': ('NO.3 W.W.B.TK(S)', '3WWS', 'WATERBALLAST'),
    'NO.4 H.F.O.TK(MP)': ('NO.4 HFO.TK(MP)', '4HFMP', 'HEAVY FUEL O.'),
    'NO.4A DB.W.B.TK(P)': ('NO.4A DB.W.B.TK(P)', '4ADBP', 'WATERBALLAST'),
    'NO.4A DB.W.B.TK(S)': ('NO.4A DB.W.B.TK(S)', '4ADBS', 'WATERBALLAST'),
    'NO.4A W.W.B.TK(P)': ('NO.4A W.W.B.TK(P)', '4AWWP', 'WATERBALLAST'),
    'NO.4A W.W.B.TK(S)': ('NO.4A W.W.B.TK(S)', '4AWWS', 'WATERBALLAST'),
    'NO.4F DB.W.B.TK(P)': ('NO.4F DB.W.B.TK(P)', '4FDBP', 'WATERBALLAST'),
    'NO.4F DB.W.B.TK(S)': ('NO.4F DB.W.B.TK(S)', '4FDBS', 'WATERBALLAST'),
    'NO.4F W.W.B.TK(P)': ('NO.4F W.W.B.TK(P)', '4FWWP', 'WATERBALLAST'),
    'NO.4F W.W.B.TK(S)': ('NO.4F W.W.B.TK(S)', '4FWWS', 'WATERBALLAST'),
    'NO.5 DB.W.B.TK(P)': ('NO.5 DB.W.B.TK(P)', '5DBP', 'WATERBALLAST'),
    'NO.5 DB.W.B.TK(S)': ('NO.5 DB.W.B.TK(S)', '5DBS', 'WATERBALLAST'),
    'NO.5 H.F.O.TK(S)': ('NO.5 HFO.TK(S)', '5HFS', 'HEAVY FUEL O.'),
    'NO.5 W.W.B.TK(P)': ('NO.5 W.W.B.TK(P)', '5WWP', 'WATERBALLAST'),
    'NO.5 W.W.B.TK(S)': ('NO.5 W.W.B.TK(S)', '5WWS', 'WATERBALLAST'),
    'NO.6 DB.W.B.TK(P)': ('NO.6 DB.W.B.TK(P)', '6DBP', 'WATERBALLAST'),
    'NO.6 DB.W.B.TK(S)': ('NO.6 DB.W.B.TK(S)', '6DBS', 'WATERBALLAST'),
    'NO.6 H.F.O.TK(P)': ('NO.6 HFO.TK(P)', '6HFP', 'HEAVY FUEL O.'),
    'NO.6 W.W.B.TK(P)': ('NO.6 W.W.B.TK(P)', '6WWP', 'WATERBALLAST'),
    'NO.6 W.W.B.TK(S)': ('NO.6 W.W.B.TK(S)', '6WWS', 'WATERBALLAST'),
    'NO.7 H.F.O.TK(S)': ('NO.7 HFO.TK(S)', '7HFS', 'HEAVY FUEL O.'),
    'NO.7 W.B.TK(P)': ('NO.7 W.B.TK(P)', '7WP', 'WATERBALLAST'),
    'NO.7 W.B.TK(S)': ('NO.7 W.B.TK(S)', '7WS', 'WATERBALLAST'),
    'NO.8 H.F.O.TK(P)': ('NO.8 HFO.TK(P)', '8HFP', 'HEAVY FUEL O.'),
    'NO.8 VOID(P)': ('NO.8 VOID(P)', '8VOIP', 'VOID SPACES'),
    'NO.8 VOID(S)': ('NO.8 VOID(S)', '8VOIS', 'VOID SPACES'),
    'SLUDGE SETT.TK(P)': ('SLUDGE SETT.TK(P)', 'SLSET', 'MISCELLANEOUS'),
    'SLUDGE TK(P)': ('SLUDGE TK(P)', 'SLUTK', 'MISCELLANEOUS'),
    'S/T L.O.DRAIN TK': ('ST LO.DRAIN TK', 'STLOD', 'MISCELLANEOUS'),
    'S.T.C.W TK': ('STCW TK', 'STCW', 'MISCELLANEOUS'),
    'WASTE OIL TK': ('WASTE OIL TK', 'WASTE', 'MISCELLANEOUS'),
    'SCRUBBER HOLDING': ('SCRUBBER HOLDING', 'SCHO', 'SCRUBBER'),
    'SCRUBBER RESIDUE': ('SCRUBBER RESIDUE', 'SCRE', 'SCRUBBER'),
    'SCRUBBER SILO 1': ('SCRUBBER SILO 1', 'SCS1', 'SCRUBBER'), 
    'SCRUBBER SILO 2': ('SCRUBBER SILO 2', 'SCS2', 'SCRUBBER'), 
    'SCRUBBER M/E PROC': ('SCRUBBER M/E PROC', 'SCME', 'SCRUBBER'), 
    'SCRUBBER G/E PROC': ('SCRUBBER G/E PROC' 'SCGE', 'SCRUBBER')
    }

tank_type_2_density = {
    'WATERBALLAST': 1.025,
    'FRESH WATER': 1.000,
    'HEAVY FUEL O.': 0.980,
    'DIESEL OIL': 0.850,
    'LUBRIC.OIL': 0.900,
    'MISCELLANEOUS': 1.000, # ???
    'VOID SPACES': 1.025, # !!!
    'SCRUBBER': 1.000
}

d_seq_port = {
    1: 'CNTXG',
    2: 'KRPUS',
    3: 'CNNGB',
    4: 'CNSHA',
    5: 'CNYTN',
    6: 'SGSIN',
    7: 'FRDKK',
    8: 'GBSOU',
    9: 'DEHAM',
    10: 'NLRTM',
    11: 'GBSOU2',
    12: 'ESALG',
    13: 'MYPKG'
}

d_port_seq = {
    'CNTXG': 1,
    'KRPUS': 2,
    'CNNGB': 3,
    'CNSHA': 4,
    'CNYTN': 5,
    'SGSIN': 6,
    'FRDKK': 7,
    'GBSOU': 8,
    'DEHAM': 9,
    'NLRTM': 10,
    'GBSOU2': 11,
    'ESALG': 12,
    'MYPKG': 13
}

os.chdir(REF_DATA)
# d_tanks_basic_infos contains (capacity, first_frame, last_frame)
d_tanks_basic_infos = vsp.read_tanks_basic_infos()

os.chdir(REP_DATA)
### Lecture du fichiers résultats source

# for some (useless ?) complements to the tansta records
record_tail = {
    'pol_pod': ':139:6', 
    'slot_position': '::5', 
    'carrier': ':172:20'
}
org_name = "CMA"
date_hour = "2021-07-16-115900"
local_id = "R0C56A02250A0D"
vessel_name = "CMA CGM JULES VERNE"
date_hour_bis = "0101010000"
date_hour_ter = "0101010000"

# file names
source_result_tk_filename = RESULT_FILENAME_BASE + ".csv"

# First reading the input to put together tanks (and to perform checks)

# collecting tank's sub-tanks at the level of ports
d_ports_tanks = {}

f_source_result_tk = open(source_result_tk_filename, 'r') 

for no_line, line in enumerate(f_source_result_tk):
    
    if no_line == 0: continue
        
    l_items = line.split(';')
    port_name = l_items[0].strip()
    port_no = d_port_seq[port_name]
    st_name = l_items[1].strip()
    tank_name = l_items[3].strip()
    block_id = int(l_items[4].strip())
    tier_no = int(l_items[2].strip())
    st_weight = float(l_items[5].strip())
    l_cg = float(l_items[6].strip())
    t_cg = float(l_items[7].strip())
    v_cg = float(l_items[8].strip())
    st_max_weight = float(l_items[9].strip())
    
    if port_no not in d_ports_tanks:
        d_ports_tanks[port_no] = {}
    if tank_name not in d_ports_tanks[port_no]:
        d_ports_tanks[port_no][tank_name] = []
    d_ports_tanks[port_no][tank_name].append((block_id, tier_no, st_weight, l_cg, t_cg, v_cg, st_max_weight))

f_source_result_tk.close()

# main function transforming subtanks into tanks
def check_aggregate(tank_name, l_tank_subtanks, is_waterballast, port_no):
    
    # get tank total weight (et check max weight)
    tank_weight = 0.0
    tank_max_weight = 0.0
    total_l_cg = 0.0; total_t_cg = 0.0; total_v_cg = 0.0
    for (block_id, tier_no, st_weight, l_cg, t_cg, v_cg, st_max_weight) in l_tank_subtanks:
        tank_weight += st_weight
        tank_max_weight += st_max_weight
        total_l_cg += st_weight * l_cg 
        total_t_cg += st_weight * t_cg 
        total_v_cg += st_weight * v_cg 
    tank_l_cg = total_l_cg / tank_weight if tank_weight != 0.0 else 0.0
    tank_t_cg = total_t_cg / tank_weight if tank_weight != 0.0 else 0.0
    tank_v_cg = total_v_cg / tank_weight if tank_weight != 0.0 else 0.0
    
    # for waterballast, checking, by getting filling ratio for each tank, look at levels
    if is_waterballast == True:
        d_tier_fillings = {}
        for (block_id, tier_no, st_weight, l_cg, t_cg, v_cg, st_max_weight) in l_tank_subtanks:
            if tier_no not in d_tier_fillings:
                d_tier_fillings[tier_no] = []
            filling = st_weight / st_max_weight
            if filling < 1e-8: filling = 0
            d_tier_fillings[tier_no].append(filling)
        #print(tank_name)
        #print(d_tier_fillings)
            
        # filling ratios must be the same at any tier
        # and if positive, all under them must have a ratio = 1
        l_tier_fillings = [(tier_no, l_fillings) for tier_no, l_fillings in d_tier_fillings.items()]
        l_tier_fillings.sort(key=lambda x: x[0])
        bottom_filled = True
        for (tier_no, l_fillings) in l_tier_fillings:
            for no_block, filling in enumerate(l_fillings):
                
                if bottom_filled == False and filling > 0.0:
                    print("at port %d, tank %s, tier %d filled in the air" % (port_no, tank_name, tier_no))
                
                if no_block == 0:
                    filling_reference = filling
                else:
                    # if too strict compare on strings %.2f
                    s_filling = "%.2f" % filling
                    s_filling_reference = "%.2f" % filling_reference
                    if s_filling != s_filling_reference:
                        print("at port %d, tank %s, at tier %d filling levels %s %s" %\
                             (port_no, tank_name, tier_no, s_filling_reference, s_filling))
                
            if filling_reference < 1.0:
                bottom_filled = False
                
    
    return tank_weight, tank_max_weight, tank_l_cg, tank_t_cg, tank_v_cg

# constitution effective des tanks
l_ports_tanks = []    

for port_no, d_tanks in d_ports_tanks.items():
    
    l_tanks = []
    for tank_name, l_tank_subtanks in d_tanks.items():
        
        is_waterballast = False
        if d_tank_names_bv_2_edi[tank_name][2] == 'WATERBALLAST': is_waterballast = True
            
        tank_weight, tank_max_weight, tank_l_cg, tank_t_cg, tank_v_cg =\
        check_aggregate(tank_name, l_tank_subtanks, is_waterballast, port_no)
    
        l_tanks.append((tank_name, tank_weight, tank_l_cg, tank_t_cg, tank_v_cg))
        
    l_ports_tanks.append((port_no, l_tanks))
    
### Writing
    
# writing the header's several rows
def write_edi_header(f_edi_result_tk, port):
    
    row_1 = "UNB+UNOA:1+" + org_name + "+" + org_name + "+"\
          + date_hour[2:4] + date_hour[5:7] + date_hour[8:10]\
          + ":" + date_hour[11:15] + "+" + local_id\
          + "++" + "+++" + "COLSUEZ" + "'\n"
    f_edi_result_tk.write(row_1)
    
    row_2 = "UNH+" + local_id\
          + "+TANSTA" + ":4:04B:UN:SMDG03" + "'\n"
    f_edi_result_tk.write(row_2)
    
    row_3 = "BGM++" + local_id + "+9" + "'\n"
    f_edi_result_tk.write(row_3)
    
    row_4 = "DTM+137:"\
          + date_hour[2:4] + date_hour[5:7] + date_hour[8:10]\
          + date_hour[11:15]\
          + ":201" + "'\n"
    f_edi_result_tk.write(row_4)
    
    row_5 = "TDT+20++1:MARITIME+" + org_name + "++++:103:ZZZ:" + vessel_name + "'\n"
    f_edi_result_tk.write(row_5)
    
    row_6 = "LOC+5+" + port + record_tail['pol_pod'] + "'\n"
    f_edi_result_tk.write(row_6)
    
    row_7 = "LOC+61+" + port + record_tail['pol_pod'] + "'\n"
    f_edi_result_tk.write(row_7)
    
    row_8 = "DTM+132:" + date_hour_bis[0:10] + ":201" + "'\n"
    f_edi_result_tk.write(row_8)
    
    row_9 = "DTM+133:" + date_hour_ter[0:10] + ":201" + "'\n"
    f_edi_result_tk.write(row_9)


# write the current line
def write_edi_tank(f_edi_result_tk, 
                   tank_name, weight, 
                   l_cg, t_cg, v_cg,
                   d_tank_names_bv_2_edi, d_tanks_basic_infos, tank_type_2_density):
    
    # first get information to store in EDI
    edi_name = d_tank_names_bv_2_edi[tank_name][0]
    edi_short_name = d_tank_names_bv_2_edi[tank_name][1]
    tank_type = d_tank_names_bv_2_edi[tank_name][2]
    density = tank_type_2_density[tank_type]
    capacity = d_tanks_basic_infos[tank_name][0]
    max_weight = capacity * density
    filling = weight / max_weight
    volume = capacity * filling
    
    # the tank position
    row_tank = "LOC+ZZZ+" + edi_short_name + ":::" + edi_name + "'\n"
    f_edi_result_tk.write(row_tank)
    
    # weight
    row_weight = "MEA+WT++TNE:" + ("%.1f" % weight) + "'\n"
    f_edi_result_tk.write(row_weight)
    
    # density
    row_density = "MEA+DEN++D41:" + ("%.3f" % density) + "'\n"
    f_edi_result_tk.write(row_density)
    
    # volume
    row_volume = "MEA+VOL++MTQ:" + ("%.3f" % volume) + "'\n"
    f_edi_result_tk.write(row_volume)
    
    # filling factor
    row_filling = "MEA+ACA++P1:" + ("%.1f" % (filling * 100.0)) + "'\n"
    f_edi_result_tk.write(row_filling)
    
    # gravity center
    row_cg = "DIM+1+MTR:" + ("%.3f" % l_cg) + ":" + ("%.3f" % t_cg) + ":" + ("%.3f" % v_cg) + "'\n"
    f_edi_result_tk.write(row_cg)
    
    # type
    row_type = "FTX+AAI+++" + tank_type + "." + "'\n"
    f_edi_result_tk.write(row_type)  

# write the tail
def write_edi_tail(f_edi_result_tk, nb_lines):
    
    # nb of lines, including this one (+1, but not the following one, not +2)
    row_1 = "UNT+" + str(nb_lines) + "+" + local_id + "'\n"
    f_edi_result_tk.write(row_1) 
    
    # the end seems to have no new line!!!!
    row_2 = "UNZ+1+" + local_id + "'"
    f_edi_result_tk.write(row_2)

    
#writing port by port    
for port_no, l_tanks in l_ports_tanks:
    
    port = d_seq_port[port_no]
    if len(port) == 6: port = port[0:-1] # enlever le 2 des ports en double (GBSOU2)
    
    edi_result_tk_filename = EDI_RESULT_FILENAME_HEAD + date_hour + ("_%02d_" % port_no)\
                           + RESULT_FILENAME_BASE + "_22.edi"
    edi_result_tk_filename = EDI_RESULT_FILENAME_HEAD + ("_%02d_" % port_no)\
                           + RESULT_FILENAME_BASE + ".edi"
    
    f_edi_result_tk = open(edi_result_tk_filename, 'w') 

    # nb of (relevant) rows for last segment
    nb_lines = 0
    
    # header
    write_edi_header(f_edi_result_tk, port)
    # 9 lines in the header
    nb_lines += 9

    # main loop
    for (tank_name, tank_weight, tank_l_cg, tank_t_cg, tank_v_cg) in l_tanks:
        write_edi_tank(f_edi_result_tk, 
                       tank_name, tank_weight, 
                       tank_l_cg, tank_t_cg, tank_v_cg,
                       d_tank_names_bv_2_edi, d_tanks_basic_infos, tank_type_2_density)
  
    # 7 lines by tank
    nb_lines += 7 * len(l_tanks)
    
    # tail
    write_edi_tail(f_edi_result_tk, nb_lines)
    f_edi_result_tk.close()

