#!/usr/bin/env python
# coding: utf-8

# # Preprocessing for Stress & Stab

#########################################################################
# ### Run Parameters
# Context (loading a BV condition, or ordinary preparation)

# for a BV condition or not
#BV_condition = '01' # a string,here for condition 4
BV_condition = None # if not within a BV condition

# with WB filled for compensing trim
WB_compensating_trim = True
# condition trim, used as a parameter to buoyancies calculations
condition_trim = 0.0

# with BV condition loading ports and discharge ports, for BV conditions cargo file obtained from macS3
BV_condition_load_port = 'MYPKG'
BV_condition_disch_port = 'KRPUS'
# as well as for tank files
#BV_condition_tank_port = 'CNTXG'
BV_condition_tank_port = 'CNTXG'

# format des tanks EDI (à garder à None si on n'est pas sûr)
edi_tank_format = None
filter_out_wb = None
# A RECOMMENTARISER ENSUITE
edi_tank_format = 'EDI_QUOTE'
filter_out_wb = False

# fichier spécifique EDI
# paramètres de départ
specific_edi_filename = None
edi_container_nb_header_lines = 9
edi_structure = 'EQD'
# spécifiques au test torsion
# A RECOMMENTARISER ENSUITE
specific_edi_filename = "ctrs.edi"
edi_container_nb_header_lines = 12
edi_structure = 'CNT'


# inclusion or not of deadweights
INCLUDE_DEADWEIGHT = True
# following instruction can be overriden
if BV_condition == '01':
    INCLUDE_DEADWEIGHT = False
    
# reading which hydrostatic file, every 0.5m or every 0.05m
HYDROSTATICS_PRECISION = 0.05 # or 0.5
HYDROSTATICS_REF_DRAFT = 'Extreme' 
BONJEAN_REF_DRAFT = 'Moulded'

# there might be a gap between drafts we are using and draft in the Bonjean Table
# Moulded vs Extreme
EXTREME_MINUS_MOULDED = 0.028
# if bonjean table and the hydrostatic is using the same kind of draft
if HYDROSTATICS_REF_DRAFT == BONJEAN_REF_DRAFT:
    gap_bonjean_ref = 0.0
# if we are using as reference extreme draft, while the Bonjean is based on moulded draft,
# we must use the latter for the interpolation function, pass the draft in our reference - the difference
if HYDROSTATICS_REF_DRAFT == 'Extreme' and BONJEAN_REF_DRAFT == 'Moulded':
    gap_bonjean_ref = -1 * EXTREME_MINUS_MOULDED
# the reverse
if HYDROSTATICS_REF_DRAFT == 'Moulded' and BONJEAN_REF_DRAFT == 'Extreme':
    gap_bonjean_ref = 1 * EXTREME_MINUS_MOULDED

##########################################################################
# ###### Files and parameters

# local directory
REP_DATA = "c:/Projets/CCS/Vessel Stowage/Python Prétraitement"

REP_TANKS_DATA = REP_DATA + "/9454450 Tanks"
if BV_condition is None:
    REP_TANKS_PORTS_DATA = REP_DATA + "/9454450 Tanks Ports"
else:
    REP_TANKS_PORTS_DATA = REP_DATA + "/9454450 BV Conditions"

if BV_condition is None:
    REP_LOADLIST_DATA = REP_DATA + "/9454450 Rotation"
else:
    REP_LOADLIST_DATA = REP_DATA + "/9454450 BV Conditions"

# A RECOMMENTARISER ENSUITE
# cas particulier test torsion
REP_TANKS_PORTS_DATA = REP_DATA + "/point 15-stress/tanks"
REP_LOADLIST_DATA = REP_DATA + "/point 15-stress/containers"


# file names
FRAMES_FILENAME = "9454450 Frames.csv"
LIGHTWEIGHT_FILENAME = "9454450 Lightweight.csv"
DEADWEIGHT_CONSTANT_FILENAME = "9454450 Deadweight Constant.csv"
BLOCKS_FILENAME = "9454450 Blocks.csv"
HYDROSTATICS_GROSS_FILENAME = "9454450 Hydrostatics Gross.csv"
HYDROSTATICS_FINE_FILENAME = "9454450 Hydrostatics Fine.csv"
BONJEAN_FRAME_AREA_FILENAME = "9454450 Bonjean Frame Area.csv"
SF_ENVELOPPE_FILENAME = "9454450 SF Enveloppe.csv"
BM_ENVELOPPE_FILENAME = "9454450 BM Enveloppe.csv"
GM_MIN_CURVE_FILENAME = "9454450 GMMin Curve.csv"

RAW_BV_CONDITIONS_FILENAME = "9454450 Raw BV Conditions.txt"

###################################################################
###################################################################

# libraries
import os

import vessel_stow_preproc as vsp


# ## Loading static data files

os.chdir(REP_DATA)


d_frames, l_frames = vsp.read_frames(FRAMES_FILENAME)
#print(l_frames)

l_lightweight_elems = vsp.read_lightweight_elems(LIGHTWEIGHT_FILENAME, 
                                                 INCLUDE_DEADWEIGHT, DEADWEIGHT_CONSTANT_FILENAME)
#print(l_lightweight_elems)

l_blocks, d_frames_block = vsp.read_blocks(BLOCKS_FILENAME, d_frames)
#print(l_blocks)
#print(d_frames_block)

d_hydrostatics_by_trim = vsp.read_hydrostatics(HYDROSTATICS_PRECISION, 
                                               HYDROSTATICS_GROSS_FILENAME, HYDROSTATICS_FINE_FILENAME)

d_frame_area_by_draft, d_frame_area_by_x_draft, l_x_in_grid, l_drafts_in_grid =vsp.read_bonjean_frame_area(BONJEAN_FRAME_AREA_FILENAME)

d_sf_enveloppe_by_frame, l_sf_enveloppe = vsp.read_sf_enveloppe(SF_ENVELOPPE_FILENAME)

d_bm_enveloppe_by_frame, l_bm_enveloppe = vsp.read_bm_enveloppe(BM_ENVELOPPE_FILENAME)

l_gm_min_points, l_gm_min_curve = vsp.read_gm_min_curve(GM_MIN_CURVE_FILENAME)

vessel_total_lightweight, vessel_total_deadweight,vessel_light_x_CG, vessel_light_y_CG, vessel_light_z_CG,vessel_dead_x_CG, vessel_dead_y_CG, vessel_dead_z_CG,vessel_max_TM = vsp.read_vessel_elems()


# ## Preparing data for Stress & Stab

# #### Vessel
# - float totalWeight
# - float xCG
# - float yCG 
# - float zCG

if INCLUDE_DEADWEIGHT == True:
    vessel_total_weight = vessel_total_lightweight + vessel_total_deadweight
    vessel_x_CG = (1/vessel_total_weight) *                   (vessel_light_x_CG * vessel_total_lightweight + vessel_dead_x_CG * vessel_total_deadweight)
    vessel_y_CG = (1/vessel_total_weight) *                   (vessel_light_y_CG * vessel_total_lightweight + vessel_dead_y_CG * vessel_total_deadweight)
    vessel_z_CG = (1/vessel_total_weight) *                   (vessel_light_z_CG * vessel_total_lightweight + vessel_dead_z_CG * vessel_total_deadweight)
    
else:
    vessel_total_weight = vessel_total_lightweight
    vessel_x_CG = vessel_light_x_CG
    vessel_y_CG = vessel_light_y_CG
    vessel_z_CG = vessel_light_z_CG
    
# also compute x_compute_TM, which is located at the exact middle of the vessel
vessel_start = l_frames[0][1]
vessel_end = l_frames[-1][2]
vessel_length = vessel_end - vessel_start
vessel_middle = vessel_start + 0.5 * vessel_length

vessel_x_compute_TM = vessel_middle


# Ecriture du vaisseau, dépend de la condition via les lightweights (au sens large du terme)
PREPROCESSED_VESSEL_BASENAME = "9454450 Preprocessed Vessel"
if BV_condition is not None:
    fn_prepro_vessel = "%s C%s.csv"% (PREPROCESSED_VESSEL_BASENAME, BV_condition)
else:
    fn_prepro_vessel = "%s.csv"% (PREPROCESSED_VESSEL_BASENAME)

f_prepro_vessel = open(fn_prepro_vessel, 'w')

s_header = "TotalWeight;xCG;yCG;zCG;maxTM;xComputeTM\n"
f_prepro_vessel.write(s_header)

s_row = "%.1f;%.2f;%.3f;%.3f;%.0f;%.2f\n" %    (vessel_total_weight, vessel_x_CG, vessel_y_CG, vessel_z_CG, vessel_max_TM, vessel_x_compute_TM)
f_prepro_vessel.write(s_row)

f_prepro_vessel.close()


# #### List of blocks
# - int blockId // clé du fichier de départ, par ordre croissant, en partant de l'arrière du navire
# - string bayId // numéro de la baie
# - float bayCoef // coefficient sur la baie
# <p> à partir des frames utilisés (départ et fin), des longueurs lues dans le fichier des frames, des trapèzes des éléments lus dans le fichier des éléments, on peut déduire :
# - float lightWeight // sommer les poids des éléments sur la longueur du bloc
# - float xCG // sommer les produits abscisse * poids éléments sur les éléments sur la longueur du bloc, et diviser par les poids des éléments
# - float xCGCont // centre de gravité longitudinal de la baie de containers
# - yCG // celui du bâteau + dead
# - xFront // block limit
# - xBack // block limit
# <p> pour les profils, un bloc est compris entre 2 points BV (sauf les extrémités), on renvoie la valeur de début, et la valeur de fin de bloc :
# - float maxSF // en fait maxSF_start, maxSF_end,
# - float minSF // en fait minSF_start, minSF_end,
# - float maxBM // en fait maxBM_start, maxBM_end,
# - float minBM // en fait minBM_start, minBM_end,

l_preproc_blocks = []

# boucler sur la liste des blocs
for (block_no, pos_start, pos_end, first_frame, last_frame, pos_bay_xcg, no_bay, bay_coeff) in l_blocks:

    block_id = block_no
    bay_id = no_bay
    bay_coef = bay_coeff
    
    light_weight = vsp.get_segment_elems_weight(pos_start, pos_end, l_lightweight_elems)
    #x_CG = get_segment_gravity_center_2(pos_start, pos_end, l_lightweight_elems)
    x_CG = vsp.get_segment_gravity_center(pos_start, pos_end, l_lightweight_elems)
    #print("CG", pos_start, pos_end, x_CG)
    x_CG_cont = pos_bay_xcg
    if x_CG_cont is None: x_CG_cont = 0.0
    
    y_CG = vessel_y_CG
    x_front = pos_end
    x_back = pos_start
    
    # get (and at extremities, compute) the SF and BM values
    max_SF_back, max_SF_front, min_SF_back, min_SF_front =    vsp.get_enveloppe_max_min_back_front(first_frame, last_frame, d_sf_enveloppe_by_frame, l_sf_enveloppe,
                                     pos_start, pos_end, l_blocks)
    #print("SF", max_SF_back, max_SF_front, min_SF_back, min_SF_front)
    max_BM_back, max_BM_front, min_BM_back, min_BM_front =    vsp.get_enveloppe_max_min_back_front(first_frame, last_frame, d_bm_enveloppe_by_frame, l_bm_enveloppe,
                                     pos_start, pos_end, l_blocks)

    #print("BM", max_BM_back, max_BM_front, min_BM_back, min_BM_front)
    l_preproc_blocks.append((block_id, bay_id, bay_coef,
                             light_weight, x_CG, x_CG_cont, y_CG, x_front, x_back,
                             max_SF_back, max_SF_front, min_SF_back, min_SF_front,
                             max_BM_back, max_BM_front, min_BM_back, min_BM_front))    

# Ecriture des blocs, dépend de la condition via les lightweights (au sens large du terme)
PREPROCESSED_BLOCKS_BASENAME = "9454450 Preprocessed Blocks"
if BV_condition is not None:
    fn_prepro_blocks = "%s C%s.csv"% (PREPROCESSED_BLOCKS_BASENAME, BV_condition)
else:
    fn_prepro_blocks = "%s.csv"% (PREPROCESSED_BLOCKS_BASENAME)

f_prepro_blocks = open(fn_prepro_blocks, 'w')


s_header = "BlockId;BayId;BayCoef;"
s_header += "LightWeight;xCG;xCGCont;yCG;xFront;xBack;"
s_header += "maxSFBack;maxSFFront;minSFBack;minSFFront;"
s_header += "maxBMBack;maxBMFront;minBMBack;minBMFront\n"
f_prepro_blocks.write(s_header)

for (block_id, bay_id, bay_coef,
     light_weight, x_CG, x_CG_cont, y_CG, x_front, x_back,
     max_SF_back, max_SF_front, min_SF_back, min_SF_front,
     max_BM_back, max_BM_front, min_BM_back, min_BM_front) in l_preproc_blocks:
    s_row = "%d;%s;%.2f;%.1f;%.2f;%.2f;%.2f;%.2f;%.2f;%.0f;%.0f;%.0f;%.0f;%.0f;%.0f;%.0f;%.0f\n" %                                 (block_id, bay_id, bay_coef,
                                  light_weight, x_CG, x_CG_cont, y_CG, x_front, x_back,
                                  max_SF_back, max_SF_front, min_SF_back, min_SF_front,
                                  max_BM_back, max_BM_front, min_BM_back, min_BM_front)
    f_prepro_blocks.write(s_row)

f_prepro_blocks.close()


# #### Liste of drafts
# - int draftNum // identifiant du draft, entier par ordre croissant
# On utilise les valeurs limites de la table hydrostatique entre 3.5 et 18 (incluse dans la table Bonjean entre 0 et 23.5)
# - float drft // tous les 0.5 mètres (cf table hydrostatique)
# - float totalWeight // poids total (structure + réservoirs + conteneurs) du navire = déplacement
# - float GMmin // GM minimum permis, donné par segments de droite en fonction du draft (calcul linéaire)
# - un certain nombre de données sont lues dans la table hydrostatique, avec (au moins dans un premier temps) le trim = 0
# - float xCB // LCB directement lu à partir du draft
# - float zM // KMT directement lu à partir du draft
# - float totalWeightMax // limite de validité = (totalWeight(curDraft) + totalWeight(curDraft+1)) / 2. On met une valeur simplement extrapolée
# - float totalWeightMin // limite de validité = (totalWeight(curDraft) + totalWeight(curDraft-1)) / 2. On met une valeur simplement extrapolée 
# - float xCGdelta // margin on CDG of vessel + tank + container, en dur 1.0
# - float yCGdelta // en dur 0.5

# condition_trim has been set in the run parameters
d_hydrostatics_by_draft = d_hydrostatics_by_trim[condition_trim]
l_hydrostatics_by_draft =    [(draft, draft_hydrostatics_data) for draft, draft_hydrostatics_data in d_hydrostatics_by_draft.items()]
l_hydrostatics_by_draft.sort(key=lambda x: x[0])


l_preproc_drafts = []

# note T_TK is not used, T for new fine file, it is the moulded draft, 
# TK for the coarse file, it is the extreme draft
# and conversely, draft is extreme draft for the new fine file, and moulded for the coarse old one
for no_draft, (draft, (T_TK, DISP, LCF, LCB, VCB, TPC, MCT, KMT)) in enumerate(l_hydrostatics_by_draft):
    
    draft_num = no_draft + 1
    drft = draft
    
    total_weight = DISP
    
    gm_min = vsp.get_gm_min(draft ,l_gm_min_curve)
    
    x_CB = LCB
    z_M = KMT
    
    if no_draft < len(l_hydrostatics_by_draft) - 1:
        next_total_weight = l_hydrostatics_by_draft[no_draft+1][1][1]
    else:
        # interpolation (drafts are regularly spaced)
        previous_total_weight = l_hydrostatics_by_draft[no_draft-1][1][1]
        next_total_weight = total_weight + (total_weight - previous_total_weight)
    total_weight_max = 0.5 * (total_weight + next_total_weight)
    
    if no_draft > 0:
        previous_total_weight = l_hydrostatics_by_draft[no_draft-1][1][1]
    else:
        # interpolation (drafts are regularly spaced)
        next_total_weight = l_hydrostatics_by_draft[no_draft+1][1][1]
        previous_total_weight = total_weight - (next_total_weight - total_weight)
    total_weight_min = 0.5 * (total_weight + previous_total_weight)
    
    x_CG_delta = 0.05
    y_CG_delta = 0.02
    
    l_preproc_drafts.append((draft_num, drft, total_weight, gm_min, x_CB, z_M, 
                             total_weight_max, total_weight_min, x_CG_delta, y_CG_delta))


# Ecriture des drafts, ne dependent pas de la condition
PREPROCESSED_DRAFTS_BASENAME = "9454450 Preprocessed Drafts"
fn_prepro_drafts = "%s.csv"% (PREPROCESSED_DRAFTS_BASENAME)

f_prepro_drafts = open(fn_prepro_drafts, 'w')

s_header = "DraftNum;Drft;TotalWeight;GMMin;xCB;zM;"
s_header += "TotalWeightMax;TotalWeightMin;xCGdelta;yCGdelta\n"
f_prepro_drafts.write(s_header)

for (draft_num, drft, total_weight, gm_min, x_CB, z_M, 
     total_weight_max, total_weight_min, x_CG_delta, y_CG_delta) in l_preproc_drafts:
    s_row = "%d;%.2f;%.2f;%.4f;%.3f;%.3f;%.2f;%.2f;%.2f;%.2f\n" %                                 (draft_num, drft, total_weight, gm_min, x_CB, z_M, 
                                  total_weight_max, total_weight_min, x_CG_delta, y_CG_delta)
    f_prepro_drafts.write(s_row)

f_prepro_drafts.close()


# #### Liste de buoyancy
# - int draftNum, int blockId
# - float buoyancy // à partir de la table de Bonjean, on a la ligne, mais pour les colonnes, il faut une interpolation
# - float xCB // l'équivalent du centre de gravité, mais avec les volumes d'eau

# having a dictionary getting draft_num depending on draft
d_draft_num_draft = {}
for (draft_num, drft, total_weight, gm_min, x_CB, z_M, 
     total_weight_max, total_weight_min, x_CG_delta, y_CG_delta) in l_preproc_drafts:
    d_draft_num_draft[draft_num] = drft


f_area_interpolation = vsp.build_area_interpolation_function(l_drafts_in_grid, l_x_in_grid, d_frame_area_by_draft,
                                                             'cubic')

# condition_trim has been set in the overall context
# number of points for interpolation
nb_points = 5

# get for each block the list of points to be used for computing the buoyancy
l_blocks_l_x = []
for (block_no, pos_start, pos_end, first_frame, last_frame, pos_bay_xcg, bay_no, bay_coeff) in l_blocks:
    
    l_x = []
    for s in range(nb_points):
        # nb points
        x = pos_start + (s * (pos_end - pos_start) / (nb_points-1))
        l_x.append(x)
    # and add points in the grid, just in case
    for x in l_x_in_grid:
        if x >= pos_start and x <= pos_end:
            if x not in l_x: 
                l_x.append(x)
    l_x.sort(key=lambda x: x)
    
    l_blocks_l_x.append((block_no, l_x))


# we must compute for each (draft_num, no_block) the buoyancy and bcg
# by draft, and then by block
d_draft_num_block_buoyancy_bcg = {}
for draft_num, drft in d_draft_num_draft.items():
    #print("DRAFT:", drft)
    # possible gaps between Bonjean and our reference
    condition_draft = drft + gap_bonjean_ref
    for (block_no, l_x) in l_blocks_l_x:
        #print("BLOCK:", block_no, l_x)
        seg_buoyancy, seg_bcg = vsp.get_segment_buoyancy_bcg(l_x, condition_draft, condition_trim,
                                                             vessel_length, vessel_middle, 
                                                             f_area_interpolation)
        d_draft_num_block_buoyancy_bcg[(draft_num, block_no)] = seg_buoyancy, seg_bcg


l_draft_num_block_buoyancy_bcg = [((draft_num, no_block), (buoyancy, bcg))            for (draft_num, no_block), (buoyancy, bcg) in d_draft_num_block_buoyancy_bcg.items()]
l_draft_num_block_buoyancy_bcg.sort(key=lambda x: x[0])


# Writing buoyancies, ne dépend pas de la condition (seules les positions sont utilisées de l_blocks)
PREPROCESSED_BUOYANCIES_BASENAME = "9454450 Preprocessed Buoyancies"
fn_prepro_buoyancies = "%s.csv"% (PREPROCESSED_BUOYANCIES_BASENAME)

f_prepro_buoyancies = open(fn_prepro_buoyancies, 'w')

s_header = "DraftNum;BlockId;Buoyancy;xCB\n"
f_prepro_buoyancies.write(s_header)

for ((draft_num, no_block), (buoyancy, bcg)) in l_draft_num_block_buoyancy_bcg:
    s_row = "%d;%d;%.2f;%.2f\n" % (draft_num, no_block, buoyancy, bcg)
    f_prepro_buoyancies.write(s_row)

f_prepro_buoyancies.close()


# ### List of tanks

d_tank_names_edi_2_bv, l_sel_tank_types, void_tank_type, wb_tank_type, l_unknown_tanks = vsp.read_tank_elems()


# ### List of tanks subdivided into sub-tanks

# #### Liste des sub-tanks (WB en fait)
# - StName // subtank name = tank name + block + tier
# - TankName // tank name
# - BlockId // starting at 1 from the back
# - TierId // starting at 1 from the bottom 
# - StCapacity // m3
# - xCG 
# - yCG
# - zCG
# - TotalCapacity // to simplify calculations

os.chdir(REP_TANKS_DATA)

l_wb_subtanks = vsp.get_waterballast_subtanks(d_frames, d_frames_block)

# back to main directory
os.chdir(REP_DATA)

# writing the tank (WB) file, which is always the same
PREPROCESSED_WB_TANKS_FILENAME = "9454450 Preprocessed WB Tanks.csv"

f_prepro_wb_tanks = open(PREPROCESSED_WB_TANKS_FILENAME, 'w')

s_header = "StName;TankName;BlockId;TierId;"
s_header += "StCapacity;xCG;yCG;zCG;TotalCapacity\n"
f_prepro_wb_tanks.write(s_header)

for (tank_name, id_block_st, tier_st, capacity_st, x_cg_st, y_cg_st, z_cg_st, capacity)    in l_wb_subtanks:
    st_name = "%s_%s_%d" % (tank_name, id_block_st, tier_st)
    s_row = "%s;%s;%s;%d;%.1f;%.3f;%.3f;%.3f;%.1f\n" %    (st_name, tank_name, id_block_st, tier_st, 
     capacity_st, x_cg_st, y_cg_st, z_cg_st, capacity)
    f_prepro_wb_tanks.write(s_row)

f_prepro_wb_tanks.close()


# ## Loading dynamic data files

# #### Liste des sub-tanks avec leur chargement
# - StName // subtank name = tank name + block + tier
# - TankName // tank name
# - BlockId // starting at 1 from the back
# - StVolume // m3
# - StWeight // t
# - xCG 
# - yCG
# - zCG

# ### Reading TANKSTA edi files with conditions in ports

# ###### preliminary stage, store first and last frames for all tanks

os.chdir(REP_TANKS_DATA)


d_tanks_basic_infos = vsp.read_tanks_basic_infos()


# ###### main stage, read the edi files

# #### Get list of filled tanks by port(s)

os.chdir(REP_TANKS_PORTS_DATA)

# for the time being, the EDI format is depending on the other parameters,
# unless already defined
if edi_tank_format is None:
    if BV_condition is None: 
        edi_tank_format = 'EDI_CRLF'
    else:
        if WB_compensating_trim == False:
            edi_tank_format = 'EDI_QUOTE'
        else:
            edi_tank_format = 'EDI_CRLF'
if filter_out_wb is None:
    if BV_condition is None: 
        filter_out_wb = True
    else:
        filter_out_wb = False

l_filled_tanks_ports = vsp.get_l_filled_tanks_ports(BV_condition, BV_condition_tank_port, WB_compensating_trim,
                                                    edi_tank_format,
                                                    void_tank_type, l_unknown_tanks, 
                                                    l_sel_tank_types, filter_out_wb)


# #### Back to common track, get subtanks and write file

l_filled_subtanks = []
for (port_name, tank_edi_name, volume, weight, l_cg, t_cg, v_cg) in l_filled_tanks_ports:
    
    # get first and last frame of the tank
    tank_name = d_tank_names_edi_2_bv[tank_edi_name]
    first_frame = d_tanks_basic_infos[tank_name][1]
    last_frame = d_tanks_basic_infos[tank_name][2]
    
    l_filled_tank_subtanks = vsp.get_l_filled_tank_subtanks(first_frame, last_frame, volume, weight, l_cg, t_cg, v_cg,
                                                            d_frames, d_frames_block)
    # complete with port and tank name
    l_filled_tank_subtanks = [(port_name, tank_name, id_block_x, volume_st, weight_st, x_cg_st, y_cg, z_cg)                          for (id_block_x, volume_st, weight_st, x_cg_st, y_cg, z_cg) in l_filled_tank_subtanks]
    
    l_filled_subtanks.extend(l_filled_tank_subtanks)


os.chdir(REP_DATA)

PREPROCESSED_FILLED_TANKS_PORTS_BASENAME = "9454450 Preprocessed Filled Tanks Ports"
PREPROCESSED_FILLED_TANKS_PORTS_BASENAME = "Filled Tanks"
if BV_condition is not None:
    fn_prepro_filled_tanks_ports = "%s C%s.csv"% (PREPROCESSED_FILLED_TANKS_PORTS_BASENAME, BV_condition)
else:
    fn_prepro_filled_tanks_ports = "%s.csv"% (PREPROCESSED_FILLED_TANKS_PORTS_BASENAME)

f_prepro_filled_tanks_ports = open(fn_prepro_filled_tanks_ports, 'w')

s_header = "PortName;StName;TankName;BlockId;"
s_header += "StWeight;xCG;yCG;zCG\n"
f_prepro_filled_tanks_ports.write(s_header)

for (port_name, tank_name, id_block_st, volume_st, weight_st, x_cg_st, y_cg_st, z_cg_st)    in l_filled_subtanks:
    st_name = "%s_%s" % (tank_name, id_block_st)
    s_row = "%s;%s;%s;%s;%.1f;%.3f;%.3f;%.3f\n" %    (port_name, st_name, tank_name, id_block_st, 
     weight_st, x_cg_st, y_cg_st, z_cg_st)
    f_prepro_filled_tanks_ports.write(s_row)

f_prepro_filled_tanks_ports.close()


# ### LoadLists : reading BAPLIE edi files

os.chdir(REP_LOADLIST_DATA)

l_containers = vsp.get_l_containers(BV_condition, BV_condition_load_port, BV_condition_disch_port,
                                    specific_edi_filename, edi_container_nb_header_lines,
                                    edi_structure)

# #### Back to common track, write file

os.chdir(REP_DATA)

PREPROCESSED_LOADLIST_BASENAME = "9454450 Preprocessed Loadlist"
PREPROCESSED_LOADLIST_BASENAME = "Containers"
if BV_condition is not None:
    fn_prepro_loadlist = "%s C%s.csv" % (PREPROCESSED_LOADLIST_BASENAME, BV_condition)
else:
    fn_prepro_loadlist = "%s.csv" % (PREPROCESSED_LOADLIST_BASENAME)

f_prepro_loadlist = open(fn_prepro_loadlist, 'w')

# output formatted loadlist
s_header = "ContId;LoadPort;DischPort;Type;Setting;Size;Height;Weight;Slot\n"
f_prepro_loadlist.write(s_header)

for (cont_id, load_port, disch_port, c_type, setting, size, height, weight, slot) in l_containers:

        s_line = "%s;%s;%s;%s;%s;%s;%s;%.3f;%s\n" %        (cont_id, load_port, disch_port, c_type, setting, size, height, weight, slot)
        f_prepro_loadlist.write(s_line)
        
f_prepro_loadlist.close()

print("THE END!!")



