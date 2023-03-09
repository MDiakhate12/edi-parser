# -*- coding: utf-8 -*-
"""
Created on Tue Nov 16 13:56:02 2021

@author: 056757706
"""

# libraries
import os
import vessel_stow_kpi as vsk

# directory and file names
##################
# TO BE MODIFIED
# correspondance entre slot position et sous-baies
REP_DATA_VESSEL = "c:/Projets/CCS/Vessel Stowage/Modèles/Data"
fn_subbays = "SubBays Capacities Extrait Prototype MP_IN.csv"
fn_stacks = "Stacks Extrait Prototype MP_IN.csv"
fn_rotation = "Rotation Ports 5 Extrait Prototype MP_IN.csv"

# positionnement dans la rotation
# pour charger une série à partir de CNTXG, il vaut mieux LEG_NO=0, LAST_PORT_SEQ=N
LEG_NO = 0
LAST_PORT_SEQ = 11

# POUR LECTURE FICHIERS ONBOARD, FAIRE SELON SOURCE
# valeurs possibles : EDI (planners), SP (pseudo_slot), CONSO (consolidé)
SOURCE_ONBOARD = "EDI"

REP_DATA_ONBOARD = "c:/Projets/CCS/Vessel Stowage/KPI/Plans réels"
FILE_NAME_ROOT_ONBOARD = "Rotation CC AVH revised"

# lecture des loadlists
REP_DATA_LOADLIST = "c:/Projets/CCS/Vessel Stowage/KPI/Loadlists"
FILE_NAME_ROOT_LOADLIST = "Loadlist"

# écriture
REP_DATA_KPI = "c:/Projets/CCS/Vessel Stowage/KPI/Plans réels"
FILE_NAME_KPI = "9454450 KPI Results.csv"

#calcul prenant en compte ou non la différence entre 20' et 40'
CRANE_SPLIT_TEU = True
# coefficient de gain du dual cycling
DUAL_CYCLING_GAIN = 0.5
# temps estimé pour un déplacement hatch cover (en heures)
HC_MOVE_TIME = 0.5

# containers to exclude
L_CONTAINERS_TO_EXCLUDE = [
]

#L_CONTAINERS_TO_EXCLUDE = [
#'ECMU8152162',
#'ECMU8080761',
#'UESU4804406',
#'ECMU8083800',
#'ECMU8145570',
#'ECMU8114922',
#'DRYU4516301',
#'APHU4625538',
#'ECMU8081198',
#'ECMU8087452',
#'APHU4640039'
#]

# Préliminaire : éléments particuliers du navire et de la rotation

os.chdir(REP_DATA_VESSEL)

d_sb_capacities = vsk.get_subbays_capacities(fn_subbays)
l_bays, l_subbays, l_cols_bay_subbay = vsk.get_lists_bay_subbay(d_sb_capacities)
d_stacks = vsk.get_stacks_capacities(fn_stacks)

d_ports_capacities = vsk.get_ports_capacities(fn_rotation)

##### Structure générale : (listes de)
#dictionnaires qui à chaque port associe un dictionnaire qui à chaque pile associe de bas en haut les caractéristiques des conteneurs (None si pas de conteneur)<p>
#Pour ces caractéristiques, on regroupe les conteneurs en groupes de conteneurs, mais là pas besoin de sophistication sur les groupes de conteneurs lours ou légers, on prend des limites sur le poids des conteneurs et non sur leur poids au sein des macro-groupes ; pour approximer les groupes du master planning, on prend des seuils de 15 t et 20 t pour les 20' et 45' (à comparer à 10 et 15 utilisés pour le calcul de la moyenne des légers)

#### Charger les résultats de slot planning ou onboard

###### Résultats slot planning (issus de CPLEX)

def load_SP_onboard(fn_results, port_seq, leg_no=0, l_containers_to_exclude=[]):

    d_sl_2_c = vsk.initialize_slots(d_stacks)

    f_results = open(fn_results, 'r')
    
    for no_line, line in enumerate(f_results):
        
        if no_line == 0: continue
    
        l_items = line.split(';')
        nb_items = len(l_items)
        if nb_items < 9: continue
    
        container_id = l_items[0].strip()
        #if container_id in l_containers_to_exclude: continue
            
        pol_name = l_items[2].strip()
        # les noms de port GBSOU et GBSOU2 sont déjà bien distingués, ne rien changer ici 
        pol_seq = vsk.port_name_2_seq(pol_name, leg_no)
        pod_name = l_items[3].strip()
        pod_seq = vsk.port_name_2_seq(pod_name, leg_no)
        
        # type
        iso_type = l_items[4].strip()
        c_size, hc = vsk.get_container_size_height(iso_type)
        
        # weight (in tons)
        weight_kg = float(l_items[5].strip())
        weight = weight_kg / 1000
        # hence weight category
        if c_size == '20':
            c_weight = 'L' if weight <= vsk.WEIGHT_THRESHOLD_FOR_20 else 'H'
        else:
            c_weight = 'L' if weight <= vsk.WEIGHT_THRESHOLD_FOR_40 else 'H'
        
        # setting
        setting = l_items[6].strip()
        
        # c_type
        # maintenant, on récupère directement 'R', 'E' ou ''
        # pour l'instant ne pas conserver la distinction plein / vide 
        # on ne conserve que les données des conteneurs groupe
        # A DISCUTER SELON INDICATEURS A VENIR
        c_type = 'GP'
        if setting == 'R': c_type = 'RE'
        
        # slot
        slot = l_items[7].strip()
        if len(slot) == 5: slot = '0'+slot
        # la distinction fait sens pour ce fichier là.
        garbage = False
        if len(slot) == 0: garbage = True
        
        if garbage == False:
            stack, ix_tier = vsk.get_slot_stack_ix_tier(slot, d_stacks)

            # present onboard if...
            if vsk.pol_pod_onboard_or_loadlist(pol_seq, pod_seq, port_seq) == vsk.ONBOARD:
                d_sl_2_c[stack][ix_tier] = (container_id, pol_seq, pod_seq, 
                                            c_size, c_type, weight, c_weight, hc)
        
    f_results.close()
    
    return d_sl_2_c

###### On-board (format consolidé, issu de Merge Onboard Loadlists)
    
def load_consolidated_onboard(fn_onboard, port_seq, leg_no=0, l_containers_to_exclude=[]):

    d_sl_2_c = vsk.initialize_slots(d_stacks)

    f_onboard = open(fn_onboard, 'r')
    
    for no_line, line in enumerate(f_onboard):
        
        if no_line == 0: continue
    
        l_items = line.split(';')
    
        # subbay
        slot = l_items[8].strip() # sur 6 caractères
        if len(slot) == 5: slot = '0'+slot # just in case
        
        # pas de garbage, simplement ignorer les loadlists
        if len(slot) == 0: continue
        garbage = False
        
        # stack et tier
        stack, ix_tier = vsk.get_slot_stack_ix_tier(slot, d_stacks)
        
        # POL and POD
        pol_name = l_items[1].strip()
        # les noms de port GBSOU et GBSOU2 sont déjà bien distingués, ne rien changer ici 
        pol_seq = vsk.port_name_2_seq(pol_name, leg_no)
        pod_name = l_items[2].strip()
        pod_seq = vsk.port_name_2_seq(pod_name, leg_no)
    
        # container id 
        container_id = l_items[0].strip()
        #if container in l_containers_to_exclude: continue
    
        # taille et hauteur déjà) calculées
        c_size = l_items[5].strip()
        hc = l_items[6].strip()
    
        # weight
        weight = float(l_items[7].strip()) # already in tons
        # hence weight category
        if c_size == '20':
            c_weight = 'L' if weight <= vsk.WEIGHT_THRESHOLD_FOR_20 else 'H'
        else:
            c_weight = 'L' if weight <= vsk.WEIGHT_THRESHOLD_FOR_40 else 'H'
    
        # settings, either '', 'E' (empty), or 'R' (effective reefer)
        # we must also determine if reefer or not
        setting = l_items[4]
        if setting == 'R': c_type = 'RE'
        else: c_type = 'GP'
        
        if garbage == False:
            
            # present onboard if...
            if vsk.pol_pod_onboard_or_loadlist(pol_seq, pod_seq, port_seq) == ONBOARD:
                d_sl_2_c[stack][ix_tier] = (container_id, pol_seq, pod_seq, 
                                            c_size, c_type, weight, c_weight, hc)
          
    f_onboard.close()
    
    return d_sl_2_c

###### Lecture d'un format EDI
    
def load_edi_onboard(fn_onboard, port_seq, leg_no=0, l_containers_to_exclude=[]):
    
    d_sl_2_c = vsk.initialize_slots(d_stacks)
    #D_TEST = {}
    
    # un seul enregistrement en fait..., la séparation se fait sur des apostrophes
    f_onboard = open(fn_onboard, 'r')
    l_line = f_onboard.readlines()
    f_onboard.close()

    # "real" lines, depending on the source
    l_lines = l_line[0].split("'")

    # cut the header and the tail
    # depending on the format
    nb_lines_header = 12
    nb_lines_footer = 2
    l_lines = l_lines[nb_lines_header:-nb_lines_footer]
        
    # attributes to retrieve, write only if complete
    cont_id = load_port = disch_port = iso_type = setting = s_size = s_height = s_weight = slot = None
    for no_line, line in enumerate(l_lines):
        
        if line[0:3] == 'LOC' and line[4:7] == '147':
                        
            # écrire le précédent container
            if no_line != 0:
                
                #eventuel incomplétude
                if cont_id is None\
                or load_port is None\
                or disch_port is None\
                or iso_type is None\
                or setting is None\
                or s_size is None\
                or s_height is None\
                or s_weight is None\
                or slot is None:
                    print("conteneur incomplet:", slot, cont_id)
                
                stack, ix_tier = vsk.get_slot_stack_ix_tier(slot, d_stacks)
                
                # for each port in the sequence, see if container on board after operations in port
                # if yes, fill the slot
                
                #if (pol_seq, pod_seq) not in D_TEST:
                #    D_TEST[(pol_seq, pod_seq)] = 0
                #D_TEST[(pol_seq, pod_seq)] += 1
                
                # present at port if...
                # cas particulier (verrue)
                if cont_id == 'CGMU5222580': pol_seq = 12
                if vsk.pol_pod_onboard_or_loadlist(pol_seq, pod_seq, port_seq) == vsk.ONBOARD:
                    
                    # get the type and weight category at last moment
                    if setting == 'R': c_type = 'RE'
                    else: c_type = 'GP'
                    if s_size == '20':
                        c_weight = 'L' if weight <= vsk.WEIGHT_THRESHOLD_FOR_20 else 'H'
                    else:
                        c_weight = 'L' if weight <= vsk.WEIGHT_THRESHOLD_FOR_40 else 'H'
                    # écriture définitive
                    if cont_id not in l_containers_to_exclude:
                        d_sl_2_c[stack][ix_tier] = (cont_id, pol_seq, pod_seq, 
                                                    s_size, c_type, weight, c_weight, s_height)
                    
                cont_id = load_port = disch_port = iso_type = setting = s_size = s_height = s_weight = slot = None
                
            # on peut lire le nouveau slot pour le nouveau conteneur
            # not keeping the first 0
            slot = line[9:15]
            
        if line[0:3] == 'MEA':
            l_items = line.split('+')
            s_weight = l_items[3][4:]
            # in tons...
            weight = float(s_weight) / 1000
            s_weight = "%.3f" % weight
        
        if line[0:3] == 'TMP':
            setting = 'R'
        
        if line[0:3] == 'EQD':
            
            # better to split on +
            
            l_items = line.split('+')
            
            iso_type = l_items[3][0:4]
            s_size, s_height = vsk.get_container_size_height(iso_type)
            
            setting = l_items[6]
            if setting == '4':
                setting = 'E'
            else:
                if setting is None: # not overwriting reefer R if any
                    setting = ''
        
            # also container's last line
            l_items_cont = l_items[2].split(':')
            cont_id = l_items_cont[0]
                
        if line[0:3] == 'LOC' and line[4:5] == '9':
            load_port = line[6:11]
            # for explanations, look at the csv function
            if load_port == "GBSOU" and leg_no not in [8, 9, 10]: load_port += "2"
            # dans cet ordre pour éviter de gérer 2 variables load_port
            if load_port == "GBSO1": load_port = "GBSOU"
            if load_port == "GBSO2": load_port = "GBSOU2"
            pol_seq = vsk.port_name_2_seq(load_port, leg_no)
            
        if line[0:3] == 'LOC' and line[4:6] == '11':
            disch_port = line[7:12]
            # VERRUE POUR TENIR COMPTE DU VOYAGE ACTUEL (VOIR SI A PERENNISER)
            if disch_port == "GBSOU" and leg_no in [8, 9, 10]: disch_port += "2"
            # dans cet ordre pour éviter de gérer 2 variables disch_port
            if disch_port == "GBSO1": disch_port = "GBSOU"
            if disch_port == "GBSO2": disch_port = "GBSOU2"
            pod_seq = vsk.port_name_2_seq(disch_port, leg_no)
        
        # useless lines    
        if line[0:3] == 'RFF':
            continue
        if line[0:3] == 'GDS':
            continue
        if line[0:3] == 'NAD':
            continue
        if line[0:3] == 'CNT':
            continue
        if line[0:3] == 'FTX':
            continue
        if line[0:3] == 'DGS':
            continue
        if line[0:3] == 'ATT':
            continue
        if line[0:3] == 'HAN': # some happen
            continue
        
    # Il reste à intégrer le dernier conteneur
    if cont_id is None\
    or load_port is None\
    or disch_port is None\
    or iso_type is None\
    or setting is None\
    or s_size is None\
    or s_height is None\
    or s_weight is None\
    or slot is None:
        print("conteneur incomplet:", slot, cont_id) 
        
    stack, ix_tier = vsk.get_slot_stack_ix_tier(slot, d_stacks)
                
    # present at port if...
    if vsk.pol_pod_onboard_or_loadlist(pol_seq, pod_seq, port_seq) == vsk.ONBOARD:
            
        # get the type and weight category at last moment
        if setting == 'R': c_type = 'RE'
        else: c_type = 'GP'
        if s_size == '20':
            c_weight = 'L' if weight <= vsk.WEIGHT_THRESHOLD_FOR_20 else 'H'
        else:
            c_weight = 'L' if weight <= vsk.WEIGHT_THRESHOLD_FOR_40 else 'H'
        if cont_id not in l_containers_to_exclude:
            d_sl_2_c[stack][ix_tier] = (cont_id, pol_seq, pod_seq, 
                                        s_size, c_type, weight, c_weight, s_height)
        
        #if (pol_seq, pod_seq) not in D_TEST:
        #    D_TEST[(pol_seq, pod_seq)] = 0
        #D_TEST[(pol_seq, pod_seq)] += 1
    
    #print(D_TEST)
    #t_q = 0
    #for (pol_seq, pod_seq), q in D_TEST.items():
    #    t_q += q
    #print(t_q)
    
    return d_sl_2_c

### Chargement onboard
    
# chargement des onboards
# valeurs possibles de la source : EDI (planners), SP (pseudo_slot), CONSO (consolidé)
f_load_onboard = None
if SOURCE_ONBOARD == "EDI": f_load_onboard = load_edi_onboard
if SOURCE_ONBOARD == "SP": f_load_onboard = load_SP_onboard
if SOURCE_ONBOARD == "CONSO": f_load_onboard = load_consolidated_onboard

os.chdir(REP_DATA_ONBOARD)

l_d_sl_2_c = []
for port_seq in range(LAST_PORT_SEQ+1):
    fn_onboard = "%s %02d.edi" % (FILE_NAME_ROOT_ONBOARD, port_seq)
    d_sl_2_c = f_load_onboard(fn_onboard, port_seq, leg_no=LEG_NO, l_containers_to_exclude=L_CONTAINERS_TO_EXCLUDE)
    l_d_sl_2_c.append(d_sl_2_c)
    
### 1ère analyse, équivalente à celle des KPI du master planning
    
# ensuite créer une liste de dictionnaires de contenus de stacks cg
l_d_st_2_cg = []
for d_sl_2_c in l_d_sl_2_c:
    d_st_2_cg = {}
    for stack, l_tiers in d_sl_2_c.items():
        if stack not in d_st_2_cg:
            d_st_2_cg[stack] = {}
        for ix_tier in range(len(l_tiers)):
            if l_tiers[ix_tier] is not None:
                container = l_tiers[ix_tier]
                cont_id = container[0]
                pol_seq = container[1]
                pod_seq = container[2]
                s_size = container[3]
                c_type = container[4]
                weight = container[5]
                c_weight = container[6]
                s_height = container[7]
                
                cg = (pol_seq, pod_seq, s_size, c_type, c_weight, s_height)
                if cg not in d_st_2_cg[stack]:
                    d_st_2_cg[stack][cg] = (1, weight)
                else:
                    new_quantity = d_st_2_cg[stack][cg][0] + 1
                    new_weight = d_st_2_cg[stack][cg][1] + weight
                    d_st_2_cg[stack][cg] = (new_quantity, new_weight)
                    
    l_d_st_2_cg.append(d_st_2_cg)
    
# à partir de ce dictionnaire au niveau des stacks, créer le même au niveau des sous-baies
l_d_sb_2_cg = []
for d_st_2_cg in l_d_st_2_cg:
    d_sb_2_cg = {}
    for stack, d_cg in d_st_2_cg.items():
        subbay = d_stacks[stack]['subbay']
        if subbay not in d_sb_2_cg:
            d_sb_2_cg[subbay] = {}
        for cg, (quantity, weight) in d_cg.items():
            if cg not in d_sb_2_cg[subbay]:
                d_sb_2_cg[subbay][cg] = (quantity, weight)
            else:
                new_quantity = d_sb_2_cg[subbay][cg][0] + quantity
                new_weight = d_sb_2_cg[subbay][cg][1] + weight
                d_sb_2_cg[subbay][cg] = (new_quantity, new_weight) 
        
    l_d_sb_2_cg.append(d_sb_2_cg)
    
#### Compléter par les loadlists théoriques, pour avoir l'ensemble des données nécessaires au calcul des KPI
    
def load_consolidated_loadlist(fn_loadlist, leg_no):
    
    # only need a dictionary of cg (which will be considered later inside the bay '')
    d_cg = {}

    f_loadlist = open(fn_loadlist, 'r')
    
    for no_line, line in enumerate(f_loadlist):
        
        if no_line == 0: continue
    
        l_items = line.split(',')
    
        # POL and POD
        pol_name = l_items[0].strip()
        # les noms de port GBSOU et GBSOU2 sont déjà bien distingués, ne rien changer ici 
        pol_seq = vsk.port_name_2_seq(pol_name, leg_no)
        pod_name = l_items[1].strip()
        pod_seq = vsk.port_name_2_seq(pod_name, leg_no)
    
        # container id 
        container_id = l_items[2].strip()
        #if container in l_containers_to_exclude: continue
    
        # à partir du type, obtenir taille et hauteur calculées
        iso_type = l_items[3]
        c_size, hc = vsk.get_container_size_height(iso_type)
        
        # weight
        weight = float(l_items[4].strip()) # already in tons
        # hence weight category
        if c_size == '20':
            c_weight = 'L' if weight <= vsk.WEIGHT_THRESHOLD_FOR_20 else 'H'
        else:
            c_weight = 'L' if weight <= vsk.WEIGHT_THRESHOLD_FOR_40 else 'H'
    
        # settings, either '', 'E' (empty), or 'R' (effective reefer)
        # we must also determine if reefer or not
        setting = l_items[5]
        if setting == 'R': c_type = 'RE'
        else: c_type = 'GP'
        
        cg = (pol_seq, pod_seq, c_size, c_type, c_weight, hc)
        
        if cg not in d_cg: d_cg[cg] = (0, 0.0)
        total_quantity = d_cg[cg][0] + 1
        total_weight = d_cg[cg][1] + weight
        d_cg[cg] = (total_quantity, total_weight)
        
    f_loadlist.close()
    
    
    return d_cg

###### Charger loadlists N de 1 à LAST_PORT_SEQ+1 et compléter avec subbay '' dans  N-1 (arrival data), à ce niveau port_seq, et celui du départ (N+1)

# chargement des loadlists
os.chdir(REP_DATA_LOADLIST)
LEG_NO = 0
LAST_PORT_SEQ = 11

l_d_ll_cg = []
for port_seq in range(1, LAST_PORT_SEQ+1):
    fn_loadlist = "%s %02d.csv" % (FILE_NAME_ROOT_LOADLIST, port_seq)
    d_ll_cg = load_consolidated_loadlist(fn_loadlist, leg_no=LEG_NO)
    l_d_ll_cg.append(d_ll_cg)

# including both
for ix_seq in range(0, LAST_PORT_SEQ):
    d_sb_2_cg = l_d_sb_2_cg[ix_seq] # onboard out of ix
    d_ll_cg = l_d_ll_cg[ix_seq] # loadlist to load at ix+1
    d_sb_2_cg[''] = d_ll_cg
    l_d_sb_2_cg[ix_seq] = d_sb_2_cg
    
### Calculs des KPI master 
    
###### On part de l_d_sb_2_cg, avec n+1 ports, de 0 à n, donnant lieu à n couples arrivées / départs, de 1 à n

l_d_sb_kpi_elems = []
for port_seq in range(1,len(l_d_sb_2_cg)):
    d_sb_kpi_elems = vsk.get_kpi_elems(port_seq, l_d_sb_2_cg, l_subbays)
    l_d_sb_kpi_elems.append(d_sb_kpi_elems)
    
for d_sb_kpi_elems in l_d_sb_kpi_elems:
    vsk.get_kpi_derived_elems(d_sb_kpi_elems)
    
d_crane_bays = vsk.distribute_bays_for_cranes(l_bays)

l_d_bay_kpi_cranes = []
for ix, d_sb_kpi_elems in enumerate(l_d_sb_kpi_elems):
    port_seq = ix + 1
    crane_single_speed = d_ports_capacities[port_seq][1]
    d_bay_kpi_cranes = vsk.get_kpi_cranes(d_sb_kpi_elems, l_bays, d_crane_bays, 
                                          crane_single_speed, DUAL_CYCLING_GAIN, HC_MOVE_TIME)
    l_d_bay_kpi_cranes.append(d_bay_kpi_cranes)
    
l_d_bay_kpi_teu = []
for d_sb_kpi_elems in l_d_sb_kpi_elems:
    d_bay_kpi_teu = vsk.get_kpi_teu(d_sb_kpi_elems, l_bays, d_sb_capacities)
    l_d_bay_kpi_teu.append(d_bay_kpi_teu)
    
d_left_right_subbays = vsk.get_left_right_subbays(d_sb_capacities)

l_d_bay_kpi_weights_distrib = []
for d_sb_kpi_elems in l_d_sb_kpi_elems:
    d_bay_kpi_weights_distrib = vsk.get_kpi_weights_distrib(d_sb_kpi_elems, l_bays, d_left_right_subbays)
    l_d_bay_kpi_weights_distrib.append(d_bay_kpi_weights_distrib)
    
##### Sommes et listes
    
l_d_kpi_sums = vsk.port_kpi_dicos_2_lists(l_d_sb_kpi_elems, 
                                          l_d_bay_kpi_cranes, l_d_bay_kpi_teu, l_d_bay_kpi_weights_distrib,
                                          d_ports_capacities, DUAL_CYCLING_GAIN, HC_MOVE_TIME)

l_l_sb_kpi_elems, l_l_bay_kpi_cranes, l_l_bay_kpi_teu, l_l_bay_kpi_weights_distrib =\
vsk.sb_kpi_dicos_2_lists(l_d_sb_kpi_elems, 
                         l_d_bay_kpi_cranes, l_d_bay_kpi_teu, l_d_bay_kpi_weights_distrib)

##### Arrangements et écriture

l_d_kpis = vsk.arrange_final_kpis(l_d_kpi_sums, l_l_sb_kpi_elems, 
                                  l_l_bay_kpi_cranes, l_l_bay_kpi_teu, l_l_bay_kpi_weights_distrib)

os.chdir(REP_DATA_KPI)

vsk.write_kpi_file(FILE_NAME_KPI, CRANE_SPLIT_TEU, l_d_kpis, 
                   d_sb_capacities, l_bays, l_subbays, l_cols_bay_subbay,
                   d_ports_capacities,
                   leg_no=0)




    

    























