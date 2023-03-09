# -*- coding: utf-8 -*-
"""
Created on Fri Nov  5 16:52:11 2021

@author: 056757706
"""


#############################################
# BASIC DATA
#############################################
# dictionaries from Port Name to sequence and reverse
d_port_name_2_seq = {'MYPKG': 0, 'CNTXG': 1, 'KRPUS': 2, 'CNNGB': 3, 'CNSHA': 4, 'CNYTN': 5, 'SGSIN': 6,
                     'FRDKK': 7, 'GBSOU': 8, 'DEHAM': 9, 'NLRTM': 10, 'GBSOU2': 11, 'ESALG': 12}
d_seq_2_port_name = {0: 'MYPKG', 1: 'CNTXG', 2: 'KRPUS', 3: 'CNNGB', 4: 'CNSHA', 5: 'CNYTN', 6: 'SGSIN',
                     7: 'FRDKK', 8: 'GBSOU', 9: 'DEHAM', 10: 'NLRTM', 11: 'GBSOU2', 12: 'ESALG'}
NB_PORTS = 13

###### Obtention de la taille et du type du conteneur à partir du type ISO
# DEFINITION EN DOUBLE DE vessel_stow_preproc.py
######

def get_container_size_height(c_type):
    
    old_code = False
    if c_type[2] >= '0' and c_type[2] <= '9': old_code = True
    
    size = '?'
    s_size = c_type[0]
    if s_size == '2': size = '20'
    if s_size == '4': size = '40'
    if old_code == False and s_size == 'L': size = '45'
    if old_code == True and s_size == '9': size = '45'
    if size == '?': print("size for %s ?" % c_type)
    
    height = '?'
    s_height = c_type[1]
    if old_code == False:
        if s_height in ['0', '2']: height = ''
        if s_height in ['5', 'E']: height = 'HC'
        if s_height == '9': height = ''
    else:
        height = ''
        if s_height in ['4', '5']: height = 'HC'
    if height == '?': print("height for %s ?" % c_type)
    
    # no hc for 20 containers, because too few of them
    if size == '20' and height == 'HC':
        height = ''
    
    return size, height


# récupérer les sous-baies et leurs capacités totales
def get_subbays_capacities(fn_subbays):

    d_sb_capacities = {}

    f_subbays = open(fn_subbays, 'r')
    
    for no_line, line in enumerate(f_subbays):
        
        if no_line == 0: continue
    
        l_items = line.split(';')
        subbay = l_items[0] 
        #if subbay[0] == '0': subbay = subbay[1:]
        cap_20_or_40 = int(l_items[4])
        cap_only_20 = int(l_items[5])
        cap_only_40 = int(l_items[6])
        capacity = 2 * cap_20_or_40 + cap_only_20 + 2 * cap_only_40
        
        nb_reefers = int(l_items[9])
        stack_size = int(l_items[10])
        nb_stacks = int(l_items[12])
    
        d_sb_capacities[subbay] = (capacity, stack_size, nb_stacks, nb_reefers)
    
    f_subbays.close()
    
    return d_sb_capacities

# listes utiles
# liste des baies et des sous baies
# gérer les colonnes mêlées baies et sous-baies (la baie, puis les sous-baies)
# à chaque index de colonne relative associer (no_baie, no_sous-baie), -1 pour la partie exclusive non remplie
def get_lists_bay_subbay(d_sb_capacities):
    
    l_cols_bay_subbay = []

    l_bays = []
    l_subbays = []

    # add empty subbay for garbage ot loadlist
    l_subbays.append('')
    l_cols_bay_subbay.append((-1,0))

    for subbay in d_sb_capacities.keys():
        l_subbays.append(subbay)
    l_subbays.sort()

    ix_bay = 0
    ix_subbay = 1 # '' already added
    prev_bay = ''
    # starting at 1 for garbage
    for subbay in l_subbays[1:]:
        bay = subbay[0:-2]
        if bay != prev_bay:
            l_bays.append(bay)
            l_cols_bay_subbay.append((ix_bay, -1))
            ix_bay += 1
            prev_bay = bay
        l_cols_bay_subbay.append((-1, ix_subbay))
        ix_subbay += 1
        
    return l_bays, l_subbays, l_cols_bay_subbay


# get the list of all subbays to consider when looking below (overstower)
# in fact, it is just the subbay below, and only if on the deck
def get_subbays_4_below(subbay):
    
    l_subbays_4_below = []
    
    macro_tier_ref = subbay[-1]
    
    if macro_tier_ref == "1":
        subbay_below = subbay[0:-1]+"0"
        l_subbays_4_below.append(subbay_below)
    
    return l_subbays_4_below

# get the list of all subbays to consider when looking above (potential overstow)
# in fact, it is just the subbay above, and only if in the hold
def get_subbays_4_above(subbay):
    
    l_subbays_4_above = []
    
    macro_tier_ref = subbay[-1]
    
    if macro_tier_ref == "0":
        subbay_above = subbay[0:-1]+"1"
        l_subbays_4_above.append(subbay_above)
    
    return l_subbays_4_above

# get the list of left and right subbays for each subbay
def get_left_right_subbays(d_sb_capacities):
    
    d_left_right_subbays = {}
    for subbay in d_sb_capacities:
        bay = subbay[0:2]
        no_row = int(subbay[2])
        if bay not in d_left_right_subbays:
            d_left_right_subbays[bay] = {'left': [], 'right': []}
        # excluding the central subbay
        if no_row % 2 == 0 and no_row != 0:
            d_left_right_subbays[bay]['left'].append(subbay)
        if no_row % 2 == 1:
            d_left_right_subbays[bay]['right'].append(subbay)
            
    return d_left_right_subbays


# obtention des stacks et de leurs capacités
def get_stacks_capacities(fn_stacks):
    
    d_stacks = {}

    f_stacks = open(fn_stacks, 'r')
    
    for no_line, line in enumerate(f_stacks):
        
        if no_line == 0: continue
    
        l_items = line.split(';')
        bay = l_items[0] # sur 2 caractères
        row = l_items[1] # sur 2 caractères
        macro_tier = l_items[2]
        subbay = l_items[3] 
        #if subbay[0] == '0': subbay = subbay[1:]
        first_tier = l_items[4]
        max_nb_std_cont = int(l_items[5])
        odd_slot = int(l_items[6])
        max_nb_45 = int(l_items[7])
        min_40_sub_45 = int(l_items[8])
        nb_reefer = int(l_items[9])
        max_weight = float(l_items[10])
        stack_height = float(l_items[11])
        max_nb_HC_at_max_stack = int(l_items[12])
    
        stack = (bay, row, macro_tier)
    
        d_stacks[stack] = {'subbay': subbay, 'first_tier': first_tier, 
                           'max_nb_std_cont': max_nb_std_cont, 'odd_slot': odd_slot, 'nb_reefer': nb_reefer,
                           'max_nb_45': max_nb_45, 'min_40_sub_45': min_40_sub_45,
                           'max_nb_HC_at_max_stack': max_nb_HC_at_max_stack,
                           'stack_height': stack_height, 'max_weight': max_weight}
    
    f_stacks.close()
    
    return d_stacks

# get the index (starting at 0) of a tier in a stack
def get_ix_tier(tier, stack, d_stacks):
    tier_no = int(tier)
    first_tier_no = int(d_stacks[stack]['first_tier'])
    return int((tier_no - first_tier_no) / 2)

# conversely
def get_tier(ix_tier, stack, d_stacks):
    first_tier_no = int(d_stacks[stack]['first_tier'])
    tier_no = first_tier_no + 2 * ix_tier
    return "%02d" % tier_no

# from a slot position, get its stack and its tier index
def get_slot_stack_ix_tier(slot_address, d_stacks):
    
    if len(slot_address) == 5: slot_address = '0' + slot_address
    bay = slot_address[0:2]
    row = slot_address[2:4]
    tier = slot_address[4:6]
    if int(tier) < 50: macro_tier = '0'
    if int(tier) > 50: macro_tier = '1'
    stack = (bay, row, macro_tier)
    ix_tier = get_ix_tier(tier, stack, d_stacks)
    
    return stack, ix_tier

# get all subbays for bay x macro_tier combination
def get_bays_macro_tiers_l_subbays(d_stacks):
    
    d_bay_macro_tier_l_subbays = {}
    
    for (bay, row, macro_tier), d_stack_items in d_stacks.items():
        
        bay_macro_tier = (bay, macro_tier)
        subbay = d_stack_items['subbay']
        
        if bay_macro_tier not in d_bay_macro_tier_l_subbays:
            d_bay_macro_tier_l_subbays[bay_macro_tier] = set()
        d_bay_macro_tier_l_subbays[bay_macro_tier].add(subbay)
    
    return d_bay_macro_tier_l_subbays



# arbitrary ABSOLUTE weight thresholds for pseudo container groups
WEIGHT_THRESHOLD_FOR_20 = 15
WEIGHT_THRESHOLD_FOR_40 = 20

# initialiser tous les slots à None
def initialize_slots(d_stacks):
    
    d_stacks_slots = {}
    for stack, d_stack in d_stacks.items():
        nb_slots = d_stack['max_nb_std_cont']
        d_stacks_slots[stack] = [None] * nb_slots
        
    return d_stacks_slots








############################################################
#### PORTS

# relationships between port names and sequence number, depending on the current point of reference (leg_no)
def port_seq_2_name(seq_no, leg_no):
    return d_seq_2_port_name[(seq_no + leg_no) % NB_PORTS]
def port_name_2_seq(port_name, leg_no):
    return (d_port_name_2_seq[port_name] - leg_no) % NB_PORTS

# get the situation of a cargo as on-board or in a loadlist
ONBOARD = 1
LOADLIST = 2
def pol_pod_onboard_or_loadlist(pol_seq, pod_seq, port_seq):
    if pod_seq < pol_seq:
        pod_seq += 13
    config = LOADLIST
    if (port_seq >= pol_seq and port_seq < pod_seq)\
    or (port_seq + 13 >= pol_seq and port_seq + 13 < pod_seq):
        config = ONBOARD
    return config

# chargement des ports (en fait obtention du nombre de grues, et de la vitesse single des grues)
def get_ports_capacities(fn_rotation, leg_no=0):

    d_ports_capacities = {}

    f_rotation = open(fn_rotation, 'r')
    
    for no_line, line in enumerate(f_rotation):
        
        if no_line == 0: continue
    
        l_items = line.split(';')
    
        #port_seq = int(l_items[0]) # absolute port_seq
        port_name = l_items[1].strip()
        port_seq = port_name_2_seq(port_name, leg_no)
        nb_cranes = int(l_items[2])
        speed_single = float(l_items[7])
    
        d_ports_capacities[port_seq] = (nb_cranes, speed_single)
    
    f_rotation.close()
    
    return d_ports_capacities


############# KPI AS SUCH #########################################


##### KPI élémentaires au niveau des sous-baies
def get_kpi_elems(port_seq, l_d_sb_2_cg, l_subbays):
    
    d_sb_kpi_elems = {}
    # initialisation à 0 de toutes les quantités de base 
    for sb in l_subbays:
        d_sb_kpi_elems[sb] = {}
        d_sb_kpi_elems[sb]['quantity_at_arrival'] = 0
        d_sb_kpi_elems[sb]['quantity_discharged'] = 0
        d_sb_kpi_elems[sb]['weight_at_arrival'] = 0.0
        d_sb_kpi_elems[sb]['weight_discharged'] = 0.0
        d_sb_kpi_elems[sb]['teu_at_arrival'] = 0
        d_sb_kpi_elems[sb]['teu_discharged'] = 0
        d_sb_kpi_elems[sb]['quantity_at_depart'] = 0
        d_sb_kpi_elems[sb]['quantity_loaded'] = 0
        d_sb_kpi_elems[sb]['quantity_to_load'] = 0
        d_sb_kpi_elems[sb]['weight_at_depart'] = 0.0
        d_sb_kpi_elems[sb]['weight_loaded'] = 0.0 
        d_sb_kpi_elems[sb]['weight_to_load'] = 0.0 
        d_sb_kpi_elems[sb]['teu_at_depart'] = 0
        d_sb_kpi_elems[sb]['teu_loaded'] = 0
        d_sb_kpi_elems[sb]['teu_to_load'] = 0
        d_sb_kpi_elems[sb]['quantity_20_at_depart'] = 0
        d_sb_kpi_elems[sb]['reefers_at_depart'] = 0
    
    if not (port_seq >= 1 and port_seq < len(l_d_sb_2_cg)):
        print("results unavailable for port %d" % port_seq)
    d_sb_2_cg_arrival = l_d_sb_2_cg[port_seq-1]
    d_sb_2_cg_depart = l_d_sb_2_cg[port_seq]
    
    # in arrival data
    for sb, d_cg in d_sb_2_cg_arrival.items():
        for cg, (quantity, weight) in d_cg.items():
            teu_factor = 1 if cg[2] == '20' else 2
            
            # ne pas inclure sous-baie vide , sauf si éventuel garbage
            if sb != '' or (sb == '' and cg[0] < port_seq):
                d_sb_kpi_elems[sb]['quantity_at_arrival'] += quantity
                d_sb_kpi_elems[sb]['weight_at_arrival'] += weight
                d_sb_kpi_elems[sb]['teu_at_arrival'] += teu_factor * quantity
            # pod = current port
            if cg[1] == port_seq and sb != '':
                d_sb_kpi_elems[sb]['quantity_discharged'] += quantity
                d_sb_kpi_elems[sb]['weight_discharged'] += weight
                d_sb_kpi_elems[sb]['teu_discharged'] += teu_factor * quantity
            # pol for the next port
            if cg[0] == port_seq and sb == '':
                d_sb_kpi_elems[sb]['quantity_to_load'] += quantity
                d_sb_kpi_elems[sb]['weight_to_load'] += weight
                d_sb_kpi_elems[sb]['teu_to_load'] += teu_factor * quantity
            
    # in departure data
    for sb, d_cg in d_sb_2_cg_depart.items():
        for cg, (quantity, weight) in d_cg.items():
            teu_factor = 1 if cg[2] == '20' else 2
            
            # ne pas inclure sous-baie vide , sauf si éventuel garbage
            if sb != '' or (sb == '' and cg[0] < port_seq):
                d_sb_kpi_elems[sb]['quantity_at_depart'] += quantity
                d_sb_kpi_elems[sb]['weight_at_depart'] += weight
                d_sb_kpi_elems[sb]['teu_at_depart'] += teu_factor * quantity
                if teu_factor == 1:
                    d_sb_kpi_elems[sb]['quantity_20_at_depart'] += quantity
                if cg[3] == 'RE':
                    d_sb_kpi_elems[sb]['reefers_at_depart'] += quantity
            # pol = current port
            if cg[0] == port_seq and sb != '':
                d_sb_kpi_elems[sb]['quantity_loaded'] += quantity
                d_sb_kpi_elems[sb]['weight_loaded'] += weight
                d_sb_kpi_elems[sb]['teu_loaded'] += teu_factor * quantity
    
    return d_sb_kpi_elems

##### KPI dérivés des élémentaires au niveau des sous-baies (move et restow)
    
# obtenir éléments dérivés au niveau des sous-baies
def get_kpi_derived_elems(d_sb_kpi_elems):

    # on ne peut pas chercher dans un dictionnaire au sein d'une boucle sur ce dictionnaire
    l_sb_kpi_elems = [(sb, d_kpi_elems) for sb, d_kpi_elems in d_sb_kpi_elems.items()]
    
    for (sb, d_kpi_elems) in l_sb_kpi_elems:
    
        # rob
        d_sb_kpi_elems[sb]['quantity_rob'] = d_sb_kpi_elems[sb]['quantity_at_arrival']\
                                           - d_sb_kpi_elems[sb]['quantity_discharged']
        d_sb_kpi_elems[sb]['teu_rob'] = d_sb_kpi_elems[sb]['teu_at_arrival']\
                                      - d_sb_kpi_elems[sb]['teu_discharged']
    
        # initial
        # if needed, skip it first
    
        # restow
        d_sb_kpi_elems[sb]['hc_move'] = 0
        d_sb_kpi_elems[sb]['hc_restow'] = 0
        d_sb_kpi_elems[sb]['hc_obligatory_restow'] = 0
        d_sb_kpi_elems[sb]['hc_teu_restow'] = 0
        d_sb_kpi_elems[sb]['hc_teu_obligatory_restow'] = 0
        if sb != '' and sb[-1] == '1':
            sb_hold = sb[0:-1] + '0'
            if sb_hold in d_sb_kpi_elems:
                if d_sb_kpi_elems[sb_hold]['quantity_discharged'] + d_sb_kpi_elems[sb_hold]['quantity_loaded'] > 0:
                    d_sb_kpi_elems[sb]['hc_move'] = 1
                    d_sb_kpi_elems[sb]['hc_restow'] = d_sb_kpi_elems[sb]['quantity_rob']
                    d_sb_kpi_elems[sb]['hc_teu_restow'] = d_sb_kpi_elems[sb]['teu_rob']
                if d_sb_kpi_elems[sb_hold]['quantity_discharged'] > 0:
                    d_sb_kpi_elems[sb]['hc_obligatory_restow'] = d_sb_kpi_elems[sb]['quantity_rob']
                    d_sb_kpi_elems[sb]['hc_teu_obligatory_restow'] = d_sb_kpi_elems[sb]['teu_rob']
                    
        # extra moves
        # se traduit par des moves supplémentaires par rapport à ce qui aurait pu s'observer en considérant
        # seulement l'équation de flux départ = arrivée - déchargement + chargement
        # ON NE REGARDE QUE LES QUANTITES
        if sb != '':
            d_sb_kpi_elems[sb]['extra_move'] = d_sb_kpi_elems[sb]['quantity_rob']\
                                             + d_sb_kpi_elems[sb]['quantity_loaded']\
                                             - d_sb_kpi_elems[sb]['quantity_at_depart']
            d_sb_kpi_elems[sb]['teu_extra_move'] = d_sb_kpi_elems[sb]['teu_rob']\
                                                 + d_sb_kpi_elems[sb]['teu_loaded']\
                                                 - d_sb_kpi_elems[sb]['teu_at_depart']
        else:
            d_sb_kpi_elems[sb]['extra_move'] = 0
            d_sb_kpi_elems[sb]['teu_extra_move'] = 0
            
        # effective load,what is viewed as a load, minus the quantity loaded
        d_sb_kpi_elems[sb]['quantity_effective_load'] = d_sb_kpi_elems[sb]['quantity_loaded']\
                                                      - d_sb_kpi_elems[sb]['extra_move']
        d_sb_kpi_elems[sb]['teu_effective_load'] = d_sb_kpi_elems[sb]['teu_loaded']\
                                                 - d_sb_kpi_elems[sb]['teu_extra_move']
                                                 
        # hence garbage
        d_sb_kpi_elems[sb]['garbage'] = d_sb_kpi_elems[sb]['quantity_to_load']\
                                      - d_sb_kpi_elems[sb]['quantity_effective_load']
        d_sb_kpi_elems[sb]['teu_garbage'] = d_sb_kpi_elems[sb]['teu_to_load']\
                                          - d_sb_kpi_elems[sb]['teu_effective_load']
            
##### KPI au niveau des baies : dual cycling et crane split
            
# répartition des baies pour le calcul du crane split
# on pourrait imaginer que cela dépende du nombre de grues, en attendant par couple de baies (sauf 34-38 et 74-78)

def distribute_bays_for_cranes(l_bays):
    d_crane_bays = {bay: ["%02d" % (int(bay)-4), bay] for bay in l_bays if bay not in ['02', '38', '78']}
    d_crane_bays['02'] = ['02']
    d_crane_bays['38'] = ['38']
    d_crane_bays['78'] = ['78']
    return d_crane_bays
            
# pour le dual cycling et crane split, les données sont au niveau de la baie (02, 06, ..., 94)
# double calcul, tenant compte ou non de 20' comptant pour moitié (twinlift possible)
def get_kpi_cranes(d_sb_kpi_elems, l_bays, d_crane_bays, 
                   crane_single_speed, dual_cycling_gain, hc_move_time):
    
    d_bay_kpi_cranes = {}
    
    for bay in l_bays:
        d_bay_kpi_cranes[bay] = {}
        d_bay_kpi_cranes[bay]['quantity_discharged'] = 0 
        d_bay_kpi_cranes[bay]['quantity_effective_load'] = 0
        d_bay_kpi_cranes[bay]['hc_restow'] = 0
        d_bay_kpi_cranes[bay]['teu_discharged'] = 0 
        d_bay_kpi_cranes[bay]['teu_effective_load'] = 0
        d_bay_kpi_cranes[bay]['hc_teu_restow'] = 0
        d_bay_kpi_cranes[bay]['hc_move'] = 0
    
    # calcul des sommes nécessaires
    for sb, d_kpi_elems in d_sb_kpi_elems.items():
        if sb == '': continue
        bay = sb[0:-2]
        d_bay_kpi_cranes[bay]['quantity_discharged'] += d_kpi_elems['quantity_discharged']
        d_bay_kpi_cranes[bay]['quantity_effective_load'] += d_kpi_elems['quantity_effective_load']
        d_bay_kpi_cranes[bay]['hc_restow'] += d_kpi_elems['hc_restow']
        d_bay_kpi_cranes[bay]['teu_discharged'] += d_kpi_elems['teu_discharged']
        d_bay_kpi_cranes[bay]['teu_effective_load'] += d_kpi_elems['teu_effective_load']
        d_bay_kpi_cranes[bay]['hc_teu_restow'] += d_kpi_elems['hc_teu_restow']
        d_bay_kpi_cranes[bay]['hc_move'] += d_kpi_elems['hc_move']
        
    # calcul du dual cycling
    # /2 parce qu'on gagne 1/2 move par move 
    #(pour 2 mouvements nécessitant 4 mvts de grues sans dual, l'équivalent de 3 avec dual)
    for bay, d_kpi_cranes in d_bay_kpi_cranes.items():
        d_bay_kpi_cranes[bay]['dual_cycling'] = min(d_bay_kpi_cranes[bay]['quantity_discharged'],
                                                    d_bay_kpi_cranes[bay]['quantity_effective_load'])\
                                              * dual_cycling_gain
        # for teu, a 40' counts for 1, 20' for 0.5 => / 2
        d_bay_kpi_cranes[bay]['teu_dual_cycling'] = min(0.5 * d_bay_kpi_cranes[bay]['teu_discharged'],
                                                        0.5 * d_bay_kpi_cranes[bay]['teu_effective_load'])\
                                                  * dual_cycling_gain
        
    # calcul du crane split
    # à chaque baie est associée un ensemble de baies sur lequel se font les sommes
    # passer par la liste
    for bay in l_bays:
        d_bay_kpi_cranes[bay]['crane_split'] = 0
        d_bay_kpi_cranes[bay]['teu_crane_split'] = 0
        if bay in d_crane_bays:
            l_crane_bays = d_crane_bays[bay]
            for crane_bay in l_crane_bays:
                d_bay_kpi_cranes[bay]['crane_split'] += d_bay_kpi_cranes[crane_bay]['quantity_discharged']\
                                                     + d_bay_kpi_cranes[crane_bay]['quantity_effective_load']\
                                                     - d_bay_kpi_cranes[crane_bay]['dual_cycling']\
                                                     + (2 * d_bay_kpi_cranes[crane_bay]['hc_restow'])\
                                                     + (hc_move_time * crane_single_speed\
                                                        * d_bay_kpi_cranes[crane_bay]['hc_move'])
                # note: 0.5 * crane_single_speed : HC move = 0.5 hours, crane_single_speed = nb moves in 0.5 hours
                # for teu, a 40' counts for 1, 20' for 0.5 => / 2
                # already done in dual cycling, so no 0.5 there
                d_bay_kpi_cranes[bay]['teu_crane_split'] += (0.5 * d_bay_kpi_cranes[crane_bay]['teu_discharged'])\
                                                         + (0.5 * d_bay_kpi_cranes[crane_bay]['teu_effective_load'])\
                                                         - d_bay_kpi_cranes[crane_bay]['teu_dual_cycling']\
                                                         + (0.5 * 2 * d_bay_kpi_cranes[crane_bay]['hc_teu_restow'])\
                                                         + (hc_move_time * crane_single_speed\
                                                            * d_bay_kpi_cranes[crane_bay]['hc_move'])
    
    return d_bay_kpi_cranes


##### KPI des taux de remplissage, au niveau de chaque baie
    
# calcul des taux de remplissage, au niveau de chaque baie, en distinguant cale et pontée
# on intègre aussi à ce niveau les ratios 20' / nb total conteneurs
def get_kpi_teu(d_sb_kpi_elems, l_bays, d_sb_capacities):
    
    d_bay_kpi_teu = {}
    
    for bay in l_bays:
        d_bay_kpi_teu[bay] = {}
        d_bay_kpi_teu[bay]['teu_hold_capacity'] = 0.0 
        d_bay_kpi_teu[bay]['teu_hold'] = 0.0
        d_bay_kpi_teu[bay]['teu_deck_capacity'] = 0.0 
        d_bay_kpi_teu[bay]['teu_deck'] = 0.0
        d_bay_kpi_teu[bay]['quantity_at_depart'] = 0
        d_bay_kpi_teu[bay]['quantity_20_at_depart'] = 0
        d_bay_kpi_teu[bay]['reefers_capacity'] = 0
        d_bay_kpi_teu[bay]['reefers_at_depart'] = 0
    
    # calcul des quantités théoriques
    for sb, (capacity, stack_size, nb_stacks, nb_reefers) in d_sb_capacities.items():
        bay = sb[0:-2]
        tier = sb[-1]
        if tier == '0':
            d_bay_kpi_teu[bay]['teu_hold_capacity'] += capacity
        if tier == '1':
            d_bay_kpi_teu[bay]['teu_deck_capacity'] += capacity
        d_bay_kpi_teu[bay]['reefers_capacity'] += nb_reefers
    
    # calcul des sommes nécessaires, pour les quantités effectives
    for sb, d_kpi_elems in d_sb_kpi_elems.items():
        if sb == '': continue
        bay = sb[0:-2]
        tier = sb[-1]
        
        if tier == '0':
            d_bay_kpi_teu[bay]['teu_hold'] += d_kpi_elems['teu_at_depart']
        if tier == '1':
            d_bay_kpi_teu[bay]['teu_deck'] += d_kpi_elems['teu_at_depart']
        
        d_bay_kpi_teu[bay]['quantity_20_at_depart'] += d_kpi_elems['quantity_20_at_depart']
        d_bay_kpi_teu[bay]['quantity_at_depart'] += d_kpi_elems['quantity_at_depart']
        
        d_bay_kpi_teu[bay]['reefers_at_depart'] += d_kpi_elems['reefers_at_depart']          
 
    # en déduire les ratios au niveau de chaque baie
    for bay in l_bays:
        # ratio teu
        # 74 et 94 n'ont pas de cales...
        if d_bay_kpi_teu[bay]['teu_hold_capacity'] != 0:
            d_bay_kpi_teu[bay]['teu_hold_empty_ratio'] = 1.0 - (d_bay_kpi_teu[bay]['teu_hold']\
                                                               /d_bay_kpi_teu[bay]['teu_hold_capacity'])
        else:
            d_bay_kpi_teu[bay]['teu_hold_empty_ratio'] = 0.0
        if d_bay_kpi_teu[bay]['teu_deck_capacity'] != 0: # should not happen                                                        
            d_bay_kpi_teu[bay]['teu_deck_empty_ratio'] = 1.0 - (d_bay_kpi_teu[bay]['teu_deck']\
                                                               /d_bay_kpi_teu[bay]['teu_deck_capacity'])
        else:
            d_bay_kpi_teu[bay]['teu_deck_empty_ratio'] = 0.0
            
        # et ratio quantité 20 / quantité totale
        d_bay_kpi_teu[bay]['ratio_20_at_depart'] = 0.0
        if d_bay_kpi_teu[bay]['quantity_at_depart'] != 0:
            d_bay_kpi_teu[bay]['ratio_20_at_depart'] = d_bay_kpi_teu[bay]['quantity_20_at_depart']\
                                                     / d_bay_kpi_teu[bay]['quantity_at_depart']
        
        # et ratios reefers
        d_bay_kpi_teu[bay]['ratio_reefers_at_depart'] = 0.0
        if d_bay_kpi_teu[bay]['reefers_capacity'] != 0:
            d_bay_kpi_teu[bay]['ratio_reefers_at_depart'] = d_bay_kpi_teu[bay]['reefers_at_depart']\
                                                          / d_bay_kpi_teu[bay]['reefers_capacity']
        
    return d_bay_kpi_teu

##### KPI des écarts de masse, au niveau de chaque baie
    
    
# calcul des masses gauches et droites et des écarts au niveau de chaque bay

def get_kpi_weights_distrib(d_sb_kpi_elems, l_bays, d_left_right_subbays):
    
    d_bay_kpi_weights_distrib = {}
    
    for bay in l_bays:
        d_bay_kpi_weights_distrib[bay] = {}
        d_bay_kpi_weights_distrib[bay]['hold_left_weight'] = 0.0 
        d_bay_kpi_weights_distrib[bay]['hold_right_weight'] = 0.0
        d_bay_kpi_weights_distrib[bay]['deck_left_weight'] = 0.0 
        d_bay_kpi_weights_distrib[bay]['deck_right_weight'] = 0.0
    
    # calcul des sommes nécessaires
    for sb, d_kpi_elems in d_sb_kpi_elems.items():
        if sb == '': continue
        bay = sb[0:-2]
        tier = sb[-1]
        if sb in d_left_right_subbays[bay]['left'] and tier == '0':
            d_bay_kpi_weights_distrib[bay]['hold_left_weight'] += d_kpi_elems['weight_at_depart']
        if sb in d_left_right_subbays[bay]['right'] and tier == '0':
            d_bay_kpi_weights_distrib[bay]['hold_right_weight'] += d_kpi_elems['weight_at_depart']
        if sb in d_left_right_subbays[bay]['left'] and tier == '1':
            d_bay_kpi_weights_distrib[bay]['deck_left_weight'] += d_kpi_elems['weight_at_depart']
        if sb in d_left_right_subbays[bay]['right'] and tier == '1':
            d_bay_kpi_weights_distrib[bay]['deck_right_weight'] += d_kpi_elems['weight_at_depart']
    
    # différentiel de masses
    for bay in l_bays:
        d_bay_kpi_weights_distrib[bay]['hold_diff_weight'] = d_bay_kpi_weights_distrib[bay]['hold_left_weight']\
                                                           - d_bay_kpi_weights_distrib[bay]['hold_right_weight']
        d_bay_kpi_weights_distrib[bay]['deck_diff_weight'] = d_bay_kpi_weights_distrib[bay]['deck_left_weight']\
                                                           - d_bay_kpi_weights_distrib[bay]['deck_right_weight']
    
    return d_bay_kpi_weights_distrib

###### Agrégation au niveau des ports
    
# agrégation des sommes au niveau des ports
def get_kpi_sums(d_sb_kpi_elems, d_bay_kpi_cranes, 
                 nb_cranes, crane_single_speed, dual_cycling_gain, hc_move_time,
                 d_bay_kpi_teu, d_bay_kpi_weights_distrib):
    
    d_kpi_sums = {}
    
    d_kpi_sums['quantity_at_arrival'] = 0
    d_kpi_sums['quantity_discharged'] = 0
    d_kpi_sums['quantity_to_load'] = 0
    d_kpi_sums['quantity_loaded'] = 0
    d_kpi_sums['quantity_at_depart'] = 0
    d_kpi_sums['quantity_rob'] = 0
    d_kpi_sums['hc_move'] = 0
    d_kpi_sums['hc_restow'] = 0
    d_kpi_sums['hc_obligatory_restow'] = 0
    d_kpi_sums['extra_move'] = 0
    d_kpi_sums['quantity_effective_load'] = 0
    d_kpi_sums['garbage'] = 0
    
    d_kpi_sums['teu_at_arrival'] = 0
    d_kpi_sums['teu_discharged'] = 0
    d_kpi_sums['teu_to_load'] = 0
    d_kpi_sums['teu_loaded'] = 0
    d_kpi_sums['teu_at_depart'] = 0
    d_kpi_sums['teu_rob'] = 0
    d_kpi_sums['hc_teu_restow'] = 0
    d_kpi_sums['hc_teu_obligatory_restow'] = 0
    d_kpi_sums['teu_extra_move'] = 0
    d_kpi_sums['teu_effective_load'] = 0
    d_kpi_sums['teu_garbage'] = 0
    
    for sb, d_kpi_elems in d_sb_kpi_elems.items():
        
        #if sb == '': continue
        
        d_kpi_sums['quantity_at_arrival'] += d_kpi_elems['quantity_at_arrival']
        d_kpi_sums['quantity_discharged'] += d_kpi_elems['quantity_discharged']
        d_kpi_sums['quantity_to_load'] += d_kpi_elems['quantity_to_load']
        d_kpi_sums['quantity_loaded'] += d_kpi_elems['quantity_loaded']
        d_kpi_sums['quantity_at_depart'] += d_kpi_elems['quantity_at_depart']
        d_kpi_sums['quantity_rob'] += d_kpi_elems['quantity_rob']
        d_kpi_sums['hc_move'] += d_kpi_elems['hc_move']
        d_kpi_sums['hc_restow'] += d_kpi_elems['hc_restow']
        d_kpi_sums['hc_obligatory_restow'] += d_kpi_elems['hc_obligatory_restow']
        d_kpi_sums['extra_move'] += d_kpi_elems['extra_move']
        d_kpi_sums['quantity_effective_load'] += d_kpi_elems['quantity_effective_load']
        d_kpi_sums['garbage'] += d_kpi_elems['garbage']
        
        d_kpi_sums['teu_at_arrival'] += d_kpi_elems['teu_at_arrival']
        d_kpi_sums['teu_discharged'] += d_kpi_elems['teu_discharged']
        d_kpi_sums['teu_to_load'] += d_kpi_elems['teu_to_load']
        d_kpi_sums['teu_loaded'] += d_kpi_elems['teu_loaded']
        d_kpi_sums['teu_at_depart'] += d_kpi_elems['teu_at_depart']
        d_kpi_sums['teu_rob'] += d_kpi_elems['teu_rob']
        d_kpi_sums['hc_teu_restow'] += d_kpi_elems['hc_teu_restow']
        d_kpi_sums['hc_teu_obligatory_restow'] += d_kpi_elems['hc_teu_obligatory_restow']
        d_kpi_sums['teu_extra_move'] += d_kpi_elems['teu_extra_move']
        d_kpi_sums['teu_effective_load'] += d_kpi_elems['teu_effective_load']
        d_kpi_sums['teu_garbage'] += d_kpi_elems['teu_garbage']
        
        
    d_kpi_sums['dual_cycling'] = 0
    d_kpi_sums['crane_split'] = 0
    d_kpi_sums['teu_dual_cycling'] = 0
    d_kpi_sums['teu_crane_split'] = 0
    for bay, d_kpi_cranes in d_bay_kpi_cranes.items():
        d_kpi_sums['dual_cycling'] += d_kpi_cranes['dual_cycling']
        if d_kpi_cranes['crane_split'] > d_kpi_sums['crane_split']:
            d_kpi_sums['crane_split'] = d_kpi_cranes['crane_split']
        
        d_kpi_sums['teu_dual_cycling'] += d_kpi_cranes['teu_dual_cycling']
        if d_kpi_cranes['teu_crane_split'] > d_kpi_sums['teu_crane_split']:
            d_kpi_sums['teu_crane_split'] = d_kpi_cranes['teu_crane_split']
    
    # rajouter les dual cycling et crane split idéaux
    d_kpi_sums['ideal_dual_cycling'] = min(d_kpi_sums['quantity_discharged'], d_kpi_sums['quantity_loaded'])\
                                     * dual_cycling_gain
    d_kpi_sums['ideal_crane_split'] = ( d_kpi_sums['quantity_discharged']\
                                      + d_kpi_sums['quantity_loaded']\
                                      - d_kpi_sums['dual_cycling']\
                                      + (2 * d_kpi_sums['hc_restow'])\
                                      + (hc_move_time * crane_single_speed * d_kpi_sums['hc_move'])) / nb_cranes
    
    # here again, 0.5 for teu, 40' count for 1, 20' for 0.5
    d_kpi_sums['teu_ideal_dual_cycling'] = min(0.5 * d_kpi_sums['teu_discharged'], 0.5 * d_kpi_sums['teu_loaded'])\
                                         * dual_cycling_gain
    d_kpi_sums['teu_ideal_crane_split'] = ( (0.5 * d_kpi_sums['teu_discharged'])\
                                          + (0.5 * d_kpi_sums['teu_loaded'])\
                                          - d_kpi_sums['teu_dual_cycling']\
                                          + (2 * 0.5 * d_kpi_sums['hc_teu_restow'])\
                                          + (hc_move_time * crane_single_speed * d_kpi_sums['hc_move'])) / nb_cranes
    
    
    # TEU, avec rapport à calculer globalement
    d_kpi_sums['teu_hold'] = 0
    d_kpi_sums['teu_deck'] = 0
    d_kpi_sums['teu_hold_capacity'] = 0
    d_kpi_sums['teu_deck_capacity'] = 0
    d_kpi_sums['quantity_20_at_depart'] = 0
    d_kpi_sums['reefers_capacity'] = 0
    d_kpi_sums['reefers_at_depart'] = 0
    for bay, d_kpi_teu in d_bay_kpi_teu.items():
        d_kpi_sums['teu_hold'] += d_kpi_teu['teu_hold']
        d_kpi_sums['teu_deck'] += d_kpi_teu['teu_deck']
        d_kpi_sums['teu_hold_capacity'] += d_kpi_teu['teu_hold_capacity']
        d_kpi_sums['teu_deck_capacity'] += d_kpi_teu['teu_deck_capacity']
        d_kpi_sums['quantity_20_at_depart'] += d_kpi_teu['quantity_20_at_depart']
        d_kpi_sums['reefers_capacity'] += d_kpi_teu['reefers_capacity']
        d_kpi_sums['reefers_at_depart'] += d_kpi_teu['reefers_at_depart']
    d_kpi_sums['teu_hold_empty_ratio'] = 1.0 - d_kpi_sums['teu_hold'] / d_kpi_sums['teu_hold_capacity']
    d_kpi_sums['teu_deck_empty_ratio'] = 1.0 - d_kpi_sums['teu_deck'] / d_kpi_sums['teu_deck_capacity']
    d_kpi_sums['ratio_20_at_depart'] = d_kpi_sums['quantity_20_at_depart'] / d_kpi_sums['quantity_at_depart']
    d_kpi_sums['ratio_reefers_at_depart'] = d_kpi_sums['reefers_at_depart'] / d_kpi_sums['reefers_capacity']
    
    # weights
    d_kpi_sums['hold_left_weight'] = 0.0
    d_kpi_sums['hold_right_weight'] = 0.0
    d_kpi_sums['hold_diff_weight'] = 0.0
    d_kpi_sums['deck_left_weight'] = 0.0
    d_kpi_sums['deck_right_weight'] = 0.0
    d_kpi_sums['deck_diff_weight'] = 0.0
    for bay, d_kpi_weights_distrib in d_bay_kpi_weights_distrib.items():
        d_kpi_sums['hold_left_weight'] += d_kpi_weights_distrib['hold_left_weight']
        d_kpi_sums['hold_right_weight'] += d_kpi_weights_distrib['hold_right_weight']
        d_kpi_sums['hold_diff_weight'] += d_kpi_weights_distrib['hold_diff_weight']
        d_kpi_sums['deck_left_weight'] += d_kpi_weights_distrib['deck_left_weight']
        d_kpi_sums['deck_right_weight'] += d_kpi_weights_distrib['deck_right_weight']
        d_kpi_sums['deck_diff_weight'] += d_kpi_weights_distrib['deck_diff_weight']
    
    return d_kpi_sums


# mettre les dictionnaires en liste et les trier par sous-baies (et baies), le tout dans une liste par ports
def port_kpi_dicos_2_lists(l_d_sb_kpi_elems, 
                           l_d_bay_kpi_cranes, l_d_bay_kpi_teu, l_d_bay_kpi_weights_distrib,
                           d_ports_capacities, dual_cycling_gain, hc_move_time):


    sequence_length = len(l_d_sb_kpi_elems)
    if len(l_d_bay_kpi_cranes) != sequence_length: print("KPI port lists inconsistency")
    if len(l_d_bay_kpi_teu) != sequence_length: print("KPI port lists inconsistency")
    if len(l_d_bay_kpi_weights_distrib) != sequence_length: print("KPI port lists inconsistency")
    
    l_d_kpi_sums = []
    for ix in range(sequence_length):
        
        # en ix, on a arrival = ix, depart = ix+1, on regarde port de départ
        port_seq = ix+1
        nb_cranes = d_ports_capacities[port_seq][0]
        crane_single_speed = d_ports_capacities[port_seq][1]
    
        d_sb_kpi_elems = l_d_sb_kpi_elems[ix]
        d_bay_kpi_cranes = l_d_bay_kpi_cranes[ix]
        d_bay_kpi_teu = l_d_bay_kpi_teu[ix]
        d_bay_kpi_weights_distrib = l_d_bay_kpi_weights_distrib[ix]
        d_kpi_sums = get_kpi_sums(d_sb_kpi_elems, d_bay_kpi_cranes, 
                                  nb_cranes, crane_single_speed,
                                  dual_cycling_gain, hc_move_time,
                                  d_bay_kpi_teu, d_bay_kpi_weights_distrib)
        l_d_kpi_sums.append(d_kpi_sums)
    
    return l_d_kpi_sums

# mettre les dictionnaires en liste et les trier par sous-baies (et baies), le tout dans une liste par ports
def sb_kpi_dicos_2_lists(l_d_sb_kpi_elems, 
                         l_d_bay_kpi_cranes, l_d_bay_kpi_teu, l_d_bay_kpi_weights_distrib):

    l_l_sb_kpi_elems = []
    for d_sb_kpi_elems in l_d_sb_kpi_elems:
        l_sb_kpi_elems = [(sb, d_kpi_elems) for sb, d_kpi_elems in d_sb_kpi_elems.items()]
        l_sb_kpi_elems.sort(key=lambda x: x[0])
        l_l_sb_kpi_elems.append(l_sb_kpi_elems)
    l_l_bay_kpi_cranes = []
    for d_bay_kpi_cranes in l_d_bay_kpi_cranes:
        l_bay_kpi_cranes = [(bay, d_kpi_cranes) for bay, d_kpi_cranes in d_bay_kpi_cranes.items()]
        l_bay_kpi_cranes.sort(key=lambda x: x[0])
        l_l_bay_kpi_cranes.append(l_bay_kpi_cranes)
    l_l_bay_kpi_teu = []
    for d_bay_kpi_teu in l_d_bay_kpi_teu:
        l_bay_kpi_teu = [(bay, d_kpi_teu) for bay, d_kpi_teu in d_bay_kpi_teu.items()]
        l_bay_kpi_teu.sort(key=lambda x: x[0])
        l_l_bay_kpi_teu.append(l_bay_kpi_teu)
    l_l_bay_kpi_weights_distrib = []
    for d_bay_kpi_weights_distrib in l_d_bay_kpi_weights_distrib:
        l_bay_kpi_weights_distrib = [(bay, d_kpi_weights_distrib) for bay, d_kpi_weights_distrib in d_bay_kpi_weights_distrib.items()]
        l_bay_kpi_weights_distrib.sort(key=lambda x: x[0])
        l_l_bay_kpi_weights_distrib.append(l_bay_kpi_weights_distrib)
        
    return l_l_sb_kpi_elems, l_l_bay_kpi_cranes, l_l_bay_kpi_teu, l_l_bay_kpi_weights_distrib

# opérer la bascule en créant des listes associant au port (index), 
# un dictionnaire associant à chaque indicateur sa valeur globale, 
# et ses valeurs par sous-baie et baie selon le cas

def arrange_final_kpis(l_d_kpi_sums, l_l_sb_kpi_elems, 
                       l_l_bay_kpi_cranes, l_l_bay_kpi_teu, l_l_bay_kpi_weights_distrib):

    sequence_length = len(l_d_kpi_sums)
    if len(l_l_sb_kpi_elems) != sequence_length: print("KPI port lists inconsistency")
    if len(l_l_bay_kpi_cranes) != sequence_length: print("KPI port lists inconsistency")
    if len(l_l_bay_kpi_teu) != sequence_length: print("KPI port lists inconsistency")
    if len(l_l_bay_kpi_weights_distrib) != sequence_length: print("KPI port lists inconsistency")

    l_d_kpis = []
    for ix in range(sequence_length):
        d_kpis = {}
    
        l_sb_kpi_elems = l_l_sb_kpi_elems[ix]
    
        d_kpis['quantity_at_arrival'] = {}
        d_kpis['quantity_at_arrival']['port'] = l_d_kpi_sums[ix]['quantity_at_arrival']
        d_kpis['quantity_at_arrival']['sb'] = [d_kpi_elems['quantity_at_arrival']\
                                               for (sb, d_kpi_elems) in l_sb_kpi_elems]
        d_kpis['quantity_discharged'] = {}
        d_kpis['quantity_discharged']['port'] = l_d_kpi_sums[ix]['quantity_discharged']
        d_kpis['quantity_discharged']['sb'] = [d_kpi_elems['quantity_discharged']\
                                               for (sb, d_kpi_elems) in l_sb_kpi_elems]
        d_kpis['quantity_to_load'] = {}
        d_kpis['quantity_to_load']['port'] = l_d_kpi_sums[ix]['quantity_to_load']
        d_kpis['quantity_to_load']['sb'] = [d_kpi_elems['quantity_to_load']\
                                            for (sb, d_kpi_elems) in l_sb_kpi_elems]
        d_kpis['quantity_loaded'] = {}
        d_kpis['quantity_loaded']['port'] = l_d_kpi_sums[ix]['quantity_loaded']
        d_kpis['quantity_loaded']['sb'] = [d_kpi_elems['quantity_loaded']\
                                           for (sb, d_kpi_elems) in l_sb_kpi_elems]
        d_kpis['quantity_at_depart'] = {}
        d_kpis['quantity_at_depart']['port'] = l_d_kpi_sums[ix]['quantity_at_depart']
        d_kpis['quantity_at_depart']['sb'] = [d_kpi_elems['quantity_at_depart']\
                                               for (sb, d_kpi_elems) in l_sb_kpi_elems]
        d_kpis['quantity_rob'] = {}
        d_kpis['quantity_rob']['port'] = l_d_kpi_sums[ix]['quantity_rob']
        d_kpis['quantity_rob']['sb'] = [d_kpi_elems['quantity_rob']\
                                        for (sb, d_kpi_elems) in l_sb_kpi_elems]
        d_kpis['hc_move'] = {}
        d_kpis['hc_move']['port'] = l_d_kpi_sums[ix]['hc_move']
        d_kpis['hc_move']['sb'] = [d_kpi_elems['hc_move']\
                                   for (sb, d_kpi_elems) in l_sb_kpi_elems]
        d_kpis['hc_restow'] = {}
        d_kpis['hc_restow']['port'] = l_d_kpi_sums[ix]['hc_restow']
        d_kpis['hc_restow']['sb'] = [d_kpi_elems['hc_restow']\
                                     for (sb, d_kpi_elems) in l_sb_kpi_elems]
        d_kpis['hc_obligatory_restow'] = {}
        d_kpis['hc_obligatory_restow']['port'] = l_d_kpi_sums[ix]['hc_obligatory_restow']
        d_kpis['hc_obligatory_restow']['sb'] = [d_kpi_elems['hc_obligatory_restow']\
                                                for (sb, d_kpi_elems) in l_sb_kpi_elems]
        d_kpis['extra_move'] = {}
        d_kpis['extra_move']['port'] = l_d_kpi_sums[ix]['extra_move']
        d_kpis['extra_move']['sb'] = [d_kpi_elems['extra_move']\
                                      for (sb, d_kpi_elems) in l_sb_kpi_elems]
        d_kpis['quantity_effective_load'] = {}
        d_kpis['quantity_effective_load']['port'] = l_d_kpi_sums[ix]['quantity_effective_load']
        d_kpis['quantity_effective_load']['sb'] = [d_kpi_elems['quantity_effective_load']\
                                                   for (sb, d_kpi_elems) in l_sb_kpi_elems]
        d_kpis['garbage'] = {}
        d_kpis['garbage']['port'] = l_d_kpi_sums[ix]['garbage']
        d_kpis['garbage']['sb'] = [d_kpi_elems['garbage']\
                                   for (sb, d_kpi_elems) in l_sb_kpi_elems]
        
        # pour tests
        d_kpis['teu_discharged'] = {}
        d_kpis['teu_discharged']['port'] = l_d_kpi_sums[ix]['teu_discharged']
        d_kpis['teu_discharged']['sb'] = [d_kpi_elems['teu_discharged']\
                                               for (sb, d_kpi_elems) in l_sb_kpi_elems]
        d_kpis['teu_loaded'] = {}
        d_kpis['teu_loaded']['port'] = l_d_kpi_sums[ix]['teu_loaded']
        d_kpis['teu_loaded']['sb'] = [d_kpi_elems['teu_loaded']\
                                               for (sb, d_kpi_elems) in l_sb_kpi_elems]
        d_kpis['teu_effective_load'] = {}
        d_kpis['teu_effective_load']['port'] = l_d_kpi_sums[ix]['teu_effective_load']
        d_kpis['teu_effective_load']['sb'] = [d_kpi_elems['teu_effective_load']\
                                               for (sb, d_kpi_elems) in l_sb_kpi_elems]
        d_kpis['hc_teu_restow'] = {}
        d_kpis['hc_teu_restow']['port'] = l_d_kpi_sums[ix]['hc_teu_restow']
        d_kpis['hc_teu_restow']['sb'] = [d_kpi_elems['hc_teu_restow']\
                                     for (sb, d_kpi_elems) in l_sb_kpi_elems]
    
        l_bay_kpi_cranes = l_l_bay_kpi_cranes[ix]
    
        d_kpis['dual_cycling'] = {}
        d_kpis['dual_cycling']['port'] = l_d_kpi_sums[ix]['dual_cycling']
        d_kpis['dual_cycling']['port_ideal'] = l_d_kpi_sums[ix]['ideal_dual_cycling']
        d_kpis['dual_cycling']['bay'] = [d_kpi_cranes['dual_cycling']\
                                         for (bay, d_kpi_cranes) in l_bay_kpi_cranes]
        d_kpis['crane_split'] = {}
        d_kpis['crane_split']['port'] = l_d_kpi_sums[ix]['crane_split']
        d_kpis['crane_split']['port_ideal'] = l_d_kpi_sums[ix]['ideal_crane_split']
        d_kpis['crane_split']['bay'] = [d_kpi_cranes['crane_split']\
                                        for (bay, d_kpi_cranes) in l_bay_kpi_cranes]
        d_kpis['teu_dual_cycling'] = {}
        d_kpis['teu_dual_cycling']['port'] = l_d_kpi_sums[ix]['teu_dual_cycling']
        d_kpis['teu_dual_cycling']['port_ideal'] = l_d_kpi_sums[ix]['teu_ideal_dual_cycling']
        d_kpis['teu_dual_cycling']['bay'] = [d_kpi_cranes['teu_dual_cycling']\
                                             for (bay, d_kpi_cranes) in l_bay_kpi_cranes]
        d_kpis['teu_crane_split'] = {}
        d_kpis['teu_crane_split']['port'] = l_d_kpi_sums[ix]['teu_crane_split']
        d_kpis['teu_crane_split']['port_ideal'] = l_d_kpi_sums[ix]['teu_ideal_crane_split']
        d_kpis['teu_crane_split']['bay'] = [d_kpi_cranes['teu_crane_split']\
                                            for (bay, d_kpi_cranes) in l_bay_kpi_cranes]
    
        l_bay_kpi_teu = l_l_bay_kpi_teu[ix]
    
        d_kpis['teu_hold'] = {}
        d_kpis['teu_hold']['port'] = l_d_kpi_sums[ix]['teu_hold']
        d_kpis['teu_hold']['bay'] = [d_kpi_teu['teu_hold']\
                                     for (bay, d_kpi_teu) in l_bay_kpi_teu]
        d_kpis['teu_deck'] = {}
        d_kpis['teu_deck']['port'] = l_d_kpi_sums[ix]['teu_deck']
        d_kpis['teu_deck']['bay'] = [d_kpi_teu['teu_deck']\
                                     for (bay, d_kpi_teu) in l_bay_kpi_teu]
        d_kpis['teu_hold_capacity'] = {}
        d_kpis['teu_hold_capacity']['port'] = l_d_kpi_sums[ix]['teu_hold_capacity']
        d_kpis['teu_hold_capacity']['bay'] = [d_kpi_teu['teu_hold_capacity']\
                                              for (bay, d_kpi_teu) in l_bay_kpi_teu]
        d_kpis['teu_deck_capacity'] = {}
        d_kpis['teu_deck_capacity']['port'] = l_d_kpi_sums[ix]['teu_deck_capacity']
        d_kpis['teu_deck_capacity']['bay'] = [d_kpi_teu['teu_deck_capacity']\
                                              for (bay, d_kpi_teu) in l_bay_kpi_teu]
        d_kpis['teu_hold_empty_ratio'] = {}
        d_kpis['teu_hold_empty_ratio']['port'] = l_d_kpi_sums[ix]['teu_hold_empty_ratio']
        d_kpis['teu_hold_empty_ratio']['bay'] = [d_kpi_teu['teu_hold_empty_ratio']\
                                                 for (bay, d_kpi_teu) in l_bay_kpi_teu]
        d_kpis['teu_deck_empty_ratio'] = {}
        d_kpis['teu_deck_empty_ratio']['port'] = l_d_kpi_sums[ix]['teu_deck_empty_ratio']
        d_kpis['teu_deck_empty_ratio']['bay'] = [d_kpi_teu['teu_deck_empty_ratio']\
                                                 for (bay, d_kpi_teu) in l_bay_kpi_teu]
        d_kpis['ratio_20_at_depart'] = {}
        d_kpis['ratio_20_at_depart']['port'] = l_d_kpi_sums[ix]['ratio_20_at_depart']
        d_kpis['ratio_20_at_depart']['bay'] = [d_kpi_teu['ratio_20_at_depart']\
                                               for (bay, d_kpi_teu) in l_bay_kpi_teu]
        d_kpis['ratio_reefers_at_depart'] = {}
        d_kpis['ratio_reefers_at_depart']['port'] = l_d_kpi_sums[ix]['ratio_reefers_at_depart']
        d_kpis['ratio_reefers_at_depart']['bay'] = [d_kpi_teu['ratio_reefers_at_depart']\
                                                    for (bay, d_kpi_teu) in l_bay_kpi_teu]
    
    
        l_bay_kpi_weights_distrib = l_l_bay_kpi_weights_distrib[ix]
    
        d_kpis['hold_left_weight'] = {}
        d_kpis['hold_left_weight']['port'] = l_d_kpi_sums[ix]['hold_left_weight']
        d_kpis['hold_left_weight']['bay'] = [d_kpi_weights_distrib['hold_left_weight']\
                                             for (bay, d_kpi_weights_distrib) in l_bay_kpi_weights_distrib]
        d_kpis['hold_right_weight'] = {}
        d_kpis['hold_right_weight']['port'] = l_d_kpi_sums[ix]['hold_right_weight']
        d_kpis['hold_right_weight']['bay'] = [d_kpi_weights_distrib['hold_right_weight']\
                                              for (bay, d_kpi_weights_distrib) in l_bay_kpi_weights_distrib]
        d_kpis['hold_diff_weight'] = {}
        d_kpis['hold_diff_weight']['port'] = l_d_kpi_sums[ix]['hold_diff_weight']
        d_kpis['hold_diff_weight']['bay'] = [d_kpi_weights_distrib['hold_diff_weight']\
                                             for (bay, d_kpi_weights_distrib) in l_bay_kpi_weights_distrib]
        d_kpis['deck_left_weight'] = {}
        d_kpis['deck_left_weight']['port'] = l_d_kpi_sums[ix]['deck_left_weight']
        d_kpis['deck_left_weight']['bay'] = [d_kpi_weights_distrib['deck_left_weight']\
                                             for (bay, d_kpi_weights_distrib) in l_bay_kpi_weights_distrib]
        d_kpis['deck_right_weight'] = {}
        d_kpis['deck_right_weight']['port'] = l_d_kpi_sums[ix]['deck_right_weight']
        d_kpis['deck_right_weight']['bay'] = [d_kpi_weights_distrib['deck_right_weight']\
                                              for (bay, d_kpi_weights_distrib) in l_bay_kpi_weights_distrib]
        d_kpis['deck_diff_weight'] = {}
        d_kpis['deck_diff_weight']['port'] = l_d_kpi_sums[ix]['deck_diff_weight']
        d_kpis['deck_diff_weight']['bay'] = [d_kpi_weights_distrib['deck_diff_weight']\
                                             for (bay, d_kpi_weights_distrib) in l_bay_kpi_weights_distrib]
    
        l_d_kpis.append(d_kpis)
        
    return l_d_kpis


# écriture du fichier des kpis
def write_kpi_file(fn_kpi, crane_split_teu, l_d_kpis, 
                   d_sb_capacities, l_bays, l_subbays, l_cols_bay_subbay,
                   d_ports_capacities,
                   leg_no=0):
    
    f_kpi = open(fn_kpi, 'w')
    
    # header
    s_header = "PortSeq;PortName;MoveType;Total;Ideal"
    for (ix_bay, ix_subbay) in l_cols_bay_subbay:
        if ix_bay >= 0:
            s_header += ";" + l_bays[ix_bay]
        if ix_subbay >= 0:
            s_sb = l_subbays[ix_subbay] if ix_subbay > 0 else ''
            s_header += ";" + s_sb
    s_header += "\n"
    f_kpi.write(s_header)
    
    # lignes pour taille des piles et nb de piles
    s_stack_size = ";;;stack size ->;"
    for (ix_bay, ix_subbay) in l_cols_bay_subbay:
        if ix_bay >= 0:
            s_stack_size += ";"
        if ix_subbay >= 0:
            # nothing for garbage
            s_sb_stack_size = str(d_sb_capacities[l_subbays[ix_subbay]][1]) if ix_subbay > 0 else ''
            s_stack_size += ";" + s_sb_stack_size
    s_stack_size += "\n"
    f_kpi.write(s_stack_size)

    s_nb_stacks = ";;;nb stacks ->;"
    for (ix_bay, ix_subbay) in l_cols_bay_subbay:
        if ix_bay >= 0:
            s_nb_stacks += ";"
        if ix_subbay >= 0:
            # nothing for garbage
            s_sb_nb_stacks = str(d_sb_capacities[l_subbays[ix_subbay]][2]) if ix_subbay > 0 else ''
            s_nb_stacks += ";" + s_sb_nb_stacks
    s_nb_stacks += "\n"
    f_kpi.write(s_nb_stacks)

    s_void = ";;;;"
    for (ix_bay, ix_subbay) in l_cols_bay_subbay:
        s_void += ";"
    s_void += "\n"
    f_kpi.write(s_void)
    
    # boucler sur les ports
    sequence_length = len(l_d_kpis)
    for ix in range(sequence_length):
    
        port_seq = ix+1
        port_stub = "%d;%s;" % (port_seq, port_seq_2_name(port_seq, leg_no))
    
        # quantity at arrival
        s_kpi = port_stub + "arrival;"
        s_kpi += "%d;" % l_d_kpis[ix]['quantity_at_arrival']['port']
        for (ix_bay, ix_subbay) in l_cols_bay_subbay:
            s_kpi += ";"
            if ix_subbay > 0:
                s_kpi += "%d" % l_d_kpis[ix]['quantity_at_arrival']['sb'][ix_subbay]
        s_kpi += "\n"
        f_kpi.write(s_kpi)
    
        # quantity discharged (not for garbage)
        s_kpi = port_stub + "discharge;"
        s_kpi += "%d;" % l_d_kpis[ix]['quantity_discharged']['port']
        for (ix_bay, ix_subbay) in l_cols_bay_subbay:
            s_kpi += ";"
            if ix_subbay > 0:
                s_kpi += "%d" % l_d_kpis[ix]['quantity_discharged']['sb'][ix_subbay]
        s_kpi += "\n"
        f_kpi.write(s_kpi)
    
        # quantity rob (not for garbage)
        s_kpi = port_stub + "rob;"
        s_kpi += "%d;" % l_d_kpis[ix]['quantity_rob']['port']
        for (ix_bay, ix_subbay) in l_cols_bay_subbay:
            s_kpi += ";"
            if ix_subbay > 0:
                s_kpi += "%d" % l_d_kpis[ix]['quantity_rob']['sb'][ix_subbay]
        s_kpi += "\n"
        f_kpi.write(s_kpi)
    
        # quantity effectively loaded (not for garbage)
        s_kpi = port_stub + "load;"
        s_kpi += "%d;" % l_d_kpis[ix]['quantity_effective_load']['port']
        for (ix_bay, ix_subbay) in l_cols_bay_subbay:
            s_kpi += ";"
            if ix_subbay > 0:
                s_kpi += "%d" % l_d_kpis[ix]['quantity_effective_load']['sb'][ix_subbay]
        s_kpi += "\n"
        f_kpi.write(s_kpi)
    
        # quantity at departure
        s_kpi = port_stub + "departure;"
        s_kpi += "%d;" % l_d_kpis[ix]['quantity_at_depart']['port']
        for (ix_bay, ix_subbay) in l_cols_bay_subbay:
            s_kpi += ";"
            if ix_subbay > 0:
                s_kpi += "%d" % l_d_kpis[ix]['quantity_at_depart']['sb'][ix_subbay]
        s_kpi += "\n"
        f_kpi.write(s_kpi)
    
        # hc move (not for garbage)
        s_kpi = port_stub + "hc move;"
        s_kpi += "%d;" % l_d_kpis[ix]['hc_move']['port']
        for (ix_bay, ix_subbay) in l_cols_bay_subbay:
            s_kpi += ";"
            if ix_subbay > 0:
                s_kpi += "%d" % l_d_kpis[ix]['hc_move']['sb'][ix_subbay]
        s_kpi += "\n"
        f_kpi.write(s_kpi)
    
        # hc restow (not for garbage)
        s_kpi = port_stub + "hc restow;"
        s_kpi += "%d;" % l_d_kpis[ix]['hc_restow']['port']
        for (ix_bay, ix_subbay) in l_cols_bay_subbay:
            s_kpi += ";"
            if ix_subbay > 0:
                s_kpi += "%d" % l_d_kpis[ix]['hc_restow']['sb'][ix_subbay]
        s_kpi += "\n"
        f_kpi.write(s_kpi)
    
        # hc obligatory restow (not for garbage)
        s_kpi = port_stub + "including obligatory;"
        s_kpi += "%d;" % l_d_kpis[ix]['hc_obligatory_restow']['port']
        for (ix_bay, ix_subbay) in l_cols_bay_subbay:
            s_kpi += ";"
            if ix_subbay > 0:
                s_kpi += "%d" % l_d_kpis[ix]['hc_obligatory_restow']['sb'][ix_subbay]
        s_kpi += "\n"
        f_kpi.write(s_kpi)
    
        # garbage (not for garbage)
        s_kpi = port_stub + "garbage;"
        s_kpi += "%d;" % l_d_kpis[ix]['garbage']['port']
        for (ix_bay, ix_subbay) in l_cols_bay_subbay:
            s_kpi += ";"
        s_kpi += "\n"
        f_kpi.write(s_kpi)
    
        # dual cycling (for bays)
        kpi_name = 'teu_dual_cycling' if crane_split_teu == True else 'dual_cycling'
        s_kpi = port_stub + "dual cycling;"
        s_kpi += "%.1f;%.1f" % (l_d_kpis[ix][kpi_name]['port'], l_d_kpis[ix][kpi_name]['port_ideal'])
        for (ix_bay, ix_subbay) in l_cols_bay_subbay:
            s_kpi += ";"
            if ix_bay >= 0:
                s_kpi += "%.1f" % l_d_kpis[ix][kpi_name]['bay'][ix_bay]
        s_kpi += "\n"
        f_kpi.write(s_kpi)
    
        # crane split (for bays)
        kpi_name = 'teu_crane_split' if crane_split_teu == True else 'crane_split'
        s_kpi = ";%d;" % d_ports_capacities[port_seq][0] + "crane split;"
        s_kpi += "%.1f;%.1f" % (l_d_kpis[ix][kpi_name]['port'], l_d_kpis[ix][kpi_name]['port_ideal'])
        for (ix_bay, ix_subbay) in l_cols_bay_subbay:
            s_kpi += ";"
            if ix_bay >= 0:
                s_kpi += "%.1f" % l_d_kpis[ix][kpi_name]['bay'][ix_bay]
        s_kpi += "\n"
        f_kpi.write(s_kpi)
    
        # teu hold ratio (for bays)
        s_kpi = port_stub + "TEU empty % in hold;"
        s_kpi += "%.1f;" % (100 * l_d_kpis[ix]['teu_hold_empty_ratio']['port'])
        for (ix_bay, ix_subbay) in l_cols_bay_subbay:
            s_kpi += ";"
            if ix_bay >= 0:
                s_kpi += "%.1f" % (100 * l_d_kpis[ix]['teu_hold_empty_ratio']['bay'][ix_bay])
        s_kpi += "\n"
        f_kpi.write(s_kpi)
    
        # teu deck ratio (for bays)
        s_kpi = port_stub + "TEU empty % on deck;"
        s_kpi += "%.1f;" % (100 * l_d_kpis[ix]['teu_deck_empty_ratio']['port'])
        for (ix_bay, ix_subbay) in l_cols_bay_subbay:
            s_kpi += ";"
            if ix_bay >= 0:
                s_kpi += "%.1f" % (100 * l_d_kpis[ix]['teu_deck_empty_ratio']['bay'][ix_bay])
        s_kpi += "\n"
        f_kpi.write(s_kpi)
        
        # ratio 20 ' (quantity)
        s_kpi = port_stub + "ratio 20' / quantity;"
        s_kpi += "%.1f;" % (100 * l_d_kpis[ix]['ratio_20_at_depart']['port'])
        for (ix_bay, ix_subbay) in l_cols_bay_subbay:
            s_kpi += ";"
            if ix_bay >= 0:
                s_kpi += "%.1f" % (100 * l_d_kpis[ix]['ratio_20_at_depart']['bay'][ix_bay])
        s_kpi += "\n"
        f_kpi.write(s_kpi)
        
        # ratio reefers / capacity
        s_kpi = port_stub + "ratio reefers / capacity;"
        s_kpi += "%.1f;" % (100 * l_d_kpis[ix]['ratio_reefers_at_depart']['port'])
        for (ix_bay, ix_subbay) in l_cols_bay_subbay:
            s_kpi += ";"
            if ix_bay >= 0:
                s_kpi += "%.1f" % (100 * l_d_kpis[ix]['ratio_reefers_at_depart']['bay'][ix_bay])
        s_kpi += "\n"
        f_kpi.write(s_kpi)
    
        # hold weight on the left
        s_kpi = port_stub + "hold weight - left;"
        s_kpi += "%.1f;" % l_d_kpis[ix]['hold_left_weight']['port']
        for (ix_bay, ix_subbay) in l_cols_bay_subbay:
            s_kpi += ";"
            if ix_bay >= 0:
                s_kpi += "%.1f" % l_d_kpis[ix]['hold_left_weight']['bay'][ix_bay]
        s_kpi += "\n"
        f_kpi.write(s_kpi)
    
        # hold weight on the right
        s_kpi = port_stub + "hold weight - right;"
        s_kpi += "%.1f;" % l_d_kpis[ix]['hold_right_weight']['port']
        for (ix_bay, ix_subbay) in l_cols_bay_subbay:
            s_kpi += ";"
            if ix_bay >= 0:
                s_kpi += "%.1f" % l_d_kpis[ix]['hold_right_weight']['bay'][ix_bay]
        s_kpi += "\n"
        f_kpi.write(s_kpi)
    
        # hold weight difference
        s_kpi = port_stub + "hold diff. weight;"
        s_kpi += "%.1f;" % l_d_kpis[ix]['hold_diff_weight']['port']
        for (ix_bay, ix_subbay) in l_cols_bay_subbay:
            s_kpi += ";"
            if ix_bay >= 0:
                s_kpi += "%.1f" % l_d_kpis[ix]['hold_diff_weight']['bay'][ix_bay]
        s_kpi += "\n"
        f_kpi.write(s_kpi)
        
         # deck weight on the left
        s_kpi = port_stub + "deck weight - left;"
        s_kpi += "%.1f;" % l_d_kpis[ix]['deck_left_weight']['port']
        for (ix_bay, ix_subbay) in l_cols_bay_subbay:
            s_kpi += ";"
            if ix_bay >= 0:
                s_kpi += "%.1f" % l_d_kpis[ix]['deck_left_weight']['bay'][ix_bay]
        s_kpi += "\n"
        f_kpi.write(s_kpi)
    
        # deck weight on the right
        s_kpi = port_stub + "deck weight - right;"
        s_kpi += "%.1f;" % l_d_kpis[ix]['deck_right_weight']['port']
        for (ix_bay, ix_subbay) in l_cols_bay_subbay:
            s_kpi += ";"
            if ix_bay >= 0:
                s_kpi += "%.1f" % l_d_kpis[ix]['deck_right_weight']['bay'][ix_bay]
        s_kpi += "\n"
        f_kpi.write(s_kpi)
    
        # deck weight difference
        s_kpi = port_stub + "deck diff. weight;"
        s_kpi += "%.1f;" % l_d_kpis[ix]['deck_diff_weight']['port']
        for (ix_bay, ix_subbay) in l_cols_bay_subbay:
            s_kpi += ";"
            if ix_bay >= 0:
                s_kpi += "%.1f" % l_d_kpis[ix]['deck_diff_weight']['bay'][ix_bay]
        s_kpi += "\n"
        f_kpi.write(s_kpi)
    
        # void
        s_void = ";;;;"
        for (ix_bay, ix_subbay) in l_cols_bay_subbay:
            s_void += ";"
        s_void += "\n"
        f_kpi.write(s_void)
    
    f_kpi.close()



