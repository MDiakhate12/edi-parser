# libraries
import os

# directory and file names
##################
# TO BE MODIFIED

# contexte d'avancement
# 0 ; situation initiale, onboard de MYPKG et en entrée de CNTXG
# 1 : en sortie de CNTXG (PORT_SEQ = 1)
LEG_NO = 0
# puis
#LEG_NO = 5
#LEG_NO = 4

# correspondance entre slot position et sous-baies
REP_DATA_VESSEL = "c:/Projets/CCS/Vessel Stowage/Modèles/Data"
fn_stacks = "Stacks Extrait Prototype MP_IN.csv"
fn_subbays = "SubBays Capacities Extrait Prototype MP_IN.csv"
# liste onboard (+ loadlist)
REP_DATA_1 = "c:/Projets/CCS/Vessel Stowage/Python Prétraitement"
#FILE_NAME = "Loaded Lists IBM 2021-08-23 - ORIGINAL.csv"
# puis
FILE_NAME_ROOT = "9454450 Containers OnBoard Loadlist"
# double lecture écriture
REP_DATA_2 = "c:/Projets/CCS/Vessel Stowage/Python Prétraitement"
ADD_FILE_NAME_ROOT = "9454450 Containers Stowing Info"
OVERSTOWING_SUBBAYS_FILE_NAME_ROOT = "9454450 Overstowing Subbays"

# other parameters
# maximal number of containers for the POD to be considered in the subbay
MAX_NB_FOR_POD_IN_SUBBAY = 3
# when computing overstow, either compute all overstows (the default),
# or only overstows with hatch cover move
# COMMENTARISER ENSUITE
ONLY_HC_MOVES = True

##########################################################


# also dictionaries from Port Name to sequence and reverse
d_port_name_2_seq = {'MYPKG': 0, 'CNTXG': 1, 'KRPUS': 2, 'CNNGB': 3, 'CNSHA': 4, 'CNYTN': 5, 'SGSIN': 6,
                     'FRDKK': 7, 'GBSOU': 8, 'DEHAM': 9, 'NLRTM': 10, 'GBSOU2': 11, 'ESALG': 12}
d_seq_2_port_name = {0: 'MYPKG', 1: 'CNTXG', 2: 'KRPUS', 3: 'CNNGB', 4: 'CNSHA', 5: 'CNYTN', 6: 'SGSIN',
                     7: 'FRDKK', 8: 'GBSOU', 9: 'DEHAM', 10: 'NLRTM', 11: 'GBSOU2', 12: 'ESALG'}

# managing the correspondance between port name, sequence number in the context of a current leg
def port_seq_2_name(seq_no, leg_no):
    return d_seq_2_port_name[(seq_no + leg_no) % 13]
def port_name_2_seq(port_name, leg_no):
    return (d_port_name_2_seq[port_name] - leg_no) % 13


#############################################
# Préliminaire : éléments particuliers du navire et de la rotation

os.chdir(REP_DATA_VESSEL)

# obtention des éléments de pile du vaisseau
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


# récupérer aussi les capacités totales des sous-baies (au cas où)
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
    
    d_sb_capacities[subbay] = capacity
    
f_subbays.close()


# get the list of all stacks to consider when looking below (overstower)
# in some cases, one or several of those stack may not exist really in the vessel, existence to be checked when used
def get_stacks_4_below(stack, only_hc_move=False):
    
    bay_ref = stack[0]
    row_ref = stack[1]
    macro_tier_ref = stack[2]
    
    bay_no = int(bay_ref)
    if bay_no % 2 == 0: l_bays = ["%02d" % (bay_no-1), "%02d" % (bay_no), "%02d" % (bay_no+1)]
    if bay_no % 4 == 1: l_bays = ["%02d" % (bay_no), "%02d" % (bay_no+1)]
    if bay_no % 4 == 3: l_bays = ["%02d" % (bay_no), "%02d" % (bay_no-1)]
   
    l_stacks_4_below = []
    for bay in l_bays:
        # at present macro tier
        if only_hc_move == False:
            l_stacks_4_below.append((bay, row_ref, macro_tier_ref))
        # plus the stack in the hold if we are on the deck
        if macro_tier_ref == "1":
            l_stacks_4_below.append((bay, row_ref, "0"))
    
    return l_stacks_4_below

# get the list of all stacks to consider when looking upper (overstowed)
# in some cases, one or several of those stack may not exist really in the vessel, existence to be checked when used
def get_stacks_4_above(stack, only_hc_move=False):
    
    bay_ref = stack[0]
    row_ref = stack[1]
    macro_tier_ref = stack[2]
    
    bay_no = int(bay_ref)
    if bay_no % 2 == 0: l_bays = ["%02d" % (bay_no-1), "%02d" % (bay_no), "%02d" % (bay_no+1)]
    if bay_no % 4 == 1: l_bays = ["%02d" % (bay_no), "%02d" % (bay_no+1)]
    if bay_no % 4 == 3: l_bays = ["%02d" % (bay_no), "%02d" % (bay_no-1)]
   
    l_stacks_4_above = []
    for bay in l_bays:
        # at present macro tier
        if only_hc_move == False:
            l_stacks_4_above.append((bay, row_ref, macro_tier_ref))
        # plus the stack in the deck if we are in the hold
        if macro_tier_ref == "0":
            l_stacks_4_above.append((bay, row_ref, "1"))
    
    return l_stacks_4_above

# get the list of all stacks to consider when looking upper but to assess capacity impact
# in some cases, one or several of those stack may not exist really in the vessel, existence to be checked when used
def get_stacks_4_capacity_above(stack):
    
    bay_ref = stack[0]
    row_ref = stack[1]
    macro_tier_ref = stack[2]
    
    bay_no = int(bay_ref)
    # just look at the 20' bays (except for 74 and 94!!)
    if bay_no % 2 == 0 and bay_no not in [74, 94]: l_bays = ["%02d" % (bay_no-1), "%02d" % (bay_no+1)]
    if bay_no % 2 == 0 and bay_no in [74, 94]: l_bays = ["%02d" % (bay_no)]
    if bay_no % 4 == 1: l_bays = ["%02d" % (bay_no), "%02d" % (bay_no+2)]
    if bay_no % 4 == 3: l_bays = ["%02d" % (bay_no), "%02d" % (bay_no-2)]
   
    l_stacks_4_capacity_above = []
    for bay in l_bays:
        # only at present macro tier
        l_stacks_4_capacity_above.append((bay, row_ref, macro_tier_ref))
    
    return l_stacks_4_capacity_above

# for any slot, get the total capacity above
def total_capacity_above(slot, d_stacks, d_sb_capacities, only_hc_move=False):
    
    bay = slot[0:2]
    row = slot[2:4]
    tier = slot[4:6]
    
    bay_no = int(bay)
    row_no = int(row)
    tier_no = int(tier)
    if tier_no < 50: macro_tier = '0'
    if tier_no > 50: macro_tier = '1'
    
    origin_stack = (bay, row, macro_tier)
    # get the stacks for above (around in fact)
    l_stacks_4_above = get_stacks_4_capacity_above(origin_stack)
    
    # get the number of slots above the current slot in one stack
    capacity_above = 0
    # only if looking also current
    if only_hc_move == False:
        for stack in l_stacks_4_above:
            # the generated stack may not exist (for instance '73')
            if stack in d_stacks:
                ix_tier = get_ix_tier(tier, stack, d_stacks)
                nb_slots_above_in_stack = d_stacks[stack]['max_nb_std_cont'] - ix_tier - 1
                # specific case of 40' only bays (74, 94)
                if bay_no in [74, 94]:
                    nb_slots_above_in_stack *= 2
                capacity_above += nb_slots_above_in_stack
            
    # if in the hold, add the total capacity of the deck sub-bay above
    if macro_tier == '0':
        stack_above = (bay, row, '1')
        if stack_above in d_stacks: # should always be the case
            subbay_above = d_stacks[stack_above]['subbay']
            if subbay_above in d_sb_capacities: # should always be the case
                capacity_above += d_sb_capacities[subbay_above]
    
    return capacity_above

# is a slot a reefer slot 
# slot as (bay, row, tier)
def is_reefer_slot(slot, d_stacks):
    
    bay = slot[0:2]
    row = slot[2:4]
    tier = slot[4:6]
    
    bay_no = int(bay)
    row_no = int(row)
    tier_no = int(tier)
    if tier_no < 50: macro_tier = '0'
    if tier_no > 50: macro_tier = '1'

    # most of the cases, where no reefer at all
    if macro_tier == '0': return False
    if bay_no % 4 == 1: return False
    if row in ['19', '20']: return False
    if bay in ['01', '02', '03', '94']: return False
    
    # for other rows, use the tier
    reefer_low_tier = int(d_stacks[(bay, row, macro_tier)]['first_tier'])
    if bay in ['06', '07'] or row in ['17', '18'] or (bay in ['82', '83', '86', '87', '90', '91'] and row == '00'):
        nb_reefers_in_stack = 2
    else:
        nb_reefers_in_stack = 3
    reefer_high_tier = reefer_low_tier + (2 * (nb_reefers_in_stack-1))
    if tier_no >= reefer_low_tier and tier_no <= reefer_high_tier:
        return True
    
    return False

################################################################################
# Liste de dictionnaires qui à chaque port associe un dictionnaire qui à chaque pile 
# associe de bas en haut les caractéristiques des conteneurs (None si pas de conteneur)

# Charger les résultats de slot planning ou onboard

# initialiser tous les slots à None
def initialize_slots(d_stacks):
    
    d_stacks_slots = {}
    for stack, d_stack in d_stacks.items():
        nb_slots = d_stack['max_nb_std_cont']
        d_stacks_slots[stack] = [None] * nb_slots
        
    return d_stacks_slots


# Chargement On-board, maintenant on le fait uniquement à partir d'un format "CONSO"
def load_onboard_stacks(fn_onboard, d_stacks):

    # same structure than results, but with port 0 as only port
    d_ports_slots = {}

    f_onboard = open(fn_onboard, 'r')
    
    for no_line, line in enumerate(f_onboard):
        
        if no_line == 0: continue
    
        l_items = line.split(';')
        
        # first read slot, if no slot, that's the loading list part, to be skipped
        slot = l_items[8].strip() # sur 6 caractères ou 0...
        if len(slot) == 0: continue
    
        # subbay
        if len(slot) == 5: slot = '0'+slot # just in case
        garbage = False # just in case
        if len(slot) == 0: garbage = True
        if garbage == False:
            stack, ix_tier = get_slot_stack_ix_tier(slot, d_stacks)
        
        # POL and POD
        # La gestion GBSOU / GBSOU2 a été effectuée lors de la constitution du
        # fichier onboard - loadlist, pas nécessaire de faire cela aussi
        # par contre la gestion de la séquence doit se faire d'après le contexte
        pol_name = l_items[1].strip()
        pol_seq = port_name_2_seq(pol_name, LEG_NO)
        pod_name = l_items[2].strip()
        pod_seq = port_name_2_seq(pod_name, LEG_NO)
    
        # container id 
        container_id = l_items[0].strip()
        #if container in l_containers_to_exclude: continue
    
        # type
        #iso_type = l_items[3].strip()
        c_size = l_items[5].strip()
        hc = l_items[6].strip()
    
        # weight
        weight = float(l_items[7].strip()) # already in tons
    
        # specials, either '', 'E' (empty), or 'R' (effective reefer)
        # we must also determine if reefer or not
        specials = l_items[4].strip()
        if specials == 'R': c_type = 'RE'
        else: c_type = 'GP'
        
        if garbage == False:
            
            # here, only one (first) port
            # (some files may contain several ports)
            port_seq = 0
            if port_seq not in d_ports_slots:
                d_ports_slots[port_seq] = initialize_slots(d_stacks)
            d_ports_slots[port_seq][stack][ix_tier] = (container_id, pol_seq, pod_seq, 
                                                       c_size, c_type, weight, hc)    
       
    f_onboard.close()
    
    return d_ports_slots


# chargement en pratique
os.chdir(REP_DATA_1)

fn_loadlist = "%s %d.csv" % (FILE_NAME_ROOT, LEG_NO)
d_ports_slots = load_onboard_stacks(fn_loadlist, d_stacks)

# Restow
# Pour chaque conteneur avec un POD overstowage PODos, on crée le dictionnaire des {PODus: nb of containers with PODus < PODos}
# Hatch overstowage, uniquement si conteneur overstowage est en cale.
# Il suffira dans le fichier de donner les renseignements sur PODos, id, slot, plus petit des PODus, éventuellement nb total de conteneurs understowage

# Il faut d'abord mettre ensemble les piles cale / pontée, et tenir compte des différences entre 20' et 40'
# Pour chaque conteneur : regarder les conteneurs des slots dessous. Si 40, regarder les 3 * 2 piles
# Si 20, regarder les 2 (1 40 et 1 20) * 2 piles

PORT_NO = 0

# impossible to double-iterate on a dictionary, transform it into a list
l_ports_slots = [(stack, l_slots) for stack, l_slots in d_ports_slots[PORT_NO].items()]

# POUR POUVOIR UTILISER LES 2 VALEURS DE PARAMETRE 
# ONLY_HC_MOVES = False POUR ECRIRE "9454450 Containers Stowing Info"
# ONLY_HC_MOVES = True POUR ECRIRE "9454450 Overstowing Subbays"
# TRANSFORMER LIGNES SUIVANTES EN FONCTION, A APPELER 2 FOIS...
# EN ATTENDANT EXECUTER 2 FOIS AVEC LES VALEURS DE PARAMETRE DIFFERENT SI BESOIN

l_overstow = []

for (stack, l_slots) in l_ports_slots:
    bay = stack[0]
    row = stack[1]
    macro_tier = stack[2]
    # get the stacks of interest related to the current stack
    l_stacks_4_below = get_stacks_4_below(stack, ONLY_HC_MOVES)
    # look at each container (slot) of the current stack
    for ix, container in enumerate(l_slots):
        if container is None: continue
        container_id = container[0]
        # this dictionnary at the level of the (slot, container) contains for each POD < POD of the container,
        # the number of containers overstowed by the current container
        d_container_overstow = {}
        pod_no = container[2]
        tier_no = int(get_tier(ix, stack, d_stacks))
        for stack_4_below in l_stacks_4_below:
            if stack_4_below not in d_ports_slots[PORT_NO]: continue
            l_slots_4_below = d_ports_slots[PORT_NO][stack_4_below]
            for ix_4_below, container_4_below in enumerate(l_slots_4_below):
                # selection on filled slot 
                if container_4_below is None: continue
                # being strictly below the current slot and with a pod strictly before the current pod
                pod_no_4_below = container_4_below[2]
                tier_no_4_below = int(get_tier(ix_4_below, stack_4_below, d_stacks))
                # add it to the dictionnary
                if pod_no_4_below < pod_no and tier_no_4_below < tier_no:
                    if pod_no_4_below not in d_container_overstow:
                        d_container_overstow[pod_no_4_below] = 0
                    d_container_overstow[pod_no_4_below] += 1
        
        # now, append the dictionary to the list of overstows, 
        # with 3 elements, slot, container, dictionary of overstow, only if there are overstos
        if len(d_container_overstow) > 0:
            l_overstow.append(((bay, row, "%02d" % tier_no), container_id, d_container_overstow))

# sorting by the slot
l_overstow.sort(key=lambda x: x[0])


# Isolated containers in a sub-bay
# If no more than N (3) containers of the same POD in any given subbay

# for each subbay, get the distribution of POL / POD
d_subbay_pods = {}

for (stack, l_slots) in l_ports_slots:
    
    subbay = d_stacks[stack]['subbay']
    
    if subbay not in d_subbay_pods:
        d_subbay_pods[subbay] = {}
        
    for container in l_slots:
        if container is None: continue
            
        pod_no = container[2]
        if pod_no not in d_subbay_pods[subbay]:
            d_subbay_pods[subbay][pod_no] = 0
        d_subbay_pods[subbay][pod_no] += 1    


l_potential_restows = []

for (stack, l_slots) in l_ports_slots:
    bay = stack[0]
    row = stack[1]
    macro_tier = stack[2]
    subbay = d_stacks[stack]['subbay']
    # get the stacks of interest related to the current stack
    l_stacks_4_above = get_stacks_4_above(stack, ONLY_HC_MOVES)
    #print("stack:", stack)
    # look at each container (slot) of the current stack
    for ix, container in enumerate(l_slots):
        if container is None: continue
        container_id = container[0]
        # only consider containers from POD with less than N containers in the sub_bay
        pod_no = container[2]
        if d_subbay_pods[subbay][pod_no] > MAX_NB_FOR_POD_IN_SUBBAY: continue
        tier_no = int(get_tier(ix, stack, d_stacks))
        
        # set of PODS above the current container
        s_pod_above = set()
        
        for stack_4_above in l_stacks_4_above:
            if stack_4_above not in d_ports_slots[PORT_NO]: continue
            #print("stack 4 above:", stack_4_above)
            l_slots_4_above = d_ports_slots[PORT_NO][stack_4_above]
            for ix_4_above, container_4_above in enumerate(l_slots_4_above):
                # selection on filled slot 
                if container_4_above is None: continue
                # being strictly above the current slot and with a pod strictly before the current pod
                pod_no_4_above = container_4_above[2]
                tier_no_4_above = int(get_tier(ix_4_above, stack_4_above, d_stacks))
                # add it to the set
                if pod_no_4_above < pod_no and tier_no_4_above > tier_no:
                    if pod_no_4_above not in s_pod_above:
                        s_pod_above.add(pod_no_4_above)
        
        # now, append the dictionary to the list of overstows, 
        # with 4 elements, slot, container, dictionary of overstow, only if there are overstow
        # and also impact in terms of capacity 
        if len(s_pod_above) > 0:
            slot = "%s%s%02d" % (bay, row, tier_no)
            capacity_above = total_capacity_above(slot, d_stacks, d_sb_capacities, ONLY_HC_MOVES)
            l_potential_restows.append(((bay, row, "%02d" % tier_no), container_id, 
                                        s_pod_above, capacity_above))

# sorting by the slot
l_potential_restows.sort(key=lambda x: x[0])


# Non-reefer containers in reefer places

l_non_reefers_at_reefer = []

for (stack, l_slots) in l_ports_slots:
    bay = stack[0]
    row = stack[1]
    macro_tier = stack[2]
    # look at each container (slot) of the current stack
    for ix, container in enumerate(l_slots):
        if container is None: continue
        container_id = container[0]
        c_type = container[4]
        # is the slot a reefer slot ?
        tier_no = int(get_tier(ix, stack, d_stacks))
        slot = "%s%s%02d" % (bay, row, tier_no)
        placed_in_reefer = is_reefer_slot(slot, d_stacks)
        # is it a (real) reefer
        is_reefer = True if c_type == 'RE' else False
        if placed_in_reefer == True and is_reefer == False:
            l_non_reefers_at_reefer.append(((bay, row, "%02d" % tier_no), container_id))
            
# sorting by the slot
l_non_reefers_at_reefer.sort(key=lambda x: x[0])



# ### réécriture du fichier complémentaire .csv du on-board avec 6 nouvelles colonnes<p>
# - Overstow (X or empty)
# - Overstow POD (Name of the first port where overstow is needed)
# - Potential Restow (X or empty)
# - Potential Restow POD (Name of the last port, where potential overstow condition might happen)
# - Potential Restow Impact
# - Non Reefer At Reefer

# 6 colonnes ci-dessus en plus de :
# Position,POL,POD,Serial Number


# optimization, transform lists into dictionnary
d_overstows = {(bay, row, tier_no): d_container_overstow for ((bay, row, tier_no), container_id, d_container_overstow) in l_overstow}
d_potential_restows = {(bay, row, tier_no): (s_pod_above, capacity_above) for ((bay, row, tier_no), container_id, s_pod_above, capacity_above) in l_potential_restows}
d_non_reefers_at_reefer = {(bay, row, tier_no): True for ((bay, row, tier_no), container_id) in l_non_reefers_at_reefer}


# writing the file

fn_onboard = "%s %d.csv" % (FILE_NAME_ROOT, LEG_NO)
f_onboard = open(fn_onboard, 'r')

os.chdir(REP_DATA_2)
fn_add_onboard = "%s %d.csv" % (ADD_FILE_NAME_ROOT, LEG_NO)
f_add_onboard = open(fn_add_onboard, 'w')
    
for no_line, line in enumerate(f_onboard):
        
    if no_line == 0:
        s_header = "Slot;LoadPort;DischPort;ContId;" +\
                   "Overstow;OverstowPOD;"+\
                   "PotentialRestow;PotentialRestowPOD;PotentialRestowImpact;"+\
                   "NonReeferAtReefer\n"
        f_add_onboard.write(s_header)
        continue
    
    l_items = line.split(';')
    
    # first read slot, if no slot, that's the loading list part, to be skipped
    slot = l_items[8].strip() # sur 6 caractères ou 0...
    if len(slot) == 0: continue
    
    # subbay
    if len(slot) == 5: slot = '0'+slot # just in case
    
    # POL and POD
    pol_name = l_items[1].strip()
    pod_name = l_items[2].strip()
    # container id 
    container_id = l_items[0].strip()
    # other items are useless

    # transform slot into tuple, key to dictionary
    t_slot = (slot[0:2], slot[2:4], slot[4:6])
    
    # container to be added in the additional file
    to_be_added = False
    
    # overstow and potential restow
    overstow = ''
    overstow_pod_name = ''
    if t_slot in d_overstows:
        to_be_added = True
        overstow = 'X'
        d_container_overstow = d_overstows[t_slot]
        overstow_pod_no = 99
        for pod_no, nb_overstow in d_container_overstow.items():
            if pod_no < overstow_pod_no:
                overstow_pod_no = pod_no
        overstow_pod_name = port_seq_2_name(overstow_pod_no, LEG_NO)
        
    potential_restow = ''
    potential_restow_pod_name = ''
    s_capacity_above = ''
    if t_slot in d_potential_restows:
        to_be_added = True
        potential_restow = 'X'
        s_pod_above = d_potential_restows[t_slot][0]
        potential_restow_pod_no = -1
        for pod_no in s_pod_above:
            if pod_no > potential_restow_pod_no:
                potential_restow_pod_no = pod_no
        potential_restow_pod_name = port_seq_2_name(potential_restow_pod_no, LEG_NO)
        s_capacity_above = "%d" % d_potential_restows[t_slot][1]
    
    non_reefer_at_reefer = ''
    if t_slot in d_non_reefers_at_reefer:
        to_be_added = True
        non_reefer_at_reefer = 'X'
    
    # writing, only if any kind of overstow or other information
    if to_be_added == True:
        s_line = "%s;%s;%s;%s;%s;%s;%s;%s;%s;%s\n" %\
                 (slot, pol_name, pod_name, container_id,\
                  overstow, overstow_pod_name,\
                  potential_restow, potential_restow_pod_name, s_capacity_above,\
                  non_reefer_at_reefer)
        f_add_onboard.write(s_line)
        
f_onboard.close()
f_add_onboard.close()


# creating a file, which shows overstowing deck bays

# first obtain the set of overstowing subbays 
s_overstowing_subbays = set()

for ((bay, row, tier_no), container_id, d_container_overstow) in l_overstow:
    stack = (bay, row, '1' if int(tier_no) >= 50 else '0')
    subbay = d_stacks[stack]['subbay']
    if subbay not in s_overstowing_subbays: s_overstowing_subbays.add(subbay)

# writing...
fn_overstowing_subbays = "%s %d.csv" % (OVERSTOWING_SUBBAYS_FILE_NAME_ROOT, LEG_NO)
f_overstowing_subbays = open(fn_overstowing_subbays, 'w')
s_header = "Subbay;Overstowing\n"
f_overstowing_subbays.write(s_header)

l_subbays = [subbay for subbay in d_sb_capacities]
l_subbays.sort()
for subbay in l_subbays:
    overstowing = '1' if subbay in s_overstowing_subbays else '0'
    s_line = "%s;%s\n" % (subbay, overstowing)
    f_overstowing_subbays.write(s_line)
    
f_overstowing_subbays.close()

