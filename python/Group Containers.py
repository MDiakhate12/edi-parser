# libraries
import os


# directory and file names
##################
# TO BE MODIFIED

# general context
# 0 ; initial situation, onboard from MYPKG and entering CNTXG
# 1 : out of CNTXG (PORT_SEQ = 1)
# and so on...
LEG_NO = 0
# puis
#LEG_NO = 1

# correspondance between slot position and subbays
REP_DATA_VESSEL = "c:/Projets/CCS/Vessel Stowage/Modèles/Data"
fn_stacks = "Stacks Extrait Prototype MP_IN.csv"
# file name for revenues
FILE_NAME_REVENUES = "Revenues by Size Type POL POD.csv"

# orignial onboard (provided by CMA-CGM)
#REP_DATA_1 = "c:/Projets/CCS/Vessel Stowage/Python Prétraitement/9454450 Rotation"
#ONBOARD_LOADLIST_FILE_NAME = "X_00_MYPKG_2.csv"
REP_DATA_1 = "c:/Projets/CCS/Vessel Stowage/Python Prétraitement"
#REP_DATA_1="C:\Projets\CCS\Vessel Stowage\Python C++ Modèles\C++\MasterPlanningV3\msvc9\data\out_10_ports_test1"
#ONBOARD_LOADLIST_FILE_NAME = "Loaded Lists IBM 2021-08-23 - ORIGINAL.csv"
ONBOARD_LOADLIST_FILE_NAME_ROOT = "9454450 Containers OnBoard Loadlist"
#ONBOARD_LOADLIST_FILE_NAME = "resultatSlotPlanning"
##############
# A RECOMMENTARISER
#REGROUP_OVERSTOW = True
REGROUP_OVERSTOW = False
OVERSTOW_FILE_NAME_ROOT = "9454450 Containers Stowing Info"
##############
# writing, in only one phase
REP_DATA_2 = "c:/Projets/CCS/Vessel Stowage/Python Prétraitement"
#ONBOARD_LOADLIST_CG_FILE_NAME_ADD = "9454450 Generated OnBoard Management 0.csv"
ONBOARD_LOADLIST_CG_FILE_NAME_ADD = "9454450 Container Groups Completed 0.csv"
# plus correspondance between containers and container groups
ONBOARD_LOADLIST_CG_CONTAINERS = "9454450 Container Groups Containers.csv"

# if ever some containers to exclude
l_containers_to_exclude = [
'ECMU8152162',
'ECMU8080761',
'UESU4804406',
'ECMU8083800',
'ECMU8145570',
'ECMU8114922',
'DRYU4516301',
'APHU4625538',
'ECMU8081198',
'ECMU8087452',
'APHU4640039'
]

# IN FACT NO EXCLUSION
l_containers_to_exclude = []


##########################################################

# Introduction : data relative to vessel and rotation

os.chdir(REP_DATA_VESSEL)

# stack items
d_stack_2_subbay = {}

f_stacks = open(fn_stacks, 'r')
    
for no_line, line in enumerate(f_stacks):
        
    if no_line == 0: continue
    
    l_items = line.split(';')
    bay = l_items[0] # sur 2 caractères
    row = l_items[1] # sur 2 caractères
    macro_tier = l_items[2]
    subbay = l_items[3] # sur 4 caractères, alors que pour cg, avec bay sur 1 caractère
    #if subbay[0] == '0': subbay = subbay[1:] ici les positions sont lues sur 6
        
    stack = (bay, row, macro_tier)
    
    d_stack_2_subbay[stack] = subbay
    
f_stacks.close()


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

# function for getting revenues for each container
# creating a dictionnary which to each POL, POD couple associate 
# the 4 possible revenues (depending on size and type))

def load_revenues_by_pol_pod_size_type(fn_pol_pod_revenues):
    
    d_pol_pod_revenues = {}

    f_pol_pod_revenues = open(fn_pol_pod_revenues, 'r')
    
    for no_line, line in enumerate(f_pol_pod_revenues):
        
        if no_line == 0: continue
        
        l_items = line.split(';')
        
        pol_name = l_items[1]
        pod_name = l_items[2]
        size_type = l_items[0]
        revenue = float(l_items[3])
        
        if (pol_name, pod_name) not in d_pol_pod_revenues:
            d_pol_pod_revenues[(pol_name, pod_name)] = {}
        d_pol_pod_revenues[(pol_name, pod_name)][size_type] = revenue
        
    f_pol_pod_revenues.close()
    
    return d_pol_pod_revenues

# function which to a container group (plus empty info if needed)
# associate its revenue
def get_container_revenue(d_pol_pod_revenues, pol_name, pod_name, size, c_type, height, empty):
    
    # if no predefined revenue, return 0.0
    if (pol_name, pod_name) not in d_pol_pod_revenues: 
        print("POL %s POD %s NOT IN REVENUES" % (pol_name, pod_name))
        return 0.0
        
    # empty with a very low revenue
    #if empty == 'E': return 10.0
    
    # reefers
    if c_type == 'RE':
        return d_pol_pod_revenues[(pol_name, pod_name)]['Reefer']
    
    # 20'
    if size == '20':
        return d_pol_pod_revenues[(pol_name, pod_name)]['20']
    
    # remaining are 40, HC or not
    if height == 'HC':
        return d_pol_pod_revenues[(pol_name, pod_name)]['40 HC']
    
    # ordinaries 40'
    return d_pol_pod_revenues[(pol_name, pod_name)]['40']

# loading the revenues
d_pol_pod_revenues = load_revenues_by_pol_pod_size_type(FILE_NAME_REVENUES)

###############################################################################
# also, getting individual overstows if needed, 
# with converting the original POL into the overstow port
d_container_overstows = {}

if REGROUP_OVERSTOW == True:
    
    os.chdir(REP_DATA_1)
    
    fn_overstows = "%s %d.csv" % (OVERSTOW_FILE_NAME_ROOT, LEG_NO)
    f_overstows = open(fn_overstows, 'r')
    
    for no_line, line in enumerate(f_overstows):
        
        if no_line == 0: continue

        l_items = line.split(';')
        
        slot = l_items[0].strip()
        load_port = l_items[1].strip()
        disch_port = l_items[2].strip()
        container_id = l_items[3].strip()
        overstow = l_items[4].strip()
        overstow_port = l_items[5].strip()
        if overstow == 'X':
            # note pod_name for unicity of container traject 
            # (if the same container is used twice in the same voyage, that must be distinguished)
            d_container_overstows[(container_id, disch_port)] = port_name_2_seq(overstow_port, LEG_NO)

    f_overstows.close()

################################################################################
# Chargement du onboard avec transformation des conteneurs sur slots en groupes
# de conteneurs sur sous-baies

# preliminary function
# get the distinction between L(ight) and H(eavy)
# as an index in the list, limit_l is the highest index included into the list of L
# and weight_limit_l, the heaviest weight in the L group
# (and then select resp. <= et >)
def lh_split_container_group(l_weights, weight_threshold):
    
    # trier la liste, au cas où
    l_weights.sort()
    
    nb_weights = len(l_weights)
    # list of first indices in sub-groups
    l_ix_weight_ssgrps = []
    prev_weight = 0.0
    for ix, weight in enumerate(l_weights):
        if weight > prev_weight:
            l_ix_weight_ssgrps.append(ix)
            prev_weight = weight
    nb_ssgrps = len(l_ix_weight_ssgrps)

    # making into a list of min & max indices min et max of each sub-groups
    l_ix12_weight_ssgrps = []
    for ii, ix_weight_ssgrp in enumerate(l_ix_weight_ssgrps):
        ix_start = ix_weight_ssgrp
        if ii == nb_ssgrps - 1:
            ix_end = nb_weights - 1
        else:
            ix_end = l_ix_weight_ssgrps[ii+1] - 1
        l_ix12_weight_ssgrps.append((ix_start, ix_end))
        

    # testing average of weights for each sum from the beginning
    limit_l = -1
    weight_limit_l = 0.0
    for ii, (ix_start, ix_end) in enumerate(l_ix12_weight_ssgrps):
    
        l_weights_below = l_weights[0:ix_end+1]
        nb_weights_below = ix_end+1
        #avg_weight = sum(l_weights_below) / nb_weights_below
    
#        if avg_weight <= weight_threshold:
        if sum(l_weights_below) <= nb_weights_below * weight_threshold:
            limit_l = ix_end
            weight_limit_l = l_weights[ix_end]

    
    return limit_l, weight_limit_l

# loading containers groups items
# we have to both define container groups, 
# with the subdivision between L and H which implies to keep track of all 
# individual weights of containers of container groups before L/H subdivision
# and keep their distribution on the sub-bays
def load_onboard_loadlist_container_groups(fn_onboard_loadlist, leg_no,
                                           d_stack_2_subbay, l_containers_to_exclude,
                                           d_container_overstows):
    
    # while reading the file, aggregate the containers into sub-groups
    # ignoring the distinction L and H, and keep track of container weights
    # at the level of each subbay
    # L/H will be computed in a secend time
    d_subbays_macro_container_groups = {}

    f_onboard_loadlist = open(fn_onboard_loadlist, 'r')
    
    for no_line, line in enumerate(f_onboard_loadlist):
        
        if no_line == 0: continue
        
        l_items = line.split(';')
            
        # subbay
        slot = l_items[8].strip() # on 6 characters or empty
        if len(slot) in [5, 6]:
            if len(slot) == 5: slot = '0' + slot
            bay = slot[0:2] 
            row = slot[2:4]
            tier = slot[4:6]
            if tier < '50': macro_tier = '0'
            if tier > '50': macro_tier = '1'
            stack = (bay, row, macro_tier)
            subbay = d_stack_2_subbay[stack]
        else:
            subbay = ''
        
        # container id 
        container = l_items[0].strip()
        if container in l_containers_to_exclude: continue
        
        # POL and POD
        
        # starting with POD
        pod_name = l_items[2].strip()
        pod_seq = port_name_2_seq(pod_name, leg_no)
    
        # only POL now
        pol_name = l_items[1].strip()
        # if an overstow, the POL can be overriden with the port where loading is done
        # note pod_name for unicity of container traject 
        # (if the same container is used twice in the same voyage, that must be distinguished)
        overstow_port_seq = -1
        if (container, pod_name) in d_container_overstows:
            overstow_port_seq = d_container_overstows[(container, pod_name)]   
        
        # No need to change the name for Southampton,
        # allready done in the input file
        pol_seq = port_name_2_seq(pol_name, leg_no)
        
        # type and hc, 20 already deprived of HC...
        #iso_type = l_items[3].strip()
        c_size = l_items[5].strip()
        hc = l_items[6].strip()
        
        # weight
        # keep real (float) weight, and for rounding issues, have the weight
        # as a integer (in hundred of kilos)
        s_weight = l_items[7].strip()
        l_weight_elems = s_weight.split('.')
        i_weight = int(l_weight_elems[0]) * 10
        if len(l_weight_elems) == 2:
            i_weight += int(l_weight_elems[1])
        
        # specials, either '', 'E' (empty), or 'R' (effective reefer)
        # we must also determine if reefer or not
        setting = l_items[4]
        if setting == 'R': c_type = 'RE'
        else: c_type = 'GP'
        # NOTE: we keep also for later use the distinction empty / not empty
        # should be stored as a 3rd category for weight...
        empty = ''
        if setting == 'E': empty = 'E'
            
        # all elements have been collected
        # 3 possibilities
        # no overstowing from container, keep things are there are
        # there is overstowing :
        # create 2 occurrences
        # 1) current subbay, original pol, but pod is changed into overstow_port
        # 2) '' (load), overstow_port for pol, pod is inchanged
        
        l_container_sb_cg = []
        if overstow_port_seq == -1:
            subbay_macro_container_group = (subbay, (pol_seq, pod_seq, c_size, c_type, hc))   
            l_container_sb_cg.append(subbay_macro_container_group)
        # but change pol_name only when overstowed must be done on next port
        else:
            # EVENTUELLEMENT, GARDER pod_seq si overstow_port_seq > 1 ?????
            subbay_macro_container_group = (subbay, (pol_seq, overstow_port_seq, c_size, c_type, hc)) 
            l_container_sb_cg.append(subbay_macro_container_group)
            subbay_macro_container_group = ('', (overstow_port_seq, pod_seq, c_size, c_type, hc)) 
            l_container_sb_cg.append(subbay_macro_container_group)
        
        # integrating
        # sb and not subbay so that original subbay is not lost
        for (sb, macro_container_group) in l_container_sb_cg:
            if (sb, macro_container_group) not in d_subbays_macro_container_groups:
                d_subbays_macro_container_groups[(sb, macro_container_group)] = []
            # add the container's weight in the list associated to the subbay X macro_container_group
            # and also the fact that it is in overstow, but for time being only if subbay != '',
            # because it will be computed later on
            # SOURCE TO BE REORGANIZED LATER ON...
            d_subbays_macro_container_groups[(sb, macro_container_group)]\
            .append((container, empty, i_weight, 
                     overstow_port_seq if sb != '' else -1,
                     i_weight if overstow_port_seq > 0 else 0,
                     subbay if sb == '' else '',
                     pol_seq if sb == '' else -1))
    
    f_onboard_loadlist.close()
    
    # second step, determine the weight limits for each macro_container_groups
    # such as seen on the set of all sub-baies
    # INCLUDING SUBBAYS NOT FILLED IN A SET ONBOARD + LOADLIST
    d_macro_container_groups_l_weights = {}
    for (subbay, macro_container_group), l_items_in_subbay in d_subbays_macro_container_groups.items():
        if macro_container_group not in d_macro_container_groups_l_weights:
            d_macro_container_groups_l_weights[macro_container_group] = []
        l_weights_in_subbay = [i_weight \
                               for (container, empty, i_weight, 
                                    overstow_port_seq, i_overstow_weight, 
                                    overstow_sce_sb, overstow_sce_pol) in l_items_in_subbay]
        d_macro_container_groups_l_weights[macro_container_group].extend(l_weights_in_subbay)
    d_macro_container_groups_weight_limit_l = {}
    for macro_container_group, l_weights in d_macro_container_groups_l_weights.items():
        if macro_container_group[2] == '20':
            weight_threshold = 10.0
            i_weight_threshold = 100
        else:
            weight_threshold = 15.0
            i_weight_threshold = 150
        # l_weights will be sorted inside next function
        #limit_l, weight_limit_l = lh_split_container_group(l_weights, weight_threshold)
        limit_l, weight_limit_l = lh_split_container_group(l_weights, i_weight_threshold)
        d_macro_container_groups_weight_limit_l[macro_container_group] = weight_limit_l
            
    # it enable creating definitive container groups in sub-bays
    d_subbays_container_groups = {}
    # as well as the lists of individual containers associated to container groups
    d_container_groups_containers = {}
    for (subbay, macro_container_group), l_items_in_subbay in d_subbays_macro_container_groups.items():
        for (container, empty, i_weight, overstow_port_seq, i_overstow_weight, 
             overstow_sce_sb, overstow_sce_pol) in l_items_in_subbay:
            weight_limit_l = d_macro_container_groups_weight_limit_l[macro_container_group]
            if i_weight <= weight_limit_l:
                c_weight = 'L'
            else:
                c_weight = 'H'
            container_group = (macro_container_group[0], macro_container_group[1],
                               macro_container_group[2], macro_container_group[3],
                               c_weight, macro_container_group[4])
            if (subbay, container_group) not in d_subbays_container_groups:
                # quantity, weight, and overstow in subbay
                d_subbays_container_groups[(subbay, container_group)] = (0, 0.0, -1, 0, 0.0, {})
            sb_cg_quantity = d_subbays_container_groups[(subbay, container_group)][0] + 1
            # back to the real weight !!!
            sb_cg_weight = d_subbays_container_groups[(subbay, container_group)][1] + (i_weight/10)
            # overstowing in subbay
            sb_cg_overstow_port_seq = d_subbays_container_groups[(subbay, container_group)][2]
            if overstow_port_seq > 0: 
                sb_cg_overstow_port_seq = overstow_port_seq
            overstow_quantity = 1 if i_overstow_weight > 0 else 0
            sb_cg_overstow_quantity = d_subbays_container_groups[(subbay, container_group)][3]\
                                    + overstow_quantity
            sb_cg_overstow_weight = d_subbays_container_groups[(subbay, container_group)][4]\
                                    + (i_overstow_weight/10)
            sb_cg_overstow_sources = d_subbays_container_groups[(subbay, container_group)][5]
            if overstow_sce_sb != '':
                if (overstow_sce_sb, overstow_sce_pol) not in sb_cg_overstow_sources:
                    sb_cg_overstow_sources[(overstow_sce_sb, overstow_sce_pol)]\
                    = (overstow_quantity, i_overstow_weight/10)
                else:
                    sb_cg_overstow_sources[(overstow_sce_sb, overstow_sce_pol)]\
                    = (sb_cg_overstow_sources[(overstow_sce_sb, overstow_sce_pol)][0]\
                                              +overstow_quantity,
                       sb_cg_overstow_sources[(overstow_sce_sb, overstow_sce_pol)][1]\
                                              +(i_overstow_weight/10))
            d_subbays_container_groups[(subbay, container_group)] = (sb_cg_quantity, sb_cg_weight, 
                                                                     sb_cg_overstow_port_seq,
                                                                     sb_cg_overstow_quantity,
                                                                     sb_cg_overstow_weight,
                                                                     sb_cg_overstow_sources)
            # and then, processing the container lists
            # NOTE, we must handle the empty information which had been lost
            if container_group not in d_container_groups_containers:
                d_container_groups_containers[container_group] = []
            d_container_groups_containers[container_group].append((container, empty))
    
    return d_subbays_container_groups, d_container_groups_containers


###################################################
# real loading
os.chdir(REP_DATA_1)

fn_onboard_loadlist = "%s %d.csv" % (ONBOARD_LOADLIST_FILE_NAME_ROOT, LEG_NO)
d_subbays_container_groups, d_container_groups_containers = \
    load_onboard_loadlist_container_groups(fn_onboard_loadlist, LEG_NO,
                                           d_stack_2_subbay,
                                           l_containers_to_exclude,
                                           d_container_overstows)

# get the 3 onboard dictionaries at level of container groups
d_onboard_loadlist_cg_2_sb = {}
d_onboard_loadlist_sb_2_cg = {}
d_onboard_loadlist_cg_total_quantity_weight = {}
d_onboard_loadlist_cg_avg_weight = {}

for (sb, cg), (quantity, weight, overstow_port_seq,\
               overstow_quantity, overstow_weight, overstow_sources)\
    in d_subbays_container_groups.items():
    
    if cg not in d_onboard_loadlist_cg_2_sb:
        d_onboard_loadlist_cg_2_sb[cg] = {}
    d_onboard_loadlist_cg_2_sb[cg][sb] = (quantity, weight,
                                          overstow_port_seq, overstow_quantity, overstow_weight, overstow_sources)
    
    if sb not in d_onboard_loadlist_sb_2_cg:
        d_onboard_loadlist_sb_2_cg[sb] = {}
    d_onboard_loadlist_sb_2_cg[sb][cg] = (quantity, weight,
                                          overstow_port_seq, overstow_quantity, overstow_weight, overstow_sources)
    
    if cg not in d_onboard_loadlist_cg_total_quantity_weight:
        d_onboard_loadlist_cg_total_quantity_weight[cg] = (0, 0.0)
    total_quantity = d_onboard_loadlist_cg_total_quantity_weight[cg][0] + quantity
    total_weight = d_onboard_loadlist_cg_total_quantity_weight[cg][1] + weight
    d_onboard_loadlist_cg_total_quantity_weight[cg] = (total_quantity, total_weight)
    
for cg, (total_quantity, total_weight) in d_onboard_loadlist_cg_total_quantity_weight.items():
    d_onboard_loadlist_cg_avg_weight[cg] = total_weight / total_quantity


# and for writing the file, we will sort it by subbay
# also at that stage, add the relevant information in the subbay '' for overstow
# to be transformed into a loadlist at next port
        
# set of cg which have been completed because already in load list
 
l_onboard_loadlist_sb_2_cg = []
for sb, d_cg in d_onboard_loadlist_sb_2_cg.items():
    l_cg = []
    for cg, (quantity, weight, 
             overstow_port_seq, overstow_quantity, overstow_weight, overstow_sources) in d_cg.items():
        
        # with data on overstowing
        with_overstow = False
        osw_quantity = 0
        osw_weight = 0.0
        osw_sources = {}
        if overstow_quantity > 0: # dans tous les cas, loadlist ou onboard
            with_overstow = True
            osw_quantity = overstow_quantity
            osw_weight = overstow_weight
            osw_sources = overstow_sources
        
        cg_items = (cg, quantity, weight,  
                    overstow_port_seq, with_overstow, osw_quantity, osw_weight, osw_sources)
        
        l_cg.append(cg_items)
    l_onboard_loadlist_sb_2_cg.append((sb, l_cg))

l_onboard_loadlist_sb_2_cg.sort(key=lambda x:x[0])


# ### writing the on-board + loadlist file in the form of container groups

os.chdir(REP_DATA_2)
f_cg_onboard_loadlist = open(ONBOARD_LOADLIST_CG_FILE_NAME_ADD, 'w')

s_header = "Subbay;LoadPort;DischPort;" +\
           "Size;cType;cWeight;Height;"+\
           "AvgWeightInSubbay;QuantityInSubbay;WeightInSubbay;"+\
           "Overstow;OverstowPod;QuantityOverstow;WeightOverstow;SourcesOverstow\n"
f_cg_onboard_loadlist.write(s_header)
    
for (subbay, l_container_groups) in l_onboard_loadlist_sb_2_cg:
        
    for (cg, quantity_in_sb, weight_in_sb, 
         overstow_port_seq, with_overstow, osw_quantity, osw_weight, osw_sources) in l_container_groups:
        
        pol_seq = cg[0]
        pol_name = port_seq_2_name(pol_seq, LEG_NO)
        pod_seq = cg[1]
        pod_name = port_seq_2_name(pod_seq, LEG_NO)
        size = cg[2]
        c_type = cg[3]
        c_weight = cg[4]
        height = cg[5]
        #avg_weight = d_onboard_cg_avg_weight[cg]
        avg_weight_in_sb = 0.0
        if quantity_in_sb != 0:
            avg_weight_in_sb = weight_in_sb / quantity_in_sb
            
        # overstow items, depending on loadlist (old or new) or onboard
        s_overstow = ''
        s_overstow_pod = ''
        s_quantity_overstow = ''
        s_weight_overstow = ''
        s_sources_overstow = ""
        if with_overstow == True: # loadlist ou subbay
            s_overstow = 'X'
            if subbay != '':
                s_overstow_pod = port_seq_2_name(overstow_port_seq, LEG_NO)
            s_quantity_overstow = "%d" % osw_quantity
            s_weight_overstow = "%.1f" % osw_weight
            if subbay == '':
                for (sb_source, pol_source), (q_source, w_source) in osw_sources.items():
                    s_sources_overstow += "%s-%s-%d-%.3f" %\
                    (sb_source, port_seq_2_name(pol_source, LEG_NO), q_source, w_source)
                    s_sources_overstow += "|"
                if len(s_sources_overstow) > 0:
                    s_sources_overstow = s_sources_overstow[:-1]
        
        # writing
        s_line = "%s;%s;%s;%s;%s;%s;%s;%.3f;%d;%.1f;%s;%s;%s;%s;%s\n" %\
                (subbay, pol_name, pod_name,\
                 size, c_type, c_weight, height,\
                 avg_weight_in_sb, quantity_in_sb, weight_in_sb, 
                 s_overstow, s_overstow_pod, s_quantity_overstow, s_weight_overstow,
                 s_sources_overstow)
        f_cg_onboard_loadlist.write(s_line)
        
f_cg_onboard_loadlist.close()

# finally, writing the list of containers with respect to container groups


os.chdir(REP_DATA_2)
f_cg_containers_onboard_loadlist = open(ONBOARD_LOADLIST_CG_CONTAINERS, 'w')

s_header = "Container;LoadPort;DischPort;" +\
           "Size;cType;cWeight;Height;Empty;Revenue\n"
f_cg_containers_onboard_loadlist.write(s_header)

for cg, l_containers in d_container_groups_containers.items():
    
    # container groups
    pol_seq = cg[0]
    pol_name = port_seq_2_name(pol_seq, LEG_NO)
    pod_seq = cg[1]
    pod_name = port_seq_2_name(pod_seq, LEG_NO)
    size = cg[2]
    c_type = cg[3]
    c_weight = cg[4]
    height = cg[5]
    
    # each container
    for (container, empty) in l_containers:
        
        # get the revenue
        revenue = get_container_revenue(d_pol_pod_revenues, 
                                        pol_name, pod_name, size, c_type, height, empty)
    
        # writing
        s_line = "%s;%s;%s;%s;%s;%s;%s;%s;%.2f\n" %\
                (container, pol_name, pod_name,\
                 size, c_type, c_weight, height, empty, revenue)
        f_cg_containers_onboard_loadlist.write(s_line)
        
f_cg_containers_onboard_loadlist.close()      




