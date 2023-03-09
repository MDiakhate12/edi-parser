# libraries
import os

import vessel_stow_kpi as vsk

# directory and file names
##################
# TO BE MODIFIED

REP_DATA_VESSEL = "c:/Projets/CCS/Vessel Stowage/Modèles/Data"
fn_stacks = "Stacks Extrait Prototype MP_IN.csv"

REP_DATA = "c:/Projets/CCS/Vessel Stowage/Python Prétraitement"
CONTAINER_GROUP_CONTAINERS_FILENAME = "9454450 Container Groups Containers.csv"
DG_LOADLIST_EXCLUSIONS_FILENAME = "9454450 DG Loadlist Exclusions.csv"

DG_CG_EXCLUSION_ZONES_FILENAME = "9454450 DG Container Groups Exclusion Zones.csv"
DG_CG_EXCLUSION_ZONES_NB_DG_FILENAME = "9454450 DG Container Groups Exclusion Zones Nb DG.csv"

##################

# Preliminaries : get for each bay x macro-tier combination the set of relevant subbays

os.chdir(REP_DATA_VESSEL)

d_stacks = vsk.get_stacks_capacities(fn_stacks)
d_bay_macro_tier_l_subbays = vsk.get_bays_macro_tiers_l_subbays(d_stacks)

# Reading the exclusions by containers and their membership in CG

# Create exclusion zones by container groups

#### The individual container is a couple (container, POL) :
# - we first get their container group in the container group file
# - we get the individual exclusion area (bay x macro-tier) 
# in the containers exclusion zones file (with a parameter set to 'master', not 'slot')
# - the unions of those indivicual areas create a set of exclusion zones, 
# - for each container group, we list the relevant exclusion zones

os.chdir(REP_DATA)

# get container group for each container
d_container_2_container_group = {}

f_containers = open(CONTAINER_GROUP_CONTAINERS_FILENAME, 'r')
for no_ligne, ligne in enumerate(f_containers):
    if no_ligne == 0: continue
    l_items = ligne.split(';')
    container = l_items[0]
    load_port_name = l_items[1]
    disch_port_name = l_items[2]
    size = l_items[3]
    c_type = l_items[4]
    c_weight = l_items[5]
    height = l_items[6]
    cg = (load_port_name, disch_port_name, size, c_type, c_weight, height)
    d_container_2_container_group[(container, load_port_name)] = cg
f_containers.close()

# get exclusion zones (set of areas) for each container
d_container_2_exclusion_zone = {}

f_loadlist_exclusions = open(DG_LOADLIST_EXCLUSIONS_FILENAME, 'r')
for no_ligne, ligne in enumerate(f_loadlist_exclusions):
    if no_ligne == 0: continue
    l_items = ligne.split(';')
    container = l_items[0]
    load_port_name = l_items[1]
    bay = l_items[2]
    macro_tier = l_items[3][:-1]
    # beware, in that file, some GBSOU refer to GBSOU2 !!
    if load_port_name == 'GBSOU' and (container, load_port_name) not in d_container_2_container_group:
        load_port_name = 'GBSOU2'
    # normal process
    if (container, load_port_name) not in d_container_2_exclusion_zone:
        d_container_2_exclusion_zone[(container, load_port_name)] = set()
    d_container_2_exclusion_zone[(container, load_port_name)].add((bay, macro_tier))
    
f_loadlist_exclusions.close()

# get set of zones as such, all container groups considered together
s_zones = set()
for (container, load_port_name), zone in d_container_2_exclusion_zone.items():
    s_zones.add(frozenset(zone))
l_zones = list(s_zones)

#print(len(l_zones))
#print('')
#for no_zone, zone in enumerate(l_zones):
#    zone_l = list(zone)
#    zone_l.sort()
#    print(no_zone, zone_l)
#    print('')

# get for each container the exclusion zone index in that list
d_container_2_ix_exclusion_zone = {}
for (container, load_port_name), container_zone in d_container_2_exclusion_zone.items():
    ix_zone = -1
    for ix, zone in enumerate(l_zones):
        if zone == container_zone:
            ix_zone = ix
            break
    d_container_2_ix_exclusion_zone[(container, load_port_name)] = ix_zone
    
#print(d_container_2_ix_exclusion_zone)  
    
# now, list exclusion zones for each container group, and count corresponding containers 
d_cg_2_ix_exclusion_zones = {}

for (container, load_port_name), ix_zone in d_container_2_ix_exclusion_zone.items():
    # contrôle de cohérence
    if (container, load_port_name) not in d_container_2_container_group:
        print((container, load_port_name))

    cg = d_container_2_container_group[(container, load_port_name)]
    if cg not in d_cg_2_ix_exclusion_zones:
        d_cg_2_ix_exclusion_zones[cg] = {}
    if ix_zone not in d_cg_2_ix_exclusion_zones[cg]:
        d_cg_2_ix_exclusion_zones[cg][ix_zone] = 0
    d_cg_2_ix_exclusion_zones[cg][ix_zone] += 1     
    
#print(d_cg_2_ix_exclusion_zones)

###### for each container group, list areas (bay x macro-tier) for each combination 
# (intersection / inclusion) of zones

def list_areas_for_zone_intersections(d_ix_zones, l_zones):
    
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

# creation of the list of exclusion zones (including combinations) for each container group
d_cg_2_combi_zones = {}
for cg, d_ix_zones in d_cg_2_ix_exclusion_zones.items():
    d_combi_zones = list_areas_for_zone_intersections(d_ix_zones, l_zones)
    d_cg_2_combi_zones[cg] = d_combi_zones
    
#############
def get_zone_list_subbays(s_area, d_bay_macro_tier_l_subbays):
    
    s_area_subbays = set()
    for area in s_area:
        for subbay in d_bay_macro_tier_l_subbays[area]:
            if subbay not in s_area_subbays:
                s_area_subbays.add(subbay)
                
    return s_area_subbays

# at last, split bay x macro_tier area into subbays, while keeping the nb of containers data
d_cg_combi_subbays = {}

for cg, d_combi_zones in d_cg_2_combi_zones.items():
    d_combi_subbays = {}
    for s_combi_area, nb_containers in d_combi_zones.items():
        s_combi_subbays = get_zone_list_subbays(s_combi_area, d_bay_macro_tier_l_subbays)
        d_combi_subbays[frozenset(s_combi_subbays)] = nb_containers
    d_cg_combi_subbays[cg] = d_combi_subbays
    
#for cg, d_combi_subbays in d_cg_combi_subbays.items():
#    print(cg)
#    print('')
#    for s_combi_subbays, nb_containers in d_combi_subbays.items():
#        l_combi_subbays = list(s_combi_subbays)
#        l_combi_subbays.sort()
#        print(l_combi_subbays)
#        print(nb_containers)
#    print('')
#    print('')

# writing 2 files
# 1) definition of zones associated to each container group
# 2) nb of containers for those zones

f_cg_exclusion_zones = open(DG_CG_EXCLUSION_ZONES_FILENAME, 'w')
f_cg_exclusion_zones_nb_dg = open(DG_CG_EXCLUSION_ZONES_NB_DG_FILENAME, 'w')

s_header_zones = "LoadPort;DischPort;Size;cType;cWeight;Height;idZone;Subbay\n"
f_cg_exclusion_zones.write(s_header_zones)

s_header_nb_dg = "LoadPort;DischPort;Size;cType;cWeight;Height;idZone;NbDG\n"
f_cg_exclusion_zones_nb_dg.write(s_header_nb_dg)

for (load_port_name, disch_port_name, size, c_type, c_weight, height), d_combi_subbays in d_cg_combi_subbays.items():
    
    for ix, (s_combi_subbays, nb_containers) in enumerate(d_combi_subbays.items()):
        l_combi_subbays = list(s_combi_subbays)
        l_combi_subbays.sort()
        # writing zones
        for subbay in l_combi_subbays:
            s_ligne_zones = "%s;%s;%s;%s;%s;%s;%d;%s\n" %\
            (load_port_name, disch_port_name, size, c_type, c_weight, height, ix, subbay)
            f_cg_exclusion_zones.write(s_ligne_zones)
        # writing nb of containers
        s_ligne_nb_dg = "%s;%s;%s;%s;%s;%s;%d;%d\n" %\
        (load_port_name, disch_port_name, size, c_type, c_weight, height, ix, nb_containers)
        f_cg_exclusion_zones_nb_dg.write(s_ligne_nb_dg)
    
    
f_cg_exclusion_zones.close()
f_cg_exclusion_zones_nb_dg.close()




