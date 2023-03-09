# -*- coding: utf-8 -*-
"""
Created on Wed Nov 17 16:46:58 2021

@author: 056757706
"""

# libraries
import os

###########################################################
#- Master = Simple rules (by bay) = Compliance Doc + Simple DG rules
#- Slot = Complex rules (by row and tier) = Complex DG Rules
    
master_slot = "master"
# master_slot = "slot"

# files and directories
REP_DATA = "c:/Projets/CCS/Vessel Stowage/Python Prétraitement"
DG_EXCLUSIONS_FILENAME = "9454450 DG Exclusions.csv"
DG_LOADLIST_FILENAME = "9454450 DG Loadlist.csv"
DG_LOADLIST_EXCLUSIONS_FILENAME = "9454450 DG Loadlist Exclusions.csv"

os.chdir(REP_DATA)

### Reading the Exclusions zones depending on the (macro-)category

##### Getting for each (macro-)category the list of exclusion zones

d_exclusions_by_category = {}
l_zones = []

f_exclusions = open(DG_EXCLUSIONS_FILENAME, 'r')
for no_line, line in enumerate(f_exclusions):
    
    line = line[:-1] # removing \n
    
    # header
    if no_line == 0:
        # take all items except first one (the title)
        l_zones = line.split(';')[1:]
        continue
        
    # lines for categories and exclusions
    l_items = line.split(';')
    category = l_items[0]
    l_exclusions = []
    for no_item, item in enumerate(l_items[1:]):
        if item == 'X':
            l_exclusions.append(l_zones[no_item])
    d_exclusions_by_category[category] = l_exclusions

f_exclusions.close()

### Getting the information container by container

#### Using the (macro-)categories defined by the vessel doc

# list of product UN number requiring an electrical protection grade > IIB T4
l_explosion_protect_IIB_T4 = [
    '1038', '1040', '1041', '1135', '1153', '1171', '1172', '1184', '1185', '1188', '1189',
    '1604', '1605', '1952', '1962', '2983', '3070', '3138', '3297', '3298', '3299', '3300',
    '1001', '3374',
    '1132',
    '1048', '1049', '1050', '1051', '1052', '1053', '1614', '1740', '1966',
    '2014', '2015', '2034', '2186', '2197', '2202', '2984', '3149', '3294', '3468', '3471', '3526'
]

# getting the DG macro category depending on elements provided by the load list
def get_DG_category(un_no, imdg_class, sub_label,
                    as_closed, liquid, solid, flammable, flash_point):
    
    if (imdg_class == '1.4S')\
    or (imdg_class == '6.1' and solid == True and as_closed == True)\
    or (imdg_class == '8' and liquid == True and flash_point is None)\
    or (imdg_class == '8' and solid == True)\
    or (imdg_class == '9' and as_closed == True):
        return 'PPP'
    
    if (imdg_class == '2.2')\
    or (imdg_class == '2.3' and flammable == False and as_closed == True)\
    or (imdg_class == '3' and flash_point >= 23 and flash_point <= 60)\
    or (imdg_class == '4.1' and as_closed == True)\
    or (imdg_class == '4.2' and as_closed == True)\
    or (imdg_class == '4.3' and liquid == True and as_closed == True and (flash_point >= 23 or flash_point is None))\
    or (imdg_class == '4.3' and solid == True and as_closed == True)\
    or (imdg_class == '5.1' and as_closed == True)\
    or (imdg_class == '8' and liquid == True and as_closed == True and flash_point >= 23 and flash_point <= 60):
        return 'PP'
    
    if (imdg_class == '6.1' and liquid == True and flash_point is None):
        return 'PX'
    
    if (imdg_class == '6.1' and liquid == True and flash_point >= 23 and flash_point <= 60 and as_closed == True):
        return 'PX+'
    
    if (imdg_class == '2.1' and as_closed == True and un_no not in l_explosion_protect_IIB_T4)\
    or (imdg_class == '2.3' and flammable == True and as_closed == True and un_no not in l_explosion_protect_IIB_T4 and sub_label != '2.1')\
    or (imdg_class == '3' and flash_point < 23 and as_closed == True and un_no not in l_explosion_protect_IIB_T4)\
    or (imdg_class == '6.1' and liquid == True and flash_point < 23 and as_closed == True and un_no not in l_explosion_protect_IIB_T4)\
    or (imdg_class == '8' and liquid == True and flash_point < 23 and as_closed == True and un_no not in l_explosion_protect_IIB_T4):
        return 'XP'
    
    if (imdg_class == '6.1' and solid == True and as_closed == False)\
    or (imdg_class == '9' and as_closed == False):
        return 'XX-'
    
    if (imdg_class == '2.1' and (as_closed == False or un_no in l_explosion_protect_IIB_T4))\
    or (imdg_class == '2.3' and flammable == True and (as_closed == False or un_no in l_explosion_protect_IIB_T4 or sub_label == '2.1'))\
    or (imdg_class == '2.3' and flammable == False and as_closed == False)\
    or (imdg_class == '3' and flash_point < 23 and (as_closed == False or un_no in l_explosion_protect_IIB_T4))\
    or (imdg_class == '4.1' and as_closed == False)\
    or (imdg_class == '4.2' and as_closed == False)\
    or (imdg_class == '4.3' and liquid == True and (as_closed == False or flash_point < 23))\
    or (imdg_class == '4.3' and solid == True and as_closed == False)\
    or (imdg_class == '5.1' and as_closed == False)\
    or (imdg_class == '5.2')\
    or (imdg_class == '6.1' and liquid == True and flash_point < 23 and (as_closed == False or un_no in l_explosion_protect_IIB_T4))\
    or (imdg_class == '6.1' and liquid == True and flash_point >= 23 and flash_point <= 60 and as_closed == False)\
    or (imdg_class == '8' and liquid == True and flash_point < 23 and (as_closed == False or un_no in l_explosion_protect_IIB_T4))\
    or (imdg_class == '8' and liquid == True and flash_point >= 23 and flash_point <= 60 and as_closed == False):
        return 'XX'
        
    if (imdg_class[0] == '1' and imdg_class != '1.4S'):
        return 'XX+'
        
    if (imdg_class == '6.2')\
    or (imdg_class == '7'):
        return 'XXX'
    
    return '?'

def test_DG_category(un_no, imdg_class, sub_label,
                    as_closed, liquid, solid, flammable, flash_point):
    
    if (imdg_class == '1.4S'): return 1
    if (imdg_class == '6.1' and solid == True and as_closed == True): return 2
    if (imdg_class == '8' and liquid == True and flash_point is None): return 3
    if (imdg_class == '8' and solid == True): return 4
    if (imdg_class == '9' and as_closed == True): return 5
    
    if (imdg_class == '2.2'): return 6
    if (imdg_class == '2.3' and flammable == False and as_closed == True): return 7
    if (imdg_class == '3' and flash_point >= 23 and flash_point <= 60): return 8
    if (imdg_class == '4.1' and as_closed == True): return 9
    if (imdg_class == '4.2' and as_closed == True): return 10
    if (imdg_class == '4.3' and liquid == True and as_closed == True and (flash_point >= 23 or flash_point is None)): return 11
    if (imdg_class == '4.3' and solid == True and as_closed == True): return 12
    if (imdg_class == '5.1' and as_closed == True): return 13
    if (imdg_class == '8' and liquid == True and as_closed == True and flash_point >= 23 and flash_point <= 60): return 14
    
    if (imdg_class == '6.1' and liquid == True and flash_point is None): return 15
    
    if (imdg_class == '6.1' and liquid == True and flash_point >= 23 and flash_point <= 60 and as_closed == True): return 16
    
    if (imdg_class == '2.1' and as_closed == True and un_no not in l_explosion_protect_IIB_T4): return 17
    if (imdg_class == '2.3' and flammable == True and as_closed == True and un_no not in l_explosion_protect_IIB_T4 and sub_label != '2.1'): return 18
    if (imdg_class == '3' and flash_point < 23 and as_closed == True and un_no not in l_explosion_protect_IIB_T4): return 19
    if (imdg_class == '6.1' and liquid == True and flash_point < 23 and as_closed == True and un_no not in l_explosion_protect_IIB_T4): return 20
    if (imdg_class == '8' and liquid == True and flash_point < 23 and as_closed == True and un_no not in l_explosion_protect_IIB_T4): return 21
    
    if (imdg_class == '6.1' and solid == True and as_closed == False): return 22
    if (imdg_class == '9' and as_closed == False): return 23
    
    if (imdg_class == '2.1' and (as_closed == False or un_no in l_explosion_protect_IIB_T4)): return 24
    if (imdg_class == '2.3' and flammable == True and (as_closed == False or un_no in l_explosion_protect_IIB_T4 or sub_label == '2.1')): return 25
    if (imdg_class == '2.3' and flammable == False and as_closed == False): return 26
    if (imdg_class == '3' and flash_point < 23 and (as_closed == False or un_no in l_explosion_protect_IIB_T4)): return 27
    if (imdg_class == '4.1' and as_closed == False): return 28
    if (imdg_class == '4.2' and as_closed == False): return 29
    if (imdg_class == '4.3' and liquid == True and (as_closed == False or flash_point < 23)): return 30
    if (imdg_class == '4.3' and solid == True and as_closed == False): return 31
    if (imdg_class == '5.1' and as_closed == False): return 32
    if (imdg_class == '5.2'): return 33
    if (imdg_class == '6.1' and liquid == True and flash_point < 23 and (as_closed == False or un_no in l_explosion_protect_IIB_T4)): return 34
    if (imdg_class == '6.1' and liquid == True and flash_point >= 23 and flash_point <= 60 and as_closed == False): return 35
    if (imdg_class == '8' and liquid == True and flash_point < 23 and (as_closed == False or un_no in l_explosion_protect_IIB_T4)): return 36
    if (imdg_class == '8' and liquid == True and flash_point >= 23 and flash_point <= 60 and as_closed == False): return 37
        
    if (imdg_class[0] == '1' and imdg_class != '1.4S'): return 38
        
    if (imdg_class == '6.2'): return 39
    if (imdg_class == '7'): return 40
    
    return 1000

#### In addition to the (macro-)categories derived from the compliance doc, we have special exclusions zones linked to stowage categories
    
#### The general exclusion data takes the form:
#- bay (differentiating 20' and 40')
#- rows (most of the time it will be all rows, storing None, but it can be a list of rows)
#- tiers (first part, 0 (hold) or 1 (deck), second part, all tiers inside, storing None if all tiers, but it can be a list of tiers)
#- In order to include those in sets, lists of rows and of tiers are stored as frozensets
    
l_macro_bays = [2 + 4 * n for n in range(0, 24)]
# list of all bays on deck
l_deck_bays = l_macro_bays.copy()
l_deck_bays.extend([n-1 for n in l_macro_bays if n not in [74, 94]])
l_deck_bays.extend([n+1 for n in l_macro_bays if n not in [74, 94]])
l_deck_bays.sort()
# list of all bays in hold
l_hold_bays = [n for n in l_macro_bays if n not in [74, 94]]
l_hold_bays.extend([n-1 for n in l_macro_bays if n not in [74, 94]])
l_hold_bays.extend([n+1 for n in l_macro_bays if n not in [74, 94]])
l_hold_bays.sort()

# list of all zones under the deck, those zones are forbidden for stowage categories C and D
l_hold_zones = ["%03d0" % n for n in l_hold_bays]

# SW1, quite complexe
l_sw_1 = [
    ('034', None, ('0', frozenset({'02', '04', '06', '08', '10', '12', '14', '16'}))),
    ('035', None, ('0', frozenset({'02', '04', '06', '08', '10', '12', '14', '16'}))),
    ('037', None, ('0', frozenset({'02', '04', '06', '08', '10', '12', '14', '16'}))),
    ('038', None, ('0', frozenset({'02', '04', '06', '08', '10', '12', '14', '16'}))),
    ('077', frozenset({'08', '09', '10', '11', '12', '13', '14', '15', '16'}), ('0', frozenset({'10'}))),
    ('078', frozenset({'08', '09', '10', '11', '12', '13', '14', '15', '16'}), ('0', frozenset({'10'}))),
    ('079', frozenset({'08', '09', '10', '11', '12', '13', '14', '15', '16'}), ('0', frozenset({'10'})))
]

# SW2, forbid a simple list
l_sw_2 = ['0341', '0351', '0371', '0381']

# polmar,
# exclude all positions for bays 1, 2, 94 on the dock
l_polmar = ['0011', '0021', '0941']
# for other bays, from 3 to 91, only external rows
l_decks_polmar_extension = ["%03d" % n for n in l_deck_bays if n not in [1,2,94]]
# bay 3 is a special case, external rows are 17 and 18 instead of 19 and 20
# and it is the first in the list from 3 to 91
l_rows_polmar_extension = [frozenset({'17', '18'})]
# all other bays
l_rows_polmar_extension.extend([frozenset({'19', '20'}) for n in l_deck_bays if n not in [1,2,3,94]])
                             
l_polmar.extend([(x[0], x[1], ('1', None)) for x in zip(l_decks_polmar_extension, l_rows_polmar_extension)])

def expand_exclusion(set_exclusions, l_updates):
    
    # taking update elements one by one
    for update in l_updates:
        
        # if update is a simple string, it is the string bbbT, bbb bay number, T macro-tier 0 or 1
        if type(update) == str:
            # look if there is already an element concerning this macro combination in the set, if yes remove it
            bay = update[0:3]
            macro_tier = update[3]
            for exclusion in set_exclusions:
                if exclusion[0] == bay and exclusion[2][0] == macro_tier:
                    set_exclusions.remove(exclusion)
                    break
            # and in any case create it, the exclusion is on the totality of the bay + macro-tier
            set_exclusions.add((bay, None, (macro_tier, None)))
            
        # if update is a triplet, before adding it, verify it is not already covered by an existing one
        if type(update) == tuple:
            bay = update[0]
            l_rows = update[1]
            macro_tier = update[2][0]
            l_tiers = update[2][1]
            # we should refine, but it is useless, at least for the time being
            # so integrate if the total coverage of the bay + macro tier is not already existing
            integration = True
            for exclusion in set_exclusions:
                if exclusion[0] == bay and exclusion[2][0] == macro_tier:
                    if exclusion[1] is None and exclusion[2][1] is None:
                        integration = False
                        break
            if integration == True:
                set_exclusions.add(update)
    
    return set_exclusions

### Creating a dictionnary for each container x loading port, containing the set of excluded zones
    
##### Depending on creating for master planning, only doc, and only at bay level, or for slot planning, with all exclusions
#- Master = Simple rules (by bay) = Compliance Doc + Simple DG rules
#- Slot = Complex rules (by row and tier) = Complex DG Rules

d_containers_exclusions = {}
test_set = set() 

f_dg_loadlist = open(DG_LOADLIST_FILENAME, 'r')
for no_line, line in enumerate(f_dg_loadlist):
    
    line = line[:-1] # removing \n
    
    # skipping the header
    if no_line == 0: continue
        
    l_items = line.split(';')
        
    # getting columns of interest
    container_id = l_items[0].strip()
    pol = l_items[2].strip()
    pod = l_items[3].strip()
    # adapt pol to SOU2 if needed
    
    s_closed_freight_container = l_items[5].strip()
    un_no = l_items[7].strip()
    imdg_class = l_items[8].strip()
    sub_label = l_items[9].strip()
    dg_remark = l_items[10].strip()
    s_flash_point = l_items[11].strip()
    s_polmar = l_items[14].strip()
    pgr = l_items[15].strip()
    s_liquid = l_items[16].strip()
    s_solid = l_items[17].strip()
    s_flammable = l_items[18].strip()
    s_non_flammable = l_items[19].strip()
    shipping_name = l_items[20].strip()
    s_stowage_segregation = l_items[23].strip()
    s_package_goods = l_items[24].strip()
    stowage_category = l_items[25].strip()
    
    # container identification
    if (container_id, pol) not in d_containers_exclusions:
        d_containers_exclusions[(container_id, pol)] = set()
    
    # getting the corresponding macro-category
    
    # transforming and combining some items
    
    # for remark a), dg_remark could have been used as well
    as_closed = True
    # but maybe useless
    #if s_stowage_segregation.find("SW5") >= 0 and s_closed_freight_container != 'x':
    if s_closed_freight_container != 'x':
        as_closed = False
    
    # for remark b)
    # ...
    
    liquid = True if s_liquid == 'x' else False
    solid = True if s_solid == 'x' else False
    if solid == False and liquid == False: solid = True
    flammable = True if s_flammable == 'x' else False
    
    polmar = True if s_polmar == 'yes' else False
    sw_1 = True if s_stowage_segregation.find("SW1") >= 0 else False
    sw_2 = True if s_stowage_segregation.find("SW2") >= 0 else False
    
    flash_point = None
    if len(s_flash_point) > 0:
        s_flash_point = s_flash_point[:-1]
        flash_point = float(s_flash_point)
    
    test = test_DG_category(un_no, imdg_class, sub_label,
                            as_closed, liquid, solid, flammable, flash_point)
    test_set.add(test)
    
    category = get_DG_category(un_no, imdg_class, sub_label,
                               as_closed, liquid, solid, flammable, flash_point)
    #print(container_id, pol, category)
    #if category == '?':
    #    print(un_no, imdg_class, sub_label,
    #          as_closed, liquid, solid, flammable, flash_point)
    
    # and use the macro-category to expand the set of forbidden zones
    d_containers_exclusions[(container_id, pol)] = expand_exclusion(d_containers_exclusions[(container_id, pol)], 
                                                                    d_exclusions_by_category[category])
    
    # plus, if necessary, the stowage conditions (no C or D on hold, whatever the circumstances)
    # and SW1, SW2 and polmar
    # depending on if for slot planning or not
    
    if stowage_category in ['C', 'D']:
        d_containers_exclusions[(container_id, pol)] = expand_exclusion(d_containers_exclusions[(container_id, pol)],
                                                                        l_hold_zones)
    if sw_2 == True:
        d_containers_exclusions[(container_id, pol)] = expand_exclusion(d_containers_exclusions[(container_id, pol)],
                                                                        l_sw_2)
    
    if master_slot == "slot":       
        if sw_1 == True:
            d_containers_exclusions[(container_id, pol)] = expand_exclusion(d_containers_exclusions[(container_id, pol)],
                                                                            l_sw_1)     
        if polmar == True:
            d_containers_exclusions[(container_id, pol)] = expand_exclusion(d_containers_exclusions[(container_id, pol)],
                                                                            l_polmar)
        
    #print(len(d_containers_exclusions[(container_id, pol)]))
    
f_dg_loadlist.close()


### Write the file of exclusions

# write the file, as a .csv file
f_loadlist_exclusions = open(DG_LOADLIST_EXCLUSIONS_FILENAME, 'w')

if master_slot == "master":
    # header
    header = "ContId;LoadPort;Bay;MacroTier\n"
    f_loadlist_exclusions.write(header)
    # ordinary rows
    for ((container_id, pol), s_exclusions) in d_containers_exclusions.items():
        for (bay, l_rows, (macro_tier, l_tiers)) in s_exclusions:
            line = "%s;%s;%s;%s\n" % (container_id, pol, bay[1:3], macro_tier)
            f_loadlist_exclusions.write(line)
            
if master_slot == "slot":
    # header
    header = "ContId;LoadPort;Bay;Row;MacroTier;Tier\n"
    f_loadlist_exclusions.write(header)
    # ordinary rows
    for ((container_id, pol), s_exclusions) in d_containers_exclusions.items():
        for (bay, l_rows, (macro_tier, l_tiers)) in s_exclusions:
            if l_rows is None:
                if l_tiers is None:
                    line = "%s;%s;%s;;%s;\n" % (container_id, pol, bay[1:3], macro_tier)
                    f_loadlist_exclusions.write(line)
                else:
                    for tier in l_tiers:
                        line = "%s;%s;%s;;%s;%s\n" % (container_id, pol, bay[1:3], macro_tier, tier)
                        f_loadlist_exclusions.write(line)
            else:
                for row in l_rows:
                    if l_tiers is None:
                        line = "%s;%s;%s;%s;%s;\n" % (container_id, pol, bay[1:3], row, macro_tier)
                        f_loadlist_exclusions.write(line)
                    else:
                        for tier in l_tiers:
                            line = "%s;%s;%s;%s;%s;%s\n" % (container_id, pol, bay[1:3], row, macro_tier, tier)
                            f_loadlist_exclusions.write(line)
            
f_loadlist_exclusions.close()




















