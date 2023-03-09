# -*- coding: utf-8 -*-
"""
Created on Wed Nov 17 15:39:04 2021

@author: 056757706
"""

# files and directories
REP_DATA = "c:/Projets/CCS/Vessel Stowage/Python Prétraitement"
DG_ZONES_FILENAME = "9454450 DG Zones.csv"
DG_GROUPS_FILENAME = "9454450 DG Groups.csv"
DG_EXCLUSIONS_FILENAME = "9454450 DG Exclusions.csv"

# libraries
import os

# get data
os.chdir(REP_DATA)

# relationships between bays and zones
d_bay_2_zone = {}
d_zone_2_bays = {}

f_zones = open(DG_ZONES_FILENAME, 'r')
for no_line, line in enumerate(f_zones):
    
    if no_line == 0: continue
    l_items = line.split(';')
    
    # bays
    bays = l_items[0]
    l_bays = bays.split()
    # zone
    zone = l_items[1][:-1] #\n
    if len(zone) == 0: zone = None
        
    for bay in l_bays:
        d_bay_2_zone[bay] = zone
    if zone is not None:
        if zone not in d_zone_2_bays: d_zone_2_bays[zone] = []
        d_zone_2_bays[zone].extend(l_bays)
           
f_zones.close()

for zone in d_zone_2_bays:
    d_zone_2_bays[zone].sort()
    
l_deck_bays = [bay for bay in d_bay_2_zone]

# get groups of columns, with bay information
l_groups_columns = []
l_groups_columns_bays = []
l_all_bays_tiers = []
l_no_groups = []

l_dg_bays_exclusions = []

f_groups = open(DG_GROUPS_FILENAME, 'r')
for no_line, line in enumerate(f_groups):
    
    l_items = line[:-1].split(';') # /n
    
    ##### 1) header to get info from, the name of columns
    if no_line == 0:
        l_groups_columns_names = l_items[1:]
        
        # to each column of the grouped data, associate its baies + 0/1 tier, looking at the name
        for column_name in l_groups_columns_names:
            hull_deck = column_name[0]
            operand = column_name[1]
            s_location_names = column_name[2:]
            if s_location_names[0] == '(':
                l_locations = s_location_names[1:-1].split(",")
            else:
                l_locations = [s_location_names]
            l_groups_columns.append((hull_deck, operand, l_locations))
        
        #print("GROUP COLUMNS:")
        #print(l_groups_columns)
        
        # for each column, list its bays in the form bbbn, bbb bay number, n 0 if hold, 1 if deck
        for (hull_deck, operand, l_locations) in l_groups_columns:
            
            tier = 1 if hull_deck == 'D' else 0
            
            l_bays = []
            if operand == '-' and hull_deck == 'D':
                l_bays = l_deck_bays.copy()
            for location in l_locations:
                location_type = location[0]
                location_number = location[1:]
                if location_type == 'Z':
                    l_location_bays = d_zone_2_bays[location_number]
                if location_type == 'B':
                    l_location_bays = [location_number]
                if operand == '.':
                    l_bays.extend(l_location_bays)
                if operand == '-':
                    l_bays.remove(location_number)
            l_bays_tiers = ["%s%d" % (location_number, tier) for location_number in l_bays]
            l_groups_columns_bays.append(l_bays_tiers)
        
        #print("GROUP COLUMNS BAYS:")
        #print(l_groups_columns_bays)  
        
        # create a flat list of bays_tiers, sorted by bays_tiers, and pointing to their exclusion column numbers
        for no_group, l_bays_tiers in enumerate(l_groups_columns_bays):
            for bay_tier in l_bays_tiers:
                l_all_bays_tiers.append((bay_tier, no_group))
        l_all_bays_tiers.sort(key=lambda x:x[0])
        
        #print("ALL BAYS TIERS")
        #print(l_all_bays_tiers)
        
        for (bay_tier, no_group) in l_all_bays_tiers:
            l_no_groups.append(no_group)
        
        #print("NO GROUPS")
        #print(l_no_groups)
        
        continue
        
    #######2) content exclusion lines
    
    dg_category = l_items[0]
    l_exclusions = []
    for (bay_tier, no_group) in l_all_bays_tiers:
        exclusion = l_items[no_group+1]
        l_exclusions.append(exclusion)
    l_dg_bays_exclusions.append((dg_category, l_exclusions))

f_groups.close()

# write the file, as a .csv file
f_exclusions = open(DG_EXCLUSIONS_FILENAME, 'w')

# header
header = "DG category;"
for no_bay_tier, (bay_tier, no_group) in enumerate(l_all_bays_tiers):
    header += bay_tier
    if no_bay_tier < len(l_all_bays_tiers) - 1:
        header += ';'
    else:    
        header += '\n'
f_exclusions.write(header)
# ordinary rows
for (dg_category, l_exclusions) in l_dg_bays_exclusions:
    row = dg_category + ';'
    for no_exclusion, exclusion in enumerate(l_exclusions):
        row += exclusion
        if no_exclusion < len(l_exclusions) - 1:
            row += ';'
        else:
            row += '\n'
    f_exclusions.write(row)
    
f_exclusions.close()

