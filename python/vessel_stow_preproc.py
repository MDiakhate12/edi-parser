# -*- coding: utf-8 -*-
"""
Created on Fri Jul 30 14:47:08 2021

@author: 056757706
"""

# libraries
import os

import numpy as np
from scipy import interpolate
from scipy import integrate

###################
## Basic Functions
###################

##### get the trapeze from the (longitudinal) gravity center
# get the (weight) height and delta (slope) using all elements of the structure elements
def get_height_start_delta(pos_start, pos_end, pos_lcg, weight):
    
    # supposing first a uniform weight = 1
    
    # ls: pos_start
    # le: pos_end
    # L = le - ls
    # lg: pos_lcg
    # Lg: lg - ls
    # gamma = Lg / L
    # W = S: assuming density = 1, whole weight taken up by "heights" 
    # hs: height_pos_start
    # he: height_end_start
    # delta_h = he - hs
    # alpha = (he - hs) / hs
    # then:
    # 1) S = L . hs . (1 + alpha/2)
    # 2) gamma = (hs/2 + delta_h/3) / (hs + delta_h/2) = (1/2 + alpha/3) + (1 + alpha/2)
    # or: alpha = (gamma - 1/2) / (1/3 - gamma/2)
    # hence, using alpha: 
    # hs = S / L(1 + alpha/2)
    # delta_h = alpha . S / L(1 + alpha/2)
    
    # we can use both values to compute the mass m of a segment l1, l2 (between ls and le)
    # m = s = (l2 - l1) . (hs + ((l1 - ls) + 1/2 .(l2 -l1)) . delta_h / L)
    
    length = (pos_end - pos_start)
    gamma = (pos_lcg - pos_start) / (pos_end - pos_start)
    # if gamma = 2/3, infinite slop, take alpha = 1000
    if gamma == 2/3:
        alpha = 100000000
    else:
        alpha = (gamma - (1/2)) / ((1/3) - (gamma/2))
    height_start = weight / (length * (1 + (alpha/2)))
    delta_height = alpha * height_start
    
    return height_start, delta_height    


##### get weights and cg in a segment

# we must get the weight of the element in the segment
# weight : compute the mass / surface of the portion of a structure element between l1 and l2
# m = s = (l2 - l1) . (hs + ((l1 - ls) + 1/2 .(l2 -l1)) . delta_h / L)
def get_elem_weight_in_segment(seg_start, seg_end, elem_start, elem_end, height_start, delta_height):
    
    # not relevant cases
    if seg_end <= seg_start: return 0.0
    if seg_start > elem_end or seg_end < elem_start: return 0.0
    
    elem_length = elem_end - elem_start
    # just in case...
    if elem_length <= 0: return 0.0
    seg_length = seg_end - seg_start
    seg_height = height_start 
    seg_height += (delta_height / elem_length) * ((seg_start - elem_start) + (seg_length / 2))
    seg_weight = seg_length * seg_height
    
    return seg_weight

# get both weight and cg of element in segment
def get_elem_cg_weight_in_segment(seg_start, seg_end, elem_start, elem_end, height_start, delta_height):
    
    # not relevant cases
    if seg_end <= seg_start: return 0.0, 0.0
    if seg_start > elem_end or seg_end < elem_start: return 0.0, 0.0
    
    elem_length = elem_end - elem_start
    seg_length = seg_end - seg_start
    
    # apply the trapeze formula
    height_seg_start = height_start + (delta_height / elem_length) * (seg_start - elem_start)
    height_seg_end = height_start + (delta_height / elem_length) * (seg_end - elem_start)
    
    # in case of...
    if height_seg_start + height_seg_end <= 0:
        cg = seg_start + seg_length / 2
        weight = 0
        return cg, weight
        
    cg = seg_start
    cg += (seg_length / 3) * (height_seg_start + 2 * height_seg_end) / (height_seg_start + height_seg_end)
    
    weight = get_elem_weight_in_segment(seg_start, seg_end, elem_start, elem_end, height_start, delta_height)
    
    return cg, weight


# FUNCTION TO BE USED ONLY FOR WEIGHT ELEMS
# get the total weight of a segment, summing all elements
# for every elements, we loop within the segment, and look at the intersection with the element,
# if any, compute the weight of the element on the segment, and increment the segment with it
def get_segment_elems_weight(seg_start, seg_end, l_weight_elems):
    
    seg_weight = 0.0
    # element by element
    for w_elem in l_weight_elems:
    
        elem_start = w_elem[0]
        elem_end = w_elem[1]
        height_start = w_elem[2]
        delta_height = w_elem[3]
    
        # inside the segment
        if seg_start <= elem_end and seg_end >= elem_start:
            actual_seg_start = max([elem_start, seg_start])
            actual_seg_end = min([elem_end, seg_end])
            elem_seg_weight = get_elem_weight_in_segment(actual_seg_start, actual_seg_end, 
                                                         elem_start, elem_end, height_start, delta_height)
            seg_weight += elem_seg_weight
        
    # we have a factor 100 in the light weight elements, while reading the files, get it back
    # THIS SHOULD BE HANDLED DIFFERENTLY (OUTSIDE THIS FUNCTION)
    return 100 * seg_weight

# FUNCTION TO BE USED ONLY FOR WEIGHT ELEMS
# get gravity center and weight of every element in the segment
# and sum the weighted gravity centers
def get_segment_gravity_center(seg_start, seg_end, l_weight_elems):
    
    # get the list of cg and weight of all elements within the segment
    l_weight_cg_weight = []
    # element by element
    for w_elem in l_weight_elems:
    
        elem_start = w_elem[0]
        elem_end = w_elem[1]
        height_start = w_elem[2]
        delta_height = w_elem[3]
    
        # inside the segment
        if seg_start <= elem_end and seg_end >= elem_start:
            actual_seg_start = max([elem_start, seg_start])
            actual_seg_end = min([elem_end, seg_end])
            cg, weight = get_elem_cg_weight_in_segment(actual_seg_start, actual_seg_end,
                                                       elem_start, elem_end, height_start, delta_height)
            if weight > 0:
                l_weight_cg_weight.append((cg, weight))
    
    # get the final cg
    total_cg_weight = 0.0
    total_weight = 0.0
    for (cg, weight) in l_weight_cg_weight:
        total_cg_weight += cg * weight
        total_weight += weight
    
    # CG is ratio
    if total_weight > 0:
        cg = total_cg_weight / total_weight
    else:
        cg = (seg_start + seg_end) / 2
    return cg

# FUNCTION TO BE USED ONLY FOR WEIGHT ELEMS
def get_segments_weights_cg(l_segments, l_weight_elems):
    
    l_seg_weights_cg = []
    
    for segment in l_segments:
        id_seg = segment[0]
        seg_start = segment[1]
        seg_end = segment[2]
        
        seg_weight = get_segment_elems_weight(seg_start, seg_end, l_weight_elems)
        seg_cg = get_segment_gravity_center(seg_start, seg_end, l_weight_elems)
        
        l_seg_weights_cg.append((id_seg, seg_weight, seg_cg))
    
    return l_seg_weights_cg


##### get buoyancies
    
# construction de la fonction d'interpolation à 2D, sur drafts et abscisses
# kind, either 'linear' or 'cubic'
def build_area_interpolation_function(l_drafts_in_grid, l_x_in_grid, d_frame_area_by_draft, kind='cubic'):
    
    # table of area values for building interpolation
    m_area = np.zeros((len(l_drafts_in_grid), len(l_x_in_grid)), float)
    for no_draft, draft in enumerate(l_drafts_in_grid):
        l_x_area = d_frame_area_by_draft[draft]
        for no_x, (x, area) in enumerate(l_x_area):
            m_area[no_draft][no_x] = area
            
    # interpolation function
    v_x = np.array(l_x_in_grid)
    v_y = np.array(l_drafts_in_grid)
    m_z = m_area
    
    f_area_interpolation = interpolate.interp2d(v_x, v_y, m_z, kind=kind)
    
    return f_area_interpolation

# for the buoyancy, use the sea water density
SEA_WATER_DENSITY = 1.025

# get the buoyancy for a set of longitudinal positions (x)
# we should take into account the global trim and the draft to make comparisons with the reference data
# it is the integrale on points within the segment
# we take either both extremities or extremities + middle
def get_segment_buoyancy_bcg(l_x, 
                             condition_draft, condition_trim, vessel_length, vessel_middle, 
                             f_area_interpolation):
    
    l_points = []
    for x in l_x:
        
        # with changing draft
        draft = condition_draft - condition_trim * (x - vessel_middle) / vessel_length 
        # hence the areas (buoyancies)
        area = f_area_interpolation(x, draft)[0]           
    
        l_points.append((x, draft, area))
    
    #print(l_points)
    
    # now use simpson to get the buoyancy (and the bcg)
    v_x = np.array([x for (x, draft, area) in l_points])
    v_area = np.array([area for (x, draft, area) in l_points])
    v_x_area = np.array([x * area for (x, draft, area) in l_points])
    
    #print(v_area)
      
    seg_buoyancy = SEA_WATER_DENSITY * integrate.simps(v_area, v_x)
    seg_bcg = SEA_WATER_DENSITY * integrate.simps(v_x_area, v_x) / seg_buoyancy   # multiply then divide
    
    #print(seg_buoyancy)
    #print('****')
    
    return seg_buoyancy, seg_bcg 


def get_segments_buoyancies_bcg(l_segments, condition_draft, condition_trim, vessel_length, vessel_middle,
                                f_area_interpolation, nb_points):
    
    l_seg_buoyancies_bcg = []
    
    for segment in l_segments:
        id_seg = segment[0]
        seg_start = segment[1]
        seg_end = segment[2]
        len_seg = seg_end - seg_start
        
        # hence the x coordinates of the nb points
        l_x = []
        for s in range(nb_points):
            # nb points
            x = seg_start + (s * len_seg / (nb_points-1))
            l_x.append(x)
        
        seg_buoyancy, seg_bcg = get_segment_buoyancy_bcg(l_x, 
                                                         condition_draft, condition_trim, 
                                                         vessel_length, vessel_middle, 
                                                         f_area_interpolation)
        
        l_seg_buoyancies_bcg.append((id_seg, seg_buoyancy, seg_bcg))
    
    return l_seg_buoyancies_bcg


##### get shear forces and bending moments
    
def get_point_shear_force(pos_x, l_segments, l_seg_weights_cg, l_seg_buoyancies_bcg, reverse=False):
    
    shear_force = 0.0
    
    nb_segs = len(l_segments)
    seg_range = range(nb_segs)
    if reverse == True: seg_range = range(nb_segs-1,-1,-1)
        
    for no_seg in seg_range:
        
        segment = l_segments[no_seg]
        id_seg = segment[0]
        seg_start = segment[1]
        seg_end = segment[2]
        seg_len = segment[3]
        
        # from beginning to start if not reverse
        if reverse == False and seg_start > pos_x:
            continue
        # or the other way
        if reverse == True and seg_end < pos_x:
            continue
        
        # get elems
        seg_weight = l_seg_weights_cg[no_seg][1]
        seg_buoyancy = l_seg_buoyancies_bcg[no_seg][1]
        #print(no_seg, seg_len, seg_weight, seg_buoyancy)
        
        #shear_force += seg_len * (seg_weight - seg_buoyancy)
        signe = 1
        if reverse == True: signe = -1
        shear_force += signe * (seg_weight - seg_buoyancy)
        
    #print("->", shear_force)
    return shear_force    

def get_segments_shear_forces(l_segments, l_seg_weights_cg, l_seg_buoyancies_bcg, reverse=False):
    
    l_seg_shear_forces = []
    
    for segment in l_segments:
        id_seg = segment[0]
        pos_x = segment[1]
        
        seg_shear_force = get_point_shear_force(pos_x, l_segments, l_seg_weights_cg, l_seg_buoyancies_bcg,
                                                reverse)
        
        l_seg_shear_forces.append((id_seg, seg_shear_force))
    
    l_seg_shear_forces.sort(key=lambda x: x[0])
    return l_seg_shear_forces


def get_point_bending_moment(pos_x, l_segments, l_seg_shear_forces, reverse=False, integration='R'):
    
    bending_moment = 0.0
    
    nb_segs = len(l_segments)
    seg_range = range(nb_segs)
    if reverse == True: seg_range = range(nb_segs-1,-1,-1)
        
    # Simpson
    l_x = []
    l_bm = []
        
    for no_seg in seg_range:
        
        
        segment = l_segments[no_seg]
        id_seg = segment[0]
        seg_start = segment[1]
        seg_end = segment[2]
        seg_len = segment[3]
        
        # from beginning to start if not reverse
        if reverse == False and seg_start > pos_x:
            continue
        # or the other way
        if reverse == True and seg_end < pos_x:
            continue
        
        # get elems
        seg_shear_force = l_seg_shear_forces[no_seg][1]
        
        signe = 1
        if reverse == True: signe = -1
        if integration == 'R':
            bending_moment += signe * seg_len * seg_shear_force
        
        if integration == 'S':
            l_x.append(seg_start + (0.5 * seg_len))
            l_bm.append(seg_shear_force)
    
    # Simpson
    if integration == 'S':
        v_x = np.array(l_x)
        v_bm = np.array(l_bm)
        bending_moment = integrate.simps(v_bm, v_x)
    
    return bending_moment   


def get_segments_bending_moments(l_segments, l_seg_shear_forces, reverse=False, integration='R'):
    
    l_seg_bending_moments = []
    
    for segment in l_segments:
        id_seg = segment[0]
        pos_x = segment[1]
        
        seg_bending_moment = get_point_bending_moment(pos_x, l_segments, l_seg_shear_forces,
                                                      reverse, integration)
        
        l_seg_bending_moments.append((id_seg, seg_bending_moment))
    
    l_seg_bending_moments.sort(key=lambda x: x[0])
    
    return l_seg_bending_moments


###### Get SF and BM enveloppe values 
    
# get (and at extremities, compute) the SF and BM values
def get_enveloppe_max_min_back_front(first_frame, last_frame, d_enveloppe_by_frame, l_enveloppe,
                                     pos_start, pos_end, l_blocks):
    
    # get (and at extremities, compute) the SF and BM values
    if first_frame in d_enveloppe_by_frame:
        max_back = d_enveloppe_by_frame[first_frame][1]
        min_back = d_enveloppe_by_frame[first_frame][0]
    # we don't check here the fit between points on SF / BM values and block frames
    # only extrapolation for the beginning
    else:
        # we need first and second values in the list
        pos_0 = pos_start
        pos_1 = pos_end
        pos_2 = l_blocks[1][2]
        lim_sup_1 = l_enveloppe[0][1][1]
        lim_sup_2 = l_enveloppe[1][1][1]
        max_back = lim_sup_1 + ((lim_sup_2 - lim_sup_1) * (pos_0 - pos_1) / (pos_2 - pos_1))
        if max_back < 0: max_back = 0
        lim_inf_1 = l_enveloppe[0][1][0]
        lim_inf_2 = l_enveloppe[1][1][0]
        min_back = lim_inf_1 + ((lim_inf_2 - lim_inf_1) * (pos_0 - pos_1) / (pos_2 - pos_1))
        if min_back > 0: min_back = 0
        
    if last_frame+1 in d_enveloppe_by_frame:
        max_front = d_enveloppe_by_frame[last_frame+1][1]
        min_front = d_enveloppe_by_frame[last_frame+1][0]
    else:
        # we need last and last before last values in the list
        nb_blocks = len(l_blocks)
        nb_in_enveloppe = len(l_enveloppe)
        pos_n = pos_end
        pos_n_1 = pos_start
        pos_n_2 = l_blocks[nb_blocks-2][1] # pos_start of before last block, 2 because of python start 0
        lim_sup_n_1 = l_enveloppe[nb_in_enveloppe-1][1][1]
        lim_sup_n_2 = l_enveloppe[nb_in_enveloppe-2][1][1]
        max_front = lim_sup_n_1 + ((lim_sup_n_1 - lim_sup_n_2) * (pos_n - pos_n_1) / (pos_n_1 - pos_n_2))
        if max_front < 0: max_front = 0
        lim_inf_n_1 = l_enveloppe[nb_in_enveloppe-1][1][0]
        lim_inf_n_2 = l_enveloppe[nb_in_enveloppe-2][1][0]
        min_front = lim_inf_n_1 + ((lim_inf_n_1 - lim_inf_n_2) * (pos_n - pos_n_1) / (pos_n_1 - pos_n_2))
        if min_front > 0: min_front = 0
              
    return max_back, max_front, min_back, min_front

###### Get the GM minimum having the draft and the GM min curve
    
def get_gm_min(draft, l_gm_min_curve):
    
    for (seg_start, seg_end, gm_min_start, slope) in l_gm_min_curve:
        if draft >= seg_start and draft < seg_end:
            gm_min = gm_min_start + (draft - seg_start) * slope
            return(gm_min)
        else:
            continue
    
    return(0.0)
    
#############################   
## Loading static data files
###############################
    
def read_frames(frames_filename):

    # reading frames
    # keeping start and end positions is enough
    # in two structures
    d_frames = {}
    l_frames = []

    f_frames = open(frames_filename, 'r')
    for no_line, line in enumerate(f_frames):
        if no_line == 0: continue
        l_items = line.split(';')
        frame_no = int(l_items[0])
        frame_start = float(l_items[2])
        frame_end = float(l_items[3])
        d_frames[frame_no] = (frame_start, frame_end)
        l_frames.append((frame_no, frame_start, frame_end))
    f_frames.close()

    # just to be sure (re)sort the list
    l_frames.sort(key=lambda x: x[0])
    
    return d_frames, l_frames


# reading the lightweight elements, and doing so, get the trapeze elements, in terms
# of initial (weight) height start and slope
# one element is then characterized by pos_start, pos_end, height_start, delta_height
def read_lightweight_elems(lightweight_filename, include_deadweight, deadweight_constant_filename):

    l_lightweight_elems = []

    f_lightweight = open(lightweight_filename, 'r')
    for no_line, line in enumerate(f_lightweight):
        if no_line == 0: continue
        l_items = line.split(';')
        pos_start = float(l_items[0])
        pos_end = float(l_items[1])
        pos_lcg = float(l_items[2])
        # just to have units with same size order and regular trapezes
        # This 100 will be canceled in function get_segment_elems_weight
        weight = float(l_items[3]) / 100.0
        height_start, delta_height = get_height_start_delta(pos_start, pos_end, pos_lcg, weight)
        l_lightweight_elems.append((pos_start, pos_end, height_start, delta_height))
    f_lightweight.close()
    
    # Also adding - IF NEEDED - the contant elems associated to the deadweight
    if include_deadweight == True:
        f_lightweight = open(deadweight_constant_filename, 'r')
        for no_line, line in enumerate(f_lightweight):
            if no_line == 0: continue
            l_items = line.split(';')
            pos_start = float(l_items[0])
            pos_end = float(l_items[1])
            pos_lcg = float(l_items[2])
            # just to have units with same size order and regular trapezes
            # This 100 will be canceled in function get_segment_elems_weight
            weight = float(l_items[3]) / 100.0
            height_start, delta_height = get_height_start_delta(pos_start, pos_end, pos_lcg, weight)
            l_lightweight_elems.append((pos_start, pos_end, height_start, delta_height))
        f_lightweight.close()
    
    return l_lightweight_elems


# reading the blocks, and getting start and end position
def read_blocks(blocks_filename, d_frames):

    l_blocks = []
    # no need for a dictionary, no_block starts at 1, add 1 to index if necessary

    f_blocks = open(blocks_filename, 'r')
    for no_line, line in enumerate(f_blocks):
        if no_line == 0: continue
        l_items = line.split(';')
        block_no = int(l_items[0])
        first_frame = int(l_items[1])
        last_frame = int(l_items[2])
        pos_start = d_frames[first_frame][0]
        pos_end = d_frames[last_frame][1]
        # get also, if making sense, the geometrical middle of the container stacks
        # i.e. the gravity center for the container bays
        # either (and rarely) directly in the file
        pos_bay_xcg = None
        if l_items[5] != "":
            pos_bay_xcg = float(l_items[5])
        # or from the bay frames    
        else:
            if l_items[6] != "" and l_items[7] != "":
                first_bay_frame = int(l_items[6])
                last_bay_frame = int(l_items[7])
                pos_bay_xcg = 0.5 * (d_frames[first_bay_frame][0] + d_frames[last_bay_frame][1])
        no_bay = l_items[3]
        coeff_bay = float(l_items[4])
    
        l_blocks.append((block_no, pos_start, pos_end, first_frame, last_frame, pos_bay_xcg, no_bay, coeff_bay))
    f_blocks.close()

    # just to be sure (re)sort the list
    l_blocks.sort(key=lambda x: x[0])

    # we need also the reverse, from a frame, give to which (unique) block it belongs
    d_frames_block = {}
    for (block_no, pos_start, pos_end, first_frame, last_frame, pos_bay_xcg, no_bay, coeff_bay) in l_blocks:
        for no_frame in range(first_frame, last_frame+1):
            d_frames_block[no_frame] = block_no
            
    return l_blocks, d_frames_block


###### Hydrostatics files, either by moulded draft every 0.5 m, or by extreme draft every 0.05 m
    
def read_hydrostatics(hydrostatics_precision, hydrostatics_gross_filename, hydrostatics_fine_filename):
    
    # the best structure is to have one dictionary of trim levels 
    # with inside a dictionary by draft
    d_hydrostatics_by_trim = {}
    
    if hydrostatics_precision == 0.5:
        f_hydrostatics = open(hydrostatics_gross_filename, 'r')

        for no_line, line in enumerate(f_hydrostatics):
            if no_line == 0: continue
            l_items = line.split(';')
            trim = float(l_items[0])
            if trim not in d_hydrostatics_by_trim:
                d_hydrostatics_by_trim[trim] = {}
            T = float(l_items[1])
            TK = float(l_items[2])
            DISP = float(l_items[3])
            LCF = float(l_items[4])
            LCB = float(l_items[5])
            VCB = float(l_items[6])
            TPC = float(l_items[7])
            MCT = float(l_items[8])
            KMT = float(l_items[9])
            d_hydrostatics_by_trim[trim][T] = (TK, DISP, LCF, LCB, VCB, TPC, MCT, KMT)
        
        f_hydrostatics.close()
        
    if hydrostatics_precision == 0.05:
        f_hydrostatics = open(hydrostatics_fine_filename, 'r')

        for no_line, line in enumerate(f_hydrostatics):
            if no_line == 0: continue
            l_items = line.split(';')
            trim = float(l_items[0])
            if trim not in d_hydrostatics_by_trim:
                d_hydrostatics_by_trim[trim] = {}
            TK = float(l_items[1])
            T = float(l_items[2])
            DISP = float(l_items[3])
            LCF = float(l_items[4])
            LCB = float(l_items[5])
            VCB = float(l_items[6]) if l_items[6] != '' else None
            TPC = float(l_items[7]) if l_items[7] != '' else None
            MCT = float(l_items[8]) if l_items[8] != '' else None
            KMT = float(l_items[9])
            d_hydrostatics_by_trim[trim][TK] = (T, DISP, LCF, LCB, VCB, TPC, MCT, KMT)
        
        f_hydrostatics.close()
    
    
    return d_hydrostatics_by_trim


# reading the Bonjean area table
def read_bonjean_frame_area(bonjean_frame_area_filename):
    
    # the best structure is to have a dictionary of draft levels with inside a sorted list of (x, area)
    # we could also need a dictionary with double entry x then draft
    d_frame_area_by_draft = {}
    d_frame_area_by_x_draft = {}

    f_bonjean_frame_area = open(bonjean_frame_area_filename, 'r')

    for no_line, line in enumerate(f_bonjean_frame_area):
        if no_line == 0: continue
        l_items = line.split(';')
        x = float(l_items[0])
        draft = float(l_items[1])
        area = float(l_items[2])
    
        # by draft
        if draft not in d_frame_area_by_draft: d_frame_area_by_draft[draft] = []
        d_frame_area_by_draft[draft].append((x, area)) 
    
        # by x and then by draft
        if x not in d_frame_area_by_x_draft: d_frame_area_by_x_draft[x] = {}
        if draft not in d_frame_area_by_x_draft[x]: d_frame_area_by_x_draft[x][draft] = area
    
    f_bonjean_frame_area.close()

    for draft, l_x_area in d_frame_area_by_draft.items():
        l_x_area.sort(key=lambda x: x[0])
        
    # also...
    l_x_in_grid = [x for x in d_frame_area_by_x_draft]
    l_x_in_grid.sort(key=lambda x: x)
    l_drafts_in_grid = [draft for draft in d_frame_area_by_draft.keys()]
    l_drafts_in_grid.sort(key=lambda x: x)
    
    return d_frame_area_by_draft, d_frame_area_by_x_draft, l_x_in_grid, l_drafts_in_grid


# reading the Shear Forces enveloppe
def read_sf_enveloppe(sf_enveloppe_filename):

    d_sf_enveloppe_by_frame = {}

    f_sf_enveloppe = open(sf_enveloppe_filename, 'r')
    for no_line, line in enumerate(f_sf_enveloppe):
        if no_line == 0: continue
        l_items = line.split(';')
        frame_no = int(l_items[0])
        lim_inf = float(l_items[1])
        lim_sup = float(l_items[2])
        d_sf_enveloppe_by_frame[frame_no] = (lim_inf, lim_sup)

    f_sf_enveloppe.close()

    # for extrapolations, the list is better
    l_sf_enveloppe = [(frame_no, limits) for frame_no, limits in d_sf_enveloppe_by_frame.items()]
    l_sf_enveloppe.sort(key=lambda x: x[0])

    return d_sf_enveloppe_by_frame, l_sf_enveloppe

# reading the Bending Moments enveloppe
def read_bm_enveloppe(bm_enveloppe_filename):
    
    d_bm_enveloppe_by_frame = {}

    f_bm_enveloppe = open(bm_enveloppe_filename, 'r')
    for no_line, line in enumerate(f_bm_enveloppe):
        if no_line == 0: continue
        l_items = line.split(';')
        frame_no = int(l_items[0])
        lim_inf = float(l_items[1])
        lim_sup = float(l_items[2])
        d_bm_enveloppe_by_frame[frame_no] = (lim_inf, lim_sup)

    f_bm_enveloppe.close()

    # for extrapolations, the list is better
    l_bm_enveloppe = [(frame_no, limits) for frame_no, limits in d_bm_enveloppe_by_frame.items()]
    l_bm_enveloppe.sort(key=lambda x: x[0])
    
    return d_bm_enveloppe_by_frame, l_bm_enveloppe

# reading the GMin curve table
# keep it as a list of points
def read_gm_min_curve(gm_min_curve_filename):

    l_gm_min_points = []

    f_gm_min_curve = open(gm_min_curve_filename, 'r')

    for no_line, line in enumerate(f_gm_min_curve):
        if no_line == 0: continue
        l_items = line.split(';')
        draft = float(l_items[0])
        gm_min = float(l_items[1])
    
        l_gm_min_points.append((draft, gm_min))

    f_gm_min_curve.close()

    l_gm_min_points.sort(key=lambda x: x[0])
    
    # hence, get the intervals with linear coefficients
    l_gm_min_curve = []
    for no_point, point in enumerate(l_gm_min_points):
    
        if no_point == len(l_gm_min_points) - 1: break
    
        draft = point[0] 
        gm_min = point[1]
        next_draft = l_gm_min_points[no_point+1][0]
        next_gm_min = l_gm_min_points[no_point+1][1]
    
        if no_point == 0:
            seg_start = min([0.0, draft])
        else:
            seg_start = draft
        if no_point == len(l_gm_min_points) - 2:
            seg_end = max([next_draft, 23.5])
        else:
            seg_end = next_draft
    
        slope = (next_gm_min - gm_min) / (next_draft - draft)
        gm_min_start = gm_min + (seg_start - draft) * slope
    
        l_gm_min_curve.append((seg_start, seg_end, gm_min_start, slope))
        
    return l_gm_min_points, l_gm_min_curve

###### Vessel as directly read from PDF
    
# just in the same mode than other items (even if ugly)
def read_vessel_elems():

    # weights
    vessel_total_lightweight = 54645.0
    vessel_total_deadweight = 1709.6

    # CG
    vessel_light_x_CG = 169.81
    vessel_light_y_CG = -0.085
    vessel_light_z_CG = 18.645
    vessel_dead_x_CG = 123.34
    vessel_dead_y_CG = 0.0
    vessel_dead_z_CG = 14.85

    # maximal torsion moment (lu en kN-m, converti en T-m, avec P=mg en unités SI)
    vessel_max_TM = 308000 / 9.81
    
    return vessel_total_lightweight, vessel_total_deadweight,\
    vessel_light_x_CG, vessel_light_y_CG, vessel_light_z_CG,\
    vessel_dead_x_CG, vessel_dead_y_CG, vessel_dead_z_CG,\
    vessel_max_TM
  
#################   
### List of tanks
#################
    
# ugly, but keep things in the same pattern
def read_tank_elems():

    # Identification, aligner noms edi TANKSTA sur noms doc BV...
    d_tank_names_edi_2_bv = {
    'AFT VOID': 'AFT VOID',
    'BILGE HOLDING TK(P)': 'BILGE HOLDING TK(P)',
    'BILGE SETT.TK(P)': 'BILGE SETT.TK(P)',
    'BOILER FW TK': 'BOILER FEED WATER TK',
    'DISP WT TK ACC.': 'DISPOSAL WATER TK UNDER ACCOM.',
    'DISP WT TK(S)': 'DISPOSAL WATER TK(S)',
    'FO OVERF.TK(P)': 'F.O OVERF.TK(P)',
    'FO.SLUDGE TK(P)': 'F.O.SLUDGE TK(P)',
    'FW.TK(P)': 'F.W.TK(P)',
    'FW.TK(S)': 'F.W.TK(S)',
    'FWD LOW VOID': 'FWD LOW VOID',
    'HFO SERV.TK(P)': 'H.F.O SERV.TK(P)',
    'HFO SETT.TK(P)': 'H.F.O SETT.TK(P)',
    'LO.SLUDGE TK(P)': 'L.O.SLUDGE TK(P)',
    'LSHFO SERV.TK(P)': 'L.S.H.F.O SERV.TK(P)',
    'LSHFO SETT.TK(P)': 'L.S.H.F.O SETT.TK(P)',
    'MDO SERV.TK(P)': 'M.D.O. SERV.TK(P)',
    'MDO STOR.TK(S)': 'M.D.O. STOR.TK(S)',
    'ME CYL.O.SERV(P)': 'M/E CYL.O SERV.TK(P)',
    'ME JCW.DRAIN TK(S)': 'M/E J.C.W.DRAIN TK(S)',
    'ME SYS.O.SETT(S)': 'M/E SYS.O SETT.TK(S)',
    'ME SYS.O.STOR(S)': 'M/E SYS.O STOR.TK(S)',
    'ME SYS.O.SUMP TK': 'M/E SYS.O.SUMP TK',
    'NO.1 CYLO.STOR(S)': 'NO.1 M/E CYL.O STOR.TK(S)',
    'NO.1 GE.LO.STOR(P)': 'NO.1 G/E L.O.STOR.TK(P)',
    'NO.1 HFO.TK(CS)': 'NO.1 H.F.O.TK(CS)',
    'NO.1 UPP.VOID(P)': 'NO.1 UPP.VOID(P)',
    'NO.1 UPP.VOID(S)': 'NO.1 UPP.VOID(S)',
    'NO.1 VOID(C)': 'NO.1 VOID(C)',
    'NO.2 CYLO.STOR(S)': 'NO.2 M/E CYL.O STOR.TK(S)',
    'NO.2 DB.W.B.TK(P)': 'NO.2 DB.W.B.TK(P)',
    'NO.2 DB.W.B.TK(S)': 'NO.2 DB.W.B.TK(S)',
    'NO.2 GE.LO.STOR(P)': 'NO.2 G/E L.O.STOR.TK(P)',
    'NO.2 HFO.TK(CP)': 'NO.2 H.F.O.TK(CP)',
    'NO.2 W.W.B.TK(P)': 'NO.2 W.W.B.TK(P)',
    'NO.2 W.W.B.TK(S)': 'NO.2 W.W.B.TK(S)',
    'NO.3 DB.W.B.TK(P)': 'NO.3 DB.W.B.TK(P)',
    'NO.3 DB.W.B.TK(S)': 'NO.3 DB.W.B.TK(S)',
    'NO.3 HFO.TK(MS)': 'NO.3 H.F.O.TK(MS)',
    'NO.3 W.W.B.TK(P)': 'NO.3 W.W.B.TK(P)',
    'NO.3 W.W.B.TK(S)': 'NO.3 W.W.B.TK(S)',
    'NO.4 HFO.TK(MP)': 'NO.4 H.F.O.TK(MP)',
    'NO.4A DB.W.B.TK(P)': 'NO.4A DB.W.B.TK(P)',
    'NO.4A DB.W.B.TK(S)': 'NO.4A DB.W.B.TK(S)',
    'NO.4A W.W.B.TK(P)': 'NO.4A W.W.B.TK(P)',
    'NO.4A W.W.B.TK(S)': 'NO.4A W.W.B.TK(S)',
    'NO.4F DB.W.B.TK(P)': 'NO.4F DB.W.B.TK(P)',
    'NO.4F DB.W.B.TK(S)': 'NO.4F DB.W.B.TK(S)',
    'NO.4F W.W.B.TK(P)': 'NO.4F W.W.B.TK(P)',
    'NO.4F W.W.B.TK(S)': 'NO.4F W.W.B.TK(S)',
    'NO.5 DB.W.B.TK(P)': 'NO.5 DB.W.B.TK(P)',
    'NO.5 DB.W.B.TK(S)': 'NO.5 DB.W.B.TK(S)',
    'NO.5 HFO.TK(S)': 'NO.5 H.F.O.TK(S)',
    'NO.5 W.W.B.TK(P)': 'NO.5 W.W.B.TK(P)',
    'NO.5 W.W.B.TK(S)': 'NO.5 W.W.B.TK(S)',
    'NO.6 DB.W.B.TK(P)': 'NO.6 DB.W.B.TK(P)',
    'NO.6 DB.W.B.TK(S)': 'NO.6 DB.W.B.TK(S)',
    'NO.6 HFO.TK(P)': 'NO.6 H.F.O.TK(P)',
    'NO.6 W.W.B.TK(P)': 'NO.6 W.W.B.TK(P)',
    'NO.6 W.W.B.TK(S)': 'NO.6 W.W.B.TK(S)',
    'NO.7 HFO.TK(S)': 'NO.7 H.F.O.TK(S)',
    'NO.7 W.B.TK(P)': 'NO.7 W.B.TK(P)',
    'NO.7 W.B.TK(S)': 'NO.7 W.B.TK(S)',
    'NO.8 HFO.TK(P)': 'NO.8 H.F.O.TK(P)',
    'NO.8 VOID(P)': 'NO.8 VOID(P)',
    'NO.8 VOID(S)': 'NO.8 VOID(S)',
    'SLUDGE SETT.TK(P)': 'SLUDGE SETT.TK(P)',
    'SLUDGE TK(P)': 'SLUDGE TK(P)',
    'ST LO.DRAIN TK': 'S/T L.O.DRAIN TK',
    'STCW TK': 'S.T.C.W TK',
    'WASTE OIL TK': 'WASTE OIL TK',
    'SCRUBBER HOLDING': 'SCRUBBER HOLDING', 
    'SCRUBBER RESIDUE': 'SCRUBBER RESIDUE',
    'SCRUBBER SILO 1': 'SCRUBBER SILO 1', 
    'SCRUBBER SILO 2': 'SCRUBBER SILO 2', 
    'SCRUBBER M/E PROC': 'SCRUBBER M/E PROC', 
    'SCRUBBER G/E PROC': 'SCRUBBER G/E PROC'
    }
    
    # list of tank types such as read in the edi file and to be selected
    # no water ballast, no void
    l_sel_tank_types = [
        "HEAVY FUEL O..",
        "LUBRIC.OIL.",
        "DIESEL OIL.",
        "FRESH WATER.",
        "MISCELLANEOUS."
    ]
    void_tank_type = "VOID SPACES."
    wb_tank_type = "WATERBALLAST."
    #l_unknown_tanks = ["SCRUBBER HOLDING", "SCRUBBER RESIDUE",
    #                   "SCRUBBER SILO 1", "SCRUBBER SILO 2", 
    #                   "SCRUBBER M/E PROC", "SCRUBBER G/E PROC"]
    l_unknown_tanks = []
    
    return d_tank_names_edi_2_bv, l_sel_tank_types, void_tank_type, wb_tank_type, l_unknown_tanks


# get list of subtanks for water ballast tanks
def get_l_wb_tank_subtanks(tank_name, capacity, first_frame, last_frame, l_fillings,
                           d_frames, d_frames_block, nb_z_splits=2):
    
    #for (filling_level, filling_volume, filling_lcg, filling_tcg, filling_vcg) in l_fillings:
        #print(filling_level, filling_volume, filling_lcg, filling_tcg, filling_vcg)
    
    
    l_wb_tank_subtanks = []
    
    # we need, have a tank subdivided in several sub-tanks
    # divided along the x-axis depending on the blocks, 1 sub-tank by block
    # divided along the z-axis if needed
    
    # for each sub-tank we need:
    # subTkId, mainTkId
    # tkTier (1 / 2), blockId, type (fuel water, now useless), maxCapacity, xCG, yCG, zCG

    # diviser en deux verticalement si et seulement :
    # WB : Il ne faut trouver ni DB ni D.B. (double bottom)
    
    # division along x-axis, get the tank blocks with its frame (inside the tank)
    # note : range(first_frame, last_frame) means that the last frame considered is last_frame - 1 !
    # the frames given in the data are the frames as positions not as segments...
    d_tank_blocks = {}
    for no_frame in range(first_frame, last_frame):
        id_block = d_frames_block[no_frame]
        if id_block not in d_tank_blocks:
            d_tank_blocks[id_block] = []
        d_tank_blocks[id_block].append(no_frame)
    
    # sorting
    l_tank_blocks = [(id_block, l_no_frames) for id_block, l_no_frames in d_tank_blocks.items()]
    l_tank_blocks.sort(key=lambda x: x[0])
    for (id_block, l_no_frames) in l_tank_blocks:
        l_no_frames.sort()
    
    # getting block id (+1 already in d_frames_block), x positions (start and end)
    l_tank_blocks_x = [(id_block, d_frames[l_no_frames[0]][0], d_frames[l_no_frames[-1]][1])\
                       for (id_block, l_no_frames) in l_tank_blocks]
    
    #print(l_tank_blocks_x)
    
    # division along z-axis
    l_tank_blocks_z = []
    # if double-bottom, only one z block
    #if tank_name.find("DB") >= 0 or tank_name.find("D.B.") >= 0:
    #    # only one tier, we store top level (bottom level is 0 in any case)
    #    top_level = l_fillings[-1][0]
    #    x_cg = l_fillings[-1][2]
    #    y_cg = l_fillings[-1][3]
    #    z_cg = l_fillings[-1][4]
    #    l_tank_blocks_z.append((top_level, x_cg, y_cg, z_cg))
    # subdivision into n tiers    
    #else:
    if True == True:
        # get the n-1 intermediate filling volumes corresponding to the tiers
        # for instance, nb_splits = 2, get just the middle...
        l_inter_filling_volumes = [capacity * (no_tier+1) / nb_z_splits for no_tier in range(nb_z_splits-1)]                      
        # for each intermediate filling volumes, get how to read values in the table (l_fillings)
        # it means, an indice plus a coefficient on how to use next indice
        l_inter_filling_keys = []
        for volume in l_inter_filling_volumes:
            for no_filling, (filling_level, filling_volume, filling_lcg, filling_tcg, filling_vcg)\
                               in enumerate(l_fillings):
                if filling_volume >= volume:
                    ix_base_row_filling = no_filling - 1
                    prev_filling_volume = l_fillings[ix_base_row_filling][1]
                    filling_interpo_coeff = (volume - prev_filling_volume) / (filling_volume - prev_filling_volume)
                    l_inter_filling_keys.append((ix_base_row_filling, filling_interpo_coeff))
                    break
        # store the relevant informations for the nb_splits vertical components
        for no_tier in range(nb_z_splits):
            
            # get where to read the data, and interpolation coefficient
            # ix lower and upper are where to read the row for the lower and upper part of the sub-tank
            if no_tier == nb_z_splits - 1:
                ix_base_row = len(l_fillings) - 1
            else:
                ix_base_row = l_inter_filling_keys[no_tier][0]
            
            # read the data (top_level, x_cg, y_cg, z_cg)
            if no_tier == nb_z_splits - 1:
                top_level = l_fillings[ix_base_row][0]
                x_cg = l_fillings[ix_base_row][2]
                y_cg = l_fillings[ix_base_row][3]
                z_cg = l_fillings[ix_base_row][4]
            # and if necessary read it at 2 points and interpolate it:
            else:
                top_level_base = l_fillings[ix_base_row][0]
                x_cg_base = l_fillings[ix_base_row][2]
                y_cg_base = l_fillings[ix_base_row][3]
                z_cg_base = l_fillings[ix_base_row][4]
                
                top_level_up = l_fillings[ix_base_row+1][0]
                x_cg_up = l_fillings[ix_base_row+1][2]
                y_cg_up = l_fillings[ix_base_row+1][3]
                z_cg_up = l_fillings[ix_base_row+1][4]
                
                interpo_coeff = l_inter_filling_keys[no_tier][1]
                
                top_level = top_level_base + interpo_coeff * (top_level_up - top_level_base)
                x_cg = x_cg_base + interpo_coeff * (x_cg_up - x_cg_base)
                y_cg = y_cg_base + interpo_coeff * (y_cg_up - y_cg_base)
                z_cg = z_cg_base + interpo_coeff * (z_cg_up - z_cg_base)
            
            l_tank_blocks_z.append((top_level, x_cg, y_cg, z_cg))
        
    #print(l_tank_blocks_z)
        
    # we have in total len(l_tank_blocks_x) * len(l_tank_blocks_z) subtanks
    nb_blocks_x = len(l_tank_blocks_x)
    nb_blocks_z = len(l_tank_blocks_z)
    # for which we must compute: capacity, xCG, yCG, zCG
        
    # capacity : along x, proportional the x block lengths, along z equi-repartition
    # x_cg, for each tier we have a different tier (interpolated) x_cg, hence a trapeze for each tier,
    # for which to compute the cg
    # y_cg, just get the interpolated value, different for each z
    # z_cg, first get the various interpolated values, then proceed from bottom to top.
    # we reed z_cg of the total of tiers below (upper row), and of the total of the current tier 
    # ZCGt@n = (1/(V.n/N)) * [((n-1)/N).V.ZCGt@n-1 + (1/N).V.ZCGn]
    # we can read + interpolate total ZCG at n and ZCG and n-1, hence data for subpart n
    # ZCGt@n = 1/n * [(n-1)*ZCGt@n-1 + ZCGn] => ZCGn = n*ZCGt@n - (n-1)*ZCGt@n-1
        
    # preambule
    # get x_start and x_end for the tank as a whole
    x_start_tank = l_tank_blocks_x[0][1]
    x_end_tank = l_tank_blocks_x[-1][2]
    x_len_tank = x_end_tank - x_start_tank
    
    # looping only x (capacities)
    # FINALLY USELESS
    l_capacity_sub_blocks_x = []
    for no_block_x, (id_block_x, x_start_block_x, x_end_block_x) in enumerate(l_tank_blocks_x):
        # we divide by the nb of tiers, getting the final sub-tank capacity
        x_len_sub_tank = x_end_block_x - x_start_block_x
        capacity_sub_block_x = (capacity / nb_blocks_z) * x_len_sub_tank / x_len_tank
        l_capacity_sub_blocks_x.append(capacity_sub_block_x)    
    #print("(OLD) CAPACITIES")
    #for no_block_x, capacity_sub_block_x in enumerate(l_capacity_sub_blocks_x):
    #    print("X:", no_block_x, "CAPA:", capacity_sub_block_x)
    
            
    # looping only on z (y_cg, z_cg), and also an intermediate x_cg
    l_x_cg_sub_blocks_z = []
    l_y_cg_sub_blocks_z = []
    l_z_cg_sub_blocks_z = []
    for no_block_z, (top_level, x_cg, y_cg, z_cg) in enumerate(l_tank_blocks_z):
        # using formula both for z and also y
        no_block = no_block_z + 1
        x_cg_total_no_block = x_cg
        y_cg_total_no_block = y_cg
        z_cg_total_no_block = z_cg
        if no_block_z == 0:
            x_cg_total_no_block_1 = 0
            y_cg_total_no_block_1 = 0
            z_cg_total_no_block_1 = 0
        else:
            x_cg_total_no_block_1 = l_tank_blocks_z[no_block_z-1][1]
            y_cg_total_no_block_1 = l_tank_blocks_z[no_block_z-1][2]
            z_cg_total_no_block_1 = l_tank_blocks_z[no_block_z-1][3]
        x_cg_sub_block = (no_block * x_cg_total_no_block) - ((no_block - 1) * x_cg_total_no_block_1)
        y_cg_sub_block = (no_block * y_cg_total_no_block) - ((no_block - 1) * y_cg_total_no_block_1)
        z_cg_sub_block = (no_block * z_cg_total_no_block) - ((no_block - 1) * z_cg_total_no_block_1)
        l_x_cg_sub_blocks_z.append(x_cg_sub_block)
        l_y_cg_sub_blocks_z.append(y_cg_sub_block)
        l_z_cg_sub_blocks_z.append(z_cg_sub_block)
    
    #print("INTERMEDIATE X CG")
    #for no_block_z, x_cg_sub_block in enumerate(l_x_cg_sub_blocks_z):
    #    print("X:", no_block_z, "X_CG:", x_cg_sub_block)
    #print("Y CG")
    #for no_block_z, y_cg_sub_block in enumerate(l_y_cg_sub_blocks_z):
    #    print("Z:", no_block_z, "Y_CG:", y_cg_sub_block)
    #print("Z CG")
    #for no_block_z, z_cg_sub_block in enumerate(l_z_cg_sub_blocks_z):
    #    print("Z:", no_block_z, "Z_CG:", z_cg_sub_block)
    
    
    # looping on z then on x (x_cg)
    # a list of lists
    l_x_cg_sub_blocks_zx = []
    for no_block_z, x_cg_sub_block in enumerate(l_x_cg_sub_blocks_z):
            
        l_capa_x_cg_sub_blocks_x = []
        # get the trapeze at the level of the tank
        # work with the capacity of the z-tier
        height_start, delta_height = get_height_start_delta(x_start_tank, x_end_tank, x_cg_sub_block, 
                                                            capacity / nb_blocks_z)
            
        # get the x_cg for each sub block
        for no_block_x, (id_block_x, x_start_block_x, x_end_block_x) in enumerate(l_tank_blocks_x):
            x_cg_sub_block, x_capa_sub_block = get_elem_cg_weight_in_segment(x_start_block_x, x_end_block_x,
                                                                         x_start_tank, x_end_tank, 
                                                                         height_start, delta_height)
                
            l_capa_x_cg_sub_blocks_x.append((x_capa_sub_block, x_cg_sub_block))
        
        # get the list into the list
        l_x_cg_sub_blocks_zx.append(l_capa_x_cg_sub_blocks_x)
    
    #print("X CG")
    for no_block_x, (id_block_x, x_start_block_x, x_end_block_x) in enumerate(l_tank_blocks_x):
        for no_block_z, (top_level, x_cg, y_cg, z_cg) in enumerate(l_tank_blocks_z):
            capa_st = l_x_cg_sub_blocks_zx[no_block_z][no_block_x][0]
            x_cg_st = l_x_cg_sub_blocks_zx[no_block_z][no_block_x][1]
            #print("X:", no_block_x, "Z:", no_block_z, "CAPA:", capa_st, "X_CG:", x_cg_st)
    
    # last part, final double-loop in which we insert definitive sub-tanks
    # we give: 
    # tier (+1), block_id (+1 already in id)
    # capacity
    # x_cg, y_cg, z_cg
        
    for no_block_x, (id_block_x, x_start_block_x, x_end_block_x) in enumerate(l_tank_blocks_x):
            
        id_block_st = id_block_x
        #capacity_st = l_capacity_sub_blocks_x[no_block_x]
            
        for no_block_z, (top_level, x_cg, y_cg, z_cg) in enumerate(l_tank_blocks_z):
                
            tier_st = no_block_z + 1
            
            capa_st = l_x_cg_sub_blocks_zx[no_block_z][no_block_x][0]
            x_cg_st = l_x_cg_sub_blocks_zx[no_block_z][no_block_x][1]
            y_cg_st = l_y_cg_sub_blocks_z[no_block_z]
            z_cg_st = l_z_cg_sub_blocks_z[no_block_z]
                
            # everything is complete
            l_wb_tank_subtanks.append((id_block_st, tier_st, capa_st, x_cg_st, y_cg_st, z_cg_st))        
    
    return l_wb_tank_subtanks

# get list of subtanks filled with effective volumes
# we don't need to split them with z
def get_l_filled_tank_subtanks(first_frame, last_frame, volume, weight, l_cg, t_cg, v_cg,
                               d_frames, d_frames_block):
    
    # list of subtanks
    l_filled_tank_subtanks = []
    
    # division along x-axis, get the tank blocks with its frame (inside the tank)
    # note : range(first_frame, last_frame) means that the last frame considered is last_frame - 1 !
    # the frames given in the data are the frames as positions not as segments...
    d_tank_blocks = {}
    for no_frame in range(first_frame, last_frame):
        id_block = d_frames_block[no_frame]
        if id_block not in d_tank_blocks:
            d_tank_blocks[id_block] = []
        d_tank_blocks[id_block].append(no_frame)
    
    # sorting
    l_tank_blocks = [(id_block, l_no_frames) for id_block, l_no_frames in d_tank_blocks.items()]
    l_tank_blocks.sort(key=lambda x: x[0])
    for (id_block, l_no_frames) in l_tank_blocks:
        l_no_frames.sort()
    
    # getting block id (+1 already in d_frames_block), x positions (start and end)
    l_tank_blocks_x = [(id_block, d_frames[l_no_frames[0]][0], d_frames[l_no_frames[-1]][1])\
                       for (id_block, l_no_frames) in l_tank_blocks]
    #print(l_tank_blocks_x)
    
    # determine the volumes, weights, x_cg, y_cg, z_cg for each subtank
    x_start_tank = l_tank_blocks_x[0][1]
    x_end_tank = l_tank_blocks_x[-1][2]
    x_len_tank = x_end_tank - x_start_tank
    # for the x_cg (100 useless, we are only interested by the cg)
    height_start, delta_height = get_height_start_delta(x_start_tank, x_end_tank, l_cg, weight)
    
    # looping on x (volumes, weights, x_cg)
    l_infos_sub_blocks_x = []
    for no_block_x, (id_block_x, x_start_block_x, x_end_block_x) in enumerate(l_tank_blocks_x):
        x_len_sub_tank = x_end_block_x - x_start_block_x
        #volume_sub_tank = volume * x_len_sub_tank / x_len_tank
        #weight_sub_tank = weight * x_len_sub_tank / x_len_tank
        # instead of simple proportion...
        
        # not forgetting empty tanks
        if weight != 0.0:
            x_cg_sub_tank, weight_sub_tank = get_elem_cg_weight_in_segment(x_start_block_x, x_end_block_x,
                                                                           x_start_tank, x_end_tank, 
                                                                           height_start, delta_height)
            volume_sub_tank = volume * (weight_sub_tank / weight)
        else:
            x_cg_sub_tank = 0.5 * (x_start_block_x + x_end_block_x)
            weight_sub_tank = 0.0
            volume_sub_tank = 0.0
        
        l_infos_sub_blocks_x.append((volume_sub_tank, weight_sub_tank, x_cg_sub_tank))
    
    # the easy part
    y_cg = t_cg
    z_cg = v_cg
    
    for no_block_x, (id_block_x, x_start_block_x, x_end_block_x) in enumerate(l_tank_blocks_x):
        volume_st = l_infos_sub_blocks_x[no_block_x][0]
        weight_st = l_infos_sub_blocks_x[no_block_x][1]
        x_cg_st = l_infos_sub_blocks_x[no_block_x][2]
        l_filled_tank_subtanks.append((id_block_x, volume_st, weight_st, x_cg_st, y_cg, z_cg))
    
    return l_filled_tank_subtanks


# get list of water ballast sub-tanks
def get_waterballast_subtanks(d_frames, d_frames_block):

    l_wb_subtanks = []

    # only relevant files are in this directory
    for fn_tank in os.listdir():
    
        # sort between raw text (extension .txt) and new text (extension .csv)
        f_name, f_extension = os.path.splitext(fn_tank)
        if f_extension != '.csv':
            continue
    
        f_tank = open(fn_tank, 'r')
    
        for no_line, line in enumerate(f_tank):
        
            # pas de header
            l_items = line.split(';')
        
            if no_line == 0:
                tank_name = l_items[0]
                #print(tank_name)
                
                # we are interested here only in WB
                tank_type = ''
                #tank_type = 'MIS'
                #if tank_name.find("HOLD") >= 0: tank_type = "HOLD"
                if tank_name.find("W.B.") >= 0: tank_type = "WB"   
                #if tank_name.find("F.W.") >= 0: tank_type = "FW"
                #if tank_name.find("H.F.O") >= 0: tank_type = "HFO"
                #if tank_name.find("M.D.O.") >= 0: tank_type = "DO"
                #if (tank_name.find("M/E") >= 0 or tank_name.find("G/E") >= 0)\
                #    and (tank_name.find("STOR") >= 0 or tank_name.find("SETT") >= 0 or tank_name.find("SERV") >= 0):
                #    tank_type = "LO"
                #if tank_name.find("VOID") >= 0: tank_type = "VOID"
            
                #print(tank_type)
                # not necessary to go on if a water ballast
                if tank_type != "WB": break
            
                capacity = float(l_items[1])
                first_frame = int(l_items[2])
                last_frame = int(l_items[3])
                l_fillings = []
            else:
                filling_level = float(l_items[0])
                filling_volume = float(l_items[1])
                filling_lcg = float(l_items[2])
                filling_tcg = float(l_items[3])
                filling_vcg = float(l_items[4])
                l_fillings.append((filling_level, filling_volume, filling_lcg, filling_tcg, filling_vcg))
            
        f_tank.close()
    
        # only for water ballast
        if tank_type != "WB": continue
    
        # calcul des éléments du tank (détermination des sous-tanks etc.)
        # 2 splits !!!
        l_wb_tank_subtanks = get_l_wb_tank_subtanks(tank_name, capacity, first_frame, last_frame, l_fillings,
                                                    d_frames, d_frames_block, nb_z_splits=4)
        # complete the list with tank name and total capacity
        l_wb_tank_subtanks = [(tank_name, id_block_st, tier_st, capacity_st, x_cg_st, y_cg_st, z_cg_st, capacity)\
                              for (id_block_st, tier_st, capacity_st, x_cg_st, y_cg_st, z_cg_st)\
                              in l_wb_tank_subtanks]
        l_wb_subtanks.extend(l_wb_tank_subtanks)

    return l_wb_subtanks

def read_tanks_basic_infos():

    d_tanks_basic_infos = {}

    # only relevant files are in this directory
    for fn_tank in os.listdir():
    
        # sort between raw text (extension .txt) and new text (extension .csv)
        f_name, f_extension = os.path.splitext(fn_tank)
        if f_extension != '.csv':
            continue
    
        f_tank = open(fn_tank, 'r')
    
        for no_line, line in enumerate(f_tank):
        
            # no header
            l_items = line.split(';')
        
            # just read first line
            if no_line == 0:
                tank_name = l_items[0]
        
                capacity = float(l_items[1])
                first_frame = int(l_items[2])
                last_frame = int(l_items[3])
            
                d_tanks_basic_infos[tank_name] = (capacity, first_frame, last_frame)
            else:
                break
            
        f_tank.close()
    
    # complete manually for some (scrubbing) tanks.
    d_tanks_basic_infos['SCRUBBER HOLDING'] = (152.10, 28, 30)
    d_tanks_basic_infos['SCRUBBER RESIDUE'] = (200.32, 27, 30)
    d_tanks_basic_infos['SCRUBBER SILO 1'] = (58.35, 35, 39)
    d_tanks_basic_infos['SCRUBBER SILO 2'] = (46.54, 35, 39)
    d_tanks_basic_infos['SCRUBBER M/E PROC'] = (51.49, 40, 43)
    d_tanks_basic_infos['SCRUBBER G/E PROC'] = (15.54, 41, 43)

    # and override for 2 others
    d_tanks_basic_infos['NO.2 M/E CYL.O STOR.TK(S)'] = (55.48, 43, 45)
    d_tanks_basic_infos['M/E SYS.O SETT.TK(S)'] = (111.42, 45, 49)
    
    
    return d_tanks_basic_infos

# get filled tanks port info
def l_get_filled_tanks_port_infos(l_rows, port_name, port_name_extension, 
                                  void_tank_type, l_unknown_tanks, l_sel_tank_types,
                                  filter_out_wb=True):
    
    l_filled_tanks_port = []
    
    for no_row, row in enumerate(l_rows):
        
        # useless header
        if no_row in [0, 1, 2, 3, 4, 6, 7, 8]:
            continue
        
        # in header, get the port if to be read there
        if no_row == 5:
            if port_name == '':
                port = row[6:11] + port_name_extension
            else:
                port = port_name + port_name_extension
            continue
        
        # useless tail
        if row[0:3] in ['UNT', 'UNZ']:
            continue
        
        # tanker rows
        #print(row)
        
        # tank name
        if row[0:3] == 'LOC':
            s_l_name = row.split(':')
            name = s_l_name[3]
        # tank weight
        if row[0:6] == 'MEA+WT':
            weight = float(row[12:])
        if row[0:7]  == 'MEA+VOL':
            volume = float(row[13:])
        if row[0:3] == 'DIM':
            s_l_cg = row[10:]
            l_s_cg = s_l_cg.split(':')
            l_cg = float(l_s_cg[0])
            # potential sign inconsistency...
            t_cg = -1 * float(l_s_cg[1])
            v_cg = float(l_s_cg[2])
            
        # most importantely, and at the end..., if wb to be filtered
        if row[0:3] == 'FTX':
            tank_to_be_kept = True
            tank_type = row[10:]
            # no void
            if tank_type == void_tank_type:
                tank_to_be_kept = False
            # no "unknown" tanks (scrubber in addendum)
            if name in l_unknown_tanks:
                tank_to_be_kept = False
            # no water ballast if filter...
            if filter_out_wb == True and tank_type not in l_sel_tank_types:
                tank_to_be_kept = False
            # if OK, store what had been saved
            if tank_to_be_kept == True:
                l_filled_tanks_port.append((port, name, volume, weight, l_cg, t_cg, v_cg))
            
    return l_filled_tanks_port

def get_l_filled_tanks_ports(BV_condition, BV_condition_tank_port, WB_compensating_trim,
                             edi_format,
                             void_tank_type, l_unknown_tanks, 
                             l_sel_tank_types, filter_out_wb=False):

    l_filled_tanks_ports = []
    
    #### Case 1: files provided by CMA, one file by port (No BV condition)
    ###### We are in the context of only fuel and miscellaneous tanks are considered (no water ballast)

    if BV_condition is None:

        # only relevant files are in this directory
        for fn_tanks_port in os.listdir():
            
            # sort between baplie edi (extension .edi) and new text (extension .csv)
            f_name, f_extension = os.path.splitext(fn_tanks_port)
            if f_extension != '.edi':
                continue
            
            # how to distinguish between GBSOU and GBSOU2 can only be done using the file name 
            port_name_extension = ''
            if f_name[-1] in ['2', '3', '4']:
                port_name_extension = f_name[-1]
    
            f_tanks_port = open(fn_tanks_port, 'r')
    
            # get the file rows as a list, so to have common treatment with format without row separator
            if edi_format == 'EDI_CRLF':
                l_rows = []
                for no_row, row in enumerate(f_tanks_port):
                    # remove ' and \n
                    l_rows.append(row[0:-2])
            if edi_format == 'EDI_QUOTE':
                l_line = f_tanks_port.readlines()
                # "real" rows
                l_rows = l_line[0].split("'")
                
            f_tanks_port.close()
    
            # work on the list of rows as a whole
            l_filled_tanks_port\
            = l_get_filled_tanks_port_infos(l_rows, '', port_name_extension, 
                                            void_tank_type, l_unknown_tanks, l_sel_tank_types, 
                                            filter_out_wb=filter_out_wb)
            l_filled_tanks_ports.extend(l_filled_tanks_port)
            
    #### case 2) Reading TANKSTA edi files coming from macS3 (no WB compensation)
    ###### we keep water ballast
    
    if BV_condition is not None and BV_condition != '01' and WB_compensating_trim == False:

        fn_edi_tank_list = "lc%s-tanks.edi" % BV_condition

        f_edi_tank_list = open(fn_edi_tank_list, 'r')
        
        # un seul enregistrement
        if edi_format == 'EDI_QUOTE':
            l_line = f_edi_tank_list.readlines()
            # "real" rows
            l_rows = l_line[0].split("'")
        #print(len(l_line)) 
        # ou plusieurs
        if edi_format == 'EDI_CRLF':
            l_rows = []
            for no_row, row in enumerate(f_edi_tank_list):
                # remove ' and \n
                l_rows.append(row[0:-2])
        
        f_edi_tank_list.close()

        # simply useless
        port_name_extension = ''

        # work on the list of rows as a whole
        l_filled_tanks_port = l_get_filled_tanks_port_infos(l_rows, BV_condition_tank_port, port_name_extension, 
                                                            void_tank_type, l_unknown_tanks, l_sel_tank_types,
                                                            filter_out_wb=False)
        l_filled_tanks_ports.extend(l_filled_tanks_port)
        
    #### case 3) Reading TANKSTA edi files coming from CMA (WB are compensating trim)
    ###### we keep water ballast
    
    if BV_condition is not None and WB_compensating_trim == True:
        
        port_name_extension = ''

        fn_edi_tank_list = "lc%s-tanks_WB_4_trim.edi" % BV_condition

        f_tanks_port = open(fn_edi_tank_list, 'r')
    
        # get the file rows as a list, so to have common treatment with format without row separator
        if edi_format == 'EDI_CRLF':
            l_rows = []
            for no_row, row in enumerate(f_tanks_port):
                # remove ' and \n
                l_rows.append(row[0:-2])
        # or one unique row
        if edi_format == 'EDI_QUOTE':
            l_line = f_tanks_port.readlines()
            # "real" rows
            l_rows = l_line[0].split("'")
    
        f_tanks_port.close()
    
        # work on the list of rows as a whole, no filter on WB !!
        l_filled_tanks_port = l_get_filled_tanks_port_infos(l_rows, BV_condition_tank_port, port_name_extension, 
                                                            void_tank_type, l_unknown_tanks, l_sel_tank_types,
                                                            filter_out_wb=False)
        l_filled_tanks_ports.extend(l_filled_tanks_port)
        
        
    return l_filled_tanks_ports


########################################
### LoadLists : reading BAPLIE edi files
########################################

###### Obtention de la taille et du type du conteneur à partir du type ISO

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


def get_l_containers(BV_condition, BV_condition_load_port, BV_condition_disch_port, 
                     specific_edi_filename=None, edi_container_nb_header_lines=9,
                     edi_structure='EQD'):
    
    l_containers = []

    #### Case 1: files provided by CMA, one file by port (NO BV Condition !!)
    
    if BV_condition is None and specific_edi_filename is None:

        # loop on files
        l_fn = os.listdir()
        for fn_loadlist in l_fn:
    
            # look at original files
            f_name, f_extension = os.path.splitext(fn_loadlist)    
            if f_name[0:2] != 'X_':
                continue
            
            f_loadlist = open(fn_loadlist , 'r')
            for no_line, line in enumerate(f_loadlist):
        
                l_items = line.split(',')
        
                # first line, see if either with or without loadlist
                if no_line == 0:
                    with_slot = False
                    if l_items[0] == 'Position':
                        with_slot = True
                    continue
    
                # current lines
                if with_slot == True:
                    slot = l_items[0]
                    inc = 1
                else:
                    slot = ''
                    inc = 0
        
                load_port = l_items[0 + inc]
                disch_port = l_items[1 + inc]
                cont_id = l_items[2 + inc]
                c_type = l_items[3 + inc]
                weight = float(l_items[4 + inc]) # already in tons
                setting = l_items[5 + inc]
                # remove 'I' (DG) from setting
                setting = setting.replace('I', '')
        
                size, height = get_container_size_height(c_type)
        
                # for GBSOU, both present in leg 08 and 11, change to SOU2 (leg 11) if:
                # POL: in the on-board file at 00, and in 11, loaded in SOU2
                # POD: for legs 09 and 10
                no_leg = f_name[2:4]
                if load_port == "GBSOU" and no_leg in ['00', '11']: load_port += "2"
                if disch_port == "GBSOU" and no_leg in ['09', '10']: disch_port += "2"
            
                #if disch_port == "MYPKG": print("Décharge à MYPKG")
        
                # ContId;LoadPort;DischPort;Type;Setting;Size;Height;Weight;Slot
                l_containers.append((cont_id, load_port, disch_port, c_type, setting, size, height, weight, slot))
        
            f_loadlist.close()
            
    #### case 2) Reading BAPLI edi files coming from macS3 (BV condition, but not '01' or '02') 
    
    #From BAPLIE edi file, generated by macS3:
    #Duquel on lit pour chaque conteneur chargé :
    #LOC+147+0011020::5' -> Slot
    #MEA+WT++KGM:12000' -> Weight
    #RFF+BM:1'
    #EQD+CN++22G0+++5' -> Type (-> Size, Height), -> Setting
    #Pas de LoadPort / DischPort, fixer avec valeurs remplies en paramètre BV_condition_load_port, _disch_port , sinon :
    #LOC+9+FRLEH:139:6' -> LoadPort
    #LOC+11+CNXMN:139:6' -> DischPort
    #In output, we should get:
    #ContId;LoadPort;DischPort;Type;Setting;Size;Height;Weight;Slot
            
    if (   BV_condition is not None and BV_condition not in ['01', '02']\
        or BV_condition is None and specific_edi_filename is not None):

        if BV_condition is not None:
            fn_edi_loadlist = "lc%s-cargo.edi" % BV_condition
        else:
            fn_edi_loadlist = specific_edi_filename

        # un seul enregistrement
        f_edi_loadlist = open(fn_edi_loadlist , 'r')
        l_line = f_edi_loadlist.readlines()
        #print(len(l_line)) 
        f_edi_loadlist.close()

        # "real" lines
        l_rows = l_line[0].split("'")
        # read header interesting lines to get POL POD
        if BV_condition is not None:
            # override load and discharge port
            load_port = BV_condition_load_port
            disch_port = BV_condition_disch_port
        # initialisation
        else:
            load_port = ''
            disch_port = ''
        # cut the header and the tail (9 and 2 rows)
        l_rows = l_rows[edi_container_nb_header_lines:-2]
        for no_row, row in enumerate(l_rows):
    
            if row[0:3] == 'LOC':
                # might be either a slot (147), a loading port (9), or a discharge port (11)
                # others (76 for instance) are ignored
                l_items = row.split('+')
                if l_items[1] == '147':
                    # not keeping the first 0 for slot
                    slot = l_items[2][1:7]
                if BV_condition is None:
                    if l_items[1] == '9':
                        load_port = l_items[2][0:5]
                    if l_items[1] == '11':
                        disch_port = l_items[2][0:5]
            if row[0:3] == 'MEA':
                l_items = row.split('+')
                if (l_items[1] == 'WT' and edi_structure == 'EQD')\
                or (l_items[1] == 'AAE' and edi_structure == 'CNT'):
                    unit_weight = l_items[3]
                    if unit_weight[0:3] == 'KGM':
                        weight = float(unit_weight[4:]) / 1000
            if row[0:3] == 'RFF':
                continue
            # EQD+CN, last interesting row, we can append the container there
            if row[0:3] == 'EQD':
                
                l_items = row.split('+')
                if l_items[1] == 'CN':
                    
                    # container (if exists)
                    cont_id = l_items[2]
                    # if does not exist, case of BV condition, generate a pseudo id
                    if cont_id == '' and BV_condition is not None:
                        cont_id = "C%06d" % ((no_row // 4) + 1)
                    
                    # type
                    c_type = l_items[3][0:4]
                    size, height = get_container_size_height(c_type)
                    empty_full = l_items[6]
                    setting = 'E' if empty_full == '4' else ''
            
            if row[0:3] == edi_structure:
                    # nothing more to add
                    container_to_add = True
                    l_items = row.split('+')
                    if edi_structure == 'EQD' and l_items[1] != 'CN':
                        container_to_add = False
                    if container_to_add == True:
                        l_containers.append((cont_id, load_port, disch_port, 
                                             c_type, setting, size, height, weight, slot))
                    
            if row[0:3] == 'NAD':
                continue
    
    #### case 3) No container to read (condition 01)
    
    if BV_condition is not None and BV_condition in ['01', '02']:
    
        # useless but OK
        l_containers = []
    
    
    return l_containers

def read_bv_conditions(raw_bv_conditions_filename):

    d_ref_weights_by_m = {}
    d_ref_buoyancies_by_m = {}
    d_ref_shear_forces = {}
    d_pct_shear_forces = {}
    d_ref_bending_moments = {}
    d_pct_bending_moments = {}

    # file
    f_raw_bv_conditions = open(raw_bv_conditions_filename, 'r')

    l_current_trim = None
    for line in f_raw_bv_conditions:
    
        # type of line
        line_type = line[0]
        if line_type in ['-', 'F', '#']: continue
        
        # new condition
        if line_type == '=':
            condition = line[1:3]
            l_ref_weights_by_m = []
            l_ref_buoyancies_by_m = []
            l_ref_shear_forces = []
            l_pct_shear_forces = []
            l_ref_bending_moments = []
            l_pct_bending_moments = []
            continue
        
        # numerical data
        if line_type >= '0' and line_type <= '9':
            l_items = line.split()
        
            no_frame = round(float(l_items[0]))
            pos_x = float(l_items[1])
                         
            shear_force = float(l_items[2])
            l_ref_shear_forces.append(shear_force)
            shear_force_pct = float(l_items[3])
            l_pct_shear_forces.append(shear_force_pct)
        
            bending_moment = float(l_items[4])
            l_ref_bending_moments.append(bending_moment)
            bending_moment_pct = float(l_items[5])
            l_pct_bending_moments.append(bending_moment_pct)
        
            weight_m = float(l_items[6])
            l_ref_weights_by_m.append(weight_m)
            buoyancy_m = float(l_items[7])
            l_ref_buoyancies_by_m.append(buoyancy_m)
    
        # end of condition
        if line_type == '*':
            d_ref_weights_by_m[condition] = l_ref_weights_by_m
            d_ref_buoyancies_by_m[condition] = l_ref_buoyancies_by_m
            d_ref_shear_forces[condition] = l_ref_shear_forces
            d_pct_shear_forces[condition] = l_pct_shear_forces
            d_ref_bending_moments[condition] = l_ref_bending_moments
            d_pct_bending_moments[condition] = l_pct_bending_moments
            continue

    f_raw_bv_conditions.close()
    
    
    return d_ref_weights_by_m, d_ref_buoyancies_by_m,\
           d_ref_shear_forces, d_pct_shear_forces,\
           d_ref_bending_moments, d_pct_bending_moments


# plus directly entering the values for draft and trim conditions
# ugly

def read_draft_trim_conditions():
    
    d_draft_conditions = {
    '01': 4.552,
    '02': 8.969,
    '03': 7.994,
    '04': 16.000,
    '05': 15.506,
    '06': 16.000,
    '07': 15.855
    }

    d_trim_conditions = {
    '01': 3.509,
    '02': 2.458,
    '03': 4.160,
    '04': 1.228,
    '05': 1.775,
    '06': 0.626,
    '07': 0.758
    }
    
    return d_draft_conditions, d_trim_conditions

