# -*- coding: utf-8 -*-
"""
Created on Fri Nov 19 16:40:26 2021

@author: 056757706
"""

# Input Files Preparation 
###########################

# libraries
import os
import csv
import re
import datetime as dt
import pickle
import random

# directories
REP_DATA = "c:/Projets/CCS/Vessel Stowage/Python Prétraitement"
REP_TANKS_RAW_DATA = REP_DATA + "/9454450 Tanks"

os.chdir(REP_DATA)

## From Raw Files to Usable Files

##### Note: files:
#BLOCKS_FILENAME = "9454450 Blocks.csv"
#GM_MIN_CURVE_FILENAME = "9454450 GMMin Curve.csv"
#have been edited manually

##### Frames

RAW_FRAMES_FILENAME = "9454450 Raw Frames.txt"

# frames
l_frames = []

# file
f_raw_frames = open(RAW_FRAMES_FILENAME, 'r')

# example of a line:
# -4 0.800 -3.200 -192.400 66 3.050 105.050 -84.150 136 1.850 274.950 85.750
# 3 (or less) set of frame elem: frame no, spacing in m to the next frame (except for the last frame, 
# which in fact is just the end, not to be included)
# then distance from A.P in m, distance from midship in m
# we will also add the end position, start position + spacing
for line in f_raw_frames:
    l_items = line.split()
    nb_items = len(l_items)
    for no_item, item in enumerate(l_items):
        if no_item % 4 == 0:
            # add previous group of four
            if no_item > 0:
                l_frames.append(frame)
            # new (temptative) group of four
            frame = []
            no_frame = int(item)
            frame.append(no_frame)
        if no_item % 4 == 1:
            spacing = float(item)
            frame.append(spacing)
        if no_item % 4 == 2:
            start_distance = float(item)
            frame.append(start_distance)
            frame.append(round(start_distance + spacing,2))
    # add last group of 4, if it is indeed a group of 4
    if len(l_items) % 4 == 0:
        l_frames.append(frame)
            
# close
f_raw_frames.close()

l_frames.sort(key=lambda x:x[0])
#print(l_frames)

FRAMES_FILENAME = "9454450 Frames.csv"

# use the list to write the definitive frame file
f_frames = open(FRAMES_FILENAME, 'w')
s_header = "NoFrame;Spacing;Start;End\n"
f_frames.write(s_header)
for frame in l_frames:
    s_frame = "%d;%.2f;%.2f;%.2f\n" % (frame[0], frame[1], frame[2], frame[3])
    f_frames.write(s_frame)

f_frames.close()

##### Lightweight Elements
###### Newer format

RAW_LIGHTWEIGHT_FILENAME = "9454450 Raw Lightweight 2.txt"

# lw_elem
l_lw_elems = []

# file
f_raw_lightweight = open(RAW_LIGHTWEIGHT_FILENAME, 'r')

# example of a line:
# ELE1 1826.7 4.03 -0.085 18.645 -6.50 18.25 ''
# 1 set of lw elem: elem name, weight in t, lcg in m, tcg in m, vcg in m, aft end in m, fwd end in m, comments 
# we simply keep the elements 1, 2, 5, 6
for line in f_raw_lightweight:
    
    lw_elem = []
    l_items = line.split()
    for no_item, item in enumerate(l_items):
        
        if no_item == 1:
            weight = float(item)
        if no_item == 2:
            lcg = float(item)
        if no_item == 5:
            aft_end = float(item)
        if no_item == 6:
            fwd_end = float(item)
        
    l_lw_elems.append((aft_end, fwd_end, lcg, weight))
            
# close
f_raw_lightweight.close()

LIGHTWEIGHT_FILENAME = "9454450 Lightweight.csv"

# use the list to write the definitive frame file
f_lightweight = open(LIGHTWEIGHT_FILENAME, 'w')
s_header = "PosStart;PosEnd;PosLCG;Weight\n"
f_lightweight.write(s_header)
for lw_elem in l_lw_elems:
    s_lw_elem = "%.2f;%.2f;%.3f;%.1f\n" % (lw_elem[0], lw_elem[1], lw_elem[2], lw_elem[3])
    f_lightweight.write(s_lw_elem)

f_lightweight.close()

##### Deadweight Constant Elems

RAW_DEADWEIGHT_CONSTANT_FILENAME = "9454450 Raw Deadweight Constant.txt"

l_dw_constant_elems = []

# file
f_dw_constant_elems = open(RAW_DEADWEIGHT_CONSTANT_FILENAME, 'r')

for line in f_dw_constant_elems:
    
    l_items = line.split()
    
    # elem_name
    elem_name = l_items[0]
    # others
    fill = float(l_items[1])
    mass = float(l_items[2])
    xm = float(l_items[3])
    ym = float(l_items[4])
    zm = float(l_items[5])
    frsm = float(l_items[6])
    wda = float(l_items[7])
    wdf = float(l_items[8])
    
   
    l_dw_constant_elems.append((elem_name, fill, mass, xm, ym, zm, frsm, wda, wdf))
    
f_dw_constant_elems.close()

# pour l'écriture, supposer que les positions de début et de fin sont celles des du navire
# récupérer cela d'après les positions du premier et dernier frame
# CE N'EST PLUS NECESSAIRE

# reading frames
# keeping start and end positions is enough
# in two structures
d_frames = {}
l_frames = []

FRAMES_FILENAME = "9454450 Frames.csv"
f_frames = open(FRAMES_FILENAME, 'r')
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

# use the list to write the definitive frame file
DEADWEIGHT_CONSTANT_FILENAME = "9454450 Deadweight Constant.csv"
f_dw_constant_elems = open(DEADWEIGHT_CONSTANT_FILENAME, 'w')
s_header = "PosStart;PosEnd;PosLCG;Weight\n"
f_dw_constant_elems.write(s_header)
for l_dw_constant_elem in l_dw_constant_elems:
    s_dw_constant_elem = "%.2f;%.2f;%.3f;%.1f\n" % (l_dw_constant_elem[7], l_dw_constant_elem[8],
                                                    l_dw_constant_elem[3], l_dw_constant_elem[2])
    f_dw_constant_elems.write(s_dw_constant_elem)

f_dw_constant_elems.close()

#### Hydrostratic File (coarse, draft every 0.50 m)

RAW_HYDROSTATICS_FILENAME = "9454450 Raw Hydrostatics.txt"

l_hydrostatics_by_trim = []

# file
f_raw_hydrostatics = open(RAW_HYDROSTATICS_FILENAME, 'r')

l_current_trim = None
for line in f_raw_hydrostatics:
    
    # type of line
    line_type = line[0]
    if line_type in ['-', 'T', 'm']: continue
    
    # relevant lines
    l_items = line.split()
    
    # trim grouping
    if line_type == 'C':
        if l_current_trim is not None:
            l_hydrostatics_by_trim.append((trim, l_current_trim))
        trim = float(l_items[4])
        l_current_trim = []
    
    # by draft
    else:
        T = float(l_items[0])
        TK = float(l_items[1])
        DISP = float(l_items[2])
        LCF = float(l_items[3])
        LCB = float(l_items[4])
        VCB = float(l_items[5])
        TPC = float(l_items[6])
        MCT = float(l_items[7])
        KMT = float(l_items[8])
        l_current_trim.append((T, TK, DISP, LCF, LCB, VCB, TPC, MCT, KMT))

# last page
l_hydrostatics_by_trim.append((trim, l_current_trim))

f_raw_hydrostatics.close()

# On veut une clé trim puis draft
# La structure est OK, la reconduire telle quelle

HYDROSTATICS_FILENAME = "9454450 Hydrostatics.csv"
f_hydrostatics = open(HYDROSTATICS_FILENAME, 'w')

s_header = "Trim;T;TK;DISP;LCF;LCB;VCB;TPC;MCT;KMT\n"
f_hydrostatics.write(s_header)

for (trim, l_current_trim) in l_hydrostatics_by_trim:

    s_trim = "%.1f;" % trim
    for l_draft in l_current_trim:
        s_ligne = s_trim
        s_ligne += "%.1f;" % l_draft[0]
        s_ligne += "%.3f;" % l_draft[1]
        s_ligne += "%.1f;" % l_draft[2]
        s_ligne += "%.2f;" % l_draft[3]
        s_ligne += "%.3f;" % l_draft[4]
        s_ligne += "%.3f;" % l_draft[5]
        s_ligne += "%.1f;" % l_draft[6]
        s_ligne += "%.1f;" % l_draft[7]
        s_ligne += "%.3f\n" % l_draft[8]
        f_hydrostatics.write(s_ligne)
    
f_hydrostatics.close()

##### Hydrostratic File (detailed, draft every 0.05 m)

RAW_HYDROSTATICS_FINE_FILENAME = "9454450 Raw Hydrostatics Fine.txt"

l_hydrostatics_by_drift = []

# file
f_raw_hydrostatics = open(RAW_HYDROSTATICS_FINE_FILENAME, 'r')

l_drafts_items = []
l_page_drafts_items = []
nb_drafts_in_line = 0 # to be read at each page
nb_trims_in_line = 10 # the same for all pages 


for line in f_raw_hydrostatics:
    
    # type of line
    line_type = line[0]
    if line_type in ['=']: continue
    
    if line[0:19] == "DRAFT MOULDED (m) :":
        line_content = line[20:]
        l_s_draft_moulded = line_content.split()
        l_draft_moulded = [float(s_draft_moulded) for s_draft_moulded in l_s_draft_moulded]
        nb_drafts_in_line = len(l_draft_moulded)
        continue
    
    if line[0:19] == "DRAFT EXTREME (m) :":
        line_content = line[20:]
        l_s_draft_extreme = line_content.split()
        l_draft_extreme = [float(s_draft_extreme) for s_draft_extreme in l_s_draft_extreme]
        if len(l_draft_extreme) != nb_drafts_in_line: print("NB DRAFTS IN LINE %d !!" % nb_drafts_in_line)
        continue
    
    if line[0:20] == "DISPL. IN S.W.(ton):":
        l_page_disp = []
        trim = 0.0
        line_content = line[21:]
        l_s_disp = line_content.split()
        l_disp = [float(s_disp) for s_disp in l_s_disp]
        if len(l_disp) != nb_drafts_in_line: print("NB DRAFTS IN LINE %d !!" % nb_drafts_in_line)
        l_page_disp.append((trim, l_disp))
        continue
    
    if line[0:15] == "L.C.F FROM AP :":
        l_page_lcf = []
        trim = 0.0
        line_content = line[16:]
        l_s_lcf = line_content.split()
        l_lcf = [float(s_lcf) for s_lcf in l_s_lcf]
        if len(l_lcf) != nb_drafts_in_line: print("NB DRAFTS IN LINE %d !!" % nb_drafts_in_line)
        l_page_lcf.append((trim, l_lcf))
        continue
    
    if line[0:15] == "L.C.B FROM AP :":
        l_page_lcb = []
        trim = 0.0
        line_content = line[16:]
        l_s_lcb = line_content.split()
        l_lcb = [float(s_lcb) for s_lcb in l_s_lcb]
        if len(l_lcb) != nb_drafts_in_line: print("NB DRAFTS IN LINE %d !!" % nb_drafts_in_line)
        l_page_lcb.append((trim, l_lcb))
        continue
    
    if line[0:18] == "V.C.B ABOVE B.L. :":
        line_content = line[19:]
        l_s_vcb = line_content.split()
        l_vcb = [float(s_vcb) for s_vcb in l_s_vcb]
        if len(l_vcb) != nb_drafts_in_line: print("NB DRAFTS IN LINE %d !!" % nb_drafts_in_line)
        continue
    
    if line[0:18] == "T.P.C.(TONN/1cm) :":
        line_content = line[19:]
        l_s_tpc = line_content.split()
        l_tpc = [float(s_tpc) for s_tpc in l_s_tpc]
        if len(l_tpc) != nb_drafts_in_line: print("NB DRAFTS IN LINE %d !!" % nb_drafts_in_line)
        continue
    
    if line[0:20] == "M.T.C. (ton-m/1cm) :":
        line_content = line[21:]
        l_s_mtc = line_content.split()
        l_mtc = [float(s_mtc) for s_mtc in l_s_mtc]
        if len(l_mtc) != nb_drafts_in_line: print("NB DRAFTS IN LINE %d !!" % nb_drafts_in_line)
        continue
    
    if line[0:12] == "K.M.T. (m) :":
        l_page_kmt = []
        trim = 0.0
        line_content = line[13:]
        l_s_kmt = line_content.split()
        l_kmt = [float(s_kmt) for s_kmt in l_s_kmt]
        if len(l_kmt) != nb_drafts_in_line: print("NB DRAFTS IN LINE %d !!" % nb_drafts_in_line)
        l_page_kmt.append((trim, l_kmt))
        continue
    
    if line[0:12] == "DISPL. TRIM=":
        s_trim = line[13:18]
        trim = float(s_trim)
        line_content = line[20:]
        l_s_disp = line_content.split()
        l_disp = [float(s_disp) for s_disp in l_s_disp]
        if len(l_disp) != nb_drafts_in_line: print("NB DRAFTS IN LINE %d !!" % nb_drafts_in_line)
        l_page_disp.append((trim, l_disp))
        continue
    
    if line[0:12] == "L.C.B. TRIM=":
        s_trim = line[13:18]
        trim = float(s_trim)
        line_content = line[20:]
        l_s_lcb = line_content.split()
        l_lcb = [float(s_lcb) for s_lcb in l_s_lcb]
        if len(l_lcb) != nb_drafts_in_line: print("NB DRAFTS IN LINE %d !!" % nb_drafts_in_line)
        l_page_lcb.append((trim, l_lcb))
        continue
    
    if line[0:12] == "K.M.T. TRIM=":
        s_trim = line[13:18]
        trim = float(s_trim)
        line_content = line[20:]
        l_s_kmt = line_content.split()
        l_kmt = [float(s_kmt) for s_kmt in l_s_kmt]
        if len(l_kmt) != nb_drafts_in_line: print("NB DRAFTS IN LINE %d !!" % nb_drafts_in_line)
        l_page_kmt.append((trim, l_kmt))
        continue
    
    if line[0:12] == "L.C.F. TRIM=":
        s_trim = line[13:18]
        trim = float(s_trim)
        line_content = line[20:]
        l_s_lcf = line_content.split()
        l_lcf = [float(s_lcf) for s_lcf in l_s_lcf]
        if len(l_lcf) != nb_drafts_in_line: print("NB DRAFTS IN LINE %d !!" % nb_drafts_in_line)
        l_page_lcf.append((trim, l_lcf))
        continue
    
    # has been manually added to the file in order to facilitate the process
    # storing the page result
    if line_type == '*':
        for no_draft in range(nb_drafts_in_line):
            
            draft_moulded = l_draft_moulded[no_draft]
            draft_extreme = l_draft_extreme[no_draft]
            vcb_0 = l_vcb[no_draft]
            tpc_0 = l_tpc[no_draft]
            mtc_0 = l_mtc[no_draft] 
            
            l_trim_disp = []
            for no_trim in range(nb_trims_in_line):
                trim = l_page_disp[no_trim][0]
                disp = l_page_disp[no_trim][1][no_draft]
                l_trim_disp.append((trim, disp))
                
            l_trim_lcf = []
            for no_trim in range(nb_trims_in_line):
                trim = l_page_lcf[no_trim][0]
                lcf = l_page_lcf[no_trim][1][no_draft]
                l_trim_lcf.append((trim, lcf))
                
            l_trim_lcb = []
            for no_trim in range(nb_trims_in_line):
                trim = l_page_lcb[no_trim][0]
                lcb = l_page_lcb[no_trim][1][no_draft]
                l_trim_lcb.append((trim, lcb))
            
            l_trim_kmt = []
            for no_trim in range(nb_trims_in_line):
                trim = l_page_kmt[no_trim][0]
                kmt = l_page_kmt[no_trim][1][no_draft]
                l_trim_kmt.append((trim, kmt))
            
            # a dictionary is more readable
            draft_items = {
                'draft_moulded': draft_moulded,
                'draft_extreme': draft_extreme,
                'vcb_0': vcb_0,
                'tpc_0': tpc_0,
                'mtc_0': mtc_0,
                'trim_disp': l_trim_disp,
                'trim_lcf': l_trim_lcf,
                'trim_lcb': l_trim_lcb,
                'trim_kmt': l_trim_kmt
            }
            l_drafts_items.append(draft_items)
    
        continue  
    
f_raw_hydrostatics.close()

# On veut une clé trim puis draft
# La structure est ainsi à retravailler

HYDROSTATICS_FINE_FILENAME = "9454450 Hydrostatics Fine.csv"
f_hydrostatics = open(HYDROSTATICS_FINE_FILENAME, 'w')

# in precedent file T is moulded draft, TK is extreme draft (TK > T)
# ISSUE: organize drafts by T or TK?
# here we assume by TK, because here the round values are by extreme draft

s_header = "Trim;TK;T;DISP;LCF;LCB;VCB;TPC;MCT;KMT\n"
f_hydrostatics.write(s_header)

# reorganize structure by trim and extreme draft
d_trim_tk = {}

for draft_items in l_drafts_items:
    
    draft_moulded = draft_items['draft_moulded']
    draft_extreme = draft_items['draft_extreme']
    vcb_0 = draft_items['vcb_0']
    tpc_0 = draft_items['tpc_0']
    mtc_0 = draft_items['mtc_0']
    l_trim_disp = draft_items['trim_disp']
    l_trim_lcf = draft_items['trim_lcf']
    l_trim_lcb = draft_items['trim_lcb']
    l_trim_kmt = draft_items['trim_kmt'] 
    
    # first items with trim 0
    d_trim_tk[(0.0, draft_extreme)] = {'T': draft_moulded, 'VCB': vcb_0, 'TPC': tpc_0, 'MCT': mtc_0}
    # adding items for list with trims
    for (trim, disp) in l_trim_disp:
        if (trim, draft_extreme) not in d_trim_tk:
            d_trim_tk[(trim, draft_extreme)] = {'T': draft_moulded}
        d_trim_tk[(trim, draft_extreme)]['DISP'] = disp
    for (trim, lcf) in l_trim_lcf:
        if (trim, draft_extreme) not in d_trim_tk:
            d_trim_tk[(trim, draft_extreme)] = {'T': draft_moulded}
        d_trim_tk[(trim, draft_extreme)]['LCF'] = lcf
    for (trim, lcb) in l_trim_lcb:
        if (trim, draft_extreme) not in d_trim_tk:
            d_trim_tk[(trim, draft_extreme)] = {'T': draft_moulded}
        d_trim_tk[(trim, draft_extreme)]['LCB'] = lcb
    for (trim, kmt) in l_trim_kmt:
        if (trim, draft_extreme) not in d_trim_tk:
            d_trim_tk[(trim, draft_extreme)] = {'T': draft_moulded}
        d_trim_tk[(trim, draft_extreme)]['KMT'] = kmt
        
# transform into sorted list                                
l_trim_tk_items = [(trim, tk, d_trim_tk_elems) for (trim, tk), d_trim_tk_elems in d_trim_tk.items()]
l_trim_tk_items.sort(key=lambda x: (x[0], x[1]))

for (trim, tk, d_trim_tk_elems) in l_trim_tk_items:

    s_ligne = "%.1f;%.3f;" % (trim, tk)
    s_ligne += "%.3f;%.1f;" % (d_trim_tk_elems['T'], d_trim_tk_elems['DISP'])
    s_ligne += "%.2f;%.2f;" % (d_trim_tk_elems['LCF'], d_trim_tk_elems['LCB'])
    s_vcb = "%.2f" % d_trim_tk_elems['VCB'] if 'VCB' in d_trim_tk_elems else ""
    s_tpc = "%.2f" % d_trim_tk_elems['TPC'] if 'TPC' in d_trim_tk_elems else ""
    s_mct = "%.2f" % d_trim_tk_elems['MCT'] if 'MCT' in d_trim_tk_elems else ""
    s_ligne += "%s;%s;%s;" % (s_vcb, s_tpc, s_mct)
    s_ligne += "%.2f\n" % d_trim_tk_elems['KMT']
    
    f_hydrostatics.write(s_ligne)
    
f_hydrostatics.close()

##### Bonjean File

RAW_BONJEAN_FRAME_AREA_FILENAME = "9454450 Raw Bonjean Frame Area.txt"

l_drafts_by_x = []

# file
f_bonjean_frame_area = open(RAW_BONJEAN_FRAME_AREA_FILENAME, 'r')

l_current_xs = None
for line in f_bonjean_frame_area:
    
    # type of line
    line_type = line[0]
    if line_type in ['-']: continue
    
    # relevant lines
    l_items = line.split()
    
    # set of x
    if line_type == 'x':
        if l_current_xs is not None:
            l_drafts_by_x.extend(l_current_xs)
        l_current_xs = []
        nb_xs = len(l_items) - 1
        for ix in range(0, nb_xs):
            l_current_xs.append((float(l_items[ix+1]),[]))
    
    # by draft
    else:
        draft = float(l_items[0][2:])
        for ix in range(0, nb_xs):
            l_current_xs[ix][1].append((draft, float(l_items[ix+1])))
        
# last page
l_drafts_by_x.extend(l_current_xs)

f_bonjean_frame_area.close()

# We may need both a structure where for each position then draft we get the area
# or the other way around, for each draft have the profil, we will privilege that

# for the file, we will have a flat structure, create the needed list(s) when loading the file
BONJEAN_FRAME_AREA_FILENAME = "9454450 Bonjean Frame Area.csv"
f_bonjean_frame_area = open(BONJEAN_FRAME_AREA_FILENAME, 'w')

s_header = "X;Draft;Area\n"
f_bonjean_frame_area.write(s_header)

for (x, l_xs) in l_drafts_by_x:
    s_x = "%.2f;" % x
    for (draft, area) in l_xs:
        s_ligne = s_x
        s_ligne += "%.2f;" % draft
        s_ligne += "%.2f\n" % area
        f_bonjean_frame_area.write(s_ligne)

f_bonjean_frame_area.close()

##### SF Enveloppe

RAW_SF_ENVELOPPE_FILENAME = "9454450 Raw SF Enveloppe.txt"

l_sf_enveloppe_by_x = []

# file
f_sf_enveloppe = open(RAW_SF_ENVELOPPE_FILENAME, 'r')

for line in f_sf_enveloppe:
    
    l_items = line.split()
    
    # frame
    no_frame = int(l_items[0])
    
    # lim sup and lim inf (in tons, at sea)
    s_lim_sup = l_items[5].replace(',', '')
    lim_sup = float(s_lim_sup)
    s_lim_inf = l_items[6].replace(',', '')
    lim_inf = float(s_lim_inf)
    
    l_sf_enveloppe_by_x.append((no_frame, lim_inf, lim_sup))
    
f_sf_enveloppe.close()

SF_ENVELOPPE_FILENAME = "9454450 SF Enveloppe.csv"

f_sf_enveloppe = open(SF_ENVELOPPE_FILENAME, 'w')

s_header = "NoFrame;LimInf;LimSup\n"
f_sf_enveloppe.write(s_header)

for enveloppe in l_sf_enveloppe_by_x:
    s_ligne = "%d;%0.f;%0.f\n" % (enveloppe[0], enveloppe[1], enveloppe[2])
    f_sf_enveloppe.write(s_ligne)

f_sf_enveloppe.close()

##### BM Enveloppe

RAW_BM_ENVELOPPE_FILENAME = "9454450 Raw BM Enveloppe.txt"

l_bm_enveloppe_by_x = []

# file
f_bm_enveloppe = open(RAW_BM_ENVELOPPE_FILENAME, 'r')

for line in f_bm_enveloppe:
    
    l_items = line.split()
    
    # frame
    no_frame = int(l_items[0])
    
    # lim sup and lim inf (in tons, at sea)
    s_lim_sup = l_items[5].replace(',', '')
    lim_sup = float(s_lim_sup)
    s_lim_inf = l_items[6].replace(',', '')
    lim_inf = float(s_lim_inf)
    
   
    l_bm_enveloppe_by_x.append((no_frame, lim_inf, lim_sup))
    
f_bm_enveloppe.close()

BM_ENVELOPPE_FILENAME = "9454450 BM Enveloppe.csv"

f_bm_enveloppe = open(BM_ENVELOPPE_FILENAME, 'w')

s_header = "NoFrame;LimInf;LimSup\n"
f_bm_enveloppe.write(s_header)

for enveloppe in l_bm_enveloppe_by_x:
    s_ligne = "%d;%0.f;%0.f\n" % (enveloppe[0], enveloppe[1], enveloppe[2])
    f_bm_enveloppe.write(s_ligne)

f_bm_enveloppe.close()

##### Tanks

os.chdir(REP_TANKS_RAW_DATA)

l_tanks = []

# only relevant files are in this directory
for fn_raw_tank in os.listdir():
    
    # sort between raw text (extension .txt) and new text (extension .csv)
    f_name, f_extension = os.path.splitext(fn_raw_tank)
    if f_extension != '.txt':
        continue
    
    f_raw_tank = open(fn_raw_tank , 'r')
    
    l_filling_levels = []
    for no_line, line in enumerate(f_raw_tank):
        
        print(no_line, line)
        # type of line
        line_type = line[0]
        if line_type == '-' and no_line > 7: break
            
        l_items = line.split()
            
        # first line
        # schema: chapter_no name with blank spacing
        if no_line == 0:
            tank_name = ' '.join(l_items[1:])
            continue
        
        # second line
        # schema: VOLUME : volume CUBIC M (POSITION : Ffstart - Ffend)
        if no_line == 1:
            volume = float(l_items[2])
            first_frame = int(l_items[7][1:])
            last_frame = int(l_items[9][1:-1])
            continue
            
        if no_line in [2, 3, 4, 5, 6]:
            continue
        
        # current lines, only get
        filling_level = float(l_items[0])
        filling_volume = float(l_items[3])
        filling_lcg = float(l_items[4])
        filling_tcg = float(l_items[5])
        filling_vcg = float(l_items[6])
        l_filling_levels.append((filling_level, filling_volume, 
                                 filling_lcg, filling_tcg, filling_vcg))
        
    tank = (tank_name, volume, first_frame, last_frame, l_filling_levels)    
    l_tanks.append(tank)
    
    f_raw_tank.close()
    
for tank in l_tanks:
    
    # / is not possible in a file name, substitute with -
    tank_name = tank[0].replace("/", "-")
    fn_tank = tank_name + ".csv"
    f_tank = open(fn_tank , 'w')
    # no header, because 2 different kinds of lines
    s_line = "%s;%.1f;%d;%d\n" % (tank[0], tank[1], tank[2], tank[3])
    f_tank.write(s_line)
    for filling_level in tank[4]:
        s_line = "%.3f;%.1f;%.3f;%.3f;%.3f\n" %\
        (filling_level[0], filling_level[1], filling_level[2], filling_level[3], filling_level[4])
        f_tank.write(s_line)
    f_tank.close()
    

