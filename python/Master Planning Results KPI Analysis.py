# -*- coding: utf-8 -*-
"""
Created on Mon Nov  8 13:19:40 2021

@author: 056757706
"""

# libraries
import os
import vessel_stow_kpi as vsk

# directory and file names
###############################################################
# TO BE MODIFIED
# correspondance entre slot position et sous-baies
REP_DATA_VESSEL = "c:/Projets/CCS/Vessel Stowage/Modèles/Data"
fn_subbays = "SubBays Capacities Extrait Prototype MP_IN.csv"
fn_rotation = "Rotation Ports 5 Extrait Prototype MP_IN.csv"

# contexte général (référence de départ, 0 => sortie de MYPKG, rentrée à CNTXG)
LEG_NO = 0

# en entrée, on doit avoir la version master planning du on-board + les résultats du master-planning
# pour n ports d'affilée
# on regarde la situation aux ports de 1 à LAST_PORT_SEQ
# en utilisant les onboard de 0 à LAST_PORT_SEQ
LAST_PORT_SEQ = 3

# master planning retraité pour l'overstow (y compris phase initiale)
REP_DATA_OBM = "c:/Projets/CCS/Vessel Stowage/KPI/global_out_stab_1118_306_4"
ROOT_FILE_NAME_OBM = "9454450 Container Groups Completed"
SOURCE_OBM = "MP_OS"

# écriture
REP_DATA_KPI = "c:/Projets/CCS/Vessel Stowage/KPI/global_out_stab_1118_306_4"
FILE_NAME_KPI = "9454450 KPI Results.csv"

# pour le crane split, 
#calcul prenant en compte ou non la différence entre 20' et 40'
CRANE_SPLIT_TEU = True
# coefficient de gain du dual cycling
DUAL_CYCLING_GAIN = 0.5
# temps estimé pour un déplacement hatch cover (en heures)
HC_MOVE_TIME = 0.5

##############################################################

# Préliminaire : éléments particuliers du navire et de la rotation

os.chdir(REP_DATA_VESSEL)

d_sb_capacities = vsk.get_subbays_capacities(fn_subbays)

l_bays, l_subbays, l_cols_bay_subbay = vsk.get_lists_bay_subbay(d_sb_capacities)

d_ports_capacities = vsk.get_ports_capacities(fn_rotation)

###### Chargement du on-board

def load_OBM_results(fn_results, port_seq, source, leg_no=0):
    
    d_sb_2_cg = {}
    d_sb_2_cg_osw = {}
    
    f_results = open(fn_results, 'r')
    
    for no_row, row in enumerate(f_results):
        
        if no_row == 0: continue
            
        l_items = row.split(';')
            
        if source == "MP_OS":
            subbay = l_items[0] # sur 4 caractères (ou 0 si vide)
            if len(subbay) == 3: subbay = '0' + subbay
            pol_name = l_items[1]
            pol_seq = vsk.port_name_2_seq(pol_name, leg_no)
            pod_name = l_items[2]
            pod_seq = vsk.port_name_2_seq(pod_name, leg_no)
            c_size = l_items[3]
            c_type = l_items[4]
            c_weight = l_items[5]
            hc = l_items[6]
            quantity = int(l_items[8])
            #avg_weight = float(l_items[7])
            weight = float(l_items[9])
            
            # overstow information
            s_with_overstow = l_items[10].strip()
            with_overstow = True if s_with_overstow == 'X' else False
            overstow_quantity = 0
            overstow_weight = 0.0
            s_overstow_quantity = l_items[12].strip()
            overstow_quantity = int(s_overstow_quantity) if len(s_overstow_quantity) > 0 else 0
            s_overstow_weight = l_items[13].strip()
            overstow_weight = float(s_overstow_weight) if len(s_overstow_weight) > 0 else 0.0
            d_overstow_sources = {}
            s_overstow_sources = l_items[14].strip()
            if len(s_overstow_sources) > 0:
                l_overstow_sources = s_overstow_sources.split('|')
                for s_overstow_sources in l_overstow_sources:
                    l_overstow_sce_elems = s_overstow_sources.split('-')
                    overstow_source_sb = l_overstow_sce_elems[0]
                    overstow_source_pol = vsk.port_name_2_seq(l_overstow_sce_elems[1], leg_no)
                    overstow_source_q = int(l_overstow_sce_elems[2])
                    overstow_source_w = float(l_overstow_sce_elems[3])
                    d_overstow_sources[(overstow_source_sb, overstow_source_pol)]\
                    = (overstow_source_q, overstow_source_w)
            
        # éliminer / conserver
        # tout d'abord le on-board
        # ATTENTION GARBAGE
        # reconnaître garbage (with overstow == False, précaution sans doute inutile)
        # onboard, mais en plus, les pol > port courant sont supposées être des load lists du futur
        garbage = False
        if subbay == '' and with_overstow == False:
            if vsk.pol_pod_onboard_or_loadlist(pol_seq, pod_seq, port_seq) == vsk.ONBOARD\
            and pol_seq < port_seq: 
                garbage = True
                
        # conserver aussi les mouvements prévus pour le port suivant
        loadlist_2_come = False
        if subbay == '':
            if vsk.pol_pod_onboard_or_loadlist(pol_seq, pod_seq, port_seq) == vsk.LOADLIST\
            and pol_seq == port_seq + 1: 
                loadlist_2_come = True
            
                
        if subbay == '' and with_overstow == False\
        and garbage == False and loadlist_2_come == False: continue
        # pas de sélection supplémentaire sur les lignes de chargement, car elles ont été faites
        # auparavant, lors de la constitution du fichier ici lu
        
        # main data, onboard data (or garbage):
        if subbay != ''\
        or (subbay == '' and garbage == True)\
        or (subbay == '' and loadlist_2_come == True):
            container_group = (pol_seq, pod_seq, c_size, c_type, c_weight, hc)
            
            #if garbage == True:
            #    print(subbay, container_group, quantity)
            #if loadlist_2_come == True:
            #    print(subbay, container_group, quantity)
            if subbay not in d_sb_2_cg:
                d_sb_2_cg[subbay] = {}
            if container_group not in d_sb_2_cg[subbay]:
                # ne pas considérer les overstow quand on regarde la liste de chargement à faire
                cg_quantity = quantity
                cg_weight = weight
                if loadlist_2_come == True:
                    cg_quantity -= overstow_quantity
                    cg_weight -= overstow_weight
                d_sb_2_cg[subbay][container_group] = (cg_quantity, cg_weight)
            else:
                print("double au niveau de", subbay, container_group)
            
        # overstow correction (only with_overstow == True should suffice)
        if subbay == '' and with_overstow == True:
            for (overstow_source_sb, overstow_source_pol),\
                (overstow_source_q, overstow_source_w) in d_overstow_sources.items():
                if overstow_source_sb not in d_sb_2_cg_osw:
                     d_sb_2_cg_osw[overstow_source_sb] = {}
                # pol and pod must match with those in the bay so that it is easily found there
                container_group_osw = (overstow_source_pol, pol_seq, c_size, c_type, c_weight, hc)
                #if overstow_source_sb == '2621':
                #    print("2621 : CG OSW", container_group_osw, "sur pod", pod_seq)
                if container_group_osw not in d_sb_2_cg_osw[overstow_source_sb]:
                    d_sb_2_cg_osw[overstow_source_sb][container_group_osw] = {}
                if pod_seq not in d_sb_2_cg_osw[overstow_source_sb][container_group_osw]:
                    d_sb_2_cg_osw[overstow_source_sb][container_group_osw][pod_seq]\
                    = (overstow_source_q, overstow_source_w)
                    #if overstow_source_sb == '2621':
                    #    print("nouveau", overstow_source_q)
                else:
                    new_osw_q =\
                    d_sb_2_cg_osw[overstow_source_sb][container_group_osw][pod_seq][0] + overstow_source_q
                    new_osw_w =\
                    d_sb_2_cg_osw[overstow_source_sb][container_group_osw][pod_seq][1] + overstow_source_w
                    d_sb_2_cg_osw[overstow_source_sb][container_group_osw][pod_seq] =\
                    (new_osw_q, new_osw_w)
                    #if overstow_source_sb == '2621':
                    #    print("complété", overstow_source_q, "->", new_osw_q)
            
    f_results.close()
    
    return d_sb_2_cg, d_sb_2_cg_osw

# get all onboards for all ports covered
l_d_sb_2_cg = []
for port_seq in range(LAST_PORT_SEQ+1):
    
    rep_data = REP_DATA_OBM
    file_name = ROOT_FILE_NAME_OBM + ' ' + str(port_seq) + ".csv"
    source = SOURCE_OBM
    os.chdir(rep_data)
    #print(port_seq)
    d_sb_2_cg, d_sb_2_cg_osw = load_OBM_results(file_name, port_seq, source, leg_no=LEG_NO)
    #print("******************")
    l_d_sb_2_cg.append((d_sb_2_cg, d_sb_2_cg_osw))
    
# transformation in situ des dictionnaires de l'on-board en fonction des overstow
# => retour à la situation réelle de l'on-board, avec les bons pol et pod
l_d_sb_2_cg_org = []

for ix, (d_sb_2_cg, d_sb_2_cg_osw) in enumerate(l_d_sb_2_cg):
    
    #print(ix)
    
    # copie du dictionnaire d'origine qu'on modifie peu à peu
    #d_sb_2_cg_org = copy.deepcopy(d_sb_2_cg)
    d_sb_2_cg_org = {}
    for sb, d_cg in d_sb_2_cg.items():
        d_sb_2_cg_org[sb] = {}
        for cg, (quantity, weight) in d_cg.items():
            d_sb_2_cg_org[sb][cg] = (quantity, weight)
    
    # en considérant les éléments d'overstow un par un
    for sb_osw, d_cg_osw in d_sb_2_cg_osw.items():
        if sb_osw not in d_sb_2_cg_org:
            print("baie %s avec overstow devrait être dans le dictionnaire du onboard")
        for cg_osw, d_pod_osw in d_cg_osw.items():
            if cg_osw not in d_sb_2_cg_org[sb_osw]:
                print("cg", cg_osw, "en overstow dans", sb_osw, "devrait être dans le dictionnaire du onboard")
            #if sb_osw == '2621':
            #    print(cg_osw, "en overstow dans 2621")
            for pod_seq_osw, (quantity_osw, weight_osw) in d_pod_osw.items():
                # 1) réduire la baie / cg dans le dictionnaire de départ de la quantité trouvée 
                new_quantity = d_sb_2_cg_org[sb_osw][cg_osw][0] - quantity_osw
                new_weight = d_sb_2_cg_org[sb_osw][cg_osw][1] - weight_osw
                #if sb_osw == '2621':
                #    print("new_quantity:", new_quantity)
                # et même supprimer si plus rien dedans
                if new_quantity == 0:
                    del d_sb_2_cg_org[sb_osw][cg_osw]
                else:
                    d_sb_2_cg_org[sb_osw][cg_osw] = (new_quantity, new_weight)
                
                # 2) créer à la place (ou incrémenter) un on-board avec le pod réel, et non d'overstow
                cg_org = (cg_osw[0], pod_seq_osw, cg_osw[2], cg_osw[3], cg_osw[4], cg_osw[5])
                #if sb_osw == '2621':
                #    print ("new cg origine", cg_org, "avec", quantity_osw)
                if cg_org not in d_sb_2_cg_org[sb_osw]:
                    d_sb_2_cg_org[sb_osw][cg_org] = (quantity_osw, weight_osw)
                    #if sb_osw == '2621': print("rajout")
                else:
                    new_quantity = d_sb_2_cg_org[sb_osw][cg_org][0] + quantity_osw
                    new_weight = d_sb_2_cg_org[sb_osw][cg_org][1] + weight_osw
                    d_sb_2_cg_org[sb_osw][cg_org] = (new_quantity, new_weight)
                    #if sb_osw == '2621': print("incrément ->", new_quantity)
    
    l_d_sb_2_cg_org.append(d_sb_2_cg_org)
    
##### KPI élémentaires au niveau des sous-baies
l_d_sb_kpi_elems = []
for port_seq in range(1,LAST_PORT_SEQ+1):
    d_sb_kpi_elems = vsk.get_kpi_elems(port_seq, l_d_sb_2_cg_org, l_subbays)
    l_d_sb_kpi_elems.append(d_sb_kpi_elems)
    
##### KPI dérivés des élémentaires au niveau des sous-baies (move et restow)
for d_sb_kpi_elems in l_d_sb_kpi_elems:
    vsk.get_kpi_derived_elems(d_sb_kpi_elems)
    
###### KPI au niveau des baies : dual cycling et crane split
    
d_crane_bays = vsk.distribute_bays_for_cranes(l_bays)

l_d_bay_kpi_cranes = []
for ix, d_sb_kpi_elems in enumerate(l_d_sb_kpi_elems):
    port_seq = ix + 1
    crane_single_speed = d_ports_capacities[port_seq][1]
    d_bay_kpi_cranes = vsk.get_kpi_cranes(d_sb_kpi_elems, l_bays, d_crane_bays,
                                          crane_single_speed, 
                                          DUAL_CYCLING_GAIN, HC_MOVE_TIME)
    l_d_bay_kpi_cranes.append(d_bay_kpi_cranes)
    
###### KPI des taux de remplissage, au niveau de chaque baie
l_d_bay_kpi_teu = []
for d_sb_kpi_elems in l_d_sb_kpi_elems:
    d_bay_kpi_teu = vsk.get_kpi_teu(d_sb_kpi_elems, l_bays, d_sb_capacities)
    l_d_bay_kpi_teu.append(d_bay_kpi_teu)
    
###### KPI des écarts de masse, au niveau de chaque baie
    
d_left_right_subbays = vsk.get_left_right_subbays(d_sb_capacities)

l_d_bay_kpi_weights_distrib = []
for d_sb_kpi_elems in l_d_sb_kpi_elems:
    d_bay_kpi_weights_distrib = vsk.get_kpi_weights_distrib(d_sb_kpi_elems, l_bays, d_left_right_subbays)
    l_d_bay_kpi_weights_distrib.append(d_bay_kpi_weights_distrib)
    
###### Agrégation au niveau des ports
l_d_kpi_sums = vsk.port_kpi_dicos_2_lists(l_d_sb_kpi_elems, 
                                          l_d_bay_kpi_cranes, l_d_bay_kpi_teu, l_d_bay_kpi_weights_distrib,
                                          d_ports_capacities, DUAL_CYCLING_GAIN, HC_MOVE_TIME)

###### Arrangement des résultats
l_l_sb_kpi_elems, l_l_bay_kpi_cranes, l_l_bay_kpi_teu, l_l_bay_kpi_weights_distrib =\
vsk.sb_kpi_dicos_2_lists(l_d_sb_kpi_elems, 
                         l_d_bay_kpi_cranes, l_d_bay_kpi_teu, l_d_bay_kpi_weights_distrib)

# maintenant, opérer la bascule en créant des listes associant au port (index), 
# un dictionnaire associant à chaque indicateur sa valeur globale, 
# et ses valeurs par sous-baie et baie selon le cas
l_d_kpis = vsk.arrange_final_kpis(l_d_kpi_sums, l_l_sb_kpi_elems, 
                                  l_l_bay_kpi_cranes, l_l_bay_kpi_teu, l_l_bay_kpi_weights_distrib)

# on a tout ce qu'il faut pour écrire le fichier .csv maintenant !!!  
os.chdir(REP_DATA_KPI)

vsk.write_kpi_file(FILE_NAME_KPI, CRANE_SPLIT_TEU, l_d_kpis, 
                   d_sb_capacities, l_bays, l_subbays, l_cols_bay_subbay,
                   d_ports_capacities,
                   leg_no=LEG_NO)

    


