# libraries
import os
#import copy
import sys

# POUR UTILISER LA LIGNE DE COMMANDE
#if len(sys.argv) < 2: print("Manque argument ")
# 1er argument (l'argument 0 étant le nom du programme)
#PORT_SEQ = int(sys.argv[1])

# directory and file names
##################
# TO BE MODIFIED
# 0 : si situation initiale, onboard de MYPKG et en entrée de CNTXG
# 1 : si situation initiale en sortie de CNTXG
# C'EST UN NUMERO ABSOLU
LEG_NO = 0

# No de séquence courant
# C'EST UN NUMERO RELATIF
# Le premier de la série, c'est quand on reçoit master planning 1, en sortie
# du premier port après traitement (en absolu, en sortie du port LEG_NO + 1)
# PORT_SEQ est à faire varier entre 1 et jusqu'à l'horizon souhaité
PORT_SEQ = 1

# correspondance entre slot position et sous-baies
REP_DATA_VESSEL = "c:/Projets/CCS/Vessel Stowage/Modèles/Data"
fn_subbays = "SubBays Capacities Extrait Prototype MP_IN.csv"

# on board initial et load lists à charger
# pour le port 1, utilisation du on board initial + les load lists
# pour les ports 2, ..., utilisation du on board généré (master planning) + les load lists
#REP_DATA_INIT = "c:/Projets/CCS/Vessel Stowage/Python Prétraitement/Test/out"
REP_DATA_INIT = "c:/Projets/CCS/Vessel Stowage/KPI/v50-CNTXG"
#FILE_NAME_INIT = "9454450 Preprocessed OnBoard LoadList Container Groups.csv"
FILE_NAME_INIT_ROOT = "9454450 Container Groups Completed"
SOURCE_INIT = "MP_OS"
# résultat du master planning (on board généré)
#REP_DATA_MP = "c:/Projets/CCS/Vessel Stowage/Python C++ Modèles/C++/MasterPlanningV3/msvc9/data/out_stab_hz1"
#FILE_NAME_MP = "MasterPlanning 1.csv"
#REP_DATA_MP = "c:/Projets/CCS/Vessel Stowage/Python Prétraitement/Test/out"
REP_DATA_MP = "c:/Projets/CCS/Vessel Stowage/KPI/v50-CNTXG"
FILE_NAME_MP_ROOT = "MasterPlanning"
# pour sortie globale
SOURCE_MP = "MP"
# écriture
REP_DATA_ADD = "c:/Projets/CCS/Vessel Stowage/KPI/v50-CNTXG"
FILE_NAME_ADD_ROOT = "9454450 Container Groups Completed"

##########################################################

# Préliminaire : éléments particuliers du navire et de la rotation

os.chdir(REP_DATA_VESSEL)

# récupérer les sous-baies et leurs capacités totales des sous-baies (au cas où)
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


################################################################################
# le chargement peut se faire :
# soit dans une vision on-board. 
# On élimine alors les conteneurs groupes dans les sous-baies vides, si on est en chargement initial
# (port_seq = 0)
# soit dans une vision loadlist (issue du fichier consolidé)
# on ne garde que les conteneurs encore à charger après port_seq. A 0, on prend tout (pol > 0),
# en sortie de 1, on prend pol > 1, etc.

def load_MP_results(d_sb_2_cg, d_cg_org_os, d_sb_2_cg_org_os,
                    fn_results, onboard_loadlist, port_seq, source, leg_no):
    
    
    f_results = open(fn_results, 'r')
    
    for no_row, row in enumerate(f_results):
        
        if no_row == 0: continue
            
        l_items = row.split(';')
        
        if source in ["MP", "MP_OS"]:
            
            subbay = l_items[0] # sur 4 caractères (ou 0 si vide)
            if len(subbay) == 3: subbay = '0' + subbay
            pol_name = l_items[1]
            pol_seq = port_name_2_seq(pol_name, leg_no)
            pod_name = l_items[2]
            pod_seq = port_name_2_seq(pod_name, leg_no)
            c_size = l_items[3]
            c_type = l_items[4]
            c_weight = l_items[5]
            hc = l_items[6]
            # hence the container group
            container_group = (pol_seq, pod_seq, c_size, c_type, c_weight, hc)
            
            quantity = int(l_items[8])
            #avg_weight = float(l_items[7])
            weight = float(l_items[9])
            
            # overstow information
            overstow_quantity = 0
            overstow_weight = 0.0
            if source == "MP_OS":
                # INTERESSANT A RECUPERER QUE DANS LE CAS DE LA PARTIE LOADLIST (sous bay = '')
                #overstow = True if l_items[10].strip() == 'X' else False
                #s_overstow_pod_seq = l_items[11].strip()
                #overstow_pod_seq = int(s_overstow_pod_seq) if len(s_overstow_pod_seq) > 0 else -1
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
                        overstow_source_pol = port_name_2_seq(l_overstow_sce_elems[1], leg_no)
                        overstow_source_q = int(l_overstow_sce_elems[2])
                        overstow_source_w = float(l_overstow_sce_elems[3])
                        d_overstow_sources[(overstow_source_sb, overstow_source_pol)]\
                                         = (overstow_source_q, overstow_source_w)
                
                
        # éliminer / conserver
        if onboard_loadlist == "ONBOARD":
            # FINALEMENT, on ne garde pas les reste-à-quai pour les ports courants
            if subbay == '': continue
        if onboard_loadlist == "LOADLIST":
            # ne pas garder ce qui a été déjà déchargé à ce niveau de la rotation
            # si pol_seq < pod_seq, ce qui a été déchargé avant le port courant
            # (port_seq) dont on vient de sortir, ne doit plus être pris en compte
            # si maintenant pol_seq > pod_seq, cela veut dire qu'on a un chargement
            # qui ira plus loin que la fin de rotation, le déchargement ne doit pas
            # être éliminé
            if pol_seq <= pod_seq and pod_seq <= port_seq: continue
            # et ce qui devait être chargé auparavant est dans le nouveau onboard,
            # à ne pas considérer non plus
            # le jeu d'essai est tel qu'on n'a pas de loadlist dans un port d'une
            # rotation ultérieure, la condition suffit alors
            if pol_seq <= port_seq: continue
                    
        # cas normal, charger les éléments de base
        if onboard_loadlist == "ONBOARD"\
        or (onboard_loadlist == "LOADLIST" and subbay == ''):
            if subbay not in d_sb_2_cg:
                d_sb_2_cg[subbay] = {}
            if container_group not in d_sb_2_cg[subbay]:
                d_sb_2_cg[subbay][container_group] = (quantity, weight)
            else:
                print("double au niveau de", subbay, container_group)
                
        # to be stored now (and not eliminated later on)
        if onboard_loadlist == "LOADLIST" and subbay == '':
            # overstow global au niveau du cg
            d_cg_org_os[container_group] = (overstow_quantity, overstow_weight)
                    
            for (overstow_source_sb, overstow_source_pol),\
                (overstow_source_q, overstow_source_w) in d_overstow_sources.items():
                if overstow_source_sb not in d_sb_2_cg_org_os:
                    d_sb_2_cg_org_os[overstow_source_sb] = {}
                # pol and pod must match with those in the bay so that it is easily found there
                container_group_osw = (overstow_source_pol, pol_seq, c_size, c_type, c_weight, hc)
                if container_group_osw not in d_sb_2_cg_org_os[overstow_source_sb]:
                    d_sb_2_cg_org_os[overstow_source_sb][container_group_osw] = {}
                if pod_seq not in d_sb_2_cg_org_os[overstow_source_sb][container_group_osw]:
                    d_sb_2_cg_org_os[overstow_source_sb][container_group_osw][pod_seq]\
                    = (overstow_source_q, overstow_source_w)
                else:
                    new_osw_q =\
                    d_sb_2_cg_org_os[overstow_source_sb][container_group_osw][pod_seq][0] + overstow_source_q
                    new_osw_w =\
                    d_sb_2_cg_org_os[overstow_source_sb][container_group_osw][pod_seq][1] + overstow_source_w
                    d_sb_2_cg_org_os[overstow_source_sb][container_group_osw][pod_seq] = (new_osw_q, new_osw_w)
                        
            
    f_results.close()
    
    return d_sb_2_cg, d_cg_org_os, d_sb_2_cg_org_os

##########################################
# chargement en pratique
d_sb_2_cg = {}
d_cg_org_os = {}
d_sb_2_cg_org_os = {}

# d'abord onboard
    
rep_data = REP_DATA_MP
file_name = FILE_NAME_MP_ROOT + ' ' + str(PORT_SEQ) + ".csv"
source = SOURCE_MP
os.chdir(rep_data)
d_sb_2_cg, d_cg_org_os, d_sb_2_cg_org_os =\
load_MP_results(d_sb_2_cg, d_cg_org_os, d_sb_2_cg_org_os, 
                file_name, "ONBOARD", PORT_SEQ, source, LEG_NO)

#print("APRES ONBOARD 5431")
#if '5431' in d_sb_2_cg:
#    print(d_sb_2_cg['5431'])
#else:
#    print("RIEN pour 5431")
#if (6, 9, '40', 'GP', 'L', '') in d_cg_org_os:
#    print(d_cg_org_os[(6, 9, '40', 'GP', 'L', '')])
#else:
#    print("RIEN ORG OS pour (6, 9, '40', 'GP', 'L', '')")
#if '5431' in d_sb_2_cg_org_os:
#    print(d_sb_2_cg_org_os['5431'])
#else:
#    print("RIEN ORG OS pour 5431")

# ensuite la loadlist
rep_data = REP_DATA_INIT
file_name = FILE_NAME_INIT_ROOT + ' ' + str(PORT_SEQ-1) + ".csv"
source = SOURCE_INIT
os.chdir(rep_data)
d_sb_2_cg, d_cg_org_os, d_sb_2_cg_org_os =\
load_MP_results(d_sb_2_cg, d_cg_org_os, d_sb_2_cg_org_os, 
                file_name, "LOADLIST", PORT_SEQ, source, LEG_NO)

#print("APRES LOADLIST 5431")
#if '5431' in d_sb_2_cg:
#print(d_sb_2_cg['9401'])
#print("VIDE", d_sb_2_cg[''][(3, 5, '45', 'GP', 'L', 'HC')])
#else:
#    print("RIEN pour 5431")
#if (6, 9, '40', 'GP', 'L', '') in d_cg_org_os:
#print(d_cg_org_os[(3, 5, '45', 'GP', 'L', 'HC')])
#else:
#    print("RIEN ORG OS pour (6, 9, '40', 'GP', 'L', '')")
#if '5431' in d_sb_2_cg_org_os:
#print(d_sb_2_cg_org_os['9401'])
#else:
#    print("RIEN ORG OS pour 5431")
#print("----")

# reconstituer la situation réelle, cela veut dire 
# dans le on-board remettre le port originel de destination
# dans les loadlists, enlever la partie overstow (qui sera recalculée)

# onboard
#d_sb_2_cg_real_ob = copy.deepcopy({subbay: d_cg for subbay, d_cg in d_sb_2_cg.items()\
#                                   if subbay != ''})
d_sb_2_cg_real_ob = {}
for subbay, d_cg in d_sb_2_cg.items():
    if subbay == '': continue
    d_sb_2_cg_real_ob[subbay] = {}
    for cg, (quantity, weight) in d_cg.items():
        d_sb_2_cg_real_ob[subbay][cg] = (quantity, weight)
    
# en considérant les éléments d'overstow un par un
for sb_osw, d_cg_osw in d_sb_2_cg_org_os.items():
    if sb_osw not in d_sb_2_cg_real_ob:
        print("baie %s avec overstow devrait être dans le dictionnaire du onboard")
    for cg_osw, d_pod_osw in d_cg_osw.items():
        if cg_osw not in d_sb_2_cg_real_ob[sb_osw]:
            print("cg", cg_osw, "en overstow dans", sb_osw, "devrait être dans le dictionnaire du onboard")
        for pod_seq_osw, (quantity_osw, weight_osw) in d_pod_osw.items():
            # 1) réduire la baie / cg dans le dictionnaire de départ de la quantité trouvée 
            new_quantity = d_sb_2_cg_real_ob[sb_osw][cg_osw][0] - quantity_osw
            new_weight = d_sb_2_cg_real_ob[sb_osw][cg_osw][1] - weight_osw
            # et même supprimer si plus rien dedans
            if new_quantity <= 0:
                del d_sb_2_cg_real_ob[sb_osw][cg_osw]
            else:
                d_sb_2_cg_real_ob[sb_osw][cg_osw] = (new_quantity, new_weight)
                    
            # 2) créer à la place (ou incrémenter) un on-board avec le pod réel, et non d'overstow
            cg_real = (cg_osw[0], pod_seq_osw, cg_osw[2], cg_osw[3], cg_osw[4], cg_osw[5])
            if cg_real not in d_sb_2_cg_real_ob[sb_osw]:
                d_sb_2_cg_real_ob[sb_osw][cg_real] = (quantity_osw, weight_osw)
            else:
                new_quantity = d_sb_2_cg_real_ob[sb_osw][cg_real][0] + quantity_osw
                new_weight = d_sb_2_cg_real_ob[sb_osw][cg_real][1] + weight_osw
                d_sb_2_cg_real_ob[sb_osw][cg_real] = (new_quantity, new_weight)

# maintenant, retouche de la loadlist
#d_sb_2_cg_real_ll = copy.deepcopy({subbay: d_cg for subbay, d_cg in d_sb_2_cg.items()\
#                                   if subbay == ''})  
d_sb_2_cg_real_ll = {}
for subbay, d_cg in d_sb_2_cg.items():
    if subbay != '': continue
    d_sb_2_cg_real_ll[subbay] = {}
    for cg, (quantity, weight) in d_cg.items():
        d_sb_2_cg_real_ll[subbay][cg] = (quantity, weight)    

# on enlève tous les mouvements d'overstow (pour les remettre ensuite)
for cg, (quantity_osw, weight_osw) in d_cg_org_os.items():
    if cg in d_sb_2_cg_real_ll['']:
        new_quantity = d_sb_2_cg_real_ll[''][cg][0] - quantity_osw
        new_weight = d_sb_2_cg_real_ll[''][cg][1] - weight_osw
        if new_quantity <= 0:
            del d_sb_2_cg_real_ll[''][cg]
        else:
            d_sb_2_cg_real_ll[''][cg] = (new_quantity, new_weight)

# remettre ensemble les deux parties
#d_sb_2_cg = d_sb_2_cg_real_ob.copy()
#d_sb_2_cg.update(d_sb_2_cg_real_ll)
d_sb_2_cg = {}
for subbay, d_cg in d_sb_2_cg_real_ob.items():
    d_sb_2_cg[subbay] = {}
    for cg, (quantity, weight) in d_cg.items():
        d_sb_2_cg[subbay][cg] = (quantity, weight)
for subbay, d_cg in d_sb_2_cg_real_ll.items():
    d_sb_2_cg[subbay] = {}
    for cg, (quantity, weight) in d_cg.items():
        d_sb_2_cg[subbay][cg] = (quantity, weight)


# et trier en liste
l_subbays_cg = [(subbay, d_cg) for subbay, d_cg in d_sb_2_cg.items()]
l_subbays_cg.sort(key=lambda x: x[0])

# Restow à cause de l'overstow
# on doit constituer deux séries de données :
# les données d'information de restow liés aux cg bloquants (sur la pontée)
# l'impact en terme de chargement-déchargement supplémentaire des blocs bloquants
# là où ils doivent être restowé, à cause de ceux du dessus

d_overstow = {}
d_overstow_moves = {}

for (subbay, d_cg) in l_subbays_cg:
    
    # garbage to be ignored
    if subbay == "": continue
    
    l_subbays_4_below = get_subbays_4_below(subbay)
    for cg, (quantity, weight) in d_cg.items():
        
        pol_seq = cg[0]
        pod_seq = cg[1]
        
        # if the pod_seq is lower than the current position, it means that we are looking beyond 
        # the horizon
        if pod_seq < PORT_SEQ: pod_seq += len(d_seq_2_port_name)
        
        # find the first port where the restow must be done, it will be decreased
        overstow_pod_seq = 999
        # looking subbay below (if exist)
        for subbay_4_below in l_subbays_4_below: # 0 or 1 subbay
            
            if subbay_4_below not in d_sb_2_cg: continue # for bays 74 and 94, and empt baies below
            
            d_cg_4_below = d_sb_2_cg[subbay_4_below]
            for cg_4_below, (quantity_4b, weight_4b) in d_cg_4_below.items():
                
                pol_seq_4b = cg_4_below[0]
                pod_seq_4b = cg_4_below[1]
                # going beyond the horizon
                if pod_seq_4b < PORT_SEQ: pod_seq_4b += len(d_seq_2_port_name)
                
                # overstow if the cg in the hold is to be discharged before that on the deck
                if pod_seq_4b < pod_seq:
                    # and port to be taken at minimum
                    if pod_seq_4b < overstow_pod_seq:
                        overstow_pod_seq = pod_seq_4b
        
        # instead of looking at all overstows, we are only interested at those done at
        # next port
        # NO, we keep all ports
        #if overstow_pod_seq == PORT_SEQ + 1:
        #but we must only consider those overstows within the horizon, 
        # if the overstow port is numerically below the current sequence, it means beyond the horizon
        # we must not consider it

        if overstow_pod_seq < len(d_seq_2_port_name) and overstow_pod_seq > PORT_SEQ:
            
            if (subbay, cg) not in d_overstow:
                d_overstow[(subbay, cg)] = (quantity, weight, overstow_pod_seq)
                
            # mouvement supplémentaire sur une sous-baie à vide, avec pol = port d'overstow
            mvt_cg = (overstow_pod_seq, cg[1], cg[2], cg[3], cg[4], cg[5])
            if mvt_cg not in d_overstow_moves:
                d_overstow_moves[mvt_cg] = (0, 0.0, {})
            mvt_cg_total_overstow_quantity = d_overstow_moves[mvt_cg][0] + quantity 
            mvt_cg_total_overstow_weight = d_overstow_moves[mvt_cg][1] + weight 
            mvt_cg_total_overstow_sources = d_overstow_moves[mvt_cg][2]
            if (subbay, cg[0]) not in d_overstow_moves[mvt_cg][2]:
                mvt_cg_total_overstow_sources[(subbay, cg[0])] = (quantity, weight)
            else:
                mvt_cg_total_overstow_sources[(subbay, cg[0])] =\
                (d_overstow_moves[mvt_cg][2][(subbay, cg[0])][0] + quantity,
                 d_overstow_moves[mvt_cg][2][(subbay, cg[0])][1] + weight)
            d_overstow_moves[mvt_cg] = (mvt_cg_total_overstow_quantity, mvt_cg_total_overstow_weight,
                                        mvt_cg_total_overstow_sources)
            #if mvt_cg[2] == '40' and mvt_cg[3] == 'GP' and mvt_cg[4] == 'L' and mvt_cg[5] == '' :
            #    print("NV MVMTS: 40 GP L -")
            #    print(mvt_cg, d_overstow_moves[mvt_cg])
            #    print("")
            

# pour n'avoir qu'une seule ligne par combinaison sous-baie X cg
# il faut d'abord combiner les on-board initials et ceux issus d'overstow              

d_subbay_xcg = {}

# on a aussi besoin de tracer les CG qui n'étaient qu'à bord et qui sont à
# recharger du fait de l'overstow
set_cg_overstow_moves_done = set()


for (subbay, d_cg) in l_subbays_cg:
    for cg, (quantity, weight) in d_cg.items():    
        
        pol_seq = cg[0]
        pol_name = port_seq_2_name(pol_seq, LEG_NO)
        pod_seq = cg[1]
        pod_name = port_seq_2_name(pod_seq, LEG_NO)
        size = cg[2]
        c_type = cg[3]
        c_weight = cg[4]
        height = cg[5]
        
        # si subbay non renseignée (loadlist, à charger), intégrer l'ensemble des
        # overstow du conteneur group dans la sous-baie vide, en quantités et 
        # poids supplémentaire
        additional_quantity = 0
        additional_weight = 0.0
        additional_sources = {}
        if subbay == '':
            if cg in d_overstow_moves:
                additional_quantity = d_overstow_moves[cg][0]
                additional_weight = d_overstow_moves[cg][1]
                additional_sources = d_overstow_moves[cg][2]
                # done
                set_cg_overstow_moves_done.add(cg)
        total_quantity = quantity + additional_quantity
        total_weight = weight + additional_weight
        
        # overstow, usually no overstow,
        # except for some bays and subbays
        overstow = ''
        overstow_pod_name = ''
        quantity_overstow = 0
        weight_overstow = 0.0
        sources_overstow = {}
        
        # il faut traiter des overstow, qui sont comme tous nouveaux
        if (subbay, cg) in d_overstow\
        or (subbay == '' and cg in d_overstow_moves):

            if (subbay, cg) in d_overstow:
                overstow_pod_seq = d_overstow[(subbay, cg)][2]
                if overstow_pod_seq < 999:
                    overstow_pod_name = port_seq_2_name(overstow_pod_seq, LEG_NO)
                    pod_name = overstow_pod_name
                quantity_overstow += d_overstow[(subbay, cg)][0]
                weight_overstow += d_overstow[(subbay, cg)][1]
                #if subbay == '5431':
                #    print("OVERSTOW SB 5431", cg, quantity_overstow)
            if subbay == '' and cg in d_overstow_moves:
                quantity_overstow += additional_quantity
                weight_overstow += additional_weight
                for (sb_source, pol_source), (add_quantity_source, add_weight_source) in additional_sources.items():
                    if (sb_source, pol_source) not in sources_overstow:
                        sources_overstow[(sb_source, pol_source)] = (0, 0.0)
                    sources_overstow[(sb_source, pol_source)] =\
                    (sources_overstow[(sb_source, pol_source)][0]+add_quantity_source,
                     sources_overstow[(sb_source, pol_source)][1]+add_weight_source) 
                    #if sb_source == '5431':
                    #    print("OVERSTOW MOVES SB '' (5431)", cg, add_quantity_source)
                
            
            if quantity_overstow > 0:
                overstow = 'X'
            
            
        xcg = (pol_name, pod_name, cg[2], cg[3], cg[4], cg[5])
        if (subbay, xcg) not in d_subbay_xcg:
            d_subbay_xcg[(subbay, xcg)] = (0, 0.0, '', '', 0, 0.0, {})
            
        n_total_quantity = d_subbay_xcg[(subbay, xcg)][0] + total_quantity
        n_total_weight = d_subbay_xcg[(subbay, xcg)][1] + total_weight
        n_overstow = d_subbay_xcg[(subbay, xcg)][2]
        if overstow != '': n_overstow = overstow
        n_overstow_pod_name = d_subbay_xcg[(subbay, xcg)][3]
        if overstow_pod_name != '': n_overstow_pod_name = overstow_pod_name
        n_quantity_overstow = d_subbay_xcg[(subbay, xcg)][4] + quantity_overstow
        n_weight_overstow = d_subbay_xcg[(subbay, xcg)][5] + weight_overstow
        n_sources_overstow = d_subbay_xcg[(subbay, xcg)][6]
        for (sb_source, pol_source), (quantity_source, weight_source) in sources_overstow.items():
            if (sb_source, pol_source) not in n_sources_overstow:
                n_sources_overstow[(sb_source, pol_source)] = (0, 0.0)
            n_sources_overstow[(sb_source, pol_source)] =\
            (n_sources_overstow[(sb_source, pol_source)][0]+quantity_source,
             n_sources_overstow[(sb_source, pol_source)][1]+weight_source)
        
                
        d_subbay_xcg[(subbay, xcg)] = (n_total_quantity, n_total_weight,\
        n_overstow, n_overstow_pod_name, n_quantity_overstow, n_weight_overstow,\
        n_sources_overstow)
            
        
# reste à traiter les CG qui n'étaient pas déjà dans une baie vide (loadlist), 
# mais en overstow par ailleurs (donc pas d'origine non plus à ce niveau)
for cg, (quantity, weight, sources) in d_overstow_moves.items(): 
    if cg not in set_cg_overstow_moves_done:
        
        subbay = ''
        
        pol_seq = cg[0]
        pol_name = port_seq_2_name(pol_seq, LEG_NO)
        pod_seq = cg[1]
        pod_name = port_seq_2_name(pod_seq, LEG_NO)
        size = cg[2]
        c_type = cg[3]
        c_weight = cg[4]
        height = cg[5]
        
        overstow = 'X'
        overstow_pod_name = ''
        
        # par prudence
        xcg = (pol_name, pod_name, cg[2], cg[3], cg[4], cg[5])            
        if (subbay, xcg) not in d_subbay_xcg:
            d_subbay_xcg[(subbay, xcg)] = (0, 0.0, '', '', 0, 0.0, {})
            
        n_total_quantity = d_subbay_xcg[(subbay, xcg)][0] + quantity
        n_total_weight = d_subbay_xcg[(subbay, xcg)][1] + weight
        n_overstow = d_subbay_xcg[(subbay, xcg)][2]
        if overstow != '': n_overstow = overstow
        # garantir le port à '' dans tous les cas
        n_overstow_pod_name = overstow_pod_name
        n_quantity_overstow = d_subbay_xcg[(subbay, xcg)][4] + quantity
        n_weight_overstow = d_subbay_xcg[(subbay, xcg)][5] + weight
        n_sources_overstow = d_subbay_xcg[(subbay, xcg)][6]
        for (sb_source, pol_source), (quantity_source, weight_source) in sources.items():
            if (sb_source, pol_source) not in n_sources_overstow:
                n_sources_overstow[(sb_source, pol_source)] = (0, 0.0)
            n_sources_overstow[(sb_source, pol_source)] =\
            (n_sources_overstow[(sb_source, pol_source)][0]+quantity_source,
             n_sources_overstow[(sb_source, pol_source)][1]+weight_source)
                
        d_subbay_xcg[(subbay, xcg)] = (n_total_quantity, n_total_weight,\
        n_overstow, n_overstow_pod_name, n_quantity_overstow, n_weight_overstow,\
        n_sources_overstow)
        

# trier par subbay et cg
l_subbay_xcg = list(d_subbay_xcg.items())
l_subbay_xcg.sort()

### réécriture du fichier complémentaire .csv du on-board avec 2 nouvelles colonnes<p>
# - Overstow (X or empty)
# - Overstow POD (Name of the first port where overstow is needed)
##### Et avec le conteneur groupe sous la forme de colonnes séparées POL, POD, size, type, height category
############################## 
# Ecriture définitive

os.chdir(REP_DATA_ADD)
file_name_add = FILE_NAME_ADD_ROOT + ' ' + str(PORT_SEQ) + ".csv"
f_add_master = open(file_name_add, 'w')

s_header = "Subbay;LoadPort;DischPort;" +\
           "Size;cType;cWeight;Height;" +\
           "AvgWeightInSubbay;QuantityInSubbay;WeightInSubbay;" +\
           "Overstow;OverstowPOD;QuantityOverstow;WeightOverstow;SourcesOverstow\n"
f_add_master.write(s_header)

for (subbay, xcg), (total_quantity, total_weight,\
    overstow, overstow_pod_name, quantity_overstow, weight_overstow, sources_overstow)\
    in l_subbay_xcg:

    # writing
    avg_weight = total_weight / total_quantity if total_quantity > 0 else 0
    s_quantity_overstow = "%d" % quantity_overstow if quantity_overstow > 0 else ''
    s_weight_overstow = "%.3f" % weight_overstow if weight_overstow > 0 else ''
    s_sources_overstow = ""
    for (sb_source, pol_source), (q_source, w_source) in sources_overstow.items():
        s_sources_overstow += "%s-%s-%d-%.3f" %\
        (sb_source, port_seq_2_name(pol_source, LEG_NO), q_source, w_source)
        s_sources_overstow += "|"
    # last | to remove
    if len(s_sources_overstow) > 0:
        s_sources_overstow = s_sources_overstow[:-1]
    
    s_line = "%s;%s;%s;%s;%s;%s;%s;%.3f;%d;%.3f;%s;%s;%s;%s;%s\n" %\
             (subbay, xcg[0], xcg[1], xcg[2], xcg[3], xcg[4], xcg[5],\
              avg_weight, total_quantity, total_weight,\
              overstow, overstow_pod_name,s_quantity_overstow,s_weight_overstow,\
              s_sources_overstow)
    f_add_master.write(s_line)

f_add_master.close()




