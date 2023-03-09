# libraries
import os
#import copy
import sys


# directory and file names
##################
# TO BE MODIFIED
# 0 : si situation initiale, onboard de MYPKG et en entrée de CNTXG, 
# situation initiale : VUE AU NIVEAU DE L'ENTREE A CPLEX-MASTER PLANNING
# A ce niveau, CNTXG vaut 1 !!! 
# 1 : si situation initiale en sortie de CNTXG
# A ce niveau, CNTXG vaut 0 = 13 !!!
# C'EST UN NUMERO ABSOLU
LEG_NO = 0

# No de séquence courant
# C'EST UN NUMERO RELATIF
# Le premier de la série, c'est quand on reçoit master planning 1, en sortie
# du premier port après traitement (en absolu, en sortie du port LEG_NO + 1)
# PORT_SEQ est à faire varier entre 1 et jusqu'à l'horizon souhaité
PORT_SEQ = 1

# on board initial et load lists à charger

# on board initial, en entrée du port traité, PORT_SEQ-1
# on ne lit que le onboard, et on retire les conteneurs avec POD = PORT_SEQ
REP_DATA_INIT = "c:/Projets/CCS/Vessel Stowage/KPI/v50-CNTXG"
FILE_NAME_INIT_ROOT = "9454450 Container Groups Completed"
SOURCE_INIT = "MP_OS"

# résultat du master planning qui est ici une liste de load lists
REP_DATA_MP_LL = "c:/Projets/CCS/Vessel Stowage/KPI/v50-CNTXG"
FILE_NAME_MP_LL = "MasterPlanning.csv"
# pour sortie non globale
SOURCE_MP = "OLD_MP"

# création du nouveau master planning réel (on-board)
REP_DATA_MP_OB = "c:/Projets/CCS/Vessel Stowage/KPI/v50-CNTXG"
FILE_NAME_MP_OB_ROOT = "MasterPlanning"

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



################################################################################
# le chargement peut se faire :
# soit dans une vision on-board. 
# On élimine alors les conteneurs groupes dans les sous-baies vides, si on est en chargement initial
# (port_seq = 0)
# soit dans une vision loadlist (issue du fichier consolidé)
# on ne garde que les conteneurs encore à charger après port_seq. A 0, on prend tout (pol > 0),
# en sortie de 1, on prend pol > 1, etc.

def load_MP_results(d_sb_2_cg,
                    fn_results, onboard_loadlist, port_seq, source, leg_no):
    
    
    f_results = open(fn_results, 'r')
    
    for no_row, row in enumerate(f_results):
        
        if no_row == 0: continue
            
        l_items = row.split(';')
        
        if source == "OLD_MP":
            
            # port de chargement
            current_port_name = l_items[0].strip()
            current_port_seq = port_name_2_seq(current_port_name, leg_no)
            
            subbay = l_items[1].strip() # sur 4 caractères (ou 0 si vide)
            if len(subbay) == 3: subbay = '0' + subbay
            
            container_group_data = l_items[2].strip()[1:-1]
            l_elems = container_group_data.split(',')
            pol_seq = int(l_elems[0].strip())
            pod_seq = int(l_elems[1].strip())
            c_size = l_elems[2].strip()
            c_type = l_elems[3].strip()
            c_weight = l_elems[4].strip()
            hc = l_elems[6].strip()
             # hence the container group
            container_group = (pol_seq, pod_seq, c_size, c_type, c_weight, hc)
            
            if current_port_seq != pol_seq:
                print("incohérence load list master planning pour:",
                      port_name, subbay, container_group)
            
            avg_weight = float(l_elems[5].strip())
            quantity = int(l_items[3].strip())
            weight = quantity * avg_weight
            
        
        if source in ["MP_OS"]:
            
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
            

        # éliminer / conserver
        
        # si onboard, on garde les sous-baies effectivement remplies,
        # et on enlève les déchargements au port courant
        if onboard_loadlist == "ONBOARD":       
            if subbay == '': continue
            if pod_seq == port_seq: continue
        
        # si loadlist, on ne garde que les chargements au port courant
        if onboard_loadlist == "LOADLIST":
            if pol_seq != port_seq: continue
                    
        if subbay not in d_sb_2_cg:
            d_sb_2_cg[subbay] = {}
        if container_group not in d_sb_2_cg[subbay]:
            d_sb_2_cg[subbay][container_group] = (quantity, weight)
        else:
            print("double au niveau de", subbay, container_group)
                
    f_results.close()
    
    return d_sb_2_cg

##########################################
# chargement en pratique
d_sb_2_cg = {}

# d'abord onboard

rep_data = REP_DATA_INIT
file_name = FILE_NAME_INIT_ROOT + ' ' + str(PORT_SEQ-1) + ".csv"
source = SOURCE_INIT
os.chdir(rep_data)
d_sb_2_cg = load_MP_results(d_sb_2_cg,  
                            file_name, "ONBOARD", PORT_SEQ, source, LEG_NO)

# ensuite la loadlist
 
rep_data = REP_DATA_MP_LL
file_name = FILE_NAME_MP_LL
source = SOURCE_MP
os.chdir(rep_data)
d_sb_2_cg = load_MP_results(d_sb_2_cg, 
                            file_name, "LOADLIST", PORT_SEQ, source, LEG_NO)

#for sb, d_cg in d_sb_2_cg.items():
#    if sb == '1811':
#        print(d_cg)
#print("FIN EXAMEN")


# et trier en liste
l_subbays_cg = [(subbay, d_cg) for subbay, d_cg in d_sb_2_cg.items()]
l_subbays_cg.sort(key=lambda x: x[0])


############################## 
# Ecriture définitive du l'on-board réel en sortie,
# comme en sortie de la version globale

os.chdir(REP_DATA_MP_OB)
file_name_mp_ob = FILE_NAME_MP_OB_ROOT + ' ' + str(PORT_SEQ) + ".csv"
f_mp_ob = open(file_name_mp_ob, 'w')

s_header = "Subbay;LoadPort;DischPort;" +\
           "Size;cType;cWeight;Height;" +\
           "AvgWeightInSubbay;QuantityInSubbay;WeightInSubbay\n"
f_mp_ob.write(s_header)

for (subbay, d_cg) in l_subbays_cg:
    
    for cg, (quantity, weight) in d_cg.items():
    
        # writing
        avg_weight = weight / quantity if quantity > 0 else 0
        
        load_port = port_seq_2_name(cg[0], LEG_NO)
        disch_port = port_seq_2_name(cg[1], LEG_NO)
   
        s_line = "%s;%s;%s;%s;%s;%s;%s;%.3f;%d;%.3f\n" %\
             (subbay, load_port, disch_port, cg[2], cg[3], cg[4], cg[5],\
              avg_weight, quantity, weight)
        f_mp_ob.write(s_line)

f_mp_ob.close()




