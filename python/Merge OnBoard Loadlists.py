# libraries
import os


##################
# TO BE MODIFIED

# paramétrage de départ
# Numéro d'étape (de leg), 0 : en sortie de MYPKG (port initial)
# 1 : en sortie de CNTXG (PORT_SEQ = 1)
# Si LEG_NO = N, on charge le on-board en sortie de N, et toutes les loadlists de N+1 à 12 inclus
#LEG_NO = 5
LEG_NO = 4

# directory
REP_DATA = "c:/Projets/CCS/Vessel Stowage/Python Prétraitement/9454450 Rotation"

# Les fichiers retenus en entrée, avant sélection liée à LEG_NO, sont ceux commençant par :
# O_nn_ pour le onboard
# L_nn_ pour les loadlists
# le nom du fichier en sortie est basé sur une racine
FILE_NAME_ONBOARD_LOADLIST_ROOT = "9454450 Containers OnBoard Loadlist"

# plusieurs sources, différentes
# regarder ce qu'on envoie
# porter dans le format :
# nb de lignes du header
# et éventuellement autres données
d_sources = {"MS3": [9], "CMA_1": [12], "CMA_2": [9]}
#SOURCE = "CMA_1"
SOURCE = "CMA_2"

########################################################################

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


### Fonctions de transformation

#### 1) CSV
##### A partir du format :
#(Position,)POL,POD,Serial Number,Type,Weight,Specials
##### Il faut obtenir en sortie le format :
#  ContId;LoadPort;DischPort;Type;Setting;Size;Height;Weight;Slot
#= Serial Number;POL;POD;Type;Specials;A DEDUIRE;A DEDUIRE;Weight
    
# lecture d'un fichier csv (peut-être un onboard initial, ou une loadlist)
# seul format possible pour les loadlists
def read_onboard_loadlist_csv(fn_onboard_loadlist, seq_no):
    
    l_containers_ob_ll = []

    f_onboard_loadlist = open(fn_onboard_loadlist , 'r')
    for no_line, line in enumerate(f_onboard_loadlist):
        
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
        s_weight = l_items[4 + inc]
        setting = l_items[5 + inc]
        
        s_size, s_height = get_container_size_height(c_type)
        
        # for GBSOU, both present in seq 8 and 11, change to SOU2 (leg 11) if:
        # POL: in the on-board file at 0, up to 7, and straring from 11, loaded in SOU2
        # POD: for seq 9 and 10 (and 8, even if it does not make too much sense to have physical pol = physical pod)
        if load_port == "GBSOU" and seq_no not in [8, 9, 10]: load_port += "2"
        if disch_port == "GBSOU" and seq_no in [8, 9, 10]: disch_port += "2"
            
        #if disch_port == "MYPKG": print("Décharge à MYPKG")
        
        l_containers_ob_ll.append((cont_id, load_port, disch_port, 
                                   c_type, setting, s_size, s_height, s_weight, slot))
        
    f_onboard_loadlist.close()

    return l_containers_ob_ll


#### 2) EDI
##### A partir du format edi de BAPLIE, généré par macS3 (condition, format dit MS3)
#Duquel on lit pour chaque conteneur chargé :
#<p>LOC+147+<b>0011020</b>::5' -> Slot 
#<p>MEA+WT++KGM:<b>12000</b>' -> Weight 
#<p>RFF+BM:1'
#<p>EQD+CN++<b>22G0</b>+++<b>5</b>' -> Type (-> Size, Height), -> Setting
#<p> Pas de LoadPort / DischPort, prendre dans l'en-tête :
#<p> LOC+5+<b>FRLEH</b>:139:6' -> LoadPort
#<p> LOC+61+<b>CNXMN</b>:139:6' -> DischPort
    
##### Ou à partir des conteneurs réels (format CMA)
#<p>LOC+147+<b>0151212</b>:9711:5'
#<p>EQD+CN+<b>CRSU1392345</b>:6346:5+<b>22G1</b>:6346:5+++<b>5</b>'
#<p>NAD+CF+CMA:LINES:306'
#<p>MEA+AAE+AET+KGM:<b>22500</b>'
#<p>LOC+9+<b>CNTXG</b>'
#<p>LOC+11+<b>NLRTM</b>'
#<p>TMP+2+-10.0:CEL' (uniquement pour reefers actifs)
#<p>CNT+8:1'

##### Il faut obtenir en sortie le format :
#ContId;LoadPort;DischPort;Type;Setting;Size;Height;Weight;Slot


# lecture d'un fichier edi (normalement un onboard initial, pas une loadlist)
# source : "CMA-1", "CMA-2": onboard given by CMA team, "MS3": MS3 condition
def read_onboard_loadlist_edi(fn_onboard_loadlist, seq_no, source):
    
    l_containers_ob_ll = []
    
    # un seul enregistrement en fait..., la séparation se fait sur des apostrophes
    f_onboard_loadlist = open(fn_onboard_loadlist , 'r')
    l_line = f_onboard_loadlist.readlines()
    f_onboard_loadlist.close()

    # "real" lines, depending on the source
    l_lines = l_line[0].split("'")
    # only for MS3, read header interesting lines to get POL POD
    if source == 'MS3':
        load_port = l_lines[5][6:11]
        disch_port = l_lines[6][7:12]
    # cut the header and the tail
    # depending on the format
    nb_lines_header = d_sources[source][0]
    nb_lines_footer = 2
    l_lines = l_lines[nb_lines_header:-nb_lines_footer]
    
    # besoin du nombre de lignes par cointainer pour savoir quand noter le conteneur dans son entier
    # uniquement pour MS3
    nb_lines_container = 4 if format == 'MS3' else None
    
    # attributes to retrieve, write only if complete
    cont_id = load_port = disch_port = c_type = setting = s_size = s_height = s_weight = slot = None
    for no_line, line in enumerate(l_lines):
        
        if line[0:3] == 'LOC' and line[4:7] == '147':
            
            # écrire le précédent container
            if no_line != 0:
                
                #eventuel incomplétude
                if cont_id is None\
                or load_port is None\
                or disch_port is None\
                or c_type is None\
                or setting is None\
                or s_size is None\
                or s_height is None\
                or s_weight is None\
                or slot is None:
                    print("conteneur incomplet:", slot, cont_id)
                
                l_containers_ob_ll.append((cont_id, load_port, disch_port, 
                                       c_type, setting, s_size, s_height, s_weight, slot))
                cont_id = load_port = disch_port = c_type = setting = s_size = s_height = s_weight = slot = None
                
            # on peut lire le nouveau slot pour le nouveau conteneur
            # not keeping the first 0
            slot = line[9:15]
        
        if line[0:3] == 'MEA':
            l_items = line.split('+')
            s_weight = l_items[3][4:]
            # in tons...
            weight = float(s_weight) / 1000
            s_weight = "%.3f" % weight
        
        if line[0:3] == 'TMP':
            setting = 'R'
        
        if line[0:3] == 'EQD':
            
            # better to split on +
            
            l_items = line.split('+')
            
            c_type = l_items[3][0:4]
            s_size, s_height = get_container_size_height(c_type)
            
            empty_full = l_items[6]
            if empty_full == '4':
                setting = 'E'
            else:
                if setting is None: # not overwriting reefer R if any
                    setting = ''
        
            # also container's last line
            # generate a pseudo id if MS3 condition
            if source == 'MS3':
                cont_id = "C%06d" % ((no_line // nb_lines_container) + 1)
            else:
                l_items_cont = l_items[2].split(':')
                cont_id = l_items_cont[0]
                
        if line[0:3] == 'LOC' and line[4:5] == '9':
            load_port = line[6:11]
            # for explanations, look at the csv function
            if load_port == "GBSOU" and seq_no not in [8, 9, 10]: load_port += "2"
            
        if line[0:3] == 'LOC' and line[4:6] == '11':
            disch_port = line[7:12]
            if disch_port == "GBSOU" and seq_no in [8, 9, 10]: disch_port += "2"
        
        # useless lines    
        if line[0:3] == 'RFF':
            continue
        if line[0:3] == 'GDS':
            continue
        if line[0:3] == 'NAD':
            continue
        if line[0:3] == 'CNT':
            continue
        if line[0:3] == 'HAN': # some happen
            continue
        
    # Il reste à intégrer le dernier conteneur
    if cont_id is None\
    or load_port is None\
    or disch_port is None\
    or c_type is None\
    or setting is None\
    or s_size is None\
    or s_height is None\
    or s_weight is None\
    or slot is None:
        print("conteneur incomplet:", slot, cont_id)         
    l_containers_ob_ll.append((cont_id, load_port, disch_port, 
                               c_type, setting, s_size, s_height, s_weight, slot))

        
    return l_containers_ob_ll


############ Running 
    
os.chdir(REP_DATA)

# only one output file
fn_formatted_onboard_loadlist = "%s %d.csv" % (FILE_NAME_ONBOARD_LOADLIST_ROOT, LEG_NO)

f_formatted_onboard_loadlist = open(fn_formatted_onboard_loadlist, 'w')
s_header = "ContId;LoadPort;DischPort;Type;Setting;Size;Height;Weight;Slot\n"
f_formatted_onboard_loadlist.write(s_header)

# loop on files
l_fn = os.listdir()
for fn_onboard_loadlist in l_fn:
    
    # look at original files, onboard and loadlist
    f_name, f_extension = os.path.splitext(fn_onboard_loadlist)
    if f_name[0:2] not in ['O_', 'L_']:
        continue
    
    # type and sequence
    f_type = f_name[0]
    seq_no = int(f_name[2:4])
    
    # Si LEG_NO = N, on charge le on-board en sortie de N, et toutes les loadlists de N+1 à 12 inclus
    if (f_type == 'O' and seq_no != LEG_NO)\
    or (f_type == 'L' and seq_no <= LEG_NO):
        continue
    
    # depending on the extension:
    if f_extension not in ['.csv', '.edi']: continue
    if f_extension == '.csv':
        l_containers_ob_ll = read_onboard_loadlist_csv(fn_onboard_loadlist, seq_no)
    if f_extension == '.edi':
        l_containers_ob_ll = read_onboard_loadlist_edi(fn_onboard_loadlist, seq_no, SOURCE)    
    
    # la liste des containers est un ensemble de chaines de caractère, même pour le poids
    for (cont_id, load_port, disch_port, c_type, setting, size, height, weight, slot) in l_containers_ob_ll:
        s_line = "%s;%s;%s;%s;%s;%s;%s;%s;%s\n" %\
        (cont_id, load_port, disch_port, c_type, setting, size, height, weight, slot)
        f_formatted_onboard_loadlist.write(s_line)


# close the formatted load list at the end    
f_formatted_onboard_loadlist.close()









