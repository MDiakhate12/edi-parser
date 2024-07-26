import logging
import pandas as pd

TEU_UNIT = 20

class RestowComputation:

    def __init__(self, logger: logging.Logger) -> None:
        
        self.logger = logger
        
    def enrich_with_utility_columns(self, df, **kwargs):
        """
            Enrichit le DataFrame avec des colonnes utilitaires définies par les arguments kwargs.

            Chaque clé dans kwargs est le nom de la colonne et chaque valeur est une fonction lambda 
            qui effectue le calcul correspondant.

            Parameters:
                df (pd.DataFrame): Le DataFrame à enrichir.
                types_dict (dict): Un dictionnaire où les clés sont les noms de colonnes et les valeurs sont les types souhaités.
                **kwargs: Les colonnes à ajouter avec leurs calculs correspondants sous forme de lambda.

            Returns:
                pd.DataFrame: Le DataFrame enrichi.
            """
        
        self.logger.info(f"Calcul des colonnes utilitaires suivants : {kwargs.keys()}")   
        
        for column_name, calculation in kwargs.items():
            df[column_name] = df.apply(calculation, axis=1)

        return df


    def compute_restow(self, df: pd.DataFrame):

        # Conversion des types de données pour enrichir les données des conteneurs
        self.logger.info("Conversion des types de données pour enrichir les données des conteneurs")

        types_dict = {
            "Weight": "float",
            "MaxWeight": "float",
            "maxWeight20": "float",
            "maxWeight40": "float",
            "Size": "int",
        }

        df_converted = df.astype(types_dict)

        # Enrichissement du DataFrame pour préparer le calcul du restow
        self.logger.info("Enrichissement du DataFrame pour préparer le calcul du restow")

        utility_colums = {
            # Combine la sous-bay et la row pour créer une colonne Stack unique
            "Stack": lambda row: row["SubBay"] + row["Row"],

            # Calcule la taille si le conteneur est lourd, sinon 0
            "SizeIfHeavyElseZero": lambda row: row["Size"] * (row["cWeight"] == "H"),

            # Calcule la taille si le conteneur est léger, sinon 0
            "SizeIfLightElseZero": lambda row: row["Size"] * (row["cWeight"] == "L"),

            # Calcule le poids si le conteneur est lourd, sinon 0
            "WeightIfHeavyElseZero": lambda row: row["Weight"] * (row["cWeight"] == "H"),

            # Calcule le poids maximal de la sous-bay
            "MaxWeightOfSubbay": lambda row: row["maxWeight20"] + 2 * row["maxWeight40"], # doit être <= 2 * maxWeight40
        }

        df_containers_enriched = self.enrich_with_utility_columns(
            df=df_converted,
            **utility_colums,
        )

        # Filtre les conteneurs en cale (MacroTier == 1) et les trie par HatchSection
        self.logger.info("Filtre les conteneurs en cale (MacroTier == 0) et les trie par HatchSection")

        df_containers_hold = df_containers_enriched[df_containers_enriched["MacroTier"] == "0"].sort_values(by="HatchSection")

        # Filtre les conteneurs sur le pont (MacroTier == 0) et les trie par HatchSection
        self.logger.info("Filtre les conteneurs sur le pont (MacroTier == 1) et les trie par HatchSection")

        df_containers_deck = df_containers_enriched[df_containers_enriched["MacroTier"] == "1"].sort_values(by="HatchSection")


        # Conditions des stacks et sous-baies non eligibles
        condition_cDG_1_7 = lambda cDG: ((cDG.str.startswith("1")) | (cDG.str.startswith("7"))).any() 
        condition_OOG_LEFT = lambda OOG_LEFT: (OOG_LEFT == "1").any()
        condition_OOG_RIGHT = lambda OOG_RIGHT: (OOG_RIGHT == "1").any()
        # TODO Add breakbulk conditions


        # DataFrame des conteneurs en cale groupés par stack
        self.logger.info("DataFrame des conteneurs en cale groupés par stack")

        df_container_hold_grouped_by_stack = (
            df_containers_hold.groupby(["Stack", "Row", "SubBay", "HatchSection"])
            .agg(
                # Nombre total de conteneurs dans la stack
                nb_containers_on_hold=("Container", "count"),

                # Nombre de conteneurs lourds ("H") dans la stack
                nb_heavy_containers_on_hold=(
                    "cWeight", 
                    lambda cWeight: (cWeight == "H").sum(),
                ),

                # Vrai si la stack contient des conteneurs dangereux (cDG entre 1 et 7)
                hold_stack_contains_dangeourous_1_7=(
                    "cDG", 
                    condition_cDG_1_7,
                ),

                # Vrai si la stack contient des conteneurs hors gabarit à gauche
                hold_stack_contains_oog_left=(
                    "OOG_LEFT", 
                    condition_OOG_LEFT,
                ),

                # Vrai si la stack contient des conteneurs hors gabarit à droite
                hold_stack_contains_oog_right=(
                    "OOG_RIGHT", 
                    condition_OOG_RIGHT,
                ),

                # Poids total des conteneurs lourds ("H") dans la stack
                total_weight_heavy_on_hold_stack=(
                    "WeightIfHeavyElseZero", 
                    lambda Weight: Weight.sum(),
                ),


                # Poids maximal parmi tous les conteneurs dans la stack
                hold_stack_max_weight=(
                    "MaxWeight", 
                    lambda MaxWeight: MaxWeight.max(),
                ),
            )
            .reset_index() 
        )

        # Calcule le poids restant pour chaque pile de conteneurs en cale
        self.logger.info("Calcule le poids restant pour chaque pile de conteneurs en cale") 
      

        df_container_hold_grouped_by_stack["remaining_weight_on_hold_stack"] = (
            df_container_hold_grouped_by_stack["hold_stack_max_weight"]
            - df_container_hold_grouped_by_stack["total_weight_heavy_on_hold_stack"]
        )

        # Vérifie si le poids restant dans chaque pile de conteneurs en cale est supérieur ou égal à 30
        self.logger.info("Vérifie si le poids restant dans chaque pile de conteneurs en cale est supérieur ou égal à 30")

        df_container_hold_grouped_by_stack["stack_remaining_weight_greater_30"] = (
            df_container_hold_grouped_by_stack["remaining_weight_on_hold_stack"] >= 30
        )

        # Détermine si une pile de conteneurs en cale est restowable
        self.logger.info("Détermine si une pile de conteneurs en cale est restowable")

        df_container_hold_grouped_by_stack["hold_stack_is_restowable"] = (
            (df_container_hold_grouped_by_stack["stack_remaining_weight_greater_30"] == True)
            & (df_container_hold_grouped_by_stack["hold_stack_contains_dangeourous_1_7"] == False)
            & (df_container_hold_grouped_by_stack["hold_stack_contains_oog_left"] == False)
            & (df_container_hold_grouped_by_stack["hold_stack_contains_oog_right"] == False)
        )


        # DataFrame des conteneurs sur le pont groupés par hatch_section et par sous_baie
        self.logger.info("DataFrame des conteneurs sur le pont groupés par hatch_section et par sous_baie")

        df_container_deck_grouped_by_subbay = (
            df_containers_deck.groupby(["HatchSection", "SubBay"])
            .agg(

                # Nombre de conteneurs sur le pont
                nb_containers_on_deck=("Container", "count"),


                # La sous-baie sur le pont contient des marchandises dangereuses de classe 1 à 7 (radioexplosifs)
                deck_subbay_contains_dangeourous_1_7=(
                    "cDG",
                    condition_cDG_1_7,
                ),


                # La sous-baie sur le pont contient des conteneurs hors gabarit à gauche
                deck_subbay_contains_oog_left=(
                    "OOG_LEFT",
                    condition_OOG_LEFT,
                ),


                # La sous-baie sur le pont contient des conteneurs hors gabarit à droite
                deck_subbay_contains_oog_right=(
                    "OOG_RIGHT",
                    condition_OOG_RIGHT,
                ),
            )
            .reset_index()
            .rename(
                columns={
                    "HatchSection": "hatch_section",
                    "SubBay": "deck_subbay",
                },
            )
        )


        df_container_deck_grouped_by_subbay["deck_subbay_is_restowable"] = (

            # La sous-baie sur le pont est redéployable si elle ne contient pas de marchandises dangereuses de classe 1 à 7
            (df_container_deck_grouped_by_subbay["deck_subbay_contains_dangeourous_1_7"] == False)

            # Et si elle ne contient pas de conteneurs hors gabarit à gauche
            & (df_container_deck_grouped_by_subbay["deck_subbay_contains_oog_left"] == False)

            # Et si elle ne contient pas de conteneurs hors gabarit à droite
            & (df_container_deck_grouped_by_subbay["deck_subbay_contains_oog_right"] == False)
        )

        # DataFrame des conteneurs en cale groupés par hatch_section et par sous_baie
        self.logger.info("DataFrame des conteneurs en cale groupés par hatch_section et par sous_baie")

        df_container_hold_grouped_by_subbay = (
            df_containers_hold.merge(
                df_container_hold_grouped_by_stack[[
                    "HatchSection", 
                    "Stack", 
                    "hold_stack_contains_dangeourous_1_7",
                    "hold_stack_contains_oog_left",
                    "hold_stack_contains_oog_right",
                    "hold_stack_is_restowable",
                ]],
                how="left",
                on=["Stack", "HatchSection"],
            )
            .groupby(["SubBay", "HatchSection"])
            .agg(
                # Le nombre de piles (stacks) dans la sous-baie de la cale
                nbStacks=("nbStacks", "first"),
                
                # Ensemble des piles (stacks) dans la sous-baie de la cale
                Stacks=("Stack", lambda x: set(x)),
                
                # Nombre de conteneurs dans la sous-baie de la cale
                nb_containers_on_hold=("Container", "count"),

                # Nombre de conteneurs lourds dans la sous-baie de la cale
                nb_heavy_containers_on_hold=(
                    "cWeight",
                    lambda cWeight: (cWeight == "H").sum(),
                ),

                # Nombre de conteneurs légers dans la sous-baie de la cale
                nb_light_containers_on_hold=(
                    "cWeight",
                    lambda cWeight: (cWeight == "L").sum(),
                ),

                # Nombre total d'équivalents vingt pieds de conteneurs dans la sous-baie de la cale
                teus_containers_on_hold=(
                    "Size",
                    lambda Size: (Size.sum() // TEU_UNIT),
                ),
                
                # Nombre total d'équivalents vingt pieds de conteneurs lourds dans la sous-baie de la cale
                teus_heavy_containers_on_hold=(
                    "SizeIfHeavyElseZero",
                    lambda Size: Size.sum() // TEU_UNIT,
                ),

                # Nombre total d'EVP de conteneurs légers dans la sous-baie de la cale
                teus_light_containers_on_hold=(
                    "SizeIfLightElseZero",
                    lambda Size: Size.sum() // TEU_UNIT,
                ),

                # Poids total des conteneurs lourds dans la sous-baie de la cale
                total_weight_heavy_on_hold=(
                    "WeightIfHeavyElseZero",
                    lambda Weight: Weight.sum(),
                ),

                # Poids maximum total supporté par la sous-baie de la cale
                total_weight_loadable_on_hold=("MaxWeightOfSubbay", "first"),

                # Capacité totale en EVP de la section de trappe
                teus_hatch_section_capacity=("teus_subbay_capacity", "first"),

                hold_subbay_contains_dangeourous_1_7=(
                    "hold_stack_contains_dangeourous_1_7",
                    lambda x: x.any(),
                ),
                hold_subbay_contains_oog_left=(
                    "hold_stack_contains_oog_left",
                    lambda x: x.any(),
                ),
                hold_subbay_contains_oog_right=(
                    "hold_stack_contains_oog_right",
                    lambda x: x.any(),
                ),

                hold_subbay_is_restowable=(
                    "hold_stack_is_restowable",
                    lambda x: x.any(),
                ),
            )
            .reset_index()
            .rename(
                columns={
                    "HatchSection": "hatch_section",
                    "SubBay": "hold_subbay",
                },
            )
        )


        # Calcul du nombre total d'équivalent vingt pieds vides dans la sous-baie de la cale (hatch_section_capacity - containers_on_hold)
        self.logger.info("Calcul du nombre total d'équivalent vingt pieds vides dans la sous-baie de la cale (hatch_section_capacity - containers_on_hold)")

        df_container_hold_grouped_by_subbay["teus_empty_on_hold"] = (
            df_container_hold_grouped_by_subbay["teus_hatch_section_capacity"]
            - df_container_hold_grouped_by_subbay["teus_containers_on_hold"]
        ).clip(lower=0)


        # Calcul du poids supplémentaire pouvant être chargé dans la sous-baie de la cale (total_weight - total_weight_of_heavy)
        self.logger.info("Calcul du poids supplémentaire pouvant être chargé dans la sous-baie de la cale (total_weight - total_weight_of_heavy)")

        df_container_hold_grouped_by_subbay["extra_weight_loadable_on_hold"] = (
            df_container_hold_grouped_by_subbay["total_weight_loadable_on_hold"]
            - df_container_hold_grouped_by_subbay["total_weight_heavy_on_hold"]
        )


        # Ordre des colonnes
        self.logger.info("Ordre des colonnes")

        columns = [
            "hatch_section",
            "hold_subbay",
            "deck_subbay",
            "nb_containers_on_deck",
            "nb_containers_on_hold",
            "nb_heavy_containers_on_hold",
            "teus_heavy_containers_on_hold",
            "nb_light_containers_on_hold",
            "teus_light_containers_on_hold",
            # "nb_empty_hold_positions",
            "teus_empty_on_hold",
            # "highest_hold_heavy_destination",
            "total_weight_heavy_on_hold",
            "total_weight_loadable_on_hold",
            "extra_weight_loadable_on_hold",
            "deck_subbay_contains_dangeourous_1_7",
            "deck_subbay_contains_oog_left",
            "deck_subbay_contains_oog_right",
            "hold_subbay_contains_dangeourous_1_7",
            "hold_subbay_contains_oog_left",
            "hold_subbay_contains_oog_right",
            "deck_subbay_is_restowable",
            "hold_subbay_is_restowable",
        ]

        # Merger les données des conteneurs de la cale et du pont par section de trappe (hatch section)
        self.logger.info("Merger les données des conteneurs de la cale et du pont par section de trappe (hatch section)")

        df_restow = (
            df_container_hold_grouped_by_subbay.merge(
                df_container_deck_grouped_by_subbay,
                how="left",
                on=["hatch_section"],
            )
            .reset_index()
            [columns]
        )

        df_restow["subbay_contains_cDG_1_7"] = df_restow["deck_subbay_contains_dangeourous_1_7"] | df_restow["hold_subbay_contains_dangeourous_1_7"]
        df_restow["subbay_contains_oog_left"] = df_restow["deck_subbay_contains_oog_left"] | df_restow["hold_subbay_contains_oog_left"]
        df_restow["subbay_contains_oog_right"] = df_restow["deck_subbay_contains_oog_right"] | df_restow["hold_subbay_contains_oog_right"]

        df_restow.drop(
            columns=[
              "deck_subbay_contains_dangeourous_1_7",
              "deck_subbay_contains_oog_left",
              "deck_subbay_contains_oog_right",
              "hold_subbay_contains_dangeourous_1_7",
              "hold_subbay_contains_oog_left",
              "hold_subbay_contains_oog_right",
            ],
            axis=1,
            inplace=True,
        )

        # Fonction de calcul du ratio
        self.logger.info("Calcul du ratio de restowage des conteneurs lourds en cale")

        def compute_ratio_restow_heavy_hold(df):

            nb_containers_to_restow = df["nb_light_containers_on_hold"] + df["nb_containers_on_deck"]

            nb_teus_to_gain_on_hold = (df["teus_empty_on_hold"] + df["teus_light_containers_on_hold"])

            return nb_containers_to_restow / nb_teus_to_gain_on_hold

        
        # Calcul du ratio de restowage des conteneurs lourds en cale
        df_restow["ratio_restow_heavy_hold"] = compute_ratio_restow_heavy_hold(df_restow)

            
        # Détermine si une sous-baie est restowable en fonction des critères de restowage pour la cale et le pont
        self.logger.info("Détermine si une sous-baie est restowable en fonction des critères de restowage pour la cale et le pont")

        df_restow["is_restowable"] = (
            df_restow["hold_subbay_is_restowable"] & df_restow["deck_subbay_is_restowable"]
        )

        return df_restow