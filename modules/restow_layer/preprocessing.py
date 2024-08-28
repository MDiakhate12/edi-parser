import numpy as np
import logging
import pandas as pd


class Preprocessing:

    def __init__(self, logger: logging.Logger) -> None:
        
        self.logger = logger


    def read_and_preprocessess_stack_data(self, df_stacks):

        df_result = df_stacks.copy()
        
        # Assure que la colonne Bay a 3 chiffres en ajoutant des zéros à gauche si nécessaire (BBB)
        self.logger.info("Assure que la colonne Bay a 3 chiffres en ajoutant des zéros à gauche si nécessaire (BBB)")
        df_result["Bay"] = df_stacks["Bay"].str.zfill(3)
        
        # Assure que la colonne Row a 2 chiffres en ajoutant des zéros à gauche si nécessaire (RR)
        self.logger.info("Assure que la colonne Row a 2 chiffres en ajoutant des zéros à gauche si nécessaire (RR)")
        df_result["Row"] = df_stacks["Row"].str.zfill(2)
        
        # Assure que la colonne FirstTier a 2 chiffres en ajoutant des zéros à gauche si nécessaire (TT)
        self.logger.info("Assure que la colonne FirstTier a 2 chiffres en ajoutant des zéros à gauche si nécessaire (TT)")
        df_result["FirstTier"] = df_stacks["FirstTier"].str.zfill(2)
        
        # Assure que la colonne SubBay a 4 chiffres en ajoutant des zéros à gauche si nécessaire (SSSS)
        self.logger.info(msg="Assure que la colonne SubBay a 4 chiffres en ajoutant des zéros à gauche si nécessaire (SSSS)")
        df_result["SubBay"] = df_stacks["SubBay"].str.zfill(4)
        
        # Extrait les trois premiers chiffres de SubBay pour créer la colonne HatchSection
        self.logger.info("Extrait les trois premiers chiffres de SubBay pour créer la colonne HatchSection")
        df_result["HatchSection"] = df_stacks["SubBay"].str[:-1]
        
        # Renomme la colonne Tier en MacroTier
        self.logger.info("Renomme la colonne Tier en MacroTier")
        df_result = df_result.rename(columns={"Tier": "MacroTier"}).reset_index(drop=True)

        return df_result



    def read_and_preprocessess_subbays_data(self, df_subbays_capacity):

        df_result = df_subbays_capacity.copy()

        # Assure que la colonne bay a 3 chiffres en ajoutant des zéros à gauche si nécessaire
        self.logger.info("Assure que la colonne bay a 3 chiffres en ajoutant des zéros à gauche si nécessaire")
        df_result["bay"] = df_subbays_capacity["bay"].str.zfill(3)

        # Assure que la colonne row a 2 chiffres en ajoutant des zéros à gauche si nécessaire
        self.logger.info("Assure que la colonne row a 2 chiffres en ajoutant des zéros à gauche si nécessaire")
        df_result["row"] = df_subbays_capacity["row"].str.zfill(2)

        # Assure que la colonne subBay a 4 chiffres en ajoutant des zéros à gauche si nécessaire
        self.logger.info("Assure que la colonne subBay a 4 chiffres en ajoutant des zéros à gauche si nécessaire")
        df_result["subBay"] = df_subbays_capacity["subBay"].str.zfill(4)

        
        # Calcul de la capacité totale TEUs par sous-baie
        self.logger.info("Calcul de la capacité totale TEUs par sous-baie")

        df_result = df_result.astype(
            {
                "20'or40'": int,
                "20'only": int,
                "40'only": int,
            }
        )

        df_result["teus_subbay_capacity"] = (
                (2 * df_result["20'or40'"])
                + df_result["20'only"]
                + (2 * df_result["40'only"])
        )

        
        # Renommage des colonnes et réinitialisation de l'index
        self.logger.info("Renommage des colonnes et réinitialisation de l'index")

        df_result = df_result.rename(
            columns={
                "bay": "MacroBay",
                "row": "MacroRow",
                "tier": "MacroTier",
                "subBay": "SubBay",
            },
        ).reset_index(drop=True)

        return df_result


    def remove_containers_without_slot(self, df):

        # Filtrage des conteneurs à bord avec emplacement attribué
        self.logger.info("Filtrage des conteneurs OnBoard (avec un Slot non null)")

        df =  df[
            df["Slot"].notnull() &
            (df["Slot"] != "") &
            (df["Slot"].str.lower() != np.nan) &
            (df["Slot"].str.lower() != "nan") &
            (df["Slot"].str.lower() != "na") 
        ]

        return df


    def compute_bay_row_and_tier(self, df):

        df_result = df.copy()

        # Extraction des informations de Bay, Row, et Tier à partir de la colonne Slot
        self.logger.info("Extraction des informations de Bay, Row, et Tier à partir de la colonne Slot")

        df_result["Tier"] = df["Slot"].apply(lambda slot: slot[-2:])
        df_result["Row"] = df["Slot"].apply(lambda slot: slot[-4:-2])
        df_result["Bay"] = df["Slot"].apply(lambda slot: str(slot[:-4]).zfill(3))

        # Fonction pour déterminer le MacroTier à partir du Slot
        def get_macro_tier(slot: str):
            if slot.isdigit() and len(slot) >= 5:
                tier = slot[-2:]

                # Si le tier est supérieur à 50 alors le conteneur est sur le pont (deck)
                if int(tier) >= 50:
                    return "1"
                
                # Sinon le conteneur est dans la cale
                else:
                    return "0"
            else:
                raise ValueError(
                    f"Format de slot invalide, le slot {slot} de longueur {len(slot)} doit être composé uniquement de chiffres et être au format BBBRRTT (B Bay, R Row, T Tier)"
                )

        # Application de la fonction pour créer la colonne MacroTier
        self.logger.info("Application de la fonction pour créer la colonne MacroTier")

        df_result["MacroTier"] = df["Slot"].apply(get_macro_tier)

        return df_result


    def merge_with_stacks_and_subbays_capacity(self, df, df_stacks, df_subbays_capacity):

        # Fusion des données des conteneurs avec les données des stacks et des capacités de sous-baies
        self.logger.info("Fusion des données des conteneurs avec les données des stacks et des capacités de sous-baies")

        return (
            df.merge(
                df_stacks,
                how="outer",
                on=["Bay", "Row", "MacroTier"],
            )
            .reset_index(drop=True)
            .merge(
                df_subbays_capacity,
                how="outer",
                on=["SubBay", "MacroTier"],
            )
            .reset_index(drop=True)
        )