import pandas as pd
from modules.restow_layer.preprocessing import Preprocessing
from modules.restow_layer.restow_computation import RestowComputation
import logging



# Objectif du code : calculer le nombre de conteneurs lourds qu'on peut placer en cale si tous les légers sont enlevés
# Contraites : Une stack de conteneur ne peut être touché que si 
    # aucun des conteneurs de la stack n'est dangereux ou OOG en cale
    # aucun des conteneurs de la stack n'est dangereux ou OOG en cale
    # la masse restant si on enlève tous les légers en cale est supérieur à 30 tonnes
    # la masse restant si on enlève tous les légers en cale est supérieur à 30 tonnes
    # aucun des conteneurs de la sous baie n'est dangereux ou OOG sur le pont
    # aucun des conteneurs de la sous baie n'est dangereux ou OOG sur le pont

# Les 45 pieds sont considérés comme 2 TEUS
# Ajouter les containers grouping de Christelle (si pas présent)
# Ajouter les BreakBulk

class RestowLayer:

    def __init__(self, logger: logging.Logger) -> None:
        
        self.logger = logger

        
    def get_df_restow(
            self,
            df_final_containers: pd.DataFrame, 
            df_stacks_input: pd.DataFrame, 
            df_subbays_capacity_input: pd.DataFrame,
        ):

        # Set logger like this because this code is garb*ge
        preprocessing = Preprocessing(self.logger)
        restow_computation = RestowComputation(self.logger)


        # Reading
        df_stacks = preprocessing.read_and_preprocessess_stack_data(df_stacks_input)
        df_subbays_capacity = preprocessing.read_and_preprocessess_subbays_data(df_subbays_capacity_input)

        
        # Preprocessing
        df_onboard_containers = preprocessing.remove_containers_without_slot(df_final_containers)

        df_onboard_containers_with_bay_row_tier = preprocessing.compute_bay_row_and_tier(df_onboard_containers)

        df_containers_merged_with_stacks = preprocessing.merge_with_stacks_and_subbays_capacity(
            df=df_onboard_containers_with_bay_row_tier, 
            df_stacks=df_stacks, 
            df_subbays_capacity=df_subbays_capacity,
        )

        # Restow Computing (Complexe Computation)
        df_restow = restow_computation.compute_restow(df_containers_merged_with_stacks)

        return df_restow
        