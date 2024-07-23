import pytest
import os
import pandas as pd
from pandas.testing import assert_frame_equal
from main import main, configure_logger
from modules.main_layer import MainLayer
from tests.test_ouputs.common import CommonTests
pd.set_option('display.max_columns', None)
import logging
logging.basicConfig(level="DEBUG", format='%(asctime)s - %(levelname)s - %(message)s')


@pytest.fixture(scope="module", params=["one_slot_flat_rack_1", "one_slot_flat_rack_2", "one_slot_flat_rack_3"])
def sim_id(request: pytest.FixtureRequest):
    """
    Simulations characteristics description:
    - one_slot_flat_rack_1: only one shared slot (2 containers)
    - one_slot_flat_rack_2: 2 shared slots (2 containers)
    - one_slot_flat_rack_3: 1 shared slots with 5 flat_racks
    """
    return request.param

class TestOutputOneSlotFlatRacksSimulation(CommonTests):

    @pytest.fixture
    def equipment_mapping(self, mock_data: None) -> pd.DataFrame:
        try:
            if not os.path.exists(f"{self.INTERMEDIATE_PATH}/equipment_mapping.csv"):
                main(self.EVENT, enable_logging=True, log_level="DEBUG")
        except:
            raise
        else:
            return pd.read_csv(f"{self.INTERMEDIATE_PATH}/equipment_mapping.csv", sep=";")
    
    @pytest.fixture
    def simulation_output(self, containers_csv: pd.DataFrame) -> None:
        """Create fake simulation output"""
        output = containers_csv[["Container", "LoadPort", "DischPort", "Weight", "Setting", "Slot"]].copy()
        output["TEST_CONTAINER_ID"] = "C" + output.index.astype(str)
        output["ISO_CODE"] = ""
        output["WEIGHT_KG"] = output["Weight"] * 1000
        output["CARRIER"] = "CMA"
        output = output.rename(columns=dict(Container="REAL_CONTAINER_ID", LoadPort="POL", DischPort="POD", Setting="SETTING", Slot="SLOT_POSITION"))
        output = output[["REAL_CONTAINER_ID", "TEST_CONTAINER_ID", "POL", "POD", "ISO_CODE", "WEIGHT_KG", "SETTING", "SLOT_POSITION", "CARRIER"]]
        output.to_csv(f"{self.OUTPUT_PATH}/output.csv", sep=";")
    
    def test_flat_rack_grouping(self, containers_csv: pd.DataFrame, equipment_mapping: pd.DataFrame):
        # get groups from containers.csv
        df_flat_racks_groups = containers_csv[containers_csv["Container"].str.startswith("DP")].reset_index(drop=True)
        df_flat_racks_groups.fillna('', inplace=True)
        df_flat_racks_groups["Height"] = df_flat_racks_groups["Height"].astype(str)

        # # expected results initialization: uncomment ONLY when a major change affects the expected output
        # df_flat_racks_groups.to_csv(f"{self.EXPECTED_TEST_RESULTS_FOLDER_PATH}/containers_flat_rack_grouping.csv", index=False)
        # equipment_mapping.to_csv(f"{self.EXPECTED_TEST_RESULTS_FOLDER_PATH}/equipment_mapping.csv", index=False)

        # get expected data
        df_flat_racks_groups_expected = pd.read_csv(f"{self.EXPECTED_TEST_RESULTS_FOLDER_PATH}/containers_flat_rack_grouping.csv")
        df_equipment_mapping_expected = pd.read_csv(f"{self.EXPECTED_TEST_RESULTS_FOLDER_PATH}/equipment_mapping.csv")
        # TODO: Later, handle types with data models
        df_flat_racks_groups_expected["Type"] = df_flat_racks_groups_expected["Type"].astype(str)
        df_flat_racks_groups_expected["Slot"] = df_flat_racks_groups_expected["Slot"].astype(str)
        df_flat_racks_groups_expected["Exclusion"] = df_flat_racks_groups_expected["Exclusion"].astype(str)

        # test data coherence
        assert_frame_equal(df_flat_racks_groups_expected, df_flat_racks_groups)
        equipment_mapping_cols = ['Container_group', 'Container', 'Slot', 'POL_nb', 'LoadPort', 'POD_nb','DischPort', 'Weight']
        assert_frame_equal(df_equipment_mapping_expected[equipment_mapping_cols], equipment_mapping[equipment_mapping_cols])

    def test_flat_rack_ungrouping(self, equipment_mapping: pd.DataFrame, simulation_output: pd.DataFrame):
        containers_in_groups = equipment_mapping["Container"].unique().tolist()

        # run ungrouping logic
        event_postprocessing = self.EVENT_POSTPROCESSING
        logger, list_handler = configure_logger(event_postprocessing["simulation_id"])
        ml = MainLayer(logger, event_postprocessing, event_postprocessing.get("reusePreviousResults", False))
        df_containers_ungrouped = ml._handle_one_slot_flat_racks_in_output()
        df_containers_ungrouped = df_containers_ungrouped[df_containers_ungrouped["REAL_CONTAINER_ID"].isin(containers_in_groups)].reset_index(drop=True)
        df_containers_ungrouped["WEIGHT_KG"] = df_containers_ungrouped["WEIGHT_KG"].astype(float)
        df_containers_ungrouped["SLOT_POSITION"] = df_containers_ungrouped["SLOT_POSITION"].astype(int)

        # # expected results initialization: uncomment ONLY when a major change affects the expected output
        # df_containers_ungrouped.to_csv(f"{self.EXPECTED_TEST_RESULTS_FOLDER_PATH}/containers_flat_rack_ungrouping.csv", index=False)

        # load expected results
        df_containers_ungrouped_expected = pd.read_csv(f"{self.EXPECTED_TEST_RESULTS_FOLDER_PATH}/containers_flat_rack_ungrouping.csv")
        
        # test data coherence
        assert_frame_equal(df_containers_ungrouped_expected, df_containers_ungrouped)
