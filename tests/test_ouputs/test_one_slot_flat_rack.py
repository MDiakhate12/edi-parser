import pytest
import os
import pandas as pd
from pandas.testing import assert_frame_equal
from main import main, configure_logger
from modules.main_layer import MainLayer
from tests.utils import get_referential_files_from_s3
from tests.test_ouputs.common import CommonTests
pd.set_option('display.max_columns', None)
import logging
logging.basicConfig(level="DEBUG", format='%(asctime)s - %(levelname)s - %(message)s')


class TestOutputOneSlotFlatRacksSimulation(CommonTests):

    # initialize mock data
    MOCK_DATA_PATH = f"{os.path.dirname(os.path.dirname(__file__))}/mock/test_output"
    MOCK_SIMULATION_ID = "one_slot_flat_rack"
    SIMULATION_FOLDER_PATH = f"{MOCK_DATA_PATH}/simulations/sim_{MOCK_SIMULATION_ID}_local"
    INTERMEDIATE_PATH = f"{SIMULATION_FOLDER_PATH}/intermediate"
    OUTPUT_PATH = f"{SIMULATION_FOLDER_PATH}/out"
    EXPECTED_TEST_RESULTS_FOLDER_PATH = f"{SIMULATION_FOLDER_PATH}/expected"
    os.makedirs(EXPECTED_TEST_RESULTS_FOLDER_PATH, exist_ok=True)
    EVENT = dict(
        vesselImo = "9454450",
        port = "CNSHK",
        description = "testscript",
        path = MOCK_DATA_PATH,
        simulation_id = f"sim_{MOCK_SIMULATION_ID}_local",
        reusePreviousResults = False,
        dg_exception_rules = "master"
    )
    EVENT_POSTPROCESSING = dict(
        vesselImo = "9454450",
        port = "CNSHK",
        description = "testscript",
        path = MOCK_DATA_PATH,
        simulation_id = f"sim_{MOCK_SIMULATION_ID}_local",
        reusePreviousResults = True,
        dg_exception_rules = "master"
    )
    logger, list_handler= configure_logger(EVENT_POSTPROCESSING["simulation_id"])

    # initialize referential data
    if not os.path.exists(f"{MOCK_DATA_PATH}/referential"):
        get_referential_files_from_s3(local_simulation_folder=MOCK_DATA_PATH, environment="prd")
    
    @pytest.fixture(scope="class")
    def equipment_mapping(self) -> pd.DataFrame:
        try:
            if not os.path.exists(f"{self.INTERMEDIATE_PATH}/equipment_mapping.csv"):
                main(self.EVENT, enable_logging=True, log_level="DEBUG")
        except:
            raise
        else:
            return pd.read_csv(f"{self.INTERMEDIATE_PATH}/equipment_mapping.csv", sep=";")
    
    @pytest.fixture(scope="class")
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

        # get expected data
        df_flat_racks_groups_expected = pd.read_csv(f"{self.EXPECTED_TEST_RESULTS_FOLDER_PATH}/containers_flat_rack_grouping.csv")
        df_equipment_mapping_expected = pd.read_csv(f"{self.EXPECTED_TEST_RESULTS_FOLDER_PATH}/equipment_mapping.csv")
        # TODO: Later, handle types with data models
        df_flat_racks_groups_expected["Type"] = df_flat_racks_groups_expected["Type"].astype(str)
        df_flat_racks_groups_expected["Slot"] = df_flat_racks_groups_expected["Slot"].astype(str)
        df_flat_racks_groups_expected["Exclusion"] = df_flat_racks_groups_expected["Exclusion"].astype(str)

        # test data coherence
        assert_frame_equal(df_flat_racks_groups_expected, df_flat_racks_groups)
        assert_frame_equal(df_equipment_mapping_expected, equipment_mapping[['Container_group', 'Container', 'Slot', 'POL_nb', 'LoadPort', 'POD_nb','DischPort', 'Weight']])

    def test_flat_rack_ungrouping(self, equipment_mapping: pd.DataFrame, simulation_output: pd.DataFrame):
        containers_in_groups = equipment_mapping["Container"].unique().tolist()

        # run ungrouping logic
        ml = MainLayer(self.logger, self.EVENT_POSTPROCESSING, self.EVENT_POSTPROCESSING.get("reusePreviousResults", False))
        df_containers_ungrouped = ml._handle_one_slot_flat_racks_in_output()
        df_containers_ungrouped = df_containers_ungrouped[df_containers_ungrouped["REAL_CONTAINER_ID"].isin(containers_in_groups)].reset_index(drop=True)
        df_containers_ungrouped["WEIGHT_KG"] = df_containers_ungrouped["WEIGHT_KG"].astype(float)
        df_containers_ungrouped["SLOT_POSITION"] = df_containers_ungrouped["SLOT_POSITION"].astype(int)

        # load expected results
        df_containers_ungrouped_expected = pd.read_csv(f"{self.EXPECTED_TEST_RESULTS_FOLDER_PATH}/containers_flat_rack_ungrouping.csv")
        
        # test data coherence
        assert_frame_equal(df_containers_ungrouped_expected, df_containers_ungrouped)
