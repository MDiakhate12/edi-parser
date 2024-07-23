import pytest
import os
import pandas as pd
from pandas.testing import assert_frame_equal
from tests.utils import get_referential_files_from_s3
from enum import Enum
from main import main
pd.set_option('display.max_columns', None)


class CommonTests:

    MOCK_DATA_PATH = None
    SIMULATION_FOLDER_PATH = None
    INTERMEDIATE_PATH = None
    OUTPUT_PATH = None
    EXPECTED_TEST_RESULTS_FOLDER_PATH = None
    EVENT = None

    @pytest.fixture
    def mock_data(self, sim_id):
        self.MOCK_DATA_PATH = f"{os.path.dirname(os.path.dirname(__file__))}/mock/test_output"
        SIMULATION_FOLDER_PATH = f"{self.MOCK_DATA_PATH}/simulations/sim_{sim_id}_local"
        self.INTERMEDIATE_PATH = f"{SIMULATION_FOLDER_PATH}/intermediate"
        self.OUTPUT_PATH = f"{SIMULATION_FOLDER_PATH}/out"
        self.EXPECTED_TEST_RESULTS_FOLDER_PATH = f"{SIMULATION_FOLDER_PATH}/expected"
        os.makedirs(self.EXPECTED_TEST_RESULTS_FOLDER_PATH, exist_ok=True)
        self.EVENT = dict(
            vesselImo = "9454450",
            port = "CNSHK",
            description = "testscript",
            path = self.MOCK_DATA_PATH,
            simulation_id = f"sim_{sim_id}_local",
            reusePreviousResults = False,
            dg_exception_rules = "master"
        )
        self.EVENT_POSTPROCESSING = dict(
            vesselImo = "9454450",
            port = "CNSHK",
            description = "testscript",
            path = self.MOCK_DATA_PATH,
            simulation_id = f"sim_{sim_id}_local",
            reusePreviousResults = True,
            dg_exception_rules = "master"
        )

        # initialize referential data
        if not os.path.exists(f"{self.MOCK_DATA_PATH}/referential"):
            get_referential_files_from_s3(local_simulation_folder=self.MOCK_DATA_PATH, environment="prd")


    @pytest.fixture
    def containers_csv(self, mock_data: None) -> pd.DataFrame:
        try:
            if not os.path.exists(f"{self.INTERMEDIATE_PATH}/containers.csv"):
                main(self.EVENT, enable_logging=True, log_level="DEBUG")
        except:
            raise
        else:
            return pd.read_csv(f"{self.INTERMEDIATE_PATH}/containers.csv", sep=";")
    
    @pytest.fixture
    def rotation_csv(self, mock_data: None) -> pd.DataFrame:
        try:
            if not os.path.exists(f"{self.INTERMEDIATE_PATH}/rotation.csv"):
                main(self.EVENT, enable_logging=True, log_level="DEBUG")
        except:
            raise
        else:
            return pd.read_csv(f"{self.INTERMEDIATE_PATH}/rotation.csv", sep=";")

    def test_containers_csv_final_columns(self, containers_csv: pd.DataFrame, mock_data: None):
        expected_columns = ['Container', 'DGheated', 'DischPort', 'Empty', 'Exclusion', 'Height', 'LoadPort', 'NonReeferAtReefer', 'OOG_AFTWARDS', 'OOG_FORWARD', 'OOG_LEFT', 'OOG_LEFT_MEASURE', 'OOG_RIGHT', 'OOG_RIGHT_MEASURE', 'OOG_TOP', 'OOG_TOP_MEASURE', 'POD_nb', 'POL_nb', 'Revenue', 'Setting', 'Size', 'Slot', 'Stowage', 'Subport', 'Type', 'Weight', 'cDG', 'cType', 'cWeight', 'overstowPort', 'priorityID', 'priorityLevel']
        actual_columns = list(containers_csv.columns)
        expected_columns.sort()
        actual_columns.sort()
        try:
            print(self.MOCK_DATA_PATH)
            assert expected_columns == actual_columns
        except AssertionError:
            print(expected_columns)
            print(actual_columns)
            raise
    
    def test_rotation_last_asian_port(self, rotation_csv: pd.DataFrame, mock_data: dict):
        # # expected results initialization: uncomment ONLY when a major change affects the expected output
        # rotation_csv.to_csv(f"{self.EXPECTED_TEST_RESULTS_FOLDER_PATH}/rotation_last_asian_port.csv", sep=";", index=False)

        rotation_csv_expected = pd.read_csv(f"{self.EXPECTED_TEST_RESULTS_FOLDER_PATH}/rotation_last_asian_port.csv", sep=";")
        assert_frame_equal(rotation_csv, rotation_csv_expected)
