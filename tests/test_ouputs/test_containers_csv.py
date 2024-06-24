import pytest
import os
from functools import reduce
import pandas as pd
from pandas.testing import assert_frame_equal
from main import main
from tests.utils import get_referential_files_from_s3


class TestOutputContainersCSV:

    # initialize mock data
    MOCK_DATA_PATH = f"{os.path.dirname(os.path.dirname(__file__))}/mock/test_output"
    MOCK_SIMULATION_ID = "1001"
    SIMULATION_FOLDER_PATH = f"{MOCK_DATA_PATH}/simulations/sim_{MOCK_SIMULATION_ID}_int"
    OUTPUT_PATH = f"{SIMULATION_FOLDER_PATH}/intermediate/containers.csv"
    EXPECTED_TEST_RESULTS_FOLDER_PATH = f"{SIMULATION_FOLDER_PATH}/expected"
    EVENT = dict(
        vesselImo = "9454450",
        port = "CNSHK",
        description = "testscript",
        path = MOCK_DATA_PATH,
        simulation_id = f"sim_{MOCK_SIMULATION_ID}_int",
        reusePreviousResults = False,
        dg_exception_rules = "master"
    )

    # initialize referential data
    if not os.path.exists(f"{MOCK_DATA_PATH}/referential"):
        get_referential_files_from_s3(local_simulation_folder=MOCK_DATA_PATH, environment="prd")

    @pytest.fixture(scope="class")
    def containers_csv(self) -> pd.DataFrame:
        # get the containers.csv file if it does not exist
        if not os.path.exists(self.OUTPUT_PATH):
            main(self.EVENT, enable_logging=True, log_level="DEBUG")
        return pd.read_csv(self.OUTPUT_PATH, sep=";")

    def test_containers_csv_final_columns(self, containers_csv: pd.DataFrame):
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

    def test_containers_csv_oog_dimensions(self, containers_csv: pd.DataFrame):
        # expected containers with OOG
        expected_containers_with_oog = pd.read_csv(f"{self.EXPECTED_TEST_RESULTS_FOLDER_PATH}/containers_with_oog.csv")

        # actual containers with OOG
        oog_cols = ['OOG_AFTWARDS', 'OOG_FORWARD', 'OOG_LEFT', 'OOG_LEFT_MEASURE', 'OOG_RIGHT', 'OOG_RIGHT_MEASURE', 'OOG_TOP', 'OOG_TOP_MEASURE']
        id_col = ["Container"]
        actual_containers_with_oog = containers_csv[reduce(lambda a, b: a | b, [containers_csv[col] != 0 for col in oog_cols])]
        actual_containers_with_oog = actual_containers_with_oog[id_col + oog_cols].reset_index(drop=True)

        # perform tests
        pd.set_option('display.max_columns', None)
        assert_frame_equal(actual_containers_with_oog, expected_containers_with_oog)