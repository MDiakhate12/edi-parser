import pytest
import os
import pandas as pd
from pandas.testing import assert_frame_equal
from main import main
pd.set_option('display.max_columns', None)


class CommonTests:

    @pytest.fixture(scope="class")
    def containers_csv(self) -> pd.DataFrame:
        try:
            if not os.path.exists(f"{self.INTERMEDIATE_PATH}/containers.csv"):
                main(self.EVENT, enable_logging=True, log_level="DEBUG")
        except:
            raise
        else:
            return pd.read_csv(f"{self.INTERMEDIATE_PATH}/containers.csv", sep=";")
    
    @pytest.fixture(scope="class")
    def rotation_csv(self) -> pd.DataFrame:
        try:
            if not os.path.exists(f"{self.INTERMEDIATE_PATH}/rotation.csv"):
                main(self.EVENT, enable_logging=True, log_level="DEBUG")
        except:
            raise
        else:
            return pd.read_csv(f"{self.INTERMEDIATE_PATH}/rotation.csv", sep=";")

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
    
    def test_rotation_last_asian_port(self, rotation_csv: pd.DataFrame):
        rotation_csv_expected = pd.read_csv(f"{self.EXPECTED_TEST_RESULTS_FOLDER_PATH}/rotation_last_asian_port.csv", sep=";")
        assert_frame_equal(rotation_csv, rotation_csv_expected)