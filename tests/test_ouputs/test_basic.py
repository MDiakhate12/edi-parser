import pytest
import os
from functools import reduce
import pandas as pd
from pandas.testing import assert_frame_equal
from main import main
from tests.utils import get_referential_files_from_s3
from tests.test_ouputs.common import CommonTests
pd.set_option('display.max_columns', None)


class TestOutputBasicSimulation(CommonTests):

    # initialize mock data
    MOCK_DATA_PATH = f"{os.path.dirname(os.path.dirname(__file__))}/mock/test_output"
    MOCK_SIMULATION_ID = "basic"
    SIMULATION_FOLDER_PATH = f"{MOCK_DATA_PATH}/simulations/sim_{MOCK_SIMULATION_ID}_local"
    INTERMEDIATE_PATH = f"{SIMULATION_FOLDER_PATH}/intermediate"
    OUTPUT_PATH = f"{SIMULATION_FOLDER_PATH}/out"
    EXPECTED_TEST_RESULTS_FOLDER_PATH = f"{SIMULATION_FOLDER_PATH}/expected"
    EVENT = dict(
        vesselImo = "9454450",
        port = "CNSHK",
        description = "testscript",
        path = MOCK_DATA_PATH,
        simulation_id = f"sim_{MOCK_SIMULATION_ID}_local",
        reusePreviousResults = False,
        dg_exception_rules = "master"
    )

    # initialize referential data
    if not os.path.exists(f"{MOCK_DATA_PATH}/referential"):
        get_referential_files_from_s3(local_simulation_folder=MOCK_DATA_PATH, environment="prd")

    def test_containers_csv_oog_dimensions(self, containers_csv: pd.DataFrame):
        # expected containers with OOG
        expected_containers_with_oog = pd.read_csv(f"{self.EXPECTED_TEST_RESULTS_FOLDER_PATH}/containers_with_oog.csv")

        # actual containers with OOG
        oog_cols = ['OOG_AFTWARDS', 'OOG_FORWARD', 'OOG_LEFT', 'OOG_LEFT_MEASURE', 'OOG_RIGHT', 'OOG_RIGHT_MEASURE', 'OOG_TOP', 'OOG_TOP_MEASURE']
        id_col = ["Container"]
        actual_containers_with_oog = containers_csv[reduce(lambda a, b: a | b, [containers_csv[col] != 0 for col in oog_cols])]
        actual_containers_with_oog = actual_containers_with_oog[id_col + oog_cols].reset_index(drop=True)

        # perform tests
        assert_frame_equal(actual_containers_with_oog, expected_containers_with_oog)

    def test_containers_stowage_for_dg_loaded_in_china(self, containers_csv: pd.DataFrame):
        df_dg_loaded_in_china = containers_csv[(containers_csv["cDG"] != "") & (containers_csv["LoadPort"].str.startswith("CN"))]
        if df_dg_loaded_in_china.shape[0] > 0:
            try:
                assert all([val == "HOLD" for val in df_dg_loaded_in_china[["Stowage"]].values.flatten()])
            except AssertionError:
                print(f"Some dangerous containers loaded in China are not stowed in HOLD.")
                print(df_dg_loaded_in_china[["Container", "LoadPort", "cDG", "Stowage"]])
                raise
        else:
            raise ValueError("Unable to check if dangerous containers loaded in China are stowed on HOLD.")
