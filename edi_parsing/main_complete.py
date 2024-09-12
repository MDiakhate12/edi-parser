import os
import glob

from utils import parsing_utils
from utils import python_utils

from data_model import baplie_segments_groups

simulation = 126
env = "prod"

base_dir = os.path.dirname(__file__)
parent_dir = os.path.dirname(base_dir)

input_dir = os.path.join(parent_dir,  f"/data/simulations/simulation_{simulation}_{env}/in")
output_dir = os.path.join(base_dir,  f"/test_output_data/simulation_{simulation}_{env}")

onboard_path = glob.glob(f"{input_dir}/*/OnBoard.edi")[0]
tank_path = glob.glob(f"{input_dir}/*/Tank.edi")[0]
loadlist_path = glob.glob(f"{input_dir}/*/LoadList.edi")[0]


onboard_locations = parsing_utils.read_edi_segments(
    edi_file_path=onboard_path,
    segments_pattern=r"LOC\+147.*?CNT\+8:\d+(?::\d+)?'",
)


loadlist_locations = parsing_utils.read_edi_segments(
    edi_file_path=loadlist_path,
    segments_pattern=r"LOC\+147.*?CNT\+8:\d+(?::\d+)?'",
)


tank_locations = parsing_utils.read_edi_segments(
    edi_file_path=tank_path,
    segments_pattern=r"LOC\+ZZZ.*?FTX\+AAI.*?'",
)


tank_segments_groups = baplie_segments_groups.TankSegmentGroup.parse_segments_groups(tank_locations)
onboard_segments_groups = baplie_segments_groups.LocationSegmentGroup.parse_segments_groups(onboard_locations)
loadlist_segments_groups = baplie_segments_groups.LocationSegmentGroup.parse_segments_groups(loadlist_locations)


onboard_data = python_utils.as_dict(onboard_segments_groups)
loadlist_data = python_utils.as_dict(loadlist_segments_groups)
tank_data = python_utils.as_dict(tank_segments_groups)


python_utils.write_json(onboard_data, f"{output_dir}/OnBoard.json")
python_utils.write_json(loadlist_data, f"{output_dir}/LoadList.json")
python_utils.write_json(tank_data, f"{output_dir}/Tank.json")