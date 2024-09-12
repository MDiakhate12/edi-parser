import os
import glob
import traceback

import argparse

from utils import parsing_utils
from utils import python_utils

from data_model.baplie_segments import Temperature, TemperatureSetting
from data_model import baplie_segments_groups

parser = argparse.ArgumentParser()
parser.add_argument("--simulation", "-s", type=str, default='164', help="Simulation number")
parser.add_argument("--env", "-e", type=str, default="prod", help="Environment (prod or dev)")
parser.add_argument("--type", "-t", type=str, default="onboard", help="File type (loadlist or onboard or tank. First char or number is also possible. You can add [type]_test in order to run on a test data)", choices=["loadlist", "onboard", "tank", 0, 1, 2, "l", "o", "t"])

args = parser.parse_args()

simulation = args.simulation
env = args.env

def check_if_test(args_type, input_type):
    if "test" in str(args_type).lower():
        return f"{input_type}_test"
    return input_type

if str(args.type).lower() in ("onboard", "o", 1):
    input_type = "OnBoard"
elif str(args.type).lower() in ("loadlist", "l", 0):
    input_type = "LoadList"
elif str(args.type).lower() in ("tank", "t", 2):
    input_type = "Tank"
else:
    raise ValueError("Invalid processing type, only ('loadlist', 'l', 0) or ('onboard', 1, o) or ('tank', 'l', 2) are allowed !")


# simulation = 126

# env = "prod"

base_dir = os.path.dirname(__file__)
parent_dir = os.path.dirname(base_dir)

print(base_dir)
print(parent_dir)

input_dir = os.path.join(
    parent_dir,
    "data",
    "simulations",
    f"simulation_{simulation}_{env}",
    "in",
)

output_dir = os.path.join(
    parent_dir,
    "output_data",
    f"simulation_{simulation}_{env}",
)

try:
    onboard_path = glob.glob(os.path.join(input_dir, f"*/{input_type}.edi"))[0]
except Exception as e:
    print("File onboard_path does not exist ! Not .edi file found", e)
    traceback.print_exc()

onboard_locations = parsing_utils.read_edi_segments(
    edi_file_path=onboard_path,
    segments_pattern=r"LOC\+147.*?CNT\+8:\d+(?::\d+)?'",
)

edi_string = """LOC+147+0140014:9711:5'
EQD+CN+CMAU7889682:6346:5+45G1:6346:5+++5'
NAD+CF+CMA:LINES:306'
MEA+AAE+VGM+KGM:29510'
HAN+ZZZ:HANDLING:306:FISHMEAL'
RFF+BN:LMM0462469'
FTX+AAA+++FISHMEAL'
TMP+2+-2.5:CEL'
RNG+4+CEL:0.0:0.0'
DIM+7+CMT::8'
DIM+8+CMT::8'
DIM+13+CMT:::40'
LOC+9+PECLL'
LOC+11+CNSHA'
LOC+76+PECLL'
LOC+83+**KHH'
DGS+IMD+9::40-20+2216++3+'
ATT+26+PSN:DGATT:306+:::FISH MEAL (FISH SCRAP), STABILIZED  ANTI-OXIDANT TREATED. MOISTURE CONTENT GREATER THAN 5% BUT NOT EXCEEDING 12%, BY MASS. FAT CONTENT NOT MORE THAN 15%'
ATT+26+TNM:DGATT:306+:::FISH MEAL, STABILIZED  ANTI-OXIDANT TREATED. MOISTURE CONTENT GREATER'
MEA+AAE+AAA+KGM:25582.70'
DGS+IMD+9::40-20+3359'
ATT+26+PSN:DGATT:306+:::FUMIGATED CARGO TRANSPORT UNIT'
MEA+AAE+AAA+KGM:1'
EQD+BB+DEHAM00002'
NAD+CF+HLC:LINES:306'
MEA+AAE+AET+KGM:30000'
MEA+AAE+VCG+CMT:75'
DIM+1+CMT:520:705:165'
RFF+AWN:0100082'
RFF+AWN:0100182'
RFF+AWN:0100282'
LOC+9+DEHAM'
LOC+12+CNSHA'
CNT+8:1'"""

try:

    onboard_locations = parsing_utils.read_edi_segments(
        edi_file_path=onboard_path,
        # edi_string=edi_string,
        segments_pattern=r"LOC\+147.*?CNT\+8:\d+(?::\d+)?'",
    )

    onboard_segments_groups = baplie_segments_groups.LocationSegmentGroup.parse_segments_groups(onboard_locations)

    onboard_data = python_utils.as_dict(onboard_segments_groups)

    output_json_name = check_if_test(args.type, input_type)
    output_file = f"{output_dir}/{output_json_name}.json"
    python_utils.write_json(onboard_data, output_file)

    # print(output_file)
    # print(onboard_locations)
    # print(onboard_data)

except Exception as e:
    print(f"Error on main.py parsing call error_message={e}")
    traceback.print_exc()