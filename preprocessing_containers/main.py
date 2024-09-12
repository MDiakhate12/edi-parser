import os
import argparse
from runner import run
import pandas as pd

parser = argparse.ArgumentParser()

parser.add_argument("--simulation", "-s", type=str, default='164', help="Simulation number")
parser.add_argument("--env", "-e", type=str, default="prod", help="Environment (prod or dev)")

args = parser.parse_args()

simulation = args.simulation
env = args.env

class InputType:
    OnBoard = "OnBoard"
    LoadList = "LoadList"
    Tank = "Tank"


print("Define base directory and parent directory")
base_dir = os.path.dirname(__file__)
parent_dir = os.path.dirname(base_dir)


df_onboard = run(
    simulation = simulation,
    env = env,
    parent_dir = parent_dir,
    input_type = InputType.OnBoard,
)

df_loadlist = run(
    simulation = simulation,
    env = env,
    parent_dir = parent_dir,
    input_type = InputType.LoadList,
)

df_containers = pd.concat([df_onboard, df_loadlist])

output_containers_path = os.path.join(
    parent_dir,
    "output_data",
    f"simulation_{simulation}_{env}",
    f"containers.csv",
)

df_containers.to_csv(output_containers_path, index=False, sep=";")
