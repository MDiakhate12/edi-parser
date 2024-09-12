def convert_length_column(value_column, unit_code_column):
  """Convert length values to meters."""

  if type(unit_code_column) == str:
    return value_column.fillna(0).astype(float) * length_unit_codes[unit_code_column]

  return value_column.fillna(0).astype(float) * unit_code_column.map(length_unit_codes)


def convert_weight_column(value_column, unit_code_column):
  """Convert weight values to kilograms."""

  if type(unit_code_column) == str:
    return value_column.fillna(0).astype(float) * weight_unit_codes[unit_code_column]

  return value_column.fillna(0).astype(float) * unit_code_column.map(weight_unit_codes)


def convert_measure(value_column, unit_code_column, unit_code_map):
  """Convert measure values to meters or kilograms."""

  if type(unit_code_column) == str:
    return value_column.fillna(0).astype(float) * unit_code_map[unit_code_column]

  return value_column.fillna(0).astype(float) * unit_code_column.map(unit_code_map)

def convert_length(value, unit_code):
  """Convert length values to meters."""

  return float(value) * length_unit_codes[unit_code]


def convert_weight(value, unit_code):
  """Convert weight values to kilograms."""

  return float(value) * weight_unit_codes[unit_code]

def convert_dim_columns(row, dim_columns, nan_replacement = np.nan):
  """Convert all dimension columns to meters. (columns obtained from DIM segment with ther corresponding DIM_.._unit_code column)"""

  for col in dim_columns:

    if is_not_null(row[col]):
      row[col] = convert_length(
        row[col],
        row[f"{col}_unit_code"]
      )
    else:
      print(type(row[col]))
      print(row[col])
      row[col] = nan_replacement      

  return row


def create_boolean_columns_from_dim_columns(row, dim_columns):
  """Create boolean columns from dimension columns. Eg: Creates OOG_TOP from OOG_TOP_MEASURE"""

  for col in dim_columns:
    row[col.replace("_MEASURE", "")] = "1" if is_not_null(row[col]) else "0"

  return row


def convert_dim_columns(row, dim_columns, nan_replacement = np.nan):
  """Convert all dimension columns to meters. (columns obtained from DIM segment with ther corresponding DIM_.._unit_code column)"""

  for col in dim_columns:

    if is_not_null(row[col]):
      print(col, row[col], row[f"{col}_unit_code"], length_unit_codes[row[f"{col}_unit_code"]])
      row[col] = convert_measure(
        value = row[col],
        unit_code = row[f"{col}_unit_code"],
        unit_code_map = length_unit_codes,
      )
    else:
      row[col] = nan_replacement      

  return row


def create_boolean_columns_from_dim_columns(row, dim_columns):
  """Create boolean columns from dimension columns. Eg: Creates OOG_TOP from OOG_TOP_MEASURE"""

  for col in dim_columns:
    row[col.replace("_MEASURE", "")] = "1" if is_not_null(row[col]) else "0"

  return row


# prompt: create a dataframe with sample data containing one container for which LoadPort and DischPort are present in rotation, one for which only LoadPort is present in rotation, one for which only DischPort is present in rotation, one for which both of them are absent, and the same combinations with case of null Slot. example of port names ['CNNGB', 'CNNSA', 'CNSHA', 'CNSHK', 'CNTAO', 'CNXMN', 'FRFOS']

import pandas as pd
data = {'Container': ['C1', 'C2', 'C3', 'C4', 'C5', 'C6', 'C7', 'C8'],
        'Slot': ['001A01', '001A02', '001A03', '001A04', '001A05', None, None, None],
        'LoadPort': ['CNNGB', 'CNNSA', 'UNKNOWN1', 'UNKNOWN2', 'CNNGB', 'CNNSA', 'UNKNOWN1', 'UNKNOWN2'],
        'DischPort': ['CNXMN', 'UNKNOWN3', 'CNSHA', 'UNKNOWN4', 'CNXMN', 'UNKNOWN3', 'CNSHA', 'UNKNOWN4']}
df = pd.DataFrame(data)

df_rotation_data = {'ShortName': ['CNNGB', 'CNNSA', 'CNSHA', 'CNSHK', 'CNTAO', 'CNXMN', 'FRFOS'],
                   'Sequence': [1, 2, 3, 4, 5, 6, 7]}
df_rotation = pd.DataFrame(df_rotation_data)

df_res = __add_pol_pod_nb(df, df_rotation)

df_res[["Slot", "DischPort", "POD_nb"]].drop_duplicates()


iso_codes = pd.read_csv("/content/size_and_type_iso_codes.csv", sep=",", header=0, dtype=str)

list(iso_codes.columns)

iso_codes

# iso_codes[[
#     "ISO_CODE",
#     "RESOURCE_SIZE",
#     "LENGTH",
#     "WIDTH",
#     "HEIGHT",
# ]]

sizes_ft = (
    iso_codes
    .copy()
    .assign(
        ISO_CODE=iso_codes["ISO_CODE"].str[0],
        RESOURCE_SIZE=iso_codes["RESOURCE_SIZE"].astype(int),
    )
    .set_index("ISO_CODE")["RESOURCE_SIZE"]
    .to_dict()
)


height_m = (
    iso_codes
    .copy()
    .assign(
        ISO_CODE=iso_codes["ISO_CODE"].str[0],
        HEIGHT=iso_codes["HEIGHT"].astype(float),
    )
    .set_index("ISO_CODE")["HEIGHT"]
    .to_dict()
)

def get_size_ft(row, size_referential):
  size_code = str(row["Type"])[0] if is_not_null(row["Type"]) else ""
  if size_code not in size_referential:
    raise ValueError(f"Invalid size code: {size_code} not in size codes referential {size_referential}")
  
  if size_code == "":
    return 0

  return size_referential[size_code]


def get_height_m(row, height_referential):
  size_code = str(row["Type"])[1] if is_not_null(row["Type"]) else ""

  if size_code not in height_referential:
    raise ValueError(f"Invalid height code: {size_code} not found in height codes referential {height_referential}")
  
  if size_code == "":
    return 0

  return height_referential[size_code]


def map_size_and_type_values(
    df,
    size_and_type_column,
    size_and_type_position,
    size_and_type_codes_map,
    output_columns_prefix,
  ):
  """Maps size and type values to a DataFrame."""

  # Extract the relevant size/type code
  size_and_type_value = df[size_and_type_column].str[size_and_type_position]

  # Map code to corresponding value
  df["measure_value"] = size_and_type_value.map(size_and_type_codes_map)

  # Flatten dataframe and add prefix
  df_flatten_measures = pd.json_normalize(df["measure_value"]).add_prefix(output_columns_prefix)
  display(df_flatten_measures)

  # Concatenate the flattened values to the original DataFrame
  return pd.concat([df, df_flatten_measures], axis=1).drop("measure_value", axis=1)

df_enriched_with_size = map_size_and_type_values(
  df = df_enriched,
  size_and_type_column = "Type",
  size_and_type_position = 0,
  size_and_type_codes_map = length_codes_map,
  output_columns_prefix = "Size_",
)

df_enriched_with_height = map_size_and_type_values(
  df = df_enriched_with_size,
  size_and_type_column = "Type",
  size_and_type_position = 1,
  size_and_type_codes_map = height_codes_map,
  output_columns_prefix = "Height_",
)

df_enriched = df_enriched_with_height.rename(columns={"Size_ft": "Size", "Height_width": "Width_m"})


from dataclasses import dataclass, field
from typing import Optional, Any


@dataclass
class Container:
    Container: str = field(
        metadata={
            "description": "Numéro unique d'identification du conteneur.",
            "example": "APHU7157770",
            "parent_field": "EQD (Equipment Details)",
            "source_file": "LoadList.edi, OnBoard.edi, Tank.edi, rotation.csv",
        }
    )
    LoadPort: str = field(
        metadata={
            "description": "Port où le conteneur est chargé.",
            "example": "SAJED",
            "parent_field": "LOC (Location)",
            "source_field": "LOC+9 (Port of Loading)",
            "source_file": "LoadList.edi, OnBoard.edi, Tank.edi, rotation.csv",
        }
    )
    POL_nb: int = field(
        metadata={
            "description": "Numéro du port de chargement.",
            "example": "12",
            "parent_field": "LOC (Location)",
            "source_field": "LOC+9 (Port of Loading Number)",
            "source_file": "LoadList.edi, OnBoard.edi, Tank.edi, rotation.csv",
        }
    )
    DischPort: str = field(
        metadata={
            "description": "Port où le conteneur sera déchargé.",
            "example": "CNSHA",
            "parent_field": "LOC (Location)",
            "source_field": "LOC+11 (Port of Discharge)",
            "source_file": "LoadList.edi, OnBoard.edi, Tank.edi, rotation.csv",
        }
    )
    POD_nb: int = field(
        metadata={
            "description": "Numéro du port de déchargement.",
            "example": "2",
            "parent_field": "LOC (Location)",
            "source_field": "LOC+11 (Port of Discharge Number)",
            "source_file": "LoadList.edi, OnBoard.edi, Tank.edi, rotation.csv",
        }
    )
    Size: int = field(
        metadata={
            "description": "Taille du conteneur (en pieds).",
            "example": "40",
            "parent_field": "EQD (Equipment Details)",
            "source_field": "EQD (Container Size)",
            "source_file": "LoadList.edi, OnBoard.edi, Tank.edi, rotation.csv",
        }
    )
    cType: str = field(
        metadata={
            "description": "Type de conteneur (par exemple, général, réfrigéré, citerne).",
            "example": "GP",
            "parent_field": "EQD (Equipment Details)",
            "source_field": "EQD (Container Type)",
            "source_file": "LoadList.edi, OnBoard.edi, Tank.edi, rotation.csv",
        }
    )
    cWeight: str = field(
        metadata={
            "description": "Indicateur de poids du conteneur.",
            "example": "L",
            "parent_field": "MEA (Measurements)",
            "source_field": "MEA (Container Weight)",
            "source_file": "LoadList.edi, OnBoard.edi, Tank.edi, rotation.csv",
        }
    )
    Height: str = field(
        metadata={
            "description": "Hauteur du conteneur (par exemple, haute, standard).",
            "example": "HC",
            "parent_field": "EQD (Equipment Details)",
            "source_field": "EQD (Container Height)",
            "source_file": "LoadList.edi, OnBoard.edi, Tank.edi, rotation.csv",
        }
    )
    cDG: bool = field(
        metadata={
            "description": "Indicateur de marchandises dangereuses (DG - Dangerous Goods).",
            "example": "0",
            "parent_field": "DGS (Dangerous Goods)",
            "source_field": "DGS (Dangerous Goods)",
            "source_file": "LoadList.edi, OnBoard.edi, Tank.edi, rotation.csv",
        }
    )
    Empty: str = field(
        metadata={
            "description": "Indicateur si le conteneur est vide (E) ou plein (F).",
            "example": "E",
            "parent_field": "EQD (Equipment Details)",
            "source_field": "EQD (Empty)",
            "source_file": "LoadList.edi, OnBoard.edi, Tank.edi, rotation.csv",
        }
    )
    Revenue: int = field(
        metadata={
            "description": "Revenu généré par le conteneur.",
            "example": "10000",
            "parent_field": "FTX (Free Text)",
            "source_field": "FTX (Revenue)",
            "source_file": "LoadList.edi, OnBoard.edi, Tank.edi, rotation.csv",
        }
    )
    Type: str = field(
        metadata={
            "description": "Type de cargaison.",
            "example": "45G1",
            "parent_field": "EQD (Equipment Details)",
            "source_field": "EQD (Cargo Type)",
            "source_file": "LoadList.edi, OnBoard.edi, Tank.edi, rotation.csv",
        }
    )
    Setting: str = field(
        metadata={
            "description": "Réglage du conteneur réfrigéré.",
            "example": "E",
            "parent_field": "TMP (Temperature)",
            "source_field": "TMP (Reefer Setting)",
            "source_file": "LoadList.edi, OnBoard.edi, Tank.edi, rotation.csv",
        }
    )
    Weight: float = field(
        metadata={
            "description": "Poids du conteneur en tonnes.",
            "example": "4.38",
            "parent_field": "MEA (Measurements)",
            "source_field": "MEA (Weight)",
            "source_file": "LoadList.edi, OnBoard.edi, Tank.edi, rotation.csv",
        }
    )
    Slot: str = field(
        metadata={
            "description": "Slot où le conteneur est placé sur le navire.",
            "example": "860586",
            "parent_field": "LOC (Location)",
            "source_field": "LOC+147 (Slot)",
            "source_file": "LoadList.edi, OnBoard.edi, Tank.edi, rotation.csv",
        }
    )
    priorityID: int = field(
        metadata={
            "description": "ID de priorité pour le conteneur.",
            "example": "1",
            "parent_field": "PRI (Priority)",
            "source_field": "PRI (Priority ID)",
            "source_file": "LoadList.edi, OnBoard.edi, Tank.edi, rotation.csv",
        }
    )
    priorityLevel: int = field(
        metadata={
            "description": "Niveau de priorité pour le conteneur.",
            "example": "1",
            "parent_field": "PRI (Priority)",
            "source_field": "PRI (Priority Level)",
            "source_file": "LoadList.edi, OnBoard.edi, Tank.edi, rotation.csv",
        }
    )
    overstowPort: int = field(
        metadata={
            "description": "Port où le conteneur doit être repositionné.",
            "example": "1",
            "parent_field": "LOC (Location)",
            "source_field": "LOC+11 (Overstow Port)",
            "source_file": "LoadList.edi, OnBoard.edi, Tank.edi, rotation.csv",
        }
    )
    NonReeferAtReefer: bool = field(
        metadata={
            "description": "Indicateur si un conteneur non-réfrigéré est placé dans un slot réfrigéré.",
            "example": "0",
            "parent_field": "EQD (Equipment Details)",
            "source_field": "EQD (Non-Reefer at Reefer)",
            "source_file": "LoadList.edi, OnBoard.edi, Tank.edi, rotation.csv",
        }
    )
    Subport: Optional[str] = field(
        metadata={
            "description": "Sous-port (détails supplémentaires sur le port).",
            "example": "",
            "parent_field": "LOC (Location)",
            "source_field": "LOC+11 (Subport)",
            "source_file": "LoadList.edi, OnBoard.edi, Tank.edi, rotation.csv",
        }
    )
    Stowage: Optional[str] = field(
        metadata={
            "description": "Position de stockage du conteneur.",
            "example": "",
            "parent_field": "LOC (Location)",
            "source_field": "LOC+147 (Stowage)",
            "source_file": "LoadList.edi, OnBoard.edi, Tank.edi, rotation.csv",
        }
    )
    DGheated: bool = field(
        metadata={
            "description": "Indicateur si les marchandises dangereuses (DG) doivent être chauffées.",
            "example": "0",
            "parent_field": "DGS (Dangerous Goods)",
            "source_field": "DGS (Dangerous Goods Heated)",
            "source_file": "LoadList.edi, OnBoard.edi, Tank.edi, rotation.csv",
        }
    )
    Exclusion: Optional[str] = field(
        metadata={
            "description": "Exclusions spécifiques pour le conteneur.",
            "example": "",
            "parent_field": "FTX (Free Text)",
            "source_field": "FTX (Exclusion)",
            "source_file": "LoadList.edi, OnBoard.edi, Tank.edi, rotation.csv",
        }
    )
    OOG_FORWARD: bool = field(
        metadata={
            "description": "Indicateur si le conteneur est hors gabarit (OOG - Out of Gauge) à l'avant.",
            "example": "0",
            "parent_field": "DIM (Dimensions)",
            "source_field": "DIM+5",
            "source_file": "LoadList.edi, OnBoard.edi, Tank.edi, rotation.csv",
        }
    )
    OOG_AFTWARDS: bool = field(
        metadata={
            "description": "Indicateur si le conteneur est hors gabarit (OOG) à l'arrière.",
            "example": "0",
            "parent_field": "DIM (Dimensions)",
            "source_field": "DIM+6",
            "source_file": "LoadList.edi, OnBoard.edi, Tank.edi, rotation.csv",
        }
    )
    OOG_RIGHT: bool = field(
        metadata={
            "description": "Indicateur si le conteneur est hors gabarit (OOG) à droite.",
            "example": "0",
            "parent_field": "DIM (Dimensions)",
            "source_field": "DIM+7",
            "source_file": "LoadList.edi, OnBoard.edi, Tank.edi, rotation.csv",
        }
    )
    OOG_RIGHT_MEASURE: float = field(
        metadata={
            "description": "Mesure de hors gabarit (OOG) à droite en mètres.",
            "example": "0.0",
            "parent_field": "DIM (Dimensions)",
            "source_field": "DIM+7",
            "source_file": "LoadList.edi, OnBoard.edi, Tank.edi, rotation.csv",
        }
    )
    OOG_LEFT: bool = field(
        metadata={
            "description": "Indicateur si le conteneur est hors gabarit (OOG) à gauche.",
            "example": "0",
            "parent_field": "DIM (Dimensions)",
            "source_field": "DIM+8",
            "source_file": "LoadList.edi, OnBoard.edi, Tank.edi, rotation.csv",
        }
    )
    OOG_LEFT_MEASURE: float = field(
        metadata={
            "description": "Mesure de hors gabarit (OOG) à gauche en mètres.",
            "example": "0.0",
            "parent_field": "DIM (Dimensions)",
            "source_field": "DIM+8",
            "source_file": "LoadList.edi, OnBoard.edi, Tank.edi, rotation.csv",
        }
    )
    OOG_TOP: bool = field(
        metadata={
            "description": "Indicateur si le conteneur est hors gabarit (OOG) en haut.",
            "example": "0",
            "parent_field": "DIM (Dimensions)",
            "source_field": "DIM+13",
            "source_file": "LoadList.edi, OnBoard.edi, Tank.edi, rotation.csv",
        }
    )
    OOG_TOP_MEASURE: float = field(
        metadata={
            "description": "Mesure de hors gabarit (OOG) en haut en mètres.",
            "example": "0.0",
            "parent_field": "DIM (Dimensions)",
            "source_field": "DIM+13",
            "source_file": "LoadList.edi, OnBoard.edi, Tank.edi, rotation.csv",
        }
    )



import requests
from requests.auth import HTTPBasicAuth
import json

def get_access_token(token_url, username, password):

  token_response = requests.post(token_url, auth=HTTPBasicAuth(username, password))

  if token_response.status_code == 200:
      return token_response.json()["accessToken"]
  else:
      print(f"Authentication failed with status code: {token_response.status_code}")
      print(token_response.text)

import time

username = "amet1264@gmail.com"
password = "Sipside12@"
token_url = "https://api.bic-boxtech.org/oauth/token"

accessToken = get_access_token(token_url, username, password)

def get_size_and_type_info(codes, accessToken):

  baseUrl = "https://api.bic-boxtech.org"
  basePath = "api/v2.0/"

  size_and_types = []
  total = len(codes)

  counter = 0

  if accessToken:

    for index, code in enumerate(codes):

      print(f"Getting size and type info for code: {code} {index+1}/{total}")

      response = requests.get(
          url = f"{baseUrl}/{basePath}iso/size_type_code/{code}",
          headers = {
              "Authorization": accessToken
          }
      )

      counter += 1

      if counter % 30 == 0:
        time.sleep(5)

      if response.status_code == 200:
        size_and_types.append(response.json())
      else:
        print(f"Request failed with status code: {response.status_code}")
        print(response.text)

    return size_and_types

  else:
    print("Failed to get access token")

codes = [
  "22G0", "22G1", "22G4", "22K2", "22K8", "22P0", "22P1", "22P3", "22R0", "22R1", "22T0",
  "22T6", "22U0", "22U1", "25G1", "42G0", "42G1", "42G4", "42P0", "42P3", "42U0", "42U1",
  "45G0", "45G1", "45P3", "45R0", "45R1", "45R4", "45U1", "45U9", "4EG1", "L5G0", "L5G1",
  "LEGB",
]

size_and_type_info = get_size_and_type_info(codes, accessToken)

print(json.dumps(size_and_type_info, indent=2))

df_codes = flatten_json(json_normalize(size_and_type_info, sep="_"))

df_codes["group_st"] = df_codes["group_st"].str[2:]

df_codes.rename(columns={
      "detail_st": "Type",
      "group_st": "cType",
      "dimensions_length_mm": "Size_MM",
      "dimensions_length_ft": "Size",
      "dimensions_height_mm": "Height_MM",
      "dimensions_height_ft": "Height_ft",
      "dimensions_width_mm": "Width_MM",
      "dimensions_width_ft": "Width_ft",
    },
    inplace=True
)

df_codes = df_codes[["Type", "cType", "Size_MM", "Size", "Height_MM", "Height_ft", "Width_MM", "Width_ft", "description", "main_characteristics"]]

df_codes

df_codes.to_json("/content/size_and_type_codes.json", indent=2, orient="records")


# Function to calculate the overstowPort for a given stack
def set_overstowPort(df_containers):

    df_containers = df_containers.sort_values(by=["Stack", "Tier"], ascending=[True, False]).copy()

    for stack in df_containers['Stack'].unique():
      try:
        df_containers["stack_closest_pod"] = df_containers.loc[df_containers["Stack"] == stack, ["POD_nb"]].min()
        df_containers["stack_closest_pod_tier"] = df_containers.loc[df_containers.query("Stack == @stack")["POD_nb"].idxmin(), "Tier"]
        df_containers["stack_is_same_stack"] = df_containers["Stack"] == stack
        df_containers["container_is_not_null"] = df_containers["Container"].notnull()
        df_containers["tier_is_greater_than_closest_pod_tier"] = df_containers['Tier'] >= df_containers["stack_closest_pod_tier"]
        df_containers["pod_nb_is_greater_than_closest_pod"] = df_containers['POD_nb'] > df_containers["stack_closest_pod"]
        df_containers["next_line_not_in_same_tier"] = np.abs(df_containers['Tier'].astype(int).shift(-1) != df_containers['Tier'].astype(int))
        df_containers["next_line_not_in_adjacent_bay"] = np.abs(df_containers['Bay'].astype(int).shift(-1) - df_containers['Bay'].astype(int))  > 1

        # display(
        #     df_containers[[
        #       "Container",
        #       "Slot",
        #       "Size",
        #       "Bay",
        #       "SubBay",
        #       "Row",
        #       "Tier",
        #       "Stack",
        #       "POL_nb",
        #       "POD_nb",
        #       "overstowPort",

        #       "stack_closest_pod",
        #       "stack_closest_pod_tier",
        #       "stack_is_same_stack",
        #       "container_is_not_null",
        #       "tier_is_greater_than_closest_pod_tier",
        #       "pod_nb_is_greater_than_closest_pod",
        #       "next_line_not_in_same_tier",
        #       "next_line_not_in_adjacent_bay",
        #     ]].query("stack_is_same_stack == True")
        # )

      except Exception as e:
          print(f"Error with stack {stack}: {e}")

    df_containers["overstowPort"] = ""

    df_containers.loc[
        (df_containers["stack_is_same_stack"] == True)
        & (df_containers["container_is_not_null"] == True)
        & (df_containers["tier_is_greater_than_closest_pod_tier"] == True)
        & (df_containers["pod_nb_is_greater_than_closest_pod"] == True)
        & (df_containers["next_line_not_in_same_tier"] == True)
        & (df_containers["next_line_not_in_adjacent_bay"] == True)
        , "overstowPort"
    ] = df_containers["stack_closest_pod"]


    return df_containers


# Function to calculate the overstowPort for a given stack
def set_overstowPort_for_stack(df_containers, stack, stack_column="Stack"):
    # Filter rows for the specific stack

    df_container_stack_filtered = df_containers.query(f"{stack_column} == '{stack}' and Container.notnull()").copy()

    try:
        # Calculate the closest POD (port of discharge)
        closest_pod_tier = df_container_stack_filtered.loc[df_container_stack_filtered["POD_nb"].idxmin(), "Tier"]
        closest_pod = df_container_stack_filtered["POD_nb"].min()
        next_line_bay = df_container_stack_filtered['Bay'].astype(int).shift(-1)

        # Update the overstowPort column in the original DataFrame
        df_containers.loc[
            (df_containers[stack_column] == stack) &
            (df_containers['Tier'] >= closest_pod_tier) &
            (df_containers['POD_nb'] > closest_pod) &
            (np.abs(df_containers['Bay'].astype(int) - next_line_bay) <= 1),
            'overstowPort'
        ] = closest_pod

        # if stack.endswith("1"):


    except Exception as e:
        print(f"Error with stack {stack}: {e}")


def set_overstowPort(df_containers, stack_column="Stack"):

    for stack in df_containers[stack_column].unique():
        set_overstowPort_for_stack(df_containers, stack)
    
    return df_containers




parser = argparse.ArgumentParser()
parser.add_argument("--simulation", "-s", type=str, default='164', help="Simulation number")
parser.add_argument("--env", "-e", type=str, default="prod", help="Environment (prod or dev)")
# parser.add_argument("--type", "-t", type=str, default="onboard", help="File type (loadlist or onboard or tank. First char or number is also possible. You can add [type]_test in order to run on a test data)", choices=["loadlist", "onboard", "tank", 0, 1, 2, "l", "o", "t"])

args = parser.parse_args()

simulation = args.simulation
env = args.env

# def check_if_test(args_type, input_type):
#     if "test" in str(args_type).lower():
#         return f"{input_type}_test"
#     return input_type

# if str(args.type).lower() in (None, "onboard+loadlist", "o+l", 0):
#     pass
# if str(args.type).lower() in ("onboard", "o", 1):
#     input_type = "OnBoard"
# elif str(args.type).lower() in ("loadlist", "l", 2):
#     input_type = "LoadList"
# elif str(args.type).lower() in ("tank", "t", 3):
#     input_type = "Tank"
# else:
#    

  df_filtered = df_filtered.query("@hatch_section == HatchSection and @pod_nb > POD_nb and @tier > Tier")[utils_columns].copy()
