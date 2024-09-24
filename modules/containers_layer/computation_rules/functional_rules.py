import re

import pandas as pd
import numpy as np

from modules.containers_layer.utils import pandas_utils



def get_non_reefer_at_reefer(row):
    """Checks if non-reefer container is at reefer slot."""

    if pandas_utils.is_not_null(row["FirstTier"]) and pandas_utils.is_not_null(row["Tier"]):

        FirstTier = int(row["FirstTier"])
        Tier = int(row["Tier"])
        NbReefer = int(row["NbReefer"])
        ContainerType = row["cType"]

        slot_is_reefer = (FirstTier <= Tier <= FirstTier + 2 * (NbReefer - 1))
        container_is_not_reefer = (ContainerType != "RE")

        return "X" if slot_is_reefer and container_is_not_reefer else ""

    else:

        return ""



def get_dg_stowage_codes(df_hz_imdg_exis_subs):
  return (
      df_hz_imdg_exis_subs
      .dropna(subset=["DGIES_STOW"])
      .assign(
        DGIES_STOW = lambda x: x["DGIES_STOW"].fillna("").str.split(" "),
        TK_DATE = lambda x: pd.to_datetime(x["TK_DATE"], format="%Y-%m-%dT%H:%M:%SZ"),
      )
      .explode("DGIES_STOW")
      .drop_duplicates()
      .groupby(["UNNO", "IMDG_AMENDMENT"])
      .agg(
          STOWAGE_CODES=("DGIES_STOW", lambda x: "-".join(set(x))),
          STOWAGE_CODES_COUNT=("DGIES_STOW", 'count'),
          STOWAGE_CODES_HAS_SW1=("DGIES_STOW", lambda x: "SW1" in list(x)),
      )
      .query("STOWAGE_CODES_COUNT >= 1")
      .sort_values(by="STOWAGE_CODES_COUNT", ascending=False)
      .drop(columns=["STOWAGE_CODES_COUNT"])
      .reset_index()
  )



def get_stowage_location(row):
  """Checks if stowage is on deck or under/below deck based on the text."""

  handling_code = str(row["handling_code"])
  handling_description = str(row["handling_description"])

  handling_code_under_deck = re.search(r"UND", handling_code, flags=re.IGNORECASE)
  handling_description_under_deck = re.search(r".*(under|below).*deck", handling_description, flags=re.IGNORECASE)

  handling_code_on_deck = re.search(r"OND", handling_code, flags=re.IGNORECASE)
  handling_description_on_deck = re.search(r".*(on).*deck", handling_description, flags=re.IGNORECASE)

  if handling_code_under_deck or handling_description_under_deck:
    return "HOLD"

  elif handling_code_on_deck or handling_description_on_deck:
    return "DECK"

  else:
    return ""

  

def get_dgheated(row):
  """Checks if stowage is away from heat based on the text.
  Rule:
    IF -> Free Text contains SW1 stowage instruction (Stowage Away From Heat)
    OR -> Dangerous Goods class is one of 2, 3, 4.1, 4.2, 4.3, 5.2 (classes that should not be close to a heat source (eg: sun, motors, reefer containers...))
    OR -> Handling instructions code contains KC or KCO (Keep Cool)"""

  stowage = str(row["Stowage"])

  dg_free_text = str(row["dg_free_text"])
  handling_instructions_code = str(row["handling_code"])
  stowage_code_has_sw1 = str(row["STOWAGE_CODES_HAS_SW1"])

  dg_class_and_subclasses = [
      str(row["dg_class"]),
      str(row["dg_subclass_1"]),
      str(row["dg_subclass_2"]),
      str(row["dg_subclass_3"]),
  ]

  dg_classes_protected_from_heat_on_deck = ["2.1", "2.2", "2.3", "3", "4.2", "4.3"]
  dg_classes_protected_from_heat = ["4.1", "5.2"]

  dg_free_text_sw1 = "SW1" in dg_free_text # dg free text that should be stowed away from heat
  handling_instructions_code_kc = handling_instructions_code.startswith("KC") or handling_instructions_code.startswith("KCO") # handling instructions code that should be stowed away from heat
  dg_stowage_code_has_sw1 = stowage_code_has_sw1.lower() == "true" # stowage code that should be stowed away from heat

  dg_protected_from_heat_on_deck = any(c in dg_classes_protected_from_heat_on_deck for c in dg_class_and_subclasses) and stowage == "DECK"
  dg_protected_from_heat = any(c in dg_classes_protected_from_heat for c in dg_class_and_subclasses)


  conditions = (
      dg_free_text_sw1 or
      handling_instructions_code_kc or
      dg_stowage_code_has_sw1 or
      dg_protected_from_heat_on_deck or
      dg_protected_from_heat
  )
  

  if conditions:
    return 1
  else:
    return 0
  


def get_cweight(row):
  if pandas_utils.is_not_null(row):
    return "L" if int(row) < 8 else "H"
  else:
    return np.nan

  

def get_size(row, size_referential, size_unit="ft", size_index=0):

  size_code = str(row["Type"])[size_index] if pandas_utils.is_not_null(row["Type"]) else np.nan

  # size_code = str(row["Type"])[0] if pandas_utils.is_not_null(row["Type"]) else np.nan

  if size_code not in size_referential:
    raise ValueError(f"Invalid size code: {size_code} in Type {row['Type']} not in size codes referential {size_referential}")

  size = size_referential[size_code][size_unit]

  if type(size) is list and size[1] == np.inf:
    return size[0]

  elif type(size) is list:
    return (size[0] + size[1]) / 2

  elif type(size) is float:
    return size

  else:
    return size
  

def get_overstowPort(row, df):

  pod_nb = row["POD_nb"]
  hatch_section = row["HatchSection"]
  tier = row["Tier"]
  macro_tier = row["MacroTier"]
  bay = row["Bay"]
  macro_stack = row["MacroStack"]

  def remove_floating_zero(interger_string):
    if pandas_utils.is_not_null(interger_string):
      return str(interger_string).replace(".0", "")
    else:
      return ""

  utils_columns = ["Container", "HatchSection", 'Bay', 'MacroStack', "MacroTier", 'Tier', 'POD_nb']

  df_filtered = df[
      (row["HatchSection"] == df["HatchSection"]) &
      (row['POD_nb'] > df['POD_nb']) &
      (row['Tier'] > df['Tier'])
  ][utils_columns].copy()

  overstow_port = df_filtered[
    (row['MacroStack'] == df_filtered['MacroStack']) &
    (np.abs(int(row['Bay']) - df_filtered['Bay'].astype(int)) <= 1)
  ]["POD_nb"].min()

  if not pandas_utils.is_not_null(overstow_port):
    overstow_port = np.inf

  if row["MacroTier"] == "1":
    min_port_below_deck = df_filtered.query("MacroTier == '0'")["POD_nb"].min()
    
    if not pandas_utils.is_not_null(min_port_below_deck):
      min_port_below_deck = np.inf

    overstow_port = min(overstow_port, min_port_below_deck)

  if overstow_port == np.inf:
    return ""

  return remove_floating_zero(overstow_port)