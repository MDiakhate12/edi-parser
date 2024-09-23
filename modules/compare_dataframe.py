import pandas as pd
import numpy as np
from pprint import pprint

def is_not_null(row):
  """Check if the row value is not null. (a row is the value you get when doing df[col].apply(lambda row: ...))"""

  return (
      (row is not None) &
      (row != np.nan) &
      (str(row).strip() != "") &
      (str(row).lower() != "nan") &
      (str(row).lower() != "na")
  )

def df_to_dict(df, key_col, value_col):
  """Create a dictionary from a DataFrame where the first column value is the key and the second column value is the value"""
  return dict(zip(df[key_col], df[value_col]))

# preprocessing_utils

def preprocessess_stack_data(df_stacks, stack_id_columns):

    """Read stack data and ensure Bay, Row, FirsTier, Subbay have the correct format.
       Create HatchSection column from Subbay"""

    print("Assure que la colonne Bay a 3 chiffres en ajoutant des zéros à gauche si nécessaire (BBB)")
    df_stacks["Bay"] = df_stacks["Bay"].str.zfill(3)

    print("Assure que la colonne Row a 2 chiffres en ajoutant des zéros à gauche si nécessaire (RR)")
    df_stacks["Row"] = df_stacks["Row"].str.zfill(2)

    print("Assure que la colonne FirstTier a 2 chiffres en ajoutant des zéros à gauche si nécessaire (TT)")
    df_stacks["FirstTier"] = df_stacks["FirstTier"].str.zfill(2)

    print("Assure que la colonne SubBay a 4 chiffres en ajoutant des zéros à gauche si nécessaire (SSSS)")
    df_stacks["SubBay"] = df_stacks["SubBay"].str.zfill(4)

    print("Extrait les trois premiers chiffres de SubBay pour créer la colonne HatchSection")
    df_stacks["HatchSection"] = df_stacks["SubBay"].str[:-1]


    df_stacks["MacroBay"] = [ 2+round((int(row)-2)/4)*4 for row in df_stacks["Bay"].astype(int) ]
    df_stacks["MacroRow"] = [ int(str(sb)[-2:-1]) for sb in df_stacks["SubBay"] ]


    macrobay_map = df_to_dict(
      df = df_stacks.groupby("MacroBay").agg({"Bay": lambda x: sorted(set(x))}).reset_index(),
      key_col = "MacroBay",
      value_col = "Bay"
    )


    # Renomme la colonne Tier en MacroTier
    print("Renomme la colonne Tier en MacroTier")
    df_stacks = df_stacks.rename(columns={"Tier": "MacroTier"}).reset_index(drop=True)

    df_stacks["MacroRow"] = [ int(str(sb)[-2:-1]) for sb in df_stacks["SubBay"] ]

    # Crée la colonne Stack à partir de SubBay et Row
    print(f"Crée la colonne Stack à partir de {stack_id_columns}")

    concat_stack_columns = lambda row: "".join([str(row[c]) for c in stack_id_columns])

    df_stacks["Stack"] = df_stacks.apply(concat_stack_columns, axis=1)


    print(f"Crée la colonne First_20_40_Stack à partir de {stack_id_columns}")
    df_stacks["First_20_40_Stack"] = df_stacks.apply(lambda x: concat_stack_columns(x) if x["Bay"] in macrobay_map[x["MacroBay"]][:2] else -1, axis=1)

    print(f"Crée la colonne Second_20_40_Stack à partir de {stack_id_columns}")
    df_stacks["Second_20_40_Stack"] = df_stacks.apply(lambda x: concat_stack_columns(x) if x["Bay"] in macrobay_map[x["MacroBay"]][1:] else -1, axis=1)

    print(f"Crée la colonne MacroStack à partir de ['MacroBay', 'Row']")
    df_stacks["MacroStack"] = df_stacks.apply(lambda row: "".join([str(row[c]) for c in ['MacroBay', 'Row']]), axis=1)

    return df_stacks


# Fonction pour déterminer le MacroTier à partir du Slot
def get_macro_tier(slot: str):
    if slot == "":
        return ""

    if slot.isdigit() and len(slot) >= 5:
        tier = slot[-2:]

        # Si le tier est supérieur à 50 alors le conteneur est sur le pont (deck)
        if int(tier) >= 50:
            return "1"

        # Sinon le conteneur est dans la cale
        else:
            return "0"
    else:
        raise ValueError(
            f"Format de slot invalide, le slot {slot} de longueur {len(slot)} doit être composé uniquement de chiffres et être au format BBBRRTT (B Bay, R Row, T Tier)"
        )


def compute_bay_row_and_tier(df):
    """Compute Bay, Row, and Tier and MacroTier columns from Slot column."""

    # Extraction des informations de Bay, Row, et Tier à partir de la colonne Slot
    print("Extraction des informations de Bay, Row, et Tier à partir de la colonne Slot")


    df_filled = df.copy()
    df_filled["Slot"] = df_filled["Slot"].fillna("").astype(str)


    df["Tier"] = df_filled["Slot"].apply(lambda slot: slot[-2:])
    df["Row"] = df_filled["Slot"].apply(lambda slot: slot[-4:-2])
    df["Bay"] = df_filled["Slot"].apply(lambda slot: str(slot[:-4]).zfill(3))
    # df["MacroBay"] = [ 2+round((int(row)-2)/4)*4 for row in df["Bay"].astype(int) ]


    # Application de la fonction pour créer la colonne MacroTier
    print("Application de la fonction pour créer la colonne MacroTier")

    df["MacroTier"] = df_filled["Slot"].apply(get_macro_tier)

    return df



def add_stack_infos(df, df_stacks):
  return compute_bay_row_and_tier(df).merge(df_stacks, how="left", on=["Bay", "Row", "MacroTier"])

df_rotation = pd.read_csv("/content/rotation.csv", sep=";", header=0, dtype=str).query("ShortName != 'MYPKG'")

final_schema = {
    "Container": {"type": str, "fillna": ""},
    "LoadPort": {"type": str, "fillna": ""},
    "POL_nb": {"type": int, "fillna": -2*len(df_rotation)},
    "DischPort": {"type": str, "fillna": ""},
    "POD_nb": {"type": int, "fillna": 2*len(df_rotation)},
    "Size": {"type": int, "fillna": 0},
    "cType": {"type": str, "fillna": ""},
    "cWeight": {"type": str, "fillna": ""},
    "Height": {"type": str, "fillna": ""},
    "cDG": {"type": str, "fillna": ""},
    "Empty": {"type": str, "fillna": ""},
    "Revenue": {"type": int, "fillna": 1},
    "Type": {"type": str, "fillna": ""},
    "Setting": {"type": str, "fillna": ""},
    "Weight": {"type": float, "fillna": 0.0},
    "Slot": {"type": str, "fillna": ""},
    "priorityID": {"type": int, "fillna": -1},
    "priorityLevel": {"type": int, "fillna": -1},
    "overstowPort": {"type": str, "fillna": ""},
    "NonReeferAtReefer": {"type": str, "fillna": ""},
    "Subport": {"type": str, "fillna": ""},
    "Stowage": {"type": str, "fillna": ""},
    "DGheated": {"type": int, "fillna": ""},
    "Exclusion": {"type": str, "fillna": ""},
    "OOG_FORWARD": {"type": int, "fillna": 0},
    "OOG_AFTWARDS": {"type": int, "fillna": 0},
    "OOG_RIGHT": {"type": int, "fillna": 0},
    "OOG_RIGHT_MEASURE": {"type": float, "fillna": 0.0},
    "OOG_LEFT": {"type": int, "fillna": 0},
    "OOG_LEFT_MEASURE": {"type": float, "fillna": 0.0},
    "OOG_TOP": {"type": int, "fillna": 0},
    "OOG_TOP_MEASURE": {"type": float, "fillna": 0.0},
}

final_columns_order = list(final_schema.keys())
final_schema_types = {k: v["type"] for k, v in final_schema.items()}
final_schema_fillna_values = {k: v["fillna"] for k, v in final_schema.items()}

df_stacks = pd.read_csv("/content/stacks.csv", sep=";", header=0, dtype=str)
df_stacks = preprocessess_stack_data(df_stacks, ["MacroBay", "Row", "MacroTier"])
df_containers_old = pd.read_csv(f"/content/containers.csv", sep=";").fillna("").astype(final_schema_types).sort_values(by="Container")
df_containers_new = pd.read_csv(f"/content/containers_new.csv", sep=";").fillna("").astype(final_schema_types).sort_values(by="Container")

df_containers_old["Slot"] = df_containers_old["Slot"].apply(lambda x: x.replace(".0", ""))
df_containers_old["cDG"] = df_containers_old["cDG"].apply(lambda x: x.replace(".0", ""))
df_containers_old["overstowPort"] = df_containers_old["overstowPort"].apply(lambda x: x.replace(".0", ""))
df_containers_old["Slot"] = df_containers_old["Slot"].apply(lambda x: x.replace(".0", "").zfill(7) if x != "" else x)
df_containers_old = add_stack_infos(df_containers_old, df_stacks)
df_containers_old["Bay"] = df_containers_old["Bay"].astype(str).apply(lambda x: x.replace(".0", ""))
df_containers_old["MacroBay"] = df_containers_old["MacroBay"].astype(str).apply(lambda x: x.replace(".0", ""))

containers_old = df_containers_old[df_containers_old["POL_nb"] != 1][final_columns_order].copy()
containers_old["ON_HOLD"] = containers_old.Slot.str[-2:].astype(int) < 50

containers_new = df_containers_new[df_containers_new["Container"].apply(is_not_null)][final_columns_order].copy()
containers_new["ON_HOLD"] = containers_new.Slot.str[-2:].astype(int) < 50

def get_differences(df_new, df_old, keys, column, other_columns = None):

    other_columns = [] if other_columns is None else other_columns

    columns = [*keys, column, *other_columns]
    return (
        df_new[columns]
        .merge(
            df_old[columns],
            on=keys,
            how="outer",
            suffixes=("_new", "_old"),
        )
        .query(f"{column}_new != {column}_old")
    )


def compare_dataframes(containers_new, containers_old):

  df_compare_all = containers_new.reset_index(drop=True) == containers_old.reset_index(drop=True)
  df_compare_hold = containers_new.query("ON_HOLD==True").reset_index(drop=True) == containers_old.query("ON_HOLD==True").reset_index(drop=True)
  df_compare_deck = containers_new.query("ON_HOLD==False").reset_index(drop=True) == containers_old.query("ON_HOLD==False").reset_index(drop=True)

  def compare(df_compare):

    good_columns = []
    columns_to_check = []

    columns_to_compare = [c for c in df_compare.columns if c not in ["Revenue", "Exclusion"]]

    for c in columns_to_compare:

      correct_columns_count = (df_compare[c] == True).sum()
      incorrect_columns_count = (df_compare[c] == False).sum()

      if correct_columns_count == df_compare.shape[0]:
          good_columns.append((c, correct_columns_count))
      else:
          columns_to_check.append((c, incorrect_columns_count))

    print("Good columns :")
    pprint(good_columns)
    print(len(good_columns), "/", len(columns_to_compare))

    print("\nColumns to check :")
    pprint(columns_to_check)
    print(len(columns_to_check), "/", len(columns_to_compare))

  print("All columns :")
  compare(df_compare_all)

  print("\nHold columns :")
  compare(df_compare_hold)

  print("\nDeck columns :")
  compare(df_compare_deck)

compare_dataframes(containers_new, containers_old)