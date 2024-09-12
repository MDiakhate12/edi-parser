from utils import pandas_utils
import pandas as pd


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


    macrobay_map = pandas_utils.df_to_dict(
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


def find_slot(data, target_value):

    for i, item in enumerate(data):
        location_identifier = str(item["LOC_147"]["location_identification"]["location_identifier"])
        location_to_find = str(target_value).zfill(7)
        if location_identifier == location_to_find:
            return i, item  # Return the index and the dictionary
    return None  # If not found, return None



def convert_measure(value, unit_code, unit_code_map):
  """Convert measure values to meters or kilograms."""

  return value * unit_code_map[unit_code]


def convert_measure_column(value_column, unit_code_column, unit_code_map, float_digits = None):
  """Convert measure column values to meters (or centimeters depending on unit_code_map given) or kilograms."""

  if type(unit_code_column) == str:
    result = value_column.fillna(0).astype(float) * unit_code_map[unit_code_column]
  else:
    result = value_column.fillna(0).astype(float) * unit_code_column.map(unit_code_map)

  if float_digits is not None:
    return result.round(float_digits)
  else:
    return result


def find_duplicates(df, keys):
  # Group the DataFrame by the key columns and count the occurrences
  duplicate_counts = df.groupby(keys)[keys[0]].count()

  # Filter for groups with more than one occurrence (duplicates)
  duplicates = duplicate_counts[duplicate_counts > 1]

  # Get the indices (rows) of the duplicate entries
  duplicate_indices = duplicates.index.tolist()

  # Filter the DataFrame to get the duplicate rows
  duplicate_rows = df[df.set_index(keys).index.isin(duplicate_indices)]

  return duplicate_rows



def get_duplicate_columns_with_different_values(df, keys):
  # Get the columns where duplicate rows have different values

  different_value_columns = []

  for col in df.columns:
      if df.groupby(keys)[col].nunique().max() > 1:
          different_value_columns.append(col)

  print("\nColumns with different values in duplicate rows:")
  print(different_value_columns)

  return different_value_columns



def aggregate_duplicate_columns(series, aggregation_functions, default_aggregation_function):
    agg_function = None

    if series.name in aggregation_functions:
        agg_function = aggregation_functions[series.name]
    else:
        agg_function = default_aggregation_function

    return agg_function(series)




def aggregate_duplicates(df_with_duplicates, aggregation_functions, default_aggregation_function):

  duplicated_columns = get_duplicate_columns_with_different_values(
      df = df_with_duplicates,
      keys = ["Container", "Slot"],
  )

  print(f"Source columns: {list(df_with_duplicates.columns)}")
  print(f"Duplicated columns: {list(duplicated_columns)}")

  other_columns = [col for col in df_with_duplicates.columns if col not in duplicated_columns]

  print(f"Other columns: {list(other_columns)}")

  return (
      df_with_duplicates
          .groupby(other_columns)
          .agg(
              lambda series: aggregate_duplicate_columns(
                  series,
                  aggregation_functions,
                  default_aggregation_function,
              )
          )
          .reset_index()
  )