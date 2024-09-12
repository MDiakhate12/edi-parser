import os
import json
import glob
from typing import Dict, Any

import pandas as pd

from computation_rules import functional_rules, legacy_rules
from utils import pandas_utils, preprocessing_utils, referential_utils

# --- Configuration and Constants ---

# Consider moving these to a separate configuration file (e.g., config.py)
# or using environment variables for greater flexibility.
FINAL_COLUMNS = {
    'EQD_CN.EQD.equipment_identification.equipment_identifier': "Container",
    'LOC_147.location_identification.location_identifier': "Slot",
    # ... (rest of your column mappings)
}

FINAL_SCHEMA = {
    "Container": {"type": str, "fillna": ""},
    "LoadPort": {"type": str, "fillna": ""},
    # ... (rest of your schema definitions)
}

AGGREGATION_FUNCTIONS = {
    'cDG': 'min',  # Simplified aggregation specification
    'overstowPort': 'min',
    'Stowage': 'max',
    'DGheated': 'min',
}

STACK_ID_COLUMNS = ["MacroBay", "Row", "MacroTier"] 

# --- Data Loading Functions ---

def load_input_data(input_data_path: str) -> pd.DataFrame:
    """Loads and preprocesses the input JSON data."""
    print(f"Reading input data from: {input_data_path}")
    with open(input_data_path, "r") as f:
        data = json.load(f)
    df = pd.json_normalize(data)
    return pandas_utils.recurive_flatten_and_explode(df)

def load_referential_stacks(referential_stacks_path: str) -> pd.DataFrame:
    """Loads and preprocesses the referential stacks data."""
    print(f"Reading referential stacks data from: {referential_stacks_path}")
    df_stacks = pd.read_csv(referential_stacks_path, sep=";", header=0, dtype=str)
    return preprocessing_utils.preprocessess_stack_data(df_stacks, STACK_ID_COLUMNS)

def load_referential_hz_imdg(referential_hz_imdg_path: str) -> pd.DataFrame:
    """Loads the referential hz_imdg_exis_subs data."""
    print(f"Reading referential hz_imdg_exis_subs data from: {referential_hz_imdg_path}")
    return pd.read_csv(referential_hz_imdg_path, sep=",", header=0, dtype=str)

def load_rotation_data(input_rotation_path: str) -> pd.DataFrame:
    """Loads the rotation data."""
    print(f"Reading rotation data from: {input_rotation_path}")
    return pd.read_csv(input_rotation_path, sep=";", header=0, dtype=str).query("ShortName != 'MYPKG'")

# --- Data Transformation Functions ---

def apply_functional_rules(df: pd.DataFrame, df_hz_imdg_exis_subs: pd.DataFrame) -> pd.DataFrame:
    """Applies all functional rules to the DataFrame."""
    df["Size"] = df.apply(
        functional_rules.get_size, 
        axis=1, 
        size_referential=referential_utils.size_and_type_codes.SIZE_CODES_MAP, 
        size_unit="ft", 
        size_index=0
    )
    df["Height_ft"] = df.apply(
        functional_rules.get_size, 
        axis=1, 
        size_referential=referential_utils.size_and_type_codes.HEIGHT_CODES_MAP, 
        size_unit="ft", 
        size_index=1
    )
    # ... (Apply other functional rules similarly)

    # Example of applying a rule that depends on another DataFrame
    dg_stowage_codes = functional_rules.get_dg_stowage_codes(df_hz_imdg_exis_subs)
    df["dg_IMDG_AMENDMENT"] = df["dg_version"].apply(lambda x: x.split("-")[0] if pandas_utils.is_not_null(x) else x)
    df_with_stowage_codes = df.merge(
        dg_stowage_codes,
        left_on=["dg_UNNO", "dg_IMDG_AMENDMENT"],
        right_on=["UNNO", "IMDG_AMENDMENT"],
        how="left",
    )
    df["DGheated"] = df_with_stowage_codes.apply(functional_rules.get_dgheated, axis=1)

    return df

def apply_legacy_rules(df: pd.DataFrame, df_rotation: pd.DataFrame) -> pd.DataFrame:
    """Applies legacy rules to the DataFrame."""
    df_ports = legacy_rules.add_pol_pod_nb(df, df_rotation)
    df["POL_nb"] = df_ports["POL_nb"]
    df["POD_nb"] = df_ports["POD_nb"]
    return df

def apply_preprocessing(df: pd.DataFrame, df_stacks: pd.DataFrame) -> pd.DataFrame:
    """Applies preprocessing steps to the DataFrame."""
    df["Weight"] = preprocessing_utils.convert_measure_column(
        df["Weight"], df["Weight_unit_code"], referential_utils.unit_codes.WEIGHT_UNIT_CODES_TNE
    ).round(3)
    # ... (Apply other preprocessing steps)
    df = preprocessing_utils.add_stack_infos(df, df_stacks)
    return df

# --- Main Processing Function ---

def process_containers_data(
    simulation: str,
    env: str,
    parent_dir: str,
    input_type: str,
) -> pd.DataFrame:
    """Main function to orchestrate the processing of containers data."""

    # --- 1. Define Paths ---
    input_dir = os.path.join(parent_dir, "output_data", f"simulation_{simulation}_{env}")
    referential_dir = os.path.join(parent_dir, "data", "referential")
    input_json_file = os.path.join(input_dir, f"{input_type}.json")
    input_data_path = glob.glob(input_json_file)[0]  # Assuming a single match
    referential_stacks_path = os.path.join(referential_dir, "vessels", "9454448", "Stacks Extrait Prototype MP_IN.csv")
    referential_hz_imdg_path = os.path.join(referential_dir, "hz_imdg_exis_subs.csv")
    input_rotation_path = os.path.join(parent_dir, "data", "simulations", f"simulation_{simulation}_{env}", "in", "rotation.csv")
    output_containers_path = os.path.join(parent_dir, "output_data", f"simulation_{simulation}_{env}", f"containers_{simulation}_{input_type}.csv")

    # --- 2. Load Data ---
    df_raw = load_input_data(input_data_path)
    df_stacks = load_referential_stacks(referential_stacks_path)
    df_hz_imdg = load_referential_hz_imdg(referential_hz_imdg_path)
    df_rotation = load_rotation_data(input_rotation_path)

    # --- 3. Initial Data Preparation ---
    df = df_raw[FINAL_COLUMNS.keys()].rename(columns=FINAL_COLUMNS).copy()
    df.replace(["", None], pd.NA, inplace=True)  # Consistent null handling
    df.dropna(subset=["Container"], inplace=True)
    df = df.drop_duplicates()

    # --- 4. Apply Rules and Transformations ---
    df = apply_functional_rules(df, df_hz_imdg)
    df = apply_legacy_rules(df, df_rotation)
    df = apply_preprocessing(df, df_stacks)

    # --- 5. Finalize DataFrame ---
    df = df[FINAL_SCHEMA.keys()]  # Enforce column order
    df = df.astype({k: v["type"] for k, v in FINAL_SCHEMA.items()})
    df = df.fillna({k: v["fillna"] for k, v in FINAL_SCHEMA.items()})

    # --- 6. Aggregate Duplicates ---
    df = preprocessing_utils.aggregate_duplicates(df, AGGREGATION_FUNCTIONS)

    # --- 7. Save Output ---
    print(f"Writing output containers data to: {output_containers_path}")
    df.to_csv(output_containers_path, index=False, sep=";")

    return df

# --- Entry Point (if running as a script) ---
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Process containers data.")
    parser.add_argument("simulation", type=str, help="Simulation identifier")
    parser.add_argument("env", type=str, help="Environment identifier")
    parser.add_argument("parent_dir", type=str, help="Parent directory path")
    parser.add_argument("input_type", type=str, help="Input type (e.g., 'OnBoard')")
    args = parser.parse_args()

    process_containers_data(
        args.simulation, args.env, args.parent_dir, args.input_type
    )
