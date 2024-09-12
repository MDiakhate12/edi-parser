import os
import json
import glob

import numpy as np
import pandas as pd

from pandas import json_normalize

from computation_rules import functional_rules, legacy_rules

from utils import pandas_utils, preprocessing_utils, referential_utils

def run(
    simulation,
    env,
    parent_dir,
    input_type,
):
    print(f"Running containers.csv generation for simulation {simulation} in environment {env} and input type {input_type}...")
    
    print("Define input and output directory")
    input_dir = os.path.join(
        parent_dir,
        "output_data",
        f"simulation_{simulation}_{env}",
    )

    print("Define referential directory")
    referential_dir = os.path.join(
        parent_dir,
        "data",
        "referential",
    )

    print("Define referential stacks directory")
    referential_stacks_path = os.path.join(
        referential_dir,
        "vessels",
        "9454448",
        "Stacks Extrait Prototype MP_IN.csv"
    )

    print("Define referential hz_imdg_exis_subs directory")
    referential_hz_imdg_exis_subs_path = os.path.join(
        referential_dir,
        "hz_imdg_exis_subs.csv"
    )

    print("Define rotation directory")
    input_rotation_path = os.path.join(
        parent_dir,
        "data",
        "simulations",
        f"simulation_{simulation}_{env}",
        "in",
        "rotation.csv"
    )

    print("Define input data path")
    input_json_file = os.path.join(input_dir, f"{input_type}.json")
    print(input_json_file)
    input_data_path = glob.glob(input_json_file)
    input_data_path = input_data_path[0]


    print("Read input data")
    with open(input_data_path, "r") as f:
        data = json.load(f)

    print("Normalize input data")
    df = json_normalize(data)

    print("Flatten input data")
    df_flatten = pandas_utils.recurive_flatten_and_explode(df)

    print(df_flatten.head())

    print("Define final columns")
    final_columns = {
    'EQD_CN.EQD.equipment_identification.equipment_identifier': "Container",
    'LOC_147.location_identification.location_identifier': "Slot",
    'EQD_CN.EQD.equipment_size_and_type.equipment_size_and_type_description_code': "Type",

    'EQD_CN.LOC_9_PORT_OF_LOADING.location_identification.location_identifier': "LoadPort",
    'EQD_CN.LOC_11_PORT_OF_DISCHARGE.location_identification.location_identifier': "DischPort",

    'EQD_CN.MEA_CONTAINER_WEIGHT.value_range.measure': "Weight",
    'EQD_CN.MEA_CONTAINER_WEIGHT.value_range.measurement_unit_code': "Weight_unit_code",
    'EQD_CN.MEA_CONTAINER_WEIGHT.measurement_details.measured_attribute_code': "Weight_attribute_code",

    'EQD_CN.EQD.full_or_empty_indication_code': "Empty",

    'EQD_CN.TMP_SG.TMP.temperature_setting.temperature_degree': 'temperature',
    'EQD_CN.TMP_SG.TMP.temperature_setting.measurement_unit_code': 'temperature_unit_code',

    'EQD_CN.HAN.handling_instruction.handling_instruction_description_code': "handling_code",
    'EQD_CN.HAN.handling_instruction.handling_instruction_description': "handling_description",

    'EQD_CN.DIM_8_OOG_LEFT_WIDTH_MEASURE.dimension_specification.width_measure': "OOG_LEFT_MEASURE_value",
    'EQD_CN.DIM_8_OOG_LEFT_WIDTH_MEASURE.dimension_specification.measurement_unit_code': "OOG_LEFT_MEASURE_unit_code",


    'EQD_CN.DIM_7_OOG_RIGHT_WIDTH_MEASURE.dimension_specification.width_measure': "OOG_RIGHT_MEASURE_value",
    'EQD_CN.DIM_7_OOG_RIGHT_WIDTH_MEASURE.dimension_specification.measurement_unit_code': "OOG_RIGHT_MEASURE_unit_code",


    'EQD_CN.DIM_13_OOG_TOP_HEIGHT_MEASURE.dimension_specification.height_measure': "OOG_TOP_MEASURE_value",
    'EQD_CN.DIM_13_OOG_TOP_HEIGHT_MEASURE.dimension_specification.measurement_unit_code': "OOG_TOP_MEASURE_unit_code",

    'EQD_CN.DIM_5_OOG_FRONT_LENGTH_MEASURE.dimension_specification.length_measure': "OOG_FORWARD_MEASURE_value",
    'EQD_CN.DIM_5_OOG_FRONT_LENGTH_MEASURE.dimension_specification.measurement_unit_code': "OOG_FORWARD_MEASURE_unit_code",

    'EQD_CN.DIM_6_OOG_BACK_LENGTH_MEASURE.dimension_specification.length_measure': "OOG_AFTWARDS_MEASURE_value",
    'EQD_CN.DIM_6_OOG_BACK_LENGTH_MEASURE.dimension_specification.measurement_unit_code': "OOG_AFTWARDS_MEASURE_unit_code",


    'EQD_CN.DGS.DGS.hazard_code.hazard_identification_code': "dg_class",
    'EQD_CN.DGS.DGS.dangerous_goods_label.marking_identifier_1': "dg_subclass_1",
    'EQD_CN.DGS.DGS.dangerous_goods_label.marking_identifier_2': "dg_subclass_2",
    'EQD_CN.DGS.DGS.dangerous_goods_label.marking_identifier_3': "dg_subclass_3",
    'EQD_CN.DGS.FTX_AAC.text_literal': "dg_free_text",
    'EQD_CN.DGS.DGS.undg_information': "dg_UNNO",
    'EQD_CN.DGS.DGS.hazard_code.hazard_code_version_identifier': "dg_version",
    'EQD_CN.DGS.ATT.attribute_details.attribute_description': "dg_proper_shipping_name",


    "edi_string": "edi_string",

    'EQD_CN.DIM_1_BREAKBULK_HEIGHT_MEASURE.dimension_specification.height_measure': "BREAKBULK_MEASURE_value",
    'EQD_CN.DIM_1_BREAKBULK_HEIGHT_MEASURE.dimension_specification.measurement_unit_code': "BREAKBULK_MEASURE_unit_code",

    'EQD_CN.DIM_19_COLLAPSED_FLAT_RACK_HEIGHT_MEASURE.dimension_specification.height_measure': "COLLAPSED_FLAT_RACK_MEASURE_value",
    'EQD_CN.DIM_19_COLLAPSED_FLAT_RACK_HEIGHT_MEASURE.dimension_specification.measurement_unit_code': "COLLAPSED_FLAT_RACK_MEASURE_unit_code",
    }

    used_columns = list(final_columns.keys())


    # Flatten
    print("Flatten input data")
    df_flatten_renamed = df_flatten[used_columns].rename(columns=final_columns).query("Container.notnull()")


    # Drop duplicates and replace empty strings and None with NaN
    print("Drop duplicates and replace empty strings and None with NaN")
    df_flatten_clean = df_flatten_renamed.drop_duplicates().copy()
    df_flatten_clean.replace("", np.nan, inplace=True)
    df_flatten_clean.replace([None], np.nan, inplace=True)


    print("Read referential stacks data")
    df_stacks = pd.read_csv(referential_stacks_path, sep=";", header=0, dtype=str)
    df_stacks = preprocessing_utils.preprocessess_stack_data(df_stacks, ["MacroBay", "Row", "MacroTier"])


    print("Read referential hz_imdg_exis_subs data")
    df_hz_imdg_exis_subs = pd.read_csv(referential_hz_imdg_exis_subs_path, sep=",", header=0, dtype=str)


    print("Read rotation data")
    df_rotation = pd.read_csv(input_rotation_path, sep=";", header=0, dtype=str).query("ShortName != 'MYPKG'")


    print("Read containers data")
    df_containers = df_flatten_clean.reset_index(drop=True).copy()

    print("Fill 'Slot' Column with leading zeros to have 7 characters")
    # df_containers["Slot"] = df_containers["Slot"].str.zfill(7)

    print("Compute functional column 'Size'")
    df_containers["Size"] = df_containers.apply(functional_rules.get_size, axis=1, size_referential=referential_utils.size_and_type_codes.SIZE_CODES_MAP, size_unit="ft", size_index=0)


    print("Compute functional column 'Height'")
    df_containers["Height_ft"] = df_containers.apply(functional_rules.get_size, axis=1, size_referential=referential_utils.size_and_type_codes.HEIGHT_CODES_MAP, size_unit="ft", size_index=1)
    df_containers["Height_m"] = df_containers.apply(functional_rules.get_size, axis=1, size_referential=referential_utils.size_and_type_codes.HEIGHT_CODES_MAP, size_unit="m", size_index=1)
    df_containers["Height"] = df_containers["Height_ft"].apply(lambda x: "HC" if x > 8.6 else "")


    print("Compute functional column 'cDG'")
    df_containers["cDG"] = df_containers["dg_class"].apply(lambda x: x if pandas_utils.is_not_null(x) else "")


    print("Compute functional column 'Stowage'")
    df_containers["Stowage"] = df_containers.apply(functional_rules.get_stowage_location, axis=1)


    # Set China port special case
    print("Compute functional column 'Stowage' - Set China port special case")
    df_containers.loc[(df_containers["cDG"] != "") & (df_containers["LoadPort"].str.startswith("CN")), "Stowage"] = "DECK"


    print("Compute functional column 'dg_IMDG_AMENDMENT'")
    dg_stowage_codes = functional_rules.get_dg_stowage_codes(df_hz_imdg_exis_subs)
    df_containers["dg_IMDG_AMENDMENT"] = df_containers["dg_version"].apply(lambda x: x.split("-")[0] if pandas_utils.is_not_null(x) else x)


    print("Compute functional column 'DGheated'")
    df_containers_with_stowage_codes = (
    df_containers
        .merge(
            dg_stowage_codes,
            left_on=["dg_UNNO", "dg_IMDG_AMENDMENT"],
            right_on=["UNNO", "IMDG_AMENDMENT"],
            how="left",
        )
    )
    df_containers["DGheated"] = df_containers_with_stowage_codes.apply(functional_rules.get_dgheated, axis=1)


    print("Compute functional column 'cType'")
    df_containers["cType"] = df_containers["temperature"].apply(lambda x: "RE" if pandas_utils.is_not_null(x) else "GP")


    print("Compute functional column 'Empty'")
    df_containers["Empty"] = df_containers["Empty"].apply(lambda x: "E" if str(x) == "4" else "")


    print("Compute functional column 'Type'")
    df_containers["Setting"] = np.where(df_containers["cType"] == "RE", "R", df_containers["Empty"])


    print("Compute functional column 'Weight'")
    df_containers["Weight"] = preprocessing_utils.convert_measure_column(df_containers["Weight"], df_containers["Weight_unit_code"] , referential_utils.unit_codes.WEIGHT_UNIT_CODES_TNE).round(3)


    print("Compute functional column 'cWeight'")
    df_containers[["Container", "Slot", "Type", "Size", "Height_ft", "Height_m", "Height", "Weight", "cDG"]]


    print("Compute functional column 'cWeight'")
    df_containers["cWeight"] = df_containers["Weight"].apply(functional_rules.get_cweight)


    print("Compute functional columns 'OOG_LEFT_MEASURE', 'OOG_RIGHT_MEASURE', 'OOG_TOP_MEASURE', 'OOG_FORWARD_MEASURE', 'OOG_AFTWARDS_MEASURE'")
    df_containers["OOG_LEFT_MEASURE"] = preprocessing_utils.convert_measure_column(df_containers["OOG_LEFT_MEASURE_value"], df_containers["OOG_LEFT_MEASURE_unit_code"], referential_utils.unit_codes.LENGTH_UNIT_CODES_CM)
    df_containers["OOG_RIGHT_MEASURE"] = preprocessing_utils.convert_measure_column(df_containers["OOG_RIGHT_MEASURE_value"], df_containers["OOG_RIGHT_MEASURE_unit_code"], referential_utils.unit_codes.LENGTH_UNIT_CODES_CM)
    df_containers["OOG_TOP_MEASURE"] = preprocessing_utils.convert_measure_column(df_containers["OOG_TOP_MEASURE_value"], df_containers["OOG_TOP_MEASURE_unit_code"], referential_utils.unit_codes.LENGTH_UNIT_CODES, 2)
    df_containers["OOG_FORWARD_MEASURE"] = preprocessing_utils.convert_measure_column(df_containers["OOG_FORWARD_MEASURE_value"], df_containers["OOG_FORWARD_MEASURE_unit_code"], referential_utils.unit_codes.LENGTH_UNIT_CODES_CM)
    df_containers["OOG_AFTWARDS_MEASURE"] = preprocessing_utils.convert_measure_column(df_containers["OOG_AFTWARDS_MEASURE_value"], df_containers["OOG_AFTWARDS_MEASURE_unit_code"], referential_utils.unit_codes.LENGTH_UNIT_CODES_CM)


    print("Compute functional columns 'OOG_LEFT', 'OOG_RIGHT', 'OOG_TOP', 'OOG_FORWARD', 'OOG_AFTWARDS'")
    df_containers["OOG_LEFT"] = df_containers["OOG_LEFT_MEASURE_value"].apply(lambda x: 1 if pandas_utils.is_not_null(x) else 0)
    df_containers["OOG_RIGHT"] = df_containers["OOG_RIGHT_MEASURE_value"].apply(lambda x: 1 if pandas_utils.is_not_null(x) else 0)
    df_containers["OOG_TOP"] = df_containers["OOG_TOP_MEASURE_value"].apply(lambda x: 1 if pandas_utils.is_not_null(x) else 0)
    df_containers["OOG_FORWARD"] = df_containers["OOG_FORWARD_MEASURE_value"].apply(lambda x: 1 if pandas_utils.is_not_null(x) else 0)
    df_containers["OOG_AFTWARDS"] = df_containers["OOG_AFTWARDS_MEASURE_value"].apply(lambda x: 1 if pandas_utils.is_not_null(x) else 0)


    print("Compute functional columns 'POL_nb' and 'POD_nb' with Legacy Rule")
    df_ports = legacy_rules.add_pol_pod_nb(df_containers, df_rotation)
    df_containers["POL_nb"] = df_ports["POL_nb"]
    df_containers["POD_nb"] = df_ports["POD_nb"]


    print("Add stacks columns to df_containers")
    df_containers = preprocessing_utils.add_stack_infos(df_containers, df_stacks)


    print("Compute functional column 'NonReeferAtReefer'")
    df_containers["NonReeferAtReefer"] = df_containers.apply(functional_rules.get_non_reefer_at_reefer, axis=1)


    print("Compute functional column 'overstowPort'")
    df_containers = df_containers.sort_values(by=["MacroStack", "Tier", "MacroTier"], ascending=[True, False, False])
    df_containers["overstowPort"] = df_containers.apply(functional_rules.get_overstowPort, df=df_containers, axis=1)



    print("Add 'is_onboard' tag")
    df_containers["is_onboard"] = input_type == "OnBoard"

    print("Handling ignored functional columns (not used for now) - setting to default constants :")

    print("Set Revenue functional column to 1")
    df_containers["Revenue"] = 1 # As specified in JIRA

    print("Set priorityID functional column to 1")
    df_containers["priorityID"] = -1

    print("Set priorityLevel functional column to -1")
    df_containers["priorityLevel"] = -1

    print("Set Subport functional column to empty string ''")
    df_containers["Subport"] = ''

    print("Set NonReeferAtReefer functional column to empty string ''")
    df_containers["Exclusion"] = ''

    print("Define final containers data schema and types of each column")
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

    print("Define final containers data columns order")
    final_columns_order = list(final_schema.keys())
    final_schema_types = {k: v["type"] for k, v in final_schema.items()}
    final_schema_fillna_values = {k: v["fillna"] for k, v in final_schema.items()}


    print("Reorder final containers data columns and convert each column to the final schema type")
    df_containers_with_column_order = df_containers[final_columns_order].fillna(value=final_schema_fillna_values).astype(final_schema_types)

    # Define aggregation functions for each column = priority rules in case of multiple values found (example multiple cDG => lowest class)
    print("Define aggregation functions for each potentially duplicate column")
    aggregation_functions = {
        'cDG': lambda x: x.min(),
        'overstowPort': lambda x: x.min(),
        'Stowage': lambda x: x.max(),
        'DGheated': lambda x: x.min(),
        # Add other columns and their aggregation functions as needed
    }

    print("Aggregate duplicated lines with aggregation functions for potentially duplicate columns or default aggregation function for other duplicated column")
    default_aggregation_function=lambda x: x.max()
    df_containers_final = preprocessing_utils.aggregate_duplicates(
        df_containers_with_column_order, 
        aggregation_functions, 
        default_aggregation_function,
    )

    print("Set output containers file path")
    output_containers_path = os.path.join(
        parent_dir,
        "output_data",
        f"simulation_{simulation}_{env}",
        f"containers_{simulation}_{input_type}.csv",
    )

    print("Write output containers data")
    df_containers_final.to_csv(output_containers_path, index=False, sep=";")

    return df_containers_final