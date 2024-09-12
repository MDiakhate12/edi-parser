import pandas as pd
import numpy as np

from utils import pandas_utils


class common_helpers:
  def is_empty(series):
    return series.apply(lambda row: not pandas_utils.is_not_null(row))


def add_pol_pod_nb(df:pd.DataFrame, df_rotation: pd.DataFrame) -> pd.DataFrame:
    """
    ** Disclaimer : This function has been reproduced as-is from the previous preprocessing code ! **
    ** Disclaimer : I disclaim all responsibility in the event of a strange function behavior ! (Mouhammad) **

    Computes a linearized version of the sequence number found in the input rotation.csv file
    In the rotation.csv, the sequence number only indicates the order of the next ports.
    We need to be able to identify the ports of already loaded/discharged containers (negative POL_nb or POD_nb),
    and differentiate them from containers that will be loaded/discharged in future stops (positive POL_nb or POD_nb).
    """

    df_rotation = df_rotation[["ShortName", "Sequence"]].copy()
    df_rotation["Sequence"] = df_rotation["Sequence"].astype(int)

    load_ports = df["LoadPort"].fillna("")
    disch_port = df["DischPort"].fillna("")

    df["POLOrig"] = [ port if port in df_rotation["ShortName"] else port[:5] for port in load_ports ]
    df["PODOrig"] = [ port if port in df_rotation["ShortName"] else port[:5] for port in disch_port ]

    df = pd.merge(df, df_rotation, how='left', left_on=["POLOrig"], right_on=["ShortName"])
    df.rename(columns={"Sequence": "POL_nb"}, inplace=True)
    df.drop(columns=["ShortName"], inplace=True)

    df = pd.merge(df, df_rotation, how='left', left_on=["PODOrig"], right_on=["ShortName"])
    df.rename(columns={"Sequence": "POD_nb"}, inplace=True)
    df.drop(columns=["ShortName"], inplace=True)

    nbPorts = len(set(df_rotation["ShortName"]))

    df["POL_nb"] = np.where(common_helpers.is_empty(df["POL_nb"]) & ~common_helpers.is_empty(df["Slot"]), -nbPorts, df["POL_nb"])
    df["POL_nb"] = np.where(common_helpers.is_empty(df["POL_nb"]) & common_helpers.is_empty(df["Slot"]), 2*nbPorts, df["POL_nb"])
    df["POD_nb"] = np.where(common_helpers.is_empty(df["POD_nb"]), 2*nbPorts, df["POD_nb"])

    df["POL_nb"] = np.where(common_helpers.is_empty(df["Slot"]), df["POL_nb"], df["POL_nb"] - nbPorts).astype(int)
    df["POD_nb"] = np.where(df["POL_nb"] > df["POD_nb"], df["POL_nb"] + nbPorts, df["POD_nb"]).astype(int)

    df.drop(columns=["POLOrig", "PODOrig"], inplace=True)

    return df