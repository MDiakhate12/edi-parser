import numpy as np

LENGTH_UNIT_CODES = {
    "MMT": 1 / 1000,
    "CMT":  1 / 100,
    "MT": 1,
    "FT": 0.3048,
    "IN": 0.0254,
    "": 0,
    "nan": 0,
    np.nan: 0,
}

LENGTH_UNIT_CODES_CM = {
    "MMT": 1 / 1000,
    "CMT":  1,
    "MT": 100,
    "FT": 30.48,
    "IN": 2.54,
    "": 0,
    "nan": 0,
    np.nan: 0,
}

WEIGHT_UNIT_CODES_TNE = {
    "GRM": 1 / 1_000_000,
    "KGM": 1 / 1000,
    "TNE": 1,
    "": 0,
    "nan": 0,
    np.nan: 0,
}