import re

import pandas as pd

def format_string(string, flag):
    string = string.lower()
    string = re.sub(r"[\s_-]+", " ", string).strip()
    parts = string.split()
    if flag == "camel":
        return parts[0] + ''.join(word.capitalize() for word in parts[1:])
    elif flag == "pascal":
        return ''.join(word.capitalize() for word in parts)
    
def wide_to_long_df(wide_df, id_col, var_name, value_name):
    long_df = wide_df.melt(
        id_vars=[id_col],
        value_vars=[col for col in wide_df.columns if col != id_col],
        var_name=var_name,
        value_name=value_name
    )
    long_df = long_df.dropna(subset=[value_name])
    return long_df

def get_column_type(series):
    # convert "yes"/"no" to boolean
    if series.dtype == object:
        s_lower = series.str.lower()
        if set(s_lower.dropna()).issubset({"yes", "no"}):
            series = s_lower.map({"yes": True, "no": False}).astype("boolean")
    # determine type and determine new header accordingly
    if pd.api.types.is_bool_dtype(series):
        typ = ':bool'
    elif pd.api.types.is_integer_dtype(series):
        typ = ':int'
    elif pd.api.types.is_float_dtype(series):
        if (series.dropna() % 1 == 0).all():
            series = series.astype("Int64")
            typ = ':int'
        else:
            typ = ':float'
    else: 
        typ = ':string'
    return series, typ