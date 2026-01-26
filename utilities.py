import re

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