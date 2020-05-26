def changing_bool_dtypes_to_str(df):
    bool_cols = df.select_dtypes(include = ['bool']).columns
    df[bool_cols] = df[bool_cols].astype(str)
    return df
    