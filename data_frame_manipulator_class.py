import pandas as pd


class DataFrameManipulator:
    def __init__(self):
        pass

    def clear_data_frame(self, df, numeric_cols=None, text_cols=None, date_cols=None, drop_cols=None):
        if drop_cols:
            df.drop(columns=[col for col in drop_cols if col in df.columns], inplace=True, errors='ignore')

        if numeric_cols:
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')

        if text_cols:
            for col in text_cols:
                if col in df.columns:
                    df[col] = df[col].astype(str)

        if date_cols:
            for col in date_cols:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors='coerce')

    def change_column_names(self, rename_dict, df):
        df.rename(columns=rename_dict, inplace=True)

    def join_two_dfs(self, df1, df2, on_columns, how_type='inner'):
        return pd.merge(df1, df2, on=on_columns, how=how_type)

    def count_missing_values(self, df):
        missing = df.isnull().sum()
        missing_df = missing[missing > 0].reset_index()
        missing_df.columns = ['Column', 'Missing_Count']
        return missing_df