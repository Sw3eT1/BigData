import pandas as pd

class DataFrameManipulator:
    def __init__(self):
        pass

    @staticmethod
    def join_two_dfs(df1, df2, join_column, type_of_join='left'):
        possible_joins = ['left', 'right', 'outer', 'inner', 'cross']
        try:
            if type_of_join not in possible_joins:
                raise ValueError("Unsupported join type")

            df = pd.merge(
                df1,
                df2,
                on=join_column,
                how=type_of_join,
            )

            return df

        except Exception as err:
            print(err)
            return None

    @staticmethod
    def change_column_names(dictionary_of_old_and_new_columns, df):

        try:
            df.rename(columns=dictionary_of_old_and_new_columns, inplace=True)
        except ValueError as err:
            print(err.args)
            return False
        return True

    @staticmethod
    def use_only_columns_needed(df, columns):
        try:
            return df[columns]
        except Exception as err:
            print(err)
        return None