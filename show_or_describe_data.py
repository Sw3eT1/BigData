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

    @staticmethod
    def use_only_columns_needed(df, columns):
        try:
            return df[columns]
        except Exception as err:
            print(err)
        return None

    @staticmethod
    def clear_data_frame(df, numeric_columns=None):
        """
        Czyści dataframe:
        - zamienia typowe placeholdery NOAA na pd.NA
        - czyści spacje w stringach
        - konwertuje wybrane kolumny na typ numeryczny
        """

        # typowe placeholdery braków danych
        missing_values = {
            9999.9: pd.NA,
            999.9: pd.NA,
            99.99: pd.NA,
            9999: pd.NA,
            999: pd.NA,
            99: pd.NA,
            '9999.9': pd.NA,
            '999.9': pd.NA,
            '99.99': pd.NA,
            '9999': pd.NA,
            '999': pd.NA,
            '99': pd.NA,
            '': pd.NA,
            ' ': pd.NA,
            'NA': pd.NA,
            'NaN': pd.NA,
            'nan': pd.NA,
            'NULL': pd.NA,
            'null': pd.NA,
            None: pd.NA
        }

        try:
            # jeśli nie podano kolumn, próbujemy czyścić wszystkie
            if numeric_columns is None:
                numeric_columns = df.columns.tolist()

            for col in numeric_columns:
                if col not in df.columns:
                    continue

                # jeśli kolumna jest tekstowa, usuwamy spacje
                if df[col].dtype == 'object':
                    df[col] = df[col].astype(str).str.strip()

                # zamiana placeholderów na NA
                df[col] = df[col].replace(missing_values)

                # próba konwersji na numeric
                df[col] = pd.to_numeric(df[col], errors='coerce')

            return True

        except Exception as err:
            print(err)
            return False