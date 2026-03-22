import pandas as pd


class DataFrameManipulator:
    def __init__(self):
        pass

    def join_two_dfs(self, df1, df2, join_columns, type_of_join='left'):
        possible_joins = ['left', 'right', 'outer', 'inner', 'cross']
        try:
            if type_of_join not in possible_joins:
                raise ValueError("Unsupported join type")

            df = pd.merge(
                df1,
                df2,
                on=join_columns,
                how=type_of_join,
            )

            return df

        except Exception as err:
            print(err)
            return None

    def change_column_names(self, dictionary_of_old_and_new_columns, df):
        try:
            df.rename(columns=dictionary_of_old_and_new_columns, inplace=True)
        except ValueError as err:
            print(err.args)
            return False
        return True

    def use_only_columns_needed(self, df, columns):
        try:
            return df[columns]
        except Exception as err:
            print(err)
            return None

    def remove_duplicates(self,df, columns=None):
        try:
            if columns is None:
                df.drop_duplicates(inplace=True)
            else:
                df.drop_duplicates(subset=columns, inplace=True)
            return True
        except Exception as err:
            print(err)
            return False

    def clean_text_columns(self,df, text_columns):
        try:
            for col in text_columns:
                if col in df.columns:
                    df[col] = df[col].apply(lambda x: x.strip() if isinstance(x, str) else x)
                    df[col] = df[col].replace({
                        '': pd.NA,
                        ' ': pd.NA,
                        'NA': pd.NA,
                        'NaN': pd.NA,
                        'nan': pd.NA,
                        'NULL': pd.NA,
                        'null': pd.NA,
                        None: pd.NA
                    })
        except Exception as err:
            print(err)
            return False
        return True


    def change_date_to_datetime(self,df, date_columns):
        try:
            for col in date_columns:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors='coerce')
        except Exception as err:
            print(err)
            return False
        return True

    def count_missing_values(self, df):
        missing_values = [
            9999.9, 999.9, 99.99,
            9999, 999, 99,
            '9999.9', '999.9', '99.99',
            '9999', '999', '99',
            '', ' ', 'NA', 'NaN', 'nan', 'NULL', 'null',
            None
        ]

        df_cleaned = df.replace(missing_values, pd.NA)

        missing_count = df_cleaned.isna().sum()

        missing_percent = round((missing_count / len(df_cleaned)) * 100, 2)

        result = pd.DataFrame({
            'column_name': df.columns,
            'missing_count': missing_count,
            'missing_percent': missing_percent
        })

        return result

    def clean_numeric_columns(self,df, numeric_columns):
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
            for col in numeric_columns:
                if col not in df.columns:
                    continue

                df[col] = df[col].replace(missing_values)
                df[col] = pd.to_numeric(df[col], errors='coerce')

            return True

        except Exception as err:
            print(err)
            return False

    def clear_data_frame(
        self,
        df,
        numeric_columns=None,
        text_columns=None,
        date_columns=None,
        duplicate_columns=None,
    ):
        """
        Czyści dataframe:
        - zamienia typowe placeholdery NOAA na pd.NA
        - konwertuje wybrane kolumny na typ numeryczny
        - czyści spacje w stringach
        - zmienia date na datetime
        - usuwa duplikaty
        """

        try:
            if text_columns is not None:
                if not self.clean_text_columns(df, text_columns):
                    return False

            if numeric_columns is not None:
                if not self.clean_numeric_columns(df, numeric_columns):
                    return False

            if date_columns is not None:
                if not self.change_date_to_datetime(df, date_columns):
                    return False

            if duplicate_columns is not None:
                if not self.remove_duplicates(df, duplicate_columns):
                    return False

            return True

        except Exception as err:
            print(err)
            return False