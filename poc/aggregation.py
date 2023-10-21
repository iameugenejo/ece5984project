import pandas


def get_field_value_count(df: pandas.DataFrame, field: str) -> pandas.DataFrame:
    return df.groupby(by=[field])[field].count().sort_values()


def get_field_unique_count(df: pandas.DataFrame, field: str) -> int:
    return df[field].unique().size
