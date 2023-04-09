import pandas as pd
from pandas.core.frame import DataFrame

COL_LEGAL_ENTITY = "legal_entity"
COL_COUNTER_PARTY = "counter_party"
COL_TIER = "tier"
COL_TIER_X = f"{COL_TIER}_x"
COL_TIER_Y = f"{COL_TIER}_y"
COL_VALUE = "value"
COL_VALUE_FOR_ACCR = "value_for_accr"
COL_VALUE_FOR_ARAP = "value_for_arap"
COL_RATING = "rating"
COL_STATUS = "status"
COL_MAX_RATING_BY_COUNTERPARTY = "max_rating_by_counterparty"


def print_divider():
    """
    Prints a divider. Useful for debug prints.
    :return:
    """
    print()
    print("*" * 80)
    print()


def set_df_debug(all_rows=False) -> None:
    """
    Sets up the dataframe printing options in Pandas. Useful for debugging.
    :param all_rows: if True, causes Pandas to print all the rows within a dataframe
    :return: None
    """
    pd.set_option("display.max_columns", None)
    pd.set_option("display.max_seq_items", None)
    pd.set_option("display.width", None)
    pd.set_option("display.max_colwidth", None)
    if all_rows:
        pd.set_option("display.max_rows", None)


def load_dataset(input_file_path_1: str, input_file_path_2: str) -> DataFrame:
    """
    Loads the two input datasets.
    :param input_file_path_1: the path to the CSV file containing the first dataset
    :param input_file_path_2: the path to the CSV file containing the second dataset
    :return: the resulting dataframe that is the first one joined to the second one on the counter party
    """
    df_1 = pd.read_csv(input_file_path_1).drop("invoice_id", axis=1)
    df_2 = pd.read_csv(input_file_path_2)

    # Join the two datasets on the counter_party
    df_merged = df_1.merge(df_2, on=COL_COUNTER_PARTY, how="left")

    return df_merged


def load_df(input_file_path: str) -> DataFrame:
    return pd.read_csv(input_file_path)

def validate(exp_results_file_path: str, actual_results_file_path: str) -> None:
    with open(exp_results_file_path, "r") as file:
        exp_data = file.read().rstrip()

    with open(actual_results_file_path, "r") as file:
        actual_data = file.read().rstrip()

    assert actual_data == exp_data, "Results not as expected"
    print(">> Validation: OK.")