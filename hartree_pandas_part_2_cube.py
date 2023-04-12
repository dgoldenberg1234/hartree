from itertools import combinations
from typing import List

import pandas as pd
from pandas.core.frame import DataFrame

from hartree_common import (
    COL_LEGAL_ENTITY,
    COL_COUNTER_PARTY,
    COL_TIER,
    COL_VALUE,
    COL_ACCR_VALUE_SUMS,
    COL_ARAP_VALUE_SUMS,
    COL_RATING,
    COL_STATUS,
    COL_MAX_RATING_BY_COUNTERPARTY,
    validate
)
from hartree_common import load_df, set_df_debug

INPUT_FILE_PATH = "pandas_output/part_1_result.csv"
OUTPUT_FILE_PATH = "pandas_output/part_2_result_cube.csv"
EXPECTED_RESULTS_FILE_PATH = "expected/expected_part_2_result_cube.csv"

COLS_TO_CUBE = [
    COL_LEGAL_ENTITY,
    COL_COUNTER_PARTY,
    COL_TIER
]

MIN_TIER_VAL = 1
MAX_TIER_VAL = 6


def cube_sum(df_in: DataFrame, cols: List[str]) -> DataFrame:
    """ Computes a cube for the specified columns. See
    https://stackoverflow.com/questions/70956074/does-python-have-a-similar-function-to-cube-function-in-sql
    :param df_in: the data frame
    :param cols: the columns
    :return: the resulting dataframe
    """
    dfs = []
    for n in range(len(cols), 0, -1):
        for subset in combinations(cols, n):
            dfs.append(df_in.groupby(list(subset)).sum().astype(int).reset_index())
    dfs.append(df_in.drop(cols, axis=1).sum().to_frame().T)
    return pd.concat(dfs)


def persist_results(df_in: DataFrame) -> None:
    """
    Persists the computed resulting dataframe into an output CSV file.
    :param df_in: the input resulting dataframe
    :return: none
    """
    df_in = df_in.sort_values([COL_LEGAL_ENTITY, COL_COUNTER_PARTY, COL_TIER])
    df_in.to_csv(OUTPUT_FILE_PATH, index=False)


if __name__ == "__main__":
    """ This generates the output CSV file which contains the 'cube' for legal_entity/counter_party/tier.
    """
    set_df_debug()

    df = load_df(INPUT_FILE_PATH)

    print(">> Loaded the input dataset.")

    df_res = cube_sum(df, COLS_TO_CUBE)

    df_res[COL_LEGAL_ENTITY].fillna(value="Total", inplace=True)
    df_res[COL_COUNTER_PARTY].fillna(value="Total", inplace=True)

    # Don't need rows with null or invalid tier value.
    # min/max tier can be computed dynamically; could also check for specific values
    # TODO figure out how to avoid these rows from getting generated in the first place
    df_res = df_res[
        (df_res[COL_TIER] >= MIN_TIER_VAL) & (df_res[COL_TIER] <= MAX_TIER_VAL)
        ]

    df_res.reset_index(inplace=True, drop=True)

    # tier values come out as float, so convert to int
    df_res[COL_TIER] = df_res[COL_TIER].apply(lambda val: val if val == "Total" else int(val))

    df_res = df_res.drop_duplicates()

    persist_results(df_res)
    print(">> Saved results to {}".format(OUTPUT_FILE_PATH))

    # TODO DG: convert to a unit test using unittest.TestCase
    validate(EXPECTED_RESULTS_FILE_PATH, OUTPUT_FILE_PATH)

    print(">> Done.")
