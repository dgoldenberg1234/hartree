from itertools import combinations
from typing import List

import pandas as pd
from pandas.core.frame import DataFrame

from hartree_common import (
    COL_LEGAL_ENTITY,
    COL_COUNTER_PARTY,
    validate
)
from hartree_common import load_df, set_df_debug

INPUT_FILE_PATH = "output/part_1_result.csv"
OUTPUT_FILE_PATH = "output/part_2_result_cube.csv"
EXPECTED_RESULTS_FILE_PATH = "expected/expected_part_2_result_cube.csv"

COLS_TO_CUBE = [
    COL_LEGAL_ENTITY,
    COL_COUNTER_PARTY,
]


def func(x):
    try:
        return int(x)
    except:
        return x


def cube_sum(df: DataFrame, cols: List[str]) -> DataFrame:
    """ Computes a cube for the specified columns.
    :param df: the data frame
    :param cols: the columns
    :return: the resulting dataframe
    """
    dfs = []
    for n in range(len(cols), 0, -1):
        for subset in combinations(cols, n):
            dfs.append(df.groupby(list(subset)).sum().astype(int).reset_index())
    dfs.append(df.drop(cols, axis=1).sum().to_frame().T)
    return pd.concat(dfs)


def persist_results(df_in: DataFrame) -> None:
    """
    Persists the computed resulting dataframe into an output CSV file.
    :param df_in: the input resulting dataframe
    :return: none
    """
    df_in = df_in.sort_values([COL_LEGAL_ENTITY, COL_COUNTER_PARTY])
    df_in.to_csv(OUTPUT_FILE_PATH, index=False)


if __name__ == "__main__":
    set_df_debug()

    df = load_df(INPUT_FILE_PATH)

    print(">> Loaded the input dataset.")

    df_res = cube_sum(df, COLS_TO_CUBE)
    df_res.fillna("Total", inplace=True)
    df_res.reset_index(inplace=True, drop=True)

    persist_results(df_res)
    print(">> Saved results to {}".format(OUTPUT_FILE_PATH))

    # TODO DG: convert to a unit test using unittest.TestCase
    validate(EXPECTED_RESULTS_FILE_PATH, EXPECTED_RESULTS_FILE_PATH)

    print(">> Done.")
