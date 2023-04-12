from pandas.core.frame import DataFrame

from hartree_common import (
    COL_LEGAL_ENTITY,
    COL_COUNTER_PARTY,
    COL_TIER,
    COL_TIER_X,
    COL_TIER_Y,
    COL_VALUE,
    COL_ACCR_VALUE_SUMS,
    COL_ARAP_VALUE_SUMS,
    COL_RATING,
    COL_STATUS,
    COL_MAX_RATING_BY_COUNTERPARTY,
    STATUS_ACCR,
    STATUS_ARAP,
    validate
)
from hartree_common import load_dataset

OUTPUT_COL_ORDER = [COL_LEGAL_ENTITY, COL_COUNTER_PARTY, COL_TIER, COL_MAX_RATING_BY_COUNTERPARTY, COL_ARAP_VALUE_SUMS,
                    COL_ACCR_VALUE_SUMS]

# TODO DG: these can be externalized e.g. passed in from the command line
INPUT_FILE_1_PATH = "input/dataset1.csv"
INPUT_FILE_2_PATH = "input/dataset2.csv"
OUTPUT_FILE_PATH = "pandas_results/part_1_result.csv"

EXPECTED_RESULTS_FILE_PATH = "expected/expected_part_1_result.csv"

USE_IMPL_2 = True


def perform_transformations(df_input: DataFrame) -> DataFrame:
    """
    Performs the necessary transformations of the two input datasets. The returned dataset contains the following
    columns:
    legal_entity
    counterparty
    tier
    max(rating by counterparty)
    sum(value where status=ARAP)
    sum(value where status=ACCR)
    :param df_input: the input dataset
    :return: the resulting dataframe after the joining and all the transformations
    """
    # For each { legal_entity, counter_party } pair, compute the respective maximum rating

    df_rating = df_input.copy()
    df_rating[COL_MAX_RATING_BY_COUNTERPARTY] = df_input.groupby([COL_LEGAL_ENTITY, COL_COUNTER_PARTY])[
        COL_RATING].transform("max")
    df_rating = df_rating[[COL_LEGAL_ENTITY, COL_COUNTER_PARTY, COL_TIER, COL_MAX_RATING_BY_COUNTERPARTY]]
    df_rating.drop_duplicates(inplace=True)

    #
    # TODO DG:
    # I have two versions of this: one is a bit heavy on groupby's, the other a bit heavy on joins.
    # There may be a way to streamline this using a better aggregation approach in Pandas.
    # Otherwise, I'd run a performance test over a larger dataset and pick the faster solution.
    #

    return do_transform_2(df_rating, df_input) if USE_IMPL_2 else do_transform(df_rating, df_input)


def do_transform_2(df_rating: DataFrame, df_merged_input: DataFrame) -> DataFrame:
    """
    Helper method to take care of raking in the max(rating by counterparty), the sum(value where status=ARAP), and the
    sum(value where status=ACCR). More group-by oriented.
    :param df_rating: the dataframe with max rating values
    :param df_merged_input: the two input datasets, merged
    :return: the resulting dataframe
    """
    df_merged_2 = compute_value_sums_2(df_merged_input)
    df_merged_3 = df_rating.merge(df_merged_2, on=[COL_LEGAL_ENTITY, COL_COUNTER_PARTY], how="outer").drop_duplicates()
    df_merged_3[COL_TIER] = df_merged_3[COL_TIER_X]
    df_merged_3.drop([COL_TIER_X, COL_TIER_Y], axis=1, inplace=True)

    return df_merged_3


def do_transform(df_rating: DataFrame, df_merged_input: DataFrame) -> DataFrame:
    """
    Helper method to take care of raking in the max(rating by counterparty), the sum(value where status=ARAP), and the
    sum(value where status=ACCR). More join-oriented.
    :param df_rating: the dataframe with max rating values
    :param df_merged_input: the two input datasets, merged
    :return: the resulting dataframe
    """
    # For each { legal_entity, counter_party } pair with status=ARAP, compute the respective sum of the values.
    df_arap = compute_value_sums(df_merged_input, STATUS_ARAP, COL_ARAP_VALUE_SUMS)

    # For each { legal_entity, counter_party } pair with status=ACCR, compute the respective sum of the values.
    df_accr = compute_value_sums(df_merged_input, STATUS_ACCR, COL_ACCR_VALUE_SUMS)

    # Merge the dataframe with the value sum aggregations
    df_merged_2 = df_arap.merge(df_accr, on=[COL_LEGAL_ENTITY, COL_COUNTER_PARTY], how="outer")
    df_merged_2.fillna(0, inplace=True)
    df_merged_2[COL_TIER] = df_merged_2[[COL_TIER_X, COL_TIER_Y]].max(axis=1)
    df_merged_2[COL_TIER] = df_merged_2[COL_TIER].astype("int32")
    df_merged_2[COL_ARAP_VALUE_SUMS] = df_merged_2[COL_ARAP_VALUE_SUMS].astype("int32")
    df_merged_2[COL_ACCR_VALUE_SUMS] = df_merged_2[COL_ACCR_VALUE_SUMS].astype("int32")
    df_merged_2 = df_merged_2[[COL_LEGAL_ENTITY, COL_COUNTER_PARTY, COL_TIER, COL_ARAP_VALUE_SUMS, COL_ACCR_VALUE_SUMS]]

    # Merge the max rating dataset with the value sum aggregations dataframe
    df_merged_3 = df_rating.merge(df_merged_2, on=[COL_LEGAL_ENTITY, COL_COUNTER_PARTY], how="outer")
    # Reconcile tier vs. tier_x/tier_y.
    df_merged_3[COL_TIER] = df_merged_3[COL_TIER_X]
    df_merged_3 = df_merged_3[
        [COL_LEGAL_ENTITY, COL_COUNTER_PARTY, COL_TIER, COL_MAX_RATING_BY_COUNTERPARTY, COL_ARAP_VALUE_SUMS,
         COL_ACCR_VALUE_SUMS]]

    # df_merged_3 = df_merged_3.sort_values([COL_LEGAL_ENTITY, COL_COUNTER_PARTY, COL_TIER]).reset_index(drop=True)

    return df_merged_3


def compute_value_sums_2(df: DataFrame) -> DataFrame:
    """
    Computes the respective value sums for the ACCR value sums vs. ARAP value sums.
    :param df: the input dataframe
    :return: the resulting dataframe
    """
    df = df.drop([COL_RATING], axis=1)

    df[COL_ACCR_VALUE_SUMS] = df[df.status == STATUS_ACCR].groupby([COL_LEGAL_ENTITY, COL_COUNTER_PARTY, COL_STATUS])[
        COL_VALUE].transform("sum")
    df[COL_ARAP_VALUE_SUMS] = df[df.status == STATUS_ARAP].groupby([COL_LEGAL_ENTITY, COL_COUNTER_PARTY, COL_STATUS])[
        COL_VALUE].transform("sum")
    df = df.drop([COL_VALUE, COL_STATUS], axis=1)
    df.fillna(0, inplace=True)
    df = df.drop_duplicates()

    df[COL_ACCR_VALUE_SUMS] = df.groupby([COL_LEGAL_ENTITY, COL_COUNTER_PARTY])[COL_ACCR_VALUE_SUMS].transform("sum")
    df[COL_ARAP_VALUE_SUMS] = df.groupby([COL_LEGAL_ENTITY, COL_COUNTER_PARTY])[COL_ARAP_VALUE_SUMS].transform("sum")
    df = df.drop_duplicates()
    df[COL_ACCR_VALUE_SUMS] = df[COL_ACCR_VALUE_SUMS].astype("int32")
    df[COL_ARAP_VALUE_SUMS] = df[COL_ARAP_VALUE_SUMS].astype("int32")

    return df


def compute_value_sums(df: DataFrame, status: str, new_col_name: str) -> DataFrame:
    """
    For each { legal_entity, counter_party } pair with the given status, computes the respective sum of the values.
    :param df_input: the input dataframe
    :return: the output dataframe with the computed value sum column added
    """
    df[new_col_name] = df[df.status == status].groupby([COL_LEGAL_ENTITY, COL_COUNTER_PARTY])[
        COL_VALUE].transform("sum")

    df = df[[COL_LEGAL_ENTITY, COL_COUNTER_PARTY, COL_TIER, new_col_name]]
    df = df.dropna()
    df = df.drop_duplicates()
    df[new_col_name] = df[new_col_name].astype("int32")

    return df


def persist_results(df: DataFrame) -> None:
    """
    Persists the computed resulting dataframe into an output CSV file.
    :param df: the input resulting dataframe
    :return: none
    """
    df = df[OUTPUT_COL_ORDER]
    df = df.sort_values([COL_LEGAL_ENTITY, COL_COUNTER_PARTY])
    df.to_csv(OUTPUT_FILE_PATH, index=False)


if __name__ == "__main__":
    """ This generates the output CSV file for the main requirement which is the output with the following columns:
    legal_entity,
    counterparty,
    tier,
    max(rating by counterparty),
    sum(value where status=ARAP),
    sum(value where status=ACCR)
    """
    df = load_dataset(INPUT_FILE_1_PATH, INPUT_FILE_2_PATH)
    print(">> Loaded the input dataset.")

    print(">> Performing the transformations...")
    df_result = perform_transformations(df)

    persist_results(df_result)
    print(">> Saved results to {}".format(OUTPUT_FILE_PATH))

    # TODO DG: convert to a unit test using unittest.TestCase
    validate(EXPECTED_RESULTS_FILE_PATH, OUTPUT_FILE_PATH)

    print(">> Done.")
