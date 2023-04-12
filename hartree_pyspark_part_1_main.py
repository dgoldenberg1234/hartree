from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    lit,
    max as smax,
    sum as ssum,
)

from hartree_common import (
    COL_INVOICE_ID,
    COL_LEGAL_ENTITY,
    COL_COUNTER_PARTY,
    COL_TIER,
    COL_VALUE,
    COL_ACCR_VALUE_SUMS,
    COL_ARAP_VALUE_SUMS,
    COL_RATING,
    COL_STATUS,
    COL_MAX_RATING_BY_COUNTERPARTY,
    STATUS_ACCR,
    STATUS_ARAP,
    find_first_file_with_ext,
    remove_files_in_dir,
    rename_file
)

INPUT_FILE_1_PATH = "input/dataset1.csv"
INPUT_FILE_2_PATH = "input/dataset2.csv"
OUTPUT_DIR_PATH = "pyspark_output"
OUTPUT_FNAME = "part_1_result.csv"

EXPECTED_RESULTS_FILE_PATH = "expected/expected_part_1_result.csv"


def load_main_dataset(spark: SparkSession) -> DataFrame:
    """
    Loads the two input CSV files and joins them into a single dataframe.
    :param spark: the spark session
    :return: the joined dataframe
    """
    # Can define schemas explicitly and pass them in via .schema(schema) instead of inferring
    df_1 = spark.read.csv(INPUT_FILE_1_PATH, header='true', inferSchema=True)
    df_2 = spark.read.csv(INPUT_FILE_2_PATH, header='true', inferSchema=True)

    df_main = (
        df_1
            .join(df_2, df_1[COL_COUNTER_PARTY] == df_2[COL_COUNTER_PARTY], how="inner")
            .drop(df_1[COL_INVOICE_ID])
            .drop(df_1[COL_COUNTER_PARTY])
            .select(COL_LEGAL_ENTITY, COL_COUNTER_PARTY, COL_RATING, COL_STATUS, COL_VALUE, COL_TIER)
    )
    return df_main


def compute_max_rating_by_counterparty(df_keys: DataFrame, df_main) -> DataFrame:
    """
    Computes the maximum rating by { legal entity, counterparty }.
    :param df_keys: the dataframe which contains all the 'keys' where each key is a tuple of
    { legal entity, counterparty, tier } where the tier doesn't vary and is tied to the entity/counterparty
    :param df_main: the main loaded input dataset
    :return: the dataframe with the 'keys' and the computed max rating by counterparty column
    """
    df_rating = (
        df_main
            .groupBy([COL_LEGAL_ENTITY, COL_COUNTER_PARTY, COL_TIER])
            .agg(smax(COL_RATING).alias(COL_MAX_RATING_BY_COUNTERPARTY))
            .withColumnRenamed(COL_LEGAL_ENTITY, f"rtg_{COL_LEGAL_ENTITY}")
            .withColumnRenamed(COL_COUNTER_PARTY, f"rtg_{COL_COUNTER_PARTY}")
            .withColumnRenamed(COL_TIER, f"rtg_{COL_TIER}")
            .orderBy([COL_LEGAL_ENTITY, COL_COUNTER_PARTY, COL_TIER])
    )

    df_result = (
        df_keys
            .join(df_rating, (df_keys[COL_LEGAL_ENTITY] == df_rating[f"rtg_{COL_LEGAL_ENTITY}"]) & (
                df_keys[COL_COUNTER_PARTY] == df_rating[f"rtg_{COL_COUNTER_PARTY}"]), how="inner")
            .select(COL_LEGAL_ENTITY, COL_COUNTER_PARTY, COL_TIER, COL_MAX_RATING_BY_COUNTERPARTY)
    )
    return df_result


def compute_accr_value_sums(df_main: DataFrame, df_result: DataFrame) -> DataFrame:
    """
    Computes the value sums for the rows in the input dataframe where status value=ACCR.
    :param df_main: the main loaded input dataframe
    :param df_result: the result of the overall transformation, to be updated in this method
    :return: the updated result with the value sums in the added ACCR value sums column
    """
    # Compute the value sums
    df_accr_sums = (
        df_main
            .filter(col(COL_STATUS) == lit(STATUS_ACCR))
            .groupBy([COL_LEGAL_ENTITY, COL_COUNTER_PARTY, COL_TIER])
            .agg(ssum(COL_VALUE).alias(COL_ACCR_VALUE_SUMS))
            .withColumnRenamed(COL_LEGAL_ENTITY, f"accr_{COL_LEGAL_ENTITY}")
            .withColumnRenamed(COL_COUNTER_PARTY, f"accr_{COL_COUNTER_PARTY}")
            .withColumnRenamed(COL_TIER, f"accr_{COL_TIER}")
    )

    # Add the value sums as a new column to the result. Fill the N/A's in the ACCR value sums column with 0.
    # (The rows with status value of ARAP will have 0).
    df_result = (
        df_result
            .join(df_accr_sums, (df_result[COL_LEGAL_ENTITY] == df_accr_sums[f"accr_{COL_LEGAL_ENTITY}"]) & (
                df_result[COL_COUNTER_PARTY] == df_accr_sums[f"accr_{COL_COUNTER_PARTY}"]), how="left_outer")
            .fillna(0, COL_ACCR_VALUE_SUMS)
            .select(COL_LEGAL_ENTITY, COL_COUNTER_PARTY, COL_TIER, COL_MAX_RATING_BY_COUNTERPARTY, COL_ACCR_VALUE_SUMS)
    )
    return df_result


def compute_arap_value_sums(df_main, df_result):
    """
    Computes the value sums for the rows in the input dataframe where status value=ARAP.
    :param df_main: the main loaded input dataframe
    :param df_result: the result of the overall transformation, to be updated in this method
    :return: the updated result with the value sums in the added ARAP value sums column
    """
    # Compute the value sums
    df_arap_sums = (
        df_main
            .filter(col(COL_STATUS) == lit(STATUS_ARAP))
            .groupBy([COL_LEGAL_ENTITY, COL_COUNTER_PARTY])
            .agg(ssum(COL_VALUE).alias(COL_ARAP_VALUE_SUMS))
            .withColumnRenamed(COL_LEGAL_ENTITY, f"arap_{COL_LEGAL_ENTITY}")
            .withColumnRenamed(COL_COUNTER_PARTY, f"arap_{COL_COUNTER_PARTY}")
            .withColumnRenamed(COL_TIER, f"arap_{COL_TIER}")
    )

    # Add the value sums as a new column to the result. Fill the N/A's in the ARAP value sums column with 0.
    # (The rows with status value of ACCR will have 0).
    df_result = (
        df_result
            .join(df_arap_sums, (df_result[COL_LEGAL_ENTITY] == df_arap_sums[f"arap_{COL_LEGAL_ENTITY}"]) & (
                df_result[COL_COUNTER_PARTY] == df_arap_sums[f"arap_{COL_COUNTER_PARTY}"]), how="left_outer")
            .fillna(0, COL_ARAP_VALUE_SUMS)
            .select(COL_LEGAL_ENTITY, COL_COUNTER_PARTY, COL_TIER, COL_MAX_RATING_BY_COUNTERPARTY, COL_ARAP_VALUE_SUMS,
                    COL_ACCR_VALUE_SUMS)
    )
    return df_result


def persist_results(df_result: DataFrame) -> None:
    """
    Persists the resulting DataFrame to a CSV file.
    :param df_result: the resulting dataframe
    :return: none
    """
    # Coalesce the results into a single CSV file.
    # Include the header row.
    # Overwrite if the previously generated output exists.
    df_result.coalesce(1).orderBy([COL_LEGAL_ENTITY, COL_COUNTER_PARTY]).write.option("header", True).mode(
        "overwrite").csv(OUTPUT_DIR_PATH)

    # Delete any .crc files generated alongside the CSV
    remove_files_in_dir(OUTPUT_DIR_PATH, ".crc")

    # Rename the output file to the specific name
    output_fname = find_first_file_with_ext(OUTPUT_DIR_PATH, ".csv")
    rename_file(OUTPUT_DIR_PATH, output_fname, OUTPUT_FNAME)


def main() -> None:
    """
    This generates the output CSV file for the main requirement which is the output with the following columns:

    legal_entity,
    counterparty,
    tier,
    max(rating by counterparty),
    sum(value where status=ARAP),
    sum(value where status=ACCR)

    using PySpark.

    :return: None
    """
    spark = SparkSession.builder.appName("hartree_challenge").getOrCreate()

    # When writing csv files, avoid generating the SUCCESS file
    spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")

    print("\n>> Running...\n")

    df_main = load_main_dataset(spark)

    df_keys = (
        df_main
            .select(COL_LEGAL_ENTITY, COL_COUNTER_PARTY, COL_TIER)
            .drop_duplicates()
    )

    df_result = compute_max_rating_by_counterparty(df_keys, df_main)

    df_result = compute_accr_value_sums(df_main, df_result)

    df_result = compute_arap_value_sums(df_main, df_result)

    persist_results(df_result)

    print("\n>> Done.\n")

    spark.stop()


if __name__ == "__main__":
    main()
