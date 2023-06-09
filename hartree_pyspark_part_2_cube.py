import os
from typing import List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    max as smax,
)

from hartree_common import (
    COL_LEGAL_ENTITY,
    COL_COUNTER_PARTY,
    COL_TIER,
    COL_ACCR_VALUE_SUMS,
    COL_ARAP_VALUE_SUMS,
    COL_MAX_RATING_BY_COUNTERPARTY,
    find_first_file_with_ext,
    remove_files_in_dir,
    rename_file,
    validate
)

INPUT_FILE_PATH = "pyspark_results_main/part_1_result.csv"

# The tier is not relevant because it is set to the given { legal_entity, counter_party } pair.
COLS_TO_CUBE = [
    COL_LEGAL_ENTITY,
    COL_COUNTER_PARTY,
    COL_TIER,
    COL_MAX_RATING_BY_COUNTERPARTY,
    COL_ARAP_VALUE_SUMS,
    COL_ACCR_VALUE_SUMS,
]

OUTPUT_DIR_PATH = "pyspark_results_cube"
OUTPUT_FNAME = "part_1_result_cube_pyspark.csv"

EXPECTED_RESULTS_FILE_PATH = "expected/expected_part_2_result_cube.csv"


def load_input_dataset(spark: SparkSession) -> DataFrame:
    """
    Loads the input CSV file (which is the output of hartree_pyspark_part_1_main).
    :param spark: the spark session
    :return: the loaded dataframe
    """
    # Can define schemas explicitly and pass them in via .schema(schema) instead of inferring
    return spark.read.csv(INPUT_FILE_PATH, header='true', inferSchema=True)


def generate_cube(df: DataFrame, cols: List[str]) -> DataFrame:
    df_cube = df.cube(cols).count().drop("count")

    df_cube = df_cube.filter(col(COL_TIER).isNotNull())

    df_cube = df_cube.fillna(0, subset=[COL_MAX_RATING_BY_COUNTERPARTY, COL_ARAP_VALUE_SUMS, COL_ACCR_VALUE_SUMS])

    df_cube = df_cube.drop_duplicates()

    df_cube = (
        df_cube
            .groupby([COL_LEGAL_ENTITY, COL_COUNTER_PARTY, COL_TIER])
            .agg(
            smax(COL_MAX_RATING_BY_COUNTERPARTY).alias(COL_MAX_RATING_BY_COUNTERPARTY),
            smax(COL_ARAP_VALUE_SUMS).alias(COL_ARAP_VALUE_SUMS),
            smax(COL_ACCR_VALUE_SUMS).alias(COL_ACCR_VALUE_SUMS),
        )
    )

    df_cube = df_cube.fillna("Total", subset=[COL_LEGAL_ENTITY, COL_COUNTER_PARTY])

    return df_cube


def persist_results(df_result: DataFrame) -> None:
    """
    Persists the resulting DataFrame to a CSV file.
    :param df_result: the resulting dataframe
    :return: none
    """
    # Coalesce the results into a single CSV file.
    # Include the header row.
    # Overwrite if the previously generated output exists.
    df_result.coalesce(1).orderBy([COL_LEGAL_ENTITY, COL_COUNTER_PARTY, COL_TIER]).write.option("header", True).mode(
        "overwrite").csv(OUTPUT_DIR_PATH)

    # Delete any .crc files generated alongside the CSV
    remove_files_in_dir(OUTPUT_DIR_PATH, ".crc")

    # Rename the output file to the specific name
    output_fname = find_first_file_with_ext(OUTPUT_DIR_PATH, ".csv")
    rename_file(OUTPUT_DIR_PATH, output_fname, OUTPUT_FNAME)


def main() -> None:
    spark = SparkSession.builder.appName("hartree_challenge").getOrCreate()

    # When writing csv files, avoid generating the SUCCESS file
    spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")

    print("\n>> Running...\n")

    df_main = load_input_dataset(spark)
    df_cube = generate_cube(df_main, COLS_TO_CUBE)

    persist_results(df_cube)

    # TODO DG: convert to a unit test using unittest.TestCase
    #
    # TODO DG: the cube generated by Pyspark differs from the one generated by Pandas on two rows:
    #
    # pandas: Total,C3,3,12,5,197
    # pyspark: Total,C3,3,6,5,145
    # and
    # pandas: Total,Total,3,12,5,197
    # pyspark: Total,Total,3,6,5,145
    #
    # Need to debug into this deeper.
    #
    output_fpath = os.path.join(OUTPUT_DIR_PATH, OUTPUT_FNAME)
    validate(EXPECTED_RESULTS_FILE_PATH, output_fpath)

    print("\n>> Done.\n")

    spark.stop()


if __name__ == "__main__":
    main()
