from chispa.dataframe_comparer import *
from ..jobs.Khang_player_scd import do_player_scd_transformation
from collections import namedtuple
PlayerSeason = namedtuple("PlayerSeason", "player_name current_season scoring_class")
PlayerScd = namedtuple("PlayerScd", "player_name scoring_class start_date end_date")


def test_scd_generation(spark):
    source_data = [
        PlayerSeason("Kobe Bryant", 1996, 'Bad'),
        PlayerSeason("Kobe Bryant", 1997, 'Good'),
        PlayerSeason("Kobe Bryant", 1998, 'Good'),
        PlayerSeason("Kobe Bryant", 1999, 'Star'),
        PlayerSeason("Kobe Bryant", 2000, 'Star'),
        PlayerSeason("Kobe Bryant", 2001, 'Star')
    ]
    source_df = spark.createDataFrame(source_data)

    actual_df = do_player_scd_transformation(spark, source_df)
    expected_data = [
        PlayerScd("Kobe Bryant", 'Bad', 1996, 1996),
        PlayerScd("Kobe Bryant", 'Good', 1997, 1998),
        PlayerScd("Kobe Bryant", 'Star', 1999, 2001)
    ]
    expected_df = spark.createDataFrame(expected_data)
    assert_df_equality(actual_df, expected_df)