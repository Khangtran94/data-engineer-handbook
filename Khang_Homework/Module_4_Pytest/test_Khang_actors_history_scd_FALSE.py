from chispa.dataframe_comparer import *
from ..jobs.Khang_actors_history_scd import do_actor_scd_transformation
from collections import namedtuple
ActorStat = namedtuple("ActorStat", "actor current_year quality_class")
ActorScd = namedtuple("ActorScd", "actor quality_class start_year end_year")


def test_scd_generation(spark):
    source_data = [
        ActorStat("Leonardo DiCaprio", 1991, "Bad"),
        ActorStat("Leonardo DiCaprio", 1992, "Bad"),
        ActorStat("Leonardo DiCaprio", 1993, "Good"),
        ActorStat("Leonardo DiCaprio", 1994, "Bad"),
        ActorStat("Leonardo DiCaprio", 1995, "Average"),
        ActorStat("Leonardo DiCaprio", 1996, "Average"),
        ActorStat("Leonardo DiCaprio", 1997, "Good")
    ]
    source_df = spark.createDataFrame(source_data)

    actual_df = do_actor_scd_transformation(spark, source_df)
    expected_data = [
        ActorScd("Leonardo DiCaprio", 'Bad', 1991, 1992),
        ActorScd("Leonardo DiCaprio", 'Good', 1993, 1993),
        ActorScd("Leonardo DiCaprio", 'Superstar', 1994, 1994),
        ActorScd("Leonardo DiCaprio", 'Average', 1994, 1996),
        ActorScd("Leonardo DiCaprio", 'Good', 1997, 1997)
    ]
    expected_df = spark.createDataFrame(expected_data)
    assert_df_equality(actual_df, expected_df)