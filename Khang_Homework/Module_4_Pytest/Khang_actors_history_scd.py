from pyspark.sql import SparkSession
query = """

WITH streak_started AS(
	SELECT actor,
			current_year,
			quality_class,
			LAG(quality_class,1) OVER(PARTITION BY actor ORDER BY current_year) <> quality_class
			OR LAG(quality_class,1) OVER(PARTITION BY actor ORDER BY current_year) IS NULL AS did_change
	FROM actors),

	streak_identified AS(
	SELECT actor,
			quality_class,
			current_year,
			SUM(CASE WHEN did_change THEN 1 ELSE 0 END)
                OVER (PARTITION BY actor ORDER BY current_year) as streak_combine
         FROM streak_started),

	aggregated AS(
	SELECT actor,
			quality_class,
			streak_combine,
			MIN(current_year) AS start_year,
			MAX(current_year) AS end_year
	FROM streak_identified
	GROUP BY 1,2,3
	ORDER BY start_year)

SELECT actor, quality_class, start_year, end_year
FROM aggregated
"""

def do_actor_scd_transformation(spark, dataframe):
    dataframe.createOrReplaceTempView("actors")
    return spark.sql(query)

def main():
    spark = SparkSession.builder \
      .master("local") \
      .appName("actors_scd") \
      .getOrCreate()
    output_df = do_actor_scd_transformation(spark, spark.table("actors"))
    output_df.write.mode("overwrite").insertInto("actors_scd")

