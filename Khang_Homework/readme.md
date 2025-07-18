
        Which player averages the most kills per game?
        
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window
    dedup_df = df.select("match_id", "player_gamertag", "player_total_kills") \
        .dropDuplicates(["match_id", "player_gamertag"])
    agg_df = dedup_df.groupBy("player_gamertag").agg(
        F.sum("player_total_kills").alias("total_kills"),
        F.count("match_id").alias("games_played"))
    result_df = agg_df.withColumn(
        "avg_kills_per_game",
        F.col("total_kills") / F.col("games_played"))
    final_df = result_df.orderBy(F.col("avg_kills_per_game").desc())

        Which playlist gets played the most?

    from pyspark.sql import functions as F
    dedup_df = df.select("match_id", "playlist_id").dropDuplicates()
    result_df = dedup_df.groupBy("playlist_id").count()
    result_df = result_df.orderBy(F.col("count").desc())

        Which map gets played the most?

    from pyspark.sql import functions as F
    dedup = df.select("match_id", "mapid").dropDuplicates()
    mapid_counts = dedup.groupBy("mapid").count().orderBy(F.col("count").desc())
    mapid_counts.show()


        Which map do players get the most Killing Spree medals on?

    from pyspark.sql import functions as F
    filtered = df.filter(F.col("classification") == "KillingSpree")
    dedup = filtered.select("match_id", "mapid", "classification").dropDuplicates()
    result = dedup.groupBy("mapid").count().orderBy(F.col("count").desc())
    result.show()
