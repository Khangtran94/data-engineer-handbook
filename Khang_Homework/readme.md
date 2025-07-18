df1 = medal_match_player.merge(match_detail, on = ['match_id','player_gamertag'])
df = df1.merge(medal, on = 'medal_id')\
    .merge(matches,on = 'match_id')\
        .merge(map, on = 'mapid')

        Which player averages the most kills per game?
        
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Step 1: Select and deduplicate
dedup_df = df.select("match_id", "player_gamertag", "player_total_kills") \
    .dropDuplicates(["match_id", "player_gamertag"])

# Step 2: Group by player and aggregate
agg_df = dedup_df.groupBy("player_gamertag").agg(
    F.sum("player_total_kills").alias("total_kills"),
    F.count("match_id").alias("games_played")
)

# Step 3: Add avg_kills_per_game column
result_df = agg_df.withColumn(
    "avg_kills_per_game",
    F.col("total_kills") / F.col("games_played")
)

# Step 4: Sort descending by avg_kills_per_game
final_df = result_df.orderBy(F.col("avg_kills_per_game").desc())

        Which playlist gets played the most?

from pyspark.sql import functions as F

# Step 1: Drop duplicates based on ['match_id', 'playlist_id']
dedup_df = df.select("match_id", "playlist_id").dropDuplicates()

# Step 2: Group by playlist_id and count
result_df = dedup_df.groupBy("playlist_id").count()

# Step 3: Sort descending (like value_counts(ascending=False))
result_df = result_df.orderBy(F.col("count").desc())

        Which map gets played the most?

from pyspark.sql import functions as F

# Step 1: Drop duplicate match_id + mapid combinations
dedup = df.select("match_id", "mapid").dropDuplicates()

# Step 2: Count how many times each mapid appears
mapid_counts = dedup.groupBy("mapid").count().orderBy(F.col("count").desc())

# To show result
mapid_counts.show()


        Which map do players get the most Killing Spree medals on?

from pyspark.sql import functions as F

# Step 1: Filter rows where classification == 'KillingSpree'
filtered = df.filter(F.col("classification") == "KillingSpree")

# Step 2: Select relevant columns and drop duplicates
dedup = filtered.select("match_id", "mapid", "classification").dropDuplicates()

# Step 3: Count mapid occurrences
result = dedup.groupBy("mapid").count().orderBy(F.col("count").desc())

# Show result
result.show()
