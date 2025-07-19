# %% [markdown]
# ### Define filepath

# %%
file_match_details = '../data/match_details.csv'
file_matches = '../data/matches.csv'
file_medals = '../data/medals.csv'
file_medals_matches_players = '../data/medals_matches_players.csv'
file_maps = '../data/maps.csv'

# %% [markdown]
# ### Load file

# %%
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, col
spark = SparkSession.builder.appName("Homework").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
### Load file into Spark
matches = spark.read.option('header','true').csv(file_matches)
match_details = spark.read.option('header','true').csv(file_match_details)
medals = spark.read.option('header','true').csv(file_medals)
medals_matches_players = spark.read.option('header','true').csv(file_medals_matches_players)
maps = spark.read.option('header','true').csv(file_maps)

# %% [markdown]
# #### Matches:

# %%
#### Show each file
matches.show(10)
print('Matches shape: {} rows and {} columns.'.format(matches.count(), len(matches.columns)))
print()
for col_name, dtype in matches.dtypes:
    print(f"{col_name:<30} {dtype}")

# %% [markdown]
# #### Match Details

# %%
#### Show each file
match_details = match_details.withColumnRenamed("player_gamertag", "f_player_gamertag")
match_details.show(10)
print('Matches shape: {} rows and {} columns.'.format(match_details.count(), len(match_details.columns)))
print()
for col_name, dtype in match_details.dtypes:
    print(f"{col_name:<30} {dtype}")

# %% [markdown]
# #### Medals

# %%
#### Show each file
medals.show(10)
print('Matches shape: {} rows and {} columns.'.format(medals.count(), len(medals.columns)))
print()
for col_name, dtype in medals.dtypes:
    print(f"{col_name:<30} {dtype}")

# %% [markdown]
# #### Medals Matches Players

# %%
#### Show each file
medals_matches_players.show(10)
print('Matches shape: {} rows and {} columns.'.format(medals_matches_players.count(), len(medals_matches_players.columns)))
print()
for col_name, dtype in medals_matches_players.dtypes:
    print(f"{col_name:<30} {dtype}")

# %% [markdown]
# #### Maps:

# %%
#### Show each file
maps.show(10)
print('Matches shape: {} rows and {} columns.'.format(maps.count(), len(maps.columns)))
print()
for col_name, dtype in maps.dtypes:
    print(f"{col_name:<30} {dtype}")

# %% [markdown]
# ## Homework
# 

# %%
#### Check automatic broadcast join
spark.conf.get("spark.sql.autoBroadcastJoinThreshold")

# %%
#### Disable automatic broadcast join
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

# %%
### Broadcast medals vs maps
from pyspark.sql.functions import broadcast
medals_broadcast = broadcast(medals)
maps_broadcast = broadcast(maps)

# %%
### Bucket join with 16 buckets on match_id
match_details.write.mode('overwrite').bucketBy(16, "match_id").sortBy("match_id").saveAsTable("bucketed_match_details")
# spark.sql("DESCRIBE EXTENDED bucketed_match_details").show(100,truncate=False)

# %%
matches.write.mode('overwrite').bucketBy(16, "match_id").sortBy("match_id").saveAsTable("bucketed_matches")
# spark.sql("DESCRIBE EXTENDED bucketed_matches").show(100, truncate=False)


# %%
medals_matches_players.write.mode('overwrite').bucketBy(16, "match_id").sortBy("match_id").saveAsTable("bucketed_medal_matches_players")
# spark.sql("DESCRIBE EXTENDED bucketed_medal_matches_players").show(truncate=False)

# %%
### read table again 
bucketed_match_details = spark.table("bucketed_match_details")
bucketed_matches = spark.table("bucketed_matches")
bucketed_mmp = spark.table("bucketed_medal_matches_players")

# %%
final = bucketed_mmp.join(bucketed_match_details, on = "match_id")\
    .join(medals_broadcast, on = 'medal_id')\
        .join(bucketed_matches, on = "match_id")\
            .join(maps_broadcast, on = "mapid")
final.explain()

# %%
final.orderBy('match_id').show()

# %% [markdown]
# ### Which player averages the most kills per game ?

# %%
from pyspark.sql import functions as F
dedup_df = final.select("match_id", "f_player_gamertag", "player_total_kills") \
    .dropDuplicates(["match_id", "f_player_gamertag"])
    
agg_df = dedup_df.groupBy("f_player_gamertag").agg(
    F.sum("player_total_kills").alias("total_kills"),
    F.count("match_id").alias("games_played"))

result_df = agg_df.withColumn(
    "avg_kills_per_game",
    F.col("total_kills") / F.col("games_played"))

player_avg_kills = result_df.orderBy(F.col("avg_kills_per_game").desc())
player_avg_kills.show()

# %% [markdown]
# ### Which playlist gets played the most ?

# %%
final.select("match_id", "playlist_id").dropDuplicates()\
    .groupBy("playlist_id").count()\
        .orderBy(F.col("count").desc()).show()

# %% [markdown]
# ###     Which map gets played the most ?

# %%
final.select('match_id','mapid').dropDuplicates()\
    .groupBy('mapid').count()\
        .orderBy(F.col('count').desc()).show()

# %% [markdown]
# ###     Which map do players get the most Killing Spree medals on ?
# 

# %%
final.filter(col('classification') == 'KillingSpree')\
    .select('match_id','mapid','classification').dropDuplicates()\
        .groupBy('mapid').count()\
            .orderBy(col('count').desc()).show()

# %% [markdown]
# ### Try sortWithinPartitions

# %%
final.show()

# %%
print('Number unique value of column mapid: {}'.format(final.select('mapid').distinct().count()))
print('Number unique value of column playlistid: {}'.format(final.select('playlist_id').distinct().count()))

# %%
playlist_sort = final.repartition("playlist_id").sortWithinPartitions("playlist_id") 
map_sort = final.repartition('mapid').sortWithinPartitions('mapid')
playlist_map_sort = final.repartition('playlist_id','mapid').sortWithinPartitions('playlist_id','mapid')
map_playlist_sort = final.repartition('mapid','playlist_id').sortWithinPartitions('mapid','playlist_id')


