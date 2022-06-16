### Song Recommendations ###
import sys

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *

from pyspark.sql import functions as F

from pyspark.ml.feature import StringIndexer
from pyspark.sql.window import Window

from pretty import SparkPretty       
pretty = SparkPretty(limit=5)

spark = SparkSession.builder.getOrCreate()
sc = SparkContext.getOrCreate()
# Compute suitable number of partitions

conf = sc.getConf()

N = int(conf.get("spark.executor.instances"))
M = int(conf.get("spark.executor.cores"))
partitions = 4 * N * M


#
## Q1
#

# (a) 
# using dataset triplets_not_mismatched from Processing Q2
# -----------------------------------------------------------------------------
# Load

mismatches_schema = StructType([
  StructField("song_id", StringType(), True),
  StructField("song_artist", StringType(), True),
  StructField("song_title", StringType(), True),
  StructField("track_id", StringType(), True),
  StructField("track_artist", StringType(), True),
  StructField("track_title", StringType(), True)
])

with open("/scratch-network/courses/2022/DATA420-22S1/data/msd/tasteprofile/mismatches/sid_matches_manually_accepted.txt", "r") as f:
  lines = f.readlines()
  sid_matches_manually_accepted = []
  for line in lines:
    if line.startswith("< ERROR: "):
      a = line[10:28]
      b = line[29:47]
      c, d = line[49:-1].split("  !=  ")
      e, f = c.split("  -  ")
      g, h = d.split("  -  ")
      sid_matches_manually_accepted.append((a, e, f, b, g, h))

matches_manually_accepted = spark.createDataFrame(sc.parallelize(sid_matches_manually_accepted, 8), schema=mismatches_schema)
matches_manually_accepted.cache()
matches_manually_accepted.show(10, 20)

print(matches_manually_accepted.count())    # 488

with open("/scratch-network/courses/2022/DATA420-22S1/data/msd/tasteprofile/mismatches/sid_mismatches.txt", "r") as f:
  lines = f.readlines()
  sid_mismatches = []
  for line in lines:
    if line.startswith("ERROR: "):
      a = line[8:26]
      b = line[27:45]
      c, d = line[47:-1].split("  !=  ")
      e, f = c.split("  -  ")
      g, h = d.split("  -  ")
      sid_mismatches.append((a, e, f, b, g, h))

mismatches = spark.createDataFrame(sc.parallelize(sid_mismatches, 64), schema=mismatches_schema)
mismatches.cache()
mismatches.show(10, 20)

print(mismatches.count())     # 19094

triplets_schema = StructType([
  StructField("user_id", StringType(), True),
  StructField("song_id", StringType(), True),
  StructField("plays", IntegerType(), True)
])
triplets = (
  spark.read.format("csv")
  .option("header", "false")
  .option("delimiter", "\t")
  .option("codec", "gzip")
  .schema(triplets_schema)
  .load("hdfs:///data/msd/tasteprofile/triplets.tsv/")
  .cache()
)
triplets.cache()
triplets.show(10, 50)

mismatches_not_accepted = mismatches.join(matches_manually_accepted, on="song_id", how="left_anti")
triplets_not_mismatched = triplets.join(mismatches_not_accepted, on="song_id", how="left_anti")

triplets_not_mismatched = triplets_not_mismatched.repartition(partitions).cache()

print(mismatches_not_accepted.count())  # 19093
print(triplets.count())                 # 48373586
print(triplets_not_mismatched.count())  # 45795111


# how many unique songs and users in the dataset ?
# -----------------------------------------------------------------------------
uniq_songs = triplets_not_mismatched.select(F.col('song_id')).distinct().count()   
uniq_songs   # 378,310 songs
uniq_users = triplets_not_mismatched.select(F.col('user_id')).distinct().count()   
uniq_users   # 1019,318 users


# (b) 
# -----------------------------------------------------------------------------
# how many different songs has the most active user played?
# what is the percentage?
active_user = (
    triplets_not_mismatched
    .groupBy('user_id')
    .agg(
      F.count(F.col('song_id')).alias('song_count'),
      F.sum(F.col('plays')).alias('play_count'))
    .withColumn('percentage (%)', F.col('song_count')/uniq_songs * 100 )
    .orderBy(F.col('play_count').desc())
)
active_user.show(5,)
# +--------------------+----------+----------+-------------------+
# |             user_id|song_count|play_count|     percentage (%)|
# +--------------------+----------+----------+-------------------+
# |093cb74eb3c517c51...|       195|     13074|0.05154502920884989|
# |119b7c88d58d0c6eb...|      1362|      9104| 0.3600222040125823|
# |3fa44653315697f42...|       146|      8025|0.03859268853585684|
# |a2679496cd0af9779...|       518|      6506|0.13692474425735507|
# |d7d2d888ae04d16e9...|      1257|      6190|0.33226718828474006|
# +--------------------+----------+----------+-------------------+

#showing in the head, the most active user played 195 different songs, 0.05% of total uniqe songs in the dataset.


# (c)
# -----------------------------------------------------------------------------
import pandas as pd
import os 
os.environ['QT_QPA_PLATFORM']='offscreen'
import matplotlib.pyplot as plt

def plot_distribution(data, name, col):
    f = plt.figure(figsize=(15,12), dpi=80) 
    
    df = data.toPandas()
    y=df[col].tolist()
    
    plt.hist(y, bins=300)
    
    plt.title(f'Distribution of {name}') 
    plt.ylabel('plays')
    plt.tight_layout()

    f.savefig(os.path.join(os.path.expanduser(f"~/{name}.png")), bbox_inches="tight")  # save as png and view in windows
    plt.close(f) 
    
# the distribution of song popularity
# -----------------------------------------------------------------------------
song_popularity = (
    triplets_not_mismatched
    .groupBy('song_id')
    .agg(
       F.count(F.col('user_id')).alias('user_count'),
       F.sum(F.col('plays')).alias('play_count'))
    .orderBy(F.col('play_count').desc())
)
song_popularity.show(5,)
# +------------------+----------+----------+
# |           song_id|user_count|play_count|
# +------------------+----------+----------+
# |SOBONKR12A58A7A7E0|     84000|    726885|
# |SOSXLTC12AF72A7F54|     80656|    527893|
# |SOEGIYH12A6D4FC0E3|     69487|    389880|
# |SOAXGDH12A8C13F8A1|     90444|    356533|
# |SONYKOW12AB01849C9|     78353|    292642|
# +------------------+----------+----------+

plot_distribution(song_popularity, 'Song Popularity', 'play_count')
 
# the distribution of user activity, can just use active_user dataset
# -----------------------------------------------------------------------------
plot_distribution(active_user, 'User Activity', 'play_count')


# (d)
# -----------------------------------------------------------------------------
# User statistics
statistics = (
    active_user
    .select('song_count', 'play_count')
    .describe()
    .toPandas()
    .set_index('summary')
    .rename_axis(None)
    .T
)
print(statistics)
              # count                mean              stddev min    max
# song_count  1019318   44.92720721109605  54.911131997473966   3   4316
# play_count  1019318  128.82423149596102  175.43956510304753   3  13074

active_user.approxQuantile('song_count', [0.0, 0.25, 0.5, 0.75, 1.0], 0.05)  # -->[3.0, 15.0, 25.0, 51.0, 4316.0]
active_user.approxQuantile('play_count', [0.0, 0.25, 0.5, 0.75, 1.0], 0.05)  # -->[3.0, 28.0, 61.0, 130.0, 13074.0]
# above result might be different each run, as it is approxmate Quantile


# -----------------------------------------------------------------------------
# Song statistics
statistics = (
    song_popularity
    .select('user_count', 'play_count')
    .describe()
    .toPandas()
    .set_index('summary')
    .rename_axis(None)
    .T
)
print(statistics)
             # count                mean              stddev min     max
# user_count  378310  121.05181200602681   748.6489783736954   1   90444
# play_count  378310   347.1038513388491  2978.6053488382245   1  726885

song_popularity.approxQuantile('user_count', [0.0, 0.25, 0.5, 0.75, 1.0], 0.05)  # -->[1.0, 4.0, 12.0, 42.0, 90444.0]
song_popularity.approxQuantile('play_count', [0.0, 0.25, 0.5, 0.75, 1.0], 0.05)  # -->[1.0, 7.0, 27.0, 126.0, 726885.0]


# -----------------------------------------------------------------------------
# Limiting
N = 8   # songs which have been played less than N times
M = 34  # users who have listened to less than M songs

unpopulary_song = song_popularity.filter(F.col('play_count')<N).select(F.col('song_id')).distinct()    
inactive_user = active_user.filter(F.col('song_count')<M).select(F.col('user_id')).distinct()          

unpopulary_song.count()     # 90750, 24.0% of uniq_songs
inactive_user.count()       # 614399, 60.28% of uniq_users


triplets_clean = (triplets_not_mismatched
                    .join(unpopulary_song, how='left_anti', on='song_id')
                    .join(inactive_user, how='left_anti', on='user_id')
)

triplets_clean.cache()
triplets_clean.count()    # 34550861

triplets_clean.select(F.col('user_id')).distinct().count()   # 404919
triplets_clean.select(F.col('song_id')).distinct().count()   # 286986


# (e)
# -----------------------------------------------------------------------------
# Encoding
user_stringIdx = StringIndexer(inputCol='user_id', outputCol='user_index')
temp = user_stringIdx.fit(triplets_clean)      # a model
triplets_temp = temp.transform(triplets_clean)

song_stringIdx = StringIndexer(inputCol='song_id', outputCol='song_index')
temp = song_stringIdx.fit(triplets_temp)
triplets_trans = temp.transform(triplets_temp)

triplets_trans.cache()
triplets_trans.show(5,)
# +--------------------+------------------+-----+----------+----------+
# |             user_id|           song_id|plays|user_index|song_index|
# +--------------------+------------------+-----+----------+----------+
# |00007ed2509128dcd...|SOCXHEU12A6D4FB331|    1|  227842.0|   64482.0|
# |00007ed2509128dcd...|SODESWY12AB0182F2E|    1|  227842.0|   51882.0|
# |00007ed2509128dcd...|SOPIROE12A6D4FD4EB|    1|  227842.0|   54706.0|
# |00007ed2509128dcd...|SOPZJAI12AB0181F53|    1|  227842.0|  192749.0|
# |00007ed2509128dcd...|SORKZYO12AF72A8CA2|    1|  227842.0|    7887.0|
# +--------------------+------------------+-----+----------+----------+

# -----------------------------------------------------------------------------
# Splitting
training, test = triplets_trans.randomSplit([0.7, 0.3])

test_not_training = test.join(training, on="user_id", how="left_anti")

training.cache()
test.cache()
test_not_training.cache()

counts = test_not_training.groupBy("user_id").count().toPandas().set_index("user_id")["count"].to_dict()

temp = (
  test_not_training
  .withColumn("id", monotonically_increasing_id())
  .withColumn("random", rand())
  .withColumn(
    "row",
    row_number()
    .over(
      Window
      .partitionBy("user_id")
      .orderBy("random")
    )
  )
)

for k, v in counts.items():
  temp = temp.where((col("user_id") != k) | (col("row") < v * 0.7))

temp = temp.drop("id", "random", "row")
temp.cache()

temp.show(50, False)

training = training.union(temp.select(training.columns))
test = test.join(temp, on=["user_id", "song_id"], how="left_anti")
test_not_training = test.join(training, on="user_id", how="left_anti")

print(f"training:      {training.count()}")
print(f"test:        {test.count()}")
print(f"test_not_training: {test_not_training.count()}")

#training:      24186846
#test:          10364015
#test_not_training: 0


#
## Q2
#
from pyspark.ml.feature import StringIndexer
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.mllib.evaluation import RankingMetrics
from pyspark.sql import Window

# (a)
# -----------------------------------------------------------------------------
# Training
als = ALS(maxIter=5, regParam=0.01, userCol="user_index", itemCol="song_index", ratingCol="plays", implicitPrefs=True)    #have a relative preferences out of it
alsModel = als.fit(training)
predictions = alsModel.transform(test)
 
predictions.cache()
predictions.show(10, 50)
# +----------------------------------------+------------------+-----+----------+----------+-----------+
# |                                 user_id|           song_id|plays|user_index|song_index| prediction|
# +----------------------------------------+------------------+-----+----------+----------+-----------+
# |6b9ef666e3b10fd428530e519e1a3f68c2fbbd1a|SOUVTSM12AC468F6A7|    1|     263.0|      12.0|0.060178164|
# |caae83a17bf6d278a6b4c2173df5d86854ad9a68|SOUVTSM12AC468F6A7|    1|     641.0|      12.0| 0.31170386|
# |e4b426ac0d1cb8ec2daf5370b106e270a7dd5b06|SOUVTSM12AC468F6A7|    7|    1830.0|      12.0|   0.702548|
# |7cfde2690996a27298b3224c9721f5be0dcae9ce|SOUVTSM12AC468F6A7|    1|    2030.0|      12.0| 0.94766414|
# |f097e52d5e59d091c78d89a065da48b78beca9ff|SOUVTSM12AC468F6A7|   16|    2536.0|      12.0| 0.98355037|
# |161e139b35da167dcc0eee607af5c2b61b8cd427|SOUVTSM12AC468F6A7|    1|    2796.0|      12.0| 0.13609116|
# |4307376bfe8916be4c15c32f0b124ee4b4a546f0|SOUVTSM12AC468F6A7|    1|    2840.0|      12.0| 0.35253036|
# |7ffc14a55b6256c9fa73fc5c5761d210deb7f738|SOUVTSM12AC468F6A7|    1|    3404.0|      12.0| 0.98204434|
# |5ed8fae2fbc1e24059abbe11963a0c377715e7b5|SOUVTSM12AC468F6A7|    2|    3572.0|      12.0|  0.5534528|
# |8441fb7f90245d5ead56e6f2e4763deec7aa5db4|SOUVTSM12AC468F6A7|   16|    4348.0|      12.0|   0.895289|
# +----------------------------------------+------------------+-----+----------+----------+-----------+


# (b)
# -----------------------------------------------------------------------------
# random select 5 users
users = test.select(['user_index']).distinct().limit(5)     #every time run different 
users.cache()     
users.show()
# +----------+
# |user_index|
# +----------+
# |  114795.0|
# |  182251.0|
# |   62295.0|
# |  304096.0|
# |  326729.0|
# +----------+

# generate top 10 recommendations for the selected users, 
k = 10
topK = alsModel.recommendForUserSubset(users, k)
topK.cache()
topK.show(5, 50)
# +----------+--------------------------------------------------+
# |user_index|                                   recommendations|
# +----------+--------------------------------------------------+
# |    114795|[{13, 0.27952263}, {10, 0.2690736}, {8, 0.26460...|
# |    326729|[{65, 0.25667712}, {39, 0.25585505}, {26, 0.232...|
# |    182251|[{280, 0.113841355}, {153, 0.100179926}, {164, ...|
# |     62295|[{182, 0.14727783}, {337, 0.1276094}, {308, 0.1...|
# |    304096|[{11, 0.41880083}, {5, 0.35885793}, {25, 0.3288...|
# +----------+--------------------------------------------------+

# -----------------------------------------------------------------------------
# Helps to extract recommendations, song_index
def extract_songs_top_k(x, k):
  x = sorted(x, key=lambda i: -i[1])
  return [i[0] for i in x][0:k]

extract_songs_top_k_udf = F.udf(lambda x: extract_songs_top_k(x, k), ArrayType(IntegerType()))

def extract_songs(x):
  x = sorted(x, key=lambda i: -i[1])
  return [i[0] for i in x]

extract_songs_udf = F.udf(lambda x: extract_songs(x), ArrayType(IntegerType()))

recommended_songs_sub = (
  topK
  .withColumn("recommended_songs", extract_songs_top_k_udf(F.col("recommendations")))
  .select("user_index", "recommended_songs")
)
recommended_songs_sub.cache()
recommended_songs_sub.show(5, False)
# +----------+----------------------------------------------------+
# |user_index|recommended_songs                                   |
# +----------+----------------------------------------------------+
# |114795    |[13, 10, 8, 21, 20, 122, 24, 6, 50, 17]             |
# |326729    |[65, 39, 26, 75, 57, 166, 170, 102, 85, 51]         |
# |182251    |[280, 153, 164, 230, 200, 193, 187, 130, 15, 352]   |
# |62295     |[182, 337, 308, 140, 3245, 500, 1025, 2129, 191, 83]|
# |304096    |[11, 5, 25, 3, 2, 89, 93, 82, 64, 7]                |
# +----------+----------------------------------------------------+

# -----------------------------------------------------------------------------   
# users actual played songs for all users (relevant songs, grond truth)
relevant_songs = (
  test
  .select( 
    F.col("user_index").cast(IntegerType()),
    F.col("song_index").cast(IntegerType()),
    F.col("plays").cast(IntegerType())
  )
  .groupBy('user_index')
  .agg(
    F.collect_list(      
      F.array(
        F.col("song_index"),
        F.col("plays")
      )
    ).alias('relevance')
  )
  .withColumn("relevant_songs", extract_songs_udf(F.col("relevance")))      
  .select("user_index", "relevant_songs")
)
relevant_songs.cache()
relevant_songs.count()    # 404919

# filter the relevant_songs for the selected users
users_list = users.select('user_index').rdd.flatMap(lambda x: x).collect()
relevant_songs.filter(F.col('user_index').isin(users_list)).show(5, False)
# +----------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
# |114795    |[5854, 4184, 1039, 9806, 239, 1356, 681, 264, 8809, 9586, 1925, 20, 388, 2187, 5703, 1306, 3847, 173, 2532, 346, 2620, 546, 972, 7320, 10256, 2669, 4175, 9459]                                                                                            |
# |326729    |[1457, 7511, 21108, 11052, 1707, 14250]                                                                                                                                                                                                                    |
# |182251    |[244734, 63512, 15209, 15007, 3148, 1738, 50051, 22842, 7117, 1448]                                                                                                                                                                                        |
# |62295     |[70132, 73666, 32886, 128378, 66179, 39969, 4324, 25469, 1419, 139273, 11589, 121012, 123782, 148154, 21463, 110963, 65529, 33709, 62422, 90618, 61044, 105680, 118627, 58045, 13089, 70452, 114250, 33483, 130460, 12823, 7966, 12154, 1476, 99985, 83852]|
# |304096    |[35, 9086, 58176, 38045, 9565, 10578, 45435, 24, 13998, 10244, 1908, 230, 15769, 119708]                                                                                                                                                                   |
# +----------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+

# ----------------------------------------------------------------------------- 
# Load metadata
metadata = (
    spark.read
    .format("com.databricks.spark.csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("hdfs:///data/msd/main/summary/metadata.csv.gz")
)

metadata.cache()
print(pretty(metadata.head().asDict()))
# {
  # 'analyzer_version': None,
  # 'artist_7digitalid': 4069,
  # 'artist_familiarity': 0.6498221002008776,
  # 'artist_hotttnesss': 0.3940318927141434,
  # 'artist_id': 'ARYZTJS1187B98C555',
  # 'artist_latitude': None,
  # 'artist_location': None,
  # 'artist_longitude': None,
  # 'artist_mbid': '357ff05d-848a-44cf-b608-cb34b5701ae5',
  # 'artist_name': 'Faster Pussy cat',
  # 'artist_playmeid': '44895',
  # 'genre': None,
  # 'idx_artist_terms': 0,
  # 'idx_similar_artists': 0,
  # 'release': 'Monster Ballads X-Mas',
  # 'release_7digitalid': 633681,
  # 'song_hotttnesss': '0.5428987432910862',
  # 'song_id': 'SOQMMHC12AB0180CB8',
  # 'title': 'Silent Night',
  # 'track_7digitalid': '7032331'
# }

# ----------------------------------------------------------------------------- 
# Let's look at user index 326729, we can get his recommended_songs indexes list and relevant_songs indexes list
reco_list1 = [65, 39, 26, 75, 57, 166, 170, 102, 85, 51]

# Let's look at the metadata information of his recommended_songs
df1 = (
    metadata
    .join(triplets_trans, on='song_id', how='left')
    .filter(F.col('song_index').isin(reco_list1))   
    .sort(F.col('song_index')))

df1.select("song_index", "artist_name", "artist_location","genre", "release","song_hotttnesss","title").distinct().show(10,False)
# +----------+-----------------------+----------------------+-----+-----------------------------------------+------------------+------------------------------------+
# |song_index|artist_name            |artist_location       |genre|release                                  |song_hotttnesss   |title                               |
# +----------+-----------------------+----------------------+-----+-----------------------------------------+------------------+------------------------------------+
# |166.0     |Five Finger Death Punch|Los Angeles           |null |War Is The Answer                        |1.0               |Bad Company                         |
# |51.0      |Evanescence            |New York, NY          |null |Fallen                                   |1.0               |Bring Me To Life                    |
# |85.0      |Jimmy Eat World        |Mesa, AZ              |null |Bleed American                           |1.0               |The Middle                          |
# |26.0      |Linkin Park            |Los Angeles, CA       |null |Road To Revolution: Live At Milton Keynes|0.9943392701758648|Bleed It Out [Live At Milton Keynes]|
# |102.0     |Nickelback             |Hanna, Alberta, Canada|null |FETENHITS - New Party Rock (set)         |null              |How You Remind Me                   |
# |39.0      |3 Doors Down           |Escatawpa, MS         |null |Total 90s                                |0.9151810636939853|Kryptonite                          |
# |65.0      |Rise Against           |Chicago               |null |Appeal To Reason                         |0.9181823430861558|Savior                              |
# |170.0     |Linkin Park            |Los Angeles, CA       |null |What I've Done                           |1.0               |What I've Done (Album Version)      |
# |75.0      |Linkin Park            |Los Angeles, CA       |null |Hybrid Theory                            |0.5372732698188906|In The End (Album Version)          |
# |57.0      |3 Doors Down           |Escatawpa, MS         |null |Here Without You                         |0.9167048412975676|Here Without You                    |
# +----------+-----------------------+----------------------+-----+-----------------------------------------+------------------+------------------------------------+


# Let's look at the metadata information of his relevant_songs
rele_list1 = [1457, 7511, 21108, 11052, 1707, 14250]
df2 = (
    metadata
    .join(triplets_trans, on='song_id', how='left')
    .filter(F.col('song_index').isin(rele_list1))   
    .sort(F.col('song_index')))

df2.select("song_index", "artist_name", "artist_location","genre", "release","song_hotttnesss","title").distinct().show(10,False)
# +----------+-----------------+----------------------+-----+--------------------------------+-------------------+-----------------------------+
# |song_index|artist_name      |artist_location       |genre|release                         |song_hotttnesss    |title                        |
# +----------+-----------------+----------------------+-----+--------------------------------+-------------------+-----------------------------+
# |1707.0    |Avenged Sevenfold|Huntington Beach, CA  |null |City Of Evil                    |1.0                |Bat Country (Album Version)  |
# |21108.0   |3 Doors Down     |Escatawpa, MS         |null |The Better Life - Deluxe Edition|0.6909670911246506 |By My Side                   |
# |7511.0    |Nickelback       |Hanna, Alberta, Canada|null |The State                       |0.29635253921585397|Leader Of Men                |
# |11052.0   |3 Doors Down     |Escatawpa, MS         |null |Away From The Sun               |0.6971656400966489 |Going Down In Flames         |
# |14250.0   |Nine Inch Nails  |Cleveland, OH         |null |Year Zero                       |0.6920594416940057 |The Good Soldier             |
# |1457.0    |3 Doors Down     |Escatawpa, MS         |null |3 Doors Down                    |0.7973552451239266 |Citizen/Soldier              |
# |7511.0    |Nickelback       |Hanna, Alberta, Canada|null |The State                       |0.29635253921585397|Leader Of Men (Album Version)|
# +----------+-----------------+----------------------+-----+--------------------------------+-------------------+-----------------------------+


# ----------------------------------------------------------------------------- 
# Let's look at user index 182251, we can get his recommended_songs indexes list and relevant_songs indexes list
reco_list2 = [280, 153, 164, 230, 200, 193, 187, 130, 15, 352]
# Let's look at the metadata information of his recommended_songs
df3 = (
    metadata
    .join(triplets_trans, on='song_id', how='left')
    .filter(F.col('song_index').isin(reco_list2))   
    .sort(F.col('song_index')))

df3.select("song_index", "artist_name", "artist_location","genre", "release","song_hotttnesss","title").distinct().show(10,False)
# +----------+----------------------------+---------------+-----+-----------------------------------------------+-------------------+----------------------------------------------+
# |song_index|artist_name                 |artist_location|genre|release                                        |song_hotttnesss    |title                                         |
# +----------+----------------------------+---------------+-----+-----------------------------------------------+-------------------+----------------------------------------------+
# |200.0     |Ramones                     |null           |null |Loud_ Fast_ Ramones:  Their Toughest Hits      |0.30106257067930703|I Wanna Be Sedated (Remastered Album Version )|
# |15.0      |Radiohead                   |Oxford, UK     |null |Pablo Honey                                    |null               |Creep (Explicit)                              |
# |153.0     |Counting Crows              |null           |null |Films About Ghosts (The Best Of Counting Crows)|1.0                |Mr. Jones                                     |
# |230.0     |Cat Stevens                 |London, England|null |Tea For The Tillerman                          |null               |Wild World                                    |
# |164.0     |Queen                       |London, England|null |Queen On Fire - Live At The Bowl               |0.8192640225344132 |Under Pressure                                |
# |187.0     |Creedence Clearwater Revival|El Cerrito, CA |null |Chronicle: 20 Greatest Hits                    |0.26695518627553855|Have You Ever Seen The Rain                   |
# |280.0     |Asia 2001                   |null           |null |Amnesia                                        |0.2998774882739778 |Epilogue                                      |
# |130.0     |Nirvana                     |London, England|null |Nirvana                                        |0.9489238881903628 |Come As You Are                               |
# |352.0     |Drowning Pool               |null           |null |Full Circle                                    |0.6517464002197019 |Reason I'm Alive (Explicit)                   |
# |193.0     |Creedence Clearwater Revival|El Cerrito, CA |null |More Creedence Gold                            |0.8584058406612279 |Fortunate Son                                 |
# +----------+----------------------------+---------------+-----+-----------------------------------------------+-------------------+----------------------------------------------+

# Let's look at the metadata information of his relevant_songs
rele_list2 = [244734, 63512, 15209, 15007, 3148, 1738, 50051, 22842, 7117, 1448]
df4 = (
    metadata
    .join(triplets_trans, on='song_id', how='left')
    .filter(F.col('song_index').isin(rele_list2))   
    .sort(F.col('song_index')))

df4.select("song_index", "artist_name", "artist_location","genre", "release","song_hotttnesss","title").distinct().show(10,False)
# +----------+-----------------+--------------------------------------------+-----+---------------------------------------------------------------------+------------------+----------------------------------------------+
# |song_index|artist_name      |artist_location                             |genre|release                                                              |song_hotttnesss   |title                                         |
# +----------+-----------------+--------------------------------------------+-----+---------------------------------------------------------------------+------------------+----------------------------------------------+
# |244734.0  |Willy Porter     |null                                        |null |Willy Porter                                                         |null              |Dishwater Blonde                              |
# |50051.0   |Bob Dylan        |Duluth, MN                                  |null |PAT GARRETT & BILLY THE KID             Original Soundtrack Recording|0.5578803889070679|Billy 1                                       |
# |1738.0    |Phish            |Burlington, VT                              |null |Billy Breathes                                                       |0.6544740345610037|Prince Caspian                                |
# |15007.0   |Simon & Garfunkel|Forest Hills, Queens, New York City, NY, USA|null |Simon And Garfunkel's Greatest Hits                                  |0.8287771295049096|The Sounds Of Silence                         |
# |3148.0    |Rancid           |Bay Area, CA                                |null |And Out Come The Wolves                                              |0.7783235245028153|Ruby Soho (Album Version)                     |
# |1448.0    |R.E.M.           |Athens, GA                                  |null |What's The Frequency_ Kenneth?                                       |0.7183634577034074|What's The Frequency_ Kenneth? (Radio Version)|
# |22842.0   |Everclear        |Portland, OR                                |null |So Much For The Afterglow                                            |0.6619403171278845|White Men In Black Suits                      |
# |63512.0   |The Vandals      |Huntington Beach, CA                        |null |Sweatin' To The Oldies: The Vandals Live                             |0.5109966480826532|Anarchy Burger (Hold The Government)          |
# |15209.0   |Beastie Boys     |New York, NY                                |null |Anthology:  The Sounds Of Science                                    |0.7107011698541876|Alive (Digitally Remastered 99)               |
# |7117.0    |Green Day        |Berkeley, CA                                |null |Dookie                                                               |0.737310450179272 |Having A Blast (Album Version)                |
# +----------+-----------------+--------------------------------------------+-----+---------------------------------------------------------------------+------------------+----------------------------------------------+


# (c)
# -----------------------------------------------------------------------------
# recommend for all users
topK = alsModel.recommendForAllUsers(k)

recommended_songs = (
  topK
  .withColumn("recommended_songs", extract_songs_top_k_udf(F.col("recommendations")))
  .select("user_index", "recommended_songs")
)
recommended_songs.cache()
recommended_songs.count()         # 404919, same as relevant_songs
recommended_songs.show(10, 50)    # very slow
# +----------+-----------------------------------------------+
# |user_index|                              recommended_songs|
# +----------+-----------------------------------------------+
# |        12|           [11, 24, 37, 3, 17, 2, 7, 35, 5, 64]|
# |        18|        [24, 13, 10, 17, 8, 6, 88, 95, 52, 122]|
# |        38|[15, 153, 280, 73, 130, 164, 200, 45, 230, 193]|
# |        67| [37, 134, 11, 72, 204, 145, 70, 222, 182, 247]|
# |        70|    [11, 15, 182, 45, 63, 7, 135, 188, 200, 37]|
# |        93|         [11, 37, 7, 3, 24, 145, 200, 72, 2, 5]|
# |       161|           [10, 24, 15, 8, 13, 4, 17, 1, 20, 6]|
# |       186|        [0, 15, 48, 47, 33, 87, 73, 32, 106, 6]|
# |       190|     [15, 72, 37, 0, 134, 27, 11, 36, 204, 135]|
# |       218|     [90, 182, 278, 48, 11, 35, 7, 128, 73, 15]|
# +----------+-----------------------------------------------+


# -----------------------------------------------------------------------------
# Combine and compare
combined = (
  recommended_songs.join(relevant_songs, on='user_index', how='inner')
  .rdd
  .map(lambda row: (row[1], row[2]))
)
combined.cache()
# print(combined.take(1))
 
metrics = RankingMetrics(combined)   

print("PRECISION @ 10: ", metrics.precisionAt(10))
print("NDCG @10:       ", metrics.ndcgAt(10))
print("MAP:            ", metrics.meanAveragePrecision)

# PRECISION @ 10:  0.050056431039294316
# NDCG @10:        0.05392804713663542
# MAP:             0.009698757041752123

