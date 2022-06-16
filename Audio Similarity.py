### Audio Similarity ### 
#
## Q1
#
#start_pyspark_shell -e 4 -c 2 -w 4 -m 4

import sys
import numpy as np

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

from pretty import SparkPretty       
pretty = SparkPretty(limit=5)

from pyspark.ml.stat import Correlation
from pyspark.ml.feature import VectorAssembler

spark = SparkSession.builder.getOrCreate()
sc = SparkContext.getOrCreate()
# Compute suitable number of partitions

conf = sc.getConf()

N = int(conf.get("spark.executor.instances"))
M = int(conf.get("spark.executor.cores"))
partitions = 4 * N * M     # 32     

# -----------------------------------------------------------------------------

# Attributes

# hdfs dfs -cat "/data/msd/audio/attributes/*" | awk -F',' '{print $2}' | sort | uniq

# NUMERIC
# real
# real 
# string
# string 
# STRING

audio_attribute_type_mapping = {
  "NUMERIC": DoubleType(),
  "real": DoubleType(),
  "string": StringType(),
  "STRING": StringType()
}

audio_dataset_names = [
  ("msd-jmir-area-of-moments-all-v1.0", "aom"),
  ("msd-jmir-lpc-all-v1.0", "lpc"),
  ("msd-jmir-methods-of-moments-all-v1.0", "mom"),
  ("msd-jmir-mfcc-all-v1.0", "mfcc"),
  ("msd-jmir-spectral-all-all-v1.0", "spec"),
  ("msd-jmir-spectral-derivatives-all-all-v1.0", "specdrv"),
  ("msd-marsyas-timbral-v1.0", "timbral"),
  ("msd-mvd-v1.0", "mvd"),
  ("msd-rh-v1.0", "rh"),
  ("msd-rp-v1.0", "rp"),
  ("msd-ssd-v1.0", "ssd"),
  ("msd-trh-v1.0", "trh"),
  ("msd-tssd-v1.0", "tssd"),
]

audio_dataset_schemas = {}
for audio_dataset_name, audio_dataset_column_prefix in audio_dataset_names:
  print(audio_dataset_name)

  audio_dataset_path = f"/scratch-network/courses/2022/DATA420-22S1/data/msd/audio/attributes/{audio_dataset_name}.attributes.csv"
  with open(audio_dataset_path, "r") as f:
    rows = [line.strip().split(",") for line in f.readlines()]

  # We could rename columns to make them easier to identify if we combined datasets

  # rows[-1][0] = "track_id"
  # for i, row in enumerate(rows[0:-1]):
  #   row[0] = f"{audio_dataset_column_prefix}_feature_{i:04d}"

  audio_dataset_schemas[audio_dataset_name] = StructType([
    StructField(row[0], audio_attribute_type_mapping[row[1]], True) for row in rows
  ])


# -----------------------------------------------------------------------------
# use schemas from Processing Q2 to load any one of the above datasets

## (a)
# Load
audio_dataset_name = "msd-jmir-spectral-all-all-v1.0"

schema = audio_dataset_schemas[audio_dataset_name]
audio = spark.read.format("com.databricks.spark.csv") \
  .option("header", "true") \
  .option("inferSchema", "false") \
  .schema(schema) \
  .load(f"hdfs:///data/msd/audio/features/{audio_dataset_name}.csv") \
  .repartition(partitions)

audio.cache()
audio.count()     # 994615
print(pretty(audio.head().asDict()))      #result could be different at each run

#{
#  'Compactness_Overall_Average_1': 1570.0,
#  'Compactness_Overall_Standard_Deviation_1': 200.6,
#  'Fraction_Of_Low_Energy_Windows_Overall_Average_1': 0.5988,
#  'Fraction_Of_Low_Energy_Windows_Overall_Standard_Deviation_1': 0.05096,
#  'MSD_TRACKID': "'TRHFHYX12903CAF953'",
#  'Root_Mean_Square_Overall_Average_1': 0.1532,
#  'Root_Mean_Square_Overall_Standard_Deviation_1': 0.09163,
#  'Spectral_Centroid_Overall_Average_1': 7.432,
#  'Spectral_Centroid_Overall_Standard_Deviation_1': 8.501,
#  'Spectral_Flux_Overall_Average_1': 0.003384,
#  'Spectral_Flux_Overall_Standard_Deviation_1': 0.005855,
#  'Spectral_Rolloff_Point_Overall_Average_1': 0.05245,
#  'Spectral_Rolloff_Point_Overall_Standard_Deviation_1': 0.07007,
#  'Spectral_Variability_Overall_Average_1': 0.004289,
#  'Spectral_Variability_Overall_Standard_Deviation_1': 0.003042,
#  'Zero_Crossings_Overall_Average_1': 25.07,
#  'Zero_Crossings_Overall_Standard_Deviation_1': 21.18
#}

audio = audio.dropna()
audio.count()  # 994615, there is no missing values 

# -----------------------------------------------------------------------------
# Descriptive Statistics
numaudio = audio.drop("MSD_TRACKID")
statistics = (
    numaudio
    .describe()    
    .toPandas()
    .set_index("summary")
    .T      
)
print(statistics)

# summary                                              count                  mean                 stddev  min      max
# Spectral_Centroid_Overall_Standard_Deviation_1      994615     6.945075321958793      3.631804093939155  0.0    73.31
# Spectral_Rolloff_Point_Overall_Standard_Deviati...  994615   0.05570656330077426    0.02650021006030337  0.0   0.3739
# Spectral_Flux_Overall_Standard_Deviation_1          994615  0.003945429422315233   0.003265333343455646  0.0  0.07164
# Compactness_Overall_Standard_Deviation_1            994615    222.51785769557006      59.72639079514589  0.0  10290.0
# Spectral_Variability_Overall_Standard_Deviation_1   994615  0.002227141353483172  0.0010397404109806923  0.0  0.01256
# Root_Mean_Square_Overall_Standard_Deviation_1       994615   0.07420121987116508    0.03176619445967484  0.0   0.3676
# Fraction_Of_Low_Energy_Windows_Overall_Standard...  994615  0.060207318112032934    0.01851647926605091  0.0   0.4938
# Zero_Crossings_Overall_Standard_Deviation_1         994615    16.802847226233283      7.530133216657504  0.0    141.6
# Spectral_Centroid_Overall_Average_1                 994615     9.110257231921976     3.8436388429686703  0.0    133.0
# Spectral_Rolloff_Point_Overall_Average_1            994615   0.06194320589479273   0.029016729824972557  0.0   0.7367
# Spectral_Flux_Overall_Average_1                     994615  0.002932155180292255  0.0024911525809720157  0.0  0.07549
# Compactness_Overall_Average_1                       994615    1638.7321744558617     106.10634441394468  0.0  24760.0
# Spectral_Variability_Overall_Average_1              994615  0.004395476870406151  0.0019958918239917404  0.0  0.02366
# Root_Mean_Square_Overall_Average_1                  994615   0.16592784214140105    0.07429866377222298  0.0   0.8564
# Fraction_Of_Low_Energy_Windows_Overall_Average_1    994615    0.5562831490668245    0.04755380098948042  0.0   0.9538
# Zero_Crossings_Overall_Average_1                    994615    26.680410639768105     10.394734197335119  0.0    280.5


# -----------------------------------------------------------------------------
# correlation
assembler = VectorAssembler(inputCols=numaudio.columns, outputCol="Features")
features = assembler.transform(numaudio).select("Features")

features.cache()
features.show(10, 80)
#+--------------------------------------------------------------------------------+
#|                                                                        Features|
#+--------------------------------------------------------------------------------+
#|[8.501,0.07007,0.005855,200.6,0.003042,0.09163,0.05096,21.18,7.432,0.05245,0....|
#|[5.101,0.04946,0.007952,241.3,0.002879,0.08716,0.03366,13.13,9.995,0.07575,0....|
#|[8.101,0.06402,0.002458,238.5,0.002335,0.08902,0.06764,18.71,15.35,0.102,0.00...|
#|[7.226,0.05985,0.005215,194.7,0.002057,0.05784,0.04056,15.88,12.98,0.1094,0.0...|
#|[4.304,0.03282,0.001262,279.3,0.002383,0.08844,0.07417,10.88,7.721,0.04463,0....|
#|[2.724,0.02075,0.001779,203.1,0.001305,0.0453,0.05082,9.718,5.263,0.02806,8.9...|
#|[15.66,0.09097,5.162E-4,178.1,0.001069,0.03922,0.08063,33.94,9.158,0.05251,2....|
#|[2.161,0.01658,0.003491,239.0,0.0018,0.05721,0.04387,7.207,3.613,0.02298,0.00...|
#|[8.862,0.07809,0.005187,218.2,0.003705,0.1118,0.05035,23.79,7.212,0.05154,0.0...|
#|[17.28,0.1152,0.01479,233.2,0.004955,0.1585,0.04368,36.05,13.94,0.09003,0.009...|
#+--------------------------------------------------------------------------------+


# -----------------------------------------------------------------------------
# Calculate correlations and determine which columns we want to keep
# get correlation matrix
correlations = Correlation.corr(features, 'Features', 'pearson').collect()[0][0].toArray()    
correlations.shape      # (16, 16)
correlations

# highly correlated features  
threshold = 0.85   # 0.7, still a lot highly correlated feature-groups. use 0.85 (0.85 the same as 0.8)
print((correlations > threshold).astype(int))
#[[1 1 0 0 0 0 0 1 0 0 0 0 0 0 0 0]
# [1 1 0 0 0 0 0 1 0 0 0 0 0 0 0 0]
# [0 0 1 0 1 1 0 0 0 0 1 0 0 0 0 0]
# [0 0 0 1 0 0 0 0 0 0 0 0 0 0 0 0]
# [0 0 1 0 1 1 0 0 0 0 0 0 0 0 0 0]
# [0 0 1 0 1 1 0 0 0 0 0 0 0 0 0 0]
# [0 0 0 0 0 0 1 0 0 0 0 0 0 0 0 0]
# [1 1 0 0 0 0 0 1 0 0 0 0 0 0 0 0]
# [0 0 0 0 0 0 0 0 1 1 0 0 0 0 0 1]
# [0 0 0 0 0 0 0 0 1 1 0 0 0 0 0 1]
# [0 0 1 0 0 0 0 0 0 0 1 0 1 1 0 0]
# [0 0 0 0 0 0 0 0 0 0 0 1 0 0 0 0]
# [0 0 0 0 0 0 0 0 0 0 1 0 1 1 0 0]
# [0 0 0 0 0 0 0 0 0 0 1 0 1 1 0 0]
# [0 0 0 0 0 0 0 0 0 0 0 0 0 0 1 0]
# [0 0 0 0 0 0 0 0 1 1 0 0 0 0 0 1]]

# Remove one of each pair of correlated variables iteratively 
def get_indexes(matrix, threshold):
    n = matrix.shape[0]
    counter = 0
    indexes = np.array(list(range(0, n)))
    for j in range(0, n):
      mask = matrix > threshold
      sums = mask.sum(axis=0)
      index = np.argmax(sums)
      value = sums[index]
      check = value > 1                               #index col/row value >1
      if check:
        k = matrix.shape[0]
        keep = [i for i in range(0, k) if i != index] #keep indexs col/row value =1   
        matrix = matrix[keep, :][:, keep]
        indexes = indexes[keep]
      else:
        break
      counter += 1
    return indexes

indexes = get_indexes(correlations, threshold)      #keep the features below the threshold
indexes   #array([ 3,  5,  6,  7, 11, 13, 14, 15])
correlations_new = correlations[indexes, :][:, indexes]
print((correlations_new > threshold).astype(int))
#[[1 0 0 0 0 0 0 0]
# [0 1 0 0 0 0 0 0]
# [0 0 1 0 0 0 0 0]
# [0 0 0 1 0 0 0 0]
# [0 0 0 0 1 0 0 0]
# [0 0 0 0 0 1 0 0]
# [0 0 0 0 0 0 1 0]
# [0 0 0 0 0 0 0 1]]

print(correlations_new.shape)        # (8,8), only kept half cols 

# -----------------------------------------------------------------------------
# Look at the keep cols and drop cols 
inputCols = np.array(numaudio.columns)[indexes]
dropCols = [c for c in numaudio.columns if c not in inputCols]
inputCols
#array(['Compactness_Overall_Standard_Deviation_1',
#       'Root_Mean_Square_Overall_Standard_Deviation_1',
#       'Fraction_Of_Low_Energy_Windows_Overall_Standard_Deviation_1',
#       'Zero_Crossings_Overall_Standard_Deviation_1',
#       'Compactness_Overall_Average_1',
#       'Root_Mean_Square_Overall_Average_1',
#       'Fraction_Of_Low_Energy_Windows_Overall_Average_1',
#       'Zero_Crossings_Overall_Average_1'], dtype='<U59')

dropCols    
#['Spectral_Centroid_Overall_Standard_Deviation_1',
# 'Spectral_Rolloff_Point_Overall_Standard_Deviation_1',
# 'Spectral_Flux_Overall_Standard_Deviation_1',
# 'Spectral_Variability_Overall_Standard_Deviation_1',
# 'Spectral_Centroid_Overall_Average_1',
# 'Spectral_Rolloff_Point_Overall_Average_1',
# 'Spectral_Flux_Overall_Average_1',
# 'Spectral_Variability_Overall_Average_1']


## (b)
# -----------------------------------------------------------------------------
# load MAGD data
MAGD_schema = StructType([
  StructField("track_id", StringType(), True),
  StructField("genre_label", StringType(), True)
])

MAGD = spark.read.csv("hdfs:///data/msd/genre/msd-MAGD-genreAssignment.tsv", sep=r'\t',schema=MAGD_schema)
MAGD.show(5,)
#+------------------+--------------+
#|          track_id|   genre_label|
#+------------------+--------------+
#|TRAAAAK128F9318786|      Pop_Rock|
#|TRAAAAV128F421A322|      Pop_Rock|
#|TRAAAAW128F429D538|           Rap|
#|TRAAABD128F429CF47|      Pop_Rock|
#|TRAAACV128F423E09E|      Pop_Rock|
#+------------------+--------------+

MAGD_match = MAGD.join(mismatches_not_accepted, on='track_id', how='left_anti')
MAGD.count()           # 422714
MAGD_match.count()     # 415350


# -----------------------------------------------------------------------------
# the distribution of genres
genre_count = (
    MAGD_match.groupby("genre_label")
    .count()
    .orderBy("count", ascending = False)
)
genre_count.cache()
genre_count.count()     # 21
genre_count.show()
# +--------------+------+
# |   genre_label| count|
# +--------------+------+
# |      Pop_Rock|234107|
# |    Electronic| 40430|
# |           Rap| 20606|
# |          Jazz| 17673|
# |         Latin| 17475|
# | International| 14094|
# |           RnB| 13874|
# |       Country| 11492|
# |     Religious|  8754|
# |        Reggae|  6885|
# |         Blues|  6776|
# |         Vocal|  6076|
# |          Folk|  5777|
# |       New Age|  3935|
# | Comedy_Spoken|  2051|
# |        Stage |  1604|
# |Easy_Listening|  1533|
# |   Avant_Garde|  1000|
# |     Classical|   542|
# |      Children|   468|
# +--------------+------+

# -----------------------------------------------------------------------------
# plot genre distribution for matched songs 
import pandas as pd
import os 
os.environ['QT_QPA_PLATFORM']='offscreen'
import matplotlib.pyplot as plt

genres = genre_count.toPandas()        # data frame

#genre_count.coalesce(1).write.csv('hdfs:///user/lxi12/outputs/msd/genres', mode='overwrite', header=True) 
#!hdfs dfs -copyToLocal hdfs:///user/lxi12/outputs/msd/genres/ /users/home/lxi12

f = plt.figure(figsize=(15,12), dpi=80) 

x=range(len(genres["count"]))
y=genres["count"].tolist()

plt.bar(x, y,tick_label=genres["genre_label"])
for a,b in zip(x,y):
    plt.text(a, b+0.05, '%.0f' % b, ha='center', va= 'bottom',fontsize=7)
    
plt.title('Distribution of Genres for Matched Songs') 
plt.xticks(rotation=45)
plt.xlabel('Genre')
plt.ylabel('Genre Count')

plt.tight_layout()
f.savefig(os.path.join(os.path.expanduser("~/genre_dist.png")), bbox_inches="tight")     # save as png and view in windows
plt.close(f) 


## (c)
# -----------------------------------------------------------------------------
# Merge genres dataset and the audio features dataset
temp = (
    audio
    .withColumn("MSD_TRACKID",regexp_replace("MSD_TRACKID","'",""))
    .withColumnRenamed("MSD_TRACKID","track_id")
)

audio_label = MAGD_match.join(temp, on='track_id', how='inner')

audio_label.cache() 
audio_label.count()         # 413289

print(pretty(audio_label.head().asDict()))
#{
#  'Compactness_Overall_Average_1': 1626.0,
#  'Compactness_Overall_Standard_Deviation_1': 172.5,
#  'Fraction_Of_Low_Energy_Windows_Overall_Average_1': 0.5304,
#  'Fraction_Of_Low_Energy_Windows_Overall_Standard_Deviation_1': 0.0558,
#  'Root_Mean_Square_Overall_Average_1': 0.1126,
#  'Root_Mean_Square_Overall_Standard_Deviation_1': 0.05665,
#  'Spectral_Centroid_Overall_Average_1': 9.114,
#  'Spectral_Centroid_Overall_Standard_Deviation_1': 6.229,
#  'Spectral_Flux_Overall_Average_1': 0.00109,
#  'Spectral_Flux_Overall_Standard_Deviation_1': 0.001519,
#  'Spectral_Rolloff_Point_Overall_Average_1': 0.06159,
#  'Spectral_Rolloff_Point_Overall_Standard_Deviation_1': 0.0534,
#  'Spectral_Variability_Overall_Average_1': 0.002902,
#  'Spectral_Variability_Overall_Standard_Deviation_1': 0.001557,
#  'Zero_Crossings_Overall_Average_1': 27.0,
#  'Zero_Crossings_Overall_Standard_Deviation_1': 16.23,
#  'genre_label': 'Pop_Rock',
#  'track_id': 'TRAAABD128F429CF47'
#}


#
## Q2
#
# (a)
import numpy as np
from datetime import datetime

from pyspark.sql import Row, DataFrame, Window
from pyspark.ml import Pipeline
from pyspark.ml.feature import PCA, StandardScaler

from pyspark.ml.classification import LogisticRegression, NaiveBayes, GBTClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator


# (b)
# -----------------------------------------------------------------------------
audio_binary = audio_label.withColumn('label', 
    when(col('genre_label')=='Electronic', 1).otherwise(0))

print(pretty(audio_binary.head().asDict()))
#{
#  'Compactness_Overall_Average_1': 1626.0,
#  'Compactness_Overall_Standard_Deviation_1': 172.5,
#  'Fraction_Of_Low_Energy_Windows_Overall_Average_1': 0.5304,
#  'Fraction_Of_Low_Energy_Windows_Overall_Standard_Deviation_1': 0.0558,
#  'Root_Mean_Square_Overall_Average_1': 0.1126,
#  'Root_Mean_Square_Overall_Standard_Deviation_1': 0.05665,
#  'Spectral_Centroid_Overall_Average_1': 9.114,
#  'Spectral_Centroid_Overall_Standard_Deviation_1': 6.229,
#  'Spectral_Flux_Overall_Average_1': 0.00109,
#  'Spectral_Flux_Overall_Standard_Deviation_1': 0.001519,
#  'Spectral_Rolloff_Point_Overall_Average_1': 0.06159,
#  'Spectral_Rolloff_Point_Overall_Standard_Deviation_1': 0.0534,
#  'Spectral_Variability_Overall_Average_1': 0.002902,
#  'Spectral_Variability_Overall_Standard_Deviation_1': 0.001557,
#  'Zero_Crossings_Overall_Average_1': 27.0,
#  'Zero_Crossings_Overall_Standard_Deviation_1': 16.23,
#  'genre_label': 'Pop_Rock',
#  'label': 0,
#  'track_id': 'TRAAABD128F429CF47'
#}

# -----------------------------------------------------------------------------
# Class balance
def print_class_balance(data, name):
  N = data.count()
  counts = data.groupBy("label").count().toPandas()
  counts["ratio"] = counts["count"] / N
  print(name)
  print(N)
  print(counts)
  print("")

print_class_balance(audio_binary, "features")

#features
#413289
#   label   count     ratio
#0      1   40026  0.096847
#1      0  373263  0.903153


# (c)
# -----------------------------------------------------------------------------
# Splitting

# drop higly correlated features 
audio_drop = audio_binary.select([c for c in audio_binary.columns if c not in dropCols])    #inputCols not include label, id, so used dropCols
audio_drop.cache()
print(pretty(audio_drop.head().asDict()))
#{
#  'Compactness_Overall_Average_1': 1626.0,
#  'Compactness_Overall_Standard_Deviation_1': 172.5,
#  'Fraction_Of_Low_Energy_Windows_Overall_Average_1': 0.5304,
#  'Fraction_Of_Low_Energy_Windows_Overall_Standard_Deviation_1': 0.0558,
#  'Root_Mean_Square_Overall_Average_1': 0.1126,
#  'Root_Mean_Square_Overall_Standard_Deviation_1': 0.05665,
#  'Zero_Crossings_Overall_Average_1': 27.0,
#  'Zero_Crossings_Overall_Standard_Deviation_1': 16.23,
#  'genre_label': 'Pop_Rock',
#  'label': 0,
#  'track_id': 'TRAAABD128F429CF47'
#}

# -----------------------------------------------------------------------------
## Assemble features
assembler = VectorAssembler(
  inputCols=[col for col in audio_drop.columns if col.endswith("_1")],
  outputCol="features"
)

features_drop = assembler.transform(audio_drop).select(["track_id", "genre_label", "features", "label"])
features_drop.cache()
features_drop.count()     # 413289
features_drop.show(5,)
#+------------------+-------------+--------------------+-----+
#|          track_id|  genre_label|            features|label|
#+------------------+-------------+--------------------+-----+
#|TRAAABD128F429CF47|     Pop_Rock|[6.229,0.0534,172...|    0|
#|TRAABPK128F424CFDB|     Pop_Rock|[8.765,0.05421,18...|    0|
#|TRAACER128F4290F96|     Pop_Rock|[6.603,0.05057,19...|    0|
#|TRAADYB128F92D7E73|         Jazz|[2.528,0.02008,24...|    0|
#|TRAAGHM128EF35CF8E|   Electronic|[5.716,0.05034,21...|    1|
#+------------------+-------------+--------------------+-----+


# -----------------------------------------------------------------------------
## Stratified Random Sampling
temp = features_drop.withColumn("track_id", monotonically_increasing_id())
training = temp.sampleBy("label", fractions={0: 0.8, 1: 0.8})
training.cache()

test = temp.join(training, on="track_id", how="left_anti")
test.cache()

training = training.drop("track_id")
test = test.drop("track_id")              # drop id here, assign random ids to it

print_class_balance(features_drop, "features")
print_class_balance(training, "training")
print_class_balance(test, "test")

#features
#413289
#   label   count     ratio
#0      1   40026  0.096847
#1      0  373263  0.903153

# training
# 330642
   # label   count     ratio
# 0      1   31954  0.096642
# 1      0  298688  0.903358

# test
# 82647
   # label  count     ratio
# 0      1   8072  0.097668
# 1      0  74575  0.902332


# -----------------------------------------------------------------------------
## DownSampling
training_downsampled = (
    training
    .withColumn("Random", rand())
    .where((col("label") != 0) | ((col("label") == 0) & (col("Random") < 2 * (40026/373263))))
)
training_downsampled.cache()

print_class_balance(training_downsampled, "training_downsampled")

# training_downsampled
# 96249
   # label  count     ratio
# 0      1  31954  0.331993
# 1      0  64295  0.668007


# (d)
# -----------------------------------------------------------------------------
## Helpers

def with_custom_prediction(predictions, threshold, probabilityCol="probability", customPredictionCol="customPrediction"):

  def apply_custom_threshold(probability, threshold):
    return int(probability[1] > threshold)

  apply_custom_threshold_udf = udf(lambda x: apply_custom_threshold(x, threshold), IntegerType())

  return predictions.withColumn(customPredictionCol, apply_custom_threshold_udf(F.col(probabilityCol)))


def print_binary_metrics(predictions, threshold=0.5, labelCol="label", predictionCol="prediction", rawPredictionCol="rawPrediction", probabilityCol="probability"):

  if threshold != 0.5:

    predictions = with_custom_prediction(predictions, threshold)
    predictionCol = "customPrediction"

  total = predictions.count()
  positive = predictions.filter((col(labelCol) == 1)).count()
  negative = predictions.filter((col(labelCol) == 0)).count()
  nP = predictions.filter((col(predictionCol) == 1)).count()
  nN = predictions.filter((col(predictionCol) == 0)).count()
  TP = predictions.filter((col(predictionCol) == 1) & (col(labelCol) == 1)).count()
  FP = predictions.filter((col(predictionCol) == 1) & (col(labelCol) == 0)).count()
  FN = predictions.filter((col(predictionCol) == 0) & (col(labelCol) == 1)).count()
  TN = predictions.filter((col(predictionCol) == 0) & (col(labelCol) == 0)).count()

  binary_evaluator = BinaryClassificationEvaluator(rawPredictionCol=rawPredictionCol, labelCol=labelCol, metricName="areaUnderROC")
  auroc = binary_evaluator.evaluate(predictions)

  print('actual total:    {}'.format(total))
  print('actual positive: {}'.format(positive))
  print('actual negative: {}'.format(negative))
  print('threshold:       {}'.format(threshold))
  print('nP:              {}'.format(nP))
  print('nN:              {}'.format(nN))
  print('TP:              {}'.format(TP))
  print('FP:              {}'.format(FP))
  print('FN:              {}'.format(FN))
  print('TN:              {}'.format(TN))
  print('precision:       {}'.format(TP / (TP + FP)))
  print('recall:          {}'.format(TP / (TP + FN)))
  print('accuracy:        {}'.format((TP + TN) / total))
  print('auroc:           {}'.format(auroc))
  
## ----- Check stuff is cached
features_drop.cache()
training.cache()
training_downsampled.cache()
test.cache()

# -----------------------------------------------------------------------------
# Logistic Regression 
## ----- using training dataset -----
t1 = datetime.now()
lr = LogisticRegression(featuresCol='features', labelCol='label')
lr_model = lr.fit(training)

lr_pred = lr_model.transform(test)
lr_pred.cache()

print_binary_metrics(lr_pred)
t2 = datetime.now()
print('time:            {}.seconds'.format(t2-t1))

# actual total:    82647
# actual positive: 8072
# actual negative: 74575
# threshold:       0.5
# nP:              887
# nN:              81760
# TP:              499
# FP:              388
# FN:              7573
# TN:              74187
# precision:       0.5625704622322435
# recall:          0.061818632309217046
# accuracy:        0.9036746645371277
# auroc:           0.7555798658536463
# time:            0:00:18.893302.seconds

## ----- using training_downsampled dataset -----
t1 = datetime.now()
lr = LogisticRegression(featuresCol='features', labelCol='label')
lr_model = lr.fit(training_downsampled)

lr_predictions = lr_model.transform(test)
lr_predictions.cache()

print_binary_metrics(lr_predictions)
t2 = datetime.now()
print('time:            {}.seconds'.format(t2-t1))

# actual total:    82647
# actual positive: 8072
# actual negative: 74575
# threshold:       0.5
# nP:              9721
# nN:              72926
# TP:              3297
# FP:              6424
# FN:              4775
# TN:              68151
# precision:       0.33916263758872545
# recall:          0.40844895936570863
# accuracy:        0.8644959889651167
# auroc:           0.7592896158841297
# time:            0:00:13.138690.seconds


# -----------------------------------------------------------------------------
## Naive Bayes
## ----- using training dataset -----
t1 = datetime.now()
nb = NaiveBayes(smoothing=1, featuresCol='features', labelCol='label')
nb_model = nb.fit(training)

nb_pred = nb_model.transform(test)
nb_pred.cache()

print_binary_metrics(nb_pred)
t2 = datetime.now()
print('time:            {}.seconds'.format(t2-t1))

# actual total:    82647
# actual positive: 8072
# actual negative: 74575
# threshold:       0.5
# nP:              9255
# nN:              73392
# TP:              1784
# FP:              7471
# FN:              6288
# TN:              67104
# precision:       0.19276066990815774
# recall:          0.22101090188305253
# accuracy:        0.8335208779508028
# auroc:           0.45405867723508875
# time:            0:00:03.718247.seconds

## ----- using training_downsampled dataset -----
t1 = datetime.now()
nb = NaiveBayes(smoothing=1, featuresCol='features', labelCol='label')
nb_model = nb.fit(training_downsampled)

nb_predictions = nb_model.transform(test)
nb_predictions.cache()

print_binary_metrics(nb_predictions)
t2 = datetime.now()
print('time:            {}.seconds'.format(t2-t1))

# actual total:    82647
# actual positive: 8072
# actual negative: 74575
# threshold:       0.5
# nP:              19946
# nN:              62701
# TP:              3378
# FP:              16568
# FN:              4694
# TN:              58007
# precision:       0.16935726461445905
# recall:          0.4184836471754212
# accuracy:        0.7427371834428352
# auroc:           0.454072899718823
# time:            0:00:03.325415.seconds


# -----------------------------------------------------------------------------
## Gradient-Boosted Tree Classifier, time consuming 
## ----- using training dataset -----
t1=datetime.now()
gbt = GBTClassifier(featuresCol='features', labelCol='label')
gbt_model = gbt.fit(training)

gbt_pred = gbt_model.transform(test)
gbt_pred.cache()


print_binary_metrics(gbt_pred)
t2 = datetime.now()
print('time:            {}.seconds'.format(t2-t1))

# actual total:    82647
# actual positive: 8072
# actual negative: 74575
# threshold:       0.5
# nP:              1546
# nN:              81101
# TP:              950
# FP:              596
# FN:              7122
# TN:              73979
# precision:       0.6144890038809832
# recall:          0.11769078295341923
# accuracy:        0.9066148801529396
# auroc:           0.8077716018455424
# time:            0:00:21.423139.seconds

## ----- using training_downsampled dataset -----
t1=datetime.now()
gbt = GBTClassifier(featuresCol='features', labelCol='label')
gbt_model = gbt.fit(training_downsampled)

gbt_predictions = gbt_model.transform(test)
gbt_predictions.cache()

print_binary_metrics(gbt_predictions)
t2 = datetime.now()
print('time:            {}.seconds'.format(t2-t1))

# actual total:    82647
# actual positive: 8072
# actual negative: 74575
# threshold:       0.5
# nP:              12609
# nN:              70038
# TP:              4209
# FP:              8400
# FN:              3863
# TN:              66175
# precision:       0.3338091839162503
# recall:          0.5214321110009911
# accuracy:        0.8516219584497925
# auroc:           0.8085893319494314
# time:            0:00:17.301989.seconds
## Comparing the AUROC, NB is the worst, logistic regression is slightly better than the GBT with high explainability ?.


#
## Q3
# 
model_list = ['LogisticRegression', 'GradientBoostedTrees']

# maxIter=100, regParam=0.0, elasticNetParam=0.0, tol=1e-06, fitIntercept=True, threshold=0.5,  aggregationDepth=2
lr = LogisticRegression(featuresCol='features', labelCol='label') 

# maxDepth=5, maxBins=32, minInstancesPerNode=1, minInfoGain=0.0, maxMemoryInMB=256, 
# cacheNodeIds=False, checkpointInterval=10, numTrees=20,
gbt = GBTClassifier(featuresCol='features', labelCol='label') 

estimator_list = [lr, gbt]

lr_paramGrid = (ParamGridBuilder()
             .addGrid(lr.regParam, [0.0, 0.01, 0.1])    # regularization parameter
             .addGrid(lr.elasticNetParam, [0.0, 0.5, 1.0])      # ElasticNet parameter
             .build())
           
gbt_paramGrid = (ParamGridBuilder()
             .addGrid(gbt.maxDepth, [2, 5, 8])
             .addGrid(gbt.maxBins, [20, 40])
             .addGrid(gbt.stepSize, [0.01, 0.1])
             .build())

paramGrids = [lr_paramGrid, gbt_paramGrid]

binary_evaluator = BinaryClassificationEvaluator(metricName="areaUnderROC")    #default auroc

# run cv and check the performance 

#evaluator=areaUnderROC
finalModel_list = []    
for i in range(len(estimator_list)):
    cv = CrossValidator(
                    estimator=estimator_list[i],
                    estimatorParamMaps=paramGrids[i],
                    evaluator=binary_evaluator,   
                    numFolds=5,seed=1000, parallelism=8)
    cv_model = cv.fit(training_downsampled)    
    cv_prediction = cv_model.transform(test) 
    finalModel_list.append(cv_model.bestModel)       #save the best model
    
    print("------Cross-Validation performance of {}:--------".format(model_list[i]))
    print_binary_metrics(cv_prediction,labelCol="label")                                 
    print("--------------------------------------------------------------")             

# ------Cross-Validation performance of LogisticRegression:--------
# actual total:    82647
# actual positive: 8072
# actual negative: 74575
# threshold:       0.5
# nP:              9721
# nN:              72926
# TP:              3297
# FP:              6424
# FN:              4775
# TN:              68151
# precision:       0.33916263758872545
# recall:          0.40844895936570863
# accuracy:        0.8644959889651167
# auroc:           0.7592946385646845
# --------------------------------------------------------------

# ------Cross-Validation performance of GradientBoostedTrees:--------
# actual total:    82647
# actual positive: 8072
# actual negative: 74575
# threshold:       0.5
# nP:              13748
# nN:              68899
# TP:              4492
# FP:              9256
# FN:              3580
# TN:              65319
# precision:       0.32673843468140823
# recall:          0.5564915758176412
# accuracy:        0.8446888574297917
# auroc:           0.8158333313952503
# --------------------------------------------------------------


#
## Q4
#
# (a) Choose Logistic Regression model 

# (b) 
# -----------------------------------------------------------------------------
# convert genre column into an integer index
from pyspark.ml.feature import StringIndexer

label_stringIdx = StringIndexer(inputCol='genre_label', outputCol='label')
audio_multi = label_stringIdx.fit(audio_label).transform(audio_label)

#drop highly correlated features
audio_multi = audio_multi.select([c for c in audio_multi.columns if c not in dropCols]) 
     
assembler = VectorAssembler(
  inputCols=[col for col in audio_multi.columns if col.endswith("_1")],
  outputCol="features"
)

features_multi = assembler.transform(audio_multi).select(["track_id", "genre_label", "features", "label"])
features_multi.cache()

features_multi.show(5,)
# +------------------+-----------+--------------------+-----+
# |          track_id|genre_label|            features|label|
# +------------------+-----------+--------------------+-----+
# |TRAAABD128F429CF47|   Pop_Rock|[172.5,0.05665,0....|  0.0|
# |TRAABPK128F424CFDB|   Pop_Rock|[189.4,0.07297,0....|  0.0|
# |TRAACER128F4290F96|   Pop_Rock|[190.6,0.09102,0....|  0.0|
# |TRAADYB128F92D7E73|       Jazz|[247.3,0.07965,0....|  3.0|
# |TRAAGHM128EF35CF8E| Electronic|[219.6,0.07639,0....|  1.0|
# +------------------+-----------+--------------------+-----+

# label is assigned by the genre_label count 

# (c)
from pyspark.mllib.evaluation import MulticlassMetrics
# -----------------------------------------------------------------------------
# class balance
print_class_balance(features_multi, "features_multi")

#features_multi
#413289
#    label   count     ratio
#0    12.0    5701  0.013794
#1     6.0   13854  0.033521
#2    13.0    3925  0.009497
#3     1.0   40026  0.096847
#4    10.0    6741  0.016311
#5    19.0     457  0.001106
#6    16.0    1523  0.003685
#7     5.0   14047  0.033988
#8     7.0   11409  0.027605
#9     4.0   17389  0.042075
#10   17.0     998  0.002415
#11   20.0     198  0.000479
#12    8.0    8720  0.021099
#13    9.0    6870  0.016623
#14   14.0    2051  0.004963
#15    0.0  232995  0.563758
#16   18.0     541  0.001309
#17    3.0   17611  0.042612
#18    2.0   20566  0.049762
#19   11.0    6064  0.014673
#20   15.0    1603  0.003879


# -----------------------------------------------------------------------------
# Split 
# ----- unbalance class -----
training_multi, test_multi = features_multi.randomSplit([0.7, 0.3])
training_multi.cache()
test_multi.cache()

print_class_balance(training_multi, "traning_multi")    
#traning_multi
#289184
#    label   count     ratio
#0    12.0    3980  0.013763
#1     6.0    9563  0.033069
#2    13.0    2733  0.009451
#3     1.0   27987  0.096779
#4    10.0    4727  0.016346
#5    19.0     325  0.001124
#6    16.0    1069  0.003697
#7     5.0    9836  0.034013
#8     7.0    7893  0.027294
#9     4.0   12157  0.042039
#10   17.0     687  0.002376
#11   20.0     137  0.000474
#12    8.0    6116  0.021149
#13    9.0    4820  0.016668
#14   14.0    1431  0.004948
#15    0.0  163303  0.564703
#16   18.0     389  0.001345
#17    3.0   12285  0.042482
#18   11.0    4217  0.014582
#19    2.0   14433  0.049909
#20   15.0    1096  0.003790

# label0, 56% ratio

print_class_balance(test_multi, "test_multi")
#test_multi
#124105
#    label  count     ratio
#0    12.0   1721  0.013867
#1     6.0   4291  0.034576
#2    13.0   1192  0.009605
#3     1.0  12039  0.097007
#4    10.0   2014  0.016228
#5    16.0    454  0.003658
#6    19.0    132  0.001064
#7     5.0   4211  0.033931
#8     7.0   3516  0.028331
#9     4.0   5232  0.042158
#10   17.0    311  0.002506
#11   20.0     61  0.000492
#12    9.0   2050  0.016518
#13    8.0   2604  0.020982
#14   14.0    620  0.004996
#15    0.0  69692  0.561557
#16   18.0    152  0.001225
#17    3.0   5326  0.042915
#18    2.0   6133  0.049418
#19   11.0   1847  0.014883
#20   15.0    507  0.004085


def print_multi_metrics(model,test):
    predictions = model.transform(test)

    predictionAndLabels = predictions.select(["prediction","label"])

    metrics = MulticlassMetrics(predictionAndLabels.rdd)

    print('accuracy:', metrics.accuracy)
    print('weightedPrecision:', metrics.weightedPrecision)
    print('weightedRecall:', metrics.weightedRecall)
    print('WeightedF1:', metrics.weightedFMeasure())
    
    balance = test.groupBy('label').count()
    for i in balance.select('label').rdd.map(lambda row : row[0]).collect():
    	print(f'Recall for class {i} is {metrics.recall(i)}, Precision for class {i} is {metrics.precision(i)}') 
        
        
# Logistic Regression
lr = LogisticRegression(featuresCol='features', labelCol='label')
lr_model = lr.fit(training_multi) 
print_multi_metrics(lr_model, test_multi)

# accuracy: 0.5710164779823537
# weightedPrecision: 0.4198636665126123
# weightedRecall: 0.5710164779823537
# WeightedF1: 0.4500112077057333
# Recall for class 6.0 is 0.0016313213703099511, Precision for class 6.0 is 0.07291666666666667
# Recall for class 13.0 is 0.057885906040268456, Precision for class 13.0 is 0.26037735849056604
# Recall for class 12.0 is 0.0005810575246949448, Precision for class 12.0 is 0.16666666666666666
# Recall for class 1.0 is 0.1694492898081236, Precision for class 1.0 is 0.3726027397260274
# Recall for class 10.0 is 0.0, Precision for class 10.0 is 0.0
# Recall for class 16.0 is 0.0, Precision for class 16.0 is 0.0
# Recall for class 19.0 is 0.0, Precision for class 19.0 is 0.0
# Recall for class 5.0 is 0.002849679411066255, Precision for class 5.0 is 0.21052631578947367
# Recall for class 7.0 is 0.0002844141069397042, Precision for class 7.0 is 0.03333333333333333
# Recall for class 4.0 is 0.002484709480122324, Precision for class 4.0 is 0.16883116883116883
# Recall for class 17.0 is 0.0, Precision for class 17.0 is 0.0
# Recall for class 20.0 is 0.0, Precision for class 20.0 is 0.0
# Recall for class 9.0 is 0.007804878048780488, Precision for class 9.0 is 0.1553398058252427
# Recall for class 8.0 is 0.0, Precision for class 8.0 is 0.0
# Recall for class 14.0 is 0.0016129032258064516, Precision for class 14.0 is 0.029411764705882353
# Recall for class 0.0 is 0.9695947884979624, Precision for class 0.0 is 0.5917023493664679
# Recall for class 18.0 is 0.0, Precision for class 18.0 is 0.0
# Recall for class 3.0 is 0.02853924145700338, Precision for class 3.0 is 0.18811881188118812
# Recall for class 11.0 is 0.005414185165132647, Precision for class 11.0 is 0.07092198581560284
# Recall for class 2.0 is 0.15832382194684494, Precision for class 2.0 is 0.3453058321479374
# Recall for class 15.0 is 0.0, Precision for class 15.0 is 0.0


## ----- Balance --> observation weight -----
label_weight = features_multi.groupBy('label').count()            

label_weight=label_weight.withColumn('weight', 100000/col('count'))
label_weight.show()
#+-----+------+-------------------+
#|label| count|             weight|
#+-----+------+-------------------+
#| 12.0|  5701| 17.540782318891424|
#|  6.0| 13854|  7.218131947451999|
#| 13.0|  3925| 25.477707006369428|
#|  1.0| 40026| 2.4983760555638836|
#| 10.0|  6741|  14.83459427384661|
#| 19.0|   457|  218.8183807439825|
#| 16.0|  1523|  65.65988181221273|
#|  5.0| 14047|  7.118957784580338|
#|  7.0| 11409|  8.765010079761591|
#|  4.0| 17389|  5.750761975961815|
#| 17.0|   998| 100.20040080160321|
#| 20.0|   198|   505.050505050505|
#|  8.0|  8720|  11.46788990825688|
#|  9.0|  6870|  14.55604075691412|
#| 14.0|  2051|  48.75670404680643|
#|  0.0|232995|0.42919375952273653|
#| 18.0|   541| 184.84288354898337|
#|  3.0| 17611|  5.678269263528477|
#|  2.0| 20566|  4.862394242925216|
#| 11.0|  6064|  16.49076517150396|
#+-----+------+-------------------+

training_weighted = training_multi.join(label_weight, on='label', how='left')
training_weighted.show(5,)
# +-----+------------------+-----------+--------------------+-----+-----------------+
# |label|          track_id|genre_label|            features|count|           weight|
# +-----+------------------+-----------+--------------------+-----+-----------------+
# |  6.0|TRAAIET128F426F476|        RnB|[231.3,0.02319,0....|13854|7.218131947451999|
# |  6.0|TRABFQC128F4265349|        RnB|[165.4,0.06981,0....|13854|7.218131947451999|
# |  6.0|TRACCML128F424C770|        RnB|[267.8,0.05804,0....|13854|7.218131947451999|
# |  6.0|TRACVSY128F146C0A5|        RnB|[203.0,0.05799,0....|13854|7.218131947451999|
# |  6.0|TRADRAH128F14A2646|        RnB|[218.8,0.08512,0....|13854|7.218131947451999|
# +-----+------------------+-----------+--------------------+-----+-----------------+


# Logistic Regression
lr = LogisticRegression(featuresCol='features', labelCol='label', weightCol='weight')
lr_weight = lr.fit(training_weighted) 
print_multi_metrics(lr_weight, test_multi)

# accuracy: 0.3139116071068853
# weightedPrecision: 0.5547041654883995
# weightedRecall: 0.31391160710688537
# WeightedF1: 0.3829796526823958
# Recall for class 6.0 is 0.09787928221859707, Precision for class 6.0 is 0.09829159840861222
# Recall for class 13.0 is 0.3271812080536913, Precision for class 13.0 is 0.07406000759589822
# Recall for class 12.0 is 0.13596746077861707, Precision for class 12.0 is 0.05464736104624007
# Recall for class 1.0 is 0.1790846415815267, Precision for class 1.0 is 0.25284390758766273
# Recall for class 10.0 is 0.07994041708043693, Precision for class 10.0 is 0.06531440162271805
# Recall for class 16.0 is 0.06167400881057269, Precision for class 16.0 is 0.018134715025906734
# Recall for class 19.0 is 0.1893939393939394, Precision for class 19.0 is 0.007807620237351655
# Recall for class 5.0 is 0.03775825219662788, Precision for class 5.0 is 0.09233449477351917
# Recall for class 7.0 is 0.07935153583617748, Precision for class 7.0 is 0.08608454180808392
# Recall for class 4.0 is 0.11735474006116207, Precision for class 4.0 is 0.1503059975520196
# Recall for class 17.0 is 0.10289389067524116, Precision for class 17.0 is 0.0055181927918606655
# Recall for class 20.0 is 0.22950819672131148, Precision for class 20.0 is 0.0026400150858004903
# Recall for class 8.0 is 0.08333333333333333, Precision for class 8.0 is 0.042193272409099744
# Recall for class 9.0 is 0.36878048780487804, Precision for class 9.0 is 0.11570247933884298
# Recall for class 14.0 is 0.6451612903225806, Precision for class 14.0 is 0.07125044531528323
# Recall for class 0.0 is 0.42055042185616714, Precision for class 0.0 is 0.8689039755714328
# Recall for class 18.0 is 0.21710526315789475, Precision for class 18.0 is 0.009329940627650551
# Recall for class 3.0 is 0.053135561396920765, Precision for class 3.0 is 0.13355356300141577
# Recall for class 11.0 is 0.24038982133188955, Precision for class 11.0 is 0.10109289617486339
# Recall for class 2.0 is 0.4725256807435187, Precision for class 2.0 is 0.27851994233541566
# Recall for class 15.0 is 0.20907297830374755, Precision for class 15.0 is 0.03628894214310168

# each class has a recall and precision. but the overall weighted measures have dropped 


## ----- DownSampling ----- 
# just downsampling the majority class 
training_multidownsampled = (
    training_multi
    .withColumn("Random", rand())
    .where((col("label") != 0) | ((col("label") == 0) & (col("Random") < 0.2 )))     #289184= traning_multi.count()
)
training_multidownsampled.cache()

print_class_balance(training_multidownsampled, "training_multidownsampled")
# training_multidownsampled
# 158689
    # label  count     ratio
# 0     6.0   9563  0.060263
# 1    13.0   2733  0.017222
# 2    12.0   3980  0.025081
# 3     1.0  27987  0.176364
# 4    10.0   4727  0.029788
# 5    16.0   1069  0.006736
# 6    19.0    325  0.002048
# 7     5.0   9836  0.061983
# 8     7.0   7893  0.049739
# 9     4.0  12157  0.076609
# 10   17.0    687  0.004329
# 11   20.0    137  0.000863
# 12    9.0   4820  0.030374
# 13    8.0   6116  0.038541
# 14   14.0   1431  0.009018
# 15    0.0  32808  0.206744
# 16   18.0    389  0.002451
# 17    3.0  12285  0.077416
# 18    2.0  14433  0.090951
# 19   11.0   4217  0.026574
# 20   15.0   1096  0.006907


# Logistic Regression
lr = LogisticRegression(featuresCol='features', labelCol='label')
lr_downsample = lr.fit(training_multidownsampled) 
print_multi_metrics(lr_downsample, test_multi)

# accuracy: 0.48280891180854923
# weightedPrecision: 0.4771847225414624
# weightedRecall: 0.4828089118085492
# WeightedF1: 0.4674535298010301
# Recall for class 12.0 is 0.0017431725740848344, Precision for class 12.0 is 0.05357142857142857
# Recall for class 6.0 is 0.04474481472850152, Precision for class 6.0 is 0.1021820117083555
# Recall for class 13.0 is 0.1401006711409396, Precision for class 13.0 is 0.21887287024901703
# Recall for class 1.0 is 0.4998754049339646, Precision for class 1.0 is 0.20960607432691303
# Recall for class 10.0 is 0.0, Precision for class 10.0 is 0.0
# Recall for class 16.0 is 0.0, Precision for class 16.0 is 0.0
# Recall for class 19.0 is 0.0, Precision for class 19.0 is 0.0
# Recall for class 5.0 is 0.007361671811921159, Precision for class 5.0 is 0.08587257617728532
# Recall for class 7.0 is 0.034698521046643914, Precision for class 7.0 is 0.10099337748344371
# Recall for class 4.0 is 0.11964831804281345, Precision for class 4.0 is 0.20694214876033057
# Recall for class 17.0 is 0.0, Precision for class 17.0 is 0.0
# Recall for class 20.0 is 0.0, Precision for class 20.0 is 0.0
# Recall for class 9.0 is 0.026341463414634145, Precision for class 9.0 is 0.13602015113350127
# Recall for class 8.0 is 0.0, Precision for class 8.0 is 0.0
# Recall for class 14.0 is 0.014516129032258065, Precision for class 14.0 is 0.05172413793103448
# Recall for class 0.0 is 0.7036675658612179, Precision for class 0.0 is 0.731634540788924
# Recall for class 18.0 is 0.0, Precision for class 18.0 is 0.0
# Recall for class 3.0 is 0.4173864063086744, Precision for class 3.0 is 0.15012155591572124
# Recall for class 11.0 is 0.07471575527883054, Precision for class 11.0 is 0.10883280757097792
# Recall for class 2.0 is 0.21131583238219467, Precision for class 2.0 is 0.294478527607362
# Recall for class 15.0 is 0.0, Precision for class 15.0 is 0.0


# Each class performance weighted is the best, downsampling is better than unbalanced model.
# overall measures, unbalance model is the best, downsampling is better than weighted. 
# because of unbalance, the null model prediction is 56%, unbalanced LR is slightly better 57%. Downsampling and weighted are worse than null. 

# -----------------------------------------------------------------------------
## Plot multiclass confusion matrix
import pandas as pd
import seaborn as sns
import os 
os.environ['QT_QPA_PLATFORM']='offscreen'
import matplotlib.pyplot as plt
from sklearn.metrics import confusion_matrix, classification_report 

## Using LR weight model 
lr_weight
lr_pred = lr_weight.transform(test_multi)              # RDD
pred = np.array(lr_pred.select('prediction').collect())
actual=np.array(test_multi.select('label').collect())

classification_report(actual, pred)

              # precision    recall  f1-score   support
         # 0.0       0.87      0.42      0.57     69692
         # 1.0       0.25      0.18      0.21     12039
         # 2.0       0.28      0.47      0.35      6133
         # 3.0       0.13      0.05      0.08      5326
         # 4.0       0.15      0.12      0.13      5232
         # 5.0       0.09      0.04      0.05      4211
         # 6.0       0.10      0.10      0.10      4291
         # 7.0       0.09      0.08      0.08      3516
         # 8.0       0.04      0.08      0.06      2604
         # 9.0       0.12      0.37      0.18      2050
        # 10.0       0.07      0.08      0.07      2014
        # 11.0       0.10      0.24      0.14      1847
        # 12.0       0.05      0.14      0.08      1721
        # 13.0       0.07      0.33      0.12      1192
        # 14.0       0.07      0.65      0.13       620
        # 15.0       0.04      0.21      0.06       507
        # 16.0       0.02      0.06      0.03       454
        # 17.0       0.01      0.10      0.01       311
        # 18.0       0.01      0.22      0.02       152
        # 19.0       0.01      0.19      0.01       132
        # 20.0       0.00      0.23      0.01        61
 # avg / total       0.55      0.31      0.38    124105
 
labels = np.array(genre_count.select('genre_label').collect())    #get actual genre labels 

f = plt.figure(figsize=(15,12), dpi=80)
ax=plt.subplot()
sns.heatmap(pd.DataFrame(confusion_matrix(actual, pred)), xticklabels=labels, yticklabels=labels, annot=True, fmt='d', cmap='YlGnBu', alpha=0.8, vmin=0)
ax.set_xlabel('predicted labels', fontsize= 22, fontweight='bold')
ax.set_ylabel('actual labels', fontsize= 22, fontweight='bold')
ax.set_title('Confusion Matrix of Multi-Class using class weight', fontsize= 22, fontweight='bold')
f.savefig(os.path.join(os.path.expanduser("~/MultiClass_CM_weight.png")), bbox_inches="tight")   
plt.close(f) 


## Using LR downsample model
lr_downsample
lr_pred = lr_downsample.transform(test_multi)      # RDD
pred = np.array(lr_pred.select('prediction').collect())
actual=np.array(test_multi.select('label').collect())

classification_report(actual, pred)

              # precision    recall  f1-score   support
         # 0.0       0.73      0.70      0.72     69692
         # 1.0       0.21      0.50      0.30     12039
         # 2.0       0.29      0.21      0.25      6133
         # 3.0       0.15      0.42      0.22      5326
         # 4.0       0.21      0.12      0.15      5232
         # 5.0       0.09      0.01      0.01      4211
         # 6.0       0.10      0.04      0.06      4291
         # 7.0       0.10      0.03      0.05      3516
         # 8.0       0.00      0.00      0.00      2604
         # 9.0       0.14      0.03      0.04      2050
        # 10.0       0.00      0.00      0.00      2014
        # 11.0       0.11      0.07      0.09      1847
        # 12.0       0.05      0.00      0.00      1721
        # 13.0       0.22      0.14      0.17      1192
        # 14.0       0.05      0.01      0.02       620
        # 15.0       0.00      0.00      0.00       507
        # 16.0       0.00      0.00      0.00       454
        # 17.0       0.00      0.00      0.00       311
        # 18.0       0.00      0.00      0.00       152
        # 19.0       0.00      0.00      0.00       132
        # 20.0       0.00      0.00      0.00        61
 # avg / total       0.48      0.48      0.47    124105
  
                        
f = plt.figure(figsize=(15,12), dpi=80)
ax=plt.subplot()
sns.heatmap(pd.DataFrame(confusion_matrix(actual, pred)), xticklabels=labels, yticklabels=labels, annot=True, fmt='d', cmap='YlGnBu', alpha=0.8, vmin=0)
ax.set_xlabel('predicted labels', fontsize= 22, fontweight='bold')
ax.set_ylabel('actual labels', fontsize= 22, fontweight='bold')
ax.set_title('Confusion Matrix of Multi-Class using downsampling', fontsize= 22, fontweight='bold')
f.savefig(os.path.join(os.path.expanduser("~/MultiClass_CM_downsample.png")), bbox_inches="tight")   
plt.close(f) 