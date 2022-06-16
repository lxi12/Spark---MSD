# Below code is from James code DataProcessingQ2.py  
### Processing ###
#
## Q2
#
# Python and pyspark modules required
#start_pyspark_shell -e 4 -c 2 -w 4 -m 4

import sys

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

from pretty import SparkPretty       
pretty = SparkPretty(limit=5)

# Required to allow the file to be submitted and run using spark-submit instead
# of using pyspark interactively

spark = SparkSession.builder.getOrCreate()
sc = SparkContext.getOrCreate()

## Q2 (a)

mismatches_schema = StructType([
  StructField("song_id", StringType(), True),
  StructField("song_artist", StringType(), True),
  StructField("song_title", StringType(), True),
  StructField("track_id", StringType(), True),
  StructField("track_artist", StringType(), True),
  StructField("track_title", StringType(), True) 
])

# python can only open files from local directory. if read HDFS, need to use spark.read .
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
matches_manually_accepted.show(10, 40)
#+------------------+-----------------+----------------------------------------+------------------+----------------------------------------+----------------------------------------+
#|           song_id|      song_artist|                              song_title|          track_id|                            track_artist|                             track_title|
#+------------------+-----------------+----------------------------------------+------------------+----------------------------------------+----------------------------------------+
#|SOFQHZM12A8C142342|     Josipa Lisac|                                 razloga|TRMWMFG128F92FFEF2|                            Lisac Josipa|                            1000 razloga|
#|SODXUTF12AB018A3DA|       Lutan Fyah|     Nuh Matter the Crisis Feat. Midnite|TRMWPCD12903CCE5ED|                                 Midnite|                   Nah Matter the Crisis|
#|SOASCRF12A8C1372E6|Gaetano Donizetti|L'Elisir d'Amore: Act Two: Come sen v...|TRMHIPJ128F426A2E2|Gianandrea Gavazzeni_ Orchestra E Cor...|L'Elisir D'Amore_ Act 2: Come Sen Va ...|
#|SOITDUN12A58A7AACA|     C.J. Chenier|                               Ay, Ai Ai|TRMHXGK128F42446AB|                         Clifton Chenier|                               Ay_ Ai Ai|
#|SOLZXUM12AB018BE39|           許志安|                                男人最痛|TRMRSOF12903CCF516|                                Andy Hui|                        Nan Ren Zui Tong|
#|SOTJTDT12A8C13A8A6|                S|                                       h|TRMNKQE128F427C4D8|                             Sammy Hagar|                 20th Century Man (Live)|
#|SOGCVWB12AB0184CE2|                H|                                       Y|TRMUNCZ128F932A95D|                                Hawkwind|                25 Years (Alternate Mix)|
#|SOKDKGD12AB0185E9C|     影山ヒロノブ|Cha-La Head-Cha-La (2005 ver./DRAGON ...|TRMOOAH12903CB4B29|                        Takahashi Hiroki|Maka fushigi adventure! (2005 Version...|
#|SOPPBXP12A8C141194|    Αντώνης Ρέμος|                        O Trellos - Live|TRMXJDS128F42AE7CF|                           Antonis Remos|                               O Trellos|
#|SODQSLR12A8C133A01|    John Williams|Concerto No. 1 for Guitar and String ...|TRWHMXN128F426E03C|               English Chamber Orchestra|II. Andantino siciliano from Concerto...|
#+------------------+-----------------+----------------------------------------+------------------+----------------------------------------+----------------------------------------+

print(matches_manually_accepted.count())  # 488


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
mismatches.show(10, 40)
#+------------------+-------------------+----------------------------------------+------------------+--------------+----------------------------------------+
#|           song_id|        song_artist|                              song_title|          track_id|  track_artist|                             track_title|
#+------------------+-------------------+----------------------------------------+------------------+--------------+----------------------------------------+
#|SOUMNSI12AB0182807|Digital Underground|                        The Way We Swing|TRMMGKQ128F9325E10|      Linkwood|           Whats up with the Underground|
#|SOCMRBE12AB018C546|         Jimmy Reed|The Sun Is Shining (Digitally Remaste...|TRMMREB12903CEB1B1|    Slim Harpo|               I Got Love If You Want It|
#|SOLPHZY12AC468ABA8|      Africa HiTech|                                Footstep|TRMMBOC12903CEB46E|Marcus Worgull|                 Drumstern (BONUS TRACK)|
#|SONGHTM12A8C1374EF|     Death in Vegas|                            Anita Berber|TRMMITP128F425D8D0|     Valen Hsu|                                  Shi Yi|
#|SONGXCA12A8C13E82E| Grupo Exterminador|                           El Triunfador|TRMMAYZ128F429ECE6|     I Ribelli|                               Lei M'Ama|
#|SOMBCRC12A67ADA435|      Fading Friend|                             Get us out!|TRMMNVU128EF343EED|     Masterboy|                      Feel The Heat 2000|
#|SOTDWDK12A8C13617B|       Daevid Allen|                              Past Lives|TRMMNCZ128F426FF0E| Bhimsen Joshi|            Raga - Shuddha Sarang_ Aalap|
#|SOEBURP12AB018C2FB|  Cristian Paduraru|                              Born Again|TRMMPBS12903CE90E1|     Yespiring|                          Journey Stages|
#|SOSRJHS12A6D4FDAA3|         Jeff Mills|                      Basic Human Design|TRMWMEL128F421DA68|           M&T|                           Drumsettester|
#|SOIYAAQ12A6D4F954A|           Excepter|                                      OG|TRMWHRI128F147EA8E|    The Fevers|Não Tenho Nada (Natchs Scheint Die So...|
#+------------------+-------------------+----------------------------------------+------------------+--------------+----------------------------------------+

print(mismatches.count())  # 19094


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
#+----------------------------------------+------------------+-----+
#|                                 user_id|           song_id|plays|
#+----------------------------------------+------------------+-----+
#|f1bfc2a4597a3642f232e7a4e5d5ab2a99cf80e5|SOQEFDN12AB017C52B|    1|
#|f1bfc2a4597a3642f232e7a4e5d5ab2a99cf80e5|SOQOIUJ12A6701DAA7|    2|
#|f1bfc2a4597a3642f232e7a4e5d5ab2a99cf80e5|SOQOKKD12A6701F92E|    4|
#|f1bfc2a4597a3642f232e7a4e5d5ab2a99cf80e5|SOSDVHO12AB01882C7|    1|
#|f1bfc2a4597a3642f232e7a4e5d5ab2a99cf80e5|SOSKICX12A6701F932|    1|
#|f1bfc2a4597a3642f232e7a4e5d5ab2a99cf80e5|SOSNUPV12A8C13939B|    1|
#|f1bfc2a4597a3642f232e7a4e5d5ab2a99cf80e5|SOSVMII12A6701F92D|    1|
#|f1bfc2a4597a3642f232e7a4e5d5ab2a99cf80e5|SOTUNHI12B0B80AFE2|    1|
#|f1bfc2a4597a3642f232e7a4e5d5ab2a99cf80e5|SOTXLTZ12AB017C535|    1|
#|f1bfc2a4597a3642f232e7a4e5d5ab2a99cf80e5|SOTZDDX12A6701F935|    1|
#+----------------------------------------+------------------+-----+


mismatches_not_accepted = mismatches.join(matches_manually_accepted, on="song_id", how="left_anti")
triplets_not_mismatched = triplets.join(mismatches_not_accepted, on="song_id", how="left_anti")

mismatches_not_accepted.cache()
print(mismatches_not_accepted.count())  # 19093 
print(triplets.count())                 # 48373586
print(triplets_not_mismatched.count())  # 45795111

# get mismatched unique song_ids
truemismatched=mismatches_not_accepted.select("SONG_ID").dropDuplicates()
truemismatched.count()                  # 18912 


## Q2 (b)

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
  "msd-jmir-area-of-moments-all-v1.0",
  "msd-jmir-lpc-all-v1.0",
  "msd-jmir-methods-of-moments-all-v1.0",
  "msd-jmir-mfcc-all-v1.0",
  "msd-jmir-spectral-all-all-v1.0",
  "msd-jmir-spectral-derivatives-all-all-v1.0",
  "msd-marsyas-timbral-v1.0",
  "msd-mvd-v1.0",
  "msd-rh-v1.0",
  "msd-rp-v1.0",
  "msd-ssd-v1.0",
  "msd-trh-v1.0",
  "msd-tssd-v1.0"
]

audio_dataset_schemas = {}
for audio_dataset_name in audio_dataset_names:
  print(audio_dataset_name)

  audio_dataset_path = f"/scratch-network/courses/2022/DATA420-22S1/data/msd/audio/attributes/{audio_dataset_name}.attributes.csv"
  with open(audio_dataset_path, "r") as f:
    rows = [line.strip().split(",") for line in f.readlines()]

  # you could rename feature columns with a short generic name

  # rows[-1][0] = "track_id"              # last col is track_id, others is feature_000i
  # for i, row in enumerate(rows[0:-1]):
  #   row[0] = f"feature_{i:04d}"

    audio_dataset_schemas[audio_dataset_name] = StructType([
    StructField(row[0], audio_attribute_type_mapping[row[1]], True) for row in rows ])
  
  print(audio_dataset_schemas[audio_dataset_name])
