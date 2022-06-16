### Processing ###
#
## Q1 
#
## data structure. Draw a directory tree
hdfs dfs -ls -h /data/msd      
hdfs dfs -ls -R /data/msd | awk '{print $8}' | sed -e 's/[^-][^\/]*\//--/g' -e 's/^/ /' -e 's/-/|/'
tree /scratch-network/courses/2022/DATA420-22S1/data/msd/
# /scratch-network/courses/2022/DATA420-22S1/data/msd/
# ├── audio
# │   ├── attributes
# │   │   ├── msd-jmir-area-of-moments-all-v1.0.attributes.csv
# │   │   ├── msd-jmir-lpc-all-v1.0.attributes.csv
# │   │   ├── msd-jmir-methods-of-moments-all-v1.0.attributes.csv
# │   │   ├── msd-jmir-mfcc-all-v1.0.attributes.csv
# │   │   ├── msd-jmir-spectral-all-all-v1.0.attributes.csv
# │   │   ├── msd-jmir-spectral-derivatives-all-all-v1.0.attributes.csv
# │   │   ├── msd-marsyas-timbral-v1.0.attributes.csv
# │   │   ├── msd-mvd-v1.0.attributes.csv
# │   │   ├── msd-rh-v1.0.attributes.csv
# │   │   ├── msd-rp-v1.0.attributes.csv
# │   │   ├── msd-ssd-v1.0.attributes.csv
# │   │   ├── msd-trh-v1.0.attributes.csv
# │   │   └── msd-tssd-v1.0.attributes.csv
# │   ├── features
# │   │   ├── msd-jmir-area-of-moments-all-v1.0.csv
# │   │   │   ├── part-00000.csv.gz
# │   │   │   ├── part-00001.csv.gz
# │   │   │   ├── part-00002.csv.gz
# │   │   │   ├── part-00003.csv.gz
# │   │   │   ├── part-00004.csv.gz
# │   │   │   ├── part-00005.csv.gz
# │   │   │   ├── part-00006.csv.gz
# │   │   │   └── part-00007.csv.gz
# │   │   ├── msd-jmir-lpc-all-v1.0.csv
# │   │   │   ├── part-00000.csv.gz
# │   │   │   ├── part-00001.csv.gz
# │   │   │   ├── part-00002.csv.gz
# │   │   │   ├── part-00003.csv.gz
# │   │   │   ├── part-00004.csv.gz
# │   │   │   ├── part-00005.csv.gz
# │   │   │   ├── part-00006.csv.gz
# │   │   │   └── part-00007.csv.gz
# │   │   ├── msd-jmir-methods-of-moments-all-v1.0.csv
# │   │   │   ├── part-00000.csv.gz
# │   │   │   ├── part-00001.csv.gz
# │   │   │   ├── part-00002.csv.gz
# │   │   │   ├── part-00003.csv.gz
# │   │   │   ├── part-00004.csv.gz
# │   │   │   ├── part-00005.csv.gz
# │   │   │   ├── part-00006.csv.gz
# │   │   │   └── part-00007.csv.gz
# │   │   ├── msd-jmir-mfcc-all-v1.0.csv
# │   │   │   ├── part-00000.csv.gz
# │   │   │   ├── part-00001.csv.gz
# │   │   │   ├── part-00002.csv.gz
# │   │   │   ├── part-00003.csv.gz
# │   │   │   ├── part-00004.csv.gz
# │   │   │   ├── part-00005.csv.gz
# │   │   │   ├── part-00006.csv.gz
# │   │   │   └── part-00007.csv.gz
# │   │   ├── msd-jmir-spectral-all-all-v1.0.csv
# │   │   │   ├── part-00000.csv.gz
# │   │   │   ├── part-00001.csv.gz
# │   │   │   ├── part-00002.csv.gz
# │   │   │   ├── part-00003.csv.gz
# │   │   │   ├── part-00004.csv.gz
# │   │   │   ├── part-00005.csv.gz
# │   │   │   ├── part-00006.csv.gz
# │   │   │   └── part-00007.csv.gz
# │   │   ├── msd-jmir-spectral-derivatives-all-all-v1.0.csv
# │   │   │   ├── part-00000.csv.gz
# │   │   │   ├── part-00001.csv.gz
# │   │   │   ├── part-00002.csv.gz
# │   │   │   ├── part-00003.csv.gz
# │   │   │   ├── part-00004.csv.gz
# │   │   │   ├── part-00005.csv.gz
# │   │   │   ├── part-00006.csv.gz
# │   │   │   └── part-00007.csv.gz
# │   │   ├── msd-marsyas-timbral-v1.0.csv
# │   │   │   ├── part-00000.csv.gz
# │   │   │   ├── part-00001.csv.gz
# │   │   │   ├── part-00002.csv.gz
# │   │   │   ├── part-00003.csv.gz
# │   │   │   ├── part-00004.csv.gz
# │   │   │   ├── part-00005.csv.gz
# │   │   │   ├── part-00006.csv.gz
# │   │   │   └── part-00007.csv.gz
# │   │   ├── msd-mvd-v1.0.csv
# │   │   │   ├── part-00000.csv.gz
# │   │   │   ├── part-00001.csv.gz
# │   │   │   ├── part-00002.csv.gz
# │   │   │   ├── part-00003.csv.gz
# │   │   │   ├── part-00004.csv.gz
# │   │   │   ├── part-00005.csv.gz
# │   │   │   ├── part-00006.csv.gz
# │   │   │   └── part-00007.csv.gz
# │   │   ├── msd-rh-v1.0.csv
# │   │   │   ├── part-00000.csv.gz
# │   │   │   ├── part-00001.csv.gz
# │   │   │   ├── part-00002.csv.gz
# │   │   │   ├── part-00003.csv.gz
# │   │   │   ├── part-00004.csv.gz
# │   │   │   ├── part-00005.csv.gz
# │   │   │   ├── part-00006.csv.gz
# │   │   │   └── part-00007.csv.gz
# │   │   ├── msd-rp-v1.0.csv
# │   │   │   ├── part-00000.csv.gz
# │   │   │   ├── part-00001.csv.gz
# │   │   │   ├── part-00002.csv.gz
# │   │   │   ├── part-00003.csv.gz
# │   │   │   ├── part-00004.csv.gz
# │   │   │   ├── part-00005.csv.gz
# │   │   │   ├── part-00006.csv.gz
# │   │   │   └── part-00007.csv.gz
# │   │   ├── msd-ssd-v1.0.csv
# │   │   │   ├── part-00000.csv.gz
# │   │   │   ├── part-00001.csv.gz
# │   │   │   ├── part-00002.csv.gz
# │   │   │   ├── part-00003.csv.gz
# │   │   │   ├── part-00004.csv.gz
# │   │   │   ├── part-00005.csv.gz
# │   │   │   ├── part-00006.csv.gz
# │   │   │   └── part-00007.csv.gz
# │   │   ├── msd-trh-v1.0.csv
# │   │   │   ├── part-00000.csv.gz
# │   │   │   ├── part-00001.csv.gz
# │   │   │   ├── part-00002.csv.gz
# │   │   │   ├── part-00003.csv.gz
# │   │   │   ├── part-00004.csv.gz
# │   │   │   ├── part-00005.csv.gz
# │   │   │   ├── part-00006.csv.gz
# │   │   │   └── part-00007.csv.gz
# │   │   └── msd-tssd-v1.0.csv
# │   │       ├── part-00000.csv.gz
# │   │       ├── part-00001.csv.gz
# │   │       ├── part-00002.csv.gz
# │   │       ├── part-00003.csv.gz
# │   │       ├── part-00004.csv.gz
# │   │       ├── part-00005.csv.gz
# │   │       ├── part-00006.csv.gz
# │   │       └── part-00007.csv.gz
# │   └── statistics
# │       └── sample_properties.csv.gz
# ├── genre
# │   ├── msd-MAGD-genreAssignment.tsv
# │   ├── msd-MASD-styleAssignment.tsv
# │   └── msd-topMAGD-genreAssignment.tsv
# ├── main
# │   └── summary
# │       ├── analysis.csv.gz
# │       └── metadata.csv.gz
# └── tasteprofile
    # ├── mismatches
    # │   ├── sid_matches_manually_accepted.txt
    # │   └── sid_mismatches.txt
    # └── triplets.tsv
        # ├── part-00000.tsv.gz
        # ├── part-00001.tsv.gz
        # ├── part-00002.tsv.gz
        # ├── part-00003.tsv.gz
        # ├── part-00004.tsv.gz
        # ├── part-00005.tsv.gz
        # ├── part-00006.tsv.gz
        # └── part-00007.tsv.gz

# 23 directories, 133 files

## get data size 
hdfs dfs -du -h /data/msd/

hdfs dfs -du -h /data/msd/audio
hdfs dfs -du -h /data/msd/audio/attributes
hdfs dfs -du -h /data/msd/audio/features
hdfs dfs -du -h /data/msd/audio/statistics

hdfs dfs -du -h /data/msd/genre
hdfs dfs -du -h /data/msd/main/summary

hdfs dfs -du -h /data/msd/tasteprofile
hdfs dfs -du -h /data/msd/tasteprofile/mismatches
hdfs dfs -du -h /data/msd/tasteprofile/triplets.tsv

## get data types
# audio/attributes data type
#Peek at the first 3 lines of each data file to check the data type in each file
files='hdfs dfs -ls /data/msd/audio/attributes |  awk -F " " '{print $8}''

for file in $files
do
	echo $file 
	hdfs dfs -cat $file | head -n3
done

# audio/features data type
files='hdfs dfs -ls /data/msd/audio/attributes |  awk -F " " '{print $8}''

# display the number of grouped data type in each file
for file in $files
do
	echo $file 
	hdfs dfs -cat $file | awk -F"," '{print $2}' | uniq -c
done

# genre data type
files=`hdfs dfs -ls /data/msd/genre |  awk -F " " '{print $8}'`

for file in $files
do
	echo $file 
	hdfs dfs -cat $file | head -n3
done

# main data type
hdfs dfs -cat /data/msd/main/summary/analysis.csv.gz | gunzip | head -n3
hdfs dfs -cat /data/msd/main/summary/metadata.csv.gz | gunzip | head -n3

# tasteprofile/mismatches data type
hdfs dfs -cat /data/msd/tasteprofile/mismatches/sid_matches_manually_accepted.txt | head -n5 
hdfs dfs -cat /data/msd/tasteprofile/mismatches/sid_mismatches.txt | head -n5 

# tasteprofile/triplets data type
files = 'hdfs dfs -ls /data/msd/tasteprofile/triplets.tsv/ | awk -F " " '{print $8}''

for file in $files
do
	echo $file
	hdfs dfs -cat $file | wc -lines
done


## count the number of rows in each datasets
# audio/attributes
hdfs dfs -cat /data/msd/audio/attributes/* | wc -l        # 3929

hdfs dfs -cat /data/msd/audio/attributes/msd-jmir-area-of-moments-all-v1.0.attributes.csv | wc -l           # 21
hdfs dfs -cat /data/msd/audio/attributes/msd-jmir-lpc-all-v1.0.attributes.csv | wc -l                       # 21
hdfs dfs -cat /data/msd/audio/attributes/msd-jmir-methods-of-moments-all-v1.0.attributes.csv | wc -l        # 11
hdfs dfs -cat /data/msd/audio/attributes/msd-jmir-mfcc-all-v1.0.attributes.csv | wc -l                      # 27
hdfs dfs -cat /data/msd/audio/attributes/msd-jmir-spectral-all-all-v1.0.attributes.csv | wc -l              # 17
hdfs dfs -cat /data/msd/audio/attributes/msd-jmir-spectral-derivatives-all-all-v1.0.attributes.csv | wc -l  # 17
hdfs dfs -cat /data/msd/audio/attributes/msd-marsyas-timbral-v1.0.attributes.csv | wc -l                    # 125
hdfs dfs -cat /data/msd/audio/attributes/msd-mvd-v1.0.attributes.csv | wc -l                                # 421
hdfs dfs -cat /data/msd/audio/attributes/msd-rh-v1.0.attributes.csv | wc -l                                 # 61
hdfs dfs -cat /data/msd/audio/attributes/msd-rp-v1.0.attributes.csv | wc -l                                 # 1441
hdfs dfs -cat /data/msd/audio/attributes/msd-ssd-v1.0.attributes.csv | wc -l                                # 169
hdfs dfs -cat /data/msd/audio/attributes/msd-trh-v1.0.attributes.csv | wc -l                                # 421
hdfs dfs -cat /data/msd/audio/attributes/msd-tssd-v1.0.attributes.csv | wc -l                               # 1177

# audio/features
# Get the file names in the features directory

files='hdfs dfs -ls /data/msd/audio/features |  awk -F " " '{print $8}''

# Count the number of rows in each csv
for file in $files
do
	echo $file
	hdfs dfs -cat $file/* | gunzip | wc -l
done

hdfs dfs -cat /data/msd/audio/features/msd-jmir-area-of-moments-all-v1.0.csv/part-* | gunzip | wc -l     # 994623
hdfs dfs -cat /data/msd/audio/features/msd-jmir-lpc-all-v1.0.csv/part-* | gunzip | wc -l                 # 994623
hdfs dfs -cat /data/msd/audio/features/msd-jmir-methods-of-moments-all-v1.0.csv/part-* | gunzip | wc -l  # 994623
hdfs dfs -cat /data/msd/audio/features/msd-jmir-mfcc-all-v1.0.csv/part-* | gunzip | wc -l                # 994623
hdfs dfs -cat /data/msd/audio/features/msd-jmir-spectral-all-all-v1.0.csv/part-* | gunzip | wc -l        # 994623
hdfs dfs -cat /data/msd/audio/features/msd-jmir-spectral-derivatives-all-all-v1.0.csv/part-* | gunzip | wc -l  # 994623
hdfs dfs -cat /data/msd/audio/features/msd-marsyas-timbral-v1.0.csv/part-* | gunzip | wc -l              # 995001
hdfs dfs -cat /data/msd/audio/features/msd-mvd-v1.0.csv/part-* | gunzip | wc -l                          # 994188
hdfs dfs -cat /data/msd/audio/features/msd-rh-v1.0.csv/part-* | gunzip | wc -l                           # 994188
hdfs dfs -cat /data/msd/audio/features/msd-rp-v1.0.csv/part-* | gunzip | wc -l                           # 994188
hdfs dfs -cat /data/msd/audio/features/msd-ssd-v1.0.csv/part-* | gunzip | wc -l                          # 994188
hdfs dfs -cat /data/msd/audio/features/msd-trh-v1.0.csv/part-* | gunzip | wc -l                          # 994188
hdfs dfs -cat /data/msd/audio/features/msd-tssd-v1.0.csv/part-* | gunzip | wc -l                         # 994188

# audio/attributes
hdfs dfs -cat /data/msd/audio/statistics/sample_properties.csv.gz | gunzip | wc -l   # 992866

# genren
hdfs dfs -cat /data/msd/genre/msd-MAGD-genreAssignment.tsv | wc -l            # 422714
hdfs dfs -cat /data/msd/genre/msd-MASD-styleAssignment.tsv | wc -l            # 273936
hdfs dfs -cat /data/msd/genre/msd-topMAGD-genreAssignment.tsv | wc -l         # 406427

# main
hdfs dfs -cat /data/msd/main/summary/analysis.csv.gz | gunzip | wc -l         # 1000001
hdfs dfs -cat /data/msd/main/summary/metadata.csv.gz | gunzip | wc -l         # 1000001

# tasteprofile 
hdfs dfs -cat /data/msd/tasteprofile/mismatches/sid_matches_manually_accepted.txt | wc -l   # 938
hdfs dfs -cat /data/msd/tasteprofile/mismatches/sid_mismatches.txt | wc -l                  # 19094
hdfs dfs -cat /data/msd/tasteprofile/triplets.tsv/part-* | gunzip | wc -l                   # 48373586