#/bin/bash

SPARK_HOME=~/spark-1.6.2-bin-hadoop2.7.1-dynamic
SPARK_SUBMIT=$SPARK_HOME/bin/spark-submit
ENV="--master yarn --deploy-mode client --executor-memory 4g --executor-cores 2 --queue default"
JAR="$SPARK_HOME/lib/spark-examples*.jar"

START="============================================================================="
MID="-------------------------------------------------------------------------------"
END="=============================================================================="

# 0. Submit data onto HDFS
hdfs dfs -rm -r /sparkbench
hdfs dfs -mkdir /sparkbench
hdfs dfs -mkdir /sparkbench/mllib
hdfs dfs -put $SPARK_HOME/data/mllib/lr_data.txt /sparkbench/mllib/
hdfs dfs -put $SPARK_HOME/data/mllib/kmeans_data.txt /sparkbench/mllib/
hdfs dfs -put $SPARK_HOME/data/mllib/pagerank_data.txt /sparkbench/mllib/


# 1. BroadcastTest [slices=2] [numElem=1000000] [broadcastAlgo=HTTP] [blockSize=4096]
APP="1. BroadcastTest [slices=2] [numElem=1000000] [broadcastAlgo=HTTP] [blockSize=4096]"
CLASS=org.apache.spark.examples.BroadcastTest
CMD="$SPARK_SUBMIT --class $CLASS $ENV $JAR 5 1000000 Http 4096"
echo -e "$START \n $APP \n $CMD \n $MID"
$CMD
read -p "Press any key to continue." var

CMD="$SPARK_SUBMIT --class $CLASS $ENV $JAR 5 1000000 Torrent 4096"
echo -e "$START \n $APP \n $CMD \n $MID"
$CMD
read -p "Press any key to continue." var

# 2. DFSReadWriteTest localfile dfsDirPath
APP="2. DFSReadWriteTest localfile dfsDirPath"
CLASS=org.apache.spark.examples.DFSReadWriteTest
hdfs dfs -rm -r /spark_dfs_read_write_test
CMD="$SPARK_SUBMIT --class $CLASS $ENV $JAR $SPARK_HOME/conf/spark-env.sh /spark_dfs_read_write_test"
echo -e "$START \n $APP \n $CMD \n $MID"
$CMD
read -p "Press any key to continue." var

# 3. GroupByTest [numMappers] [numKVPairs] [KeySize] [numReducers]
APP="3. GroupByTest [numMappers] [numKVPairs] [KeySize] [numReducers]"
CLASS=org.apache.spark.examples.GroupByTest
CMD="$SPARK_SUBMIT --class $CLASS $ENV $JAR 20 10000 1000 1000"
echo -e "$START \n $APP \n $CMD \n $MID"
$CMD
read -p "Press any key to continue." var

# 4. MultipleBroadcastTest
APP="4. MultipleBroadcastTest"
CLASS=org.apache.spark.examples.MultiBroadcastTest
CMD="$SPARK_SUBMIT --class $CLASS $ENV $JAR 5 1000000"
echo -e "$START \n $APP \n $CMD \n $MID"
$CMD
read -p "Press any key to continue." var

# 5. SimpleSkewedGroupByTest [numMappers] [numKVPairs] [valSize] [numReducers] [ratio]
APP="5. SimpleSkewedGroupByTest [numMappers] [numKVPairs] [valSize] [numReducers] [ratio]"
CLASS=org.apache.spark.examples.SimpleSkewedGroupByTest
CMD="$SPARK_SUBMIT --class $CLASS $ENV $JAR 20 10000 1000 8 2"
echo -e "$START \n $APP \n $CMD \n $MID"
$CMD
read -p "Press any key to continue." var

# 6. SkewedGroupByTest [numMappers] [numKVPairs] [KeySize] [numReducers]
APP="6. SkewedGroupByTest [numMappers] [numKVPairs] [KeySize] [numReducers]"
CLASS=org.apache.spark.examples.SkewedGroupByTest
CMD="$SPARK_SUBMIT --class $CLASS $ENV $JAR 20 10000 1000 8"
echo -e "$START \n $APP \n $CMD \n $MID"
$CMD
read -p "Press any key to continue." var

# 7. SparkALS [M] [U] [F] [iters] [slices]
APP="7. SparkALS [M] [U] [F] [iters] [slices]"
CLASS=org.apache.spark.examples.SparkALS
CMD="$SPARK_SUBMIT --class $CLASS $ENV $JAR 100 500 10 10 10"
echo -e "$START \n $APP \n $CMD \n $MID"
$CMD
read -p "Press any key to continue." var

# 8. SparkHdfsLR <file> <iters>
APP="8. SparkHdfsLR <file> <iters>"
CLASS=org.apache.spark.examples.SparkHdfsLR
FILE="/sparkbench/mllib/lr_data.txt"
CMD="$SPARK_SUBMIT --class $CLASS $ENV $JAR $FILE 10"
echo -e "$START \n $APP \n $CMD \n $MID"
$CMD
read -p "Press any key to continue." var

# 9. SparkKMeans <file> <k> <convergeDist>
APP="9. SparkKMeans <file> <k> <convergeDist>"
CLASS=org.apache.spark.examples.SparkKMeans
FILE="/sparkbench/mllib/kmeans_data.txt"
CMD="$SPARK_SUBMIT --class $CLASS $ENV $JAR $FILE 2 0.01"
echo -e "$START \n $APP \n $CMD \n $MID"
$CMD
read -p "Press any key to continue." var

# 10. SparkLR [slices]
APP="10. SparkLR [slices]"
CLASS=org.apache.spark.examples.SparkLR
CMD="$SPARK_SUBMIT --class $CLASS $ENV $JAR 4"
echo -e "$START \n $APP \n $CMD \n $MID"
$CMD
read -p "Press any key to continue." var

# 11. SparkPageRank <file> <iter>
APP="11. SparkPageRank <file> <iter>"
CLASS=org.apache.spark.examples.SparkPageRank
FILE="/sparkbench/mllib/pagerank_data.txt"
CMD="$SPARK_SUBMIT --class $CLASS $ENV $JAR $FILE 10"
echo -e "$START \n $APP \n $CMD \n $MID"
$CMD
read -p "Press any key to continue." var

# 12. SparkPi
APP="12. SparkPi"
CLASS=org.apache.spark.examples.SparkPi
CMD="$SPARK_SUBMIT --class $CLASS $ENV $JAR 5"
echo -e "$START \n $APP \n $CMD \n $MID"
$CMD
read -p "Press any key to continue." var

# 13. SparkTC
APP="13. SparkTC"
CLASS=org.apache.spark.examples.SparkTC
CMD="$SPARK_SUBMIT --class $CLASS $ENV $JAR 16"
echo -e "$START \n $APP \n $CMD \n $MID"
#$CMD
read -p "Press any key to continue." var

# 14. SparkTachyonHdfsLR
#APP="14. SparkTachyonHdfsLR"
#CLASS=org.apache.spark.examples.SparkTachyonHdfsLR
#FILE="/sparkbench/mllib/lr_data.txt"
#CMD="$SPARK_SUBMIT --class $CLASS $ENV $JAR $FILE 10"
#echo -e "$START \n $APP \n $CMD \n $MID"
#$CMD
#read -p "Press any key to continue." var

# 15. SparkTachyonPi
#APP="15. SparkTachyonPi"
#CLASS=org.apache.spark.examples.SparkTachyonPi
#CMD="$SPARK_SUBMIT --class $CLASS $ENV $JAR"
#echo -e "$START \n $APP \n $CMD \n $MID"
#$CMD
#read -p "Press any key to continue." var

# Delete the test data
hdfs dfs -rm -r /sparkbench
hdfs dfs -rm -r /spark_dfs_read_write_test