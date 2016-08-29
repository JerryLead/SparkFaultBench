import os

classdicts = {'Scan':'sql.Scan', 'Join':'sql.Join', 'Aggregate':'sql.Aggregate', 'Mix':'sql.Mix'}
jardicts = {'Scan':'ScanSQL', 'Join':'JoinSQL', 'Aggregate':'AggregateSQL', 'Mix':'MixSQL'}
params = {}
dirpath = os.getcwd()

f = open("%s/config.txt" %dirpath,"r")
line = f.readline().strip()
while line:
    pas = line.split("=")
    params[pas[0].strip()] = pas[1].strip()
    line = f.readline().strip()

dfs_path = params["HDFS_PATH"]
rankings_file = params["RANKINGS_FILE"]
rankings_skewed_file = params["RANKINGS_SKEWED_FILE"]
uservisit_file = params["USERVISITS_FILE"]
uservisit_skewed_file = params["USERVISITS_SKEWED_FILE"]
print rankings_file,rankings_skewed_file,uservisit_file,uservisit_skewed_file

f = open("%s/testlist.txt" %dirpath,"r")
line = f.readline().strip()
while line:
    classname = classdicts[line]
    testname = jardicts[line]
    print "start %s\n" %line
    os.system("${SPARK_HOME}/bin/spark-submit "
              "--master yarn "
              "--deploy-mode cluster "
              "--queue default "
              "--driver-memory 4g "
              "--executor-memory 2g "
              "--executor-cores 2  "
              "--class %s %s/%s.jar %s %s %s"
              %(classname,dirpath,testname,dfs_path,rankings_file,uservisit_file))
    print "finish %s\n" %line
    line = f.readline().strip()