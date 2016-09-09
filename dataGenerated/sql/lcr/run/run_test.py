import os

classdicts = {'Scan':'sql.Scan', 'Join':'sql.Join', 'Aggregate':'sql.Aggregate', 'Mix':'sql.Mix'}
jardicts = {'Scan':'ScanSQL', 'Join':'JoinSQL', 'Aggregate':'AggregateSQL', 'Mix':'MixSQL'}
params = {}
dirpath = os.getcwd()

f = open("%s/config.txt" %dirpath,"r")
lines = f.readlines()
f.close()
for line in lines:
    line = line.strip()
    if len(line)>1:
        if not line.strip()[0] == '#':
            pas = line.split("=")
            params[pas[0].strip()] = pas[1].strip()


dfs_path = params["HDFS_PATH"]
rankings_file = params["RANKINGS_FILE"]
rankings_skewed_file = params["RANKINGS_SKEWED_FILE"]
uservisit_file = params["USERVISITS_FILE"]
uservisit_skewed_file = params["USERVISITS_SKEWED_FILE"]
runtype = params["RUN_TYPE"]
print (runtype)


def runspark(appname, taskname, f1, f2):
    classname = classdicts[taskname]
    testname = jardicts[taskname]
    print ("start %s\n" % appname)

    os.system("${SPARK_HOME}/bin/spark-submit "
              "--master yarn "
              "--deploy-mode cluster "
              "--queue default "
              "--driver-memory 5g "
              "--executor-memory 4g "
              "--executor-cores 2  "
              "--class %s %s/%s.jar %s %s %s"
              %(classname,dirpath,testname, dfs_path, f1, f2))

    print ("finish %s\n" % appname)

f = open("%s/testlist.txt" % dirpath,"r")
lines = f.readlines()
f.close()
for line in lines:
    if len(line) <= 1:
        continue
    line = line.strip()
    if (runtype == 'ALL'):
        runspark(line, line, rankings_file, uservisit_file)
        runspark('skewed '+line, line, rankings_skewed_file, uservisit_skewed_file)
    elif runtype == 'NORMAL':
        runspark(line, line, rankings_file, uservisit_file)
    elif runtype == 'SKEWED':
        runspark('skewed '+line, line, rankings_skewed_file, uservisit_skewed_file)
    else:
        print ("wrong run type, stop test!")
        break
