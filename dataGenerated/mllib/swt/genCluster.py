import random
import sys

filetoWrite = sys.argv[1]
dimen = sys.argv[2]
column = sys.argv[3]

cf = open(filetoWrite, "w")
for i in range(int(column)):
    for j in range(int(dimen)):
        cf.write(str(random.uniform(0, 1)) + " ")
    cf.write("\n")
cf.close()




