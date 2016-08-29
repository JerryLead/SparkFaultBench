import random
import sys

filetoWrite = sys.argv[1]
dimen = sys.argv[2]
column = sys.argv[3]
label_no = sys.argv[4]
sparse = sys.argv[5]  # sparse percent

cf = open(filetoWrite, "w")
for i in range(int(column)):
    if i < int(label_no):
        cf.write("0 ")
    else:
        cf.write("1 ")
    for j in range(1, int(dimen)):
        na = random.randint(0, 100)
        if na < int(sparse):
            cf.write(str(j) + ":" + str(random.randint(0, 255)) + " ")
    cf.write("\n")
cf.close()

# print("Num of label_no is " + str(label_no) + ", num of label_yes is " + str(int(column) - int(label_no)))



