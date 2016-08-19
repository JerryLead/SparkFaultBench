import csv
import re

# import sys

directory = sys.argv[1]
# directory = 'E:/Shen/SparkFaultTolerant/DataSource/sotc-09test-passion.csv'

Max = {}   # max value of each column
Min = {}   # min value ofeach column
listnum = []  # columns consist of numbers

with open(directory, 'r') as csvfile:
    reader = csv.reader(csvfile)
    column = [[]]  # column[0] is the labels, no need to change
    # Get each column, except "NA"
    row_num = 0  # nomalize
    for row in reader:
        for i in range(1, len(row)):
            if len(column) < i + 2:
                column.append([])
            if row[i] != "NA":
                # find the columns consist of numbers
                if re.search(r"[^0-9.-]", row[i]) == None:
                    column[i].append(float(row[i]))
                    if row_num == 0: # nomalize
                        # only find once, so if the first row is a number,
                        # the other row must be numbers
                        listnum.append(i) # nomalize
                else: column[i].append(row[i])
        row_num += 1 # nomalize

    # get max and min value of columns consist of numbers   # nomalize
    for i in listnum:
        Max[i] = max(column[i])
        Min[i] = min(column[i])

    # get column sets and convert to dictionaries
    r = 0
    for co in column:
        co = set(co)
        i = 0
        dict = {}
        for j in co:
            dict[j] = i
            i += 1

        column[r] = dict
        # get max and min of columns do not consist of numbers
        # nomalize
        try:
            a = Max[r]
        except(KeyError):
            Max[r] = len(co)-1
            Min[r] = 0

        r += 1
csvfile.close()

fileToWrite = sys.argv[2]
# fileToWrite = 'E:/Shen/SparkFaultTolerant/DataSource/sotc-09test-passion-result-nomal.txt'
cf = open(fileToWrite, "w")
with open(directory, 'r') as csvfile:
    reader = csv.reader(csvfile)
    for row in reader:
        cf.write(str(row[0]) + " ")
        for i in range(1, len(row)):
            if re.search(r"[^0-9.-]", row[i]) == None:
                # nomalize
                cf.write(str(i) + ":" + str(round((float(row[i])-Min[i])/(Max[i]-Min[i])*100)) + " ")

                # not nomalized
                # cf.write(str(i) + ":" + str(row[i]) + " ")
            else:
                try:
                    value = column[i][row[i]]
                except(KeyError):
                    pass
                else:
                    # nomalize
                    cf.write(str(i) + ":" + str(round((value-Min[i])/(Max[i]-Min[i])*100)) + " ")

                    # not nomalized
                    # cf.write(str(i) + ":" + str(value) + " ")
        cf.write("\n")
cf.close()



