import csv
import re
import sys

directory = sys.argv[1]  #path of .csv file
# directory = 'E:/Shen/SparkFaultTolerant/DataSource/test3.csv'
categories = sys.argv[2].split(",")   # which columns are categories

with open(directory, 'r') as csvfile:
    reader = csv.reader(csvfile)
    column = [[]]  # column[0] is the labels, no need to change
    # Get each column, except "NA"
    for row in reader:
        for i in range(1, len(row)):
            if len(column) < i + 2:
                column.append([])
            if row[i] != "NA":
                # find the columns consist of numbers
                if re.search(r"[^0-9.-]", row[i]) == None:
                    column[i].append(float(row[i]))
                else: column[i].append(row[i])

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
        r += 1
csvfile.close()

fileToWrite = sys.argv[3]  #path of .txt file to write
cf = open(fileToWrite, "w")
with open(directory, 'r') as csvfile:
    reader = csv.reader(csvfile)
    for row in reader:
        cf.write(str(row[0]) + " ")
        deviation = 0
        for i in range(1, len(row)):
            if re.search(r"[^0-9.-]", row[i]) == None:
                cf.write(str(i + deviation) + ":" + str(row[i]) + " ")
            else:
                try:
                    # print(categories)
                    categories.index(i)
                    try:
                        value = column[i][row[i]]
                    except(KeyError):
                        pass
                    else:
                        cf.write(str(i + deviation + value) + ":" + str(1) + " ")
                        deviation += len(column[i]) - 1
                except(ValueError):
                    try:
                        value = column[i][row[i]]
                    except(KeyError):
                        pass
                    else:
                        cf.write(str(i + deviation) + ":" + str(value) + " ")
        cf.write("\n")
cf.close()



