 #!F://Python
import random
file=open('test.txt','w')
num=[]
i=0
col=0
while i<1000000:
	i=i+1
	num.append(random.random()*10)
for r in num:
	col+=1
	file.write(" " + str(r))
	if (col)%2==0:
		file.write("\n")
file.close()
