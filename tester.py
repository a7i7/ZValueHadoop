import sys
f = open(sys.argv[1],'r')

variance = True
mean = 9.9462674578

curr_avg = 0.0
curr_cnt = 0
for x in f:
	if not variance:
		data = float(x)
	else:
		data = float(x)-mean
		data = data*data

	curr_avg = (curr_avg*curr_cnt+data)/(curr_cnt+1)
	curr_cnt+=1
print "%.10f" %curr_avg
