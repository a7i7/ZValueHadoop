import sys
import random
N = 10**8
mew = 10
sigma = 120
f = open(sys.argv[1],'w')
for i in range(N):
	print i," of ",N
	f.write(str(random.gauss(mew,sigma))+'\n')
f.close();
