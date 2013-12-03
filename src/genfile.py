import sys

size = sys.argv[1]
out = open(size + 'k.txt', 'w')

for i in range(1024 * int(size)):
	out.write('a')

out.close()

