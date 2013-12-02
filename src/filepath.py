import os
paths = ['/fs/b/d', '/fs/a', '/fs/a/c']

for path in paths:
	start = 2
	end = 0 
	while True:
		end = path.find('/', start)
		if end != -1:
			dir = path[:end]
			print dir
			# if not os.path.exists(dir):
			# 	os.makedirs(dir)
			start = end + 1
		else:
			break

