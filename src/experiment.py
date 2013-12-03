import os, shutil

src_dir = 'files/'
dest_dir = 'disk/'

for file in os.listdir(src_dir):
	if file[0] != '.':
		for i in range(5):
			src = src_dir + file
			dest = dest_dir + str(i) + '_' + file
			shutil.copy(src, dest)



