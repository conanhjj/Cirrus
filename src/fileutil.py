__author__ = 'Wind'

import hashlib

class FileUtil:
    def __init__(self):
        pass

    #accept string as file_path
    @staticmethod
    def split_path(file_path):
        pos = file_path.rfind("/")
        if pos == -1 or pos == len(file_path)-1:
            raise ValueError("Illegal file path")

        file_dir = file_path[0:pos+1]
        file_name = file_path[pos+1:]

        return tuple([file_dir, file_name])
    
    @staticmethod
    def file_md5(file_path):
        with open(file_path, 'r') as f:
            data = f.read()
            m = hashlib.md5()
            m.update(data)
            return m.hexdigest()

if __name__ == "__main__":
    print FileUtil.split_path("/123456.ext")
    print FileUtil.split_path("/123/456.ext")
    print FileUtil.split_path("/123456.ext/")  # illegal
    print FileUtil.split_path("123456.ext")  # illegal

