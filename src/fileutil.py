__author__ = 'Wind'


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


