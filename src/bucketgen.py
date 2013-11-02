__author__ = 'Wind'


class BucketGenerator:

    def __init__(self, bucket_file="bucket"):
        self.bucket2dir = {}
        self.dir2bucket = {}
        self.bucket_file = bucket_file
        self.load_bucket_file()

    # TODO: implement loading
    def load_bucket_file(self):
        self.bucket2dir = {}
        self.dir2bucket = {}

    # TODO: implement writing
    def flush_to_disk(self):
        with open(self.bucket_file, 'w') as f:
            f.write("to be implemented")

    def get_bucket(self, file_dir):
        if not file_dir in self.dir2bucket:
            bucket = self.__gen(file_dir)
            self.dir2bucket[file_dir] = bucket

            if not bucket in self.bucket2dir:
                self.bucket2dir[bucket] = []
            self.bucket2dir[bucket].append(file_dir)

        return self.dir2bucket[file_dir]

    @staticmethod
    def __gen(file_dir):
        return "example_bucket"

if __name__ == "__main__":
    bucket_gen = BucketGenerator()
    print bucket_gen.get_bucket("123/")
    print bucket_gen.get_bucket("456/")
    bucket_gen.flush_to_disk()
