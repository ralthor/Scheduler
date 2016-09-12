from os import listdir
from os.path import isfile, join


def file_list(my_path, sub_str='', start=0, n=-1):
    only_files = [f for f in listdir(my_path) if (sub_str in f) and isfile(join(my_path, f))]
    if n > 0 or start != 0:
        only_files = only_files[start:n + start]
    return only_files
