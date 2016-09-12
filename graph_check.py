from __future__ import print_function
import random
import sys
import socket
# import math
import pickle

import os

from Definitions.MultiWorkflow.JobList import Constraint, JobItem
import Definitions.Resources
import copy
import db.definitions
import db.reader
import db.writer
import db.files
from os.path import join


def print_list(l):
    for kk, member in enumerate(l):
        if kk != 0:
            str = ", {}"
        else:
            str = "{}"
        print(str.format(member), end='')
    print()


all_jobs = {'S': Definitions.WorkflowReader.reader.read_jobs('S.txt'),  # 25-30
            'M': Definitions.WorkflowReader.reader.read_jobs('M.txt'),  # 50-60
            'L': Definitions.WorkflowReader.reader.read_jobs('L.txt'),  # 100
            'XXL': Definitions.WorkflowReader.reader.read_jobs('XXL.txt')  # 1000
           }
for job_class in all_jobs.keys():
    for name in all_jobs[job_class].keys():
        output_file = name + '_' + job_class
        print(output_file, end=', ')
        g = all_jobs[job_class][name]
        tasks = g.tasks
        runtimes = list(sorted(map(lambda t: t.weight, tasks.values())))
        print_list(runtimes[2:])


exit()


def main(args):
    try:
        if len(args) < 1:
            print('Required command line arguments are not specified\n'
                  ' usage: python main.py testname dbfilename filenamepart start_number number_of_test_sets policy')
            exit()
        test_name = args[0]
        file_name_part = args[1]
        start = int(args[2])
        file_number = int(args[3])

        test_directory = join('../plans', test_name)

        file_names = db.files.file_list(test_directory, file_name_part, start, file_number)
        file_list = []
        for f in file_names:
            file_list.append(join(test_directory, f))

        if len(file_list) == 0:
            print("No input file")
            exit()
    # ----------------------- Retrieving Everything needed:
        numbers = []
        resources_set = []
        graph_set = []
        makespan_list = []
        cost_list = []
        constraint_values = []
        constraint = []
        job = []
        names = []
        test = 0

        for dumb_file in file_list:
            from_retrieved = pickle.load(open(dumb_file, 'rb'))

            # test, numbers, resources_set, graph_set, makespan_list, cost_list, constraint_values,\
            #     constraint, job, names = from_retrieved
            test, numbers2, resources_set2, graph_set2, makespan_list2, cost_list2, constraint_values2,\
                constraint2, job2, names2 = from_retrieved

            numbers += numbers2
            resources_set += resources_set2
            graph_set += graph_set2
            makespan_list += makespan_list2
            cost_list += cost_list2
            constraint_values += constraint_values2
            constraint += constraint2
            job += job2
            names += names2

    # --------------
        to_do = list(range(len(names)))
        random.shuffle(to_do)
    # --------------

        host_name = socket.gethostname()

        print("Hostname: {}".format(host_name))

        test.workload_len = len(to_do)
        workload_len = test.workload_len

        def print_list(l):
            for kk, member in enumerate(l):
                if kk != 0:
                    str = ", {}"
                else:
                    str = "{}"
                print(str.format(member), end='')
            print()

        for i in range(workload_len):
            tasks = graph_set[i].tasks
            runtimes = list(sorted(map(lambda t: t.weight, tasks.values())))
            print_list(runtimes)

    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)
        raise e

if __name__ == "__main__":
    try:
        main(sys.argv[1:])
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)