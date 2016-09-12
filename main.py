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


def main(args):
    try:
        if len(args) < 1:
            print('Required command line arguments are not specified\n'
                  ' usage: python main.py testname dbfilename filenamepart start_number number_of_test_sets policy')
            exit()
        test_name = args[0]
        database_file = args[1]
        file_name_part = args[2]
        start = int(args[3])
        file_number = int(args[4])
        policy = args[5]

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

        reader = db.reader.Reader(database_file)
        rows = reader.read_test(test.test_name)
        row = rows.fetchone()
        test = db.definitions.Test(row)
        timeslot = test.time_slot
        bandwidth = test.bandwidth
    #    workload_len = test.workload_len
        powers = []
        prices = []
        numbers = []
        for r in test.resource_array:
            powers.append(r[0])
            prices.append(r[1])
            numbers.append(r[2])
        reader.connection.close()

        host_name = socket.gethostname()

        print("Hostname: {}".format(host_name))

        test.workload_len = len(to_do)
        workload_len = test.workload_len

    # ----------------------- End of loading needed things.

        # Preparing the resources in the cloud:

        def type_of_resource(r_id):
            limit = 0
            for p in range(len(numbers)):
                limit += numbers[p]
                if r_id < limit:
                    return p
            else:
                return -1

        n = [0] * len(numbers)
        for jj in range(workload_len):
            i = to_do[jj]
            resources = resources_set[i]
            for r in range(0, resources.len):
                if resources.resource_cost(r) != 0:
                    n[type_of_resource(r)] += 1

        # TODO: decreasing resources, to force efficient use of resources!
        c_resource = 1.2
        for i in range(len(n)):
            n[i] = int(n[i] * c_resource)

        power_list, price_list, timeslot_list = [], [], []
        for i in range(len(test.resource_array)):
            power_list += [powers[i]] * n[i]
            price_list += [prices[i]] * n[i]
            # TODO: Tests must be changed, but it works for now (in case of change: both planner and main):
            timeslot_list += [60 * timeslot] * n[i]

        resource_spec = (power_list, price_list, timeslot_list)

        # resource_spec = ([power1] * n[0] + [power2] * n[1] + [power3] * n[2],
        #                  [price1] * n[0] + [price2] * n[1] + [price3] * n[2],
        #                  [timeslot] * (n[0] + n[1] + n[2]))

        cloud_resources = Definitions.Resources.CostAwareResources(resource_spec[0], resource_spec[1], resource_spec[2],
                                                                   bandwidth)

        # -------- Making a multi-workflow list, which contains all workflows (they will schedule together)
        jobs = []
        for jj in range(workload_len):
            i = to_do[jj]
            if constraint[i] is Constraint.deadline:
                graph_set[i].makespan = makespan_list[i]  # resources_set[i].makespan
            else:
                graph_set[i].cost = cost_list[i]  # resources_set[i].plan_cost
                graph_set[i].makespan = makespan_list[i]  # resources_set[i].makespan

            prev_resources = resources_set[i]

            job_item = JobItem(copy.deepcopy(job[i]), constraint[i],
                               constraint_values[i], cloud_resources, graph_set[i], prev_resources)
            jobs.append(job_item)

        print("==")
        # prev_cloud_cost = 0
        # previously_scheduled_graph = -1

        current_critical = [0] * workload_len

        # gap-rate calculation:
        gap_rate = [0] * workload_len
        s = gap_rate[:]
        sum_task_number = sum(map(lambda graph: len(graph.tasks), graph_set))
        for jj in range(workload_len):
            i = to_do[jj]
            gap_rate[jj] = resources_set[i].gap_rate
            s[jj] = len(graph_set[i].tasks) / (gap_rate[jj] * sum_task_number)
        iterator = min(s)
        ref_s = s[:]

        # end of gap-rate calculation

        # ===============================================================================================================
        # ===============================================================================================================
        # ===============================================================================================================
    # ----------------------- START THE MAIN PHASE: (with different policies as functions):
        def prr_policy():
            try:
                # scheduling dummy tasks (get rid of them!):
                for i in range(workload_len):
                    jobs[i].scheduler.schedule_next(do_head_nodes=True)
                    cloud_resources.costs = cloud_resources.price_of_each_graph()
                    current_critical[i] = jobs[i].critical_now

                # MAIN WHILE of Scheduler:
                while True:
                    cloud_resources.costs = cloud_resources.price_of_each_graph()
                    for i in range(len(jobs)):
                        job = jobs[i]
                        consumed_cost = cloud_resources.costs[job.g.name]
                        job.scheduler.remaining_budget = job.scheduler.total_budget - consumed_cost
                        job.scheduler.recalculate_sub_budget()

                    max_s = max(s)
                    if max_s <= 0:
                        if max_s == -1e50:
                            break
                        for j in range(workload_len):
                            if ref_s[j] != -1e50:
                                s[j] += ref_s[j]
                            else:
                                s[j] = ref_s[j]
                    epsilon = 0.00000002
                    for k in range(workload_len):
                        if abs(max_s - s[k]) < epsilon:
                            j = k
                            break

                    s[j] -= iterator

                    critical_job = jobs[j]

                    first_task_in_round = True
                    previous_resource = -1
                    while True:
                        if critical_job.scheduler.finished:
                            s[j] = -1e50
                            ref_s[j] = -1e50
                            break

                        eft, cost, resource_id = critical_job.scheduler.schedule_next(do_head_nodes=True)
                        if first_task_in_round:
                            if cost == 0:
                                break
                            first_task_in_round = False
                            previous_resource = resource_id
                        elif previous_resource == resource_id and cost == 0:
                            if s[j] > 0:
                                s[j] -= iterator
                            continue
                        else:
                            break
            except Exception as e:
                exc_type, exc_obj, exc_tb = sys.exc_info()
                fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                print(exc_type, fname, exc_tb.tb_lineno)
                raise e

        # ===============================================================================================================
        def rr_policy():
            try:
                # scheduling dummy tasks (get rid of them!):
                for i in range(workload_len):
                    jobs[i].scheduler.schedule_next(do_head_nodes=True)
                    cloud_resources.costs = cloud_resources.price_of_each_graph()
                    current_critical[i] = jobs[i].critical_now

                # MAIN WHILE of Scheduler:
                ready_list = list(reversed(range(workload_len)))
                while ready_list:
                    j = ready_list.pop()
                    critical_job = jobs[j]
                    if critical_job.scheduler.finished:
                        continue
                    else:
                        ready_list.insert(0, j)

                    cloud_resources.costs = cloud_resources.price_of_each_graph()
                    for i in range(len(jobs)):
                        job = jobs[i]
                        consumed_cost = cloud_resources.costs[job.g.name]
                        job.scheduler.remaining_budget = job.scheduler.total_budget - consumed_cost
                        job.scheduler.recalculate_sub_budget()

                    first_task_in_round = True
                    previous_resource = -1
                    while not critical_job.scheduler.finished:
                        eft, cost, resource_id = critical_job.scheduler.schedule_next(do_head_nodes=True)
                        if first_task_in_round:
                            if cost == 0:
                                break
                            first_task_in_round = False
                            previous_resource = resource_id
                        elif previous_resource == resource_id and cost == 0:
                            continue
                        else:
                            break
            except Exception as e:
                exc_type, exc_obj, exc_tb = sys.exc_info()
                fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                print(exc_type, fname, exc_tb.tb_lineno)
                raise e
        # ===============================================================================================================
        def fcfs_policy():
            try:
                # scheduling dummy tasks (get rid of them!):
                for i in range(workload_len):
                    jobs[i].scheduler.schedule_next(do_head_nodes=True)
                    cloud_resources.costs = cloud_resources.price_of_each_graph()
                    current_critical[i] = jobs[i].critical_now

                # MAIN WHILE of Scheduler:
                ready_list = list(reversed(range(workload_len)))
                while len(ready_list) > 0:
                    j = ready_list.pop()
                    critical_job = jobs[j]
                    if critical_job.scheduler.finished:
                        continue
                    else:
                        ready_list.append(j)

                    cloud_resources.costs = cloud_resources.price_of_each_graph()
                    for i in range(len(jobs)):
                        job = jobs[i]
                        consumed_cost = cloud_resources.costs[job.g.name]
                        job.scheduler.remaining_budget = job.scheduler.total_budget - consumed_cost
                        job.scheduler.recalculate_sub_budget()

                    first_task_in_round = True
                    previous_resource = -1
                    while not critical_job.scheduler.finished:
                        eft, cost, resource_id = critical_job.scheduler.schedule_next(do_head_nodes=True)
                        if first_task_in_round:
                            if cost == 0:
                                break
                            first_task_in_round = False
                            previous_resource = resource_id
                        elif previous_resource == resource_id and cost == 0:
                            continue
                        else:
                            break
            except Exception as e:
                exc_type, exc_obj, exc_tb = sys.exc_info()
                fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                print(exc_type, fname, exc_tb.tb_lineno)
                raise e
        # ===============================================================================================================
        def fair_policy():
            try:
                # scheduling dummy tasks (get rid of them!):
                for i in range(workload_len):
                    jobs[i].scheduler.schedule_next(do_head_nodes=True)
                    cloud_resources.costs = cloud_resources.price_of_each_graph()
                    # current_critical[i] = jobs[i].critical_now

                ready_list = list(range(workload_len))
                # MAIN WHILE of Scheduler:
                while ready_list:
                    cloud_resources.costs = cloud_resources.price_of_each_graph()
                    for i in range(len(jobs)):
                        job = jobs[i]
                        consumed_cost = cloud_resources.costs[job.g.name]
                        job.scheduler.remaining_budget = job.scheduler.total_budget - consumed_cost
                        job.scheduler.recalculate_sub_budget()

                    most_critical = -1
                    criticality = 100
                    ready_list_index = -1
                    for index, ii in enumerate(ready_list):
                        job = jobs[ii]
                        current_critical = job.critical_now
                        if current_critical < criticality:
                            criticality = current_critical
                            most_critical = ii
                            ready_list_index = index

                    job_index = most_critical  # ready_list[most_critical]
                    job = jobs[job_index]
                    del ready_list[ready_list_index]

                    job.scheduler.schedule_next(do_head_nodes=True)

                    if job.scheduler.finished:
                        continue
                    else:
                        ready_list.append(job_index)
                return
            except Exception as e:
                exc_type, exc_obj, exc_tb = sys.exc_info()
                fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                print(exc_type, fname, exc_tb.tb_lineno)
                raise e
        # ===============================================================================================================
        if policy == 'prr':
            prr_policy()
        elif policy == 'rr':
            rr_policy()
        elif policy == 'fcfs':
            fcfs_policy()
        elif policy == 'fair':
            fair_policy()
        elif policy in ['zhao', 'interleaving']:
            print('Policy {} has not yet been implemented...'.format(policy))
            exit()
        else:
            print("Policy must be in {'prr', 'rr', 'fcfs', 'fair', 'zhao', 'interleaving'}")
            exit()
        # ===============================================================================================================

        writer = db.writer.Writer(database_file)
        result_id = writer.write_result_head(test.test_name)

        # ------------ printing the result of scheduling:
        print()
        costs = cloud_resources.price_of_each_graph()
        sum_separate = 0
        s_e = []
        print('+---+----------+--------+--------+--------+---------+--------+--------+------+------+')
        print('|job|constraint| value  | ms old | ms new |prev cost|new cost|gap-rate|c-rate|m-rate|')
        print('+---+----------+--------+--------+--------+---------+--------+--------+------+------+')
        for jj in range(len(jobs)):
            i = to_do[jj]
            prev_makespan = makespan_list[i]  # resources_set[i].makespan
            if graph_set[i].endID not in cloud_resources.job_task_schedule[graph_set[i].name]:
                print("|{:3d}|problem!".format(i))
                continue
            cloud_makespan = cloud_resources.job_task_schedule[graph_set[i].name][graph_set[i].endID].EFT
            prev_cost = cost_list[i]  # resources_set[i].plan_cost
            cloud_cost = costs[graph_set[i].name]
            m_rate = prev_makespan / cloud_makespan
            c_rate = prev_cost / cloud_cost
            if constraint[i] is Constraint.deadline:
                c = ' Deadline '
                m_rate = constraint_values[i] / cloud_makespan
                s_e.append(c_rate)
            else:
                c = '  Budget  '
                c_rate = constraint_values[i] / cloud_cost
                s_e.append(m_rate)
            print('|{:3d}|{}|{:8.3f}|{:8.3f}|{:8.3f}'
                  '|{:9.0f}|{:8.2f}|{:8.5f}|{:6.4f}|{:6.4f}|'
                  ''.format(i, c, constraint_values[i], prev_makespan, cloud_makespan,
                            prev_cost, cloud_cost, resources_set[i].gap_rate,
                            c_rate, m_rate))
            deadline = -1
            budget = -1
            if constraint[i] is Constraint.deadline:
                deadline = constraint_values[i]
            else:
                budget = constraint_values[i]

            job_name = names[i]
            job_size = len(graph_set[i].tasks) - 2
            result_object = db.definitions.Result(result_id, test.test_name, policy, c.strip(),
                                                  deadline, budget, prev_makespan, cloud_makespan, prev_cost, cloud_cost,
                                                  resources_set[i].gap_rate, c_rate, m_rate, job_name, job_size)
            writer.write_results(result_object, False)

            sum_separate += cost_list[i]  # resources_set[i].plan_cost
        writer.commit()
        print('+---+----------+--------+--------+--------+---------+--------+--------+------+------+')

        A = sum(s_e) / workload_len
        sigma_u = 0
        for se in s_e:
            sigma_u += abs(se - A)
        U = sigma_u / workload_len
        print()
        print("Overall Cloud Cost:{:6.3f}".format(cloud_resources.plan_cost))
        print("Separate Runs Cost:{:6.3f}".format(sum_separate))
        print("\nUnfairness:{:8.5f}".format(U))

        cloud_resources_gap_rate = cloud_resources.gap_rate
        print("\nCloud gap-ratio:{:8.5f}".format(cloud_resources_gap_rate))

        writer.change_result_head(result_id, test.test_name, sum_separate,
                                  U, workload_len, cloud_resources_gap_rate, timeslot_list[0], c_resource)

        # cloud_resources.write_schedule(database_file, test.test_name, 'multi', policy, workload_len)
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