from Definitions.MultiWorkflow.JobList import Constraint, JobItem

from matplotlib import pyplot as plt
import numpy as np
import random

import Scheduler.ICPCP
import Scheduler.HEFT
import Scheduler.BHEFT
import Definitions.Resources
import Scheduler.BudgetPessimistic
import Scheduler.DeadlineOptimisticAlpha
import copy
import Scheduler.Multi_Workflow

from db.definitions import Test
import pickle
import Definitions
from IPython.display import clear_output

def run_all(verbose=False):
    create_pickle()

    row, test, job, constraint, names, sizes = read_jobs()
    test.time_slot = 1
    for r in test.resource_array:
        r[1] *= 12
    bandwidth, workload_len, timeslot_list, powers, prices, numbers, power_list, price_list, timeslot_list, resource_spec, main_resources = create_resources(test)
    makespan_list, cost_list, resources_set, graph_set, constraint_values = calculate_reference_plans(workload_len, main_resources, job, constraint, verbose=verbose)

    to_write = (bandwidth, workload_len, timeslot_list, powers, prices, numbers, power_list, 
                price_list, timeslot_list, resource_spec, main_resources,
                makespan_list, cost_list, resources_set, graph_set, constraint_values)

    pickle.dump(to_write, open('refrence_plan2.pickle', 'wb'))

    read_object = pickle.load(open('refrence_plan2.pickle', 'rb'))

    bandwidth, workload_len, timeslot_list, powers, prices, numbers, power_list, price_list, timeslot_list, resource_spec, main_resources, makespan_list, cost_list, resources_set, graph_set, constraint_values = read_object

    jobs, cloud_resources = create_multi_workflow_resouces(test, resources_set, constraint, cost_list, 
                                                           makespan_list, job, graph_set, constraint_values)
    fair_policy(workload_len, jobs, cloud_resources)
    successful_sched = is_successful(cloud_resources, cost_list, makespan_list, constraint,
                                     constraint_values, jobs, graph_set, names, workload_len)
    if verbose:
        show_results(cloud_resources, cost_list, makespan_list, constraint,
                     constraint_values, jobs, graph_set, names, workload_len, resources_set)
        show_schedule(cloud_resources)
        if successful_sched:
            print('successful scheduling')
        else:
            print('not successful')
    return successful_sched


def create_pickle():
    row = 'test1', 10, '{"t": 1, "r": [[1, 1, 6], [2, 3, 3]]}', 0.5, 1, 0, 0, 1e50

    test = Test(row)
    test.c_resource = 0.8
    job, constraint, names, sizes = Scheduler.Multi_Workflow.make_workload(test)

    names = []
    for j in job:
        i = 1
        while f'{j.type[:-1]}:{i}' in names:
            i += 1
        j.name = f'{j.type[:-1]}:{i}'
        names.append(f'{j.type[:-1]}:{i}')

    [j.name for j in job]
    cnstr = [c is Constraint.budget for c in constraint]
    variable = (row, test, job, cnstr, names, sizes)
    pickle.dump(variable, open('dump.pickle', 'wb'))


def read_jobs():
    row, test, job, cnstr, names, sizes = pickle.load(open('dump.pickle', 'rb'))
    constraint = [Constraint.budget if b else Constraint.deadline for b in cnstr]
    return row, test, job, constraint, names, sizes


def create_resources(test):
    bandwidth = test.bandwidth
    workload_len = test.workload_len
    timeslot_list = []
    powers = []
    prices = []
    numbers = []
    for r in test.resource_array:
        powers.append(r[0])
        prices.append(r[1])
        numbers.append(r[2])

    power_list, price_list, timeslot_list = [], [], []
    for i in range(len(test.resource_array)):
        power_list += [powers[i]] * numbers[i]
        price_list += [prices[i]] * numbers[i]
        timeslot_list += [60 * test.time_slot] * numbers[i]

    resource_spec = (power_list, price_list, timeslot_list)

    main_resources = Definitions.Resources.CostAwareResources(resource_spec[0], resource_spec[1], resource_spec[2],bandwidth)
    
    return bandwidth, workload_len, timeslot_list, powers, prices, numbers, power_list, price_list, timeslot_list, resource_spec, main_resources


def calculate_reference_plans(workload_len, main_resources, job, constraint, verbose=True):
    makespan_list = []
    cost_list = []
    resources_set = []
    graph_set = []
    constraint_values = []

    for i in range(workload_len):
        resources = copy.deepcopy(main_resources)
        g = copy.deepcopy(job[i])
        Scheduler.HEFT.schedule(g, resources)
        g_heft = g
        cost = resources.plan_cost
        makespan = resources.makespan

        heft_resources = resources

        if constraint[i] is Constraint.budget:
            c = 'Budget'
            budget_factor = np.random.normal(8, 3) if random.random() >= 0.2 else np.random.normal(2, 1.4)
            attempts = 3
            while attempts > 0:
                attempts -= 1
                resources = copy.deepcopy(main_resources)
                g = copy.deepcopy(job[i])
                Scheduler.BHEFT.schedule(g, resources, cost * budget_factor)
                if cost * budget_factor >= resources.plan_cost:
                    break
                budget_factor = np.random.normal(8, 3) if random.random() >= 0.2 else np.random.normal(2, 1.4)

            constraint_factor = budget_factor
            constraint_value = cost * budget_factor
        else:
            c = 'Deadline'
            deadline_factor = np.random.normal(8, 1.4) if random.random() >= 0.2 else np.random.normal(2, 1.4)

            resources = heft_resources
            attempts = 3
            while attempts > 0:
                attempts -= 1
                resources = copy.deepcopy(main_resources)
                g = copy.deepcopy(job[i])
                Scheduler.ICPCP.schedule(g, resources, makespan * deadline_factor)
                if makespan * deadline_factor >= resources.makespan:
                    break
                else:
                    deadline_factor = np.random.normal(8, 1.4) if random.random() >= 0.2 else np.random.normal(2, 1.4)
            constraint_factor = deadline_factor
            constraint_value = makespan * deadline_factor
        if verbose:
            print("heft cost:{0:5.1f} | cost:{1:5.1f} | heft ms:{2:5.2f} | ms:{3:5.2f} "
                  "| Nodes:{4:4d} | {5:>8} | factor: {6:5.2f}".format(cost, resources.plan_cost, makespan,
                                                                      resources.makespan, len(g.tasks) - 2, c,
                                                                      constraint_factor))

        # ---Store results for next use:
        makespan_list.append(resources.makespan)
        cost_list.append(resources.plan_cost)
        resources_set.append(copy.deepcopy(resources))
        graph_set.append(g)
        constraint_values.append(constraint_value)
    return makespan_list, cost_list, resources_set, graph_set, constraint_values


def show_schedule(resources, save_number=None, current_time=None):
    sched = resources.show_schedule()

    num_plots = sum([len(item[0]) for item in sched])
    figure_number = random.randint(1, 10000)
    fig = plt.figure(figure_number, figsize=[10,10])
    colormap = plt.cm.gist_ncar
    plt.gca().set_prop_cycle('color', [colormap(i) for i in np.linspace(0, 0.9, num_plots)])
    colors = plt.rcParams["axes.prop_cycle"].by_key()["color"]
    my_label = []
    k = 0

    for i, entry in enumerate(sched):
        name = entry[0]
        est = entry[1]
        eft = entry[2]
        for j in range(len(est)):
#             print(f'({k}, {i}, {est[j]}, {eft[j]}),') ####################
            k += 1
            graph_name = name[j].split('-')[0]
            first_visit = False
            if not graph_name in my_label:
                my_label.append(graph_name)
                first_visit = True

            my_color = my_label.index(graph_name)
            if first_visit:
                plt.plot([est[j], eft[j]], [i/5, i/5], linewidth=2, label=graph_name, 
                         color=colors[my_color])
            else:
                plt.plot([est[j], eft[j]], [i/5, i/5], linewidth=2, 
                         color=colors[my_color])

    if current_time:
        plt.plot([current_time, current_time], [0, len(sched)/5], linewidth=1, color='red')
    plt.legend(loc='center')

    plt.legend()
    plt.title(f'total cost: {resources.plan_cost}')
    if not save_number is None:
        plt.savefig(f'images/{save_number}', bbox_inches='tight')
        plt.close(figure_number)
    else:
        plt.show()

def create_multi_workflow_resouces(test, resources_set, constraint, cost_list, 
                                   makespan_list, job, graph_set, constraint_values):
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
    for i in range(workload_len):
        resources = resources_set[i]
        for r in range(0, resources.len):
            if resources.resource_cost(r) != 0:
                n[type_of_resource(r)] += 1

    c_resource = test.c_resource # the best was 0.8 in the tests # 0.4..1.2
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
    for i in range(workload_len):
        if constraint[i] is Constraint.deadline:
            graph_set[i].makespan = makespan_list[i]  # resources_set[i].makespan
        else:
            graph_set[i].cost = cost_list[i]  # resources_set[i].plan_cost
            graph_set[i].makespan = makespan_list[i]  # resources_set[i].makespan

        prev_resources = resources_set[i]

        job_item = JobItem(copy.deepcopy(job[i]), constraint[i],
                           constraint_values[i], cloud_resources, graph_set[i], prev_resources)
        jobs.append(job_item)

    # prev_cloud_cost = 0
    # previously_scheduled_graph = -1

    current_critical = [0] * workload_len

    # gap-rate calculation:
    gap_rate = [0] * workload_len
    s = gap_rate[:]
    sum_task_number = sum(map(lambda graph: len(graph.tasks), graph_set))
    for i in range(workload_len):
        gap_rate[i] = resources_set[i].gap_rate
        s[i] = len(graph_set[i].tasks) / (gap_rate[i] * sum_task_number)
    iterator = min(s)
    ref_s = s[:]
    return jobs, cloud_resources

def fair_policy(workload_len, jobs, cloud_resources,
                show_online_schedule=False, arrivals=False):
    # scheduling dummy tasks (get rid of them!):
    for i in range(workload_len):
        jobs[i].scheduler.schedule_next(do_head_nodes=True)
        cloud_resources.costs = cloud_resources.price_of_each_graph()
        # current_critical[i] = jobs[i].critical_now

    figure_number = 1

    # MAIN WHILE of Scheduler:
    current_time = 0
    while any([not job.scheduler.finished for job in jobs]):
        cloud_resources.costs = cloud_resources.price_of_each_graph()
        for i in range(len(jobs)):
            job = jobs[i]
            consumed_cost = cloud_resources.costs[job.g.name]
            job.scheduler.remaining_budget = job.scheduler.total_budget - consumed_cost
            job.scheduler.recalculate_sub_budget()

        while all([job.scheduler.next_ready_task(current_time)==-1 for job in jobs]):
            current_time = min([x for x in [job.scheduler.next_event(current_time) for job in jobs] if not x is None])
        ready_list = [i for i, job in enumerate(jobs) if job.scheduler.next_ready_task(current_time) != -1]

        most_critical = max([(jobs[ii].critical_now, ii) for ii in ready_list])[1]

        if show_online_schedule:
            show_schedule(cloud_resources, figure_number, current_time)
        figure_number += 1

        job = jobs[most_critical]

        job.scheduler.schedule_next(do_head_nodes=True, arrival_time=current_time)


def fair_policy_old(show_online_schedule=False, arrivals=False):
    try:
        # scheduling dummy tasks (get rid of them!):
        for i in range(workload_len):
            jobs[i].scheduler.schedule_next(do_head_nodes=True)
            cloud_resources.costs = cloud_resources.price_of_each_graph()
            # current_critical[i] = jobs[i].critical_now

        figure_number = 1
        ready_list = list(range(workload_len))

        # MAIN WHILE of Scheduler:
        arrival_time = 0
        while ready_list:
#             arrival_time += 10 # -------------------------------------------
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

            if show_online_schedule:
                show_schedule(cloud_resources, figure_number)
            figure_number += 1
            job_index = most_critical  # ready_list[most_critical]
            job = jobs[job_index]

            del ready_list[ready_list_index]

            job.scheduler.schedule_next(do_head_nodes=True, arrival_time=arrival_time)

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

def show_results(cloud_resources, cost_list, makespan_list, constraint,
                 constraint_values, jobs, graph_set, names, workload_len, resources_set):
    # ------------ printing the result of scheduling:
    print()
    costs = cloud_resources.price_of_each_graph()
    sum_separate = 0
    s_e = []
    print('+---+----------+--------+--------+--------+---------+--------+--------+------+------+')
    print('|job|constraint| value  | ms old | ms new |prev cost|new cost|gap-rate|c-rate|m-rate|')
    print('+---+----------+--------+--------+--------+---------+--------+--------+------+------+')
    for i in range(len(jobs)):
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

        sum_separate += cost_list[i]  # resources_set[i].plan_cost
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

def is_successful(cloud_resources, cost_list, makespan_list, constraint,
                  constraint_values, jobs, graph_set, names, workload_len):
    costs = cloud_resources.price_of_each_graph()
    sum_separate = 0
    s_e = []
    for i in range(len(jobs)):
        prev_makespan = makespan_list[i]  # resources_set[i].makespan
        if graph_set[i].endID not in cloud_resources.job_task_schedule[graph_set[i].name]:
            print("|{:3d}|problem!".format(i))
            continue
        cloud_makespan = cloud_resources.job_task_schedule[graph_set[i].name][graph_set[i].endID].EFT
        prev_cost = cost_list[i]
        cloud_cost = costs[graph_set[i].name]
        m_rate = prev_makespan / cloud_makespan
        c_rate = prev_cost / cloud_cost
        if constraint[i] is Constraint.deadline:
            c = ' Deadline '
            m_rate = constraint_values[i] / cloud_makespan
            if m_rate < 1:
                print(f'it is {c} constrained, m_rate is {m_rate} -- constraint_values: {constraint_values[i]}, cloud_makespan: {cloud_makespan}')
                return False
            s_e.append(c_rate)
        else:
            c = '  Budget  '
            c_rate = constraint_values[i] / cloud_cost
            if c_rate < 1:
                print(f'it is {c} constrained, c_rate is {c_rate} -- constraint_values: {constraint_values[i]}, cloud_cost: {cloud_cost}')
                return False
            s_e.append(m_rate)
        deadline = -1
        budget = -1
        if constraint[i] is Constraint.deadline:
            deadline = constraint_values[i]
        else:
            budget = constraint_values[i]

        job_name = names[i]
        job_size = len(graph_set[i].tasks) - 2

        sum_separate += cost_list[i]  # resources_set[i].plan_cost

    A = sum(s_e) / workload_len
    sigma_u = 0
    for se in s_e:
        sigma_u += abs(se - A)
    U = sigma_u / workload_len
    if cloud_resources.plan_cost > sum_separate:
        return False
    return True