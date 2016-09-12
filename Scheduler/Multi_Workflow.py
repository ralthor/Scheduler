import math
import random
import copy
import string

from Definitions.Resources import Constraint
import Definitions.WorkflowReader.reader
import Definitions
from Definitions import Graph
from Definitions.Resources import CostAwareResources


def assign_sub_deadlines(reference_graph, target_graph, asap=None):
    for t_id in reference_graph.tasks.keys():
        sub_deadline = reference_graph.tasks[t_id].lft
        target_graph.tasks[t_id].sub_deadline = sub_deadline
        if asap is not None:
            target_graph.tasks[t_id].asap = asap


def assign_sub_budgets(reference_graph, target_graph, reference_resources):
    resources = reference_resources  # reference_graph.resources
    for r in range(resources.len):
        resource_cost = resources.resource_cost(r)
        if resource_cost == 0:
            continue
        task_in_resource = resources.tasksOfResource[r]
        sum_runtime = sum(map(lambda s: s.runtime, task_in_resource))
        for sch in task_in_resource:
            task_id = sch.task.id
            task_runtime = sch.runtime
            # if sum_runtime == 0:
            #     target_graph.tasks[task_id].sub_budget = 0
            # else:
            target_graph.tasks[task_id].sub_budget = float(task_runtime) / sum_runtime * resource_cost


def assign_cost_for_each_task(g, resources=CostAwareResources([], [], [], 0)):
    for r in range(0, resources.len):
        resource_cost = resources.resource_cost(r)
        if resource_cost == 0:
            continue
        scheduled_task_in_resource = resources.tasksOfResource[r]
        sum_weight = sum(map(lambda t: t.task.weight, scheduled_task_in_resource))
        for schedule in scheduled_task_in_resource:
            g.tasks[schedule.task.id].cost = resource_cost * schedule.task.weight / sum_weight
            g.tasks[schedule.task.id].eft = schedule.EFT


def normalize_dag(g=Graph.Graph(), w_coefficient=1, e_coefficient=1):
    sum_w = sum(map(lambda t: t.weight, g.tasks.values()))
    edges = map(lambda t: list(t.successor.values()), g.tasks.values())
    sum_e = sum([item for sub_list in edges for item in sub_list])
    for task in g.tasks.values():
        task.weight /= sum_w
        task.weight *= w_coefficient

        for c in task.successor:
            task.successor[c] /= sum_e / e_coefficient
            g.tasks[c].predecessor[task.id] /= sum_e / e_coefficient


def randomize_dag(g=Graph.Graph(), w_coefficient=[0.3, 1.7], e_coefficient=[0.3, 1.7]):
    precision = 10000
    w_start = int(w_coefficient[0] * precision)
    w_end = int(w_coefficient[1] * precision)
    e_start = int(e_coefficient[0] * precision)
    e_end = int(e_coefficient[1] * precision)
    for task in g.tasks.values():
        task.weight *= float(random.randrange(w_start, w_end)) / precision
        for c in task.successor:
            task.successor[c] *= float(random.randrange(e_start, e_end)) / precision
            g.tasks[c].predecessor[task.id] = task.successor[c]


def compute_gap_rate(resources=CostAwareResources([], [], [], 0), r=-1, t=-1, list_is_required=False):
    timeslot_len = resources.timeslot[r]
    t += float(timeslot_len / 2)
    tasks_in_resource = resources.tasksOfResource[r]
    length_r = len(tasks_in_resource)
    try:
        start_task_index = 0
        while tasks_in_resource[start_task_index].task.dummy_task:
            start_task_index += 1
        end_task_index = length_r - 1
        while tasks_in_resource[end_task_index].task.dummy_task:
            end_task_index -= 1
        if end_task_index < start_task_index:
            if list_is_required:
                return 0, -1, -1, []
            else:
                return 0, -1, -1
    except LookupError:
        if list_is_required:
            return 0, -1, -1, []
        else:
            return 0, -1, -1
    t0 = tasks_in_resource[start_task_index].EST
    x = float(t - t0) / timeslot_len
    if x < 0:
        if list_is_required:
            return 0, -1, -1, []
        else:
            return 0, -1, -1
    start_time_of_timeslot = math.floor(x) * timeslot_len + t0
    end_time_of_timeslot = math.ceil(x) * timeslot_len + t0
    if start_time_of_timeslot == end_time_of_timeslot:
        end_time_of_timeslot += timeslot_len

    sum_gap = 0
    gap_list = []
    for i in range(start_task_index + 1, end_task_index + 1):
        # find the gap between "i"th and "i-1"th tasks:
        gap_start = tasks_in_resource[i - 1].EFT
        gap_end = tasks_in_resource[i].EST
        if end_time_of_timeslot <= gap_start:
            break
        if gap_end <= start_time_of_timeslot:
            continue
        if gap_start < start_time_of_timeslot:
            gap_start = start_time_of_timeslot
        if end_time_of_timeslot < gap_end:
            gap_end = end_time_of_timeslot
        sum_gap += gap_end - gap_start
        if not list_is_required or gap_end == gap_start:
            continue
        gap_list.append((gap_start, gap_end))
    if start_time_of_timeslot < tasks_in_resource[end_task_index].EFT < end_time_of_timeslot:
        sum_gap += end_time_of_timeslot - tasks_in_resource[end_task_index].EFT
        if list_is_required:
            gap_list.append((tasks_in_resource[end_task_index].EFT, end_time_of_timeslot))
    if list_is_required:
        return float(sum_gap) / timeslot_len, start_time_of_timeslot, end_time_of_timeslot, gap_list
    else:
        return float(sum_gap) / timeslot_len, start_time_of_timeslot, end_time_of_timeslot


def tasks_in_interval(resources=CostAwareResources([], [], [], 0), r=-1, t=-1):
    timeslot_len = resources.timeslot[r]
    t += float(timeslot_len / 2)
    tasks_in_resource = resources.tasksOfResource[r]
    length_r = len(tasks_in_resource)
    start_task_index = 0
    end_task_index = length_r - 1

    if length_r == 0:
        return []

    t0 = tasks_in_resource[start_task_index].EST
    x = float(t - t0) / timeslot_len
    if x < 0:
        return []
    start_time_of_timeslot = math.floor(x) * timeslot_len + t0
    end_time_of_timeslot = math.ceil(x) * timeslot_len + t0
    if start_time_of_timeslot == end_time_of_timeslot:
        end_time_of_timeslot += timeslot_len

    task_list = []
    for i in range(start_task_index, end_task_index + 1):
        # find the task ids within the timeslot (each portion of them)
        task_start_time = tasks_in_resource[i].EST
        task_finish_time = tasks_in_resource[i].EFT
        if start_time_of_timeslot <= task_finish_time <= end_time_of_timeslot or \
           start_time_of_timeslot <= task_start_time <= end_time_of_timeslot:
            task_list.append(tasks_in_resource[i].task.id)
    return task_list


def generate_gammavariate_list(n, alpha, beta):
    r_list = []
    for i in range(n):
        r = random.gammavariate(alpha, beta)
        r_list.append(r)
    return r_list


def generate_hyper_gammavariate_list(n=list(), alpha=list(), beta=list()):
    result = []
    for i in range(len(n)):
        random_list = generate_gammavariate_list(n[i], alpha[i], beta[i])
        result += random_list

    return sorted(result)
    # sample: generate_hyper_gammavariate_list([300, 700], [5, 45], [501.266, 136.709])


def make_workload(test, desired_average_runtime=30):
    """

    :param test:db.definitions.Test
    :param desired_average_runtime:
    :return:
    """
    # ----------Read Jobs:
    all_jobs = {'S': Definitions.WorkflowReader.reader.read_jobs('S.txt'),  # 25-30
                'M': Definitions.WorkflowReader.reader.read_jobs('M.txt'),  # 50-60
                'L': Definitions.WorkflowReader.reader.read_jobs('L.txt'),  # 100
                'XXL': Definitions.WorkflowReader.reader.read_jobs('XXL.txt')}  # 1000

    workload_len = test.workload_len
    bandwidth = test.bandwidth

    job_size = generate_hyper_gammavariate_list([int(0.3 * workload_len), int(0.7 * workload_len)],
                                                [5, 45], [501.266, 136.709])

    current_average_runtime = sum(job_size) / workload_len

    # 60 is multiplied in order to map workflow sizes to 60 timeslots. (they were set before for timeslot = 1)
    job_size = list(map(lambda s: 60 * s * desired_average_runtime / current_average_runtime, job_size))
    small_ratio = test.small
    medium_ratio = test.medium
    large_ratio = test.large
    budget_constraint_ratio = test.budget_ratio
    # small_ratio = 0.5
    # medium_ratio = 0.3
    # large_ratio = 0.2
    # budget_constraint_ratio = 0.5

    workflow_names = ['Montage', 'CyberShake', 'Epigenomics', 'Inspiral']  # 'Sipht',

    small_size = small_ratio * workload_len
    medium_size = medium_ratio * workload_len
    large_size = large_ratio * workload_len
    job = []
    constraint = []
    sizes = []
    names = []
    for i in range(workload_len):
        randomly_selected_workflow = random.randint(0, len(workflow_names) - 1)
        name = workflow_names[randomly_selected_workflow]

        if small_size != 0:
            selected_size = 'S'
            small_size -= 1
        elif medium_size != 0:
            mediums = ['M', 'L']
            selected_size = mediums[random.randint(0, 1)]
            medium_size -= 1
        elif large_size != 0:
            selected_size = 'XXL'
            large_size -= 1
        else:
            break
        g = copy.deepcopy(all_jobs[selected_size][name])
        sizes.append(selected_size)
        names.append(name)

        c = 'Deadline'
        if random.random() >= 1 - budget_constraint_ratio:
            constraint.append(Constraint.budget)
            c = 'Budget'
        else:
            constraint.append(Constraint.deadline)
        print("{0: >3}.{1:12} | Constraint: {4:>8} | "
              "Nodes:{2:4d} | Size:{3:5.2f}".format(selected_size, name, len(g.tasks) - 2, job_size[i], c))
        g.set_name(''.join(random.choice(string.ascii_lowercase) for _ in range(5)))
        # Scheduler.Multi_Workflow.randomize_dag(g)
        normalize_dag(g, job_size[i], bandwidth / 10)
        job.append(g)

    return job, constraint, names, sizes


def make_static_workload(BW):
    # ----------Read Jobs:
    all_jobs = {'S': Definitions.WorkflowReader.reader.read_jobs('S.txt'),  # 25-30
                'M': Definitions.WorkflowReader.reader.read_jobs('M.txt'),  # 50-60
                'L': Definitions.WorkflowReader.reader.read_jobs('L.txt'),  # 100
                'XXL': Definitions.WorkflowReader.reader.read_jobs('XXL.txt')}  # 1000

    job_size = [8, 10, 28, 29, 30, 32, 36, 40, 41, 43]

    workload_len = 10
    desired_average_runtime = 30  # on time-unit
    current_average_runtime = sum(job_size) / workload_len
    job_size = list(map(lambda s: s * desired_average_runtime / current_average_runtime, job_size))

    workflow_names = ['Montage', 'CyberShake', 'Epigenomics', 'Sipht', 'Inspiral']

    workflow_indices = [2, 1, 0, 4, 1, 1, 2, 0, 2, 4]
    # sizes = ['S', 'S', 'S', 'S', 'S', 'L', 'M', 'L', 'M', 'M']
    sizes = ['L'] * 10
    c = 'Deadline'

    job = []
    constraint = []
    for i in range(workload_len):
        selected_workflow_index = workflow_indices[i]
        name = workflow_names[selected_workflow_index]
        selected_size = sizes[i]
        g = copy.deepcopy(all_jobs[selected_size][name])

        # c = 'Deadline'
        # c = 'Budget'
        if c == 'Deadline':
            c = 'Budget'
            constraint.append(Constraint.budget)
        else:
            c = 'Deadline'
            constraint.append(Constraint.deadline)

        print("{0: >3}.{1:12} | Constraint: {4:>8} | "
              "Nodes:{2:4d} | Size:{3:5.2f}".format(selected_size, name, len(g.tasks) - 2, job_size[i], c))
        g.set_name(i)
        # Scheduler.Multi_Workflow.randomize_dag(g)
        normalize_dag(g, job_size[i], BW / 10)
        job.append(g)

    return job, constraint


def make_static_workload_2(BW):
    # ----------Read Jobs:
    all_jobs = {'S': Definitions.WorkflowReader.reader.read_jobs('S.txt'),  # 25-30
                'M': Definitions.WorkflowReader.reader.read_jobs('M.txt'),  # 50-60
                'L': Definitions.WorkflowReader.reader.read_jobs('L.txt'),  # 100
                'XXL': Definitions.WorkflowReader.reader.read_jobs('XXL.txt')}  # 1000

    job_size = [8, 8, 10, 10, 28, 28, 29, 29, 30, 30, 32, 32, 36, 36, 40, 40, 41, 41, 43, 43]

    workload_len = 20
    desired_average_runtime = 30  # on time-unit
    current_average_runtime = sum(job_size) / workload_len
    job_size = list(map(lambda s: s * desired_average_runtime / current_average_runtime, job_size))

    workflow_names = ['Montage', 'CyberShake', 'Epigenomics', 'Sipht', 'Inspiral']

    workflow_indices = [2, 2, 1, 1, 0, 0, 4, 4, 1, 1, 1, 1, 2, 2, 0, 0, 2, 2, 4, 4]
    # sizes = ['S', 'S', 'S', 'S', 'S', 'L', 'M', 'L', 'M', 'M']
    sizes = ['L'] * 20
    c = 'Deadline'

    job = []
    constraint = []
    for i in range(workload_len):
        selected_workflow_index = workflow_indices[i]
        name = workflow_names[selected_workflow_index]
        selected_size = sizes[i]
        g = copy.deepcopy(all_jobs[selected_size][name])

        # c = 'Deadline'
        # c = 'Budget'
        if c == 'Deadline':
            c = 'Budget'
            constraint.append(Constraint.budget)
        else:
            c = 'Deadline'
            constraint.append(Constraint.deadline)

        print("{0: >3}.{1:12} | Constraint: {4:>8} | "
              "Nodes:{2:4d} | Size:{3:5.2f}".format(selected_size, name, len(g.tasks) - 2, job_size[i], c))
        g.set_name(i)
        # Scheduler.Multi_Workflow.randomize_dag(g)
        normalize_dag(g, job_size[i], BW / 10)
        job.append(g)

    return job, constraint
