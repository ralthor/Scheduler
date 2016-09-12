from __future__ import print_function

import math

import db
from db import writer
from enum import Enum

from Definitions.Graph import Task


class Constraint(Enum):
    deadline = 1
    budget = 2
    none = 3


def f_range(x, y, jump):
    while x < y:
        yield x
        x += jump


class TaskSchedule:
    def __init__(self, task, est=-1, runtime=-1, eft=-1, resource=-1):
        self.task = task
        self.EFT = eft
        self.EST = est
        self.runtime = runtime
        self.resource = resource


class Resources(object):
    len = -1
    bandwidth = 0

    def __init__(self, powers, bandwidth):  # e.g. [1,1,2,2,4]
        number_of_resources = len(powers)
        self.power = powers
        self.tasksOfResource = []  # ordered set of TaskSchedule objects in every resource
        for i in range(number_of_resources):
            self.tasksOfResource.append([])
        self.len = number_of_resources
        self.bandwidth = bandwidth
        self.job_task_schedule = {}  # job_task_schedule['Mine_10_1'][4].EFT == 12

    def find_gap(self, resource, start_time, runtime):
        if resource == -1:
            return start_time, -1
        number_of_tasks = len(self.tasksOfResource[resource])
        if number_of_tasks == 0:
            return start_time, 0
        elif self.tasksOfResource[resource][0].EST >= start_time + runtime:
            return start_time, 0
        elif number_of_tasks == 1:
            if self.tasksOfResource[resource][0].EFT < start_time:
                return start_time, 1
            else:
                return self.tasksOfResource[resource][0].EFT, 1
        else:
            for i in range(1, number_of_tasks):
                if self.tasksOfResource[resource][i].EST <= start_time:
                    continue
                elif start_time < self.tasksOfResource[resource][i - 1].EFT:
                    gap = self.tasksOfResource[resource][i].EST - self.tasksOfResource[resource][i - 1].EFT
                    if gap < runtime:
                        continue
                    else:
                        return self.tasksOfResource[resource][i - 1].EFT, i
                elif self.tasksOfResource[resource][i - 1].EFT <= start_time < self.tasksOfResource[resource][i].EST:
                    if self.tasksOfResource[resource][i].EST - start_time < runtime:
                        continue
                    else:
                        return start_time, i
            else:  # no gap is found, put it at the end (it can be done using append method)
                return max(self.tasksOfResource[resource][-1].EFT, start_time), -1

    def calculate_eft(self, task, resource_id):
        g = task.graph
        if resource_id == -1:
            graphs_task_on_resource = []
            task_runtime_on_resource = task.weight / max(self.power)
        else:
            task_runtime_on_resource = task.weight / self.power[resource_id]
            graphs_task_on_resource = list(
                map(lambda t: t.task.id if t.task.graph.name == g.name else -1, self.tasksOfResource[resource_id]))
        max_est_of_task = 0
        for p in task.predecessor:
            # check if p and task.id on the same resource_id
            if p in graphs_task_on_resource:
                communication_delay = 0
            else:
                communication_delay = task.predecessor[p] / self.bandwidth
            if g.name not in self.job_task_schedule or p not in self.job_task_schedule[g.name]:
                continue  # TODO: this keyError may happen only in Sipht 100, for key 15 and 24. But why? Find and Fix.
            p_eft = self.job_task_schedule[g.name][p].EFT
            if p_eft + communication_delay > max_est_of_task:
                max_est_of_task = p_eft + communication_delay
        # EST Of Task is found and stored in max_est_of_task
        # Find a gap to schedule it:
        start_time, place_id = self.find_gap(resource_id, max_est_of_task, task_runtime_on_resource)
        eft_task = start_time + task_runtime_on_resource
        return start_time, eft_task, task_runtime_on_resource, place_id

    def schedule(self, task_schedule, place_id=-1):
        """
        Schedules a task in a place id. if place_id is -1 the schedule is appended to the last.
        :type task_schedule: TaskSchedule
        :type place_id: int
        """
        resource = task_schedule.resource
        if place_id == -1:
            self.tasksOfResource[resource].append(task_schedule)
        else:
            self.tasksOfResource[resource].insert(place_id, task_schedule)
        if task_schedule.task.graph.name in self.job_task_schedule:
            pass
        else:
            self.job_task_schedule[task_schedule.task.graph.name] = {}
        self.job_task_schedule[task_schedule.task.graph.name][task_schedule.task.id] = task_schedule

    def show_schedule(self, job_id, finishing=None):
        for r in range(0, self.len):
            names = []
            est = []
            eft = []

            def add_entries(x):
                if x.task.graph.name != job_id:
                    return
                names.append(x.task.id)
                est.append(x.EST)
                eft.append(x.EFT)

            list(map(add_entries, self.tasksOfResource[r]))

            def print_list(x):
                first = True
                for e in x:
                    if first:
                        first = False
                    else:
                        print(',', end=' ')
                    print(e, end=' ')
                print()

            print_list(names)
            print_list(est)
            print_list(eft)
        if finishing is not None:
            print(finishing)

    def write_schedule(self, db_file, test_name='N/A', extra='single', policy='', job_count=1):
        w = writer.Writer(db_file)
        w.create_plan()

        w.create_plan_head()
        unique_jobs_id = w.write_plan_head(test_name, policy, job_count)

        def add_entries(x):
            job_name, job_type, task_id, jobs_id, start_time,\
            finish_time, resource_id, resource_speed, \
            job_component_id, extra_params = x.task.graph.name, x.task.graph.type, x.task.id, unique_jobs_id\
                                             , x.EST,\
                                             x.EFT, r, self.power[r], policy, extra

            w.write_plan(job_name, job_type, task_id, jobs_id, start_time, finish_time, resource_id,
                         resource_speed, job_component_id, extra_params)

        for r in range(0, self.len):
            list(map(add_entries, self.tasksOfResource[r]))

        w.commit()
        w.close()

    @property
    def average_power(self):
        return math.fsum(self.power) / self.len

    @property
    def makespan(self):
        eft = 0
        for i in range(0, self.len):
            tasks_in_resource = self.tasksOfResource[i]
            if len(tasks_in_resource) == 0:
                continue
            eft = max(eft, tasks_in_resource[-1].EFT)
        return eft

    def sum_gaps_resource(self, resource_id):
        tasks_in_current_resource = self.tasksOfResource[resource_id]
        num_tasks = len(tasks_in_current_resource)
        if num_tasks <= 1:
            return 0
        sum_gaps = 0
        for i in range(1, num_tasks):
            if tasks_in_current_resource[i - 1].task.dummy_task or tasks_in_current_resource[i].task.dummy_task:
                continue
            finish_prev = tasks_in_current_resource[i - 1].EFT
            start_current = tasks_in_current_resource[i].EST
            gap_length = start_current - finish_prev
            if gap_length < 0:
                raise Exception('Schedule is not correct, check gaps!')
            sum_gaps += gap_length
        return sum_gaps

    @property
    def sum_internal_gaps(self):
        sum_gaps = 0
        for r in range(0, self.len):
            sum_gaps += self.sum_gaps_resource(r)
        return sum_gaps

    def select_resource(self, task):
        est_best, eft_best, runtime_on_resource_best, place_id_best, resource_id_best = -1, -1, -1, -1, -1
        for r in range(0, self.len):
            max_est_of_task, eft_task, task_runtime_on_resource, place_id = self.calculate_eft(task, r)
            if eft_best == -1 or eft_task < eft_best:
                est_best, eft_best, runtime_on_resource_best, place_id_best, resource_id_best = \
                    max_est_of_task, eft_task, task_runtime_on_resource, place_id, r
        return est_best, runtime_on_resource_best, eft_best, resource_id_best, place_id_best

    def get_fastest_empty_resource(self):
        for r in range(self.len - 1, -1, -1):
            if len(self.tasksOfResource[r]) == 0:
                return r
        else:
            return -1


class CostAwareResources(Resources):
    def __init__(self, powers, prices, timeslot_len, bandwidth):
        super(CostAwareResources, self).__init__(powers, bandwidth)
        self.timeslot = timeslot_len
        self.price = prices
        self.head_nodes = {}
        self.sum_weight_scheduled = {}

    def resource_cost(self, resource_id, start_time=-1, eft=-1, cost_only=True):
        """
        computes a resource's cost. if cost_only==True, only returns cost, else it returns also start and finish-times.
        :param resource_id:
        :param start_time:
        :param eft:
        :param cost_only:
        :return:
        """
        tasks_in_resource = self.tasksOfResource[resource_id]
        sum_runtime = sum(map(lambda s: s.runtime, tasks_in_resource))
        if sum_runtime == 0:
            return 0 if cost_only else (0, 0, 0)
        length = len(tasks_in_resource)
        start_index = 0
        end_index = -1
        if length > 0 and tasks_in_resource[0].task.dummy_task:
            length -= 1
            start_index = 1
        if length > 0 and tasks_in_resource[-1].task.dummy_task:
            length -= 1
            end_index = -2
        if length <= 0:
            if eft == -1:
                return 0 if cost_only else (0, -1, -1)
            else:
                return math.ceil((eft - start_time) / self.timeslot[resource_id]) * self.price[resource_id]
        if start_time != -1:
            task_start_time = min(tasks_in_resource[start_index].EST, start_time)
        else:
            task_start_time = tasks_in_resource[start_index].EST
        task_finish_time = max(tasks_in_resource[end_index].EFT, eft)
        reservation = task_finish_time - task_start_time
        cost = math.ceil(reservation / self.timeslot[resource_id]) * self.price[resource_id]
        if cost_only:
            return cost
        else:
            return cost, task_start_time, task_finish_time

    def resource_start_time(self, resource_id):
        tasks_in_resource = self.tasksOfResource[resource_id]
        length = len(tasks_in_resource)
        start_index = 0
        while length > 0 and tasks_in_resource[start_index].task.dummy_task:
            start_index += 1
            length -= 1
        if length == 0:
            return -1
        return tasks_in_resource[start_index].EST

    @property
    def plan_cost(self):
        cost = 0
        for i in range(0, self.len):
            cost += self.resource_cost(i)
        return cost

    def calculate_shared_cost_within_timeslot(self, timeslot_start, est, ft, resource_id, task_id=None):
        timeslot_end = timeslot_start + self.timeslot[resource_id]
        if ft <= timeslot_start or est >= timeslot_end:
            return 0
        tasks = self.tasksOfResource[resource_id]
        task_ids = self.task_id_in_timeslot(resource_id, timeslot_start)
        sum_w = 0
        for id in task_ids:
            if task_id == id:
                continue
            start_time = tasks[id].EST
            finish_time = tasks[id].EFT
            if start_time < timeslot_start:
                start_time = timeslot_start
            if finish_time > timeslot_end:
                finish_time = timeslot_end
            sum_w += finish_time - start_time
        if est < timeslot_start:
            est = timeslot_start
        if ft > timeslot_end:
            ft = timeslot_end
        if ft == est:
            return 0
        share = float(ft - est) / (sum_w + ft - est)
        return share * self.price[resource_id]

    def task_id_in_timeslot(self, resource_id, timeslot_start):
        timeslot_end = timeslot_start + self.timeslot[resource_id]
        task_ids = []

        for id in range(len(self.tasksOfResource[resource_id])):
            s = self.tasksOfResource[resource_id][id]
            if timeslot_start <= s.EST <= timeslot_end or timeslot_start <= s.EFT <= timeslot_end \
                    or s.EST < timeslot_start and timeslot_end < s.EFT:
                task_ids.append(id)
        return task_ids

    def calculate_task_shared_cost(self, est=-1, ft=-1, resource_id=-1, task_id=None):
        if task_id is not None:
            # this task has already been scheduled
            est = self.tasksOfResource[resource_id][task_id].EST
            ft = self.tasksOfResource[resource_id][task_id].EFT

        timeslot_len = self.timeslot[resource_id]
        resource_start_time = self.resource_start_time(resource_id)
        if resource_start_time == -1:
            resource_start_time = est
        timeslot_start = float(timeslot_len) * math.floor((est - resource_start_time) /
                                                          timeslot_len) + resource_start_time
        timeslot_end = float(timeslot_len) * math.ceil((ft - resource_start_time) /
                                                       timeslot_len) + resource_start_time
        shared_cost = 0
        for interval in f_range(timeslot_start, timeslot_end + timeslot_len / 2, timeslot_len):
            share_in_interval = self.calculate_shared_cost_within_timeslot(interval, est, ft, resource_id, task_id)
            shared_cost += share_in_interval
        return shared_cost

    def calculate_eft_and_cost(self, task, resource_id):
        """
        calculates eft and cost of a certain task on a certain resource.
        :param task:Definitions.Task()
        :param resource_id:
        :return:
        """
        start_time, eft, runtime_on_resource, place_id = self.calculate_eft(task, resource_id)
        if task.dummy_task:
            return start_time, eft, runtime_on_resource, place_id, 0
        else:
            # cost_with_task = self.resource_cost(resource_id, start_time, eft)
            # cost_without_task = self.resource_cost(resource_id)
            # cost = cost_with_task - cost_without_task
            # cost = self.calculate_task_shared_cost(start_time, eft, resource_id)
            cost = self.calculate_share_cost_change(resource_id, start_time, eft, task.graph.name, True)
            return start_time, eft, runtime_on_resource, place_id, cost

    def sum_external_gaps_resource(self, r):
        c, s, e = self.resource_cost(r, cost_only=False)
        reservation = e - s
        timeslot = self.timeslot[r]
        gap = timeslot - reservation % timeslot
        if gap == timeslot:
            return 0
        else:
            return gap

    @property
    def sum_external_gaps(self):
        sum_gaps = 0
        for r in range(0, self.len):
            sum_gaps += self.sum_external_gaps_resource(r)
        return sum_gaps

    @property
    def sum_gaps(self):
        return self.sum_internal_gaps + self.sum_external_gaps

    @property
    def occupied_resources(self):
        counter = 0
        for i in range(self.len):
            if self.resource_cost(i) != 0:
                counter += self.price[i]
        return counter

    @property
    def gap_rate(self):
        return self.sum_gaps / self.makespan / self.occupied_resources

    def select_resource(self, task=Task(), test=None):
        eft_best = -1
        def something_found():
            return eft_best != -1
        #
        # if test is not None:
        #     remaining_budget = task.graph.remaining_budget
        #     task.sub_budget = ?

        if task.asap is not None:
            if not task.asap:  # budget workflow
                if not test:
                    print('', end='')
                # fastest affordable
                est_best, eft_best, runtime_on_resource_best, place_id_best, resource_id_best, cost_best = \
                    -1, -1, -1, -1, -1, -1

                for r in range(0, self.len):
                    start_time, eft, runtime_on_resource, place_id, cost = self.calculate_eft_and_cost(task, r)
                    # if eft_best == -1 or eft_best > eft > task.sub_deadline or task.sub_deadline >= eft and (
                    #                 cost < cost_best or eft_best > task.sub_deadline):
                    if not something_found() or \
                            eft < eft_best  and task.sub_deadline < eft_best or \
                            task.sub_budget < cost_best and eft <= task.sub_deadline and cost < cost_best or \
                            eft <= task.sub_deadline and cost <= task.sub_budget and \
                            (eft_best > task.sub_deadline or cost_best > task.sub_budget) or \
                            eft <= task.sub_deadline and cost <= task.sub_budget and eft < eft_best or \
                            eft <= task.sub_deadline and cost <= task.sub_budget and eft == eft_best and cost < cost_best:
                        est_best, eft_best, runtime_on_resource_best, place_id_best, resource_id_best, cost_best = \
                            start_time, eft, runtime_on_resource, place_id, r, cost
                        continue
                if not test:
                    print('', end='')
                return est_best, runtime_on_resource_best, eft_best, resource_id_best, place_id_best, cost_best
            elif task.asap:  # deadline workflow
                # cheapest before sub-deadline
                if not test:
                    print('', end='')
                est_best, eft_best, runtime_on_resource_best, place_id_best, resource_id_best, cost_best = \
                    -1, -1, -1, -1, -1, -1
                for r in range(0, self.len):
                    start_time, eft, runtime_on_resource, place_id, cost = self.calculate_eft_and_cost(task, r)
                    # if eft_best == -1 or eft_best > eft > task.sub_deadline or task.sub_deadline >= eft and (
                    #                 cost < cost_best or eft_best > task.sub_deadline):
                    if not something_found() or \
                            eft < eft_best  and task.sub_deadline < eft_best or \
                            task.sub_budget < cost_best and eft <= task.sub_deadline and cost < cost_best or \
                            eft <= task.sub_deadline and cost <= task.sub_budget and \
                            (eft_best > task.sub_deadline or cost_best > task.sub_budget) or \
                            eft <= task.sub_deadline and cost <= task.sub_budget and cost < cost_best or \
                            eft <= task.sub_deadline and cost <= task.sub_budget and cost == cost_best and eft < eft_best:
                        est_best, eft_best, runtime_on_resource_best, place_id_best, resource_id_best, cost_best = \
                            start_time, eft, runtime_on_resource, place_id, r, cost
                        # if cost_best == -1 or cost_best > cost > task.sub_budget or task.sub_budget >= cost and (
                        #                 eft < eft_best or cost_best > task.sub_budget):
                        #     est_best, eft_best, runtime_on_resource_best, place_id_best, resource_id_best, cost_best = \
                        #         start_time, eft, runtime_on_resource, place_id, r, cost
                        continue
                if not test:
                    print('', end='')
                return est_best, runtime_on_resource_best, eft_best, resource_id_best, place_id_best, cost_best
        else:
            # minimize time (as in HEFT) TODO: it doesn't return cost (as the sixth return value)
            return super(CostAwareResources, self).select_resource(task)

    def price_of_each_graph(self):
        graph_names = self.job_task_schedule.keys()
        costs = {}
        for name in graph_names:
            costs[name] = 0
        for r in range(self.len):
            for id in range(len(self.tasksOfResource[r])):
                name = self.tasksOfResource[r][id].task.graph.name
                cost = self.calculate_task_shared_cost(resource_id=r, task_id=id)
                costs[name] += cost
        return costs

    def get_cheapest_empty_resource(self):
        for r in range(self.len):
            if len(self.tasksOfResource[r]) == 0:
                return r
        else:
            return -1

    def schedule(self, task_schedule, place_id=-1, do_head_nodes=False):
        super(CostAwareResources, self).schedule(task_schedule, place_id)

        # head_node computations:
        if not do_head_nodes:
            return
        if task_schedule.task.graph.name in self.head_nodes:
            prev_heads = self.head_nodes[task_schedule.task.graph.name]
            parents_of_current_task = task_schedule.task.predecessor.keys()
            self.head_nodes[task_schedule.task.graph.name] = self.head_nodes[task_schedule.task.graph.name].difference(
                parents_of_current_task)
            self.head_nodes[task_schedule.task.graph.name].add(task_schedule.task.id)
        else:
            self.head_nodes[task_schedule.task.graph.name] = set()
            self.head_nodes[task_schedule.task.graph.name].add(task_schedule.task.id)
            self.sum_weight_scheduled[task_schedule.task.graph.name] = 0

        self.sum_weight_scheduled[task_schedule.task.graph.name] += task_schedule.task.weight

    def calculate_share_cost_change(self, resource_id, est=-1, eft=-1, job_id=-1, only_this_job=False):
        sum_w = {}
        for i in range(len(self.tasksOfResource[resource_id])):
            sch = self.tasksOfResource[resource_id][i]
            job = sch.task.graph.name
            if job not in sum_w:
                sum_w[job] = 0
            sum_w[job] += sch.EFT - sch.EST
        sum_w_all_old = sum(sum_w.values())
        prev_cost_resource = self.resource_cost(resource_id)
        prev_cost_job = {}
        for j in sum_w.keys():
            if sum_w_all_old == 0:
                prev_cost_job[j] = 0
            else:
                prev_cost_job[j] = float(prev_cost_resource) * sum_w[j] / sum_w_all_old
        if est == -1:
            return prev_cost_job

        new_cost_resource = prev_cost_resource = self.resource_cost(resource_id, start_time=est, eft=eft)
        if job_id not in sum_w:
            sum_w[job_id] = 0
        sum_w[job_id] += eft - est
        sum_w_all_new = sum_w_all_old + eft - est

        new_cost_job = {}
        changes = {}
        for j in sum_w.keys():
            if sum_w_all_new == 0:
                new_cost_job[j] = 0
            else:
                new_cost_job[j] = float(new_cost_resource) * sum_w[j] / sum_w_all_new
            if j not in prev_cost_job:
                changes[j] = new_cost_job[j]
            else:
                changes[j] = new_cost_job[j] - prev_cost_job[j]
        if only_this_job:
            return changes[job_id]
        return changes
