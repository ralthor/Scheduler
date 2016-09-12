import math
import Scheduler.HEFT
import Definitions.Resources


def schedule(g, resources, budget, upward_rank_is_calculated=False, priority_list=None):
    """
    Schedules using BHEFT algorithm
    :type g: Graph
    :type resources: Definitions.Resources.CostAwareResources
    :type budget: float
    """
    if not upward_rank_is_calculated:
        g.upward_rank(g.startID, resources.average_power, resources.bandwidth)
    if priority_list is None:
        priority_list = Scheduler.HEFT.list_of_task_id_on_upward_rank(g)

    sum_budget_remaining = 0
    for i in range(0, resources.len):
        sum_budget_remaining += resources.price[i] / (resources.timeslot[i] * resources.power[i])
    sum_budget_allocated = 0
    average_price_of_computation = sum_budget_remaining / resources.len

    sum_weight = math.fsum(map(lambda t: t.weight, g.tasks.values()))
    sum_budget_remaining = average_price_of_computation * sum_weight
    for tId in priority_list:
        est_best, eft_best, runtime_on_resource_best, place_id_best, resource_id_best, cost_best = \
            -1, -1, -1, -1, -1, -1
        sab_k = budget - sum_budget_allocated - sum_budget_remaining
        ctb_k = g.tasks[tId].weight * average_price_of_computation
        if sab_k >= 0 and sum_budget_remaining != 0:
            ctb_k += sab_k + ctb_k * sab_k / sum_budget_remaining
        affordable_found = False
        for r in range(0, resources.len):
            start_time, eft, runtime_on_resource, place_id, cost = resources.calculate_eft_and_cost(
                g.tasks[tId], r)
            if (not affordable_found and cost <= ctb_k) or (
                    affordable_found and cost <= ctb_k and eft < eft_best) or (
                not affordable_found and sab_k >= 0 and (eft < eft_best or eft_best == -1)) or(
                not affordable_found and sab_k < 0 and (cost < cost_best or cost_best == -1)
            ):
                if cost <= ctb_k:
                    affordable_found = True
                est_best, eft_best, runtime_on_resource_best, place_id_best, resource_id_best, cost_best = \
                    start_time, eft, runtime_on_resource, place_id, r, cost
                continue

        task_schedule = Definitions.Resources.TaskSchedule(g.tasks[tId], est_best, runtime_on_resource_best, eft_best,
                                                           resource_id_best)
        sum_budget_remaining -= g.tasks[tId].weight * average_price_of_computation
        sum_budget_allocated += cost_best
        resources.schedule(task_schedule, place_id_best)

# sample call series for this (above) algorithm:
# min_resources = Definitions.Resources.CostAwareResources([1], [1], [5], BW)
# Scheduler.HEFT.schedule(g, min_resources)
# cost_min = min_resources.plan_cost
#
# max_resources = Definitions.Resources.CostAwareResources([1, 2], [1, 3], [5, 5], BW)
# Scheduler.HEFT.schedule(g, max_resources)
# cost_max = max_resources.plan_cost
#
# for alpha in range(3, 10, 2):
#     budget = cost_min + alpha / 10 * (cost_max - cost_min)
#     resources = Definitions.Resources.CostAwareResources([1, 2], [1, 3], [5, 5], BW)
#     Scheduler.BHEFT.schedule(g, resources, budget)
#     # resources.show_schedule(g.name)
#     # print('--')
#     cost = resources.plan_cost
#     print(budget, cost, resources.makespan)
#     print('--')


class SchedulerClass:
    def __init__(self, g, resources, budget, upward_rank_is_calculated=False, priority_list=None):
        if not upward_rank_is_calculated:
            g.upward_rank(g.startID, resources.average_power, resources.bandwidth)
        if priority_list is None:
            self.priority_list = Scheduler.HEFT.list_of_task_id_on_upward_rank(g)
        self.g = g
        self.resources = resources
        self.last_unscheduled_task_id = 0

        self.sum_budget_remaining = 0
        for i in range(0, resources.len):
            self.sum_budget_remaining += resources.price[i] / (resources.timeslot[i] * resources.power[i])
        self.sum_budget_allocated = 0
        self.average_price_of_computation = self.sum_budget_remaining / resources.len

        sum_weight = math.fsum(map(lambda t: t.weight, g.tasks.values()))
        self.sum_budget_remaining = self.average_price_of_computation * sum_weight
        self.budget = budget

    def schedule_next(self, only_test=False):
        if self.last_unscheduled_task_id not in range(0, len(self.priority_list)):
            return
        t_id = self.priority_list[self.last_unscheduled_task_id]
        task = self.g.tasks[t_id]
        est_best, eft_best, runtime_on_resource_best, place_id_best, resource_id_best, cost_best = \
            -1, -1, -1, -1, -1, -1
        sab_k = self.budget - self.sum_budget_allocated - self.sum_budget_remaining
        ctb_k = task.weight * self.average_price_of_computation
        if sab_k >= 0 and self.sum_budget_remaining != 0:
            ctb_k += sab_k + ctb_k * sab_k / self.sum_budget_remaining
        affordable_found = False
        for r in range(0, self.resources.len):
            start_time, eft, runtime_on_resource, place_id, cost = self.resources.calculate_eft_and_cost(task, r)
            if (not affordable_found and cost <= ctb_k) or (
                    affordable_found and cost <= ctb_k and eft < eft_best) or (
                not affordable_found and sab_k >= 0 and (eft < eft_best or eft_best == -1)) or(
                not affordable_found and sab_k < 0 and (cost < cost_best or cost_best == -1)
            ):
                if cost <= ctb_k:
                    affordable_found = True
                est_best, eft_best, runtime_on_resource_best, place_id_best, resource_id_best, cost_best = \
                    start_time, eft, runtime_on_resource, place_id, r, cost
                continue

        if not only_test:
            task_schedule = Definitions.Resources.TaskSchedule(task, est_best, runtime_on_resource_best, eft_best,
                                                               resource_id_best)
            self.sum_budget_remaining -= task.weight * self.average_price_of_computation
            self.sum_budget_allocated += cost_best
            self.resources.schedule(task_schedule, place_id_best)
            self.last_unscheduled_task_id += 1
        else:
            return eft_best, cost_best

    @property
    def finished(self):
        return self.last_unscheduled_task_id >= len(self.priority_list)

# sample call series for this (above) algorithm:
# min_resources = Definitions.Resources.CostAwareResources([1], [1], [5], BW)
# Scheduler.HEFT.schedule(g, min_resources)
# cost_min = min_resources.plan_cost
#
# max_resources = Definitions.Resources.CostAwareResources([1, 2], [1, 3], [5, 5], BW)
# Scheduler.HEFT.schedule(g, max_resources)
# cost_max = max_resources.plan_cost
#
# for alpha in range(3, 10, 2):
#     budget = cost_min + alpha / 10 * (cost_max - cost_min)
#     resources = Definitions.Resources.CostAwareResources([1, 2], [1, 3], [5, 5], BW)
#     bheft = Scheduler.BHEFT.SchedulerClass(g, resources, budget)
#     while not bheft.finished
#         bheft.schedule_next()
#     # resources.show_schedule(g.name)
#     # print('--')
#     cost = resources.plan_cost
#     print(budget, cost, resources.makespan)
#     print('--')
