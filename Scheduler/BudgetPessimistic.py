import math
import Scheduler.HEFT
import Definitions.Resources


def schedule(g, resources, budget, upward_rank_is_calculated=False, priority_list=None):
    """
    Schedules using BudgetPessimistic algorithm, it tries not to go further than the Budget, if so, it selects the
    less cost effective resource
    :param priority_list: list
    :param upward_rank_is_calculated: boolean
    :type g: Graph
    :type resources: Definitions.Resources.CostAwareResources
    :type budget: float
    """
    if not upward_rank_is_calculated:
        g.upward_rank(g.startID, resources.average_power, resources.bandwidth)
    if priority_list is None:
        priority_list = Scheduler.HEFT.list_of_task_id_on_upward_rank(g)

    sum_budget_remaining = budget
    sum_budget_allocated = 0

    sum_weight_remaining = math.fsum(map(lambda t: t.weight, g.tasks.values()))
    sum_weight_allocated = 0
    for t_id in priority_list:
        task = g.tasks[t_id]
        if sum_weight_remaining == 0:
            task.sub_budget = 0
        else:
            task.sub_budget = task.weight / sum_weight_remaining * sum_budget_remaining
        # resource selection:
        est, runtime_on_resource, eft, resource_id, place_id, cost = resources.select_resource(task)

        # scheduling:
        task_schedule = Definitions.Resources.TaskSchedule(task, est, runtime_on_resource, eft, resource_id)
        resources.schedule(task_schedule, place_id)

        # allocation and remaining budget and weight:
        sum_weight_remaining -= task.weight
        sum_weight_allocated += task.weight

        sum_budget_remaining -= cost
        sum_budget_allocated += cost


class SchedulerClass:
    def __init__(self, g, resources, budget, upward_rank_is_calculated=False, priority_list=None):
        if not upward_rank_is_calculated:
            g.upward_rank(g.startID, resources.average_power, resources.bandwidth)
        if priority_list is None:
            self.priority_list = Scheduler.HEFT.list_of_task_id_on_upward_rank(g)
        self.g = g
        self.resources = resources
        self.last_unscheduled_task_id = 0

        self.sum_budget_remaining = budget
        self.sum_budget_allocated = 0
        self.sum_weight_remaining = math.fsum(map(lambda t: t.weight, g.tasks.values()))
        self.sum_weight_allocated = 0

    def schedule_next(self, only_test=False):
        if self.last_unscheduled_task_id not in range(0, len(self.priority_list)):
            return
        t_id = self.priority_list[self.last_unscheduled_task_id]
        task = self.g.tasks[t_id]
        if self.sum_weight_remaining == 0:
            task.sub_budget = 0
        else:
            task.sub_budget = task.weight / self.sum_weight_remaining * self.sum_budget_remaining
        # resource selection:
        est, runtime_on_resource, eft, resource_id, place_id, cost = self.resources.select_resource(task)

        if not only_test:
            # scheduling:
            task_schedule = Definitions.Resources.TaskSchedule(task, est, runtime_on_resource, eft, resource_id)
            self.resources.schedule(task_schedule, place_id)

            # allocation and remaining budget and weight:
            self.sum_weight_remaining -= task.weight
            self.sum_weight_allocated += task.weight

            self.sum_budget_remaining -= cost
            self.sum_budget_allocated += cost
            self.last_unscheduled_task_id += 1
        else:
            return eft, cost

    @property
    def finished(self):
        return self.last_unscheduled_task_id >= len(self.priority_list)
