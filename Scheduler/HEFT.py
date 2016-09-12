import Definitions.Resources


def list_of_task_id_on_upward_rank(g):
    """
    calculates and returns an ordered list of task ids based on their decreasing rank
    :type g: Definitions.Graph.Graph
    :rtype : list
    """
    task_list = [g.startID]
    ready_tasks = {g.startID}
    children_pool = {}
    for p in ready_tasks:
        children = g.tasks[p].successor
        children = set(children.keys())
        children_pool = set.union(children, children_pool)
    ready_tasks = children_pool
    while len(ready_tasks) != 0:
        p = max(ready_tasks, key=lambda x: g.tasks[x].upward_rank)
        task_list.append(p)
        ready_tasks.remove(p)
        children = g.tasks[p].successor
        children = set(children.keys())
        ready_tasks = set.union(children, ready_tasks)
    return task_list


def schedule(g, resources, upward_rank_is_calculated=False, priority_list=None):
    if not upward_rank_is_calculated:
        g.upward_rank(g.startID, resources.average_power, resources.bandwidth)
    if priority_list is None:
        priority_list = list_of_task_id_on_upward_rank(g)

    for tId in priority_list:
        task = g.tasks[tId]
        est_best, runtime_on_resource_best, eft_best, resource_id_best, place_id_best = resources.select_resource(task)
        task_schedule = Definitions.Resources.TaskSchedule(task, est_best, runtime_on_resource_best, eft_best,
                                                           resource_id_best)
        resources.schedule(task_schedule, place_id_best)


class SchedulerClass:
    def __init__(self, g, resources, budget, upward_rank_is_calculated=False, priority_list=None):
        if not upward_rank_is_calculated:
            g.upward_rank(g.startID, resources.average_power, resources.bandwidth)
        if priority_list is None:
            self.priority_list = list_of_task_id_on_upward_rank(g)

        self.g = g
        self.resources = resources
        self.last_unscheduled_task_id = 0

    def schedule_next(self, only_test=False):
        if self.last_unscheduled_task_id not in range(0, len(self.priority_list)):
            return
        t_id = self.priority_list[self.last_unscheduled_task_id]
        task = self.g.tasks[t_id]
        for tId in self.priority_list:
            task = self.g.tasks[tId]
            est_best, runtime_on_resource_best, eft_best, resource_id_best, place_id_best = \
                self.resources.select_resource(task)
            task_schedule = Definitions.Resources.TaskSchedule(task, est_best, runtime_on_resource_best, eft_best,
                                                               resource_id_best)
            self.resources.schedule(task_schedule, place_id_best)

    @property
    def finished(self):
        return self.last_unscheduled_task_id >= len(self.priority_list)
