import Definitions.Resources
import Scheduler.HEFT


def schedule(g, resources, deadline, alpha=0.5, upward_rank_is_calculated=False, priority_list=None):
    """
    Schedules using DeadlineOptimisticAlpha algorithm, it tries not to go further than the deadline, except for the
    tasks with early deadlines.
    :type g: Graph
    :type resources: Definitions.Resources.CostAwareResources
    :type deadline: float
    :type alpha: float
    """
    if not upward_rank_is_calculated:
        g.upward_rank(g.startID, resources.average_power, resources.bandwidth)
    if priority_list is None:
        priority_list = Scheduler.HEFT.list_of_task_id_on_upward_rank(g)

    limit = deadline / 2

    for t_id in priority_list:
        task = g.tasks[t_id]
        beta = - alpha / limit * task.sub_deadline + alpha
        task.sub_deadline *= (1 + beta)
        # resource selection:
        est, runtime_on_resource, eft, resource_id, place_id, cost = resources.select_resource(task)

        # scheduling:
        task_schedule = Definitions.Resources.TaskSchedule(task, est, runtime_on_resource, eft, resource_id)
        resources.schedule(task_schedule, place_id)


class SchedulerClass:
    def __init__(self, g, resources, deadline, alpha=0.0, upward_rank_is_calculated=False, priority_list=None):
        if not upward_rank_is_calculated:
            g.upward_rank(g.startID, resources.average_power, resources.bandwidth)
        if priority_list is None:
            self.priority_list = Scheduler.HEFT.list_of_task_id_on_upward_rank(g)
        self.g = g
        self.resources = resources
        self.last_unscheduled_task_id = 0

        self.limit = deadline / 2
        self.alpha = alpha
        self.remaining_budget = 0
        self.total_budget = 0
        self.sum_share = 0

    def set_budget(self, budget):
        """
        must be called after setting task.sub_budgets.
        :param budget:
        :return:
        """
        self.remaining_budget = budget
        self.total_budget = budget
        self.sum_share = sum(map(lambda t: t.sub_budget, self.g.tasks.values()))

    def scheduled_task_ids(self):
        if self.finished:
            return self.priority_list[:]
        task_id_in_p_list = self.last_unscheduled_task_id
        if task_id_in_p_list == 0:
            return []
        else:
            task_id_in_p_list -= 1
        scheduled = self.priority_list[:task_id_in_p_list]
        return scheduled


    def recalculate_sub_budget(self):
        if self.finished:
            return
        task_id_in_p_list = self.last_unscheduled_task_id
        t_id = self.priority_list[task_id_in_p_list]
        task = self.g.tasks[t_id]
        unscheduled = self.priority_list[task_id_in_p_list:]
        sum_unscheduled = sum(map(lambda u: self.g.tasks[u].sub_budget, unscheduled))
        if sum_unscheduled == 0:
            return
        task.sub_budget = self.remaining_budget * task.sub_budget / sum_unscheduled


    def next_ready_task(self, arrival_time, verbose=0):
        """
        Finds the ready task with the highest upward rank at the given time.
        There may be several unscheduled tasks, but it is important that
        their predecessors are finished before the given time.

        Returns -1 if no task is found
        """
        if self.last_unscheduled_task_id == 0:
            return self.last_unscheduled_task_id

        candidates = self.priority_list[self.last_unscheduled_task_id:]
        scheduled_tasks = self.priority_list[:self.last_unscheduled_task_id]
        gname = self.g.name
        if verbose:
            print(f' candidates: {candidates}\n scheduleds: {scheduled_tasks}\n gname: {gname}')
        
        ready_tasks = []
        for t_id in candidates:
            task = self.g.tasks[t_id]
            unscheduled_predecessors = [p for p in task.predecessor if not p in scheduled_tasks]
            
            if verbose:
                print(f'task id: {t_id}\n   unscheduled_predecessors: {unscheduled_predecessors}')
        
            if unscheduled_predecessors:
                continue
            predecessors_finished_after_current_time = [p for p in task.predecessor
                                                        if self.resources.job_task_schedule[gname][p].EFT > arrival_time]
            
            if verbose:
                print(f'task id: {t_id}\n   predecessors_finished_after_current_time: {predecessors_finished_after_current_time}')
        
            if predecessors_finished_after_current_time:
                continue
            ready_tasks.append(t_id)
        if not ready_tasks:
            return -1
        ready_tasks_with_index = [(self.priority_list.index(x), x) for x in ready_tasks]
        return sorted(ready_tasks_with_index)[0][1]


    def next_event(self, arrival_time, verbose=0):
        scheduled_tasks = self.priority_list[:self.last_unscheduled_task_id]
        gname = self.g.name

        if verbose:
            print(f'scheduled_tasks: {scheduled_tasks}')
            print(f'gname: {gname}')

        min_eft = None
        for t_id in scheduled_tasks:
            task_eft = self.resources.job_task_schedule[gname][t_id].EFT
            if verbose:
                print(f'task id:{t_id}, task EFT: {task_eft} ')
            if  task_eft > arrival_time and (min_eft is None or task_eft < min_eft):
                min_eft = task_eft
        return min_eft


    def schedule_next(self, only_test=False, do_head_nodes=False, calc_resource_cost_change=False, arrival_time=0):
        if self.finished:
            return
        t_id = self.priority_list[self.last_unscheduled_task_id]
        task = self.g.tasks[t_id]

        beta = - self.alpha / self.limit * task.sub_deadline + self.alpha
        task.sub_deadline *= (1 + beta)
        # resource selection:
        est, runtime_on_resource, eft, resource_id, place_id, cost = self.resources.select_resource(
            task, test=only_test, arrival_time=arrival_time)

        if not only_test:
            # scheduling:
            task_schedule = Definitions.Resources.TaskSchedule(task, est, runtime_on_resource, eft, resource_id)
            self.resources.schedule(task_schedule, place_id, do_head_nodes)
            self.last_unscheduled_task_id += 1
            return eft, cost, resource_id
        else:
            if calc_resource_cost_change:
                change = self.resources.calculate_share_cost_change(resource_id, est, eft, task.graph.name)
                return eft, cost, change
            return eft, cost, resource_id

    @property
    def finished(self):
        return self.last_unscheduled_task_id >= len(self.priority_list)
