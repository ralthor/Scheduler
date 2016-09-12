import math
import Scheduler.HEFT
import Definitions.Resources


class CPU:
    def __init__(self, number_of_cpus):
        self.len = number_of_cpus
        self.cpu = []
        self.tasks = {}
        self.eft = [0] * number_of_cpus
        self.est = self.eft[:]
        self.backup_eft = []
        self.backup_est = []
        for i in range(0, self.len):
            self.cpu.append(set())

    def add_task(self, task_id, cpu_id):
        self.cpu[cpu_id].add(task_id)
        self.tasks[task_id] = cpu_id

    def add_tasks(self, task_list, cpu_id):
        for t in task_list:
            self.add_task(t, cpu_id)

    def cpu_task(self, task_id):
        if task_id in self.tasks:
            return self.tasks[task_id]
        else:
            return -1

    def task_cpu(self, cpu_id):
        return self.cpu[cpu_id]

    def remove_task(self, task_id):
        if task_id in self.tasks:
            cpu_id = self.tasks.pop(task_id)
            self.cpu[cpu_id].remove(task_id)

    def remove_tasks(self, task_list):
        for t in task_list:
            self.remove_task(t)

    def backup(self):
        self.backup_eft = self.eft[:]
        self.backup_est = self.est[:]
        self.eft = [0] * self.len
        self.est = self.eft[:]

    def restore(self):
        self.eft = self.backup_eft[:]
        self.est = self.backup_est[:]

    def cost_resource(self, price, timeslot, resource_id):
        return math.ceil((self.eft[resource_id] - self.est[resource_id]) / timeslot) * price

    def cost_all(self, prices, timeslots):
        return sum(map(lambda r: self.cost_resource(prices[r], timeslots[r], r), range(0, self.len)))


def calc_est(g, priority_list, resources, cpu_s=CPU(1), task_id=-1):
    power_max = max(resources.power)

    if task_id == -1:
        start_index = 0
    else:
        start_index = priority_list.index(task_id)

    for i in range(start_index, len(priority_list)):
        t_id = priority_list[i]
        resource_id = cpu_s.cpu_task(t_id)
        if resource_id == -1:
            tasks_on_resource_tid = []
        else:
            tasks_on_resource_tid = cpu_s.task_cpu(resource_id)
        max_est = 0
        parents = g.tasks[t_id].predecessor
        for parent in parents:
            network_latency = g.tasks[t_id].predecessor[parent] / resources.bandwidth
            parent_resource_id = cpu_s.cpu_task(parent)
            if parent_resource_id == -1:
                parent_runtime = g.tasks[parent].weight / power_max
            else:
                if parent in tasks_on_resource_tid:
                    network_latency = 0
                parent_runtime = g.tasks[parent].weight / resources.power[parent_resource_id]
            if hasattr(g.tasks[parent], 'est'):
                est = g.tasks[parent].est + parent_runtime + network_latency
            else:
                continue
            max_est = max(est, max_est) if max_est != -1 else est
        if resource_id != -1:
            if max_est < cpu_s.eft[resource_id]:
                max_est = cpu_s.eft[resource_id]
            runtime = g.tasks[t_id].weight / resources.power[resource_id]
            if max_est + runtime > cpu_s.eft[resource_id]:
                if cpu_s.eft[resource_id] == 0:  # task is the resource's 1st task
                    cpu_s.est[resource_id] = max_est
                cpu_s.eft[resource_id] = max_est + runtime
        g.tasks[t_id].est = max_est


def calc_lft(g, deadline, priority_list, resources, cpu_s=CPU(1), task_id=-1):
    power_max = max(resources.power)

    if task_id == -1:
        start_index = len(priority_list)-1
    else:
        start_index = priority_list.index(task_id)

    for i in range(start_index, -1, -1):
        t_id = priority_list[i]
        resource_id = cpu_s.cpu_task(t_id)
        if resource_id == -1:
            tasks_on_resource_tid = []
        else:
            tasks_on_resource_tid = cpu_s.task_cpu(resource_id)
        min_lft = deadline
        children = g.tasks[t_id].successor
        for child in children:
            network_latency = g.tasks[t_id].successor[child] / resources.bandwidth
            child_resource_id = cpu_s.cpu_task(child)
            if child_resource_id == -1:
                child_runtime = g.tasks[child].weight / power_max
            else:
                if child in tasks_on_resource_tid:
                    network_latency = 0
                child_runtime = g.tasks[child].weight / resources.power[child_resource_id]

            lft = g.tasks[child].lft - child_runtime - network_latency
            min_lft = min(lft, min_lft)
        g.tasks[t_id].lft = min_lft


def put_path_on_resource(pcp, g, resources, resource_id, cpu_s=CPU(1), just_check=True):
    """
    checks running of a path (stored in pcp) on a resource (stored in resource_id)
    :param pcp: list
    :param g: Definitions.Graph
    :param resources:
    :param resource_id:
    :param cpu_s: CPU
    :return:
    """
    cost_before = cpu_s.cost_all(resources.price, resources.timeslot)

    cpu_s.backup()

    cpu_s.add_tasks(pcp, resource_id)
    calc_est(g, resources.priority_list, resources, cpu_s)  # , pcp[0])
    calc_lft(g, g.tasks[g.endID].lft, resources.priority_list, resources, cpu_s, pcp[-1])
    cost_after = cpu_s.cost_all(resources.price, resources.timeslot)
    min_flexibility = min(map(lambda x: g.tasks[x].lft - g.tasks[x].est, g.tasks.keys()))

    if just_check:
        cpu_s.remove_tasks(pcp)
        cpu_s.restore()

    return min_flexibility >= 0, cost_after - cost_before, min_flexibility


def assign_path(pcp, g, resources, cpu_s=CPU(1)):
    min_cost = -1
    selected_resource = -1
    max_flexibility = -1
    for resource_id in range(0, resources.len):
        possible, cost, flexibility = put_path_on_resource(pcp, g, resources, resource_id, cpu_s, just_check=True)
        if possible and (min_cost == -1 or cost < min_cost or (cost == min_cost and flexibility > max_flexibility)):
            selected_resource = resource_id
            min_cost = cost
            max_flexibility = flexibility
    if selected_resource == -1:
        selected_resource = resources.get_cheapest_empty_resource()
        # raise Exception('No Resource is selected!!!')
    put_path_on_resource(pcp, g, resources, selected_resource, cpu_s, just_check=False)
    # print(pcp)
    # print('', end='')
    for t in pcp:
        g.tasks[t].scheduled = True


def assign_parents(t_id, g, resources, cpu_s=CPU(1)):
    def has_unscheduled(task_list):
        for t in task_list:
            if not g.tasks[t].scheduled:
                return True
        else:
            return False

    def critical_parent_id(task_id):
        # it finds the unassigned parent with minimum lft-est
        parents = g.tasks[task_id].predecessor.keys()
        min_ct = -1
        critical_parent_id_inside_function = -1
        for p in parents:
            if g.tasks[p].scheduled:
                continue
            ct = g.tasks[p].lft - g.tasks[p].est
            if min_ct == -1 or ct < min_ct:
                min_ct = ct
                critical_parent_id_inside_function = p
                continue
        return critical_parent_id_inside_function

    while has_unscheduled(g.tasks[t_id].predecessor.keys()):
        pcp = []
        ti = t_id
        while has_unscheduled(g.tasks[ti].predecessor.keys()):
            critical_parent = critical_parent_id(ti)
            pcp.insert(0, critical_parent)
            ti = critical_parent
        assign_path(pcp, g, resources, cpu_s)

        # Here, it must calculate EST and LFT again, though it is done in assign_path function

        for ti in pcp:
            assign_parents(ti, g, resources, cpu_s)


def schedule(g, resources, deadline, upward_rank_is_calculated=False, priority_list=None, no_change_on_resources=False):
    """
    Schedules using a variation of IC-PCP algorithm
    This algorithms sorts tasks on resources based on their upward rank.
    :type resources: Definitions.Resources.CostAwareResources
    :type deadline: float
    """
    if not upward_rank_is_calculated:
        g.upward_rank(g.startID, resources.average_power, resources.bandwidth)
    if priority_list is None:
        priority_list = Scheduler.HEFT.list_of_task_id_on_upward_rank(g)

    for t in g.tasks:
        g.tasks[t].scheduled = False
        g.tasks[t].delay = 0

    g.tasks[g.startID].scheduled = True
    g.tasks[g.endID].scheduled = True

    cpu_s = CPU(resources.len)
    resources.job_task_schedule[g.name] = {}
    calc_est(g, priority_list, resources, cpu_s)
    calc_lft(g, deadline, priority_list, resources, cpu_s)
    resources.priority_list = priority_list
    assign_parents(g.endID, g, resources, cpu_s)

    for t_id in resources.priority_list:
        t = g.tasks[t_id]
        resource_id = cpu_s.cpu_task(t_id)
        runtime_on_resource = t.weight / resources.power[resource_id]
        t.eft = t.est + runtime_on_resource
        task_schedule = Definitions.Resources.TaskSchedule(t, t.est, runtime_on_resource, t.eft, resource_id)
        if no_change_on_resources:
            pass
        else:
            resources.schedule(task_schedule)

    if no_change_on_resources:
        del resources.priority_list

# HOW to USE:
# resources = Definitions.Resources.CostAwareResources([1] * 2 + [2] * 2, [1] * 2 + [3] * 2, [5] * 4, BW)
# Scheduler.ICPCP.schedule(g, resources, deadline=38)
