from __future__ import print_function

import copy
from Definitions.Graph import Graph
from Definitions.Resources import CostAwareResources, Constraint
import Scheduler.BHEFT
import Scheduler.ICPCP
import Scheduler.DeadlineOptimisticAlpha
from Scheduler import Multi_Workflow


class JobItem:
    def __init__(self, g=Graph(), constraint_type=Constraint.none, constraint=0,
                 resources=CostAwareResources([], [], [], 0), reference_graph=None, reference_resources=None):
        if constraint_type is Constraint.none:
            raise Exception('Constraint type must be defined')
        elif constraint <= 0:
            raise Exception('Constraint must be a non-zero positive value')

        self.g = copy.deepcopy(g)
        self.type = constraint_type
        self.constraint = constraint
        self.reference_graph = reference_graph

        if constraint_type is Constraint.budget:
            if self.reference_graph is None:
                self.reference_graph = copy.deepcopy(g)
                resources_copy = copy.deepcopy(resources)
                Scheduler.BHEFT.schedule(self.reference_graph, resources_copy, constraint)
                self.reference_graph.resources = resources_copy
                # verbose:
                # resources_copy.show_schedule(self.reference_graph.name,
                #                              '--' + str(resources_copy.makespan) + ', '
                #                              + str(resources_copy.plan_cost))
                self.reference_graph.cost = resources_copy.plan_cost
            self.reference_graph.budget = constraint
            self.reference_graph.critical_first = self.reference_graph.budget / self.reference_graph.cost
            self.critical_first = self.reference_graph.critical_first
            # self.scheduler = Scheduler.BHEFT.SchedulerClass(self.g, resources, constraint)
            # reference_resources = reference_graph.resources # it is moved to method parameters
            for t in reference_graph.tasks.values():
                t.lft = reference_resources.job_task_schedule[reference_graph.name][t.id].EFT
            Multi_Workflow.assign_sub_deadlines(self.reference_graph, self.g, asap=False)
            Multi_Workflow.assign_sub_budgets(self.reference_graph, self.g, reference_resources)
            self.scheduler = Scheduler.DeadlineOptimisticAlpha.SchedulerClass(
                self.g, resources, reference_graph.makespan)
        elif constraint_type is Constraint.deadline:
            if self.reference_graph is None:
                self.reference_graph = copy.deepcopy(g)
                resources_copy = copy.deepcopy(resources)
                Scheduler.ICPCP.schedule(self.reference_graph, resources_copy, constraint)
                self.reference_graph.resources = resources_copy
                # resources_copy.show_schedule(self.reference_graph.name,
                #                              '--' + str(resources_copy.makespan) + ', '
                #                              + str(resources_copy.plan_cost))
                self.reference_graph.makespan = resources_copy.makespan
            self.reference_graph.deadline = constraint
            self.reference_graph.critical_first = self.reference_graph.deadline / self.reference_graph.makespan
            self.critical_first = self.reference_graph.critical_first
            Multi_Workflow.assign_sub_deadlines(self.reference_graph, self.g, asap=True)
            Multi_Workflow.assign_sub_budgets(self.reference_graph, self.g, reference_resources)
            self.scheduler = Scheduler.DeadlineOptimisticAlpha.SchedulerClass(self.g, resources, constraint)
        else:
            raise Exception("Define a constraint!")
        Multi_Workflow.assign_cost_for_each_task(self.reference_graph, reference_resources)
        # print('', end='')
        self.sum_w = sum(map(lambda task: task.weight, self.reference_graph.tasks.values()))
        self.overall_budget = reference_resources.plan_cost
        self.scheduler.set_budget(reference_resources.plan_cost)

    @property
    def critical_now(self):
        if self.scheduler.finished:
            return 1e10

        if self.g.name in self.scheduler.resources.head_nodes:
            head_nodes = self.scheduler.resources.head_nodes[self.g.name]
        else:
            head_nodes = set()

        def calc_cr(task_id):
            current_finish = self.scheduler.resources.job_task_schedule[self.g.name][task_id].EFT
            if hasattr(self.reference_graph.tasks[task_id], 'eft'):
                reference_finish = self.reference_graph.tasks[task_id].eft
            # else:
            #     print('.', end='')
            if current_finish == 0:
                return 0
            return float(reference_finish) / current_finish

        if len(head_nodes) == 0:
            average = 0
        else:
            average = sum(map(calc_cr, head_nodes)) / len(head_nodes)
        # if self.g.name in self.scheduler.resources.sum_weight_scheduled:
        #     sum_weight_scheduled = self.scheduler.resources.sum_weight_scheduled[self.g.name]
        # else:
        #     sum_weight_scheduled = 0
        # sum_weight_unscheduled = self.sum_w - sum_weight_scheduled

        time_critical = average

        if time_critical < 1 and self.type is Constraint.deadline:
            return -2 * time_critical

        # Cost computations:
        scheduled_tasks = self.scheduler.scheduled_task_ids()
        reference_cost = sum(map(lambda t: self.reference_graph.tasks[t].sub_budget, scheduled_tasks))

        if self.g.name in self.scheduler.resources.costs:
            current_cost = self.scheduler.resources.costs[self.g.name]
        else:
            current_cost = 0

        if current_cost == 0:
            cost_critical = self.critical_first
        else:
            cost_critical = float(reference_cost) / current_cost

        return time_critical + cost_critical

    @property
    def critical(self):
        if self.scheduler.finished:
            return 1e10

        r, c, changes = self.scheduler.schedule_next(only_test=True, calc_resource_cost_change=True)
        rank_number = self.scheduler.last_unscheduled_task_id
        t_id = self.scheduler.priority_list[rank_number]
        if not hasattr(self.reference_graph.tasks[t_id], 'cost'):
            return self.critical_first

        predecessors = self.g.tasks[t_id].predecessor.keys()
        if len(predecessors) == 0:
            head_nodes = set()
        else:
            head_nodes = self.scheduler.resources.head_nodes[self.g.name].difference(predecessors)
        head_nodes.add(t_id)

        def calc_cr(task_id):
            if task_id == t_id:
                current_finish = r
            else:
                current_finish = self.scheduler.resources.job_task_schedule[self.g.name][task_id].EFT
            # TODO: reference finish must become the deadline (in case of deadline job)?!
            if not hasattr(self.reference_graph.tasks[task_id], 'eft'):
                print()
            reference_finish = self.reference_graph.tasks[task_id].eft
            if current_finish == 0:
                return 0
            return float(reference_finish) / current_finish

        average = sum(map(calc_cr, head_nodes)) / len(head_nodes)
        if self.g.name in self.scheduler.resources.sum_weight_scheduled:
            sum_weight_scheduled = self.scheduler.resources.sum_weight_scheduled[self.g.name]
        else:
            sum_weight_scheduled = 0
        sum_weight_unscheduled = self.sum_w - sum_weight_scheduled
        time_critical = (sum_weight_scheduled * average + sum_weight_unscheduled * self.critical_first) / self.sum_w

        if self.g.name in self.scheduler.resources.costs:
            current_cost = self.scheduler.resources.costs[self.g.name]
        else:
            current_cost = 0
        reference_cost = float(self.overall_budget) * sum_weight_scheduled / self.sum_w

        if current_cost == 0:
            cost_critical = self.critical_first
        else:
            cost_critical = float(reference_cost) / current_cost

        return cost_critical
        # return time_critical + cost_critical

        # R, C = self.reference_graph.tasks[t_id].eft, self.reference_graph.tasks[t_id].cost
        # # if self.type == Constraint.deadline:
        # #     if c == 0:
        # #         return self.critical_first
        # #     else:
        # #         return c / C
        # # else:
        # if r == 0:
        #     return self.critical_first
        # else:
        #     return R / r
