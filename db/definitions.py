import json


class Result:
    def __init__(self, head_id, test_name, method, constraint, deadline, budget, makespan_old, makespan_new,
                 cost_old, cost_new, gap_rate, c_rate, m_rate, job_name, job_size):
        self.head_id = head_id
        self.test_name = test_name
        self.method = method
        self.constraint = constraint
        self.deadline = deadline
        self.budget = budget
        self.makespan_old = makespan_old
        self.makespan_new = makespan_new
        self.cost_old = cost_old
        self.cost_new = cost_new
        self.gap_rate = gap_rate
        self.c_rate = c_rate
        self.m_rate = m_rate
        self.job_name = job_name
        self.job_size = job_size

    def set_value(self, head_id, test_name, method, constraint, deadline, budget, makespan_old, makespan_new,
                  cost_old, cost_new, gap_rate, c_rate, m_rate, job_name, job_size):
        self.head_id = head_id
        self.test_name = test_name
        self.method = method
        self.constraint = constraint
        self.deadline = deadline
        self.budget = budget
        self.makespan_old = makespan_old
        self.makespan_new = makespan_new
        self.cost_old = cost_old
        self.cost_new = cost_new
        self.gap_rate = gap_rate
        self.c_rate = c_rate
        self.m_rate = m_rate
        self.job_name = job_name
        self.job_size = job_size

    def set_value_by_row(self, row):
        head_id, test_name, method, constraint, deadline, budget, makespan_old, makespan_new,\
        cost_old, cost_new, gap_rate, c_rate, m_rate, job_name, job_size = row

        self.set_value(head_id, test_name, method, constraint, deadline, budget, makespan_old, makespan_new,
                       cost_old, cost_new, gap_rate, c_rate, m_rate, job_name, job_size)

    def get_row(self):
        return self.head_id, self.test_name, self.method, self.constraint, self.deadline, self.budget, \
            self.makespan_old, self.makespan_new, self.cost_old, self.cost_new, self.gap_rate, self.c_rate, \
            self.m_rate, self.job_name, self.job_size


class Test:
    # def __init__(self, test_name, workload_len, resources, budget_ratio, small, medium, large):
    def __init__(self, row):
        test_name, workload_len, resources, budget_ratio, small, medium, large, bandwidth = row
        self.test_name = test_name
        self.workload_len = workload_len
        self.resources = resources
        self.budget_ratio = budget_ratio
        self.small = small
        self.medium = medium
        self.large = large
        self.bandwidth = bandwidth

        resource_def = json.loads(self.resources)
        self.time_slot = resource_def['t']
        self.resource_array = resource_def['r']

    def set_value(self, test_name, workload_len, resources, budget_ratio, small, medium, large, bandwidth):
        self.test_name = test_name
        self.workload_len = workload_len
        self.resources = resources
        self.budget_ratio = budget_ratio
        self.small = small
        self.medium = medium
        self.large = large
        self.bandwidth = bandwidth

        resource_def = json.loads(self.resources)
        self.time_slot = resource_def['t']
        self.resource_array = resource_def['r']

    def set_value_by_row(self, row):
        test_name, workload_len, resources, budget_ratio, small, medium, large, bandwidth = row
        self.set_value(test_name, workload_len, resources, budget_ratio, small, medium, large, bandwidth)
