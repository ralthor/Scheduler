class Task:
    weight = 0
    upward_rank = -1
    id = -1
    dummy_task = False

    def __init__(self, weight=-1, ids=None, es=None, task_id=-1, graph_object=None):
        self.asap = None
        self.successor = {}
        self.predecessor = {}
        self.weight = weight
        if es is not None:
            self.add_successors(ids, es)
        self.graph = graph_object
        self.id = task_id

    def add_successor(self, id, e):
        """
        add a successor edge like this:  self.id -(e)-> id
        :type e: float
        :type id: int
        """
        self.successor[id] = e

    def add_successors(self, ids, es):
        for i in range(0, len(ids)):
            self.add_successor(ids[i], es[i])

    sub_budget = -1
    sub_deadline = -1
    max_cost = -1
    max_time = -1

    @property
    def is_budget_task(self):
        return self.sub_budget != -1

    @property
    def is_deadline_task(self):
        return self.sub_deadline != -1


class Graph:
    startID = -1
    endID = -1
    type = 'unknown'

    def __init__(self):
        self.tasks = {}
        self.name = ''

    def set_name(self, name):
        self.name = name

    def set_type(self, g_type):
        self.type = g_type

    def add_task(self, id, weight, ids, es):
        temp = Task(weight, ids, es, id, self)
        self.tasks[id] = temp

    def set_start_end(self, start_id, end_id):
        self.startID, self.endID = start_id, end_id

    def upward_rank(self, node_id, average_resource_power=1, average_bandwidth=1):
        if self.tasks[node_id].upward_rank != -1:
            pass
        elif len(self.tasks[node_id].successor) == 0:
            self.tasks[node_id].upward_rank = self.tasks[node_id].weight/average_resource_power
        else:
            max_children_rank = -1
            for childID in self.tasks[node_id].successor:
                ur_child = self.upward_rank(childID,
                                            average_resource_power,
                                            average_bandwidth) + \
                           self.tasks[node_id].successor[childID]/average_bandwidth
                if max_children_rank < ur_child:
                    max_children_rank = ur_child

            self.tasks[node_id].upward_rank = self.tasks[node_id].weight/average_resource_power + max_children_rank
        return self.tasks[node_id].upward_rank

    def set_predecessors(self):
        for tid in self.tasks:
            t = self.tasks[tid]
            for s in t.successor:
                self.tasks[s].predecessor[tid] = t.successor[s]
