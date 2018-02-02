import Definitions
import Definitions.WorkflowReader.reader
#import db.definitions
import Definitions.Resources
import Scheduler.ICPCP

all_jobs = {'S': Definitions.WorkflowReader.reader.read_jobs('S.txt'),  # 25-30
                'M': Definitions.WorkflowReader.reader.read_jobs('M.txt'),  # 50-60
                'L': Definitions.WorkflowReader.reader.read_jobs('L.txt'),  # 100
                'XXL': Definitions.WorkflowReader.reader.read_jobs('XXL.txt')}
n = 8
processorList = [2] * n + [2] * n + [4] * (n * 2 / 3) + [8] * (n / 2) + [16] * (n / 3)
resources = Definitions.Resources.CostAwareResources(processorList, processorList, [60] * len(processorList), 20000)

g = all_jobs['S']['CyberShake']

Scheduler.ICPCP.schedule(g, resources, 150)
print resources.makespan
print resources.plan_cost


def run(s, name, dl):
    g1 = all_jobs[s][name]
    Scheduler.ICPCP.schedule(g1, resources, dl)
    print resources.makespan
    print resources.plan_cost
    print list(map(resources.resource_cost, range(resources.len)))
