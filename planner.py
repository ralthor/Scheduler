#
#
# Important notice:
#
# call this file via 'dbtest.py' output
#

import sys
import socket
import pickle
from Definitions.MultiWorkflow.JobList import Constraint
import Scheduler.ICPCP
import Scheduler.HEFT
import Scheduler.BHEFT
import Definitions.Resources
import Scheduler.BudgetPessimistic
import Scheduler.DeadlineOptimisticAlpha
import copy
import Scheduler.Multi_Workflow
import db.definitions
import db.reader
import db.writer


def main(args):
    if len(args) < 1:
        print('Required command line arguments are not specified\n'
              ' usage: python planner.py testname dbfilename outputfile <write_plan (optional)>')
        exit()
    # testname represents a test in the database (it is the key in the tests table)
    test_name = args[0]
    database_file = args[1]
    dumb_file = args[2]
    write_plan = False
    if len(args) > 3:
        if args[3] == 'write_plan':
            write_plan = True
    reader = db.reader.Reader(database_file)
    rows = reader.read_test(test_name) # reading the test from the table
    row = rows.fetchone()
    test = db.definitions.Test(row)
    timeslot = test.time_slot
    bandwidth = test.bandwidth
    workload_len = test.workload_len
    powers = []
    prices = []
    numbers = []
    for r in test.resource_array:
        powers.append(r[0])
        prices.append(r[1])
        numbers.append(r[2])

    host_name = socket.gethostname()

    print("Hostname: {}".format(host_name))

    power_list, price_list, timeslot_list = [], [], []
    for i in range(len(test.resource_array)):
        power_list += [powers[i]] * numbers[i]
        price_list += [prices[i]] * numbers[i]
        # TODO: Tests must be changed, but it works for now (in case of change: both planner and main):
        timeslot_list += [60 * timeslot] * numbers[i]

    resource_spec = (power_list, price_list, timeslot_list)

    main_resources = Definitions.Resources.CostAwareResources(resource_spec[0], resource_spec[1], resource_spec[2],
                                                              bandwidth)

    # ----------Workload Generation:
    job, constraint, names, sizes = Scheduler.Multi_Workflow.make_workload(test)

    # ---------Schedule Jobs one by one on the reference system
    # ---------(it includes finding a good constraint for them and some measurements)
    makespan_list = []
    cost_list = []
    resources_set = []
    graph_set = []
    constraint_values = []

    for i in range(workload_len):
        resources = copy.deepcopy(main_resources)
        g = copy.deepcopy(job[i])
        Scheduler.HEFT.schedule(g, resources)
        g_heft = g
        cost = resources.plan_cost
        makespan = resources.makespan

        heft_resources = resources

        if constraint[i] is Constraint.budget:
            c = 'Budget'
            budget_factor = 0.2
            while True:
                resources = copy.deepcopy(main_resources)
                g = copy.deepcopy(job[i])
                Scheduler.BHEFT.schedule(g, resources, cost * budget_factor)
                if cost * budget_factor >= resources.plan_cost:
                    break
                budget_factor += 0.2
            # if budget_factor >= 1:
            #     resources = heft_resources
            #     g = g_heft
            #     constraint_factor = 1
            # else:

            constraint_factor = budget_factor
            constraint_value = cost * budget_factor
        else:
            c = 'Deadline'
            deadline_factor = 1.2

            if sizes[i] == 'XXL':
                if names[i] == 'Montage':
                    deadline_factor = 27
                elif names[i] == 'Sipht':
                    deadline_factor = 13
                elif names[i] == 'Inspiral':
                    deadline_factor = 4

            resources = heft_resources
            first_computation = True
            while True:
                resources = copy.deepcopy(main_resources)
                g = copy.deepcopy(job[i])
                Scheduler.ICPCP.schedule(g, resources, makespan * deadline_factor)
                if makespan * deadline_factor >= resources.makespan:
                    # if resources.plan_cost >= cost:
                    #     resources = heft_resources
                    #     deadline_factor = 1
                    if not first_computation:
                        break
                    else:
                        deadline_factor *= 2
                        first_computation = False
                        continue
                deadline_factor *= 1.2
            constraint_factor = deadline_factor
            constraint_value = makespan * deadline_factor
        print("heft cost:{0:5.1f} | cost:{1:5.1f} | heft ms:{2:5.2f} | ms:{3:5.2f} "
              "| Nodes:{4:4d} | {5:>8} | factor: {6:5.2f}".format(cost, resources.plan_cost, makespan,
                                                                  resources.makespan, len(g.tasks) - 2, c,
                                                                  constraint_factor))

        # ---Store results for next use:
        makespan_list.append(resources.makespan)
        cost_list.append(resources.plan_cost)
        resources_set.append(copy.deepcopy(resources))
        graph_set.append(g)
        constraint_values.append(constraint_value)

        if write_plan:
            resources.write_schedule(database_file, test_name)

    # ---------- End of workload generation

    # ---------Starting Method:

    # find out each task's deadline and start-time (it's stored for deadline wfs in est & lft of task, do it for
    # budget ones).
    # Done: it's stored for deadline wfs in est & lft of task, do it for budget ones
    for i in range(workload_len):
        if constraint[i] is Constraint.deadline:
            continue
        resources = resources_set[i]
        for tor in resources.tasksOfResource:
            for sch in tor:
                sch.task.est = sch.EST
                sch.task.eft = sch.EFT

    # ---------- End of sub-budget and sub-deadline assignments

# ===================================================================================  vvv Current changes vvv
# ----------------------- storing Everything needed with pickle:
    to_store = [test, numbers, resources_set, graph_set, makespan_list, cost_list,
                constraint_values, constraint, job, names]
    pickle.dump(to_store, open(dumb_file, 'wb'))

if __name__ == "__main__":
    try:
        main(sys.argv[1:])
    except:
        print(" === ERROR :")
        e = sys.exc_info()
        for m in e:
            print(m)
