# Multi-workflow Scheduler
This is an implementation for a multi-workflow scheduler on cloud. 

```
                                               +------------+
                                               | Schedulers |
            workflow                           |  - HEFT    |
             files                             |  - BHEFT   |
               |                               |  - ICPCP   |
               |                               +------------+
               v                                     |
+------------------------------+                     |
|WorkflowReader.reader.read_job|                     |                       +----------------------+
+------------------------------+                     v                       |  Resources           |
               |                         +--------------------------+        | +------------------+ |
               |                         | Multi-workflow Scheduler |        | |CostAwareResources| |
               v               +-------->|  Policies:               |<------>| +------------------+ |
            Jobs               |         |  - RR                    |        +----------------------+
           +--------+          |         |  - PRR                   |                 |
           | Graphs |----------+         |  - FCFS                  |                 v
           +--------+                    |  - Fair                  |           Schedule Plans
                                         +--------------------------+
```

## How to run?
There are three main runners:

 - test creator,
 - test runner,
 - query.

In addition, since I have implemented several other schedulers (for single workflows), those also can be run on separate workflows. I will describe how.

## Workflows
The tests are carried on the workflows, which are stored in S.txt, M.txt, L.txt, XXL.txt which represents the size of the workflows (S for Short, M for Medium, etc.).

Workflows are read from those files by `Definitions.WorkflowReader.reader.read_jobs` function. The read instance is a list of `Definitions.Graph` instances. A `Graph` class provides required information and operation on a set of `Task` instances (implemented in the same file).

I have generated several scientific workflows (SIPHT, CyberShake, LIGO, Montage, and Epigenomics) using Pegasus, and stored them in a straightforward (but not standard) text format. I have also developed a minimal javascript tool to visualize those workflows. I'll add that to the project soon.

## test creator
`planner.py` is getting the `testname`, `db-filename` `outputfile` and an optional `write_plan`, and prepares a set of tests.

## test runner
This task is done by `main1.py` file. It gets the `testname`, `db-filename`, `filenamepart`, `start_number`, `number_of_test_sets`, and `policy`.

The `testname` argument is the master key for the table of tests. It helps to distinguish between different test cases, for example, it helps to query on different tests which have something in common in their name.

 As it is shown in `main1.py`, `db-filename` is used to retrive the aspects of the test and to store the results. I used SQLite for db, because I wanted to share the database file via NTFS, and different instances of runners can connect it using its mounted location.

`outputfile` is the file to store some results that are not going to be saved in the database. These data may be used to debug or to describe wiered results.

After the runner is finished, results can be queried in the next running process.

## query
`report_plots.py` is responsible for querying and plotting the results. There are several functions in this file which use sql queries to to retrive the stored results in the database and use `matplotlib.pyplot` to plot the diagram.

## Resources
`Resources` class is implemented in `Definitions` folder. It provides the behavior we expect from the resources on a computing cluster. Since the proposed method is designed for computing clouds, where cost of the resources is important, `Resources` class is inherited by `CostAwareResources` class, considering the costs of the resources. Resource classes are responsible for assigning, querying, scheduling (on specific resource), showing, and saving the resources.

## workflow schedulers
There are several schedulers implemented in this project.

### multi-workflow schedulers
The main task is to implement a multi-workflow scheduler. The whole multi-workflow scheduling is separated among different files of the project. There are four policies implemented to do so:
 - FCFS
 - Round-Robin
 - Priority Round-Robin
 - Fair
 
These policies are implemented in a function in `main1.py`, and the are called like this:
```py
	if policy == 'prr':
			prr_policy()
	elif policy == 'rr':
			rr_policy()
	elif policy == 'fcfs':
			fcfs_policy()
	elif policy == 'fair':
			fair_policy()
```
Above functions are called based on the selected policy in the arguments of the `main1.py`. In each policy, a set of workflows stored in `jobs` array are scheduled. Each entry of `jobs` array is an instance of `JobItem` class in `Definitions\MultiWorkflow` folder.

The difference between policies is the way they select the next workflow to schedule from. Each job has a `scheduler` instance (the class is in `Scheduler` folder), which schedules its next task on an instance of `Resources` class (the class is in `Definitions` folder). Since we are doing multi-workflow scheduling, the resources are the same for all of the workflows.

### HEFT workflow scheduler
It is implemented in `Scheduler/HEFT.py` file. This file contains a `SchedulerClass` which accepts a `Resources` instance and schedules the given workflow (stored in a `Graph` instance `g`) on them.

It's an implementation for the Heterogeneous Earliest-Finish-Time (HEFT) algorithm presented [here](https://ieeexplore.ieee.org/document/993206).

### BHEFT workflow scheduler
It is implemented in `Scheduler/BHEFT.py` file. This file contains a `SchedulerClass` which accepts a `CostAwareResources` instance and schedules the given workflow on them. There are several call samples in the python file.

It's a implementation for the Budget-constrained Heterogeneous Earliest Finish Time (BHEFT) algorithm presented [here](https://link.springer.com/chapter/10.1007/978-3-642-28675-9_8).

### IC-PCP workflow scheduler
It is implemented in `Scheduler/ICPCP.py` file. This file contains a `SchedulerClass` which accepts a `CostAwareResources` instance and schedules the given workflow on them. There is a small guide at the end of that python file.

It is an implementation for IaaS Cloud Partial Critical Paths (IC-PCP) algorithm presented [here](https://www.sciencedirect.com/science/article/pii/S0167739X12001008).