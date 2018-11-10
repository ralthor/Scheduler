# Multi-workflow Scheduler
This is a multi-workflow scheduler on cloud.

## How to run?
There are three main runners:

 - test creator,
 - test runner,
 - query.

In addition, since I have implemented several other schedulers (for single workflows), those also can be run on separate workflows. I will describe how.

## Workflows
The tests are carried on the workflows, which are stored in S.txt, M.txt, L.txt, XXL.txt which represents the size of the workflows (S for Short, M for Medium, etc.).
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


## workflow schedulers

### multi-workflow schedulers

### BHEFT workflow scheduler

### IC-PCP workflow scheduler
