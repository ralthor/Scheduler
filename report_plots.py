# import numpy as np
# import matplotlib.pyplot as pyplot
import random
import sys

import reports.plot as p
import db.reader
import unicodedata
import matplotlib.pyplot as plt

additional_condition = ''

def query_workload_p(r, constraint=None, budget_ratio=None):

    additional_title = ''
    if constraint is not None:
        constraint_limit = '''and r."constraint" = '{}' '''.format(constraint)
        additional_title += 'for {} constraints'.format(constraint)
    else:
        constraint_limit = ''

    if budget_ratio is not None:
        budget_limit = ''' and t.budget_ratio = {} '''.format(budget_ratio)
        if additional_title != '':
            additional_title += ' and '
        additional_title += 'for budget-ratio = {}'.format(budget_ratio)
    else:
        budget_limit = ''

    query = '''
		select
		        --sum(cost_new)/sum(cost_old)
		        count(c_rate)/sum(c_rate)
		            as cost, sum(h.U)/count(h.U) as uf,
				h.workload_len as `size`, method,
				--sum(makespan_new)/sum(makespan_old)
				count(m_rate)/sum(m_rate)
				    as makespan,
				sum(h.gap_rate)/count(h.gap_rate)
				    as g
			from results r inner join result_head h
				on r.head_id = h.id
				inner join test_spec t on t."key" = h.testname --where --
				  where h.testname like 't1b%'
				  ''' + constraint_limit + budget_limit + additional_condition + '''
			group by
				size,
				method
			order by
				method, size'''
    rows = r.select_query(query, [])

    n = 16

    u_data = {}
    u_data['rr'] = [0] * n
    u_data['prr'] = [0] * n
    u_data['fcfs'] = [0] * n
    u_data['fair'] = [0] * n
    c_data = {}
    c_data['rr'] = [0] * n
    c_data['prr'] = [0] * n
    c_data['fcfs'] = [0] * n
    c_data['fair'] = [0] * n
    m_data = {}
    m_data['rr'] = [0] * n
    m_data['prr'] = [0] * n
    m_data['fcfs'] = [0] * n
    m_data['fair'] = [0] * n
    g_data = {}
    g_data['rr'] = [0] * n
    g_data['prr'] = [0] * n
    g_data['fcfs'] = [0] * n
    g_data['fair'] = [0] * n
    my_map = {'10': 0, '20': 1, '30': 2, '40': 3, '50': 4, '60': 5, '70': 6, '80': 7, '90': 8,
              '100': 9, '110': 10, '120': 11, '130': 12, '140': 13, '150': 14, '160': 15}
    for row in rows:
        cost = row[0]
        u = row[1]
        size = str(row[2])
        method = row[3]
        msp = row[4]
        g_rate = row[5]
        # print(size)
        method = unicodedata.normalize('NFKD', method).encode('ascii', 'ignore')

        # print("{}, {}".format(method, size))
        if method not in u_data:
            print("not Exist!")
            continue
        u_data[method][my_map[size]] = u
        c_data[method][my_map[size]] = cost
        m_data[method][my_map[size]] = msp
        g_data[method][my_map[size]] = g_rate

    x = list(sorted(map(lambda k: int(k), my_map.keys())))

    title = 'Unfairness {}'.format(additional_title)
    p.plot([x, x, x, x], [u_data['rr'], u_data['prr'], u_data['fcfs'], u_data['fair']],
           'Workload', 'Unfairness', ('RR', 'PRR', 'Ordered', 'Direct'),
           'best', title + ' workload', show=False)

    title = 'Normalized Cost {}'.format(additional_title)
    p.plot([x, x, x, x], [c_data['rr'], c_data['prr'], c_data['fcfs'], c_data['fair']],
           'Workload', 'Normalized Cost', ('RR', 'PRR', 'Ordered', 'Direct'),
           'best', title + ' workload', show=False)

    title = 'Normalized Makespan {}'.format(additional_title)
    p.plot([x, x, x, x], [m_data['rr'], m_data['prr'], m_data['fcfs'], m_data['fair']],
           'Workload', 'Normalized Makespan',
           ('RR', 'PRR', 'Ordered', 'Direct'),
           'best', title + ' workload', show=False)

    title = 'Gap-ratio {}'.format(additional_title)
    p.plot([x, x, x, x], [g_data['rr'], g_data['prr'], g_data['fcfs'], g_data['fair']],
           'Workload', 'Gap-ratio',
           ('RR', 'PRR', 'Ordered', 'Direct'),
           'best', title + ' workload', show=False)

def query_coef_p(r, constraint=None, budget_ratio=None):

    additional_title = ''
    if constraint is not None:
        constraint_limit = '''and r."constraint" = '{}' '''.format(constraint)
        additional_title += 'for {} constraints'.format(constraint)
    else:
        constraint_limit = ''

    if budget_ratio is not None:
        budget_limit = ''' and t.budget_ratio = {} '''.format(budget_ratio)
        if additional_title != '':
            additional_title += ' and '
        additional_title += 'for budget-ratio = {}'.format(budget_ratio)
    else:
        budget_limit = ''

    query = '''
		select
		        --sum(cost_new)/sum(cost_old)
		        count(c_rate)/sum(c_rate)
		            as cost,
		        sum(h.U)/count(h.U) as uf,
				h.c, method,
				--sum(makespan_new)/sum(makespan_old)
				count(m_rate)/sum(m_rate)
				    as makespan,
				sum(h.gap_rate)/count(h.gap_rate)
				    as g
			from results r inner join result_head h
				on r.head_id = h.id
				inner join test_spec t on t."key" = h.testname --where --
				  where h.gap_rate <> -1
				  ''' + constraint_limit + budget_limit + additional_condition + '''
			group by
				h.c,
				method
			order by
				method, h.c'''
    rows = r.select_query(query, [])

    n = 9

    u_data = {}
    u_data['rr'] = [0] * n
    u_data['prr'] = [0] * n
    u_data['fcfs'] = [0] * n
    u_data['fair'] = [0] * n
    c_data = {}
    c_data['rr'] = [0] * n
    c_data['prr'] = [0] * n
    c_data['fcfs'] = [0] * n
    c_data['fair'] = [0] * n
    m_data = {}
    m_data['rr'] = [0] * n
    m_data['prr'] = [0] * n
    m_data['fcfs'] = [0] * n
    m_data['fair'] = [0] * n
    g_data = {}
    g_data['rr'] = [0] * n
    g_data['prr'] = [0] * n
    g_data['fcfs'] = [0] * n
    g_data['fair'] = [0] * n
    my_map = lambda c: int((float(c) - 0.4) * 10 + 0.5)
    for row in rows:
        cost = row[0]
        u = row[1]
        coef = str(row[2])
        method = row[3]
        msp = row[4]
        g_rate = row[5]
        # print(size)
        method = unicodedata.normalize('NFKD', method).encode('ascii', 'ignore')

        # print("{}, {}".format(method, size))
        if method not in u_data:
            print("not Exist!")
            continue
        u_data[method][my_map(coef)] = u
        c_data[method][my_map(coef)] = cost
        m_data[method][my_map(coef)] = msp
        m_data[method][my_map(coef)] = msp
        g_data[method][my_map(coef)] = g_rate

    x = [0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1, 1.1, 1.2]

    title = 'Unfairness {}'.format(additional_title)
    p.plot([x, x, x, x], [u_data['rr'], u_data['prr'], u_data['fcfs'], u_data['fair']],
           'Resource factor', 'Unfairness', pdf_name=title, show=False,
           do_low_high=False, new_low=0, new_high=0.15)

    title = 'Normalized Cost {}'.format(additional_title)
    p.plot([x, x, x, x], [c_data['rr'], c_data['prr'], c_data['fcfs'], c_data['fair']],
           'Resource factor', 'Normalized Cost', ('RR', 'PRR', 'Ordered', 'Direct'),
           'best', title, show=False, do_low_high=False, new_low=0.9, new_high=1.15)

    title = 'Normalized Makespan {}'.format(additional_title)
    p.plot([x, x, x, x], [m_data['rr'], m_data['prr'], m_data['fcfs'], m_data['fair']],
           'Resource factor', 'Normalized Makespan',
           ('RR', 'PRR', 'Ordered', 'Direct'),
           'best', title, show=False, do_low_high=False, new_low=0.8, new_high=1.05)
    title = 'Gap-ratio {}'.format(additional_title)
    p.plot([x, x, x, x], [g_data['rr'], g_data['prr'], g_data['fcfs'], g_data['fair']],
           'Resource factor', 'Gap-ratio',
           ('RR', 'PRR', 'Ordered', 'Direct'),
           'best', title, show=False, do_low_high=False, new_low=0.0, new_high=0.02)

def query_coef_p_more_param(r, constraint=None, budget_ratio=None, show=False,
                            pdf_name=None, figure_num=None):

    additional_title = ''
    if constraint is not None:
        constraint_limit = '''and r."constraint" = '{}' '''.format(constraint)
        additional_title += 'for {} constraints'.format(constraint)
    else:
        constraint_limit = ''

    if budget_ratio is not None:
        budget_limit = ''' and t.budget_ratio = {} '''.format(budget_ratio)
        if additional_title != '':
            additional_title += ' and '
        additional_title += 'for budget-ratio = {}'.format(budget_ratio)
    else:
        budget_limit = ''

    query = '''
		select
		        --sum(cost_new)/sum(cost_old)
		        count(c_rate)/sum(c_rate)
		            as cost,
		        sum(h.U)/count(h.U) as uf,
				h.c, method,
				--sum(makespan_new)/sum(makespan_old)
				count(m_rate)/sum(m_rate)
				    as makespan
			from results r inner join result_head h
				on r.head_id = h.id
				inner join test_spec t on t."key" = h.testname --where --
				  where h.testname like 't1b%'
				  ''' + constraint_limit + budget_limit + additional_condition + '''
			group by
				h.c,
				method
			order by
				method, h.c'''
    rows = r.select_query(query, [])

    n = 9

    u_data = {}
    u_data['rr'] = [0] * n
    u_data['prr'] = [0] * n
    u_data['fcfs'] = [0] * n
    u_data['fair'] = [0] * n
    c_data = {}
    c_data['rr'] = [0] * n
    c_data['prr'] = [0] * n
    c_data['fcfs'] = [0] * n
    c_data['fair'] = [0] * n
    m_data = {}
    m_data['rr'] = [0] * n
    m_data['prr'] = [0] * n
    m_data['fcfs'] = [0] * n
    m_data['fair'] = [0] * n
    my_map = lambda c: int((float(c) - 0.4) * 10 + 0.5)
    for row in rows:
        cost = row[0]
        u = row[1]
        coef = str(row[2])
        method = row[3]
        msp = row[4]
        # print(size)
        method = unicodedata.normalize('NFKD', method).encode('ascii', 'ignore')

        # print("{}, {}".format(method, size))
        if method not in u_data:
            print("not Exist!")
            continue
        u_data[method][my_map(coef)] = u
        c_data[method][my_map(coef)] = cost
        m_data[method][my_map(coef)] = msp

    x = [0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1, 1.1, 1.2]

    file_name = None

    # title = 'Unfairness {}'.format(additional_title)
    # if pdf_name is not None:
    #     file_name = title + '_resource_coef'
    # p.plot([x, x, x, x], [u_data['rr'], u_data['prr'], u_data['fcfs'], u_data['fair']],
    #        'resource coef.', 'Unfairness', ('RR', 'PRR', 'Ordered', 'Direct'),
    #        'best', pdf_name=file_name, show=show)

    # title = 'Normalized Cost {}'.format(additional_title)
    # if pdf_name is not None:
    #     file_name = title + '_resource_coef'
    # p.plot([x, x, x, x], [c_data['rr'], c_data['prr'], c_data['fcfs'], c_data['fair']],
    #        'resource coef.', 'Normalized Cost', ('RR', 'PRR', 'Ordered', 'Direct'),
    #        'best', pdf_name=file_name, show=show, figure_num=figure_num)

    title = 'Normalized Makespan and Cost{}'.format(additional_title)
    if pdf_name is not None:
        file_name = title + '_rc' + pdf_name
    p.plot([x, x, x, x, x, x, x, x],
           [m_data['rr'], m_data['prr'], m_data['fcfs'], m_data['fair'],
            c_data['rr'], c_data['prr'], c_data['fcfs'], c_data['fair']],
           'resource coef.', 'Normalized Makespan and Cost',
           ('RR', 'PRR', 'Ordered', 'Direct'),
           'best', pdf_name=file_name, show=show, figure_num=figure_num)

def query_budget_rate(r):
    rows = r.select_query('''
		select sum(cost_old)/sum(cost_new) as cost, sum(h.U)/count(h.U) as uf,
			t.budget_ratio, method
			from results r
				inner join result_head h on r.head_id = h.id
				inner join test_spec t on h.testname = t.key
			group by
				t.budget_ratio,
				method
			order by
				method, t.budget_ratio''', [])

    n = 3

    u_data = {}
    u_data['rr'] = [0] * n
    u_data['prr'] = [0] * n
    u_data['fcfs'] = [0] * n
    u_data['fair'] = [0] * n
    c_data = {}
    c_data['rr'] = [0] * n
    c_data['prr'] = [0] * n
    c_data['fcfs'] = [0] * n
    c_data['fair'] = [0] * n
    my_map = lambda x: int(float(x) * 2 + 0.5)
    for row in rows:
        cost = row[0]
        u = row[1]
        b = str(row[2])
        method = row[3]
        # print(size)
        method = unicodedata.normalize('NFKD', method).encode('ascii', 'ignore')

        print("{}, {}".format(method, b))
        if method not in u_data:
            print("not Exist!")
            continue
        u_data[method][my_map(b)] = u
        c_data[method][my_map(b)] = 1.0 / cost

    x = ['Deadline only', 'Budget and Deadline', 'Budget only']

    p.bar([u_data['rr'], u_data['prr'], u_data['fcfs'], u_data['fair']],
          '', '', 'Average Unfairness', x, ('RR', 'PRR', 'Ordered', 'Direct')
          , pdf_name='U_budget.pdf')

    x = [0, 0.5, 1]
    p.plot([x, x, x, x], [c_data['rr'], c_data['prr'], c_data['fcfs'], c_data['fair']],
           'Budget constraint ratio of workload', 'Cost', ('RR', 'PRR', 'Ordered', 'Direct'),
           'upper right', 'cost_budget.pdf')

def query_num_resource(r):
    rows = r.select_query('''
		select sum(cost_old)/sum(cost_new) as cost, sum(h.U)/count(h.U) as uf, t.num_resource as num, method
			from results r
				inner join result_head h on r.head_id = h.id
				inner join test_spec t on h.testname = t.key
			group by
				t.num_resource,
				method
			having not (h.testname like 'test%')
			order by
				method, t.num_resource''', [])

    n = 3

    u_data = {}
    u_data['rr'] = [0] * n
    u_data['prr'] = [0] * n
    u_data['fcfs'] = [0] * n
    u_data['fair'] = [0] * n
    c_data = {}
    c_data['rr'] = [0] * n
    c_data['prr'] = [0] * n
    c_data['fcfs'] = [0] * n
    c_data['fair'] = [0] * n
    my_map = {'20': 0, '40': 1, '80': 2}
    for row in rows:
        cost = row[0]
        u = row[1]
        num = str(row[2])
        method = row[3]
        # print(size)
        method = unicodedata.normalize('NFKD', method).encode('ascii', 'ignore')

        print("{}, {}".format(method, num))
        if method not in u_data:
            print("not Exist!")
            continue
        u_data[method][my_map[num]] = u
        c_data[method][my_map[num]] = 1.0 / cost

    x = ['60', '120', '240']

    p.bar([u_data['rr'], u_data['prr'], u_data['fcfs']],
          '', 'Number of reference system resources', 'Average Unfairness', x,
          ('RR', 'PRR', 'Ordered', 'Direct'),
          pdf_name='U_res.pdf')

    x = [1, 2, 3]
    p.plot([x, x, x, x], [c_data['rr'], c_data['prr'], c_data['fcfs'], c_data['fair']],
           'resource set (60, 120, 240)', 'Cost', ('RR', 'PRR', 'Ordered', 'Direct'),
           'upper right', 'cost_res.pdf')

def query_timeslot(r):
    rows = r.select_query('''
		select sum(cost_old)/sum(cost_new) as cost, sum(h.U)/count(h.U) as uf, t.timeslot as num, method
			from results r
				inner join result_head h on r.head_id = h.id
				inner join test_spec t on h.testname = t.key
			group by
				t.timeslot,
				method
			order by
				method, t.timeslot''', [])

    n = 2

    u_data = {}
    u_data['rr'] = [0] * n
    u_data['prr'] = [0] * n
    u_data['fcfs'] = [0] * n
    c_data = {}
    c_data['rr'] = [0] * n
    c_data['prr'] = [0] * n
    c_data['fcfs'] = [0] * n
    my_map = {'3': 0, '6': 1}
    for row in rows:
        cost = row[0]
        u = row[1]
        timeslot = str(row[2])
        method = row[3]
        # print(size)
        method = unicodedata.normalize('NFKD', method).encode('ascii', 'ignore')

        print("{}, {}".format(method, timeslot))
        if method not in u_data:
            print("not Exist!")
            continue
        u_data[method][my_map[timeslot]] = u
        c_data[method][my_map[timeslot]] = 1.0 / cost

    x = ['3', '6']

    p.bar([u_data['rr'], u_data['prr'], u_data['fcfs']], '', 'Length of timeslot', 'Average Unfairness', x,
          ('RR', 'PRR', 'Ordered'), 'lower right', pdf_name='U_timeslot.pdf')
    p.bar([c_data['rr'], c_data['prr'], c_data['fcfs']], '', 'Length of timeslot', 'Average normalized cost', x,
          ('RR', 'PRR', 'Ordered'), 'lower right', pdf_name='cost_timeslot.pdf')
    # x = [3, 6]
    # p.plot([x, x, x], [c_data['rr'], c_data['prr'], c_data['fcfs']], 'Timeslot length', 'Cost', ('RR', 'PRR', 'Ordered'),
    #        'upper right')

def query_workflow(r):
    rows = r.select_query('''
    select
        --makespan_new,
        --cost_new,
	    1/m_rate m,
		1/c_rate c,
		job_name
	from
			results r inner join result_head h on
				r.head_id = h.id
		where --h.id = 26599 and
		 job_size=100 and workload_len = 100 and deadline=-1
		 and h.t = 60 and h.c = 0.7 and method = 'fair' ''', [])

    c={}
    m={}
    types = ['Inspiral', 'Epigenomics', 'CyberShake', 'Montage']

    for t in types:
        c[t]=[]
        m[t]=[]

    for row in rows:
        # if random.random() > 0.01:
        #     continue
        makespan = row[0]
        cost = row[1]
        workflow_type = row[2]
        workflow_type = unicodedata.normalize('NFKD', workflow_type).encode('ascii', 'ignore')
        c[workflow_type].append(cost)
        m[workflow_type].append(makespan)

    colors = ['orange', 'red', 'blue', 'green']
    markers = ['^', 'x', '+', 'o']

    figure = plt.figure(num=1, figsize=(6, 6), dpi=80)
    ax1 = figure.add_subplot(111)
    plt.xlabel('Makespan')
    plt.ylabel('Cost')
    plt.tight_layout()
    plt.grid(True,axis='both')
    ax1.ticklabel_format(axis='y', style='sci', useOffset=False)

    for i in range(4):
        plt.scatter(m[types[i]], c[types[i]], s=30, marker=markers[i],
                 facecolors='none', edgecolors=colors[i], linewidth=0.8)
    plt.legend(types, 'best')#'low'upper left')  #
    plt.show()

#-------------------------------------------------------------------------------------------------------

# additional_condition = ' and h.c = 0.9 '
# additional_condition = ' and h.t = 5 and h.c = 0.4'
additional_condition = ' and h.t = 60 and h.c = 0.5'
# additional_condition = ' and h.t = 60 and workload_len=80 and budget<>-1'
# additional_condition = ' and h.t = 60 and budget<>-1'

reader = db.reader.Reader(sys.argv[1])

#query_workflow(reader) #Compares different workflow types (both cost and makespan)

# query_coef_p_more_param(reader, show=True, figure_num=1, pdf_name='_t05_', budget_ratio='1')
# query_coef_p_more_param(reader, show=True, figure_num=1, pdf_name='_t60_ALL')

# query_coef_p(reader)
# query_coef_p(reader, budget_ratio=1)
# query_coef_p(reader, budget_ratio=0.5)
# query_coef_p(reader, budget_ratio=0)
# query_coef_p(reader, constraint='Deadline')
# query_coef_p(reader, constraint='Budget')

# query_workload_p(reader)
# query_workload_p(reader, budget_ratio=1)
# query_workload_p(reader, budget_ratio=0.5)
query_workload_p(reader, budget_ratio=0)
# query_workload_p(reader, constraint='Deadline')
# query_workload_p(reader, constraint='Budget')

# query_budget_rate(reader)
# query_num_resource(reader)
# query_timeslot(reader)
