import random
import numpy as np
from matplotlib import pyplot as pyplot
from matplotlib.patches import Rectangle
import reports.plot
from db import reader

r = reader.Reader('/var/scratch/rezaeian/mydb_medium')
rows = r.select_query('''
    select job_name, job_type, task_id, jobs_id, start_time, finish_time, resource_id,
                   resource_speed
        from plans p
            where jobs_id = 5
            --extra_params=? and job_component_id=?
            --where job_name=?
            ''',())
                      #['multi','fcfs'])
                      # ('jxxbb', ))

def name_2_color(name):
    ords = list(map(ord, name))
    c1 = (ords[0]*ords[1]) % 255
    c2 = (ords[3]*ords[4]) % 255
    c3 = (ords[2]*ords[4]*ords[0]) % 255
    return "#{:0>2x}{:0>2x}{:0>2x}".format(c1, c2, c3)


fig = pyplot.figure()
ax = fig.add_subplot(111)
max_x = 0
max_y = 0
job_name_to_color = dict()
for row in rows:
    job_name, job_type, task_id, job_id, start_time, \
    finish_time, resource_id, resource_speed = row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7]

    max_y = max(max_y, resource_id)
    max_x = max(max_x, finish_time)

    reports.plot.add_rect(resource_id, start_time, finish_time, name_2_color(job_name))
    # reports.plot.add_rect(resource_id, start_time, finish_time, job_name_to_color[job_name])

ax.plot(max_x, 1, zorder=1)
ax.plot(0, 0, zorder=1)
pyplot.show()

# colors = ['g','r','y','b','c','m','k']
# fig = pyplot.figure()
# ax = fig.add_subplot(111)
# maxim = 0
# for i in range(100):
#     r = random.randint(0, 9)
#     start = random.random() * 100
#     stop = start + random.random() * 10
#     color = random.randint(0, len(colors) - 1)
#     reports.plot.add_rect(r, start, stop, colors[color])
#     if stop > maxim:
#         maxim = stop
# ax.plot(maxim, 1, zorder=1)
# ax.plot(0, 0, zorder=1)
# pyplot.show()
