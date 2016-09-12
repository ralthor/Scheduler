import codecs
import Definitions.Graph as Graph


def read_jobs(file_name):
    """
    Reads workflows as jobs from file with file_name
    :param file_name:
    :rtype : dict
    """
    jobs = {}
    file = codecs.open(file_name, "r", "utf-8")
    while 1:
        lines = file.readlines(10000000)  # ~10M buffer.
        if not lines:
            break
        # Process lines.
        for line in lines:
            if len(line) == 0 or line[0] == ';':
                continue
            try:
                g = Graph.Graph()
                rest = line.split('|')
                name_part = rest[0]
                parts = name_part.split(',')
                job_id = parts[0]
                g.set_name(job_id)
                g.set_type(job_id + file_name[0])
                start_id, end_id = map(int, parts[1:])
                g.set_start_end(start_id, end_id)
                task_id = 0
                for component in rest[1:]:
                    children = component.split(',')
                    task_id, w, children = task_id + 1, float(children[0]), children[1:]
                    children, edge_weights = children[0:(len(children) // 2)], children[(len(children) // 2):]
                    children = list(map(int, children))
                    edge_weights = list(map(float, edge_weights))
                    g.add_task(task_id, w, children, edge_weights)
                jobs[job_id] = g
                g.set_predecessors()
                g.tasks[start_id].dummy_task = True
                g.tasks[end_id].dummy_task = True
            except RuntimeError:
                print('Runtime Exception!')
    file.close()
    return jobs
