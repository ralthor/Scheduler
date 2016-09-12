import db.connection
import db.definitions


class Writer:
    def __init__(self, db_file, buffer_size=1000):
        self.connection = db.connection.Connection(db_file)
        self.cursor = self.connection.get_cursor()
        self.internal_counter = 0
        self.buffer_size = buffer_size

    def close(self):
        self.connection.commit()
        self.connection.close()

    def create_plan(self, recreate=False):
        if recreate:
            self.cursor.execute('''DROP Table If Exists plans ''')

        self.cursor.execute('''
            Create Table If Not Exists plans
                (`id` integer primary key autoincrement,
                `job_name` text,
                `job_type` text,
                `task_id` integer,
                `jobs_id` integer,
                `start_time` real,
                `finish_time` real,
                `resource_id` integer,
                `resource_speed` integer,
                `job_component_id` text default NULL,
                `extra_params` text default NULL
                )
                 ''')
        self.connection.commit()

    def create_plan_head(self, recreate=False):
        if recreate:
            self.cursor.execute('''DROP Table If Exists plan_head ''')

        self.cursor.execute('''
            Create Table If Not Exists plan_head
                (`id` integer primary key autoincrement,
                `testname` text,
                `method` text,
                `job_count` integer,
                `job_component_id` text default NULL,
                `extra_params` text default NULL
                )
                 ''')
        self.connection.commit()

    def create_tests(self):
        self.cursor.execute('''CREATE TABLE tests ("testname" TEXT NOT NULL DEFAULT (1),"workload_len" INTEGER NOT NULL
            DEFAULT (20),"resources" TEXT NOT NULL DEFAULT ('{"t":3, "r":[[1, 1, 10], [2, 3, 10], [3, 8, 10]]}'),
            "budget_ratio" REAL NOT NULL DEFAULT (0.5),"small" REAL NOT NULL DEFAULT (0.5),"medium" REAL NOT NULL
            DEFAULT (0.3),"large" REAL NOT NULL DEFAULT (0.2),"BW" INTEGER NOT NULL DEFAULT (1000))''')
        self.connection.commit()

    def create_tasks(self):
        self.cursor.execute('''CREATE TABLE "tasks" ("testname" TEXT NOT NULL,"job_id" INTEGER NOT NULL,
            "task_id" INTEGER NOT NULL,"resource_number" INTEGER NOT NULL,"start" REAL NOT NULL,"end" REAL NOT NULL,
            "id" INTEGER PRIMARY KEY NOT NULL)''')
        self.connection.commit()

    def create_results(self):
        self.cursor.execute('''CREATE TABLE "results" (
            "id" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
            "head_id" INTEGER NOT NULL,
            "testname" TEXT NOT NULL,
            "method" TEXT NOT NULL,
            "constraint" TEXT NOT NULL DEFAULT ('Budget'),
            "deadline" REAL NOT NULL DEFAULT (-1),
            "budget" REAL NOT NULL DEFAULT (-1),
            "makespan_old" REAL NOT NULL,
            "makespan_new" REAL NOT NULL,
            "cost_old" REAL NOT NULL,
            "cost_new" REAL NOT NULL,
            "gap_rate" REAL NOT NULL,
            "c_rate" REAL NOT NULL,
            "m_rate" REAL NOT NULL),
            "job_name" TEXT NOT NULL DEFAULT ('Not Specified'),
            "job_size" INTEGER NOT NULL DEFAULT(0)''')
        self.connection.commit()

    def write_results(self, results, commit_now=True):
        self.cursor.execute('''INSERT INTO results (head_id, testname, method, `constraint`, deadline, budget,
        makespan_old, makespan_new, cost_old, cost_new, gap_rate, c_rate, m_rate, job_name, job_size)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', results.get_row())
        if commit_now:
            self.connection.commit()

    def commit(self):
        self.connection.commit()

    def create_result_head(self):
        self.cursor.execute('''CREATE TABLE "result_head" (
            "id" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
            "testname" TEXT NOT NULL,
            "separate_cost" INTEGER NOT NULL DEFAULT (-1),
            "U" REAL NOT NULL DEFAULT(-1),
            "gap_rate" REAL NOT NULL DEFAULT(-1),
            "workload_len" INTEGER NOT NULL DEFAULT(0))''')
        self.connection.commit()

    def write_result_head(self, test_name):
        self.cursor.execute('''INSERT INTO result_head (id, testname) VALUES (NULL, ?)''', (test_name,))
        id = self.cursor.lastrowid  # query_db('SELECT last_insert_rowid()')
        self.connection.commit()
        return id

    def write_plan_head(self, test_name, method='single', job_count=1):
        self.cursor.execute('''
          INSERT INTO plan_head (id, testname, method, job_count)
            VALUES (NULL, ?, ?, ?)
        ''', (test_name, method, job_count))
        id = self.cursor.lastrowid
        self.connection.commit()
        return id

    def change_result_head(self, id, test_name, separate_cost, u, workload_len, final_gap_rate, t, c):
        self.cursor.execute('''UPDATE result_head
                                    SET testname=?, separate_cost=?, U=?, workload_len=?,
                                    gap_rate=?, t=?, c=?
                                  WHERE id=?''',
                            (test_name, separate_cost, u,
                             workload_len, final_gap_rate, t, c, id))
        self.connection.commit()

    def write_plan(self, job_name, job_type, task_id, jobs_id, start_time, finish_time, resource_id,
                   resource_speed, job_component_id='', extra_params=''):
        self.cursor.execute('''
          INSERT INTO plans (id, job_name, job_type, task_id, jobs_id, start_time, finish_time, resource_id,
                   resource_speed, job_component_id, extra_params)
                   VALUES (NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                            (job_name, job_type, task_id, jobs_id, start_time, finish_time, resource_id,
                             resource_speed, job_component_id, extra_params))
        self.buffer_based_commit()

    def buffer_based_commit(self):
        self.internal_counter += 1
        if self.internal_counter > self.buffer_size:
            self.commit()
            self.internal_counter = 0

# writing and getting the row_id:
# g.db.execute('INSERT INTO downloads (name, owner, mimetype) VALUES (?, ?, ?)', [name, owner, mimetype])
# file_entry = query_db('SELECT last_insert_rowid()')
# g.db.commit()
