import db.connection
import db.definitions


class Reader:
    def __init__(self, db_file):
        self.connection = db.connection.Connection(db_file)
        self.cursor = self.connection.get_cursor()

    def read_test(self, test_name):
        rows = self.cursor.execute('''select testname, workload_len, resources,
                                    budget_ratio, small, medium, large, bw
                                    from `tests` where testname = ? ''', (test_name,))
        return rows

    def select_query(self, query, params):
        rows = self.cursor.execute(query, params)
        return rows
