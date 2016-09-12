import sqlite3


class Connection:
    def __init__(self, database_name):
        self.connection = sqlite3.connect(database_name)

    def get_cursor(self):
        return self.connection.cursor()

    def commit(self):
        self.connection.commit()

    def close(self):
        self.connection.close()
