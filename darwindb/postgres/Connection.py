import psycopg2

class Connection:

    def __init__(self, host, dbname, user, password):
        self.host = host
        self.dbname = dbname
        self.user = user
        self.password = password

    def connect(self):
        self.conn = psycopg2.connect("host='{}' dbname='{}' user='{}' password='{}'".format(
            self.host, self.dbname, self.user, self.password))

    def cursor(self):
        return self.conn.cursor()
    
    def commit(self):
        return self.conn.commit()


