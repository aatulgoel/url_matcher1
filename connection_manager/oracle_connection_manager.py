class ManageConnection:

    def __init__(self):
        oracle_connection = None

    def create_connection(self):
        import cx_Oracle
        try:
            connection_string = "url_matcher/url_matcher@localhost"
            self.oracle_connection = cx_Oracle.connect(connection_string)
            conn = self.oracle_connection
            return conn
        except Exception as e:
            print(e)

    def test_connection(self):
        cur = self.oracle_connection.cursor()
        cur.execute("select * from dual")

        for dummy in cur.fetchall():
            print(dummy)
        cur.close()

    def distroy_connection(self):
        self.oracle_connection.close()


if __name__ == '__main__':
    test_oracle_connection = ManageConnection()
    test_oracle_connection.create_connection()
    test_oracle_connection.test_connection()
    test_oracle_connection.distroy_connection()