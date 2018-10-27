class ManageConnection:

    def get_connection(self):
        import cx_Oracle
        from sqlalchemy import create_engine

        host = 'localhost'
        port = '1521'
        sid = 'xe'
        user = 'url_matcher'
        password = 'url_matcher'
        sid = cx_Oracle.makedsn(host, port, sid=sid)

        connection_string = 'oracle://{user}:{password}@{sid}'.format(
            user=user,
            password=password,
            sid=sid
        )

        try:
            engine = create_engine(
                connection_string,
                convert_unicode=False,
                pool_recycle=10,
                pool_size=50,
                echo=True
            )

            connection = engine.connect()
            return connection

        except Exception as e:
            print(e)

    def test_connection(self):

        connection = self.get_connection()

        result = connection.execute('select * from DUAL')
        for row in result:
            print(row)


if __name__ == '__main__':
    test_oracle_connection = ManageConnection()
    test_oracle_connection.get_connection()
    test_oracle_connection.test_connection()