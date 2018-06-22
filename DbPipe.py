
import mysql.connector
import psycopg2

mysqlDbConfig = {
    'host': '127.0.0.1',
    'user': 'root',
    'password': 'root',
    'database': 'test_data', }

pgDbConfig = {
    'host': '127.0.0.1',
    'user': 'postgres',
    'password': 'pass',
    'database': 'bazeika_db', }

mysqlConn = None
mysqlCursor = None

pgConn = None
pgCursor = None

try:
    mysqlConn = mysql.connector.connect(**mysqlDbConfig)
    pgConn = psycopg2.connect(**pgDbConfig)

    mysqlCursor = mysqlConn.cursor()
    pgCursor = pgConn.cursor()

    _SQL_select_from_mysql = """select * from test_launching_history"""

    _SQL_insert_to_postgres = """insert into test_launching_history 
                                (id, test_id, status, create_date, release_cycle) 
                                values 
                                (%s, %s, %s, %s, %s)"""

    mysqlCursor.execute(_SQL_select_from_mysql)

    mysqlRes = mysqlCursor.fetchall()

    for row in mysqlRes:
        pgCursor.execute(_SQL_insert_to_postgres, (row[0], row[1], row[2], row[3], row[4]))

    pgConn.commit()

except (Exception, psycopg2.DatabaseError, mysql.connector.DatabaseError) as error:
    print(error)

finally:
    if mysqlConn is not None and mysqlCursor is not None:
        mysqlConn.close()
        mysqlCursor.close()
        print('Mysql database connection closed.')

    if pgConn is not None and pgCursor is not None:
        pgConn.close()
        pgCursor.close()
        print('Postgres database connection closed.')

