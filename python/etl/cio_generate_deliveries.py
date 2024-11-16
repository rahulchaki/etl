
import uuid
from random import  Random
from typing import Dict

import mysql.connector


useDB= "use {database}"
dropDB = "DROP DATABASE IF EXISTS `{database}`"
createDB = """
CREATE DATABASE `{database}` 
/*!40100 DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci */ /*!80016 DEFAULT ENCRYPTION='N' */
"""

dropTable = "DROP TABLE  IF EXISTS `{table}` "
createTable = """
        CREATE TABLE `{table}` (
          `uuid` varbinary(192) NOT NULL,
          `data` varbinary(60000)   NOT NULL,
          PRIMARY KEY (`uuid`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
        """


def generate_data( rows ):
    rand = Random()
    return [{'uuid':uuid.uuid4().bytes, 'data': rand.randbytes( rand.randint( 1024*1, 1024*32 ) ) } for _ in range(rows)]

def insertInto( conn, database, table, batch, numBatches):
    for _ in range( numBatches ):
        cursor = conn.cursor()
        data = generate_data(batch)
        insert_row = ("INSERT INTO {database}.{table} ( uuid, data ) VALUES ( %(uuid)s, %(data)s )"
                      .format(database=database, table=table))
        cursor.executemany(insert_row, data)
        conn.commit()



def execSql( conn, stmnts: list[str] ):
    cursor = conn.cursor()
    for stmnt in stmnts:
        cursor.execute(stmnt)
    conn.commit()

def setupDatabases( conn, schemas: Dict[str, list[str]] ):
    databases = list(schemas.keys())
    for database in databases:
        execSql(
            conn,
            [
                dropDB.format(database=database) ,
                createDB.format(database=database)
            ]
        )
        for table in schemas[database]:
            execSql(
                conn,
                [
                    useDB.format(database=database),
                    dropTable.format(table=table),
                    createTable.format(table=table)
                ]
            )
            insertInto( conn, database, table, 10, 100 )

def loadDatabases( conn ):
    cursor = conn.cursor()
    cursor.execute( " SHOW DATABASES")
    rows = cursor.fetchall()
    return [row[0] for row in rows]


def run():
    conn = mysql.connector.connect(host='localhost', port=3306, user='root')
    schemas = {}
    for i in range( 100 ):
        database = "production_env{env}".format(env=i)
        schemas[database] = ['delivs_2024_10']
    setupDatabases( conn, schemas )
    #print(loadDatabases(conn))
    conn.close()





if __name__ == "__main__":
    run()