
import polars as pl

def export( host, database, table ):
    mysql = "mysql://root@{host}:3306/{database}".format(host=host, database=database)
    df = (pl
          .read_database_uri(uri=mysql, query="select TO_BASE64(uuid) as uuid, TO_BASE64(data) as data from {table}".format(table=table), engine='connectorx')
          .lazy())
    print(df.head(10))
    df.write_parquet("results.parquet")

    dfP = pl.read_parquet("results.parquet")
    print(dfP.schema)
    print(dfP.head(3))


if __name__ == "__main__":
    export("localhost", "production_env4", "delivs_2024_10")