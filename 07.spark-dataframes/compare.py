import os
import duckdb

b = os.environ["KUBERNETES_NAMESPACE"]
con = duckdb.connect()
con.execute("INSTALL httpfs")
con.execute("LOAD httpfs")
con.execute(f"""
CREATE OR REPLACE SECRET lab_s3 (
    TYPE s3,
    KEY_ID '{os.environ["AWS_ACCESS_KEY_ID"]}',
    SECRET '{os.environ["AWS_SECRET_ACCESS_KEY"]}',
    SESSION_TOKEN '{os.environ["AWS_SESSION_TOKEN"]}',
    ENDPOINT '{os.environ["AWS_S3_ENDPOINT"]}',
    REGION '{os.environ["AWS_DEFAULT_REGION"]}',
    URL_STYLE 'path'
)
""")
con.execute("SET TimeZone = 'UTC'")
d = f"s3://{b}/silver/dbt"
s = f"s3://{b}/silver/spark"
print("ORDERS", con.execute(f"SELECT 'dbt' AS engine, count(*) AS n, sum(quantity) AS qty FROM read_parquet('{d}/orders.parquet') UNION ALL SELECT 'spark', count(*), sum(quantity) FROM read_parquet('{s}/orders.parquet/*.parquet')").fetchall())
print("USERS", con.execute(f"SELECT 'dbt' AS engine, count(*) AS n FROM read_parquet('{d}/users.parquet') UNION ALL SELECT 'spark', count(*) FROM read_parquet('{s}/users.parquet/*.parquet')").fetchall())
for label, path in [("dbt orders", f"{d}/orders.parquet"), ("spark orders", f"{s}/orders.parquet/*.parquet"), ("dbt users", f"{d}/users.parquet"), ("spark users", f"{s}/users.parquet/*.parquet")]:
    cols = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{path}')").fetchall()
    print(label, [(c[0], c[1]) for c in cols])
