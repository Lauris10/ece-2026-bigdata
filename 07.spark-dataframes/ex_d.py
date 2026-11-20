import functools
import datetime

print = functools.partial(print, flush=True)
exec(open("/home/onyxia/work/07.spark-dataframes/setup.py").read())

print("== EX4 sur la vraie table")
before = spark.sql("SELECT snapshot_id FROM lab.silver.orders.snapshots ORDER BY committed_at DESC").first()["snapshot_id"]
n_before = spark.table("lab.silver.orders").count()
first_user = stg_users.select("user_id").first()["user_id"]
new_order = spark.createDataFrame(
    [("00000000-0000-0000-0000-0000000000ff", first_user, datetime.datetime(2026, 11, 20, 12, 0, 0), 3, "croissant")],
    "order_id string, user_id string, ordered_at timestamp, quantity int, product string",
)
new_order.writeTo("lab.silver.orders").append()
snaps = spark.sql("SELECT snapshot_id, operation, committed_at FROM lab.silver.orders.snapshots ORDER BY committed_at").collect()
for r in snaps:
    print("EX4 snapshot", r["snapshot_id"], r["operation"], r["committed_at"])
now_n = spark.table("lab.silver.orders").count()
old_n = spark.sql(f"SELECT count(*) AS n FROM lab.silver.orders VERSION AS OF {before}").first()["n"]
print("EX4 Iceberg: avant", n_before, "| apres", now_n, "| VERSION AS OF snapshot", before, "->", old_n)
p = f"s3a://{BUCKET}/silver/spark/orders.parquet"
new_order.write.mode("append").parquet(p)
print("EX4 Parquet apres ajout:", spark.read.parquet(p).count(), "lignes")
print("ex_d OK")
spark.stop()
