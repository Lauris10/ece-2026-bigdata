import functools
import datetime

print = functools.partial(print, flush=True)
exec(open("/home/onyxia/work/07.spark-dataframes/setup.py").read())

print("== EX1 birthdate_is_valid")
first_order = stg_orders.groupBy("user_id").agg(F.min("ordered_at").alias("first_order_at"))
users_df = (
    stg_users.join(first_order, "user_id", "left")
    .withColumn("birthdate_is_valid", F.when(F.col("birthdate") > F.to_date("first_order_at"), F.lit(False)).otherwise(F.lit(True)))
    .drop("first_order_at")
)
stg_users.createOrReplaceTempView("stg_users")
stg_orders.createOrReplaceTempView("stg_orders")
users_sql = spark.sql("""
    SELECT u.user_id, u.username, u.name, u.sex, u.email, u.birthdate, u.street, u.city, u.state, u.zip_code,
           COALESCE(u.birthdate <= CAST(o.first_order_at AS DATE), TRUE) AS birthdate_is_valid
    FROM stg_users u
    LEFT JOIN (SELECT user_id, min(ordered_at) AS first_order_at FROM stg_orders GROUP BY user_id) o
      ON u.user_id = o.user_id
""")
print("EX1 users:", users_df.count(), "| invalides DataFrame:", users_df.filter(~F.col("birthdate_is_valid")).count(), "| invalides SQL:", users_sql.filter("NOT birthdate_is_valid").count())
print("EX1 lignes differentes entre les deux:", users_df.exceptAll(users_sql.select(*users_df.columns)).count())

print("== EX4 append Iceberg et time travel")
spark.sql("DROP TABLE IF EXISTS lab.silver.orders_timetravel PURGE")
stg_orders.writeTo("lab.silver.orders_timetravel").createOrReplace()
first_user = stg_users.select("user_id").first()["user_id"]
new_order = spark.createDataFrame(
    [("00000000-0000-0000-0000-0000000000ff", first_user, datetime.datetime(2026, 11, 20, 12, 0, 0), 3, "croissant")],
    "order_id string, user_id string, ordered_at timestamp, quantity int, product string",
)
new_order.writeTo("lab.silver.orders_timetravel").append()
snaps = spark.sql("SELECT snapshot_id, operation, committed_at FROM lab.silver.orders_timetravel.snapshots ORDER BY committed_at").collect()
for r in snaps:
    print("EX4 snapshot", r["snapshot_id"], r["operation"], r["committed_at"])
first_id = snaps[0]["snapshot_id"]
now_n = spark.table("lab.silver.orders_timetravel").count()
old_n = spark.sql(f"SELECT count(*) AS n FROM lab.silver.orders_timetravel VERSION AS OF {first_id}").first()["n"]
print("EX4 Iceberg: lignes actuelles", now_n, "| lignes au premier snapshot", old_n)
p = f"s3a://{BUCKET}/silver/spark/orders_append_test.parquet"
stg_orders.write.mode("overwrite").parquet(p)
new_order.write.mode("append").parquet(p)
print("EX4 Parquet apres ajout:", spark.read.parquet(p).count(), "lignes, sans historique")
spark.sql("DROP TABLE lab.silver.orders_timetravel PURGE")
print("ex_a OK")
spark.stop()
