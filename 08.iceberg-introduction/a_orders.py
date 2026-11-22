import functools

print = functools.partial(print, flush=True)
exec(open("/home/onyxia/work/08.iceberg-introduction/session.py").read())


def rows(sql):
    return [r.asDict() for r in spark.sql(sql).collect()]


def line(tag, value):
    print("L08", tag, value)


def files(table):
    out = rows(f"SELECT file_path, record_count, file_size_in_bytes FROM {table}.files")
    for r in out:
        r["file_path"] = r["file_path"].split("/")[-1]
    return out


line("namespaces", [r[0] for r in spark.sql("SHOW NAMESPACES IN lab").collect()])
line("orders", rows("SELECT count(*) AS orders, sum(quantity) AS quantity FROM lab.silver.orders"))
line("snapshots columns", spark.sql("SELECT * FROM lab.silver.orders.snapshots").columns)
for r in rows("SELECT snapshot_id, operation, committed_at FROM lab.silver.orders.snapshots ORDER BY committed_at"):
    line("snapshot", r)
for r in files("lab.silver.orders"):
    line("file", r)

stg_orders = spark.table("lab.silver.orders")
new_orders = stg_orders.limit(3).withColumn("order_id", F.expr("uuid()"))
new_orders.writeTo("lab.silver.orders").append()
for r in rows("SELECT snapshot_id, operation, committed_at FROM lab.silver.orders.snapshots ORDER BY committed_at"):
    line("snapshot after append", r)
line("count after append", rows("SELECT count(*) AS n FROM lab.silver.orders"))
for r in files("lab.silver.orders"):
    line("file after append", r)

snapshots = spark.sql("SELECT snapshot_id FROM lab.silver.orders.snapshots ORDER BY committed_at").collect()
first_snapshot = snapshots[0]["snapshot_id"]
line("first snapshot", first_snapshot)
line("count at first snapshot", rows(f"SELECT count(*) AS n FROM lab.silver.orders VERSION AS OF {first_snapshot}"))
line("count current", rows("SELECT count(*) AS n FROM lab.silver.orders"))

spark.sql("ALTER TABLE lab.silver.orders ADD COLUMN source STRING")
line("describe", [(r["col_name"], r["data_type"]) for r in spark.sql("DESCRIBE lab.silver.orders").collect()])
spark.sql("INSERT INTO lab.silver.orders SELECT order_id, user_id, ordered_at, quantity, product, 'spark' FROM lab.silver.orders LIMIT 1")
line("after insert", rows("SELECT count(*) AS n, count(source) AS with_source FROM lab.silver.orders"))
for r in rows("SELECT snapshot_id, operation, committed_at FROM lab.silver.orders.snapshots ORDER BY committed_at"):
    line("snapshot final", r)
for r in files("lab.silver.orders"):
    line("file final", r)
line("columns at first snapshot", spark.sql(f"SELECT * FROM lab.silver.orders VERSION AS OF {first_snapshot}").columns)
line("columns now", spark.table("lab.silver.orders").columns)
line("a_orders OK", "")
spark.stop()
