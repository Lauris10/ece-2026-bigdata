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


def snaps(table):
    return rows(f"SELECT snapshot_id, operation, committed_at FROM {table}.snapshots ORDER BY committed_at")


line("users", rows("SELECT count(*) AS users FROM lab.silver.users"))
for r in snaps("lab.silver.users"):
    line("snapshot", r)
first_snapshot = snaps("lab.silver.users")[0]["snapshot_id"]

users = spark.table("lab.silver.users")
batch1 = (
    users.limit(3)
    .withColumn("user_id", F.expr("uuid()"))
    .withColumn("username", F.concat(F.col("username"), F.lit("_new")))
    .withColumn("email", F.concat(F.lit("new_"), F.col("email")))
)
batch1.writeTo("lab.silver.users").append()
for r in snaps("lab.silver.users"):
    line("snapshot after append", r)
line("count current", rows("SELECT count(*) AS n FROM lab.silver.users"))
line("count at first snapshot", rows(f"SELECT count(*) AS n FROM lab.silver.users VERSION AS OF {first_snapshot}"))
for r in files("lab.silver.users"):
    line("file after append", r)

spark.sql("ALTER TABLE lab.silver.users ADD COLUMN country STRING")
line("describe", [(r["col_name"], r["data_type"]) for r in spark.sql("DESCRIBE lab.silver.users").collect()])
batch2 = (
    spark.table("lab.silver.users").limit(2)
    .withColumn("user_id", F.expr("uuid()"))
    .withColumn("username", F.concat(F.col("username"), F.lit("_c")))
    .withColumn("email", F.concat(F.lit("c_"), F.col("email")))
    .withColumn("country", F.lit("US"))
)
batch2.writeTo("lab.silver.users").append()
line("count and countries", rows("SELECT count(*) AS n, count(country) AS with_country FROM lab.silver.users"))
line("by country", rows("SELECT country, count(*) AS n FROM lab.silver.users GROUP BY country ORDER BY n DESC"))
for r in snaps("lab.silver.users"):
    line("snapshot final", r)
for r in files("lab.silver.users"):
    line("file final", r)
line("columns at first snapshot", spark.sql(f"SELECT * FROM lab.silver.users VERSION AS OF {first_snapshot}").columns)
line("columns now", spark.table("lab.silver.users").columns)
line("b_users OK", "")
spark.stop()
