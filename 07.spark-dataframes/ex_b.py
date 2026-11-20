import functools
import time

print = functools.partial(print, flush=True)
exec(open("/home/onyxia/work/07.spark-dataframes/setup.py").read())

print("== EX3 cache")
def count_time(label, df):
    t = time.perf_counter()
    n = df.count()
    print("EX3", label, n, round(time.perf_counter() - t, 3), "s")
count_time("users sans cache (1)", users_bronze)
count_time("users sans cache (2)", users_bronze)
users_c = users_bronze.cache()
count_time("users avec cache (remplissage)", users_c)
count_time("users avec cache (lecture)", users_c)
print("EX3 niveau de stockage:", users_c.storageLevel)
count_time("orders sans cache (1)", orders_bronze)
count_time("orders sans cache (2)", orders_bronze)
orders_c = orders_bronze.cache()
count_time("orders avec cache (remplissage)", orders_c)
count_time("orders avec cache (lecture)", orders_c)
users_c.unpersist()
orders_c.unpersist()
print("ex_b OK")
spark.stop()
