import functools
import time

print = functools.partial(print, flush=True)
exec(open("/home/onyxia/work/07.spark-dataframes/setup.py").read())

print("== EX3 UI")
users_c = users_bronze.cache()
orders_c = orders_bronze.cache()
print("EX3 users", users_c.count(), "orders", orders_c.count())
print("EX3 users (cache)", users_c.count(), "orders (cache)", orders_c.count())
print("ex_b3 PRET")
time.sleep(240)
users_c.unpersist()
orders_c.unpersist()
print("ex_b3 OK")
spark.stop()
