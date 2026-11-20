import functools
import re
import time

print = functools.partial(print, flush=True)
exec(open("/home/onyxia/work/07.spark-dataframes/setup.py").read())

print("== EX2 UDF")
base = users_typed.select("address_line_2").cache()
base.count()
def timed(label, df, n=3):
    ts = []
    for _ in range(n):
        t = time.perf_counter()
        df.select("zip").collect()
        ts.append(time.perf_counter() - t)
    print("EX2", label, "min", round(min(ts), 3), "s | moyenne", round(sum(ts) / len(ts), 3), "s")
@F.udf("string")
def zip_udf(address_line_2):
    m = re.search(r"([A-Z]{2}) ([0-9]{5})$", address_line_2 or "")
    return m.group(2) if m else ""
timed("regexp_extract", base.select(F.regexp_extract("address_line_2", r"([A-Z]{2}) ([0-9]{5})$", 2).alias("zip")))
timed("udf python", base.select(zip_udf("address_line_2").alias("zip")))
try:
    import pandas as pd
    @F.pandas_udf("string")
    def zip_pandas(s: pd.Series) -> pd.Series:
        return s.fillna("").str.extract(r"([A-Z]{2}) ([0-9]{5})$")[1].fillna("")
    timed("pandas_udf", base.select(zip_pandas("address_line_2").alias("zip")))
except Exception as e:
    print("EX2 pandas_udf indisponible:", type(e).__name__, str(e)[:150])
print("ex_c OK")
spark.stop()
