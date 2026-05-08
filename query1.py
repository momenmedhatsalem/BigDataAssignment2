from pyspark import SparkContext
import time

sc = SparkContext("local[*]", "WikimediaQ1")
sc.setLogLevel("ERROR")

# ── Load & parse ──────────────────────────────────────────────────────────────
raw = sc.textFile("pagecounts-20160101-000000")   # adjust path if needed

def parse(line):
    parts = line.strip().split(" ")
    if len(parts) < 4:
        return None
    try:
        return int(parts[3])          # page_size is the 4th field
    except ValueError:
        return None

sizes = raw.map(parse).filter(lambda x: x is not None).cache()

# ══════════════════════════════════════════════════════════════════════════════
# APPROACH 1 — Map-Reduce Paradigm
# ══════════════════════════════════════════════════════════════════════════════
t0 = time.time()

# Each element becomes (size, size, size, 1) → (min, max, sum, count)
def to_tuple(s):
    return (s, s, s, 1)

def combine(a, b):
    return (min(a[0], b[0]),
            max(a[1], b[1]),
            a[2] + b[2],
            a[3] + b[3])

mn, mx, total, cnt = sizes.map(to_tuple).reduce(combine)
avg_mr = total / cnt

t1 = time.time()
mr_time = t1 - t0

print("=== Map-Reduce Approach ===")
print(f"  Min  Page Size : {mn}")
print(f"  Max  Page Size : {mx}")
print(f"  Avg  Page Size : {avg_mr:.4f}")
print(f"  Time           : {mr_time:.4f}s\n")

# ══════════════════════════════════════════════════════════════════════════════
# APPROACH 2 — Spark Actions (loop / aggregate style)
# ══════════════════════════════════════════════════════════════════════════════
t2 = time.time()

mn2  = sizes.min()
mx2  = sizes.max()
avg2 = sizes.mean()

t3 = time.time()
loop_time = t3 - t2

print("=== Spark Loop / Aggregate Approach ===")
print(f"  Min  Page Size : {mn2}")
print(f"  Max  Page Size : {mx2}")
print(f"  Avg  Page Size : {avg2:.4f}")
print(f"  Time           : {loop_time:.4f}s\n")

# ══════════════════════════════════════════════════════════════════════════════
# Performance Comparison
# ══════════════════════════════════════════════════════════════════════════════
print("=== Performance Comparison ===")
print(f"  Map-Reduce time : {mr_time:.4f}s")
print(f"  Loop/Agg   time : {loop_time:.4f}s")
faster = "Map-Reduce" if mr_time < loop_time else "Loop/Aggregate"
print(f"  Faster approach : {faster}")

sc.stop()