# member3_queries4_5.py
from pyspark.sql import SparkSession
import time

spark = SparkSession.builder.appName("WikimediaQ4Q5").getOrCreate()

raw = spark.sparkContext.textFile("pagecounts-20160101-000000_parsed.out")

def parse_line(line):
    parts = line.strip().split(" ")
    if len(parts) < 4:
        return None
    try:
        return (parts[0], parts[1], int(parts[2]), int(parts[3]))
    except ValueError:
        return None

parsed = raw.map(parse_line).filter(lambda x: x is not None)

# ════ QUERY 4 ════════════════════════════════════════════════════

# ── Map-Reduce ──
start = time.time()

top5_mr = (
    parsed
    .map(lambda x: (x[0], x[2]))
    .reduceByKey(lambda a, b: a + b)
    .takeOrdered(5, key=lambda x: -x[1])
)

mr4_time = time.time() - start

# ── Loop (aggregateByKey) ──
start = time.time()

project_hits = (
    parsed
    .map(lambda x: (x[0], x[2]))
    .aggregateByKey(
        0,
        lambda acc, v: acc + v,
        lambda a, b: a + b
    )
)
top5_lp = project_hits.takeOrdered(5, key=lambda x: -x[1])

lp4_time = time.time() - start

print("\n" + "="*70)
print("QUERY 4: Top 5 Projects by Total Page Hits")
print("="*70)
print(f"\n{'Rank':<6} {'Project':<15} {'Total Hits':>15}")
print("-"*40)
for i, (project, hits) in enumerate(top5_mr, 1):
    print(f"{i:<6} {project:<15} {hits:>15,}")
print(f"\nMap-Reduce Time: {mr4_time:.4f}s")
print(f"AggregateByKey Time: {lp4_time:.4f}s")
print(f"Speedup: {mr4_time/lp4_time:.2f}x" if lp4_time < mr4_time else f"Slowdown: {lp4_time/mr4_time:.2f}x")

# ════ QUERY 5 ════════════════════════════════════════════════════

# ── Map-Reduce ──
start = time.time()

# Map to (project, (title, hits)), then reduce to keep max hits per project
max_per_project_mr = (
    parsed
    .map(lambda x: (x[0], (x[1], x[2])))
    .reduceByKey(lambda a, b: a if a[1] >= b[1] else b)
    .collect()
)

mr5_time = time.time() - start

# ── Loop (aggregateByKey with tuple comparison) ──
start = time.time()

max_per_project_lp = (
    parsed
    .map(lambda x: (x[0], (x[1], x[2])))
    .aggregateByKey(
        ("", 0),
        lambda acc, v: v if v[1] > acc[1] else acc,
        lambda a, b: a if a[1] >= b[1] else b
    )
    .collect()
)

lp5_time = time.time() - start

print("\n" + "="*70)
print("QUERY 5: Page with Highest Hits per Project (First 10 Projects)")
print("="*70)
print(f"\n{'Project':<15} {'Page Title':<35} {'Hits':>10}")
print("-"*70)
for project, (title, hits) in sorted(max_per_project_mr[:10]):
    title_display = title[:32] + "..." if len(title) > 35 else title
    print(f"{project:<15} {title_display:<35} {hits:>10,}")
print(f"\nTotal Projects: {len(max_per_project_mr)}")
print(f"Map-Reduce Time: {mr5_time:.4f}s")
print(f"AggregateByKey Time: {lp5_time:.4f}s")
print(f"Speedup: {mr5_time/lp5_time:.2f}x" if lp5_time < mr5_time else f"Slowdown: {lp5_time/mr5_time:.2f}x")
print("="*70 + "\n")