# ============================================================
# Wikimedia Pageviews - Queries 4 & 5
# Each query is solved twice:
#   1. Map-Reduce paradigm (distributed, using Spark RDD operations)
#   2. Loop paradigm (plain Python, reading the file line by line)
# ============================================================

from pyspark.sql import SparkSession
import time

# --- Setup Spark ---
spark = SparkSession.builder.appName("WikimediaQ4Q5").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# --- Load the raw dataset into Spark (used by Map-Reduce approaches) ---
raw = spark.sparkContext.textFile("pagecounts-20160101-000000_parsed.out")

# --- Parse each line into (project, title, hits, size) ---
def parse_line(line):
    parts = line.strip().split(" ")
    if len(parts) < 4:
        return None
    try:
        return (parts[0], parts[1], int(parts[2]), int(parts[3]))
    except ValueError:
        return None

# --- Helper to write a list of lines to a file ---
def write_file(path, lines):
    with open(path, "w") as f:
        f.write("\n".join(lines) + "\n")

# --- Helper to iterate over raw lines from Spark (used by Loop approaches) ---
# toLocalIterator() streams lines one partition at a time to the driver,
# without using any Spark transformations
def read_file_lines():
    for line in raw.toLocalIterator():
        parts = line.strip().split(" ")
        if len(parts) < 4:
            continue
        try:
            yield (parts[0], parts[1], int(parts[2]), int(parts[3]))
        except ValueError:
            continue


# ============================================================
# QUERY 4: Top 5 projects by total page hits
# ============================================================

# ---- Approach 1: Map-Reduce --------------------------------
# Step 1 (Map):    each record -> (project, hits)
# Step 2 (Reduce): sum hits per project across all workers
# Step 3:          pick top 5 by total hits
start = time.time()

parsed = raw.map(parse_line).filter(lambda x: x is not None)

top5_mapreduce = (
    parsed
    .map(lambda x: (x[0], x[2]))               # (project, hits)
    .reduceByKey(lambda a, b: a + b)            # sum hits per project
    .takeOrdered(5, key=lambda x: -x[1])        # top 5
)

time_q4_mapreduce = time.time() - start

# ---- Approach 2: Loop --------------------------------------
# Step 1: read the file line by line using plain Python
# Step 2: iterate and accumulate hits per project in a dict
# Step 3: sort and pick top 5
start = time.time()

hits_per_project = {}
for project, title, hits, size in read_file_lines():
    hits_per_project[project] = hits_per_project.get(project, 0) + hits   # accumulate

top5_loop = sorted(hits_per_project.items(), key=lambda x: -x[1])[:5]    # top 5

time_q4_loop = time.time() - start

# ---- Write results to files --------------------------------
def format_q4(label, data, t):
    lines = [
        f"QUERY 4 ({label}): Top 5 Projects by Total Page Hits",
        "=" * 50,
        f"{'Rank':<6} {'Project':<15} {'Total Hits':>15}",
        "-" * 40,
    ]
    for rank, (project, hits) in enumerate(data, 1):
        lines.append(f"{rank:<6} {project:<15} {hits:>15,}")
    lines += ["", f"Time: {t:.4f}s"]
    return lines

write_file("query4/map_reduce.txt", format_q4("Map-Reduce", top5_mapreduce, time_q4_mapreduce))
write_file("query4/loop.txt",       format_q4("Loop",       top5_loop,      time_q4_loop))

# ---- Write comparison --------------------------------------
faster = "Map-Reduce" if time_q4_mapreduce < time_q4_loop else "Loop"
ratio  = max(time_q4_mapreduce, time_q4_loop) / min(time_q4_mapreduce, time_q4_loop)

write_file("query4/comparison.txt", [
    "QUERY 4: Performance Comparison",
    "=" * 50,
    f"Map-Reduce Time : {time_q4_mapreduce:.4f}s",
    f"Loop Time       : {time_q4_loop:.4f}s",
    "",
    f"Results match   : {top5_mapreduce == top5_loop}",
    f"Faster approach : {faster} ({ratio:.2f}x faster)",
])


# ============================================================
# QUERY 5: Per project, find the page with the highest hits
# ============================================================

# ---- Approach 1: Map-Reduce --------------------------------
# Step 1 (Map):    each record -> (project, (title, hits))
# Step 2 (Reduce): keep the (title, hits) with the higher hits
start = time.time()

parsed = raw.map(parse_line).filter(lambda x: x is not None)

top_page_mapreduce = (
    parsed
    .map(lambda x: (x[0], (x[1], x[2])))                          # (project, (title, hits))
    .reduceByKey(lambda a, b: a if a[1] >= b[1] else b)            # keep max hits
    .collect()
)

time_q5_mapreduce = time.time() - start

# ---- Approach 2: Loop --------------------------------------
# Step 1: read the file line by line using plain Python
# Step 2: iterate and keep track of the max-hit page per project
start = time.time()

max_page_per_project = {}
for project, title, hits, size in read_file_lines():
    if hits > max_page_per_project.get(project, ("", 0))[1]:      # compare hits
        max_page_per_project[project] = (title, hits)              # update if higher

top_page_loop = list(max_page_per_project.items())

time_q5_loop = time.time() - start

# ---- Write results to files --------------------------------
def format_q5(label, data, t):
    lines = [
        f"QUERY 5 ({label}): Page with Highest Hits per Project",
        "=" * 70,
        f"{'Project':<15} {'Page Title':<35} {'Hits':>10}",
        "-" * 70,
    ]
    for project, (title, hits) in sorted(data):
        title_display = title[:32] + "..." if len(title) > 35 else title
        lines.append(f"{project:<15} {title_display:<35} {hits:>10,}")
    lines += ["", f"Total Projects: {len(data)}", f"Time: {t:.4f}s"]
    return lines

write_file("query5/map_reduce.txt", format_q5("Map-Reduce", top_page_mapreduce, time_q5_mapreduce))
write_file("query5/loop.txt",       format_q5("Loop",       top_page_loop,      time_q5_loop))

# ---- Write comparison --------------------------------------
mr_dict = dict(top_page_mapreduce)
lp_dict = dict(top_page_loop)
mismatches = [(p, mr_dict[p], lp_dict[p]) for p in mr_dict if mr_dict.get(p) != lp_dict.get(p)]

faster = "Map-Reduce" if time_q5_mapreduce < time_q5_loop else "Loop"
ratio  = max(time_q5_mapreduce, time_q5_loop) / min(time_q5_mapreduce, time_q5_loop)

comparison_lines = [
    "QUERY 5: Performance Comparison",
    "=" * 50,
    f"Map-Reduce Time : {time_q5_mapreduce:.4f}s",
    f"Loop Time       : {time_q5_loop:.4f}s",
    "",
    f"Results match   : {len(mismatches) == 0}",
    f"Faster approach : {faster} ({ratio:.2f}x faster)",
]
if mismatches:
    comparison_lines += ["", "Mismatches (project | map-reduce | loop):"]
    for p, mr_val, lp_val in mismatches:
        comparison_lines.append(f"  {p}: {mr_val}  vs  {lp_val}")

write_file("query5/comparison.txt", comparison_lines)

# ---- Summary -----------------------------------------------
print("\nDone! Results written to query4/ and query5/")
print(f"\nQuery 4 — Map-Reduce: {time_q4_mapreduce:.4f}s | Loop: {time_q4_loop:.4f}s")
print(f"Query 5 — Map-Reduce: {time_q5_mapreduce:.4f}s | Loop: {time_q5_loop:.4f}s")
