from pyspark import SparkConf, SparkContext
import time
import re
import logging

logging.getLogger("py4j").setLevel(logging.ERROR)

if __name__ == "__main__":
    conf = SparkConf().setAppName("WikimediaQ2Q3").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    sc.setLogLevel("ERROR")

    parsed = (
        sc.textFile("pagecounts-20160101-000000_parsed.out")
            .map(lambda line: line.split(" ")) 
    )
    
    start = time.time()

    image_pages = parsed.filter(
        lambda x: x[1].lower().endswith((".jpg", ".png", ".gif"))
    )

    total_image_pages = image_pages.count()

    non_english_image_pages = image_pages.filter(
        lambda x: x[0] != "en"
    ).count()

    end = time.time()

    print("=== MAP REDUCE VERSION ===")
    print("Total image pages:", total_image_pages)
    print("Non-English image pages:", non_english_image_pages)
    print("Execution time:", end - start, "seconds")

    start = time.time()

    def count_images_partition(partition):
        total = 0
        non_english = 0
        for row in partition:
            project, title = row[0], row[1].lower()
            if title.endswith((".jpg", ".png", ".gif")):
                total += 1
                if project != "en":
                    non_english += 1
        yield (total, non_english)

    partition_counts = parsed.mapPartitions(count_images_partition)

    total_image_pages_loop, non_english_loop = partition_counts.reduce(
        lambda a, b: (a[0] + b[0], a[1] + b[1])
    )

    end = time.time()

    print("=== LOOP VERSION ===")
    print("Total image pages:", total_image_pages_loop)
    print("Non-English image pages:", non_english_loop)
    print("Execution time:", end - start, "seconds")

    start = time.time()

    terms = (
        parsed
        .flatMap(lambda x: x[1].lower().split("_"))
        .map(lambda word: re.sub(r'[^a-z0-9]', '', word))
        .filter(lambda word: word != "")
    )

    word_counts = (
        terms
        .map(lambda word: (word, 1))
        .reduceByKey(lambda a, b: a + b)
    )

    top10 = word_counts.takeOrdered(
        10,
        key=lambda x: -x[1]
    )

    end = time.time()

    print("=== MAP REDUCE TOP 10 ===")
    for word, count in top10:
        print(word, count)

    print("Execution time:", end - start, "seconds")

    start = time.time()

    def word_count_partition(partition):
        import re
        freq = {}
        for row in partition:
            title = row[1].lower()
            words = title.split("_")
            for word in words:
                word = re.sub(r'[^a-z0-9]', '', word)
                if word:
                    freq[word] = freq.get(word, 0) + 1
        for k, v in freq.items():
            yield (k, v)

    partitioned_counts = parsed.mapPartitions(word_count_partition)

    total_counts = partitioned_counts.reduceByKey(lambda a, b: a + b)

    top10_loop = total_counts.takeOrdered(10, key=lambda x: -x[1])

    end = time.time()

    print("=== LOOP-TYPE TOP 10 ===")
    for word, count in top10_loop:
        print(word, count)

    print("Execution time:", end - start, "seconds")