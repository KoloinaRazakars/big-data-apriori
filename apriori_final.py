#!/usr/bin/env python3
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

print("="*60)
print("🚀 APRIORI FINAL - STRING ITEMS FIX")
start_total = time.time()

spark = SparkSession.builder \
    .appName("AprioriStringFix") \
    .master("local[*]") \
    .config("spark.executor.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "50") \
    .getOrCreate()

# 1. CHARGEMENT + SPLIT STRING
print("\n[1/6] Chargement + split string...")
df = spark.read.option("header", "true").csv("hdfs://localhost:9000/miniproject/dataset/processed/transactions.csv")
df.printSchema()
df.show(3, truncate=False)

# ✅ FIX: split(string) → array
transactions = df.withColumn("items_array", split(col("items"), ",")) \
                .select("InvoiceNo", "items_array") \
                .filter(size("items_array") >= 2)
total_tx = transactions.count()
min_sup = int(0.01 * total_tx)
transactions.cache()
print(f"✓ {total_tx:,} transactions | min_sup={min_sup}")

# 2. 1-ITEMSETS (MAINTENANT OK)
print("\n[2/6] 1-itemsets...")
l1 = transactions.select(explode("items_array").alias("item")) \
    .groupBy("item").count() \
    .filter(col("count") >= min_sup) \
    .orderBy(col("count").desc())
num_l1 = l1.count()
l1.cache()
print(f"✓ L1: {num_l1}")
l1.show(5)

# 3. 2-ITEMSETS (Spark SQL)
print("\n[3/6] 2-itemsets...")
transactions.createOrReplaceTempView("baskets")
l2 = spark.sql(f"""
    SELECT 
        CONCAT(item1, '|', item2) as pair,
        COUNT(*) as count
    FROM (
        SELECT explode(items_array) as item1, items_array as basket
        FROM baskets
    ) t1 
    LATERAL VIEW explode(basket) t2 as item2
    WHERE item1 < item2
    GROUP BY item1, item2
    HAVING COUNT(*) >= {min_sup}
""")
num_l2 = l2.count()
l2.cache()
print(f"✓ L2: {num_l2}")
l2.show(5)

# 4. RÈGLES
print("\n[4/6] Règles...")
rules = l2.withColumn("antecedent", split(col("pair"), '\\|')[0]) \
    .withColumn("consequent", split(col("pair"), '\\|')[1]) \
    .withColumn("support", col("count")/lit(total_tx)) \
    .withColumn("confidence", col("count")/lit(min_sup)) \
    .filter(col("confidence") >= 0.3) \
    .orderBy(col("confidence").desc())
num_rules = rules.count()
print(f"✓ {num_rules} règles")
rules.show(10, truncate=False)

# 5. SAUVEGARDE
print("\n[5/6] Sauvegarde...")
HDFS_OUT = "hdfs://localhost:9000/miniproject/output/apriori"
l1.coalesce(1).write.mode("overwrite").parquet(f"{HDFS_OUT}/l1")
l2.coalesce(1).write.mode("overwrite").parquet(f"{HDFS_OUT}/l2")
rules.coalesce(1).write.mode("overwrite").parquet(f"{HDFS_OUT}/rules")
print("✓ HDFS OK")

# 6. RÉSUMÉ
print("\n[6/6] Résumé...")
total_time = time.time() - start_total
print("\n" + "="*60)
print("✅ APRIORI TERMINÉ!")
print(f"Temps: {total_time:.1f}s ({total_tx/total_time:.0f} tx/s)")
print(f"Résultats: L1={num_l1} | L2={num_l2} | Règles={num_rules}")
print("vs FP-Growth: 39s | 2405 itemsets | 2155 règles")
print(f"Performance: Apriori {total_time/39.17:.1f}x plus lent")
print("="*60)

spark.stop()
