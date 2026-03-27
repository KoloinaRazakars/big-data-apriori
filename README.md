# 🛒 Big Data Market Basket Analysis with Apriori (PySpark + HDFS)

## Project Overview
This project focuses on **market basket analysis at scale** using the **Apriori algorithm** implemented with **PySpark** and executed on **Hadoop HDFS**.
The objective is to discover:
- Frequent itemsets from transaction data
- Meaningful associations between products
- Insights into customer purchasing behavior
This project demonstrates practical experience in **distributed data processing**, **association rule mining**, and **big data analytics**.


## Objectives
- Implement Apriori in a distributed environment (Spark)
- Process large-scale transactional data from HDFS
- Extract frequent patterns efficiently
- Generate association rules for business insights
- Compare Apriori performance with FP-Growth

## Tech Stack
- **Python**
- **PySpark**
- **Hadoop HDFS**
- **Spark SQL**
- **Matplotlib (visualization)**
- **Ubuntu Linux**

##  Methodology
### 1. Data Processing
- Load transaction dataset from HDFS
- Convert string-based item lists into arrays
- Filter transactions with insufficient items

### 2. Frequent Itemset Mining
- Generate **1-itemsets (L1)** using aggregation
- Generate **2-itemsets (L2)** using Spark SQL joins
- Apply minimum support threshold

### 3. Association Rule Extraction
- Compute support and confidence
- Filter rules based on minimum confidence
- Sort rules by importance

### 4. Result Storage
- Save outputs in distributed storage (HDFS)
- Export results for analysis and visualization

## Results

The Apriori algorithm successfully generated:
- Frequent 1-itemsets
- Frequent 2-itemsets
- Association rules

Results are available in the `results/` folder:
- `l1.csv`
- `l2.csv`
- `rules.csv`

Execution logs are included for reproducibility.
## Performance Analysis

The performance of Apriori was compared to FP-Growth:

- Apriori is significantly slower due to:
  - Candidate generation
  - Multiple dataset scans

This confirms known theoretical limitations of Apriori in large-scale environments.

## Visualization

A visualization module is included to:
- Display itemset distribution
- Highlight pruning effects
- Summarize extracted patterns

Note: Some visualizations use manually recorded summary values derived from execution outputs.

## Limitations
- Limited to 1-itemsets and 2-itemsets
- Confidence calculation is simplified
- Visualization is partially manual
- Not fully optimized for large distributed clusters

## Future Improvements
- Extend to higher-order itemsets (L3+)
- Implement full confidence and lift metrics
- Automate visualization pipeline
- Optimize Spark execution
- Benchmark on larger datasets

