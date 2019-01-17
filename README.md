# Frequent-Itemsets
For NTHU - Introduction to Massive Data Analysis (2018 Fall). Implement PCY algorithm to get frequent items, frequent pairs and frequent triples, and produce association rules, using Hadoop MapReduce.

## Dataset
I use the 'groceries.csv' as my dataset. It contains 9835 baskets, each with various items.

## Usage
yarn jar FrequentItemsets-1.0-SNAPSHOT.jar org.apache.hadoop.examples.FrequentItemsets <dataset> <out-dir>

## Output file
- <out-dir>_freq: All the frequent itemsets that are above the support threshold
- <out-dir>_rule: All the association rules that are above the confidence threshold
