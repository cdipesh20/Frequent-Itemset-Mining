# Frequent-Itemset-Mining
The aim of this project is to mine frequently reviewed sets of businesses (frequent itemsets) and association rules using the Savasere-Omiecinski-Navathe (SON) algorithm in combination with the A-priori algorithm.

This project has two tasks:-

__Task1:-__

In this task, 2 kinds of market-basket model are created from a small sample of the Yelp dataset.
First one creates combinations of frequent businesses given a support threshold.
Second one creates combinations of frequent users given a support threshold.

__Task2:-__

This task is similar to task1 except that mining is done for the complete Yelp dataset.

Following are the steps performed:-
1. Reading the Yelp CSV file in to RDD and then build the market-basket model.
2. Filter out qualified users who reviewed more than k businesses. (k is the filter threshold).
3. Apply the SON algorithm code to the filtered market-basket model.
