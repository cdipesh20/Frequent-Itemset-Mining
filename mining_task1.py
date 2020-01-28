from pyspark import SparkContext
import sys
import time
import itertools
from collections import Counter


t1=time.time()
sc = SparkContext('local[*]', 'task1')
support = int(sys.argv[2])
input_file_path = sys.argv[3]
partitions = int()
csvRDD = sc.textFile(input_file_path)


def Apriori(iterator):
    L1 = Counter()

    baskets = list(iterator)
    ps = int(support / partitions)

    for each_basket in baskets:
        L1 += Counter(each_basket)

    L1 = [key for key, value in L1.items() if value >= ps]
    L1.sort()
    itemsets = [((x,),1) for x in L1]
    freq_count = len(L1)
    candidate = list(itertools.combinations(L1, 2))
    candidate_size = 2

    while(freq_count > 0):
        frequent = []
        freq_count = 0
        dict = {}
        candidate_size += 1

        for each_item in candidate:
            for each_basket in baskets:
                if(set(each_item).issubset(each_basket)):
                    dict.setdefault(each_item, 0)
                    dict[each_item] += 1

        for key, value in dict.items():
            if value >= ps:
                frequent.append(set(key))
                itemsets.append((key, 1))
                freq_count += 1

        candidate = set()
        for i in range(0, len(frequent)):
            for j in range(i, len(frequent)):
                s1 = frequent[i]
                s2 = frequent[j]
                if len(s1.intersection(s2)) == candidate_size-2:
                    un = s1.union(s2)
                    candidate.add(tuple(sorted(un)))
        candidate = sorted(candidate)

    return itemsets

def findFrequentItemsets(iterator, candidates):
    baskets = list(iterator)
    dict = {}
    for each_item in candidates:
        for each_basket in baskets:
            if(set(each_item).issubset(each_basket)):
                dict.setdefault(each_item, 0)
                dict[each_item] += 1

    return dict.items()

def getstring(itemsets):
    string1 = ""
    prev_size = 99999
    current_size = 99999
    C1 = []
    for item in itemsets:
        current_size = len(item)
        if(current_size > prev_size):
            string2 = str(C1)
            string2 = string2.replace(', (', ',(')
            string2 = string2[1:-1]
            if(current_size == 2 and len(C1) > 0):
                string2 = string2.replace(',)',')')
            string1 += string2
            string1 += '\n\n'
            C1 = []
        C1.append(item)
        prev_size = current_size

    if(len(C1)>0):
        string2 = str(C1)
        string2 = string2.replace(', (', ',(')
        string2 = string2[1:-1]
        if (current_size == 1 and len(C1) > 0):
            string2 = string2.replace(',)', ')')
        string1 += string2
        string1 += '\n\n'
    return string1

if sys.argv[1] == '1':

    #Create baskets
    case1RDD = csvRDD.map(lambda x:x.split(',')).filter(lambda x: x[0]!='user_id').groupByKey().map(lambda x: set(x[1]))
    partitions = case1RDD.getNumPartitions()

    candidate_itemsets = case1RDD.mapPartitions(Apriori).reduceByKey(lambda a,b:a).map(lambda x:x[0]).collect()

    candidate_itemsets.sort()
    candidate_itemsets.sort(key=len)

    frequent_itemsets = case1RDD.mapPartitions(lambda x:findFrequentItemsets(x,candidate_itemsets)).reduceByKey(lambda a,b: a+b).filter(lambda x:x[1] >= support).map(lambda x:x[0]).collect()

    frequent_itemsets.sort()
    frequent_itemsets.sort(key=len)

    f = open(sys.argv[4], "w")
    f.write("Candidates:\n")
    f.write(getstring(candidate_itemsets))

    f.write("Frequent Itemsets:\n")
    f.write(getstring(frequent_itemsets))
    f.close()

elif sys.argv[1] == '2':

    case2RDD = csvRDD.map(lambda x:x.split(',')).filter(lambda x: x[0]!='user_id').map(lambda x: (x[1],x[0])).groupByKey().map(lambda x: set(x[1]))


    partitions = case2RDD.getNumPartitions()


    candidate_itemsets = case2RDD.mapPartitions(Apriori).reduceByKey(lambda a, b: a).map(lambda x: x[0]).collect()

    candidate_itemsets.sort()
    candidate_itemsets.sort(key=len)

    frequent_itemsets = case2RDD.mapPartitions(lambda x: findFrequentItemsets(x, candidate_itemsets)).reduceByKey(
        lambda a, b: a + b).filter(lambda x: x[1] >= support).map(lambda x: x[0]).collect()

    frequent_itemsets.sort()
    frequent_itemsets.sort(key=len)

    f = open(sys.argv[4], "w")
    f.write("Candidates:\n")
    f.write(getstring(candidate_itemsets))

    f.write("Frequent Itemsets:\n")
    f.write(getstring(frequent_itemsets))
    f.close()

print("Duration: ", time.time() - t1)