# ReduceBykey and Collect
# One of the most popular pair RDD transformations is reduceByKey() which
# operates on key, value (k,v) pairs and merges the values for each key.
# In this exercise, you'll first create a pair RDD from a list of tuples,
#  then combine the values with the same key and finally print out the result.
#
# Remember, you already have a SparkContext sc available in your workspace.
import findspark
findspark.init()
import pyspark
sc = pyspark.SparkContext(appName="demoPySpark")

tuples = [(1,2),(3,4),(3,6),(4,5)]
# Create PairRDD Rdd with key value pairs
Rdd = sc.parallelize(tuples)

# Apply reduceByKey() operation on Rdd
Rdd_Reduced = Rdd.reduceByKey(lambda x, y: y + x)

# Iterate over the result and print the output
for num in Rdd_Reduced.collect():
  print("Key {} has {} Counts".format(num[0], num[1]))