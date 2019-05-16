# Map and Collect
# The main method by which you can manipulate data is PySpark is using map().
# The map() transformation takes in a function and applies it to each element in the RDD. I
# t can be used to do any number of things, from fetching the website associated
# with each URL in our collection to just squaring the numbers. In this simple exercise,
# you'll use map() transformation to cube each number of the numbRDD RDD that you created earlier


#NOTE collect() should only be used to retrieve results for small datasets. It shouldn’t be used on large datasets

import findspark
findspark.init()
import pyspark
sc = pyspark.SparkContext(appName="actionNtransformation")

my_list = [1,2,3,4]
numbRDD = sc.parallelize(my_list)
# Create map() transformation to cube numbers
cubedRDD = numbRDD.map(lambda x: x**3)

# Collect the results
numbers_all = cubedRDD.collect()

# Print the numbers from numbers_all
for numb in numbers_all:
	print(numb)
