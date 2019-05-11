import findspark
findspark.init()
import pyspark
sc = pyspark.SparkContext(appName="demoPySpark")

# Check the number of partitions in fileRDD
file_path = sc.textFile("/Users/mindstix/PycharmProjects/DataCamp/datacamp/errorLogs.log")
print("Number of partitions in fileRDD is", file_path.getNumPartitions())

# Create a fileRDD_part from file_path with 5 partitions
fileRDD_part = sc.textFile(file_path, minPartitions=5)

# Check the number of partitions in fileRDD_part
print("Number of partitions in fileRDD_part is", fileRDD_part.getNumPartitions())

#/Users/mindstix/Session/lib/python3.6/site-packages/pyspark
