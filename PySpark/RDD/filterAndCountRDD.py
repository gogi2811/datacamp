# The RDD transformation filter() returns a new RDD containing only the elements that satisfy a particular
# function. It is useful for filtering large datasets based on a keyword. For this exercise,
# you'll filter out lines containing keyword Spark from fileRDD RDD which consists of lines of text from the README.md file. Next,
# you'll count the total number of lines containing the keyword Spark and finally print the first 4 lines of the filtered RDD.
import findspark
findspark.init()
import pyspark
sc = pyspark.SparkContext(appName="demoPySpark")

fileRDD = sc.textFile("/Users/mindstix/PycharmProjects/DataCamp/datacamp/errorLogs.log")

# Filter the fileRDD to select lines with Spark keyword
fileRDD_filter = fileRDD.filter(lambda line: 'Spark' in line)

# How many lines are there in fileRDD?
print("The total number of lines with the keyword Spark is", fileRDD_filter.count())

# Print the first four lines of fileRDD
for line in fileRDD_filter.take(4):
  print(line)