# Print my_list in the console
import findspark
findspark.init()
import pyspark
sc = pyspark.SparkContext(appName="demoPySpark")
my_list = [1,2,3,4]
print("Input list is", my_list)

# Square all numbers in my_list
squared_list_lambda = list(map(lambda x: x**2, my_list))

# Print the result of the map function
print("The squared numbers are", squared_list_lambda)