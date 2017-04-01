import os
from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName('p3a')
sc = SparkContext(conf=conf)

# Find local path to csv data files. (WORKS IF RUN LOCALLY - from /spark directory)
students_pr_path = 'file:///' + ''.join(os.getcwd().split('/')[:-1]) + '/hive/studentsPR.csv'

# Readable names
school_id = 2
sex = 5
student_id = 6

# Search
result = sc.textFile(students_pr_path) \
            .map(lambda x: str(x).split(',')) \
            .filter(lambda x: x[sex] == 'F' and x[school_id] == '71381') \
            .map(lambda x: x[student_id])

# Print Student ID's
for x in result.collect():
    print(x)
