import os
from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName('p3b')
sc = SparkContext(conf=conf)

# Find local path to csv data files. (WORKS IF RUN LOCALLY - from /spark directory)
escuelas_pr_path = 'file:///' + ''.join(os.getcwd().split('/')[:-1]) + '/hive/escuelasPR.csv'

# Readable names
city = 2
school_id = 3
cities = ['Ponce', 'Cabo Rojo', 'Dorado']

# Search
result = sc.textFile(escuelas_pr_path) \
            .map(lambda x: str(x).split(',')) \
            .filter(lambda x: x[city] in cities) \
            .map(lambda x: x[school_id])

# Print School ID's
for x in result.collect():
    print(x)
