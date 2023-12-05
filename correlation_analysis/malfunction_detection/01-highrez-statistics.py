import pyspark
import argparse
import json
from pyspark.sql import SQLContext
# import itertools
import os
import sys
import time
# import easydict
# import pandas as pd
import datetime as dt
from datetime import timedelta

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


def main():

    def timestamp(string):
        string = string.split(' ')
        string0 = string[0].split('-')
        string1 = string[1].split('.')
        string2 = string1[0].split(':')
        # print(int(string1[1] + '000'))
        # .isoformat(timespec='milliseconds')
        return dt.datetime(year=int(string0[0]), month=int(string0[1]), day=int(string0[2]), hour=int(string2[0]), minute=int(string2[1]), second=int(string2[2]), microsecond=int(string1[1] + '000'))

    data2 = sqlContext.read.format('com.databricks.spark.csv').options(
        header='true', inferschema='true').load(args.input_file2).rdd.map(list)

    data = sqlContext.read.format('com.databricks.spark.csv').options(
        header='true', inferschema='true').load(args.input_file1).rdd.map(list).union(data2)

    data2.unpersist()

    data3 = sqlContext.read.format('com.databricks.spark.csv').options(
        header='true', inferschema='true').load(args.input_file3).rdd.map(list)

    data = data.union(data3)

    data3.unpersist()

    data = data.map(lambda x: [timestamp(x[0]), x[1], x[2], x[3]]) \
        .filter(lambda x: x[2] in [81.0, 82.0]) \
        .filter(lambda x: x[0] >= timestamp(args.startdt)) \
        .filter(lambda x: x[0] <= timestamp(args.enddt)) \
        .sortBy(lambda x: x[0]).groupBy(lambda x: x[3]) \
        .map(lambda x: (x[0], list(x[1]), list(x[1])[1:] + [[timestamp(args.enddt)] + list(x[1])[-1][1:]])) \
        .map(lambda x: (list(zip(x[1], x[2])))) \
        .map(lambda x: [list(i) for i in x]) \
        .flatMap(lambda x: x) \
        .map(lambda x: ((x[0][1], x[0][3], x[0][2], x[0][0].isoweekday(), x[0][0].date(), x[0][0].hour), [(x[1][0] - x[0][0]) / timedelta(microseconds=1), 1])) \
        .reduceByKey(lambda x, y: [x[0] + y[0], x[1] + y[1]]) \
        .map(lambda x: {"intersection": x[0][0], "parameter": x[0][1], "phase": x[0][2], "weekday": x[0][3], "date": x[0][4].__str__(), "hour": x[0][5], "cumu_sec": x[1][0]/100000, "count": x[1][1], "avg_sec": x[1][0] / (x[1][1] * 100000)})

    # print(data.take(10))

    output = data.collect()
    f = open(args.output_file, 'w')
    for i in output:
        json.dump(i, f)
        f.write('\n')

    # with open('/home/yuankun/Desktop/MnDOT/2023/10-highrez-analysis/output.txt', 'w') as f:
    #     for j in output:
    #         f.write(str(j))


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='A1T1')
    parser.add_argument('--startdt', type=str,
                        default='2023-07-01 00:00:00.000', help='the start datetime')
    parser.add_argument('--enddt', type=str,
                        default='2023-09-30 23:59:59.900', help='the end datetime')
    parser.add_argument('--input_file1', type=str,
                        default='/home/yuankun/Desktop/MnDOT/python_proj_haoji/MNDot/sorted_data/sorted_ControllerLogs_Signal_51_2023_7.csv', help='the input file')
    parser.add_argument('--input_file2', type=str,
                        default='/home/yuankun/Desktop/MnDOT/python_proj_haoji/MNDot/sorted_data/sorted_ControllerLogs_Signal_51_2023_8.csv', help='the input file')
    parser.add_argument('--input_file3', type=str,
                        default='/home/yuankun/Desktop/MnDOT/python_proj_haoji/MNDot/sorted_data/sorted_ControllerLogs_Signal_51_2023_9.csv', help='the input file')
    parser.add_argument('--output_file', type=str,
                        default='/home/yuankun/Desktop/MnDOT/2023/10-highrez-analysis/output-stat-51-2023JulAugSep.json', help='the output file')

    args = parser.parse_args()

    if __name__ == '__main__':
        sc_conf = pyspark.SparkConf() \
            .setAppName('task1') \
            .setMaster('local[*]') \
            .set('spark.driver.memory', '8g') \
            .set('spark.executor.memory', '4g')

        sc = pyspark.SparkContext(conf=sc_conf)
        sc.setLogLevel("OFF")
        sqlContext = SQLContext(sc)

        main()
