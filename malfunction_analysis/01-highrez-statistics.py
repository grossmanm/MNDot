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
    data = None
    for file in args.input_files:
        print(file)
        if data == None:
            data = sqlContext.read.format('com.databricks.spark.csv').options(
                header='true', inferschema='true').load(os.path.join(args.input_dir, file)).rdd.map(list)
        else:
            data2 = sqlContext.read.format('com.databricks.spark.csv').options(
                header='true', inferschema='true').load(os.path.join(args.input_dir, file)).rdd.map(list)
            data = data.union(data2)
            data2.unpersist()

    data = data.map(lambda x: [x[0], x[1], x[2], x[3]]) \
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
    parser.add_argument('--input_dir', type=str, default=os.path.join(os.path.dirname(os.getcwd()), 'data/signal_data_sorted/'), help='the directory of the input files')
   
    parser.add_argument('--output_file', type=str,
                        default=os.path.join(os.path.dirname(os.getcwd()),'data/singal_data_stats/output-stat-51-2023JulAugSep.json'), help='the output file')
    parser.add_argument('--input_files', metavar='file', type=str, nargs='+', help='input filenamepip (s)')

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
