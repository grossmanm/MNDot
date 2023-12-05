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
import numpy as np
from scipy import stats
import datetime as dt
from datetime import timedelta

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


def main():

    # convert the date in string format to a datetime.date object
    def datestamp(string):
        string = string.split('-')
        # .isoformat(timespec='milliseconds')
        return dt.date(year=int(string[0]), month=int(string[1]), day=int(string[2]))

    # convert the datetime in string format to a datetime.datetime object
    def timestamp(string):
        string = string.split(' ')
        string0 = string[0].split('-')
        string1 = string[1].split('.')
        string2 = string1[0].split(':')
        return dt.datetime(year=int(string0[0]), month=int(string0[1]), day=int(string0[2]), hour=int(string2[0]), minute=int(string2[1]), second=int(string2[2]), microsecond=int(string1[1] + '000'))

    # fill the empty hours with 0 in a day
    def fill_ept(a_list):
        hour_idx = list(range(0, 24))
        cumu = []
        cnt = []
        avg = []
        # print(len(a_list))
        j = 0
        for i in hour_idx:
            # print(i, j, a_list[j][0])
            if j >= len(a_list):
                cumu.append(0.0)
                cnt.append(0.0)
                avg.append(0.0)
            else:
                if i == a_list[j][0]:
                    cumu.append(a_list[j][1][0])
                    cnt.append(a_list[j][1][1])
                    avg.append(a_list[j][1][2])
                    j += 1
                else:
                    cumu.append(0.0)
                    cnt.append(0.0)
                    avg.append(0.0)

        return np.array((hour_idx, cumu, cnt, avg))

    # sort the timestamp by the day of the week
    def wkday(elem_list):
        wkday_id = []
        for i in range(int((timestamp(args.enddt) - timestamp(args.startdt)) / timedelta(days=1))):
            wkday_id.append((timestamp(args.startdt) +
                            dt.timedelta(days=i)).isoweekday())
        # print(elem[0])

        def wtf(elem):
            return wkday_id.index(elem[0])

        elem_list.sort(key=wtf)
        # print(wkday_id.index(elem[0]))
        return elem_list

    # make up the empty dates, fill it out with 0s, make sure the record is complete
    def add_dates(e_list):
        if len(e_list) < int((timestamp(args.enddt) - timestamp(args.startdt))/timedelta(days=1)):
            # print(e_list)
            current_dates = sorted([i[0] for i in e_list])
            # print(current_dates)
            for i in range(int((timestamp(args.enddt) - timestamp(args.startdt))/timedelta(days=1))):
                if (timestamp(args.startdt) + dt.timedelta(days=i)).date() not in current_dates:
                    hour_idx = list(range(0, 24))
                    cumu = []
                    cnt = []
                    avg = []
                    for j in hour_idx:
                        cumu.append(0.0)
                        cnt.append(0.0)
                        avg.append(0.0)
                    e_list.append([(timestamp(args.startdt) + dt.timedelta(days=i)).date(), np.concatenate((np.full(
                        (1, 24), (timestamp(args.startdt) + dt.timedelta(days=i)).isoweekday()), np.array((hour_idx, cumu, cnt, avg))), axis=0)])
            # print(sorted(e_list))
        return sorted(e_list)

    # make up the empty days of a week
    def add_wkdays(w_list):
        if len(w_list) < 7:
            # print(e_list)
            current_wkdays = sorted([i[0] for i in w_list])
            # print(current_dates)
            for i in range(1, 8):
                if i not in current_wkdays:
                    hour_idx = list(range(0, 24))
                    cumu = []
                    cnt = []
                    avg = []
                    for j in hour_idx:
                        cumu.append(0.0)
                        cnt.append(0.0)
                        avg.append(0.0)
                    w_list.append([i, np.concatenate(
                        (np.full((1, 24), i), np.array((hour_idx, cumu, cnt, avg))), axis=0)])
            # print(sorted(e_list))
        return sorted(w_list)

    # calculate the local correlation using 12 timestamps between the corresponding factors of the selected week and other weeks
    def local_cor(arr1, arr2):
        localcor_cumu = []
        localcor_cnt = []
        localcor_avg = []
        for i in range(168):
            if i-6 < 0 and i+6 <= 168:
                # None
                localcor_cumu.append(stats.pearsonr(
                    arr1[2][:i+6], arr2[2][:i+6]).statistic)
                localcor_cnt.append(stats.pearsonr(
                    arr1[3][:i+6], arr2[3][:i+6]).statistic)
                localcor_avg.append(stats.pearsonr(
                    arr1[4][:i+6], arr2[4][:i+6]).statistic)
                # print(stats.pearsonr(arr1[2][0:i+6], arr2[2][0:i+6]).statistic)
                # print(arr1[2][:i+6])

            elif i-6 >= 0 and i+6 > 168:
                localcor_cumu.append(stats.pearsonr(
                    arr1[2][i-6:], arr2[2][i-6:]).statistic)
                localcor_cnt.append(stats.pearsonr(
                    arr1[3][i-6:], arr2[3][i-6:]).statistic)
                localcor_avg.append(stats.pearsonr(
                    arr1[4][i-6:], arr2[4][i-6:]).statistic)
                # print(arr1[2][i-6:])

            else:
                if len(arr1[2][i-6:i+6]) != len(arr2[2][i-6:i+6]):
                    print(len(arr1[2]))
                    print(len(arr2[2]))
                localcor_cumu.append(stats.pearsonr(
                    arr1[2][i-6:i+6], arr2[2][i-6:i+6]).statistic)
                localcor_cnt.append(stats.pearsonr(
                    arr1[3][i-6:i+6], arr2[3][i-6:i+6]).statistic)
                localcor_avg.append(stats.pearsonr(
                    arr1[4][i-6:i+6], arr2[4][i-6:i+6]).statistic)
                # None
                # print(arr1[2][i-6:i+6])
        date_list168 = []
        for kk in range(int((timestamp(args.enddt) - timestamp(args.startdt))/timedelta(days=1))):
            for k24 in range(24):
                date_list168.append(
                    (timestamp(args.startdt) + dt.timedelta(days=kk)).date())
        # print(date_list168)

        return np.array([date_list168, list(arr1[0]), list(arr1[1]), localcor_cumu, localcor_cnt, localcor_avg]).T
        # print(len(localcor_cumu))
        # print(len(localcor_cnt))
        # print(len(localcor_avg))
        # print(list(arr1[0]))
        # print(type(arr1[1][:, None]))
        # print(arr1.shape)
        # print(type(np.array((localcor_cumu, localcor_cnt, localcor_avg)).T))
        # print(np.array([list(arr1[0]), list(arr1[1]), localcor_cumu, localcor_cnt, localcor_avg]).shape)
        # print(np.append([np.array(list(arr1[0])), np.array(list(arr1[1])), np.array(localcor_cumu), np.array(localcor_cnt), np.array(localcor_avg)], axis=0))
        # print(np.concatenate([arr1[0][:, None], arr1[1][:, None], np.array(localcor_cumu), np.array(localcor_cnt), np.array(localcor_avg)], axis=0))
        # return np.concatenate((arr1[0], arr1[1], np.array((localcor_cumu, localcor_cnt, localcor_avg)).T), axis=0)

        # print(len(arr1[]))
        # for i in arr1:
        # stats.pearsonr(arr1[2][:], arr2[2]).statistic
        # print(stats.pearsonr(arr1[2], arr2[2]).statistic)

    def datehourstamp(string, hour):
        string = string.split('-')
        return dt.datetime(year=int(string[0]), month=int(string[1]), day=int(string[2]), hour=hour)

    def endtimestamp(string):
        string = string.split(' ')
        string0 = string[0].split('-')
        string1 = string[1].split('.')
        string2 = string1[0].split(':')
        return dt.datetime(year=int(string0[0]), month=int(string0[1]), day=int(string0[2]), hour=int(string2[0]))

    def hourdifference(item):
        return (item[0], (item[1] - item[0]) / timedelta(hours=1))

    def assign_anomalyID(datehour_list):
        startdatetime = None
        enddatetime = None
        start_end_list = []
        for item in datehour_list:
            if startdatetime is None:
                startdatetime = item[0]
            if item[1] > 1.0:
                enddatetime = item[0]
                start_end_list.append(
                    (startdatetime, enddatetime + dt.timedelta(hours=1) - dt.timedelta(seconds=1)))
                startdatetime = None
                enddatetime = None
            elif item[1] == 1.0 and item[0].hour == 23:
                enddatetime = item[0]
                start_end_list.append(
                    (startdatetime, enddatetime + dt.timedelta(hours=1) - dt.timedelta(seconds=1)))
                startdatetime = None
                enddatetime = None

        return [{'AnomalyID': i+1, 'intersection': args.intersectionID, 'StartTime': start_end_list[i][0].__str__(), 'EndTime': start_end_list[i][1].__str__()} for i in range(len(start_end_list))]

    data = sc.textFile(args.input_file) \
        .map(lambda x: json.loads(x)) \
        .map(lambda x: (x['hour'], [x['cumu_sec'], x['count'], x['avg_sec']], (x['intersection'], x['parameter'], x['phase'], x['weekday'], datestamp(x['date']))))

    # print(wkday())
    # print(int((timestamp(args.enddt) - timestamp(args.startdt))/timedelta(days=1)))
    # print(data.map(lambda x: (x[2][4], x[0])).distinct().count())

    # Ground truth (code 1-4) trend: average trend of the the four weeks before and after the selected week
    ground_truth_multiweektrend = data.filter(lambda x: x[2][2] in [81.0, 82.0]) \
        .filter(lambda x: x[2][1] in [1.0, 2.0, 3.0, 4.0]) \
        .filter(lambda x: (x[2][4] < timestamp(args.startdt).date() and x[2][4] >= (timestamp(args.startdt) - dt.timedelta(days=14)).date()) or (x[2][4] >= timestamp(args.enddt).date() and x[2][4] < (timestamp(args.enddt) + dt.timedelta(days=14)).date())) \
        .groupBy(lambda x: (x[2])) \
        .map(lambda x: ((x[0][0], x[0][1], x[0][2], x[0][3]), fill_ept(sorted(list(x[1]))))) \
        .groupBy(lambda x: x[0]) \
        .map(lambda x: (x[0], [i[1] for i in list(x[1])])) \
        .map(lambda x: ((x[0][0], x[0][1], x[0][2]), [x[0][3], np.concatenate((np.full((1, 24), x[0][3]), np.mean(np.stack(x[1]), axis=0)), axis=0)])) \
        .groupBy(lambda x: x[0]) \
        .map(lambda x: (x[0], [i[1] for i in list(x[1])])) \
        .map(lambda x: (x[0], add_wkdays(x[1]))) \
        .map(lambda x: (x[0], wkday(x[1]))) \
        .map(lambda x: (x[0], np.concatenate([i[1] for i in x[1]], axis=1))) \

    # print(ground_truth_multiweektrend.take(1))
    # print(ground_truth_multiweektrend.count())
    # print(sorted(ground_truth_multiweektrend.collect()))
    # print(len(ground_truth_multiweektrend.collect()))

    # Ground truth of the selected week's time series: the selected week
    ground_truth_selectedweek = data.filter(lambda x: x[2][2] in [81.0, 82.0]) \
        .filter(lambda x: x[2][1] in [1.0, 2.0, 3.0, 4.0]) \
        .filter(lambda x: (dt.datetime(year=x[2][4].year, month=x[2][4].month, day=x[2][4].day, hour=x[0]) >= timestamp(args.startdt)) and (dt.datetime(year=x[2][4].year, month=x[2][4].month, day=x[2][4].day, hour=x[0]) < timestamp(args.enddt))) \
        .groupBy(lambda x: (x[2])) \
        .map(lambda x: ((x[0][0], x[0][1], x[0][2], x[0][3]), (x[0][4], fill_ept(sorted(list(x[1])))))) \
        .groupBy(lambda x: x[0]) \
        .map(lambda x: (x[0], [i[1] for i in list(x[1])])) \
        .map(lambda x: ((x[0][0], x[0][1], x[0][2]), [x[1][0][0], np.concatenate((np.full((1, 24), x[0][3]), x[1][0][1]), axis=0)])) \
        .groupBy(lambda x: x[0]) \
        .map(lambda x: (x[0], sorted([i[1] for i in list(x[1])]))) \
        .map(lambda x: (x[0], add_dates(x[1]))) \
        .map(lambda x: (x[0], np.concatenate([i[1] for i in x[1]], axis=1))) \
        .join(ground_truth_multiweektrend) \
        .map(lambda x: (x[0], local_cor(x[1][0], x[1][1]))) \

    output11 = ground_truth_selectedweek.map(lambda x: [(x[0], (x[1][i][0], x[1][i][2]), (x[1][i][3], x[1][i][4], x[1][i][5], 'GroundTruth')) for i in range(x[1].shape[0])]) \
        .flatMap(lambda x: x) \
        .sortBy(lambda x: (x[0], x[1])) \
        .map(lambda x: {"type": x[2][3], "intersection": x[0][0], "parameter": x[0][1], "phase": x[0][2], "date": x[1][0].__str__(), "hour": x[1][1], "cumu_sec_pearson": x[2][0], "count_pearson": x[2][1], "avg_sec_pearson": x[2][2]}) \
        .collect()

    f = open(output_GTcorr_file, 'w')
    for i in output11:
        json.dump(i, f)
        f.write('\n')
    # print(output21.take(5))

    ground_truth_selectedweek = ground_truth_selectedweek.map(lambda x: (x[0], x[1][(x[1][:, 3] <= args.threshold) | (x[1][:, 4] <= args.threshold) | (x[1][:, 5] <= args.threshold)])) \
        .filter(lambda x: x[1].shape[0] > 0) \
        .map(lambda x: [(x[0], (x[1][i][0], x[1][i][2]), (x[1][i][3], x[1][i][4], x[1][i][5], 'GroundTruth')) for i in range(x[1].shape[0])]) \
        .flatMap(lambda x: x) \

    # print(ground_truth_selectedweek.take(5))
    # .map(lambda x: [(x[0], (x[1][i][0], x[1][i][2])) for i in range(x[1].shape[0])]) \
    # .flatMap(lambda x: x) \
    # .map(lambda x: x[1])

    output12 = ground_truth_selectedweek.sortBy(lambda x: (x[0], x[1])) \
        .map(lambda x: {"type": x[2][3], "intersection": x[0][0], "parameter": x[0][1], "phase": x[0][2], "date": x[1][0].__str__(), "hour": x[1][1], "cumu_sec_pearson": x[2][0], "count_pearson": x[2][1], "avg_sec_pearson": x[2][2]}) \
        .collect()

    # print('1111111111')

    # with open('/home/yuankun/Desktop/MnDOT/2023/10-highrez-analysis/output1.txt', 'w') as f:
    #     for j in output12:
    #         # print(str(j))
    #         f.write(str(j))

    f = open(output_GTanomalyhours_with_pearson, 'w')
    for i in output12:
        json.dump(i, f)
        f.write('\n')

    # print('2222222222')

    ground_truth_selectedweek = ground_truth_selectedweek.map(
        lambda x: x[1]).distinct()

    # .collect()
    # .map(lambda x: (x[0], x[1][(x[1][:,3] >= np.percentile(x[1], 85, axis=0)[3]) | (x[1][:,4] >= np.percentile(x[1], 85, axis=0)[4]) | (x[1][:,5] >= np.percentile(x[1], 85, axis=0)[5])])) \
    # .collect()
    # print(ground_truth_selectedweek.collect())
    # print(ground_truth_selectedweek)

    # NIT trend: average trend of the the four weeks before and after the selected week
    NIT_multiweektrend = data.filter(lambda x: x[2][2] in [81.0, 82.0]) \
        .filter(lambda x: x[2][1] not in [1.0, 2.0, 3.0, 4.0]) \
        .filter(lambda x: (x[2][4] < timestamp(args.startdt).date() and x[2][4] >= (timestamp(args.startdt) - dt.timedelta(days=14)).date()) or (x[2][4] >= timestamp(args.enddt).date() and x[2][4] < (timestamp(args.enddt) + dt.timedelta(days=14)).date())) \
        .groupBy(lambda x: (x[2])) \
        .map(lambda x: ((x[0][0], x[0][1], x[0][2], x[0][3]), fill_ept(sorted(list(x[1]))))) \
        .groupBy(lambda x: x[0]) \
        .map(lambda x: (x[0], [i[1] for i in list(x[1])])) \
        .map(lambda x: ((x[0][0], x[0][1], x[0][2]), [x[0][3], np.concatenate((np.full((1, 24), x[0][3]), np.mean(np.stack(x[1]), axis=0)), axis=0)])) \
        .groupBy(lambda x: x[0]) \
        .map(lambda x: (x[0], [i[1] for i in list(x[1])])) \
        .map(lambda x: (x[0], add_wkdays(x[1]))) \
        .map(lambda x: (x[0], wkday(x[1]))) \
        .map(lambda x: (x[0], np.concatenate([i[1] for i in x[1]], axis=1))) \

    # print(ground_truth_multiweektrend.take(1))
    # print(sorted(ground_truth_multiweektrend.collect()))
    # print(len(ground_truth_multiweektrend.collect()))

    # NIT the selected week's time series: the selected week
    NIT_selectedweek = data.filter(lambda x: x[2][2] in [81.0, 82.0]) \
        .filter(lambda x: x[2][1] not in [1.0, 2.0, 3.0, 4.0]) \
        .filter(lambda x: (dt.datetime(year=x[2][4].year, month=x[2][4].month, day=x[2][4].day, hour=x[0]) >= timestamp(args.startdt)) and (dt.datetime(year=x[2][4].year, month=x[2][4].month, day=x[2][4].day, hour=x[0]) < timestamp(args.enddt))) \
        .groupBy(lambda x: (x[2])) \
        .map(lambda x: ((x[0][0], x[0][1], x[0][2], x[0][3]), (x[0][4], fill_ept(sorted(list(x[1])))))) \
        .groupBy(lambda x: x[0]) \
        .map(lambda x: (x[0], [i[1] for i in list(x[1])])) \
        .map(lambda x: ((x[0][0], x[0][1], x[0][2]), [x[1][0][0], np.concatenate((np.full((1, 24), x[0][3]), x[1][0][1]), axis=0)])) \
        .groupBy(lambda x: x[0]) \
        .map(lambda x: (x[0], sorted([i[1] for i in list(x[1])]))) \
        .map(lambda x: (x[0], add_dates(x[1]))) \
        .map(lambda x: (x[0], np.concatenate([i[1] for i in x[1]], axis=1))) \
        .join(NIT_multiweektrend) \
        .map(lambda x: (x[0], local_cor(x[1][0], x[1][1]))) \

    output21 = NIT_selectedweek.map(lambda x: [(x[0], (x[1][i][0], x[1][i][2]), (x[1][i][3], x[1][i][4], x[1][i][5], 'NIT')) for i in range(x[1].shape[0])]) \
        .flatMap(lambda x: x) \
        .sortBy(lambda x: (x[0], x[1])) \
        .map(lambda x: {"type": x[2][3], "intersection": x[0][0], "parameter": x[0][1], "phase": x[0][2], "date": x[1][0].__str__(), "hour": x[1][1], "cumu_sec_pearson": x[2][0], "count_pearson": x[2][1], "avg_sec_pearson": x[2][2]}) \
        .collect()

    f = open(output_NITcorr_file, 'w')
    for i in output21:
        json.dump(i, f)
        f.write('\n')
    # print(output21.take(5))

    NIT_selectedweek = NIT_selectedweek.map(lambda x: (x[0], x[1][(x[1][:, 3] <= args.threshold) | (x[1][:, 4] <= args.threshold) | (x[1][:, 5] <= args.threshold)])) \
        .filter(lambda x: x[1].shape[0] > 0) \
        .map(lambda x: [(x[0], (x[1][i][0], x[1][i][2]), (x[1][i][3], x[1][i][4], x[1][i][5], 'NIT')) for i in range(x[1].shape[0])]) \
        .flatMap(lambda x: x) \

    output22 = NIT_selectedweek.sortBy(lambda x: (x[0], x[1])) \
        .map(lambda x: {"type": x[2][3], "intersection": x[0][0], "parameter": x[0][1], "phase": x[0][2], "date": x[1][0].__str__(), "hour": x[1][1], "cumu_sec_pearson": x[2][0], "count_pearson": x[2][1], "avg_sec_pearson": x[2][2]}) \
        .collect()

    # print('3333333333')
    # print(NIT_selectedweek.take(5))
    # .map(lambda x: [(x[0], (x[1][i][0], x[1][i][2])) for i in range(x[1].shape[0])]) \
    # .flatMap(lambda x: x) \
    # .map(lambda x: x[1])

    # with open('/home/yuankun/Desktop/MnDOT/2023/10-highrez-analysis/output2.txt', 'w') as f:
    #     for j in output21:
    #         # print(str(j))
    #         f.write(str(j))

    f = open(output_NITanomalyhours_with_pearson, 'w')
    for i in output22:
        json.dump(i, f)
        f.write('\n')

    NIT_selectedweek = NIT_selectedweek.map(lambda x:  (x[1], x[0][0])).distinct()
    # .collect()
    # .map(lambda x: (x[0], x[1][(x[1][:,3] >= np.percentile(x[1], 85, axis=0)[3]) | (x[1][:,4] >= np.percentile(x[1], 85, axis=0)[4]) | (x[1][:,5] >= np.percentile(x[1], 85, axis=0)[5])])) \
    # .collect()
    # print(NIT_selectedweek.collect())

    # print(ground_truth_selectedweek.count())
    # print(NIT_selectedweek.count())

    # Anomalies: the matched time points that have a pearson correlation below -0.8
    # subtraction
    anomalies_timestamps = NIT_selectedweek.subtract(ground_truth_selectedweek) \
        .sortBy(lambda x: x) \
        .map(lambda x: {"intersection": x[1], "date": str(x[0][0].year) + '-' + str(x[0][0].month) + '-' + str(x[0][0].day), "hour": x[0][1]}) \

    # print(len(anomalies_timestamps))

    # # print('44444444444')
    # print(anomalies_timestamps)

    f = open(output_file1, 'w')
    for i in anomalies_timestamps.collect():
        json.dump(i, f)
        f.write('\n')

    # with open('/home/yuankun/Desktop/MnDOT/2023/10-highrez-analysis/output.txt', 'w') as f:
    #     for j in output:
    #         f.write(str(j))

    anomalies = anomalies_timestamps.map(lambda x: (datehourstamp(x['date'], int(x['hour'])))) \
        .sortBy(lambda x: x) \


    header = anomalies.first()
    anomalies1 = anomalies.map(lambda x: endtimestamp(args.enddt) if x == header else x) \
        .sortBy(lambda x: x) \
        .collect()

    anomalies = anomalies.collect()

    f = open(output_file2, 'w')
    for i in assign_anomalyID(list(map(hourdifference, list(zip(anomalies, anomalies1))))):
        json.dump(i, f)
        f.write('\n')


if __name__ == '__main__':

    # stationID = 51
    # intermediate_file = '2023JulAugSep'

    parser = argparse.ArgumentParser(description='A1T1')
    # the startdt and enddt should be complete days with time 00:00:00.000
    # the time range should be no more than 7 complete days
    parser.add_argument('--intersectionID', type=int,
                        default=51.0, help='')
    parser.add_argument('--startdt', type=str,
                        default='2023-01-01 00:00:00.000', help='the start datetime')
    parser.add_argument('--enddt', type=str,
                        default='2023-01-08 00:00:00.000', help='the end datetime')
    parser.add_argument('--threshold', type=float,
                        default=-0.6, help='the threshold of pearson correlation to define strong negetive correlation')
    parser.add_argument('--input_file', type=str,
                        default=os.path.join(os.path.dirname(os.getcwd()),'data/signal_data_stats/output-stat-51-2022Dec2023Jan.json'), help='the input file')
    
    parser.add_argument('--output_dir', type=str, default=os.path.join(os.path.dirname(os.getcwd()), 'data/signal_data_analysis/'),help='where the output files will be written')
    parser.add_argument('--output_file_basename', type=str, default='output-detected-51-22Dec23Jan-20230101-20230108', help='the base filename for all output files')
    args = parser.parse_args()

    output_GTcorr_file = os.path.join(args.output_dir, args.output_file_basename+'-allGroundTruthPearson.json')
    output_NITcorr_file = os.path.join(args.output_dir, args.output_file_basename+'-allNITPearson.json')
    output_GTanomalyhours_with_pearson = os.path.join(args.output_dir, args.output_file_basename+'-GroundTruthanomalyhours_with_pearson.json')
    output_NITanomalyhours_with_pearson = os.path.join(args.output_dir, args.output_file_basename+'-NITanomalyhours_with_pearson.json')
    output_file1 = os.path.join(args.output_dir, args.output_file_basename+'-seperateHours.json')
    output_file2 = os.path.join(args.output_dir, args.output_file_basename+'-combinedHours.json')

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
