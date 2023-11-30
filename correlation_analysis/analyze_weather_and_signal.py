import pandas as pd
import numpy as np
from plots import *
from data_loader import *
from locorr import *
import os
import argparse
from datetime import timedelta

parser = argparse.ArgumentParser()
parser.add_argument('--weather_data_file', type=str, help='Location of hourly weather data file')
parser.add_argument('--traffic_data_dir', type=str, help='Location of traffic data')
parser.add_argument('--output_dir', type=str, help='Where the local correlation results will be written')

args = parser.parse_args()

weather_data_file = args.weather_data_file
traffic_data_dir = args.traffic_data_dir
output_dir = args.output_dir

hourly = 1
if not weather_data_file:
    weather_data_file = os.path.join(os.path.dirname(os.getcwd()), 'data/weather_data/hourly/Little Canada_8_9_hourly.csv')
if not traffic_data_dir:
    traffic_data_dir = os.path.join(os.path.dirname(os.getcwd()), 'data/traffic/hourly/')
if not output_dir:
    output_dir = os.path.join(os.path.dirname(os.getcwd()), 'data/results/')

input_size = 14


traffic_data_files = os.listdir(traffic_data_dir)

df = pd.read_csv(weather_data_file)

# print(df)

# visibility = df['VISIBLITY'].to_numpy()
# humidity = df['HUMIDITY'].to_numpy()
# temp = df[tempurature_name].to_numpy()
# type = df['PRECIP TYPE'].to_numpy()
#
# valid_len = len(visibility)
#
# min_visibility = min(visibility)
# # visibility = np.concatenate(([min_visibility]*6*hourly, visibility, [min_visibility]*7*hourly))
# # plot_result('visibility', visibility)
#
# min_humidity = min(humidity)
# # humidity = np.concatenate(([min_humidity]*6*hourly, humidity, [min_humidity]*7*hourly))
# # plot_result('HUMIDITY', humidity)
#
# second_min = sorted(np.unique(temp))[1]
# # temp = np.concatenate(([second_min]*6*hourly, temp, [second_min]*7*hourly))
# # plot_result(tempurature_name, temp)
#
# min_type = min(type)
# # type = np.concatenate(([min_type]*6*hourly, type, [min_type]*7*hourly))
# # plot_result('PRECIP TYPE', type)
#
# def handle_missing_data(metric, min_val):
#
#     for i in range(len(metric)):
#         if metric[i] <= min_val:
#             if i == 0:
#                 metric[i] = metric[i+1]
#             elif i == len(metric) - 1:
#                 metric[i] = metric[i-1]
#             else:
#                 metric[i] = (metric[i-1] + metric[i+1])/2
#
#     return metric
#
# visibility = handle_missing_data(visibility, min_visibility)
# humidity = handle_missing_data(humidity, min_humidity)
# temp = handle_missing_data(temp, second_min)
# type = handle_missing_data(type, min_type)
# plot_result('visibility', visibility)
# plot_result('HUMIDITY', humidity)
# plot_result(tempurature_name, temp)
# plot_result('PRECIP TYPE', type)


def analyze_data(file_name1):

    street_id = file_name1.split('_')[0]
    time_span = file_name1.split('_')[1] + '_' + file_name1.split('_')[2]

    id2street = {'51': '65_81st', '210': '51_crc2', '1039': '694_eriver_nramp', '1041':'694_eriver_nramp'}

    columns = ['VISIBLITY', 'HUMIDITY', 'PRECIP TYPE',
                                       'PRECIP RATE', 'WIND DIR', 'WIND SPEED',
                                       'AIR TEMP', 'MIN TEMP', 'MAX TEMP', 'WET BULB TEMP',
                                        'DEW POINT', 'SURFACE TEMP', 'SUBSURFACE TEMP',
                                       'SURFACE STATUS']


    local_corr_map_high = {}
    local_corr_map_low = {}
    cut = 150
    detection1 = get_data(os.path.join(traffic_data_dir, file_name1))
    # detection2 = get_data(dir + file_name2)
    # detection3 = get_data(dir + file_name3)

    res = pd.DataFrame(columns=['Date'] + columns)
    for i in range(len(columns)):
    # for i in [15]:
        metric = df[columns[i]].to_numpy()
        # detection1[20:20+valid_len]

        # np_data = np.array([detection1[20:20+valid_len], detection2[20:20+valid_len], detection3[20:20+valid_len],
        #                     visibility, humidity, temp, type]).transpose()

        # valid_len = min(len(detection1), len(detection2), len(detection3))
        valid_len = min(len(metric), len(detection1))
        np_data = np.array([detection1[:valid_len], metric[:valid_len]]).transpose()
        data = pd.DataFrame(np_data, columns=[id2street[street_id], columns[i]])

        df_max_scaled = data.copy()

        # apply normalization techniques on Columns
        for column in data.columns:
            df_max_scaled[column] = df_max_scaled[column] / df_max_scaled[column].abs().max()

        # view normalized data
        # print(df_max_scaled)
        np_data = df_max_scaled.to_numpy()
        score0 = np.asarray(localCorr(np_data[:, 0], np_data[:, 1], input_size))

        # plot_loc_corr(np_data[:,0], columns[id2street[street_id]], np_data[:,1], columns[i], score0)

        before = 0
        after = 0

        v1 = list(np_data[:,0])
        filled_value = max(v1)
        for _ in range(before):
            v1.insert(0, filled_value)
        for _ in range(after):
            v1.append(filled_value)

        # v2 = list(columns[id2street[street_id]])
        #
        # filled_value = max(v2)
        # for _ in range(before):
        #     v2.insert(0, filled_value)
        # for _ in range(after):
        #     v2.append(filled_value)

        v3 = list(np_data[:,1])

        filled_value = max(v3)
        for _ in range(before):
            v3.insert(0, filled_value)
        for _ in range(after):
            v3.append(filled_value)

        # v4 = list(columns[i])
        #
        # filled_value = max(v4)
        # for _ in range(before):
        #     v4.insert(0, filled_value)
        # for _ in range(after):
        #     v4.append(filled_value)

        v5 = list(score0)

        filled_value = max(score0)
        for _ in range(before):
            v5.insert(0, filled_value)
        for _ in range(after):
            v5.append(filled_value)


        #plot_loc_corr(v1, columns[id2street[street_id]], v3, columns[i], v5, )

        start_date = datetime(2023, 8, 1)
        delta = timedelta(hours=1)

        date = []

        record = []
        transfered_prob = NormalizeData(v5)
        for a in range(len(transfered_prob)):
            delta_time = delta * (a + input_size)
            date.append(str(start_date + delta_time) )
            record.append(transfered_prob[a])
            #print(str(start_date + delta_time) +
            #      ', ' + str(transfered_prob[a]))
        if i == 3:
            res['Date'] = date

        res[columns[i]] = record

        local_corr_map_high[columns[i]] = np.mean(sorted(score0)[cut:])
        local_corr_map_low[columns[i]] = np.mean(sorted(score0)[:cut])

    print('Intersection: ' + id2street[street_id])
    print('Time span: ' + time_span)
    # print(local_corr_map_high)
    # print(local_corr_map_low)
    ratio = {}
    for key in local_corr_map_low.keys():
        ratio[key] = local_corr_map_high[key] / local_corr_map_low[key]

    sorted_ratio = sorted(ratio.items(), key=lambda x:x[1])
    #print(sorted_ratio)
    columns_to_drop = []
    for x in range(-1, -4, -1):
        columns_to_drop.append(sorted_ratio[x][0])

    columns_to_drop.append('SURFACE STATUS')
    res.drop(columns=columns_to_drop, inplace=True)

    to_file = os.path.join(output_dir,'local_correlation_' + id2street[street_id] + '.csv')
    print(to_file)
    res.to_csv(to_file, index=None)

for file in traffic_data_files:
    analyze_data(file)
# analyze_data(file_name2)
# analyze_data(file_name3)
#
# analyze_data(file_name4)
# analyze_data(file_name5)
# analyze_data(file_name6)