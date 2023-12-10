import pandas as pd
import numpy as np
from plots import *
from data_loader import *
from locorr import *
import os
import argparse
import scipy.spatial as scp
from datetime import timedelta
from datetime import datetime

parser = argparse.ArgumentParser()
parser.add_argument('--weather_data_dir', type=str, help='Location of hourly weather data')
parser.add_argument('--traffic_data_dir', type=str, help='Location of traffic data')
parser.add_argument('--output_dir', type=str, help='Where the local correlation results will be written')

args = parser.parse_args()

weather_data_dir = args.weather_data_dir
traffic_data_dir = args.traffic_data_dir
output_dir = args.output_dir

hourly = 1
if not weather_data_dir:
    weather_data_dir = os.path.join(os.path.dirname(os.getcwd()), 'data/weather_data/means/')
if not traffic_data_dir:
    traffic_data_dir = os.path.join(os.path.dirname(os.getcwd()), 'data/traffic/hourly/')
if not output_dir:
    output_dir = os.path.join(os.path.dirname(os.getcwd()), 'data/correlation_scores/')

input_size = 14

traffic_data_files = os.listdir(traffic_data_dir)



#df = pd.read_csv(weather_data_file)


weather_location_map = {'cayuga':[44.96670383773909, -93.08861734632399], 'coates':[44.66570387767664, -93.0100457485304], 
                        'delano':[45.04, -93.76], 'bethel':[45.346454, -93.237496], '35w':[44.79853511526915, -93.2902160282043],
                        'inner grove':[44.875089237096034, -93.07399238699924], 'little canada':[45.03818723828624, -93.06287892907933],
                        'maple grove':[45.095097942100466, -93.44926670394744], 'mayer':[44.90596911346018, -93.86977661071903],
                        'minnetonka':[44.93996649497237, -93.46034320452821]}
nit_location_map = {'51':[45.1150,-93.241732], '1039':[45.069585,-93.278772], '1041':[45.06892,-93.279158], '899':[44.790226,-93.21963],
                    '596':[45.125053, -93.264553], '54': [45.04274, -93.247337], '210':[45.02791,-93.167081], '878':[45.322122,-93.236216],
                    '877':[45.317065,-93.235865], '899':[44.79022,-93.21963], '898':[44.79023,-93.223347]}

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

weather_station_tree = scp.KDTree(list(weather_location_map.values()))

def analyze_data(file_name1):
    id2street = {'51': '65_81st', '210': '51_crc2', '1039': '694_eriver_sramp', '1039': '694_eriver_nramp', '1041':'694_eriver_sramp','899':'77_cliff_eramp'}

    columns = ['VISIBLITY_m', 'HUMIDITY_m', 'PRECIP RATE_m', 'WIND SPEED_m','AIR TEMP_m', 
               'MIN TEMP_m', 'MAX TEMP_m', 'WET BULB TEMP_m','DEW POINT_m', 'SURFACE TEMP_m', 'SUBSURFACE TEMP_m']

    out_columns = ['VISIBLITY', 'HUMIDITY', 'PRECIP RATE', 'WIND SPEED','AIR TEMP', 
               'MIN TEMP', 'MAX TEMP', 'WET BULB TEMP','DEW POINT', 'SURFACE TEMP', 'SUBSURFACE TEMP']
    print(file_name1)
    intersection_id = file_name1.split('_')[3]
    nearest_weather_station = weather_station_tree.query([nit_location_map[intersection_id]], k=1)[1][0]
    nearest_weather_station = list(weather_location_map.keys())[nearest_weather_station]
    # find the csv containing the data for the nearest weather station
    cut=150
    weather_data_files = os.listdir(weather_data_dir)
    weather_file = None
    for file in weather_data_files:
        if nearest_weather_station in file.lower():
            weather_file = file
    df = pd.read_csv(os.path.join(weather_data_dir, weather_file))


    local_corr_map_high = {}
    local_corr_map_low = {}
    traffic_data = pd.read_csv(os.path.join(traffic_data_dir, file_name1))
    dates_dict = {}
    start_month = datetime.strptime(traffic_data['date'][0], '%Y-%m-%d').month
    for i, item in traffic_data.iterrows():
        key = (datetime.strptime(item['date'], '%Y-%m-%d'), item['hour'])
        if key not in dates_dict:
            dates_dict[key] = item['count']
    traffic_data = traffic_data['count'].to_numpy()

    # detection2 = get_data(dir + file_name2)
    # detection3 = get_data(dir + file_name3)

    res = pd.DataFrame(columns=['date', 'hour'] + out_columns)
    for i in range(len(columns)):
    # for i in [15]:
        metric = [df['date'].to_numpy(), df['hour'].to_numpy(), df[columns[i]].to_numpy()]
        used_times = []
        traffic_counts=[]
        weather_metric=[]
        for j in range(len(metric[0])):
            key = (datetime.strptime(metric[0][j], '%Y-%m-%d'), metric[1][j])
            if key in dates_dict:    
                used_times.append(key[0]+timedelta(hours=int(key[1])))
                traffic_counts.append(dates_dict[key])
                weather_metric.append(metric[2][j])
        
        # detection1[20:20+valid_len]

        # np_data = np.array([detection1[20:20+valid_len], detection2[20:20+valid_len], detection3[20:20+valid_len],
        #                     visibility, humidity, temp, type]).transpose()

        # valid_len = min(len(detection1), len(detection2), len(detection3))
        valid_len = min(len(weather_metric), len(traffic_counts))
        np_data = np.array([traffic_counts[:valid_len], weather_metric[:valid_len]]).transpose()
        data = pd.DataFrame(np_data, columns=[id2street[intersection_id], columns[i]])

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
        start_date = min(used_times)
        delta = timedelta(hours=1)

        date = []

        record = []
        transfered_prob = NormalizeData(v5)
        for a in range(len(transfered_prob)):
            delta_time = delta * (a + input_size)
            date.append(start_date + delta_time) 
            record.append(transfered_prob[a])
            #print(str(start_date + delta_time) +
            #      ', ' + str(transfered_prob[a]))
        dates = [x.date() for x in date]
        hours = [x.hour for x in date]
        res['date'] = dates
        res['hour'] = hours 
        res[out_columns[i]] = record    

        local_corr_map_high[columns[i]] = np.mean(sorted(score0)[cut:])
        local_corr_map_low[columns[i]] = np.mean(sorted(score0)[:cut])

    print('Intersection: ' + id2street[intersection_id])
    #print('Time span: ' + time_span)
    # print(local_corr_map_high)
    # print(local_corr_map_low)
    ratio = {}
    for key in local_corr_map_low.keys():
        ratio[key] = local_corr_map_high[key] / local_corr_map_low[key]

    to_file = os.path.join(output_dir,'local_correlation_' + id2street[intersection_id] +'_'+ str(start_month)+'.csv')
    res.to_csv(to_file, index=None)

for file in traffic_data_files:
    analyze_data(file)
# analyze_data(file_name2)
# analyze_data(file_name3)
#
# analyze_data(file_name4)
# analyze_data(file_name5)
# analyze_data(file_name6)