import pandas as pd
from datetime import datetime
import numpy as np
import os
import re
from collections import Counter
import warnings
import argparse
warnings.filterwarnings("ignore")


parser = argparse.ArgumentParser()
parser.add_argument('--data_dir', type=str, help='location of weather data files')
parser.add_argument('--output_dir', type=str, help='where the results will be written to')

args = parser.parse_args()

dir = args.data_dir
path = args.output_dir



if not dir:
    dir = os.path.join(os.path.dirname(os.getcwd()), 'data/weather_data/updated_schema/')
if not path:
    path = os.path.join(os.path.dirname(os.getcwd()), 'data/weather_data/hourly/')

# file_name = 'sorted_ControllerLogs_Signal_1039_2022_6.csv'
# file_name = 'sorted_ControllerLogs_Signal_1039_2022_7.csv'
# file_name = 'sorted_ControllerLogs_Signal_1039_2022_12.csv'
# file_name = 'sorted_ControllerLogs_Signal_1039_2023_1.csv'

example = [['Little Canada_Aug_1_Aug_20_changed_schema.csv', 'Little Canada_Aug_21_Sep_9_changed_schema.csv']
           ]
precip_types = {
                'unidentifiedSlight': -1,
                'None': 0,
                'noPrecipitation': 1,
                'Rain': 2,
                'rainSlight': 3,
                'rainModerate': 4,
                'rainHeavy': 5,
                'snowSlight':6,
                'snowModerate':7,
                'snowHeavy': 8,
                'other': 9,
                'frozenPrecipitationSlight':10}

wind_direction_types = {

    '-': -1, '?': -1, 'ESE': 0, 'SE': 1, 'E': 2, 'SSE': 3, 'S': 4, 'SSW': 5, 'ENE': 6, 'WNW': 7, 'NW': 8, 'VRB': 9, 'NNE': 10, 'N': 11,
    'NNW': 12, 'NE': 13, 'WSW': 14, 'W': 15, 'SW': 16
}

surface_status = {
                'None': -1,
                'error': 0,
                'dry': 1,
                'wet': 2,
                'chemicallyWet': 3,
                'traceMoisture': 4,
                'iceWatch': 5,
                'iceWarning': 6,
                'snowWatch': 7,
                'other': 8,

}

format = '%m/%d/%Y %H:%M %p'
# format = "%Y-%m-%d %H:%M:%S.%f"
# format = "%m/%d/%y %H:%M"
format1 = "%m/%d/%y %H:%M"
format2 = "%m/%d/%Y %H %p"

days_span = 30
grain_span = 60*60

id2month = {}
lowest_temp = -18
tempurature_name = 'AIR TEMP'

def extract_numeric_value(string):
    pattern = r'([0-9]+(?:\.[0-9]+)?)'

    matches = re.findall(pattern, string)
    if matches:
        return float(matches[0])
    else:
        return None

def get_idx(grain_span, time, start_time):
    return int((time - start_time).total_seconds()/grain_span)

def aggregate(arr, mode='ave'):
    if mode == 'ave':
        return round(np.mean(arr), 4)
    elif mode == 'major':
        counter = Counter(arr)
        if len(counter) == 1:
            return counter.most_common(1)[0][0]
        else:
            # if counter.most_common(2)[0][0] == 1:
            return counter.most_common(2)[1][0]
            # else:
            #     return counter.most_common(2)[0][0]
# time_steps = int(12 * 24 * days_span / grain_span)
# x_axis = np.arange(0, time_steps)


total = len(os.listdir(dir))

c = 1
recent_temp1 = lowest_temp
recent_temp2 = lowest_temp
recent_temp3 = lowest_temp
#for filename in example:
for filename in sorted(os.listdir(dir)):
    print(filename)
    print(str(c) + "/" + str(total))
    print(filename[0] + " is starting...")

    df = pd.read_csv(dir + filename)
    #df1 = df1.dropna()

    #df2 = pd.read_csv(dir + filename[1])
    #df2 = df2.dropna()
    #df = pd.concat([df1, df2], ignore_index=True)

    col = df['PRECIP TYPE'].unique()
    dictionary = {}
    for x, c in enumerate(col):
        dictionary[c] = x

    data = {}
    data['VISIBLITY'] = {}
    data['HUMIDITY'] = {}
    data['PRECIP TYPE'] = {}
    data['PRECIP RATE'] = {}
    data['WIND DIR'] = {}
    data['WIND SPEED'] = {}
    data[tempurature_name] = {}
    data['MIN TEMP'] = {}
    data['MAX TEMP'] = {}
    data['WET BULB TEMP'] = {}
    data['DEW POINT'] = {}
    data['SURFACE TEMP'] = {}
    data['SUBSURFACE TEMP'] = {}
    data['SURFACE STATUS'] = {}

    for i, row in df.iterrows():
        if type(row['EVENTDATE'])==float:
            continue
        seg = row['EVENTDATE'].split(' ')
        if 'midnight' in seg[1]:
            time = seg[0][:-4] + seg[0][-2:] + ' 00:00'
        elif 'noon' in seg[1]:
            time = seg[0][:-4] + seg[0][-2:] + ' 12:00'
        else:
            time = row['EVENTDATE']
        if 'a.m.' in time:
                time = time.replace('a.m.', 'AM')
        elif 'p.m.' in time:
            time = time.replace('p.m.', 'PM')
        try:
            time = datetime.strptime(time, format)
        except:
            try:
                time = datetime.strptime(time, format1)
            except:
                time = datetime.strptime(time, format2)
        # idx = int((time - start_time).total_seconds())
        date = time.date()
        hour = time.time().hour
        idx = (date, hour)
        # if idx >= time_steps:
        #     break
        if idx not in data['VISIBLITY']:
            data['VISIBLITY'][idx] = []
        if idx not in data['HUMIDITY']:
            data['HUMIDITY'][idx] = []
        if idx not in data['PRECIP TYPE']:
            data['PRECIP TYPE'][idx] = []
        if idx not in data['PRECIP RATE']:
            data['PRECIP RATE'][idx] = []
        if idx not in data['WIND DIR']:
            data['WIND DIR'][idx] = []
        if idx not in data['WIND SPEED']:
            data['WIND SPEED'][idx] = []
        if idx not in data[tempurature_name]:
            data[tempurature_name][idx] = []
        if idx not in data['MIN TEMP']:
            data['MIN TEMP'][idx] = []
        if idx not in data['MAX TEMP']:
            data['MAX TEMP'][idx] = []
        if idx not in data['WET BULB TEMP']:
            data['WET BULB TEMP'][idx] = []
        if idx not in data['DEW POINT']:
            data['DEW POINT'][idx] = []
        if idx not in data['SURFACE TEMP']:
            data['SURFACE TEMP'][idx] = []
        if idx not in data['SUBSURFACE TEMP']:
            data['SUBSURFACE TEMP'][idx] = []
        if idx not in data['SURFACE STATUS']:
            data['SURFACE STATUS'][idx] = []



            ## handle outlier
        if row['VISIBLITY'] != 'None' and row['VISIBLITY'] != '—' and type(row['VISIBLITY']) != float:
            data['VISIBLITY'][idx].append(float(extract_numeric_value(row['VISIBLITY'])))
        else:
            data['VISIBLITY'][idx].append(0)
        if row['HUMIDITY'] != 'None' and row['HUMIDITY'] != '—' and type(row['HUMIDITY']) != float:
            data['HUMIDITY'][idx].append(float(extract_numeric_value(row['HUMIDITY'])))
        else:
            data['HUMIDITY'][idx].append(0)
        if type(row['PRECIP TYPE']) == float:
            data['PRECIP TYPE'][idx].append('None')
        else:
            data['PRECIP TYPE'][idx].append(row['PRECIP TYPE'])
        if row['PRECIP RATE'] != 'None' and row['PRECIP RATE'] != '-':
            data['PRECIP RATE'][idx].append(float(row['PRECIP RATE']))
        else:
            data['PRECIP RATE'][idx].append(0)
        data['WIND DIR'][idx].append(row['WIND DIR'])
        if row['WIND SPEED'] == '?':
            data['WIND SPEED'][idx].append(0)
        elif row['WIND SPEED'] != 'None' and row['WIND SPEED'] != '-':
            data['WIND SPEED'][idx].append(float(extract_numeric_value(row['WIND SPEED'])))
        else:
            data['WIND SPEED'][idx].append(0)
        # if row[tempurature_name] != 'None' and '-' not in row[tempurature_name]:
        if row[tempurature_name] != 'None' and row[tempurature_name] != '-':
            tempurature = float(extract_numeric_value(row[tempurature_name]))
            data[tempurature_name][idx].append(tempurature)
            recent_temp1 = tempurature
        else:
            data[tempurature_name][idx].append(recent_temp1)
        if row['MIN TEMP'] != 'None' and row['MIN TEMP'] != '-':
            tempurature = float(extract_numeric_value(row['MIN TEMP']))
            data['MIN TEMP'][idx].append(tempurature)
            recent_temp2 = tempurature
        else:
            data['MIN TEMP'][idx].append(recent_temp2)
        if row['MAX TEMP'] != 'None' and row['MAX TEMP'] != '-':
            tempurature = float(extract_numeric_value(row['MAX TEMP']))
            data['MAX TEMP'][idx].append(tempurature)
            recent_temp3 = tempurature
        else:
            data['MAX TEMP'][idx].append(recent_temp3)
        if row['WET BULB TEMP'] != 'None' and row['WET BULB TEMP'] != '-':
            tempurature = float(extract_numeric_value(row['WET BULB TEMP']))
            data['WET BULB TEMP'][idx].append(tempurature)
            recent_temp4 = tempurature
        else:
            data['WET BULB TEMP'][idx].append(recent_temp4)
        if row['DEW POINT'] != 'None' and row['DEW POINT'] != '-':
            tempurature = float(extract_numeric_value(row['DEW POINT']))
            data['DEW POINT'][idx].append(tempurature)
            recent_temp5 = tempurature
        else:
            data['DEW POINT'][idx].append(recent_temp5)
        if row['SURFACE TEMP'] != 'None' and row['SURFACE TEMP'] != '-' and type(row['SURFACE TEMP']) != float:
            tempurature = float(extract_numeric_value(row['SURFACE TEMP']))
            data['SURFACE TEMP'][idx].append(tempurature)
            recent_temp6= tempurature
        else:
            data['SURFACE TEMP'][idx].append(recent_temp6)
        if row['SUBSURFACE TEMP'] != 'None' and row['SUBSURFACE TEMP'] != '-' and type(row['SUBSURFACE TEMP']) != float:
            tempurature = float(extract_numeric_value(row['SUBSURFACE TEMP']))
            data['SUBSURFACE TEMP'][idx].append(tempurature)
            recent_temp7 = tempurature
        else:
            data['SUBSURFACE TEMP'][idx].append(recent_temp7)
        if type(row['SURFACE STATUS']) == float:
            data['SURFACE STATUS'][idx].append('None')
        else:
            data['SURFACE STATUS'][idx].append(row['SURFACE STATUS'])

    df = pd.DataFrame(columns=['VISIBLITY', 'HUMIDITY', 'PRECIP TYPE',
                                'PRECIP RATE', 'WIND DIR', 'WIND SPEED',
                                'AIR TEMP', 'MIN TEMP', 'MAX TEMP', 'WET BULB TEMP',
                                'DEW POINT', 'SURFACE TEMP', 'SUBSURFACE TEMP',
                                'SURFACE STATUS'])
    keys = sorted(list(data['VISIBLITY'].keys()))
    if len(keys) == 0:
        continue
    for i in keys:
        if i in data[tempurature_name]:
            visibility = aggregate(data['VISIBLITY'][i])
            humidity = aggregate(data['HUMIDITY'][i])
            temp = aggregate(data[tempurature_name][i])
            precip_type = aggregate(data['PRECIP TYPE'][i], mode='major')
            precip_type = precip_types[precip_type]
            rate = aggregate(data['PRECIP RATE'][i])
            direction = aggregate(data['WIND DIR'][i], mode='major')
            direction = wind_direction_types[direction]
            wind_speed = aggregate(data['WIND SPEED'][i])
            min_temp = aggregate(data['MIN TEMP'][i])
            max_temp = aggregate(data['MAX TEMP'][i])
            wet_bulb_temp = aggregate(data['WET BULB TEMP'][i])
            dew_point = aggregate(data['DEW POINT'][i])
            surface_temp = aggregate(data['SURFACE TEMP'][i])
            subsurface_temp = aggregate(data['SUBSURFACE TEMP'][i])
            status = aggregate(data['SURFACE STATUS'][i], mode='major')
            status = surface_status[status]

            row = {'VISIBLITY' : visibility, 'HUMIDITY' : humidity, 'PRECIP TYPE':precip_type,
                    'PRECIP RATE' : rate, 'WIND DIR' : direction, 'WIND SPEED' : wind_speed,
                    tempurature_name : temp, 'MIN TEMP' : min_temp, 'MAX TEMP' : max_temp,
                    'WET BULB TEMP' : wet_bulb_temp, 'DEW POINT' : dew_point,
                    'SURFACE TEMP' : surface_temp, 'SUBSURFACE TEMP' : subsurface_temp,
                    'SURFACE STATUS' : status}
        else:
            row = {'VISIBLITY' : 0, 'HUMIDITY' : 0, 'PRECIP TYPE' : 0,
                    'PRECIP RATE' : 0, 'WIND DIR' : 0, 'WIND SPEED' : wind_speed,
                    tempurature_name : lowest_temp, 'MIN TEMP' : lowest_temp, 'MAX TEMP' : lowest_temp,
                    'WET BULB TEMP': lowest_temp, 'DEW POINT': lowest_temp,
                    'SURFACE TEMP': lowest_temp, 'SUBSURFACE TEMP': lowest_temp,
                    'SURFACE STATUS' : 0}

        df = df._append(row, ignore_index = True)

    prefix = filename[:-19]
    # if z == 0:
    #     df.to_csv(path + prefix + "_6_7_daily.csv", index=False)
    #     print("output summer for " + prefix)
    # elif z == 1:
    #     df.to_csv(path + prefix + "_12_1_daily.csv", index=False)
    #     print("output winter for " + prefix)
    if grain_span == 60*60:
        df.to_csv(os.path.join(path, prefix + ".csv"), index=False)
    elif grain_span == 60*60*24:
        df.to_csv(os.path.join(path, prefix + ".csv"), index=False)


# if len(id2month[id].keys()) == 4:
#     if 6 in id2month[id] and 7 in id2month[id]:
#         summer = np.concatenate((id2month[id][6], id2month[id][7]))
#         DF_summer = pd.DataFrame(summer)
#         DF_summer.to_csv(path + str(id) + "_6_7_hourly.csv", index=False, header=False)
#         print("output summer for "+str(id))
#     if 12 in id2month[id] and 1 in id2month[id]:
#         winter = np.concatenate((id2month[id][12], id2month[id][1]))
#
#         i = -1
#         while winter[i] == 0:
#             i -= 1
#         winter = winter[:i + 1]
#         DF_winter = pd.DataFrame(winter)
#         DF_winter.to_csv(path + str(id) + "_12_1_hourly.csv", index=False, header=False)
#         print("output winter for " + str(id))
#

