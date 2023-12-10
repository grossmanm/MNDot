import csv
import pandas as pd
from datetime import datetime
import matplotlib.pyplot as plt
import numpy as np
import os
from collections import Counter
import warnings
import re
import argparse
warnings.filterwarnings("ignore")

parser = argparse.ArgumentParser()
parser.add_argument('--source_dir', type=str, default=os.path.join(os.path.dirname(os.getcwd()),'data/weather_data/updated_schema/'), help='Directory where raw weather data is stored')
parser.add_argument('--output_dir', type=str, default=os.path.join(os.path.dirname(os.getcwd()),'data/weather_data/means/'), help='Output directroy of weather data')

args = parser.parse_args()

dir = args.source_dir
path = args.output_dir

precip_types = {
                'Unidentified': -1,
                'None': 0,
                '-': 0,
                'noPrecipitation': 1,
                'Rain': 2,
                'rainModerate': 3,
                'rainHeavy': 4,
                'snowSlight':5,
                'snowModerate':6,
                'snowHeavy': 7,
                'other': 8,
                'frozenPrecipitationSlight':10,
                'Snow':11}
wind_direction_types = {

    '-': -1, 'ESE': 0, 'SE': 1, 'E': 2, 'SSE': 3, 'S': 4, 'SSW': 5, 'ENE': 6, 'WNW': 7, 'NW': 8, 'VRB': 9, 'NNE': 10, 'N': 11,
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
def deviation(arr, mode='std'):
    if mode == 'std':
        return round(np.std(arr),4)
    elif mode == 'freq':
        frequency_table = pd.Series(arr).value_counts().reset_index()
        frequency_table.columns = ['Category', 'Frequency']
        total = len(arr)
        percent_split = [x/total for x in frequency_table['Frequency']]
        columns = [x for x  in frequency_table['Category']]
        rates = {}
        for i in range(len(columns)):
            rates[columns[i]] = percent_split[i]
        return rates
total = len(os.listdir(dir))

c = 1
recent_temp1 = lowest_temp
recent_temp2 = lowest_temp
recent_temp3 = lowest_temp
# for filename in example:
for filename in sorted([x for x in os.listdir(dir) if 'changed' in x]):
    print(str(c) + "/" + str(total))
    print(filename + " is starting...")

    df = pd.read_csv(dir + filename)

    col = df['WET BULB TEMP'].unique()
    data = {}
    data['date'] = {}
    data['hour'] = {}
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
        idx = (date,hour)
        # if idx >= time_steps:
        #     break
        if idx not in data['date']:
            data['date'][idx] = 0
        if idx not in data['hour']:
            data['hour'][idx] = 0
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
        data['date'][idx] = date
        data['hour'][idx] = hour
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

        
    df = pd.DataFrame(columns=['date', 'hour', 'VISIBLITY_m', 'HUMIDITY_m', 'PRECIP TYPE_m',
                                'PRECIP RATE_m', 'WIND DIR_m', 'WIND SPEED_m',
                                'AIR TEMP_m', 'MIN TEMP_m', 'MAX TEMP_m', 'WET BULB TEMP_m',
                                'DEW POINT_m', 'SURFACE TEMP_m', 'SUBSURFACE TEMP_m',
                                'SURFACE STATUS_m','VISIBLITY_std', 'HUMIDITY_std', 'PRECIP TYPE_std',
                                'PRECIP RATE_std', 'WIND DIR_std', 'WIND SPEED_std',
                                'AIR TEMP_std', 'MIN TEMP_std', 'MAX TEMP_std', 'WET BULB TEMP_std',
                                'DEW POINT_std', 'SURFACE TEMP_std', 'SUBSURFACE TEMP_std',
                                'SURFACE STATUS_std'])
    keys = sorted(list(data['VISIBLITY'].keys()))
    if keys != []:
        for i in keys:
            if i in data[tempurature_name]:
                date = data['date'][i]
                hour = data['hour'][i]
                visibility_mean = aggregate(data['VISIBLITY'][i])
                humidity_mean = aggregate(data['HUMIDITY'][i])
                temp_mean = aggregate(data[tempurature_name][i])
                precip_type_mean = aggregate(data['PRECIP TYPE'][i], mode='major')
                rate_mean = aggregate(data['PRECIP RATE'][i])
                direction_mean = aggregate(data['WIND DIR'][i], mode='major')
                wind_speed_mean = aggregate(data['WIND SPEED'][i])
                min_temp_mean = aggregate(data['MIN TEMP'][i])
                max_temp_mean = aggregate(data['MAX TEMP'][i])
                wet_bulb_temp_mean = aggregate(data['WET BULB TEMP'][i])
                dew_point_mean = aggregate(data['DEW POINT'][i])
                surface_temp_mean = aggregate(data['SURFACE TEMP'][i])
                subsurface_temp_mean = aggregate(data['SUBSURFACE TEMP'][i])
                status_mean = aggregate(data['SURFACE STATUS'][i], mode='major')

                # calculate standard deviations and frequency distributions
                visibility_std = deviation(data['VISIBLITY'][i])
                humidity_std = deviation(data['HUMIDITY'][i])
                temp_std = deviation(data[tempurature_name][i])
                precip_std = deviation(data['PRECIP TYPE'][i], mode='freq')
                rate_std = deviation(data['PRECIP RATE'][i])
                direction_std = deviation(data['WIND DIR'][i], mode='freq')
                wind_speed_std = deviation(data['WIND SPEED'][i])
                min_temp_std = deviation(data['MIN TEMP'][i])
                max_temp_std = deviation(data['MAX TEMP'][i])
                wet_bulb_temp_std = deviation(data['WET BULB TEMP'][i])
                dew_point_std = deviation(data['DEW POINT'][i])
                surface_temp_std = deviation(data['SURFACE TEMP'][i])
                subsurface_temp_std = deviation(data['SUBSURFACE TEMP'][i])
                status_std = deviation(data['SURFACE STATUS'][i], mode='freq')

                row = {'date':date, 'hour': hour, 'VISIBLITY_m' : visibility_mean, 'HUMIDITY_m' : humidity_mean, 'PRECIP TYPE_m':precip_type_mean,
                    'PRECIP RATE_m' : rate_mean, 'WIND DIR_m' : direction_mean, 'WIND SPEED_m' : wind_speed_mean,
                    tempurature_name+'_m' : temp_mean, 'MIN TEMP_m' : min_temp_mean, 'MAX TEMP_m' : max_temp_mean,
                    'WET BULB TEMP_m' : wet_bulb_temp_mean, 'DEW POINT_m' : dew_point_mean,
                    'SURFACE TEMP_m' : surface_temp_mean, 'SUBSURFACE TEMP_m' : subsurface_temp_mean,
                    'SURFACE STATUS_m' : status_mean, 'VISIBLITY_std': visibility_std, 'HUMIDITY_std': humidity_std, 'PRECIP TYPE_std':precip_std,
                    'PRECIP RATE_std': rate_std, 'WIND DIR_std':direction_std, 'WIND SPEED_std': wind_speed_std,
                    tempurature_name+'_std': temp_std, 'MIN TEMP_std': min_temp_std, 'MAX TEMP_std': max_temp_std,
                    'WET BULB TEMP_std': wet_bulb_temp_std, 'DEW POINT_std': dew_point_std,
                    'SURFACE TEMP_std': surface_temp_std, 'SUBSURFACE TEMP_std': subsurface_temp_std,
                    'SURFACE STATUS_std': status_std}
            else:
                row = {'date': 0, 'hour': -1, 'VISIBLITY_m' : 0, 'HUMIDITY_m' : 0, 'PRECIP TYPE_m' : 'None',
                    'PRECIP RATE_m' : 0, 'WIND DIR_m' : 0, 'WIND SPEED_m' : wind_speed_mean,
                    tempurature_name+'_m' : lowest_temp, 'MIN TEMP_m' : lowest_temp, 'MAX TEMP_m' : lowest_temp,
                    'WET BULB TEMP_m': lowest_temp, 'DEW POINT_m': lowest_temp,
                    'SURFACE TEMP_m': lowest_temp, 'SUBSURFACE TEMP_m': lowest_temp,
                    'SURFACE STATUS_m' : 0, 'VISIBILITY_std': 0, 'HUMIDITY_std': 0, 'PRECIP TYPE_std': 'None',
                    'PRECIP RATE_std': 0, 'WIND DIR_std': 0, 'WIND SPEED_std': wind_speed_std,
                    tempurature_name+'_std' : 0, 'MIN TEMP_std': 0, 'MAX TEMP_std': 0,
                    'WET BULB TEMP_std': 0, 'DEW POINT_std': 0,
                    'SURFACE TEMP_std': 0, 'SUBSURFACE TEMP_std': 0,
                    'SURFACE STATUS_std': 0}

            df = df._append(row, ignore_index = True)

    prefix = filename.split('-')[-1].strip()[:-4]

    # if z == 0:
    #     df.to_csv(path + prefix + "_6_7_daily.csv", index=False)
    #     print("output summer for " + prefix)
    # elif z == 1:
    #     df.to_csv(path + prefix + "_12_1_daily.csv", index=False)
    #     print("output winter for " + prefix)
    df.to_csv(path + prefix + ".csv", index=False)

