import pandas as pd
from datetime import datetime
import os
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--weather_data_dir', type=str, help='path to folder containing files with weather mean and standard devation values')
parser.add_argument('--correlation_dir', type=str, help='path to folder containing weather correlation results')
parser.add_argument('--malfunction_type_file', type=str, help='path to file containing malfunction types')
parser.add_argument('--output_dir', type=str, help='Where to write the output csv')

args = parser.parse_args()

weather_mean_std_dir = args.weather_data_dir
weather_variable_folder = args.correlation_dir
occlusion_detection_file = args.malfunction_type_file
output_dir = args.output_dir

if not weather_mean_std_dir:
    weather_mean_std_dir = os.path.join(os.path.dirname(os.getcwd()), 'data/weather_data/means/')
if not weather_variable_folder:
    weather_variable_folder = os.path.join(os.path.dirname(os.getcwd()), 'data/weather_data/correlation/')
if not occlusion_detection_file:
    occlusion_detection_file = os.path.join(os.path.dirname(os.getcwd()), 'data/results/detected_malfunctions.csv')
if not output_dir:
    output_dir = os.path.join(os.path.dirname(os.getcwd()), 'data/results/')

out_file = os.path.join(output_dir, 'malfunction_db.csv')

weather_date_template = "%Y-%m-%d %H:%M:%S"

occlusion_date_template = "%Y-%m-%d"

weather_variable_files = ['local_correlation_65_81st.csv', 'local_correlation_694_eriver_nramp.csv']

malfunction_dict = {'malfunction_id':[],'camera_name':[], 'date':[], 'hour':[], 'detection_technology':[], 'malfunction_type':[], 'weather_variables':[], 'weather_variable_mean':[], 'weather_variable_std':[]}

weather_variable_dict = {}

camera_map = {'65_81st':'65_81st_Vision_Stream1', '694_eriver_nramp':'694_eriver_nramp_vision'}

weather_dict = {}
weather_mean_std_files = os.listdir(weather_mean_std_dir)
for file in weather_mean_std_files:
    weather_mean_std_df = pd.read_csv(os.path.join(weather_mean_std_dir, file))
    for i, item in weather_mean_std_df.iterrows():
        date = item['date']
        if date != '0':
            date = datetime.strptime(date, occlusion_date_template).date()
        else:
            continue
        hour = item['hour']
        item_dict = item.to_dict()
        item_dict.pop('date')
        item_dict.pop('hour')
        weather_dict[(date, hour)] = item_dict
for file in weather_variable_files:
    weather_df = pd.read_csv(os.path.join(weather_variable_folder, file))
    camera_name_f = file.replace('local_correlation_','').replace('.csv', '')
    camera_name = camera_map[camera_name_f]
    for i, item in weather_df.iterrows():
        date_time = datetime.strptime(item['Date'],weather_date_template)
        date = date_time.date()
        hour = date_time.time().hour
        causes = item[1:]
        weather_variables = []
        variable_list = causes.keys()
        for j in range(len(causes)):
            if causes[j] >= 0.8:
                weather_variables.append(variable_list[j])
        weather_variable_dict[(date,hour,camera_name)] = weather_variables
        
occlusion_df = pd.read_csv(occlusion_detection_file)
counter = 0
for i, item in occlusion_df.iterrows():
    malfunction_id = item['malfunction_id']
    camera_name = item['camera_name']
    date = item['date']
    date = datetime.strptime(date, occlusion_date_template).date()
    hour = int(item['hour'])
    occlusion = item['occlusion']
    snapshot_id = item['snapshot_file']
    if occlusion:
        malfunction_type = 'occlusion'
    else:
        malfunction_type = 'unknown'
    dict_query = (date, hour, camera_name)
    if dict_query in weather_variable_dict:
        counter+=1
        weather_variables = weather_variable_dict[dict_query]
        malfunction_dict['malfunction_id'].append(malfunction_id)
        malfunction_dict['camera_name'].append(camera_name)
        malfunction_dict['date'].append(date)
        malfunction_dict['hour'].append(hour)
        malfunction_dict['detection_technology'].append('vision')
        malfunction_dict['malfunction_type'].append(malfunction_type)
        malfunction_dict['weather_variables'].append(weather_variables)
        weather_mean_list = []
        weather_std_list = []
        if (date, hour) in weather_dict:
            weather_data = weather_dict[(date,hour)]

            for w in weather_variables:
                weather_mean_list.append(weather_data[w+'_m'])
                weather_std_list.append(weather_data[w+'_std'])
        else:
            weather_mean_list.extend([-1.0 for _ in range(len(weather_variables))])
            weather_std_list.extend([-1.0 for _ in range(len(weather_variables))])
        malfunction_dict['weather_variable_mean'].append(weather_mean_list)
        malfunction_dict['weather_variable_std'].append(weather_std_list)

malfunction_df = pd.DataFrame(malfunction_dict)
malfunction_df.to_csv(out_file)
