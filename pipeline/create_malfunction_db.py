import pandas as pd
from datetime import datetime
import os
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--correlation_dir', type=str, help='path to folder containing weather correlation results')
parser.add_argument('--malfunction_type_file', type=str, help='path to file containing malfunction types')
parser.add_argument('--output_dir', type=str, help='Where to write the output csv')

args = parser.parse_args()

weather_variable_folder = args.correlation_dir
occlusion_detection_file = args.malfunction_type_file
output_dir = args.output_dir

if not weather_variable_folder:
    weather_variable_folder = os.path.join(os.path.dirname(os.getcwd()), 'data/malfunctions/weather_correlation/')
if not occlusion_detection_file:
    occlusion_detection_file = os.path.join(os.path.dirname(os.getcwd()), 'data/results/detected_malfunctions.csv')
if not output_dir:
    output_dir = os.path.join(os.path.dirname(os.getcwd()), 'data/results/')

out_file = os.path.join(output_dir, 'malfunction_db.csv')

date_template = "%Y-%m-%d"


malfunction_dict = {'malfunction_id':[],'camera_name':[], 'date':[], 'hour':[], 'detection_technology':[], 'malfunction_type':[], 'weather_variables':[], 'weather_variable_mean':[], 'weather_variable_std':[]}

weather_variable_dict = {}

#camera_map = {'65_81st':'65_81st_Vision_Stream1', '694_eriver_nramp':'694_eriver_nramp_vision', 
#              '47_8th':'47_85th_Iteris_Stream1', '51_crc2':'51_crc2_iteris', 
#              '65_41st':'65_41st_gridsmart', '77_cliff_eramp':'77_cliff_eramp_vision', '694_eriver_sramp':'694_eriver_sramp_vision'}
weather_variable_files = os.listdir(weather_variable_folder)
for file in weather_variable_files:
    weather_df = pd.read_csv(os.path.join(weather_variable_folder, file))
    camera_name = weather_df['location'][0]
    for i, item in weather_df.iterrows():
        date_time = datetime.strptime(item['malfunction_date'],date_template)
        date = date_time.date()
        hour = int(item['malfunction_hour'])
        cor_variables = item['weather_variables'].strip('[]').replace("'",'').split(',')
        for j in range(len(cor_variables)):
            cor_variables[j] = cor_variables[j].lstrip().rstrip()
        cor_means = item['means'].strip('[]').replace(' ', '').split(',')
        if cor_means == ['']:
            cor_means = []
        for j in range(len(cor_means)):
            cor_means[j] = float(cor_means[j])
        cor_stds = item['standard_deviations'].strip('[]').replace(' ', '').split(',')
        if cor_stds == ['']:
            cor_stds = []
        for j in range(len(cor_stds)):
            cor_stds[j] = float(cor_stds[j])
        weather_variable_dict[(date,hour,camera_name)] = (cor_variables, cor_means, cor_stds)
        
occlusion_df = pd.read_csv(occlusion_detection_file)
counter = 0
for i, item in occlusion_df.iterrows():
    malfunction_id = item['malfunction_id']
    camera_name = item['camera_name']
    if 'vision' in camera_name:
        detection_tech = 'vision'
    elif 'iteris' in camera_name:
        detection_tech = 'iteris'
    date = item['date']
    date = datetime.strptime(date, date_template).date()
    hour = int(item['hour'])
    malfunction_type = item['malfunction_type']
    snapshot_id = item['snapshot_file']
    dict_query = (date, hour, camera_name)
    if dict_query in weather_variable_dict:
        counter+=1
        weather_variables, weather_var_means, weather_var_stds = weather_variable_dict[dict_query]
        malfunction_dict['malfunction_id'].append(malfunction_id)
        malfunction_dict['camera_name'].append(camera_name)
        malfunction_dict['date'].append(date)
        malfunction_dict['hour'].append(hour)
        malfunction_dict['detection_technology'].append(detection_tech)
        malfunction_dict['malfunction_type'].append(malfunction_type)
        malfunction_dict['weather_variables'].append(weather_variables)
        malfunction_dict['weather_variable_mean'].append(weather_var_means)
        malfunction_dict['weather_variable_std'].append(weather_var_stds)

malfunction_df = pd.DataFrame(malfunction_dict)
malfunction_df.to_csv(out_file)
