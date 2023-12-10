import pandas as pd
from datetime import datetime
import json
import os
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--malfunction_dir', type=str, help='Where the malfunction files are stored')
parser.add_argument('--correlation_dir', type=str, help='Where the correlated variable files are stored')
parser.add_argument('--output_dir', type=str, help='the output directory')

args = parser.parse_args()

malfunction_dir = args.malfunction_dir
correlation_dir = args.correlation_dir
output_dir = args.output_dir

if not malfunction_dir:
    malfunction_dir = os.path.join(os.path.dirname(os.getcwd()), 'data/signal_data_analysis/')

if not correlation_dir:
    correlation_dir = os.path.join(os.path.dirname(os.getcwd()), 'data/weather_data/extracted_scores/')

if not output_dir:
    output_dir = os.path.join(os.path.dirname(os.getcwd()), 'data/malfunctions/weather_correlation/')

malfunction_files = [x for x in os.listdir(malfunction_dir) if x.startswith('output-detected') and x.endswith('seperateHours.json')]
correlation_files = os.listdir(correlation_dir)

id_to_camera = {'51':'65_81st_Vision_Stream1','1039':'694_eriver_nramp_vision','1041':'694_eriver_sramp_vision', '596':'47_85th_Iteris_Stream1','54':'65_41st_gridsmart','210':'51_crc2_iteris', 
                 '878':'s_65_viking_nuturn_gridsmart','877':'s_65_viking_suturn_gridsmart','899':'77_cliff_eramp_vision','898':'77_cliff_wramp_vision'}

for malf_file in malfunction_files:
    out_dict = {'malfunction_date':[], 'malfunction_hour':[], 'location':[], 'weather_variables':[], 'means':[], 'standard_deviations':[]}
    malfunction_location = None
    with open(os.path.join(malfunction_dir, malf_file), 'r') as f:
        malf_data = f.readlines()
        for line in malf_data:
            malfunction = json.loads(line)
            malfunction_intersection = str(int(malfunction['intersection']))
            malfunction_camera = id_to_camera[malfunction_intersection]
            if not malfunction_location:
                malfunction_location = malfunction_camera
            malfunction_date = datetime.strptime(malfunction['date'],'%Y-%m-%d').date()
            malfunction_hour = int(malfunction['hour'])
            correlation_file = None
            for cor_file in correlation_files:
                cor_df = pd.read_csv(os.path.join(correlation_dir, cor_file))
                cor_month = datetime.strptime(cor_df['date'][0],'%Y-%m-%d').month
                cor_location = cor_df['loc'][0]
                if cor_location in malfunction_camera and cor_month == malfunction_date.month:
                    correlation_file = cor_file
            cor_df = pd.read_csv(os.path.join(correlation_dir, correlation_file))
            correlation_data = None
            for i, item in cor_df.iterrows():
                cor_date = datetime.strptime(item['date'],'%Y-%m-%d').date()
                cor_hour = int(item['hour'])
                if cor_date == malfunction_date and cor_hour== malfunction_hour:
                    cor_variables = item['variables'].strip('[]').replace("'",'').split(',')
                    for j in range(len(cor_variables)):
                        cor_variables[j] = cor_variables[j].lstrip().rstrip()
                    cor_means = item['means'].strip('[]').replace(' ', '').split(',')
                    for j in range(len(cor_means)):
                        cor_means[j] = float(cor_means[j])
                    cor_stds = item['standard_devs'].strip('[]').replace(' ', '').split(',')
                    for j in range(len(cor_stds)):
                        cor_stds[j] = float(cor_stds[j])
                    correlation_data = (cor_variables, cor_means, cor_stds)
            if not correlation_data:
                correlation_data = ([],[],[])
            out_dict['malfunction_date'].append(malfunction_date)
            out_dict['malfunction_hour'].append(malfunction_hour)
            out_dict['location'].append(malfunction_camera)
            out_dict['weather_variables'].append(correlation_data[0])
            out_dict['means'].append(correlation_data[1])
            out_dict['standard_deviations'].append(correlation_data[2])
    out_df = pd.DataFrame(out_dict)
    out_df.to_csv(os.path.join(output_dir, 'malfunction_correlation_'+malfunction_location+'.csv'))
            
