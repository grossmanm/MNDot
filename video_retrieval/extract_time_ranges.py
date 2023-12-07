import pandas as pd
import json
import argparse
import os
from datetime import datetime


id_to_camera = {'51':'65_81st_Vision_Stream1','1039':'694_eriver_nramp_vision','1041':'694_eriver_sramp_vision', '596':'47_85th_Iteris_Stream1','54':'65_41st_gridsmart','210':'51_crc2_iteris', 
                 '878':'s_65_viking_nuturn_gridsmart','877':'s_65_viking_suturn_gridsmart','899':'77_cliff_eramp_vision','898':'77_cliff_wramp_vision'}
parser = argparse.ArgumentParser()
parser.add_argument('--detection_dir', type=str, help='Directory where the results of malfunction detection')
parser.add_argument('--output_dir', type=str, help='Location of the output csv file' )

args = parser.parse_args()

detection_dir = args.detection_dir
output_dir = args.output_dir

if not detection_dir:
    detection_dir = os.path.join(os.path.dirname(os.getcwd()), 'data/signal_data_analysis/')

if not output_dir:
    output_dir = os.path.join(os.path.dirname(os.getcwd()), 'data/intermediate_files/')

written_data = []
date_template_csv = '%Y-%m-%d'
detection_files = [file for file in os.listdir(detection_dir) if file.startswith('output-detected') and file.endswith('seperateHours.json')]
malfunction_id = 0
out_dict = {'malfunction_id':[], 'camera_name':[], 'date':[], 'hours':[]}
for file in detection_files:
    times = []
    camera_id = file.split('-')[2]
    camera_name = id_to_camera[camera_id]

    filepath = os.path.join(detection_dir, file)
    with open(filepath, 'r') as f:
        data = f.readlines()
        hours = []
        for line in data:
            anomaly = json.loads(line)
           # malfunction_id = anomaly['AnomalyID']
            
            date = datetime.strptime(anomaly['date'], date_template_csv).date()
            hour = anomaly['hour']
            hours = [hour]
            if (camera_name, date, hours) not in written_data:
               # print((camera_name, date, hour))
                out_dict['malfunction_id'].append(malfunction_id)
                out_dict['camera_name'].append(camera_name)
                out_dict['date'].append(date)
                out_dict['hours'].append(hours)
                written_data.append((camera_name, date, hours))
                malfunction_id+=1

out_df = pd.DataFrame(out_dict)
out_df.to_csv(os.path.join(output_dir, 'time_ranges.csv'))