import os
import psycopg2
import pandas as pd
import argparse
from datetime import datetime
import json
    
id_to_camera = {'51':'65_81st_Vision_Stream1','1039':'694_eriver_nramp_vision','1041':'694_eriver_sramp_vision', '596':'47_85th_Iteris_Stream1','54':'65_41st_gridsmart','210':'51_crc2_iteris', 
                 '878':'s_65_viking_nuturn_gridsmart','877':'s_65_viking_suturn_gridsmart','899':'77_cliff_eramp_vision','898':'77_cliff_wramp_vision'}
    
date_template = '%Y-%m-%d'

parser = argparse.ArgumentParser()
parser.add_argument('--time_ranges', type=str, help='File containing the time ranges of malfunctions, output by extract_time_ranges.py')
parser.add_argument('--output_dir', type=str, help='Where to write the output csv')
parser.add_argument('--permission_file', type=str, help='File containing database connection info')

args = parser.parse_args()

time_ranges_file = args.time_ranges
output_dir = args.output_dir
permissions = args.permission_file

if not time_ranges_file:
    time_ranges_file = os.path.join(os.path.dirname(os.getcwd()), 'data/intermediate_files/time_ranges.csv')

if not output_dir:
    output_dir = os.path.join(os.path.dirname(os.getcwd()), 'data/intermediate_files/')

if not permissions:
    permissions = os.path.join(os.path.dirname(os.getcwd()), 'permissions/sql.json')

with open(permissions) as f:
    permissions = json.load(f)

time_ranges_df = pd.read_csv(time_ranges_file)

out_dict = {'drive':[]}
for i, item in time_ranges_df.iterrows():
    camera_name = item['camera_name']
    date = item['date']


    con = psycopg2.connect(
        host=permissions['host'],
        dbname ='mndot',
        user=permissions['user'],
        password=permissions['pwd'],
        port=5432
    )

    cursor_obj = con.cursor()

    # convert date to table template
    date_obj = str(datetime.strptime(date,date_template).date())
    date_obj = date_obj.replace('-','')
    date_string = '_'+date_obj

    cursor_obj.execute(f"SELECT {date_string} FROM public.cameras where cameras.cameras= '{camera_name}'")
    drive_ids = cursor_obj.fetchall()
    # extract drive names
    if drive_ids != []:
        for drive_list in drive_ids[0]:
            if drive_list != None:
                drive_list = drive_list.split(',')
                for drive in drive_list:
                    if drive != '' and drive not in out_dict['drive']:
                        out_dict['drive'].append(drive)

out_df = pd.DataFrame(out_dict)
out_df.to_csv(os.path.join(output_dir, 'hard_drives.csv'))