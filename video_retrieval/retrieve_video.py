import os
import paramiko
from scp import SCPClient
from datetime import datetime
import pandas as pd
import json
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--time_ranges', type=str, help='File containing the time ranges of malfunctions, output by extract_time_ranges.py')
parser.add_argument('--source_folder', type=str, help='Drive location of videos')
parser.add_argument('--video_dir', type=str, help='Where to put the retrieved videos')
parser.add_argument('--output_dir', type=str, help='Where to write the output csv')
parser.add_argument('--permission_file', type=str, help='File containing server info')

args = parser.parse_args()

time_ranges_file = args.time_ranges
source_folder = args.source_folder
video_dir = args.video_dir
output_dir = args.output_dir
permissions = args.permission_file

if not time_ranges_file:
    time_ranges_file = os.path.join(os.path.dirname(os.getcwd()), 'data/intermediate_files/time_ranges.csv')

if not source_folder:
    source_folder = '/mnt/test_video/'

if not video_dir:
    video_dir = os.path.join(os.path.dirname(os.getcwd()), 'data/videos/')

if not output_dir:
    output_dir = os.path.join(os.path.dirname(os.getcwd()), 'data/intermediate_files/')

if not permissions:
    permissions = os.path.join(os.path.dirname(os.getcwd()), 'permissions/benjen.json')

time_ranges_df = pd.read_csv(time_ranges_file)

output_file = os.path.join(output_dir, 'video_locations.csv')
# initialize ssh
ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy( paramiko.AutoAddPolicy() )

with open(permissions) as f:
    permissions = json.load(f)

# connect to directory
ssh.connect(hostname=permissions['hostname'],username=permissions['user'],password=permissions['pwd'])

date_template_csv = '%Y-%m-%d'

out_dict = {'malfunction_id':[], 'camera_name':[], 'date':[], 'hour':[], 'files':[]}
if os.path.isfile(output_file):
    prev_out_df = pd.read_csv(output_file)
    for i, item in prev_out_df.iterrows():
        out_dict['malfunction_id'].append(item['malfunction_id'])
        out_dict['camera_name'].append(item['camera_name'])
        out_dict['date'].append(item['date'])
        out_dict['hour'].append(item['hour'])
        out_dict['files'].append(item['files'])

for i, item in time_ranges_df.iterrows():
    malfunction_id = item['malfunction_id']
    camera_name = item['camera_name']
    date = item['date']
    date = datetime.strptime(date, date_template_csv).date()
    hours = item['hours']
    stdin, stdout, stderr = ssh.exec_command(f'cd {source_folder}{camera_name}/; ls')
    
    files = stdout.readlines()
    # use if files are already locally stored
   # files = [x for x in os.listdir('/data4/malcolm/malfunction_videos/') if os.path.isfile(os.path.join('/data4/malcolm/malfunction_videos/',x))]
    files_for_anomaly = []
    hours = hours.strip('[]')
    hours = hours.split(',')
    for hour in hours:
        hour = int(float(hour))
        for file in files:
            file = file.replace('\n','')
            # remove .mp4 
            t_file = file[:-4]
            t_file = t_file.split('-')
            f_camera_name = t_file[2]
            # extract date and time
            date_template = '%Y%m%d'
            time_template = '%H%M%S'
            try:
                f_date, start_time = t_file[3].split('_')
                end_time = t_file[4]
            except:
                f_date, start_time = t_file[1].split('_')
                end_time = t_file[2]

            f_date = datetime.strptime(f_date, date_template).date()
            
            start_time = datetime.strptime(start_time, time_template).time()
            end_time = datetime.strptime(end_time, time_template).time()
            # check if the video spans multiple hours if so make sure to make a duplicate entry for both hours
            if start_time.hour == end_time.hour:
                f_hours = [start_time.hour]
            else:
                f_hours = [start_time.hour, end_time.hour]
            
            # check if the date of the file matches the requested date
            # if they match check if any of the hours match
            # if any match this is a file we should retreive
            if date == f_date and hour in f_hours and file not in files_for_anomaly and camera_name in f_camera_name:
                files_for_anomaly.append(file)
    # if we already have a malfunction id but the file list is empty
    if malfunction_id in out_dict['malfunction_id']:
        idx = out_dict['malfunction_id'].index(malfunction_id)
        if out_dict['files'][idx] == '[]':
            out_dict['files'][idx] = files_for_anomaly
    elif malfunction_id not in out_dict['malfunction_id']:
        out_dict['malfunction_id'].append(malfunction_id)
        out_dict['camera_name'].append(camera_name)
        out_dict['date'].append(date)
        out_dict['hour'].append(hours)
        out_dict['files'].append(files_for_anomaly)

    # download the files we identified
    # comment out if reading locally
    with SCPClient(ssh.get_transport()) as scp:
        for file in files_for_anomaly:
            #check if the file already has been downloaded, if it has skip
            if not os.path.isfile(os.path.join(video_dir,file)):
                print(f"Downloading file: {file}")
                scp.get(f"{source_folder}{camera_name}/{file}",video_dir)

out_df = pd.DataFrame(out_dict)
out_df.to_csv(output_file)