import os
import cv2
import numpy as np 
from datetime import datetime
import pandas as pd
import argparse
from select_camera import camera_to_use

parser = argparse.ArgumentParser()
parser.add_argument('--malfunction_file', type=str, help='File containing malfunctions and video locations')
parser.add_argument('--video_dir', type=str, help='Location of videos to be read')
parser.add_argument('--output_dir', type=str, help='Where to write the output')

args = parser.parse_args()

malfunction_file = args.malfunction_file
video_dir = args.video_dir
output_dir = args.output_dir


if not malfunction_file:
    malfunction_file = os.path.join(os.path.dirname(os.getcwd()), 'data/intermediate_files/video_locations.csv')

if not video_dir:
    video_dir = os.path.join(os.path.dirname(os.getcwd()), 'data/videos/') 

if not output_dir:
    output_dir = os.path.join(os.path.dirname(os.getcwd()), 'data/results/')

output_file = os.path.join(output_dir, 'detected_blur.csv')

blur_threshold = 700

malfunction_df = pd.read_csv(malfunction_file)
out_dict = {'malfunction_id':[], 'camera_name': [], 'date':[], 'hour':[], 'blur':[]}
read_video_files = {}
for i, item in malfunction_df.iterrows():
    malfunction_id = item['malfunction_id']
    malfunction_camera_name = item['camera_name']
    malfunction_date = item['date']
    malfunction_hour = float(item['hour'].strip("[]").replace("'",''))
    video_list = item['files'].strip("[]'").replace("'",'').split(',')
    video_files = [os.path.join(video_dir, x) for x in video_list if os.path.isfile(os.path.join(video_dir,x)) and x.endswith('.mp4')]
    if video_files == []:
        out_dict['malfunction_id'].append(malfunction_id)
        out_dict['camera_name'].append(malfunction_camera_name)
        out_dict['date'].append(malfunction_date)
        out_dict['hour'].append(malfunction_hour)
        out_dict['blur'].append(False)
    for video in video_files:
         # extract information from file name
        filename = video.split('/')[-1]
        
        # remove .mp4 
        filename = filename[:-4]
            

        #extract camera name
        recording_info = filename.split('-')
        if filename.startswith('nit'):
            camera_name = recording_info[0][11:]
        else:
            camera_name = recording_info[2][7:]

        camera_type = None

        #extract camera type
        camera_name_lst = camera_name.split('_')
        for name_part in camera_name_lst:
            if 'movision' in name_part.lower():
                camera_type = 'movision'
            elif 'vision' in name_part.lower():
                camera_type = 'vision'
            elif 'iteris' in name_part.lower():
                camera_type = 'iteris'
            elif 'gridsmart' in name_part.lower():
                camera_type = 'gridsmart'

        if not camera_type:
            print("CANNOT EXTACT CAMERA TYPE")
            break
        

        # extract date and time
        date_template = '%Y%m%d'
        time_template = '%H%M%S'
        if filename.startswith('nit'):
            date, start_time = recording_info[1].split('_')
            end_time = recording_info[2]
        else:
            date, start_time = recording_info[3].split('_')
            end_time = recording_info[4]

        date = datetime.strptime(date, date_template).date()

        start_time = datetime.strptime(start_time, time_template).time()
        end_time = datetime.strptime(end_time, time_template).time()

        hour = end_time.hour

        if start_time.hour == end_time.hour:
            hours = [start_time.hour]
        else:
            hours = [start_time.hour, end_time.hour]
        cap = cv2.VideoCapture(video)
        
        ret, frame = cap.read()
        # double check that this isn't a mangled video file if it is don't read it
        if not ret:
            out_dict['malfunction_id'].append(malfunction_id)
            out_dict['camera_name'].append(malfunction_camera_name)
            out_dict['blur'].append(False)
            out_dict['date'].append(malfunction_date)
            out_dict['hour'].append(malfunction_hour)
            continue

        if video in read_video_files:
            for i in range(len(hours)):
                out_dict['malfunction_id'].append(malfunction_id)
                out_dict['camera_name'].append(malfunction_camera_name)
                out_dict['blur'].append(read_video_files[video])
                out_dict['date'].append(malfunction_date)
                out_dict['hour'].append(malfunction_hour)
        
        frame_idx = 100

        frame_size = frame.shape

        split, use_tr, use_tl, use_br, use_bl = camera_to_use(camera_name=camera_name)

        i=0
        count = 0
        if not split:
            out_size = 1
        else:
            out_size = use_tr+use_tl+use_br+use_bl
        total_variances = np.zeros(out_size)
        while ret:
            if i%frame_idx == 0:
                count+=1
                variances = []
                if split:
                    if use_tl:
                        tl_frame = frame[0:frame_size[0]//2, 0:frame_size[1]//2]
                        tl_gray = cv2.cvtColor(tl_frame, cv2.COLOR_BGR2GRAY)
                        tl_var = cv2.Laplacian(tl_gray, cv2.CV_64F).var()
                        variances.append(tl_var)
                    if use_tr:
                        tr_frame = frame[0:frame_size[0]//2, frame_size[1]//2:]
                        tr_gray = cv2.cvtColor(tr_frame, cv2.COLOR_BGR2GRAY)
                        tr_var = cv2.Laplacian(tr_gray, cv2.CV_64F).var()
                        variances.append(tr_var)
                    if use_bl:
                        bl_frame = frame[frame_size[0]//2:,0:frame_size[1]//2]
                        bl_gray = cv2.cvtColor(bl_frame, cv2.COLOR_BGR2GRAY)
                        bl_var = cv2.Laplacian(bl_gray, cv2.CV_64F).var()
                        variances.append(bl_var)
                    if use_br:
                        br_frame = frame[frame_size[0]//2:,frame_size[1]//2:]
                        br_gray = cv2.cvtColor(br_frame, cv2.COLOR_BGR2GRAY)
                        br_var = cv2.Laplacian(br_gray, cv2.CV_64F).var()
                        variances.append(br_var)
                else:
                    gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
                    var = cv2.Laplacian(gray, cv2.CV_64F).var()
                    variances.append(var)
                for j in range(len(variances)):
                    total_variances[j]+=variances[j]
                
            ret, frame = cap.read()
            i+=1
        blur_present = False
        for var in total_variances:
            avg_var = var/count
            if avg_var < blur_threshold:
                blur_present = True
        for i in range(len(hours)):
            record_changed = False
            for j in range(len(out_dict['malfunction_id'])):
                if out_dict['malfunction_id'][j] == malfunction_id and out_dict['blur'][j] != blur_present:
                    out_dict['blur'][j] = True
                    record_changed = True
                elif out_dict['malfunction_id'][j] == malfunction_id:
                    record_changed = True
            if not record_changed:
                out_dict['malfunction_id'].append(malfunction_id)
                out_dict['camera_name'].append(malfunction_camera_name)
                out_dict['blur'].append(blur_present)
                out_dict['date'].append(malfunction_date)
                out_dict['hour'].append(malfunction_hour)
        read_video_files[video] = blur_present
out_df = pd.DataFrame(out_dict)
out_df.to_csv(output_file)