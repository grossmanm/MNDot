import os
import cv2
import numpy as np 
from datetime import datetime
import pandas as pd
import argparse
import glare
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

output_file = os.path.join(output_dir, 'detected_glare.csv')

glare_threshold = 0.1

malfunction_df = pd.read_csv(malfunction_file)
out_dict = {'malfunction_id':[], 'camera_name': [], 'date':[], 'hour':[], 'glare':[]}
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
        out_dict['glare'].append(False)
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
            out_dict['glare'].append(False)
            out_dict['date'].append(malfunction_date)
            out_dict['hour'].append(malfunction_hour)
            continue

        if video in read_video_files:
            for i in range(len(hours)):
                out_dict['malfunction_id'].append(malfunction_id)
                out_dict['camera_name'].append(malfunction_camera_name)
                out_dict['glare'].append(read_video_files[video])
                out_dict['date'].append(malfunction_date)
                out_dict['hour'].append(malfunction_hour)
        
        frame_idx = 5000

        frame_size = frame.shape

        split, use_tr, use_tl, use_br, use_bl = camera_to_use(camera_name=camera_name)

        i=0
        count = 0
        if not split:
            out_size = 1
        else:
            out_size = use_tr+use_tl+use_br+use_bl
        total_glares = np.zeros(out_size)
        max_glare = 0
        while ret:
            if i%frame_idx == 0:
                count +=1
                glares = []
                if split:
                    if use_tl:
                        tl_frame = frame[0:frame_size[0]//2, 0:frame_size[1]//2]
                        tl_glare = glare.detect_glare(tl_frame)
                        tl_glare_sum = (tl_glare/255>=0.85).sum()
                        tl_percent = tl_glare_sum/(tl_frame.shape[0]*tl_frame.shape[1])
                        glares.append(tl_percent)
                    if use_tr:
                        tr_frame = frame[0:frame_size[0]//2, frame_size[1]//2:]
                        tr_glare = glare.detect_glare(tr_frame)
                        tr_glare_sum = (tr_glare/255>=0.85).sum()
                        tr_percent = tr_glare_sum/(tr_frame.shape[0]*tr_frame.shape[1])
                        glares.append(tr_percent)
                    if use_bl:
                        bl_frame = frame[frame_size[0]//2:,0:frame_size[1]//2]
                        bl_glare = glare.detect_glare(bl_frame)
                        bl_glare_sum = (bl_glare/255>=0.85).sum()
                        bl_percent = bl_glare_sum/(bl_frame.shape[0]*bl_frame.shape[1])
                        glares.append(bl_percent)
                    if use_br:
                        br_frame = frame[frame_size[0]//2:,frame_size[1]//2:]
                        br_glare = glare.detect_glare(br_frame)
                        br_glare_sum = (br_glare/255>=0.85).sum()
                        br_percent = br_glare_sum/(br_frame.shape[0]*br_frame.shape[1])
                        glares.append(br_percent)
                else:
                    frame_glare = glare.detect_glare(frame)
                    glare_sum = (frame_glare/255>0.85).sum()
                    glare_percent = glare_sum/(frame.shape[0]*frame.shape[1])
                    glares.append(glare_percent)
                for j in range(len(glares)):
                    total_glares[j]+=glares[j]
                    if glares[j]>max_glare:
                        max_glare = glares[j]
            ret, frame = cap.read()
            i+=1
        glare_present = False
        for g in total_glares:
            avg_glare = g/count
            if avg_glare > glare_threshold:
                glare_present = True
        if max_glare > .5:
            glare_present = True

        for i in range(len(hours)):
            record_changed = False
            for j in range(len(out_dict['malfunction_id'])):
                if out_dict['malfunction_id'][j] == malfunction_id and out_dict['glare'][j] != glare_present:
                    out_dict['glare'][j] = True
                    record_changed = True
                elif out_dict['malfunction_id'][j] == malfunction_id:
                    record_changed = True
            if not record_changed:
                out_dict['malfunction_id'].append(malfunction_id)
                out_dict['camera_name'].append(malfunction_camera_name)
                out_dict['glare'].append(glare_present)
                out_dict['date'].append(malfunction_date)
                out_dict['hour'].append(malfunction_hour)
        read_video_files[video] = glare_present
out_df = pd.DataFrame(out_dict)
out_df.to_csv(output_file)
