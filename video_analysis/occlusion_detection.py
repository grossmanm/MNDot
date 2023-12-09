import os
import cv2
import numpy as np
from datetime import datetime
import pandas as pd
import argparse
from moviepy.editor import VideoFileClip
from select_camera import camera_to_use

# params for ShiTomasi corner detection
feature_params = dict( maxCorners = 100,
    qualityLevel = 0.05,
    minDistance = 20,
    blockSize = 5 )
# Parameters for lucas kanade optical flow
lk_params = dict( winSize = (20, 20),
    maxLevel = 3,
    criteria = (cv2.TERM_CRITERIA_EPS | cv2.TERM_CRITERIA_COUNT, 10, 0.03))


top_bound = 20
bottom_bound = 125

occlusion_max = 350

min_video_dur = 30

parser = argparse.ArgumentParser()
parser.add_argument('--malfunction_file', type=str, help='File containing malfunctions and video locations')
parser.add_argument('--snapshot_dir', type=str, help='Directory where snapshot files will be saved')
parser.add_argument('--video_dir', type=str, help='Location of videos to be read')
parser.add_argument('--output_dir', type=str, help='Where to write the output')


args = parser.parse_args()

malfunction_file = args.malfunction_file
snapshot_file_dir = args.snapshot_dir
video_dir = args.video_dir
output_dir = args.output_dir

if not malfunction_file:
    malfunction_file = os.path.join(os.path.dirname(os.getcwd()), 'data/intermediate_files/video_locations.csv')

if not snapshot_file_dir:
    snapshot_file_dir = os.path.join(os.path.dirname(os.getcwd()), 'data/snapshots/')

if not video_dir:
    video_dir = os.path.join(os.path.dirname(os.getcwd()), 'data/videos/') 

if not output_dir:
    output_dir = os.path.join(os.path.dirname(os.getcwd()), 'data/results/')

output_file = os.path.join(output_dir, 'detected_occlusion.csv')


weekday_threshold_map = [0.0065, 0.0044, 0.0038, 0.0048, 0.0103, 0.0271, 
                         0.0487, 0.0659, 0.0609, 0.0539, 0.0523, 0.0553, 
                         0.0584, 0.0607, 0.0684, 0.0753, 0.0784, 0.0728, 
                         0.0562, 0.0413, 0.0336, 0.0270, 0.0199, 0.0126]
weekend_threshold_map = [0.0132, 0.0084, 0.0065, 0.0052, 0.0059, 0.0099,
                         0.0168, 0.0247, 0.0369, 0.0522, 0.0660, 0.0739,
                         0.0779, 0.0777, 0.0778, 0.0772, 0.0750, 0.0691,
                         0.0602, 0.0492, 0.0399, 0.0329, 0.0254, 0.0169]
    
malfunction_df = pd.read_csv(malfunction_file)
out_dict = {'malfunction_id':[], 'camera_name': [], 'date':[], 'hour':[], 'occlusion':[], 'snapshot_file':[]}
read_video_files = {}
for i, item in malfunction_df.iterrows():
    snapshot_file = None
    snapshot = None
    malfunction_id = item['malfunction_id']
    malfunction_camera_name = item['camera_name']
    malfunction_date = item['date']
    malfunction_hour = float(item['hour'].strip("[]").replace("'",''))
    video_list = item['files'].strip("[]'").replace("'",'').split(',')
    video_files = [os.path.join(video_dir, x) for x in video_list if os.path.isfile(os.path.join(video_dir,x)) and x.endswith('.mp4')]
    #video_files = [os.path.join(video_dir, x) for x in os.listdir(video_dir) if os.path.isfile(os.path.join(video_dir, x)) and x.endswith('.mp4')]
    #video_files = ['/home/malcolm/MNDot/video_data/blur/cs-u-benjen_65_81st_Vision_Stream1-20230103_125943-135945.mp4']
    if video_files == []:
        out_dict['malfunction_id'].append(malfunction_id)
        out_dict['camera_name'].append(malfunction_camera_name)
        out_dict['date'].append(malfunction_date)
        out_dict['hour'].append(malfunction_hour)
        out_dict['occlusion'].append(False)
        out_dict['snapshot_file'].append(snapshot_file)
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
        if date.weekday() < 5:
            threshold_map = weekday_threshold_map
        else:
            threshold_map = weekend_threshold_map

        occlusion_threshold = occlusion_max*threshold_map[hour]
        # check if the video spans multiple hours if so make sure to make a duplicate entry for both hours
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
            out_dict['occlusion'].append(False)
            out_dict['date'].append(malfunction_date)
            out_dict['hour'].append(malfunction_hour)
            out_dict['snapshot_file'].append(snapshot_file)
            continue
        video_clip = VideoFileClip(video)
        if video_clip.duration < min_video_dur:
            out_dict['malfunction_id'].append(malfunction_id)
            out_dict['camera_name'].append(malfunction_camera_name)
            out_dict['occlusion'].append(False)
            out_dict['date'].append(malfunction_date)
            out_dict['hour'].append(malfunction_hour)
            out_dict['snapshot_file'].append(snapshot_file)
            continue

        # if we've already encountered this video just grab the results for the file and record it
        snapshot_file = f'snapshot_{malfunction_id}.png'
        if video in read_video_files:
            for i in range(len(hours)):
                out_dict['malfunction_id'].append(malfunction_id)
                out_dict['camera_name'].append(malfunction_camera_name)
                out_dict['occlusion'].append(read_video_files[video][0])
                out_dict['date'].append(malfunction_date)
                out_dict['hour'].append(malfunction_hour)
                out_dict['snapshot_file'].append(snapshot_file)
                cv2.imwrite(os.path.join(snapshot_file_dir,snapshot_file), read_video_files[video][1])
        # change frames to index by depending on camera_type
        frame_idx = 15
        if camera_type == 'iteris':
            frame_idx = 30
        elif camera_type == 'vision':
            frame_idx = 15

        frame_size = frame.shape

        split, use_tr, use_tl, use_br, use_bl = camera_to_use(camera_name=camera_name)


        frames = []
        old_grays = []
        if split:
            if use_tl:
                tl_frame = frame[0:frame_size[0]//2, 0:frame_size[1]//2]
                old_tl_gray = cv2.cvtColor(tl_frame, cv2.COLOR_BGR2GRAY)
                frames.append(tl_frame)
                old_grays.append(old_tl_gray)
            if use_tr:
                tr_frame = frame[0:frame_size[0]//2, frame_size[1]//2:]
                old_tr_gray = cv2.cvtColor(tr_frame, cv2.COLOR_BGR2GRAY)
                frames.append(tr_frame)
                old_grays.append(old_tr_gray)
                
            if use_bl:
                bl_frame = frame[frame_size[0]//2:,0:frame_size[1]//2]
                old_bl_gray = cv2.cvtColor(bl_frame, cv2.COLOR_BGR2GRAY)
                frames.append(bl_frame)
                old_grays.append(old_bl_gray)
            if use_br:
                br_frame = frame[frame_size[0]//2:,frame_size[1]//2:]
                old_br_gray = cv2.cvtColor(br_frame, cv2.COLOR_BGR2GRAY)
                frames.append(br_frame)
                old_grays.append(old_br_gray)    
        else:
            old_gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
            frames.append(frame)
            old_grays.append(old_gray)


        # create smaller mask for key point identification
        
        feature_mask = np.ones((frames[0].shape[0], frames[0].shape[1]), dtype=np.uint8)
        for i in range(len(feature_mask)):
            for j in range(len(feature_mask[i])):
                if ( i<=top_bound or i>=frames[0].shape[0]-bottom_bound):
                    feature_mask[i][j] = 0

        zero_points = []
        for old_gray in old_grays:
            p0 = cv2.goodFeaturesToTrack(old_gray, mask = feature_mask, **feature_params)
            zero_points.append(p0)

        avg_mag_lists = [[] for i in range(len(frames))]


        # Create some random colors and a mask
        mask = np.zeros_like(frames[0])
        color = np.random.randint(0, 255, (100, 3))

        
        f_num = 0
        while ret:
            if f_num % frame_idx == 0:
                snapshot = frame
                frames = []
                grays = []
                if split:
                    if use_tl:
                        tl_frame = frame[0:frame_size[0]//2, 0:frame_size[1]//2]
                        tl_gray = cv2.cvtColor(tl_frame, cv2.COLOR_BGR2GRAY)
                        frames.append(tl_frame)
                        grays.append(tl_gray)
                    if use_tr:      
                        tr_frame = frame[0:frame_size[0]//2, frame_size[1]//2:]
                        tr_gray = cv2.cvtColor(tr_frame, cv2.COLOR_BGR2GRAY)
                        frames.append(tr_frame)
                        grays.append(tr_gray)
                    if use_bl:
                        bl_frame = frame[frame_size[0]//2:,0:frame_size[1]//2]
                        bl_gray = cv2.cvtColor(bl_frame, cv2.COLOR_BGR2GRAY)
                        frames.append(bl_frame)
                        grays.append(bl_gray)
                    if use_br:
                        br_frame = frame[frame_size[0]//2:,frame_size[1]//2:]
                        br_gray = cv2.cvtColor(br_frame, cv2.COLOR_BGR2GRAY)
                        frames.append(br_frame)
                        grays.append(br_gray)
                
                tracked_points = []
                status = []
                for i in range(len(grays)):
                    if not type(zero_points[i]) == type(np.array([1])):    
                        tracked_points.append(None)
                        status.append(None)
                    else:
                        p1, st, err = cv2.calcOpticalFlowPyrLK(old_grays[i], grays[i], zero_points[i], None, **lk_params)
                        tracked_points.append(p1)
                        status.append(st)


                magnitudes = []
                for i in range(len(tracked_points)):
                    avg_mag = 0
                    if tracked_points[i] is not None:
                        good_new = tracked_points[i][status[i]==1]
                        good_old = zero_points[i][status[i]==1]
                        for j, (new,old) in enumerate(zip(good_new, good_old)):
                            diff = new-old
                            mag = np.linalg.norm(diff)
                            avg_mag += mag
                            if i == 0:
                                a, b = new.ravel()
                                c, d = old.ravel()
                                mask = cv2.line(mask, (int(a), int(b)), (int(c), int(d)), color[j].tolist(), 2)
                                write_frame = cv2.circle(frames[i], (int(a), int(b)), 5, color[j].tolist(), -1)
                    magnitudes.append(avg_mag)
                for i in range(len(magnitudes)):
                    avg_mag_lists[i].append(magnitudes[i])

                old_grays = grays


                zero_points = []
                for gray in old_grays:
                    p0 = cv2.goodFeaturesToTrack(gray, mask = feature_mask, **feature_params)
                    zero_points.append(p0)
                mask = np.zeros_like(frames[0])
                
            ret,frame = cap.read()
            f_num+=1


        occlusion_present = False
        for avg_mag_list in avg_mag_lists:
            avg_mag_list = np.array(avg_mag_list)
            if avg_mag_list.mean() < occlusion_threshold:
                occlusion_present = True


        # record records for each hour captured in the video file
        for i in range(len(hours)):
            record_changed = False
            # iterate over records to check if there is a duplicate
            for j in range(len(out_dict['malfunction_id'])):
                # check if there is a duplicate and occlusion values do not match, if they don't favor occlusion
                if out_dict['malfunction_id'][j] == malfunction_id and out_dict['occlusion'][j] != occlusion_present:
                    out_dict['occlusion'][j] = True
                    record_changed = True
                    # if we're updating a record we want to update the malfunction_frame as well
                    out_dict['snapshot_file'][j] = snapshot_file
                    cv2.imwrite(os.path.join(snapshot_file_dir, snapshot_file), snapshot)
                elif out_dict['malfunction_id'][j] == malfunction_id:
                    record_changed = True
            # if we did not update an old record, write a new one
            if not record_changed:
                out_dict['malfunction_id'].append(malfunction_id)
                out_dict['camera_name'].append(malfunction_camera_name)
                out_dict['occlusion'].append(occlusion_present)
                out_dict['date'].append(malfunction_date)
                out_dict['hour'].append(malfunction_hour)
                out_dict['snapshot_file'].append(snapshot_file)
                cv2.imwrite(os.path.join(snapshot_file_dir,snapshot_file), snapshot)
        read_video_files[video] = (occlusion_present,snapshot)
out_df = pd.DataFrame(out_dict)
out_df.to_csv(output_file)
