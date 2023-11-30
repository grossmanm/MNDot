import pandas as pd
import argparse
import os

parser = argparse.ArgumentParser()
parser.add_argument('--occlusion_file', type=str, help='Output of occlusion_detection.py, contains detected occlusion for malfunctions')
parser.add_argument('--output_dir', type=str, help='Where to write the output csv')

args = parser.parse_args()

occlusion_file = args.occlusion_file
output_dir = args.output_dir

if not occlusion_file:
    occlusion_file = os.path.join(os.path.dirname(os.getcwd()), 'data/results/detected_occlusion.csv')

if not output_dir:
    output_dir = os.path.join(os.path.dirname(os.getcwd()), 'data/results/')

out_file = os.path.join(output_dir, 'detected_malfunctions.csv')

occlusion_df = pd.read_csv(occlusion_file)

out_dict={'malfunction_id':[], 'camera_name': [],'date':[], 'hour':[], 'malfunction_type':[], 'snapshot_file':[]}   

for i, item in occlusion_df.iterrows():
    malfunction_id = item['malfunction_id']
    occlusion_present = item['occlusion']
    camera_name = item['camera_name']
    date = item['date']
    hour = item['hour']
    snapshot_file = item['snapshot_file']
    
    malfunction_type = 'unknown'

    if occlusion_present:
        malfunction_type='occlusion'
    out_dict['malfunction_id'].append(malfunction_id)
    out_dict['camera_name'].append(camera_name)
    out_dict['date'].append(date)
    out_dict['hour'].append(hour)
    out_dict['malfunction_type'].append(malfunction_type)
    out_dict['snapshot_file'].append(snapshot_file)

out_df = pd.DataFrame(out_dict)

out_df.to_csv(out_file)