import pandas as pd
import os
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--malfunction_file', type=str, help='Location of malfunctions file, output by create_malfunction_db.py')
parser.add_argument('--output_dir', type=str, help='Where to write the output csv')

args = parser.parse_args()

malfunction_file = args.malfunction_file
output_dir = args.output_dir

if not malfunction_file:
    malfunction_file = os.path.join(os.path.dirname(os.getcwd()), 'data/results/malfunction_db.csv')
if not output_dir:
    output_dir = os.path.join(os.path.dirname(os.getcwd()), 'data/results')

out_file = os.path.join(output_dir, 'malfunction_locations.csv')

out_dict = {'intersection':[], 'malfunction_type':[], 'malfunction_count':[]}

counter = {}
malfunction_df = pd.read_csv(malfunction_file)

for i, item in malfunction_df.iterrows():
    intersection = item['camera_name']
    malfunction_type = item['malfunction_type']
    counter_key = (intersection, malfunction_type)
    if counter_key not in counter:
        counter[counter_key] = 0
    counter[counter_key]+=1

for counter_key in counter:
    intersection = counter_key[0]
    malfunction_type = counter_key[1]
    count = counter[counter_key]
    out_dict['intersection'].append(intersection)
    out_dict['malfunction_type'].append(malfunction_type)
    out_dict['malfunction_count'].append(count)

out_df = pd.DataFrame(out_dict)
out_df.to_csv(out_file)