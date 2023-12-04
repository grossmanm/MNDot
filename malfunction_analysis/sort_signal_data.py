import os
import argparse
import pandas as pd
from datetime import datetime

parser = argparse.ArgumentParser()
parser.add_argument('--signal_data_dir', type=str, help='Directory containing unsorted actuated singal data')
parser.add_argument('--output_dir', type=str, help='Where to write the sorted actuated signal data')

args = parser.parse_args()

signal_data_dir = args.signal_data_dir
output_dir = args.output_dir

if not signal_data_dir:
    signal_data_dir = os.path.join(os.path.dirname(os.getcwd()), 'data/signal_data/')
if not output_dir:
    output_dir = os.path.join(os.path.dirname(os.getcwd()), 'data/signal_data_sorted/')


date_template = '%Y-%m-%d %H:%M:%S.%f'

signal_files = os.listdir(signal_data_dir)

for file in signal_files:
    print(f"Sorting file: {file}")
    signal_df = pd.read_csv(os.path.join(signal_data_dir, file))
    if signal_df.shape[0] < 10:
        print("File empty, no data to sort")
        continue
    signal_df.columns = ["time", "detector", "phrase", "parameter"]
    signal_df = signal_df[:-1]
    for i, item in signal_df.iterrows():
        item['time']= datetime.strptime(item['time'], date_template)
    signal_df.sort_values(by='time', ascending=True, inplace=True)
    signal_df.to_csv(os.path.join(output_dir, 'sorted_'+file), index=False)