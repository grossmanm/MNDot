import pandas as pd
import argparse
import os
from datetime import datetime
import numpy as np

parser = argparse.ArgumentParser()
parser.add_argument('--correlation_dir', type=str, help='directory where location correlation scores are stored')
parser.add_argument('--known_correlation_file', type=str, help='location of file containing weather variables that are known to be highly correlated')
parser.add_argument('--output_dir', type=str, help='where the output file will be written')

args = parser.parse_args()



correlation_dir = args.correlation_dir
known_correlation_file = args.known_correlation_file
output_dir = args.output_dir

if not correlation_dir:
    correlation_dir = os.path.join(os.path.dirname(os.getcwd()), 'data/correlation_scores/')
if not known_correlation_file:
    known_correlation_file = os.path.join(os.path.dirname(os.getcwd()),'data/weather_data/known_correlations.csv')
if not output_dir:
    output_dir = os.path.join(os.path.dirname(os.getcwd()), 'data/weather_data/global_variables/')

k_percent = 80
correlation_files = os.listdir(correlation_dir)

known_correlation = pd.read_csv(known_correlation_file)
known_correlation_list = known_correlation['variable'].to_numpy()

for file in correlation_files:
    file_template = file.replace('local_correlations', '')[:-4]
    out_dict = {'variable':[]}
    correlation_df = pd.read_csv(os.path.join(correlation_dir, file))
    # get column names and exclude date and hour
    weather_variables = list(correlation_df.columns)[2:]

    ratios = {}
    known_vals = []
    for var in weather_variables:
        weather_var_data = correlation_df[var].to_numpy()
        top_threshold = np.percentile(weather_var_data, 100-k_percent)
        top_vars = weather_var_data[weather_var_data >=top_threshold]
        bottom_vars = weather_var_data[weather_var_data<top_threshold]
        top_mean = top_vars.mean()
        bottom_mean = bottom_vars.mean()
        ratio = top_mean/bottom_mean
        ratios[var]=ratio
        if var in known_correlation_list:
            known_vals.append(ratio)

    
    for var in ratios:
        ratio = ratios[var]
        use = False
        for known_ratio in known_vals:
            if abs(ratio-known_ratio) <=0.1:
                use = True
        if use:
            out_dict['variable'].append(var)
    out_df = pd.DataFrame(out_dict)
    out_df.to_csv(os.path.join(output_dir, 'global_correlated_vars'+file_template+'.csv'))
