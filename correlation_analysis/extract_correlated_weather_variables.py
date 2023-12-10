import pandas as pd
import numpy as np
import scipy.spatial as scp
from datetime import datetime
import argparse
import os

parser = argparse.ArgumentParser()
parser.add_argument('--correlation_data_dir', type=str, help='directory where location correlation scores are stored')
parser.add_argument('--mean_data_dir', type=str, help='directory where mean and standard devation values are stored')
parser.add_argument('--global_correlation_dir', type=str, help='location where globally correlated variables are stored')
parser.add_argument('--output_dir', type=str, help='where the output files will be stored')

args = parser.parse_args()

correlation_data_dir = args.correlation_data_dir
mean_data_dir = args.mean_data_dir
global_correlation_dir = args.global_correlation_dir
output_dir = args.output_dir

if not correlation_data_dir:
    correlation_data_dir = os.path.join(os.path.dirname(os.getcwd()), 'data/correlation_scores/')
if not mean_data_dir:
    mean_data_dir = os.path.join(os.path.dirname(os.getcwd()), 'data/weather_data/means/')
if not global_correlation_dir:
    global_correlation_dir = os.path.join(os.path.dirname(os.getcwd()), 'data/weather_data/global_variables/')
if not output_dir:
    output_dir =  os.path.join(os.path.dirname(os.getcwd()), 'data/weather_data/extracted_scores/')


threshold = 0.8

weather_location_map = {'cayuga':[44.96670383773909, -93.08861734632399], 'coates':[44.66570387767664, -93.0100457485304], 
                        'delano':[45.04, -93.76], 'bethel':[45.346454, -93.237496], '35w':[44.79853511526915, -93.2902160282043],
                        'inner grove':[44.875089237096034, -93.07399238699924], 'little canada':[45.03818723828624, -93.06287892907933],
                        'maple grove':[45.095097942100466, -93.44926670394744], 'mayer':[44.90596911346018, -93.86977661071903],
                        'minnetonka':[44.93996649497237, -93.46034320452821]}
nit_location_map = {'65_81st':[45.1150,-93.241732], '694_eriver_nramp':[45.069585,-93.278772], '694_eriver_nramp':[45.06892,-93.279158], '77_cliff_eramp':[44.790226,-93.21963],
                    '51_crc2':[45.02791,-93.167081],'77_cliff_eramp':[44.79022,-93.21963]}

correlation_files = os.listdir(correlation_data_dir)
mean_files = os.listdir(mean_data_dir)
global_correlation_files = os.listdir(global_correlation_dir)
weather_station_tree = scp.KDTree(list(weather_location_map.values()))

for file in correlation_files:
    out_dict = {'date':[], 'hour':[], 'loc':[], 'variables':[], 'means':[], 'standard_devs':[]}
    month = file.split('_')[-1][:-4]
    file_location = None
    for loc in nit_location_map:
        if loc in file:
            file_location = loc
    if not file_location:
        file_location = '65_81st'
    nearest_weather_station = weather_station_tree.query([nit_location_map[file_location]], k=1)[1][0]
    nearest_weather_station = list(weather_location_map.keys())[nearest_weather_station]
    loc_mean_file = None
    for mean_file in mean_files:
        if nearest_weather_station in mean_file.lower():
            loc_mean_file = mean_file
    glob_correlation_file = None

    for global_file in global_correlation_files:
        global_file_month = global_file.split('_')[-1][:-4]
        if file_location in global_file and global_file_month == month:
            glob_correlation_file = global_file

    # get globally correlated variables
    glob_correlation_df = pd.read_csv(os.path.join(global_correlation_dir, glob_correlation_file))
    glob_correlation_vars = []
    for i, item in glob_correlation_df.iterrows():
        glob_correlation_vars.append(item['variable'])
    
    means_stds= {}
    mean_df = pd.read_csv(os.path.join(mean_data_dir, mean_file))
    mean_columns = list(mean_df.columns)[2:]
    for i, item in mean_df.iterrows():
        key = (item['date'], item['hour'])
        values = {}
        for var in mean_columns:
            values[var] = item[var]
        means_stds[key] = values

    correlation_df = pd.read_csv(os.path.join(correlation_data_dir, file))
    for i, item in correlation_df.iterrows():
        correlated_vars = []
        for var in glob_correlation_vars:
            if item[var] >= threshold:
                correlated_vars.append(var)
        if correlated_vars == []:
            var_list = []
            for var in glob_correlation_vars:
                var_list.append(item[var])
            largest_indices = sorted(range(len(var_list)), key=lambda i: var_list[i], reverse=True)[:3]
            for index in largest_indices:
                correlated_vars.append(glob_correlation_vars[index])
        mean_values = []
        standard_dev_values = []
        time_key = (item['date'], item['hour'])
        if time_key not in means_stds:
            for var in correlated_vars:
                mean_values.append(-1)
                standard_dev_values.append(-1)
        else:
            mean_dev_vals = means_stds[time_key]
            for var in correlated_vars:
                mean_values.append(mean_dev_vals[var+'_m'])
                standard_dev_values.append(mean_dev_vals[var+'_std'])
        
        out_dict['date'].append(item['date'])
        out_dict['hour'].append(item['hour'])
        out_dict['loc'].append(file_location)
        out_dict['variables'].append(correlated_vars)
        out_dict['means'].append(mean_values)
        out_dict['standard_devs'].append(standard_dev_values)
    out_df = pd.DataFrame(out_dict)
    out_df.to_csv(os.path.join(output_dir, 'correlated_variables_'+str(file_location)+'_'+str(month)+'.csv'))