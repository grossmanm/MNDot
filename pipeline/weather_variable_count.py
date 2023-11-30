import pandas as pd
import json
import numpy as np
import matplotlib.pyplot as plt
import random
import ast
import argparse
import os

vars_to_ignore = ['PRECIPTYPE', 'WINDDIR', 'SURFACESTATUS']

def calc_mean(arr, mode):
    # we do not include the group sizes because all sizes are assumed to be equal (1 hour of data)
    if mode == 'mean':
        avg = np.round(np.mean(arr),4)
    elif mode == 'mode':
        vals, counts = np.unique(arr, return_counts=True)
        avg = vals[np.argmax(counts)]
    return avg

def calc_std(arr, mode):
    if mode == 'std':
        squared = np.square(arr)
        avg = np.round(np.mean(squared),4)
        std = np.sqrt(avg)
    if mode == 'freq':
        freq_dict = {}
        num_items = len(arr)
        for item in arr:
            for var in item:
                if var not in freq_dict:
                    freq_dict[var] = 0
                freq_dict[var] += item[var]
        for var in freq_dict:
            freq_dict[var] = freq_dict[var]/num_items
        std = freq_dict
    return std

parser = argparse.ArgumentParser()
parser.add_argument('--malfunction_file', type=str, help='Location of malfunctions file, output by create_malfunction_db.py')
parser.add_argument('--output_dir', type=str, help='Where to write the output csv and graphs')

args = parser.parse_args()

malfunction_file = args.malfunction_file
output_dir = args.output_dir

if not malfunction_file:
    malfunction_file = os.path.join(os.path.dirname(os.getcwd()), 'data/results/malfunction_db.csv')
if not output_dir:
    output_dir = os.path.join(os.path.dirname(os.getcwd()), 'data/results/')

out_file = os.path.join(output_dir, 'malfunction_weather_correlation.json')
out_rate = os.path.join(output_dir, 'malfunction_weather_correlation_rate.png')
out_mean = os.path.join(output_dir, 'malfunction_weather_correlation_mean.png')
out_mode = os.path.join(output_dir, 'malfunction_weather_correlation_mode')

malfunction_df = pd.read_csv(malfunction_file)

#iterate over malfunctions and count number of times a weather variable correlates with a malfunction type
categories = []

out_dict = {}
number_of_malfunctions = {}
malfunction_means = {}
malfunction_stds = {}

for i, item in malfunction_df.iterrows():
    malfunction_type = item['malfunction_type']
    weather_variables = item['weather_variables']
    weather_means = item['weather_variable_mean']
    weather_stds = item['weather_variable_std']
    weather_variables = weather_variables.strip('[]').replace("'",'').replace(' ','').split(',')
    weather_means = weather_means.strip('[]').replace("'",'').replace(' ','').split(',')
    weather_stds = weather_stds.strip('[]').replace("'",'').replace('"','').replace(' ','').split(',')
    final_weather_stds = []
    for j in range(len(weather_means)):
        if weather_means[j].replace('.','').isdigit():
            weather_means[j] = float(weather_means[j])
    freq = False
    # resolving formatting conflicts 
    for j in range(len(weather_stds)):
        if not freq:
            if weather_stds[j].replace('.','').replace('-','').isdigit():
                final_weather_stds.append(float(weather_stds[j]))
            elif '{' in weather_stds[j] and '}' in weather_stds[j]:
                weather_var, weather_std = weather_stds[j].strip('{}').split(':')
                temp_dict = {weather_var:float(weather_std)}
                final_weather_stds.append(temp_dict)
            else:
                freq = True
                weather_var, weather_std = weather_stds[j].strip('{}').split(':')
                temp_dict = {weather_var:float(weather_std)}
        else:
            if '{' not in weather_stds[j] and '}' not in weather_stds[j] and not weather_stds[j].replace('.','').isdigit():
                weather_var, weather_std = weather_stds[j].split(':')
                temp_dict[weather_var] = float(weather_std)
            elif '}' in weather_stds[j]:
                weather_var, weather_std = weather_stds[j].strip('}').split(':')
                temp_dict[weather_var] = float(weather_std)
                final_weather_stds.append(temp_dict)
                freq = False
    # encountering a new malfunction type, make a sub-dictionary for storing weather variables and counts
    if malfunction_type not in out_dict:
        out_dict[malfunction_type]={}
        number_of_malfunctions[malfunction_type]=0
        malfunction_means[malfunction_type]={}
        malfunction_stds[malfunction_type]={}
    # iterate over weather variables and check they exist in the malfunction_type sub-dictionary
    # if they don't intialize the counter to 0
    number_of_malfunctions[malfunction_type]+=1
    for j in range(len(weather_variables)):
        if weather_variables[j] not in out_dict[malfunction_type]:
            out_dict[malfunction_type][weather_variables[j]] = 0
            malfunction_means[malfunction_type][weather_variables[j]] = []
            malfunction_stds[malfunction_type][weather_variables[j]] = []
        out_dict[malfunction_type][weather_variables[j]] += 1
        malfunction_means[malfunction_type][weather_variables[j]].append(weather_means[j])
        malfunction_stds[malfunction_type][weather_variables[j]].append(final_weather_stds[j])
        if weather_variables[j] not in categories:
            categories.append(weather_variables[j])


# calculate overall mean and standard deviation for each variable and malfunction type
for type in malfunction_means:
    for weather_var in malfunction_means[type]:
        mean_list = malfunction_means[type][weather_var]
        mean_list = [x for x in mean_list if x!='-1']
        mode = 'mean'
        if weather_var == 'PRECIPTYPE' or weather_var == 'WINDDIR' or weather_var == 'SURFACESTATUS':
            mode = 'mode'
        avg = calc_mean(mean_list,mode)
        malfunction_means[type][weather_var] = avg
for type in malfunction_stds:
    for weather_var in malfunction_stds[type]:
        std_list = malfunction_stds[type][weather_var]
        std_list = [x for x in std_list if x!=-1]
        mode = 'std'
        if weather_var == 'PRECIPTYPE' or weather_var == 'WINDDIR' or weather_var == 'SURFACESTATUS':
            mode = 'freq'
        std = calc_std(std_list, mode)
        malfunction_stds[type][weather_var] = std
# plot as a bar graph
all_counts = []

for type in list(out_dict.keys()):
    counts = np.zeros(len(categories))
    read_dict = out_dict[type]
    for i in range(len(categories)):
        category = categories[i]
        if category in read_dict:
            counts[i] = read_dict[category]
    all_counts.append(counts)

colors = [(random.random(), random.random(), random.random()) for _ in out_dict]

bar_width=0.35
bar_position_set = np.arange(len(categories))
for i in range(len(list(out_dict.keys()))):
    malfunction_type = list(out_dict.keys())[i]
    random_rgb = colors[i]
    plt.bar(bar_position_set, all_counts[i]/number_of_malfunctions[malfunction_type], width=bar_width, label=malfunction_type, color=random_rgb)
    bar_position_set = bar_position_set+bar_width

plt.xlabel('Weather Variables')
plt.xticks(np.arange(len(categories)), categories, rotation=45)
plt.ylabel('Rate')
plt.title("Correlation of Weather Variables and Malfunction Type")
plt.legend()
plt.tight_layout()
plt.savefig(out_rate)
plt.clf()

# plot mean and standard deviations
all_means = []
all_modes = []
mean_categories = [x for x in categories if x not in vars_to_ignore]
mode_categories = [x for x in categories if x in vars_to_ignore]

for type in malfunction_means:
    means = np.zeros(len(mean_categories))
    modes = ['' for _ in range(len(mode_categories))]
    read_dict = malfunction_means[type]
    for i in range(len(categories)):
        category = categories[i]
        if category in mean_categories:
            idx = mean_categories.index(category)
            means[idx] = read_dict[category]
        else:
            idx = mode_categories.index(category)
            modes[idx] = read_dict[category]
    all_means.append(means)
    all_modes.append(modes)

all_stds = []
all_freqs = []
for type in malfunction_stds:
    stds = np.zeros(len(mean_categories))
    freqs = ['' for _ in range(len(mode_categories))]
    read_dict = malfunction_stds[type]
    for i in range(len(categories)):
        category = categories[i]
        if category in mean_categories:
            idx = mean_categories.index(category)
            stds[idx] = read_dict[category]
        else:
            idx = mode_categories.index(category)
            freqs[idx] = read_dict[category]
    all_stds.append(stds)
    all_freqs.append(freqs)

bar_position_set = np.arange(len(mean_categories))
for i in range(len(list(malfunction_means.keys()))):
    malfunction_type = list(malfunction_means.keys())[i]
    random_rgb = colors[i]
    plt.bar(bar_position_set, all_means[i], yerr=all_stds[i], capsize=5, width=bar_width, label=malfunction_type, color=random_rgb)
    bar_position_set = bar_position_set+bar_width

plt.xlabel('Weather Variables')
plt.xticks(np.arange(len(mean_categories)), mean_categories, rotation=45)
plt.ylabel('Mean Value')
plt.title("Mean Value of Weather Variables for Malfunction Types")
plt.legend()
plt.tight_layout()
plt.savefig(out_mean)
plt.clf()

# make a pie chart for the modal values
for i in range(len(list(malfunction_means.keys()))):
    malfunction_type = list(malfunction_means.keys())[i]
    fig, axs = plt.subplots(1, len(all_modes[i]), figsize=(10, 5))
    for j in range(len(all_modes[i])):
        sizes = []
        labels = []
        for var in all_freqs[i][j]:
            val = all_freqs[i][j][var]
            labels.append(var)
            sizes.append(val)
        if len(all_modes[i]) > 1:
            axs[j].pie(sizes, labels=labels, autopct='%1.1f', startangle=90)
            axs[j].set_title(f"Variation of {mode_categories[j]}")
        else:
            axs.pie(sizes, labels=labels, autopct='%1.1f', startangle=90)
            axs.set_title(f"Variation of {mode_categories[j]}")

    plt.suptitle(malfunction_type)
    plt.tight_layout()
    plt.savefig(out_mode+'_'+malfunction_type+'.png')
    plt.clf()

with open(out_file, 'w') as f:
    json.dump(out_dict, f)
    f.write('\n')
    json.dump(malfunction_means, f)
    f.write('\n')
    json.dump(malfunction_stds, f)
