import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime, timedelta
import random
import argparse
import os

date_template = '%Y-%m-%d'

parser = argparse.ArgumentParser()
parser.add_argument('--malfunction_file', type=str, help='Location of malfunctions file, output by create_malfunction_db.py')
parser.add_argument('--output_dir', type=str, help='Where to write the output csv and graph')

args = parser.parse_args()

malfunction_file = args.malfunction_file
output_dir = args.output_dir

if not malfunction_file:
    malfunction_file = os.path.join(os.path.dirname(os.getcwd()), 'data/results/malfunction_db.csv')
if not output_dir:
    output_dir = os.path.join(os.path.dirname(os.getcwd()), 'data/results/')

out_file = os.path.join(output_dir, 'malfunction_rates.csv')
out_graph = os.path.join(output_dir, 'malfunction_rates.png')


malfunction_df = pd.read_csv(malfunction_file)


count_dict = {}
min_date = None
max_date = None
day_iterator = timedelta(days=1)
# generate min and max date for data while we're iterating over malfunctions
for i, item in malfunction_df.iterrows():
    date = item['date']
    detection_technology = item['detection_technology']
    malfunction_type = item['malfunction_type']
    # check if the detection_technology exists in the count_dict, if not create a new sub-dictionary for it
    if detection_technology not in count_dict:
        count_dict[detection_technology] = {}
    # check if malfunction_type exists in the sub-dictionary, if not create a sub-sub-dictionary for it
    if malfunction_type not in count_dict[detection_technology]:
        count_dict[detection_technology][malfunction_type] = {}
    # check if the date exists in the sub-sub-dictionary for the detection technology and malfunction_type
    # if not create a counter for the date and initialize it to one. If it does exist, increment the counter
    if date not in count_dict[detection_technology][malfunction_type]:
        count_dict[detection_technology][malfunction_type][date] = 0
    count_dict[detection_technology][malfunction_type][date] += 1
    date = datetime.strptime(date, date_template).date()
    if not min_date:
        min_date = date
    if not max_date:
        max_date = date
    if date < min_date:
        min_date = date
    if date > max_date:
        max_date= date
# create a new graph and table for each detection_technology
out_graph+=f"_vision_iteris.png"
for detection_technology in count_dict:
    out_file+=f"_{detection_technology}.csv"
    
    malfunction_types = count_dict[detection_technology]
    out_dict = {'malfunction_type':[], 'date':[], 'malfunction_count':[]}
    # create a new plot for each malfunction
    for malfunction_type in malfunction_types:
        x = []
        y = []
        # populate dates 
        cur_date = min_date
        while cur_date<=max_date:
            x.append(cur_date)
            y.append(0)
            cur_date+=day_iterator
        # iterate over read dates and write the counts to the lists
        for date_key in malfunction_types[malfunction_type]:
            date = datetime.strptime(date_key, date_template).date()
            count = malfunction_types[malfunction_type][date_key]
            for i in range(len(x)):
                if x[i]==date:
                    y[i]=count
        # write to out_dict
        for i in range(len(x)):
            out_dict['malfunction_type'].append(malfunction_type)
            out_dict['date'].append(str(x[i]))
            out_dict['malfunction_count'].append(y[i])
        # create plot
        random_rgb = (random.random(), random.random(), random.random())
        plt.plot(x, y, label=malfunction_type+' '+detection_technology, color=random_rgb)
    
plt.gca().xaxis.set_major_formatter(mdates.DateFormatter(date_template))
plt.gcf().autofmt_xdate()
plt.xlabel('Date')
plt.ylim(0)
plt.ylabel('Malfunction Count')
plt.title(f"Malfunction Rates for Vision and Iteris")
plt.legend()
plt.savefig(out_graph, dpi=300, bbox_inches='tight')
plt.clf()

out_df = pd.DataFrame(out_dict)
out_df.to_csv(out_file)