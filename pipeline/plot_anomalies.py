import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import json

files = ['/home/malcolm/MNDot/MNDot/data/signal_data_analysis/output_detected_51_2023-01-01_2023-01-08_NITanomalyhours_with_pearson.json', '/home/malcolm/MNDot/MNDot/data/signal_data_analysis/output_detected_51_2023-01-04_2023-01-11_NITanomalyhours_with_pearson.json']

recorded_pairs = []

day_counts = {'2023-01-01':0, '2023-01-02':0, '2023-01-03':0, '2023-01-04':0, '2023-01-05':0, '2023-01-06':0, '2023-01-07':0, '2023-01-08':0, '2023-01-09':0, '2023-01-10':0}

for file in files:
    with open(file,'r') as f:
        data = f.readlines()
        for line in data:
            anom = json.loads(line)
            date = anom['date']
            hour = anom['hour']
            if (date, hour) not in recorded_pairs:
                recorded_pairs.append((date,hour))
                if date not in day_counts:
                    day_counts[date]=0
                day_counts[date]+=1
plt.plot(list(day_counts.keys()), list(day_counts.values()), color='red')
#plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
plt.gcf().autofmt_xdate()
plt.xlabel('Date')
plt.ylim(0)
plt.ylabel('Anomaly Count')
plt.title(f"Detected Anomalies Over Time")
plt.legend()
plt.savefig('anomalies.png', dpi=300, bbox_inches='tight')
plt.clf()