import pandas as pd

import os
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--data_dir', type=str, help='Location of weather data')
parser.add_argument('--output_dir', type=str, help='Where the new files will be written to')

args = parser.parse_args()

directory_path = args.data_dir
output_dir = args.output_dir

if not directory_path:
    directory_path = os.path.join(os.path.dirname(os.getcwd()), 'data/weather_data/raw')
if not output_dir:
    output_dir = os.path.join(os.path.dirname(os.getcwd()), 'data/weather_data/changed')

# from Time,Timestamp,adjacent_snow_depth,air_temperature,dewpoint_temperature,
# maximum_temperature,minimum_temperature,precip_accumulation_10min,precip_accumulation_12hr,
# precip_accumulation_1hr,precip_accumulation_24hr,precip_accumulation_3hr,
# precip_accumulation_5min,precip_accumulation_6hr,precip_accumulation_since_midnight,
# precip_accumulation_yesterday,precip_end_time,precip_intensity,precip_rate,
# precip_start_time,precip_type,pressure,relative_humidity,snow_rate,solar_radiation_24hr,
# sun,sun_minutes,visibility,wetbulb_temperature,wind_direction,wind_direction_cardinal,
# wind_gust,wind_gust_direction,wind_gust_direction_cardinal,wind_gust_mph,wind_situation,
# wind_speed,wind_speed_mph,chemical_factor,chemical_percent,depth,freezing_temperature,
# friction,ice_thickness,percent_ice,snow_depth,snowpack_depth,surface_condition,
# surface_temperature,water_level,subsurface_moisture,subsurface_temperature
# to
for root, dirs, files in os.walk(directory_path):
    for file in files:
        if '_changed_schema' in file:
            continue
        print(os.path.join(root, file))
        df = pd.read_csv(root+file)
        df.rename(columns={'Time': 'EVENTDATE',
                           'visibility': 'VISIBLITY',
                           'relative_humidity': 'HUMIDITY',
                           'precip_rate': 'PRECIP RATE',
                           'wind_gust_direction_cardinal': 'WIND DIR',
                           'wind_speed': 'WIND SPEED',
                           'maximum_temperature': 'MAX TEMP',
                           'minimum_temperature': 'MIN TEMP',
                           'wetbulb_temperature': 'WET BULB TEMP',
                           'dewpoint_temperature': 'DEW POINT',
                           'surface_temperature': 'SURFACE TEMP',
                           'surface_condition': 'SURFACE STATUS',
                           'subsurface_temperature': 'SUBSURFACE TEMP',
                           'air_temperature': 'AIR TEMP',
                            'precip_type': 'PRECIP TYPE'
                           }, inplace=True)
        seg = file.split('.')
        new_name = seg[0]  + '_changed_schema.csv'
        df.to_csv(os.path.join(output_dir, new_name), index=False)
