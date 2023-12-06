# MNDot
Project Folder for MNDot Traffic Camera Project. The code is for evaluating the performance of NIT cameras in the Twin Cities Metropolitan area

Note: This project was built using python 3.10.12

## Before Running
Run `pip install requirements.txt` to ensure all required python packages are installed

## Data
This project uses three types of data; actuated signal data, weather data, and traffic camera data. The preprocessing and schema requirements are outlined below.

### Actuated Signal Data
Actuated Singal Data reports traffic flow through an intersection for a variety traffic detection technologies. It is reported in the from of a .csv. In Minnesota, two possible schema are used for actuated signal data. Older records use the first schema and newer records use the second.

| time | detector | phase | parameter | 
|------|----------|-------|-----------|

| Timestamp | Event Name | Event ID | Event Value | 
|-----------|------------|----------|-------------|

Once you have downloaded the signal data you can place it in `/data/signal_data/`. This is the default directory used by the project.

Actuated signal data is often delivered unsorted by time stamp, to sort the code run the following code:

1. If you are using the first schema of the two listed above, run `python sort_signal_data.py --signal_data_dir='data/signal_data/ --output_dir='data/signal_data_sorted/`
2. If you are using the second schema of the two listed above, run `python 00-highrez-preprocess.py --target_intersection=51 --filePath=data/signal_data --output_filePath=data/signal_data_sorted/`


### Weather Data

Weather data comes in the form of a .csv file and two possible schemas.

| EVENTDATE | WEATHERSENSOR | VSIBILITY | HUMIDITY | PRECIP RATE | WIND DIR | WIND SPEED | MAX TEMP | MIN TEMP | WET BULB TEMP | DEW POINT | FRICTION | SURFACE TEMP | SURFACE STATUS | SUBSURFACE TEMP | AIR TEMP | PRECIP TYPE | 
|-----------|---------------|-----------|----------|-------------|----------|------------|----------|----------|---------------|-----------|----------|--------------|---------------|------------|-----------|----------|