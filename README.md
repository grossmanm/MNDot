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

Weather data comes in the form of a .csv with the following schema.

| EVENTDATE | WEATHERSENSOR | VSIBILITY | HUMIDITY | PRECIP RATE | WIND DIR | WIND SPEED | MAX TEMP | MIN TEMP | WET BULB TEMP | DEW POINT | FRICTION | SURFACE TEMP | SURFACE STATUS | SUBSURFACE TEMP | AIR TEMP | PRECIP TYPE | 
|-----------|---------------|-----------|----------|-------------|----------|------------|----------|----------|---------------|-----------|----------|--------------|---------------|------------|-----------|----------|

If the weather data does not follow this schema, try running `python change_schema.py`
### Video Data
The video data is stored in the form of .mp4 files and is video taken from NIT cameras around the Twin-Cities Metropolitan area. Video files should follow one of the two naming schema listed below:
1. `cs-u-benjen_[NIT device name]-[date]_[start time]_[end time].mp4`
2. `nit_video2_[NIT device name]-[date]_[start time]_[end time].mp4`

Ex) `cs-u-benjen_694_eriver_nramp_vision-20230115_075949-085948`
    `nit_video2_65_81st_Vision_Stream1-20220703_190454_200033`

## To Run

### Malfunction Detection
If you're actuated signal data has already been ordered and is in the correct format you should run the following commands:
`python 01-highrez-satistics.py --startdt=2022-12-01 00:00:00.000 --enddt=2023-02-01 00:00:00.000 --input_dir=data/signal_data_sorted/ --output_file=data/signal_data_stats/output-stat-51-2022Dec2023Jan.json --input_files= sorted_ControllerLogs_Signal_51_2022_12.csv sorted_ControllerLogs_Signal_51_2023_01.csv`

This will generate time-series statstics about the actuated signal data. 

Next You will calculate local correlation scores and extract malfunctions. You should ensure that --stardt and --enddt are exacty 7 days apart:
`python 02-highrez-localcorr.py --intersectionID=51, --stardt=2023-01-01 --enddt=2023-01-08 --threshold=-0.6 --input_file=data/signal_data_stats/output_stat_51_2022-12-01_2023-01-30.json --output_dir=data/signal_data_analysis/ --output_file_baseme=output_detected`

Several files will be output from this process, malfunctions are stored in file named `output_detected_{intersection id}_{start date}_{end_date}-separateHours.json`

### Weather Correlation
We need to generate hourly traffic data and change the temporal resolution of the weather data to match. Run the following commands

`python generate_traffic_daily_volume.py --traffic_data_dir=data/signal_data_sorted/ --output_dir=data/signal_data_hourly/`

`python calc_hourly_mean_std.py --data_dir=data/weather_data/updated_schema/ --output_dir=data/weather_data/hourly/`

Next we will analyze the results of the previous step and generate correlation values for the weather variables at each timestep

`python analyze_weather_and_signal.py --weather_data_dir=data/weather_data/means/ --traffic_data_dir=data/signal_data_hourly/ --output_dir=data/correlation_scores/`

Next we will calculate global correlation and extract globally correlated variables

`python compute_global_correlation.py --correlation_dir=data/correlation_scores/ --known_correlation_file=data/weather_data/known_correlations.csv --output_dir=data/weather_data/global_variables/`

This will return a file with globally correlated variables for each month covered by the weather data

Next we will calculate the highly correlated variables for each hour in our data. We will include mean and standard devations for each the highly correlated variables

`python extract_correlated_weather_variables.py --correlation_data_dir=data/correlation_scores/ --mean_data_dir=data/weather_data/means/ --global_correlation_dir=data/weather_data/global_variables/ --output_dir=data/weather_data/extracted_scores/`

Next we will associate the highly correlated variables with malfunctions detected in the Malfunction Detection section

`python correlate_to_malfunciton.py --malfunction_dir=data/signal_data_analysis/ --correlation_dir=data/weather_data/extracted_scores/ --output_dir=data/malfunctions/weather_correlation/`


### Video Detection
We read the malfunciton data and assign them id's 
`python extract_time_ranges.py --detection_dir=data/signal_data_analysis/ --output_dir=data/intermediate_files/`

Now we want to find which videos are associated with which malfunctions

`python retrieve_video.py --time_ranges=data/intermediate_files/time_ranges.csv --source_folder=data/videos/ --output_dir=data/intermediate_files/`

Now we have a csv file with malfunctions and their associated files. Next we will look for occlusion. This will take quite a while

`python occlusion_detection.py --malfunction_file=data/intermediate_files/video_lcoations.csv --snapshot_dir=data/snapshots/ --video_dir=data/videos/ --output_dir=data/results/`

Now we assign a malfunction type based off of the detection

`python assign_malfunction_type.py --occlusion_file=data/results/detected_occlusion.py --output_file=data/results/detected_malfunctions.csv`

## Malfunction Analysis

Here we create a malfunciton database and analyze the results of the previous three sections.

To create the malfunction database run the follow line:

`python create_malfunction_db.py --correlation_dir=data/weather_data/correlation/ --occlusion_detection_file=data/results/detected_malfunctions.csv --output_dir='data/results/`

This will create a `malfunction_db.csv` file that we will use to create the analysis results

We have created four files for analyzing the malfunction results and they are split as follows:

1. We analyze the weather conditions surrounding specific types of malfunctions and detection technologies.
`python weather_variable_count_type.py --malfunction_file=data/results/malfunction_db.csv --output_dir=data/results/`
`python weather_variable_count_detector.py --malfunction_file=data/results/malfunction_db.csv --output_dir=data/results`
This process will output several files:
    `malfunction_weather_type_correlation.json` and `malfunction_weather_detector_correlation.json` contains the raw data used to generate the plots.
    `malfunction_weather_type_correlation_rate.png` and `malfunction_weather_detector_correlation_rate.png` displays a comparison of the rate that different weather variables cause malfuncitons in NIT devices for different malfunction types
    `malfunction_weather_type_correlation_mean.png` and `malfunction_weather_detector_correlation_mean.png` displays the mean and standard deviation of each of the weather variables that influence NIT malfunctions.

2. We analyze the weather conditions surrounding malfunctions in specific camera types.
`python weather_variable_count_detector.py --malfunction_file=data/results/malfunction_db.csv --output_dir=data/results/`

This process will output several files:
    `malfunction_weather_detector_correlation.json` contains the raw data used to generate the plots.
    `malfunction_weather_detector_correlation_rate.png` displays a comparison of the rate that different weather variables cause malfunctions in each NIT device type.
    `malfunction_weather_detector_correlation_mean.png` displays the mean and standard deviation of each of the weather variables that influence NIT malfunctions for each device type.

3. We analyze the number of malfunctions that occur at each location
`python malfunctions_per_location.py --malfunction_file=data/results/malfunction_db.csv --output_dir=data/results/`

This process outputs a CSV file containing the counts of malfunctions of each type that occur at each location

4. We analyze the rate of malfunctions over time for each detection technology

`python calc_malfunction_rate.py --malfunction_file=data/results/malfunction_db.csv --output_dir=data/results/`

This process outputs a CSV and a plot that describes the number of malfunctions experience per day by different detection technologies.
