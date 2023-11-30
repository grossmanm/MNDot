import csv

import pandas
import torch
import pandas as pd
from datetime import datetime
import matplotlib.pyplot as plt
import numpy as np
import os
from torch.utils.data import Dataset, DataLoader
from sklearn.preprocessing import StandardScaler as sklearn_StandardScaler

#dir = './daily_volumn/'
#plot_dir = './plot/'
#time_series_dir = './volumn_time_series/'

# file_name = 'sorted_ControllerLogs_Signal_1039_2022_6.csv'
# file_name = 'sorted_ControllerLogs_Signal_1039_2022_7.csv'
# file_name = '51_12_1.csv'
# file_name = '210_12_1.csv'
# file_name = '1039_12_1.csv'
#file_name = '51_12_1_hourly.csv'
# file_name = '210_12_1.csv'
# file_name = '1039_12_1.csv'
# file_name = '1041_12_1.csv'
# # file_name = 'sorted_ControllerLogs_Signal_1039_2023_1.csv'
#
## Below block is used for VAE_weather_metrics. When use VAE_test, comment below block.
#dir = './different_factors/'
#file_name = 'Cayuga St Br_12_1_hourly.csv'
# column = 'VISIBLITY'
# column = 'HUMIDITY'
# column = 'SUBSURFACE TEMP'
# column = 'SURFACE TEMP'
# column = 'WIND SPEED'
column = 'MAX TEMP'



def get_dataset(dir, file_name):



    df = pd.read_csv(dir + file_name, header=None)

    activate82 = df.to_numpy()

    return activate82

def get_df(dir, file_name):



    df = pd.read_csv(dir + file_name)



    return df

def get_data(file):



    df = pd.read_csv(file, header=None)

    activate82 = np.squeeze(df.to_numpy())

    return activate82

class StandardScaler():
    def __init__(self):
        self.mean = 0.
        self.std = 1.

    def fit(self, data):
        self.mean = data.mean(0)
        self.std = data.std(0)

    def transform(self, data):
        mean = torch.from_numpy(self.mean).type_as(data).to(data.device) if torch.is_tensor(data) else self.mean
        std = torch.from_numpy(self.std).type_as(data).to(data.device) if torch.is_tensor(data) else self.std
        return (data - mean) / std

    def inverse_transform(self, data):
        mean = torch.from_numpy(self.mean).type_as(data).to(data.device) if torch.is_tensor(data) else self.mean
        std = torch.from_numpy(self.std).type_as(data).to(data.device) if torch.is_tensor(data) else self.std
        return (data * std) + mean

class Dataset_Day(Dataset):
    def __init__(self, root_path, flag='train', size=None,
                 features='S', data_path=None,
                 target='OT', scale=True, inverse=False, timeenc=3, freq='h', cols=None):
        # size [seq_len, label_len, pred_len]
        # info
        if size == None:
            self.seq_len = 24 * 4 * 4
            self.label_len = 24 * 4
            self.pred_len = 24 * 4
        else:
            self.seq_len = size[0]
            self.label_len = size[1]
            self.pred_len = size[2]
        # init
        assert flag in ['train', 'test', 'val', 'all']
        type_map = {'train': 0, 'val': 1, 'test': 2, 'all':3}
        self.set_type = type_map[flag]

        self.features = features
        self.target = target
        self.scale = scale
        self.inverse = inverse
        self.timeenc = timeenc
        self.freq = freq

        self.root_path = root_path
        self.data_path = data_path
        self.__read_data__()

    def __read_data__(self):
        self.scaler = StandardScaler()

        # n = 100
        data = get_dataset()
        df_data = data

        # if n > 0:
        #     row = data['visits_by_day'].iloc[:n]
        # else:
        #     row = data['visits_by_day']
        # arr = np.array(row.apply(lambda x: np.fromstring(x[1:-1], dtype=int, sep=',')).tolist())
        #
        # df_data = arr[37]
        # # border1s = [0, 12*30*24 - self.seq_len, 12*30*24+4*30*24 - self.seq_len]
        # # border2s = [12*30*24, 12*30*24+4*30*24, 12*30*24+8*30*24]
        # # border1s = [0, 4 * 30 * 24 - self.seq_len, 5 * 30 * 24 - self.seq_len]
        # # border2s = [4 * 30 * 24, 5 * 30 * 24, 20 * 30 * 24]
        # border1s = [0, 430 - self.seq_len, 480 - self.seq_len, 0]
        # border2s = [430, 480, len(df_data), -1]

        #
        # border1 = border1s[self.set_type]
        # border2 = border2s[self.set_type]

        if self.features == 'M' or self.features == 'MS':
            cols_data = df_data.columns[1:]
            df_data = df_data[cols_data]
        elif self.features == 'S':
            df_data = df_data

        if self.scale:
            training_data = df_data
            self.scaler.fit(training_data)
            data = self.scaler.transform(df_data)

        else:
            data = df_data

        #
        # date = pd.DataFrame(pd.date_range(start="2018-12-31 00:00:00", end="2020-06-14 00:00:00"), columns=['date'])
        # # assert len(date) == len(data)
        #
        # if self.timeenc == 2:
        #     train_df_stamp = date[border1s[0]:border2s[0]]
        #     train_df_stamp['date'] = pd.to_datetime(train_df_stamp.date)
        #     # train_date_stamp = time_features(train_df_stamp, timeenc=self.timeenc)
        #     # date_scaler = sklearn_StandardScaler().fit(train_date_stamp)
        #
        #     df_stamp = date[border1:border2]
        #     df_stamp['date'] = pd.to_datetime(df_stamp.date)
        #     # data_stamp = time_features(df_stamp, timeenc=self.timeenc, freq=self.freq)
        #     # data_stamp = date_scaler.transform(data_stamp)
        # else:
        #     df_stamp = date[border1:border2]
        #     df_stamp['date'] = pd.to_datetime(df_stamp.date)
        #     # data_stamp = time_features(df_stamp, timeenc=self.timeenc, freq=self.freq)

        self.data_x = np.expand_dims(data, axis=1)
        # if self.inverse:
        #     self.data_y = df_data.values
        # else:
        #     self.data_y = np.expand_dims(data, axis=1)
        # self.data_stamp = data_stamp

    def __getitem__(self, index):
        # if self.set_type == 2: pdb.set_trace()
        s_begin = index
        s_end = s_begin + self.seq_len
        # r_begin = s_end - self.label_len
        # r_end = r_begin + self.label_len + self.pred_len

        seq_x = self.data_x[s_begin:s_end]
        # if self.inverse:
        #     seq_y = np.concatenate(
        #         [self.data_x[r_begin:r_begin + self.label_len], self.data_y[r_begin + self.label_len:r_end]], 0)
        # else:
        #     seq_y = self.data_y[r_begin:r_end]
        # seq_x_mark = self.data_stamp[s_begin:s_end]
        # seq_y_mark = self.data_stamp[r_begin:r_end]

        return seq_x

    def __len__(self):
        return len(self.data_x) - self.seq_len - self.pred_len + 1

    def inverse_transform(self, data):
        return self.scaler.inverse_transform(data)


class Dataset_Weather(Dataset):
    def __init__(self, column, root_path, flag='train', size=None,
                 features='S', data_path=None,
                 target='OT', scale=True, inverse=False, timeenc=3, freq='h', cols=None):
        # size [seq_len, label_len, pred_len]
        # info
        self.column = column
        if size == None:
            self.seq_len = 24 * 4 * 4
            self.label_len = 24 * 4
            self.pred_len = 24 * 4
        else:
            self.seq_len = size[0]
            self.label_len = size[1]
            self.pred_len = size[2]
        # init
        assert flag in ['train', 'test', 'val', 'all']
        type_map = {'train': 0, 'val': 1, 'test': 2, 'all':3}
        self.set_type = type_map[flag]

        self.features = features
        self.target = target
        self.scale = scale
        self.inverse = inverse
        self.timeenc = timeenc
        self.freq = freq

        self.root_path = root_path
        self.data_path = data_path
        self.__read_data__()

    def __read_data__(self):
        self.scaler = StandardScaler()

        # n = 100
        data = get_df()
        df_data = data

        # if n > 0:
        #     row = data['visits_by_day'].iloc[:n]
        # else:
        #     row = data['visits_by_day']
        # arr = np.array(row.apply(lambda x: np.fromstring(x[1:-1], dtype=int, sep=',')).tolist())
        #
        # df_data = arr[37]
        # # border1s = [0, 12*30*24 - self.seq_len, 12*30*24+4*30*24 - self.seq_len]
        # # border2s = [12*30*24, 12*30*24+4*30*24, 12*30*24+8*30*24]
        # # border1s = [0, 4 * 30 * 24 - self.seq_len, 5 * 30 * 24 - self.seq_len]
        # # border2s = [4 * 30 * 24, 5 * 30 * 24, 20 * 30 * 24]
        # border1s = [0, 430 - self.seq_len, 480 - self.seq_len, 0]
        # border2s = [430, 480, len(df_data), -1]

        #
        # border1 = border1s[self.set_type]
        # border2 = border2s[self.set_type]

        if self.features == 'M' or self.features == 'MS':
            cols_data = df_data.columns[1:]
            df_data = df_data[cols_data]
        elif self.features == 'S':
            df_data = df_data[self.column]

        if self.scale:
            training_data = df_data
            self.scaler.fit(training_data)
            data = self.scaler.transform(df_data)

        else:
            data = df_data[self.column]

        #
        # date = pd.DataFrame(pd.date_range(start="2018-12-31 00:00:00", end="2020-06-14 00:00:00"), columns=['date'])
        # # assert len(date) == len(data)
        #
        # if self.timeenc == 2:
        #     train_df_stamp = date[border1s[0]:border2s[0]]
        #     train_df_stamp['date'] = pd.to_datetime(train_df_stamp.date)
        #     # train_date_stamp = time_features(train_df_stamp, timeenc=self.timeenc)
        #     # date_scaler = sklearn_StandardScaler().fit(train_date_stamp)
        #
        #     df_stamp = date[border1:border2]
        #     df_stamp['date'] = pd.to_datetime(df_stamp.date)
        #     # data_stamp = time_features(df_stamp, timeenc=self.timeenc, freq=self.freq)
        #     # data_stamp = date_scaler.transform(data_stamp)
        # else:
        #     df_stamp = date[border1:border2]
        #     df_stamp['date'] = pd.to_datetime(df_stamp.date)
        #     # data_stamp = time_features(df_stamp, timeenc=self.timeenc, freq=self.freq)

        self.data_x = np.expand_dims(data, axis=1)
        # if self.inverse:
        #     self.data_y = df_data.values
        # else:
        #     self.data_y = np.expand_dims(data, axis=1)
        # self.data_stamp = data_stamp

    def __getitem__(self, index):
        # if self.set_type == 2: pdb.set_trace()
        s_begin = index
        s_end = s_begin + self.seq_len
        # r_begin = s_end - self.label_len
        # r_end = r_begin + self.label_len + self.pred_len

        seq_x = self.data_x[s_begin:s_end]
        # if self.inverse:
        #     seq_y = np.concatenate(
        #         [self.data_x[r_begin:r_begin + self.label_len], self.data_y[r_begin + self.label_len:r_end]], 0)
        # else:
        #     seq_y = self.data_y[r_begin:r_end]
        # seq_x_mark = self.data_stamp[s_begin:s_end]
        # seq_y_mark = self.data_stamp[r_begin:r_end]

        return seq_x

    def __len__(self):
        return len(self.data_x) - self.seq_len - self.pred_len + 1

    def inverse_transform(self, data):
        return self.scaler.inverse_transform(data)

def NormalizeData(data):
    return (data - np.min(data)) / (np.max(data) - np.min(data))