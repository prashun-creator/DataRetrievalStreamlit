import datetime
import functools
import sklearn
from datetime import timedelta
from functools import reduce
import csv
import re
from operator import itemgetter
import openai
from sklearn.impute import KNNImputer
import pandas as pd
import numpy as np
from numpy import isnan
from pymongo import MongoClient
import urllib
import json

import numpy as np
import pandas
import pandas as pd
import streamlit as st
# from pandas_profiling import ProfileReport
# from pandas.core.base import DataError

from pymongo import MongoClient
import csv
import re
import logging
import logging.config
import sys
import os
from calculation import *
logger = logging.getLogger(__name__)

# @st.cache

mongo_secret_key = os.environ.get("MONGO_SECRET_KEY")
def convert_df(df):
    return df.to_csv().encode('utf-8')


client = \
    MongoClient(mongo_secret_key)
db = client.fasal
device = db['DeviceTelemetry']
plot = db['plot']
plot_hist = db['plotCropHistory']
cust = db['customer']
db2 = client['forecast-service']
forecast = db2['weatherForecastFinalData']


def get_sensor_data(id_wasp, sensor_list, date_1, date_2):
    """
    Function to extract sensor data for a sensor ID.

    id_wasp: str
         sensor ID for which data needs to be imputed.

    sensor_list: array
         list of sensor names for which data is required

    start_date: str
        date from which data is required in the format of 'YYYY-MM-DD'

     end_date: str😃
        date up to which data is required in the format of 'YYYY-MM-DD'

   """

    date_1 = datetime.datetime(int(date_1.split('-')[0]),
                               int(date_1.split('-')[1]),
                               int(date_1.split('-')[2]))
    date_2 = datetime.datetime(int(date_2.split('-')[0]),
                               int(date_2.split('-')[1]),
                               int(date_2.split('-')[2]))

    df = pd.DataFrame(
        list(
            device.find({
                "id_wasp": id_wasp,
                'sensor': {
                    "$in": sensor_list
                },
                "datetime": {
                    "$gte": date_1,
                    "$lte": date_2
                }
            })))
    try:
        df = df[['id_wasp', 'datetime', 'sensor', 'value']]
        dflist = []
        df['value'] = df['value'].astype('float64')
        for sensor in df.sensor.unique().tolist():
            df_sensor = df[(df['sensor'] == sensor)][[
                'id_wasp', 'value', 'datetime'
            ]].rename(columns={'value': sensor})
            dflist.append(df_sensor)
        df_final = reduce(
            lambda df1, df2: pd.merge(df1, df2, on=['id_wasp', 'datetime']),
            dflist)
        df_final = df_final.drop_duplicates(subset=['id_wasp', 'datetime'],
                                            keep=False)
        df_final['datetime'] = pd.to_datetime(
            df_final['datetime']).dt.tz_localize('UTC').dt.tz_convert(
            'Asia/Kolkata')
        df_final['date'] = df_final['datetime'].dt.date
        df_final['hour'] = df_final['datetime'].dt.hour
        df_final['month'] = df_final['datetime'].dt.month
        sensor_test = sensor_list.copy()
        if 'PLV2' in sensor_test:
            sensor_test.remove('PLV2')
            df_x = df_final.groupby(['id_wasp', 'date', 'month', 'hour'
                                     ])[sensor_test].mean().reset_index()
            df_y = df_final.groupby(['id_wasp', 'date', 'month',
                                    'hour'])['PLV2'].sum().reset_index()
            df_final = pd.merge(df_x,
                                df_y,
                                how='inner',
                                on=['id_wasp', 'date', 'month', 'hour'])
        else:
            df_final = df_final.groupby(['id_wasp', 'date', 'month', 'hour'
                                         ])[sensor_list].mean().reset_index()

        df_final = df_final.drop_duplicates(
            subset=['id_wasp', 'date', 'month', 'hour'], keep=False)
        df_final.rename(columns={'id_wasp': 'sensorId'}, inplace=True)
        sensor_plot = {'sensorId': [], 'plotId': []}
        for value in df_final['sensorId'].unique():
            plotId = get_plotId(value)
            sensor_plot['plotId'].append(plotId)
            sensor_plot['sensorId'].append(value)
        sensor_plot = dict(zip(sensor_plot['sensorId'], sensor_plot['plotId']))
        df_final['plotId'] = df_final['sensorId'].map(sensor_plot)
        return df_final
    except Exception as e:
        df_final = pd.DataFrame()
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        logger.error(
            f"function {fname}, line no: {e.__traceback__.tb_lineno},error: {e}")


# @st.cache
def get_plot_crop_1(crop_name, sensor_list, date_1, date_2):
    """
    Function to fetch plot details for for a given crop.

    crop_name: str
         crop name for which data is required, e.g. 'chilli'.

    """
    plot_data = pd.DataFrame(list(plot.find({"crop.cropName": crop_name})))[[
        '_id', 'name', 'customer', 'sensorId']]
    cust_data = pd.DataFrame(
        list(cust.find({"_id": {
            "$in": plot_data.customer.unique().tolist()
        }})))[['_id', 'name', 'type']]
    cust_data = cust_data[cust_data['type'].isin(['PAID', 'TRIAL'])].copy()
    plot_final = plot_data[plot_data['customer'].isin(
        cust_data._id.unique().tolist())].copy()
    plot_final = plot_final.rename(columns={
        '_id': 'plotId',
        'customer': 'customerId',
        'name': 'plotName'
    })
    cust_data = cust_data.rename(columns={
        '_id': 'customerId',
        'name': 'customerName'
    })
    plot_final = pd.merge(plot_final, cust_data, how='left', on=['customerId'])
    idwasp_list = find_sensorid(plot_final, sensor_list, date_1)

    df_list = []
    for id_wasp in idwasp_list:
        df = get_sensor_data(id_wasp, sensor_list, date_1, date_2)
        df_list.append(df)
    df_all = pd.concat(df_list)
    df_all = df_all.rename(columns={'id_wasp': 'sensorId'})
    df_all = pd.merge(df_all,
                      plot_final[[
                          'customerName', 'type', 'plotId', 'plotName',
                          'sensorId'
                      ]],
                      how='left',
                      on=['sensorId'])
    st.write(df_all)

    return df_all


def get_sensorId1(plot_id):
    sensorId = list(plot.find({'_id': plot_id}))
    sensorId = sensorId[0].get('sensorId')
    return sensorId


def run_time1(data, h):
    s = pd.to_datetime(data['date'].astype(str) + ' ' +
                       data['hour'].astype(str).str.zfill(0) + ':00:' + '00',
                       format="%Y%m%d %H:%M:%S")
    data['hour'] = (
        s +
        pd.DateOffset(hours=h)).dt.strftime("%H").str.lstrip("0").str.zfill(2)
    data['date'] = (s + pd.DateOffset(hours=h)).dt.strftime("%Y-%m-%d")

    return data


def get_weather_forecast_data1(plotid, date_1, date_2, day):
    """
    Function to extract weather forecast data for a list of plot ids.

    plotid_list: str
         plot ID whose forecast is required

    start_date: str
        date from which data is required in the format of 'YYYY-MM-DD'

     end_date: str
        date up to which data is required in the format of 'YYYY-MM-DD'

    day: int
      forecast day for which data is required, e.g. is 0 is next day forecast, 1 is day after tomorrow, etc

    """
    df = pd.DataFrame(
        list(
            forecast.find({
                "plotId": plotid,
                "date": {
                    "$gte": date_1,
                    "$lte": date_2
                }
            })))
    # print(df.head())
    if not df.empty:
        df['data_2'] = [d.get('data') for d in df.data]
        df['forecast'] = [d.get('forecast') for d in df.data_2]
        df_1 = df.set_index('plotId')['forecast'].apply(
            pd.Series)['forecastday'].apply(pd.Series)[[day]]
        df_1['Hour'] = [d.get('hour') for d in df_1[day]]
        df_1 = df_1['Hour'].apply(
            pd.Series).stack().reset_index().rename(columns={
                'level_1': 'Hour',
                0: 'data'
            })
        df_1['time'] = [d.get('time') for d in df_1.data]
        df_1['time'] = pd.to_datetime(df_1['time'])
        df_1['hour'] = df_1['time'].dt.hour
        df_1['date'] = df_1['time'].dt.date
        df_1['hour'] = df_1['time'].dt.hour
        df_1['date'] = df_1['time'].dt.date
        df_1['humidity'] = [d.get('humidity') for d in df_1.data]
        df_1['wind_kph'] = [d.get('wind_kph') for d in df_1.data]
        df_1['precip_mm'] = [d.get('precip_mm') for d in df_1.data]
        df_1['temp_c'] = [d.get('temp_c') for d in df_1.data]
        df_1['cloud'] = [d.get('cloud') for d in df_1.data]
        df_1 = df_1.drop_duplicates(subset=['plotId', 'time'])
        df_1 = df_1[[
            'plotId', 'date', 'hour', 'humidity', 'wind_kph', 'precip_mm',
            'temp_c', 'cloud'
        ]]
        return df_1
    else:
        df_1 = df
        return df_1


def initial_input_crop_specific():
    """function to take initial input for app"""
    crop = list(db['plot'].distinct('crop.cropName'))
    crop_list = st.selectbox('Select the crop', crop)
    plotid_name = {'name': [], 'plotId': []}
    plotids = list(plot.find({'crop.cropName': crop_list}))
    for i in range(len(plotids)):
        plotid = plotids[i].get('_id')
        name = plotids[i].get('name')
        plotid_name['name'].append(name)
        plotid_name['plotId'].append(plotid)
    sensor = st.sidebar.radio('Select the one',
                              ('Selected sensors', 'All sensors'))
    if sensor == 'All sensors':
        sensor_list = [
            'TC', 'HUM', 'PRES', 'PLV2', 'LUX', 'ANE', 'LDR', 'SOILTC',
            'SOIL_B', 'SOIL_C', 'LW'
        ]
    if sensor == 'Selected sensors':
        sensor_list = st.sidebar.multiselect(
            'Enter your sensor requirement:',
            ('TC', 'HUM', 'PRES', 'PLV2', 'LUX', 'ANE', 'LDR', 'SOILTC',
             'SOIL_B', 'SOIL_C', 'LW'))
    startDate = st.date_input('Enter startDate')
    startDate = str(pd.to_datetime(startDate).date())
    endDate = st.date_input('Enter endDate')
    endDate = str(pd.to_datetime(endDate).date())
    if startDate == endDate:
        st.write('Please change the start date to proceed')
    if len(sensor_list) == 0:
        st.write('Please select atleast one sensor to proceed')

    select_plot = st.sidebar.radio(
        'Want to get data for specific plot or whole crop Data', ('Specific plots', 'All plots in this crop'))
    if select_plot == 'Specific plots':
        data = pd.DataFrame(plotid_name)
        st.write(data)
        data = convert_df(data)

        st.download_button("Download the plot_list with plot_name",
                           data,
                           "file.csv",
                           "text/csv",
                           key='download-csv1')

        choice = st.sidebar.radio(
            'Enter plotId manually', ('Yes', 'No, i will upload'))
        if choice == 'Yes':
            plotId = []
            try:
                n = int(st.text_input('Enter the number of plotId you have'))
            except:
                st.write('Provide a number')
                n = 0
            for i in range(n):
                x = st.text_input(f'Enter plotId {i+1}')
                plotId.append(x)
            st.write(plotId)

        elif choice == 'No, i will upload':
            x = st.sidebar.file_uploader(
                label="Upload your dataset having plotId column")
            if x is not None:
                plot_csv = pd.read_csv(x)
            plotId = plot_csv['plotId'].values
            st.write('uploaded csv have following plotId', plotId)
        sensorId = []
        df_all = pd.DataFrame()
        for value in plotId:
            x = get_sensorId1(value)
            sensorId.append(x)
        for value in sensorId:
            x = pd.DataFrame(get_sensor_data(
                value, sensor_list, startDate, endDate))
            df_all = pd.concat([df_all, x],ignore_index=True)
        st.write(f'data shape :  {df_all.shape}')
        st.write(df_all)
        st.write('Data for these customers:',
                 df_all.groupby(['plotId']).count())
        data = convert_df(df_all)

        st.download_button("Download the hourly sensor data available for this crop",
                           data,
                           "file.csv",
                           "text/csv",
                           key='download-csv-unique')

    elif select_plot == 'All plots in this crop':
        df_all = get_plot_crop_1(crop_list, sensor_list, startDate, endDate)
        st.write(
            f'Data for these plots are available we for {crop_list} from {startDate} to {endDate}')
        st.write(f'data shape :  {df_all.shape}')
        st.write('Data for these customers:',
                 df_all.groupby(['plotName']).count())

        df_all1 = convert_df(df_all)

        st.download_button("Download the hourly sensor data available for this crop",
                           df_all1,
                           "file.csv",
                           "text/csv",
                           key='download-csv2')

    return df_all, crop_list, sensor_list, startDate, endDate


def data_analyzer():
    st.write(
        'Provide plotIs and wait while we analysing your dataset.... Your report will come here : '
    )
    opt = st.sidebar.radio("How do you want to import the dataset",
                           ("Upload CSV", 'via link'),
                           index=0)

    if opt == "Upload CSV":
        x = st.sidebar.file_uploader(label="Upload your dataset")
        if x is not None:
            df = pd.read_csv(x)
            # profile = ProfileReport(df)
    else:
        paste = st.text_input("Paste the link of the csv here")

        num_rows = st.number_input("Number of rows you want to import",
                                   min_value=10,
                                   value=1000)
        check = st.checkbox("Import")
        if check:
            df = pd.read_csv(paste, nrows=num_rows, on_bad_lines='skip')
            # profile = ProfileReport(df)
    return profile


def find_sensorid(df, sensor_list, date_1):
    date_x = datetime.datetime(int(date_1.split('-')[0]),
                               int(date_1.split('-')[1]),
                               int(date_1.split('-')[2]))
    date_final = (date_x + timedelta(90)).strftime("%Y-%m-%d")
    idwasp_final_list = []
    idwasp_list = df.sensorId.unique().tolist()
    for id_wasp in idwasp_list:
        df = get_sensor_data11(id_wasp, sensor_list, date_1, date_final)
        if (len(df) > 30) & (set(sensor_list).issubset(set(df.columns))):
            idwasp_final_list.append(id_wasp)
    return idwasp_final_list


def get_sensor_data11(id_wasp, sensor_list, date_1, date_2):
    """
    Function to extract sensor data for a sensor ID.

    id_wasp: str
        sensor ID for which data needs to be imputed.

    sensor_list: array
        list of sensor names for which data is required

    start_date: str
        date from which data is required in the format of 'YYYY-MM-DD'

    end_date: str😃
        date up to which data is required in the format of 'YYYY-MM-DD'

"""
    try:

        date_1 = datetime.datetime(int(date_1.split('-')[0]),
                                   int(date_1.split('-')[1]),
                                   int(date_1.split('-')[2]))
        date_2 = datetime.datetime(int(date_2.split('-')[0]),
                                   int(date_2.split('-')[1]),
                                   int(date_2.split('-')[2]))

        df = pd.DataFrame(
            list(
                device.find({
                    "id_wasp": id_wasp,
                    'sensor': {
                        "$in": sensor_list
                    },
                    "datetime": {
                        "$gte": date_1,
                        "$lte": date_2
                    }
                })))
        sensor_value = df['sensor'].unique()
        rem_sensor = [x for x in sensor_list if x not in sensor_value]
        df = df[['id_wasp', 'datetime', 'sensor', 'value']]
        dflist = []
        df['value'] = df['value'].astype('float64')
        for sensor in sensor_value:
            df_sensor = df[(df['sensor'] == sensor)][[
                'id_wasp', 'value', 'datetime'
            ]].rename(columns={'value': sensor})
            dflist.append(df_sensor)

        df_final = functools.reduce(
            lambda df1, df2: pd.merge(df1, df2, on=['id_wasp', 'datetime']
                                      ), dflist)

        # df_final = df_final.drop_duplicates(subset=['id_wasp', 'datetime'],
        #                                     keep=False)

        # try:
        #     timezone = self.get_time_zone(id_wasp)
        #     curr_tz = str(tzlocal.get_localzone())
        #     df_final['datetime'] = pd.DatetimeIndex(
        #         pd.to_datetime(df_final['datetime'])).tz_localize(
        #         curr_tz).tz_convert(timezone)
        # except:
        #     curr_tz = str(tzlocal.get_localzone())
        #     df_final['datetime'] = pd.DatetimeIndex(
        #         pd.to_datetime(df_final['datetime'])).tz_localize(
        #         curr_tz).tz_convert('Asia/Kolkata')

        df_final['date'] = df_final['datetime'].dt.date
        df_final['hour'] = df_final['datetime'].dt.hour
        df_final['month'] = df_final['datetime'].dt.month
        df_final.drop(columns='datetime', inplace=True)

        df_final = df_final.drop_duplicates(
            subset=['id_wasp', 'date', 'month', 'hour'], keep='first')
        df_final.rename(columns={'id_wasp': 'sensorId'}, inplace=True)

        # for value in sensor_value:

        #     if value == 'SOIL_B':
        #         df_final['SOIL_B'] = (150940 -
        #                             (df_final['SOIL_B'] * 19.74)) / (
        #                                     (df_final['SOIL_B'] * 2.8875) -
        #                                     137.5)
        #     if value == 'SOIL_C':
        #         df_final['SOIL_C'] = (150940 -
        #                             (df_final['SOIL_C'] * 19.74)) / (
        #                                     (df_final['SOIL_C'] * 2.8875) -
        #                                     137.5)
        for value in rem_sensor:
            df_final[value] = np.nan
    except Exception as e:
        df_final = pd.DataFrame()
        # df_final = pd.DataFrame(columns=['date','month','hour','sensorId']+sensor_list)
        logger.warning(f" line no: {e.__traceback__.tb_lineno},warnings: {e}")

    return df_final


# def get_data_iter(id_wasp, sensor_list, date_1, date_2):
#     date_1 = datetime.datetime(int(date_1.split('-')[0]),
#                                int(date_1.split('-')[1]),
#                                int(date_1.split('-')[2]))
#     date_2 = datetime.datetime(int(date_2.split('-')[0]),
#                                int(date_2.split('-')[1]),
#                                int(date_2.split('-')[2]))
#     dfList = []
#     if date_1 + timedelta(120) <= date_2:
#         df_final = get_sensor_data(id_wasp, sensor_list,
#                                    date_1.strftime("%Y-%m-%d"),
#                                    date_2.strftime("%Y-%m-%d"))

#     else:
#         date_compare = date_1
#         while date_compare < date_2:
#             date_compare = date_compare + timedelta(120)
#             if date_compare >= date_2:
#                 df = get_sensor_data(id_wasp, sensor_list,
#                                      date_1.strftime("%Y-%m-%d"),
#                                      date_2.strftime("%Y-%m-%d"))
#                 dfList.append(df)
#             else:
#                 df = get_sensor_data(id_wasp, sensor_list,
#                                      date_1.strftime("%Y-%m-%d"),
#                                      date_compare.strftime("%Y-%m-%d"))
#                 dfList.append(df)
#         df_final = pd.concat(dfList)
#     return df_final


def remove_customer(data):
    """Remove customer from the existing dataframe """
    cust_remove = st.text_input(
        'Provide the plotId of customer to remove from data ')
    data = data[data['plotId'] != str(cust_remove)]
    return data


def add_customer(data, sensor_list, start_date, end_date):
    n = int(
        st.text_input(
            'Provide the number of customer to be added: (Please provide data for the same crop)'
        ))

    for i in range(n):
        pid = st.text_input(f'Provide the plotId for customer {i}: ')
        name = st.text_input(f'Provide the name of customer {i}:')
    SEN = str(get_sensorId1(pid))
    st.write('sensorId is: ', SEN)
    cus = pd.DataFrame(get_sensor_data(SEN, sensor_list, start_date, end_date))
    cus.rename(columns={'id_wasp': 'sensorId'}, inplace=True)
    cus['customerName'] = name
    cus['plotId'] = pid
    data_1 = pd.concat([data, cus])

    return data_1


def get_sen_data_pid():
    sensor_list = [
            'TC', 'HUM', 'PRES', 'PLV2', 'LUX', 'ANE', 'LDR', 'SOILTC',
            'SOIL_B', 'SOIL_C', 'LW'
        ]
    choice = st.sidebar.radio(
        'How do you want to input:',
        ('Using plotId one by one', 'upload a csv having plotId column'))
    sensor = st.sidebar.radio('Select the one',
                              ('All sensors', 'Selected sensors'))
    if sensor == 'Selected sensors':
        sensor_list = st.sidebar.multiselect(
            'Enter your sensor requirement:',
            ('TC', 'HUM', 'PRES', 'PLV2', 'LUX', 'ANE', 'LDR', 'SOILTC',
             'SOIL_B', 'SOIL_C', 'LW'))
    if sensor == 'All sensors':
        sensor_list = [
            'TC', 'HUM', 'PRES', 'PLV2', 'LUX', 'ANE', 'LDR', 'SOILTC',
            'SOIL_B', 'SOIL_C', 'LW'
        ]
    start_date = st.date_input('Enter startDate')
    start_date = str(pd.to_datetime(start_date).date())
    end_date = st.date_input('Enter endDate')
    end_date = str(pd.to_datetime(end_date).date())

    if start_date >= end_date:
        st.write('startDate cannot be greater than or equal to endDate')

    if choice == 'Using plotId one by one':
        customer = int(
            st.text_input('Provide the  total number of customers:'))
        plotid = []
        for i in range(customer):
            pid = st.text_input(f'Provide the plotId for customer {i}: ')
            plotid.append(pid)
        cus1 = sensor_data_plotId(plotid, sensor_list, start_date, end_date)
        st.write('data',cus1)
    if choice == 'upload a csv having plotId column':
        x = st.sidebar.file_uploader(label="upload a csv having plotIds")
        if x is not None:
            plotids = pd.read_csv(x)
        if 'plotId' in plotids.columns:
            pid = plotids['plotId']
            pid = list(pid.values)
        else:
            st.write('plotId column is not in the csv, please provide to proceed')
        cus1 = sensor_data_plotId(pid, sensor_list, start_date, end_date)
    return cus1, sensor_list, start_date, end_date


def sensor_data_plotId(plotid, sensor_list, start_date, end_date):
    sen = []
    for value in plotid:
        sen.append(get_sensorId1(value))
    sen_plot = {'sensorId': sen, 'plotId': plotid}
    st.write(sen_plot['plotId'])
    dict1 = dict(zip(sen_plot['sensorId'], sen_plot['plotId']))
    
    cus1 = pd.DataFrame()
    for value in sen_plot['sensorId']:
        cus = get_sensor_data_new(value, sensor_list, str(start_date), str(end_date))
        cus1 = pd.concat([cus1, cus])

    cus1['plotId'] = cus1['sensorId'].map(dict1)
    return cus1


def soil_cb(data_1, sensor_list):
    for value in sensor_list:

        if value == 'SOIL_B':
            data_1['SOIL_B'] = (150940 - (data_1['SOIL_B'] * 19.74)) / (
                (data_1['SOIL_B'] * 2.8875) - 137.5)
        if value == 'SOIL_C':
            data_1['SOIL_C'] = (150940 - (data_1['SOIL_C'] * 19.74)) / (
                (data_1['SOIL_C'] * 2.8875) - 137.5)
    return data_1


def filter_raw(df_all_1, sensor_list, date_1, date_2):
    try:
        a = st.sidebar.radio('You want to add or remove some customer',
                             ('No', 'Add', 'Remove'))
        if a == 'Add':
            data_1 = add_customer(df_all_1, sensor_list, date_1, date_2)

        if a == 'No':
            data_1 = df_all_1
        if a == 'Remove':
            data_1 = remove_customer(df_all_1)

        soil = st.sidebar.radio('You want soil moisture data conversion to CB',
                                ('Yes', 'No'))
        if soil == 'Yes':
            data_1 = soil_cb(data_1, sensor_list)
            st.write(data_1)
        else:
            st.write(data_1)

        b = st.sidebar.radio(
            'Model will run midnight to midnight', ('Yes', 'No'))
        if b == 'Yes':
            pass
        else:
            run = st.text_input('Provide the model run-time (0-23)')
            run = int(run)
            data_1 = run_time1(data_1, run)
            if 0 <= run <= 23:
                if 0 <= run < 12:
                    st.write(
                        f'Model will run from {abs(run)}AM to {abs(run)}AM')
                else:
                    st.write(
                        f'Model will run from {abs(run - 12)}PM to {abs(run - 12)}PM'
                    )
            else:

                st.write(" Provide valid input")

        c = st.sidebar.radio(
            'Work with hourly data or daily average data?',
            ('hourly_data', 'daily_avg or daily_sum'))

        if c == 'hourly_data':
            pass
        if c == 'daily_avg or daily_sum':
            data_1 = get_aggregated_data(data_1)
            # dic = {sensor: '' for sensor in sensor_list}
            # for sensor in sensor_list:
            #     dic[sensor] = st.sidebar.radio(
            #         f'{sensor}',
            #         ('daily_avg', 'daily_sum'),
            #         key=dic[sensor])
            # dic_1 = {
            #     'daily_avg': (1, 'mean'),
            #     'daily_sum': (1, 'sum')
            # }
            # data_1 = data_1.groupby(['plotId','sensorId','date'],as_index=False, sort=False).agg(
            #     {sensor: dic_1[dic[sensor]][1]
            #         for sensor in sensor_list})
        return data_1
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        logger.error(
            f"++++++function {fname}, line no: {e.__traceback__.tb_lineno},error: {e}")


def get_aggregated_data(df):
    if len(df) > 0:
        aggregation_methods = {'sensor': [], 'agg': []}
        sensor_list = [
            'TC', 'HUM', 'PRES', 'PLV2', 'LUX', 'ANE', 'LDR', 'SOILTC',
            'SOIL_B', 'SOIL_C', 'LW'
        ]
        available_sensors = []
        for columns in df.columns:
            if columns in sensor_list:
                available_sensors.append(columns)
                aggregation_methods['sensor'].append(columns)
                x = st.sidebar.radio(f'{columns}', ('mean', 'sum'))
                aggregation_methods['agg'].append(x)

        aggregation_methods = dict(
            zip(aggregation_methods['sensor'], aggregation_methods['agg']))
        st.write('your selected methods: ', aggregation_methods)
        daily_data_df = df.groupby(['plotId', 'date']).agg(
            aggregation_methods).reset_index()
        daily_data_df.columns = [''.join(col).strip()
                                 for col in daily_data_df.columns.values]
        return daily_data_df
    else:
        st.write('no data available to aggregate')
        return pd.DataFrame()


def data_impute(data, sensor_list, date1, date2):
    """will give you empty column if there is no data for such columns and if column value missed, imputed data """
    columns = list(data.columns)
    missing = []
    for value in sensor_list:
        if value in columns:
            pass
        else:
            missing.append(value)

    df = pd.DataFrame()

    for sid in data.sensorId.unique():
        empty_df = pd.DataFrame(
            {'date': pd.date_range(date1, date2, freq='1H')})
        empty_df['hour'] = empty_df['date'].dt.hour
        empty_df['date'] = empty_df['date'].dt.date
        empty_df['date'] = empty_df['date'].apply(
            lambda x: pd.to_datetime(x, format='%Y-%m-%d').date())

        empty_df['sensorId'] = sid

        df = df.append(empty_df)
    df['sensorId'] = df['sensorId'].astype('category')
    data['sensorId'] = data['sensorId'].astype('category')

    data['date'] = data['date'].apply(
        lambda x: pd.to_datetime(x, format='%Y-%m-%d').date())

    merged = pd.merge(df, data, on=['sensorId', 'date', 'hour'], how='left')

    plot = []
    for value in merged['sensorId'].values.unique():
        plot.append(get_plotId(value))
    sen_plot = {'sensorId': merged['sensorId'].values.unique(), 'plotId': plot}
    dict1 = dict(zip(sen_plot['sensorId'], sen_plot['plotId']))

    merged['plotId'] = merged['sensorId'].map(dict1)

    impute_df = merged

    calculation_columns = [ele for ele in columns if ele in sensor_list]

    for value in calculation_columns:
        Null_percent = impute_df[f'{value}'].isnull().sum() / len(impute_df)

        if Null_percent * 100 <= 1:
            impute_df[f'{value}'] = impute_df[f'{value}'].fillna(
                method='bfill')
        else:
            imputer = KNNImputer(
                n_neighbors=5, weights='uniform', metric='nan_euclidean')
            imputer.fit(impute_df[f'{value}'].values.reshape(-1, 1))
            impute_df[f'{value}'] = imputer.transform(
                impute_df[f'{value}'].values.reshape(-1, 1))

    for value in missing:
        impute_df[f'{value}'] = 'No_sensor_value'

    return impute_df


def get_plotId(sen_id):

    cust = pd.DataFrame(
        list(db.customer.find({"type": {
            "$in": ["PAID", "TRIAL"]
        }})))

    # customer_name_map = pd.Series(cust.name.values, index=cust._id).to_dict()
    customers = cust._id.unique(
    )  # Getting _id to map in plot collection customer field
    df = pd.DataFrame(list(db.plot.find({"isActive": {"$in": [True]}})))
    y = df[df["customer"].isin(customers)]
    plot_id_list = y._id.unique()
    sensoridlist = pd.DataFrame(
        list(db.plot.find({"_id": {
            "$in": list(plot_id_list)
        }})))
    sensoridlist = sensoridlist[["_id", "sensorId"]]
    sensoridlist.columns = ["plotId", "sensorId"]
    sensoridlist.sensorId.unique()
    pd.DataFrame()
    id_wasp_map = pd.DataFrame(
        [sensoridlist.plotId.values, sensoridlist.sensorId]).T
    id_wasp_map.columns = ['plotId', 'sensorId']
    x = []
    for sid in id_wasp_map[['sensorId', 'plotId']].iterrows():
        plotId = sid[1].plotId
        sensorId = sid[1].sensorId
        if sensorId == sen_id:
            x = plotId
            break
    return x


def get_3rdparty_data(lat, long, date_1, date_2):
    if date_1 < date_2:
        api_key = '2L8BATCWVJSW7JT39TUBC4LTR'
        start_date = date_1  # format "YYYY-MM-DD"
        end_date = date_2  # format "YYYY-MM-DD"
        url = "https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/" + \
            lat + "%2C%20" + long + "/" + start_date + "/" + end_date + \
            "?unitGroup=metric&elements=datetime%2CdatetimeEpoch%2Cname" \
            "%2Clatitude%2Clongitude%2Ctemp%2Chumidity%2Cprecip%2Cwindspeed%2Csolarradiation%2Csunrise%2Csunset%2Csoiltemp04%2Csoilmoisture01%2Csoilmoisture04&include=hours&key=" \
                + api_key + "&contentType=json"

        # Get data from visual crossing
        r = urllib.request.urlopen(url).read()
        if len(r) >= 1:
            # Convert json to dataframe
            df = pd.DataFrame(json.loads(r.decode()))
            df['date'] = df['days'].apply(pd.Series)['datetime']
            df['hour'] = df['days'].apply(pd.Series)['hours']
            df['sunrise'] = df['days'].apply(pd.Series)['sunrise']
            df['sunset'] = df['days'].apply(pd.Series)['sunset']
            df['day_length_hrs'] = (
                (pd.to_datetime(df['sunset']) -
                    pd.to_datetime(df['sunrise'])).dt.total_seconds() /
                3600).round()
            df['day_length_%'] = df['day_length_hrs'] * 100 / 24
            df_1 = df.set_index(
                ['address', 'date', 'day_length_hrs',
                    'day_length_%'])[['hour']]
            df_1 = df_1['hour'].apply(
                pd.Series).stack().reset_index().rename(columns={
                    'level_4': 'hour',
                    0: 'data'
                })
            df_1['TC'] = [d.get('temp') for d in df_1.data]
            df_1['HUM'] = [d.get('humidity') for d in df_1.data]
            df_1['PLV2'] = [d.get('precip') for d in df_1.data]
            df_1['ANE'] = [d.get('windspeed') for d in df_1.data]
            df_1['LUX'] = [d.get('solarradiation') for d in df_1.data]
            df_1['LUX'] = df_1['LUX'] * 120
            df_1['LDR'] = df_1['LUX'] * 100 / 100000
            df_1['SOILTC'] = [d.get('soiltemp04') for d in df_1.data]
            df_1['SOIL_B'] = [d.get('soilmoisture01') for d in df_1.data]
            df_1['SOIL_B'] = df_1['SOIL_B'] * 0.2
            df_1['SOIL_C'] = [d.get('soilmoisture04') for d in df_1.data]
            df_1['SOIL_C'] = df_1['SOIL_C'] * 0.2
            df_1['latitude'] = df_1['address'][0].split(',')[0]
            df_1['longnitude'] = df_1['address'][0].split(',')[1]
            df_1['LW'] = 0
            df_2 = df_1[[
                'date', 'hour', 'TC', 'HUM', 'ANE', 'LW', 'PLV2', 'LUX',
                'LDR', 'SOILTC', 'SOIL_B', 'SOIL_C', 'latitude',
                'longnitude'
            ]]

        else:
            df_2 = pd.DataFrame()
    else:
        df_2 = pd.DataFrame()

    return df_2


def data_imputation(data, sensor_list1):
    import time
    import math
    data3 = pd.DataFrame()
    sensor_list = []
    for sensor in sensor_list1:
        if data[f"{sensor}"].count() != 0:
            sensor_list.append(sensor)
    st.write(f"imputation for {sensor_list}")
    for sensorId in data['sensorId'].unique():
        df_temp = data[data['sensorId'] == sensorId].reset_index(drop=True)
        df_temp.rename(columns={'index': 'index1'}, inplace=True)

        for sensor in sensor_list:
            timeout = time.time() + 45  # 45seconds for one sensor
            try:
                for i in range(len(df_temp)):
                    if time.time() > timeout:
                        break

                    row = df_temp.iloc[i]
                    hour = row.hour
                    index = i

                    if math.isnan(row[sensor]):
                        date = row.date
                        date = pd.to_datetime(date,
                                              format='%Y-%m-%d').date()
                        j, k = 1, 1
                        while True:
                            if len(df_temp[sensor].loc[
                                (df_temp['hour'] == hour)
                                    & (df_temp['date'] == str(date -
                                                              timedelta(j))
                                       )].values) == 0:
                                break

                            elif math.isnan(df_temp[sensor].loc[
                                (df_temp['hour'] == hour)
                                    & (df_temp['date'] == str(
                                        date - timedelta(j)))].values[0]):
                                j += 1
                            else:
                                break
                        while True:
                            if len(df_temp[sensor].loc[
                                (df_temp['hour'] == hour)
                                    & (df_temp['date'] == str(date +
                                                              timedelta(k))
                                       )].values) == 0:
                                break
                            elif math.isnan((df_temp[sensor].loc[
                                (df_temp['hour'] == hour)
                                    & (df_temp['date'] == str(
                                        date + timedelta(k)))].values)[0]):
                                k += 1
                            else:
                                break

                        if len(df_temp[sensor].loc[
                            (df_temp['hour'] == hour) &
                            (df_temp['date'] == str(date - timedelta(j))
                             )].values) == 0 or math.isnan(
                            df_temp[sensor].loc[
                                (df_temp['hour'] == hour)
                                & (df_temp['date'] == str(
                                    date - timedelta(j)))].values[0]):

                            df_temp.loc[index,
                                        sensor] = df_temp[sensor].loc[
                                            (df_temp['hour'] == hour)
                                            & (df_temp['date'] == str(
                                                date +
                                                timedelta(k)))].values[0]

                        elif len(df_temp[sensor].loc[
                            (df_temp['hour'] == hour) & (
                                df_temp['date'] == str(date + timedelta(k))
                            )].values) == 0 or math.isnan(
                                (df_temp[sensor].loc[
                                    (df_temp['hour'] == hour)
                                    & (df_temp['date'] == str(
                                        date + timedelta(k)))].values)[0]):
                            df_temp.loc[index,
                                        sensor] = df_temp[sensor].loc[
                                            (df_temp['hour'] == hour)
                                            & (df_temp['date'] == str(
                                                date -
                                                timedelta(j)))].values[0]

                        else:

                            df_temp.loc[index, sensor] = (
                                df_temp[sensor].loc[
                                    (df_temp['hour'] == hour) &
                                    (df_temp['date'] == str(
                                        date - timedelta(j)))].values[0] +
                                df_temp[sensor].loc[
                                    (df_temp['hour'] == hour) &
                                    (df_temp['date'] == str(
                                        date + timedelta(k)))].values[0]
                            ) / 2

            except:
                st.write(
                    f"data is too less to impute for sensor {sensor}")
        data3 = data3.append(df_temp)
    return data3


def empty_df(data):
    merged1 = pd.DataFrame()
    for sid in data['sensorId'].unique():
        data1 = data[data['sensorId'] == sid].reset_index(drop='index')
        empty_df = pd.DataFrame({
            'date':
            pd.date_range(min(data1['date']), max(data1['date']), freq='1H')
        })
        empty_df['hour'] = empty_df['date'].dt.hour
        empty_df['date'] = empty_df['date'].dt.date
        empty_df['date'] = empty_df['date'].apply(
            lambda x: pd.to_datetime(x, format='%Y-%m-%d').date())

        empty_df['sensorId'] = sid
        empty_df['plotId'] = get_plotId(sid)
        data1['sensorId'] = sid
        data1['plotId'] = get_plotId(sid)
        data1['date'] = data1['date'].astype(str)
        empty_df['date'] = empty_df['date'].astype(str)

        merged = pd.merge(empty_df,
                          data1,
                          on=['sensorId', 'plotId', 'date', 'hour'],
                          how='left')
        merged1 = pd.concat([merged1, merged])
    merged1 = merged1.drop(columns=['month'])
    st.write('Before Imputation', merged1)
    return merged1


def chatgpt_query():
    st.title("Chatting with ChatGPT")
    st.sidebar.header("Instructions")
    st.sidebar.info(
        '''This is a web application that allows you to interact with 
       the OpenAI API's implementation of the ChatGPT model.
       Enter a **query** in the **text box** and **press enter** to receive 
       a **response** from the ChatGPT
       '''
    )
    # Set the model engine and your OpenAI API key
    openai.api_key = ""
    main()


def main():
    '''
    This function gets the user input, pass it to ChatGPT function and 
    displays the response
    '''
    # Get user input
    user_query = st.text_input(
        "Enter query here, to exit enter :q", "what is Python?")
    if user_query != ":q" or user_query != "":
        # Pass the query to the ChatGPT function
        response = ChatGPT(user_query)
        return st.write(f"{user_query} {response}")


def ChatGPT(user_query):
    ''' 
    This function uses the OpenAI API to generate a response to the given 
    user_query using the ChatGPT model
    '''
    # Use the OpenAI API to generate a response
    model_engine = "text-davinci-003"
    completion = openai.Completion.create(
        engine=model_engine,
        prompt=user_query,
        max_tokens=1024,
        n=1,
        temperature=0.5,
    )
    response = completion.choices[0].text
    return response


def upload_plotId():

    choice = st.sidebar.radio('Enter plotId manually',
                              ('Yes', 'No, i will upload'))
    if choice == 'Yes':
        plotId = []
        try:
            n = int(st.text_input('Enter the number of plotId you have'))
        except:
            st.write('Provide a number')
            n = 0
        for i in range(n):
            x = st.text_input(f'Enter plotId {i+1}')
            plotId.append(x)
        st.write(plotId)

    elif choice == 'No, i will upload':
        x = st.sidebar.file_uploader(
            label="**Upload your dataset having plotId column and column name is on first row**")
        if x is not None:
            plot_csv = pd.read_csv(x)
        plotId = plot_csv['plotId'].values
        st.write('uploaded csv have following plotId', plotId)
        plotId = plotId.tolist()

    return plotId


def upload_plotId_start_end_date():

    choice = st.sidebar.radio('Enter plotId manually',
                              ('Yes', 'No, i will upload'))
    plot_csv = None
    plotId = None
    if choice == 'Yes':
        plotId = []
        try:
            n = int(st.text_input('Enter the number of plotId you have'))
        except:
            st.write('Provide a number')
            n = 0
        for i in range(n):
            x = st.text_input(f'Enter plotId {i+1}')
            plotId.append(x)
        st.write(plotId)

    elif choice == 'No, i will upload':
        x = st.sidebar.file_uploader(
            label="**Upload your dataset having plotId,start_date,end_date column and column name is on first row **")
        if x is not None:
            plot_csv = pd.read_csv(x)
        st.write('uploaded csv have following plotId',
                 plot_csv['plotId'].values)

    return plotId, plot_csv


def download(data):
    data = convert_df(data)
    st.download_button("Download the above data",
                       data,
                       "file.csv",
                       "text/csv",
                       key='download-csv3')


def crop_v2_data_extraction():
    crop_framework = client['crop-framework']
    fasal_db = client.fasal
    data_builder = crop_framework['dataBuilder']
    gdd = fasal_db['dailyV2gdd']
    choice = st.sidebar.radio('Choose your requirement :',('GDD V2','Data Builder V2'))
    if choice == 'GDD V2':
        plotid = st.text_input('Please Enter the plotId :')
        gdd = pd.DataFrame(list(gdd.find({'plotId':plotid})))
        st.write(gdd)
    if choice == 'Data Builder V2':
        plotid = st.text_input('Please Enter the plotId :')
        gdd = pd.DataFrame(list(data_builder.find({'plotId':plotid})))
        st.write(gdd)

    # add_radio = st.radio.header(
    #     "Moving with data for",
    #     ("cropV2gdd", "dataBuilderData")
    # )
    # if add_radio == 'cropV2gdd':
    #     plotId = upload_plotId()
    #     st.write('xxx')
    #     data = gdd.find({'plotId':plot})
    #     st.write(data)
    #     download(data)

    # elif add_radio == 'dataBuilderData':
    #     plotId, plot_csv = upload_plotId_start_end_date()
    #     data = pd.DataFrame()
    #     if plot_csv is not None:
    #         for row in plot_csv.iterrows():
    #             start_date = row[1].start_date
    #             end_date = row[1].end_date
    #             plot = row[1].plotId
    #             try:
    #                 x = pd.DataFrame(list(data_builder.find({'plotId': plot, '$and': [
    #                                  {'date': {"$gte": str(start_date), "$lte": str(end_date)}}]})))
    #             except Exception as e:
    #                 st.write(f"{e,e.__traceback__.tb_lineno}")
    #             data = data.append(x)
    #     if plotId:
    #         data = pd.DataFrame(
    #             list(data_builder.find({'plotId': {"$in": plotId}})))
    #     st.dataframe(data)
    #     download(data)

def set_dark_mode():
    # Define custom CSS styles for dark mode
    dark_mode = """
        <style>
            body {
                background-color: #1a1a1a;
                color: #ffffff;
            }
            .st-df-header th {
                color: #ffffff;
                background-color: #333333;
            }
        </style>
    """
    st.markdown(dark_mode, unsafe_allow_html=True)

def get_sensor_data_new(id_wasp, sensor_list, date_1, date_2):
    """
    Function to extract sensor data for a sensor ID.

    id_wasp: str
        sensor ID for which data needs to be imputed.

    sensor_list: array
        list of sensor names for which data is required

    start_date: str
        date from which data is required in the format of 'YYYY-MM-DD'

    end_date: str😃
        date up to which data is required in the format of 'YYYY-MM-DD'

    """
    if type(id_wasp) == str:
        id_wasp = [id_wasp]
    date_1 = datetime.datetime(int(date_1.split('-')[0]),
                            int(date_1.split('-')[1]),
                            int(date_1.split('-')[2]))
    date_2 = datetime.datetime(int(date_2.split('-')[0]),
                            int(date_2.split('-')[1]),
                            int(date_2.split('-')[2]))

    df = pd.DataFrame(
        list(
            device.find({
                "id_wasp": {"$in":id_wasp},
                'sensor': {
                    "$in": sensor_list
                },
                "datetime": {
                    "$gte": date_1,
                    "$lte": date_2
                }
            })))
    try:
        df = df[['id_wasp', 'datetime', 'sensor', 'value']]


        dflist = pd.DataFrame()
        df['value'] = df['value'].apply(pd.to_numeric, errors='ignore')
        sensor = df.sensor.unique()
        sensor_list_map = {'RSSI':'mean','TC':'mean','HUM':'mean','SOILTC':'mean','ANE':'mean','PLV2':'sum','SOIL_B':'mean','SOIL_C':'mean','LDR':'mean','LW':'sum','LUX':'mean','BAT':'mean','PRES':'mean'}
        actual_sensor_list_map ={k: v for k, v in sensor_list_map.items() if k in sensor}
        for sensor in df.sensor.unique():
            df_sensor = df[(df['sensor'] == sensor)][[
                'id_wasp', 'value', 'datetime'
            ]].rename(columns={'value': sensor})
            # dflist = dflist.append(df_sensor)
            dflist = pd.concat([dflist, df_sensor], ignore_index = True)
        df_final =dflist
        df_final['datetime'] = pd.to_datetime(
        df_final['datetime']).dt.tz_localize('UTC').dt.tz_convert(
            'Asia/Kolkata')
        df_final['date'] = df_final['datetime'].dt.date
        df_final['hour'] = df_final['datetime'].dt.hour
        df_final = df_final.groupby(by=['id_wasp','date', 'hour'
                                    ]).agg(actual_sensor_list_map).reset_index()

        df_final.rename(columns={'id_wasp':'sensorId'},inplace=True)
        for value in sensor_list:
            if value in df_final.columns:

                if value == 'SOIL_B':
                    df_final['SOIL_B'] = (150940 - (df_final['SOIL_B'] * 19.74)) / (
                            (df_final['SOIL_B'] * 2.8875) - 137.5)
                if value == 'SOIL_C':
                    df_final['SOIL_C'] = (150940 - (df_final['SOIL_C'] * 19.74)) / (
                            (df_final['SOIL_C'] * 2.8875) - 137.5)
        df_final.sort_values(['date','hour'])
    except Exception as e:
        print(e)
        df_final = pd.DataFrame(columns=['date','hour','sensorId']+sensor_list)

    return df_final