from Data_collection_methods import *
import streamlit as st
import seaborn as sns
import matplotlib.pyplot as plt

def sensor_data_calc(choice):
    if choice == 'Sensor Data crop specific':
        df_all,crop_list,sensor_list,date_1,date_2 = initial_input_crop_specific()
        sensor = ['LUX','PRES','SOIL_B','SOIL_C']
        sensor1 = [x for x in sensor if x not in sensor_list]
        if len(sensor) >0:
            data_without_lux = df_all
        else:
            data_without_lux = df_all[sensor1]
        fig = plt.figure(figsize=(10, 4))
        st.write('BoxenPlot: an advancement of boxplots, designed to visualize distributions more accurately')
        sns.boxenplot(data=data_without_lux)
        st.pyplot(fig)

        try:
            df_all = df_all.drop(['type', 'plotName', 'stateCode'], axis=1)
        except Exception as e:
            logger.info(f'warning : {e}')

        imputation =st.sidebar.radio('want data imputation to fill missing houly data',('No','Yes'))
        if imputation == 'No':
            pass
        else :
            st.write('length of current data',len(df_all))
            merged = empty_df(df_all)
            imputed = data_imputation(merged,sensor_list)
            st.write('len of data after imputation',len(imputed))
            st.write('Imputed data',imputed)
            df_all = imputed
            csv = convert_df(imputed)
            st.download_button("Press to Download the Imputed data",
                            csv,
                            "file.csv",
                            "text/csv",
                            key='download-imputed data')
        
        data_1 = filter_raw(df_all, sensor_list, date_1, date_2)
        st.write('length of filtered data based on selection :',len(data_1))
        st.write('data getting based on your selection',data_1)

        data_1 = convert_df(data_1)

        st.download_button("Press to Download the sensor data",
                           data_1,
                           "file.csv",
                           "text/csv",
                           key='download-csv')
        gdd_select = st.sidebar.radio('GDD_calc old',('No','Yes'))
        if gdd_select == 'Yes':
            if 'TC' in df_all.columns:
                columns = {'TC':['min','max','mean']}
                temp_data = df_all.groupby(['sensorId','date']).agg(columns).reset_index()
                temp_data.columns = [' '.join(col).strip() for col in temp_data.columns.values]
                try:
                    sen_plot = dict(zip(df_all['sensorId'].values,df_all['plotId'].values))
                    temp_data['plotId'] = temp_data['sensorId'].map(sen_plot)
                
                except:
                    pass
                base_temp = float(st.text_input(f"Enter the base temparature for {crop_list}"))
                temp_data['GDD'] =((temp_data['TC max'] + temp_data['TC min']) / 2) - base_temp
                st.dataframe(temp_data)
                temp_data = convert_df(temp_data)

                st.download_button("Press to Download the GDD data",
                                temp_data,
                                "file.csv",
                                "text/csv",
                                key='download-csv')
                
            else:
                st.write('Select TC as sensor, temparature is required to calculate GDD')
  
        
    if choice == 'Sensor Data using plotId':
        df_all_1, sensor_list, date_1, date_2 = get_sen_data_pid()

        st.write('Data for these customers:',
                 df_all_1['plotId'].value_counts())
        # choice =st.sidebar.radio('want data imputation to fill missing houly data',('No','Yes'))
        # if choice == 'No':
        #     data_1 = filter_raw(df_all_1, sensor_list, date_1, date_2)
        #     st.write('length of filtered data based on selection :',len(data_1))
        #     st.write(' data based on current selection',data_1)
            
        #     csv = convert_df(data_1)
        #     st.download_button("Press to Download the sensor data based on your selection",
        #                     csv,
        #                     "file.csv",
        #                     "text/csv",
        #                     key='download-csv')
        # if choice == 'Yes':
        #     st.write('length of current data',len(df_all_1))
        #     merged = empty_df(df_all_1)
        #     imputed = data_imputation(merged,sensor_list)
        #     st.write('len of data after imputation',len(imputed))
        #     st.write('Imputed data',imputed)
        #     csv = convert_df(imputed)
        #     st.download_button("Press to Download the Imputed data",
        #                     csv,
        #                     "file.csv",
        #                     "text/csv",
        #                     key='download-csv')
        #     data_1 = filter_raw(imputed, sensor_list, date_1, date_2)
        #     st.write('data based on current selection',data_1)
        #     csv = convert_df(data_1)
        #     st.download_button("Press to Download the data based on your selection",
        #                     csv,
        #                     "file.csv",
        #                     "text/csv",
        #                     key='download')
        gdd_select = st.sidebar.radio('GDD_calc old',('No','Yes'))
        if gdd_select == 'Yes':
            data_1 = df_all_1.copy()
            if 'TC' in data_1.columns:
                columns = {'TC':['min','max','mean']}
                temp_data = data_1.groupby(['sensorId','date']).agg(columns).reset_index()
                temp_data.columns = [' '.join(col).strip() for col in temp_data.columns.values]
                try:
                    sen_plot = dict(zip(data_1['sensorId'].values,data_1['plotId'].values))
                    temp_data['plotId'] = temp_data['sensorId'].map(sen_plot)
                except:
                    pass
                try:
                    base_temp = float(st.text_input(f"Enter the base temparature"))
                except:
                    st.write('Please enter a integer or float value')
                temp_data['GDD'] =((temp_data['TC max'] + temp_data['TC min']) / 2) - base_temp
                st.dataframe(temp_data)
                temp_data = convert_df(temp_data)

                st.download_button("Press to Download the GDD data",
                                temp_data,
                                "file.csv",
                                "text/csv",
                                key='download gdd')
            else:
                st.write('Select TC as sensor, canopy temparature(TC) is required to calculate GDD')
            


def weather_forecast_calc(choice):
    if choice == 'Weather forecast Data':

        forecast_data = pd.DataFrame()
        # plot_choice = st.sidebar.radio(
        #     'Enter plotId', ('One by one', 'upload a csv having plotIds'))

        # if plot_choice == 'One by one':
        #     cust_count = int(
        #         st.text_input(
        #             'Provide the number of customers for which you want to see the forecasted data:'
        #         ))
        #     pid = []
        #     for i in range(cust_count):

        #         p_id = st.text_input(
        #             f'Provide the plot Id for customer {i+1}:')
        #         pid.append(p_id)
        # if plot_choice == 'upload a csv having plotIds':
        #     x = st.sidebar.file_uploader(label="Upload your dataset")
        #     try:
        #         if x is not None:
        #             plotids = pd.read_csv(x)
        #             pid = plotids['plotId'].unique()
        #             pid = list(pid.values)
        #     except:
        #         st.write('file should must have plotId column and in CSV only')
        plot_id = st.text_input('Enter your plot Id:')
        array = list(range(1,14))
        days = st.selectbox('for how many days, forecast data you want(1-14) 1 is today, 2 is till tomorrow..?',array)
        # DATE_1 = datetime.datetime.today()
        # DATE_2 = DATE_1 + timedelta(1)
        # DATE_1 = str(DATE_1.date())
        # DATE_2 = str(DATE_2.date())
        data = pd.DataFrame()
        for day in range(days):
            try:
                forecast_data = get_weather_forecast_data_streamlit(str(plot_id),day)
                data = pd.concat([data, forecast_data], ignore_index= True)
            except Exception as e:
                st.write(f'not getting data for forecast_day{day} for plotId {plot_id}, error : {e}')
        # forecast_data = get_weather_forecast_data_streamlit(plot_id, 13)
        st.write(data)

        # csv = convert_df(forecast_data)

        # st.download_button("Press to Download the forecast data",
        #                    csv,
        #                    "file.csv",
        #                    "text/csv",
        #                    key='download-forecast data')


def gdd_calc(choice):
    if choice == 'GDD calculation':
        crop_list, sensor_list, date_1, date_2 = initial_input()
        df_all = get_plot_crop_1(crop_list, sensor_list, date_1, date_2)

        st.write('Data for these customers:',
                 df_all['customerName'].value_counts())
        a = st.sidebar.radio('You want to add or remove some customer',
                             ('No', 'Add', 'Remove'))
        if a == 'Add':
            data_1 = add_customer(df_all, sensor_list, date_1, date_2)
            st.write('All the customers value counts',
                     data_1['customerName'].value_counts())
            st.write(data_1)

        if a == 'No':
            data_1 = df_all
        if a == 'Remove':
            data_1 = remove_customer(df_all)
            st.write('After removing customer, row value counts  ',
                     data_1['customerName'].value_counts())
        data_1 = data_1.groupby([
            'customerName', 'type', 'plotId', 'plotName', 'stateCode',
            'sensorId', 'date'
        ]).agg({
            'TC': ['max', 'min'],
            'HUM': 'mean',
            'ANE': 'mean',
            'PLV2': 'sum'
        }).reset_index()
        data_1.columns = [
            ' '.join(col).strip() for col in data_1.columns.values
        ]
        base = st.number_input(
            f'Enter the base temperature for the {crop_list}')
        data_1['GDD'] = ((data_1['TC max'] + data_1['TC min']) / 2) - base
        st.table(data_1.head())

        csv = convert_df(data_1)

        st.download_button("Press to Download the GDD data",
                           csv,
                           "file.csv",
                           "text/csv",
                           key='download-csv')

    if choice == 'GDD calculation using plotId':
        df_all_1, sensor_list, date_1, date_2 = get_sen_data_pid()
        st.write('Data for these customers:',
                 df_all_1['plotId'].value_counts())
        data_1 = filter_raw(df_all_1, sensor_list, date_1, date_2)
        st.table(data_1.head())
        data_1 = filter_raw(df_all_1, sensor_list, date_1, date_2)
        st.table(data_1.head())

        data_1 = data_1.groupby(['plotId', 'date']).agg({
            'TC': ['max', 'min']
        }).reset_index()
        data_1.columns = [
            ' '.join(col).strip() for col in data_1.columns.values
        ]
        base = st.number_input(f'Enter the base temperature :')
        data_1['GDD'] = ((data_1['TC max'] + data_1['TC min']) / 2) - base
        st.table(data_1.head())

        csv = convert_df(data_1)

        st.download_button("Press to Download the GDD data",
                           csv,
                           "file.csv",
                           "text/csv",
                           key='download-csv')

def get_api_data_calc():
    """get the data from 3rdParty API using latitude and longitude"""
    startDate = st.date_input('Enter startDate')
    startDate = str(pd.to_datetime(startDate).date())
    endDate = st.date_input('Enter endDate')
    endDate = str(pd.to_datetime(endDate).date())
    if startDate >= endDate:
        st.write('startDate should be less than endDate')
    
    lat = st.text_input('Enter latitude')
    lon = st.text_input('Enter longitude')
    api_data = get_3rdparty_data(lat,lon,startDate,endDate)
    st.dataframe(api_data)
    csv = convert_df(api_data)

    st.download_button("click to Download the data",
                    csv,
                    "file.csv",
                    "text/csv",
                    key='download-csv')


def data_plotwise(plot_id, start_date, end_date):
    sensor_list = [
                'TC', 'HUM', 'ANE', 'PLV2', 'LW', 'LUX', 'SOILTC', 'SOIL_B', 'SOIL_C',
                'LDR','PRES'
            ]
    # evapotranspirationActual = db['evapotranspirationActual']
    # actualVPD = db['actualVPD']
    
    
    def get_custid_using_plotid(plot_id):
        return plot.find_one({'_id':plot_id},{'customer':1}).get('customer')
    
    def get_plotids_using_customerid(customerId):
        customer_data = list(plot.find({'customer':customerId, 'isActive':True}))
        plotids = [x['_id'] for x in customer_data]
        sensorIds = [x['sensorId'] for x in customer_data]
        # sensorNodes = [x['sensorNodes'][0] for x in customer_data]
        plotName = [x['name'] for x in customer_data]
        farmId = [x['farmId'] for x in customer_data]
        return plotids,sensorIds,plotName, farmId
    
    def get_sensor_id(plot_id):
        sensor_id = list(plot.find({'_id':plot_id}))
        sensor_id = [d['sensorId'] for d in sensor_id if 'sensorId' in d]
        return sensor_id[0]
    
    customer_id = get_custid_using_plotid(plot_id)
    plotids,sensorIds,plotName, farmId = get_plotids_using_customerid(customer_id)
    plot_name = dict(zip(plotids, plotName))
    plotId_10 = [plot_id]
    dailyDataDf_ = pd.DataFrame()
    for _ in plotId_10:
        dailyDataDf_ = pd.DataFrame()
        hourly_data = pd.DataFrame()
        sensorId = get_sensor_id(_)

        try:
            start_date_1 = str((pd.to_datetime(start_date) - timedelta(1)).date())
            x = get_sensor_data_new(sensorId, sensor_list, start_date_1, end_date)
            x['date'] = x['date'].apply(lambda x: str(x))
            x = x[x['date']>= start_date]
            x['plotId'] = _
            x['plotName'] = x['plotId'].map(plot_name)
            st.write('hourly data', x)
            # download_csv(x, 'download hourly data')
            # hourly_data['plotName'] = hourly_data['plotId'].map(plot_name)
            # sensor_list_map = {'vpd':'mean','evapoTranspiration':'mean','RSSI':'mean','TC':['mean','min','max'],'HUM':['mean','min','max'],'SOILTC':'mean',
            #                   'temp_c':'mean','humidity':'mean','precip_mm':'mean','wind_kph':'mean',
            #                   'ANE':'mean','PLV2':'sum','SOIL_B':'mean','SOIL_C':'mean','LW':'sum','LUX':'mean','LDR':'mean','PRES':'mean'}
            sensor_list_map = {'vpd':'mean','evapoTranspiration':'mean','RSSI':'mean','TC':['mean','min','max'],'HUM':['mean','min','max'],'SOILTC':['mean','min','max'],
                            'temp_c':['mean','min','max'],'humidity':['mean','min','max'],'precip_mm':['mean','min','max'],'wind_kph':['mean','min','max'],
                            'ANE':['mean','min','max'],'PLV2':['mean','min','max','sum'],'SOIL_B':['mean','min','max'],'SOIL_C':['mean','min','max'],'LW':['mean','min','max'],'LUX':['mean','min','max'],'LDR':['mean','min','max'],'PRES':['mean','min','max']}

            dic ={k: v for k, v in sensor_list_map.items() if k in x.columns}
            dailyDataDf = x.groupby(['plotId','date']).agg(dic).reset_index()
            dailyDataDf.columns = [''.join(col).strip() for col in dailyDataDf.columns.values]
            dailyDataDf['plotName'] = dailyDataDf['plotId'].map(plot_name)
            st.write('daily data',dailyDataDf)
            # download_csv(dailyDataDf, 'download daily avg data')
        except Exception as e:
            print(e,_)
    return x, dailyDataDf
            
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

def download_csv(x, text):
    if "uploader_key" not in st.session_state:
        st.session_state.uploader_key = 0

    uploaded_file = st.file_uploader(
        x, type="csv", key=f"uploader_{st.session_state.uploader_key}"
    )


    def update_key():
        st.session_state.uploader_key += 1


    if uploaded_file is not None:
        st.download_button(
            text,
            data=uploaded_file,
            file_name="uploaded_file.csv",
            mime="text/csv",
            on_click=update_key,
        )

def data_customerwise(customer_id):
    start_date = str(st.date_input('Data From'))
    end_date = str(st.date_input('Data Till'))  
    sensor_list = [
                'TC', 'HUM', 'ANE', 'PLV2', 'LW', 'LUX', 'SOILTC', 'SOIL_B', 'SOIL_C',
                'LDR','PRES'
            ]
    def get_sensor_id(plot_id):
        sensor_id = list(plot.find({'_id':plot_id}))
        sensor_id = [d['sensorId'] for d in sensor_id if 'sensorId' in d]
        return sensor_id[0]
    def get_plotids_using_customerid(customerId):
        customer_data = list(plot.find({'customer':customerId, 'isActive':True}))
        plotids = [x['_id'] for x in customer_data]
        sensorIds = [x['sensorId'] for x in customer_data]
        # sensorNodes = [x['sensorNodes'][0] for x in customer_data]
        plotName = [x['name'] for x in customer_data]
        farmId = [x['farmId'] for x in customer_data]
        return plotids,sensorIds,plotName, farmId
    
    evapotranspirationActual = db['evapotranspirationActual']
    actualVPD = db['actualVPD']
    plotids,sensorIds,plotName, farmId = get_plotids_using_customerid(customer_id)
    plot_name = dict(zip(plotids, plotName))
    plotId_10 = plotids
    dailyDataDf = pd.DataFrame()
    hourly_data = pd.DataFrame()
    for _ in plotId_10:
        sensorId = get_sensor_id(_)
        dailyDataDf_ = pd.DataFrame()
        
        try:
            start_date_1 = str((pd.to_datetime(start_date) - timedelta(1)).date())
            x = get_sensor_data_new(sensorId, sensor_list, start_date_1, end_date)
            x['date'] = x['date'].apply(lambda x: str(x))
            x = x[x['date']>= start_date]
            x['plotId'] = _

            hourly_data = pd.concat([hourly_data, x ],ignore_index= True)
            # hourly_data['plotName'] = hourly_data['plotId'].map(plot_name)
            # sensor_list_map = {'vpd':'mean','evapoTranspiration':'mean','RSSI':'mean','TC':['mean','min','max'],'HUM':['mean','min','max'],'SOILTC':'mean',
            #                   'temp_c':'mean','humidity':'mean','precip_mm':'mean','wind_kph':'mean',
            #                   'ANE':'mean','PLV2':'sum','SOIL_B':'mean','SOIL_C':'mean','LW':'sum','LUX':'mean','LDR':'mean','PRES':'mean'}
            sensor_list_map = {'vpd':'mean','evapoTranspiration':'mean','RSSI':'mean','TC':['mean','min','max'],'HUM':['mean','min','max'],'SOILTC':['mean','min','max'],
                            'temp_c':['mean','min','max'],'humidity':['mean','min','max'],'precip_mm':['mean','min','max'],'wind_kph':['mean','min','max'],
                            'ANE':['mean','min','max'],'PLV2':['mean','min','max','sum'],'SOIL_B':['mean','min','max'],'SOIL_C':['mean','min','max'],'LW':['mean','min','max'],'LUX':['mean','min','max'],'LDR':['mean','min','max'],'PRES':['mean','min','max']}
            dic ={k: v for k, v in sensor_list_map.items() if k in hourly_data.columns}
            # hourly_data['plotName'] = hourly_data['plotId'].map(plot_name)
            # hourly_data_23 = pd.concat([hourly_data_23, hourly_data], ignore_index= True)
            dailyDataDf_ = hourly_data.groupby(['plotId','date']).agg(dic).reset_index()
            dailyDataDf_.columns = [''.join(col).strip() for col in dailyDataDf_.columns.values]
            dailyDataDf_['plotName'] = dailyDataDf_['plotId'].map(plot_name)
            dailyDataDf = pd.concat([dailyDataDf, dailyDataDf_],ignore_index=True)

        except Exception as e:
            print(e,_)
    st.write('visualise hourly data', hourly_data)
    # hourly_data.to_csv("/Users/prashunchauhan/Downloads/fasal/DataRetrieval/Automation/hourly_data.csv")
    # dailyDataDf.to_csv("/Users/prashunchauhan/Downloads/fasal/DataRetrieval/Automation/daily.csv")
    # download_csv(x, 'download hourly data')
    st.write('visualise daily data',dailyDataDf)
    # download_csv(dailyDataDf, 'download daily avg data')
    return hourly_data, dailyDataDf
    
def data_analysis():
    st.sidebar.header("Upload Your File")
    uploaded_file = st.sidebar.file_uploader("Upload a CSV or Excel file", type=["csv", "xlsx"])

    if uploaded_file:
        file_extension = uploaded_file.name.split(".")[-1]

        # Read Data Based on File Type
        if file_extension == "csv":
            df = pd.read_csv(uploaded_file)
        elif file_extension == "xlsx":
            df = pd.read_excel(uploaded_file)

        # Display Data
        st.subheader("📌 Uploaded Data Preview")
        st.write(df.head())

        # Handle Missing Values
        if st.checkbox("🛠 Handle Missing Values"):
            df.fillna(df.mean(), inplace=True)
            st.write("✔️ Missing values replaced with column mean.")

        # Remove Duplicates
        if st.checkbox("🗑 Remove Duplicates"):
            df.drop_duplicates(inplace=True)
            st.write("✔️ Duplicates removed.")

        # Data Summary
        st.subheader("📈 Data Summary")
        st.write(df.describe())

        # Column Selection
        numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
        selected_col = st.sidebar.selectbox("Select a Numeric Column for Analysis", numeric_cols)

        if selected_col:
            # Histogram
            st.subheader(f"📊 Histogram of {selected_col}")
            fig, ax = plt.subplots()
            sns.histplot(df[selected_col], bins=20, kde=True, ax=ax)
            st.pyplot(fig)

            # Boxplot
            st.subheader(f"📌 Boxplot of {selected_col}")
            fig, ax = plt.subplots()
            sns.boxplot(y=df[selected_col], ax=ax)
            st.pyplot(fig)

        # Pairplot
        if st.checkbox("📌 Show Pairplot"):
            st.subheader("📊 Pairplot of Numeric Columns")
            fig = sns.pairplot(df[numeric_cols])
            st.pyplot(fig)

        # Scatter Plot
        col_x = st.sidebar.selectbox("Select X-axis Column", numeric_cols)
        col_y = st.sidebar.selectbox("Select Y-axis Column", numeric_cols)

        if st.sidebar.button("Show Scatter Plot"):
            st.subheader(f"📍 Scatter Plot: {col_x} vs {col_y}")
            fig, ax = plt.subplots()
            sns.scatterplot(x=df[col_x], y=df[col_y], ax=ax)
            st.pyplot(fig)

        # Correlation Heatmap
        if st.checkbox("🔍 Show Correlation Heatmap"):
            st.subheader("📈 Correlation Heatmap")
            fig, ax = plt.subplots()
            sns.heatmap(df.corr(), annot=True, cmap="coolwarm", ax=ax)
            st.pyplot(fig)

        # # Data Filtering
        # st.sidebar.subheader("🔍 Filter Data")
        # filter_column = st.sidebar.selectbox("Select Column to Filter", df.columns)
        # unique_values = df[filter_column].unique()
        # selected_value = st.sidebar.selectbox(f"Filter {filter_column} by", unique_values)

        # filtered_data = df[df[filter_column] == selected_value]
        # st.subheader(f"📌 Filtered Data ({filter_column} = {selected_value})")
        # st.write(filtered_data.head())

        # Aggregate Data
        st.sidebar.subheader("📊 Aggregate Data")
        agg_column = st.sidebar.selectbox("Select Column for Aggregation", numeric_cols)
        agg_func = st.sidebar.selectbox("Choose Aggregation Function", ["Mean", "Sum", "Count"])

        if agg_func == "Mean":
            result = df.groupby(filter_column)[agg_column].mean()
        elif agg_func == "Sum":
            result = df.groupby(filter_column)[agg_column].sum()
        elif agg_func == "Count":
            result = df.groupby(filter_column)[agg_column].count()

        st.subheader(f"📊 Aggregated Data ({agg_func} of {agg_column})")
        st.write(result)

        # Download Processed Data
        st.subheader("⬇️ Download Processed Data")
        towrite = io.BytesIO()
        df.to_csv(towrite, index=False)
        towrite.seek(0)
        st.download_button(label="Download CSV", data=towrite, file_name="processed_data.csv", mime="text/csv")
        
def get_weather_forecast_data_streamlit(plot_id,day):
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
    weather_forecast_final_data = db2['weatherForecastFinalData']

    df = pd.DataFrame(
        list(
            weather_forecast_final_data.find({
                "plotId": plot_id,
                "date": str(datetime.datetime.now().date())
                }
            )))
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
            'temp_c'
        ]]
        df_1.columns = [
            'plotId', 'date', 'hour', 'HUM', 'ANE', 'PLV2', 'TC'
        ]
    else:
        df_1 = pd.DataFrame(columns = ['plotId', 'date', 'hour', 'HUM', 'ANE', 'PLV2', 'TC'])
    return df_1

    
def weekly_data(hourly_data):
    df = pd.DataFrame(hourly_data)
    df['timestamp'] = pd.to_datetime(df['date'] + ' ' + df['hour'].astype(str) + ':00')
    df.set_index('timestamp', inplace=True)
    df.drop(columns=['date', 'hour'], inplace=True)
    weekly_min = df.resample('W').min().add_suffix('_min')
    weekly_max = df.resample('W').max().add_suffix('_max')
    weekly_summary = pd.concat([weekly_min, weekly_max], axis=1)
    return weekly_summary
    