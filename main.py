from streamlit_pandas_profiling import st_profile_report
import openai
from calculation import *
from Data_collection_methods import *
import logging
import os
import logging.config
import yaml
import sys

set_dark_mode()
with open('Automation/logger.yml', 'r') as f:
            config = yaml.safe_load(f.read())
            logging.config.dictConfig(config)

logger = logging.getLogger(__name__)

logger.info("Starting the dataRetrieval tool")
st.title('Data Retrieval')

choice = st.sidebar.radio("Select the required one from here",
                          ('Customerwise Data','Weekly Data','Sensor Data using plotId',
                          'Sensor or forecast data using lat_long (3rdparty)',
                           'Weather forecast Data', 'Analyse your dataset',
                           'CropFrameworkV2 Data'))
try:
    if choice == 'Customerwise Data':
        x = list(db.customer.find({},{'name':1,'_id':1}))
        name = [name['name'] for name in x ]
        id = [id['_id'] for id in x ]
        name_id = dict(zip(name, id))
        customer_name = st.selectbox('choose your customer', name)
        customer_id = name_id.get(customer_name)
        plot_names = [name['name'] for name in list(plot.find({'customer':customer_id},{'name':1,'_id':0}))]
        plot_choice = st.sidebar.radio('Select the One',('Choose your plot','all plot'))
        if plot_choice == 'Choose your plot':
            plot_name_sel = st.selectbox('Choose your plot', plot_names)
            plot_id_sel = plot.find_one({'name':plot_name_sel},{'name':0,'_id':1}).get('_id')
            start_date = str(st.date_input('Data From'))
            end_date = str(st.date_input('Data Till'))        
            st.write(f'data for plotId : {plot_id_sel} from {start_date} to {end_date}')
            hourly, daily = data_plotwise(plot_id_sel, start_date, end_date)
        if plot_choice == 'all plot':      
            hourly, daily = data_customerwise(customer_id)
    if choice == "Weekly Data":
        x = list(db.customer.find({},{'name':1,'_id':1}))
        name = [name['name'] for name in x ]
        id = [id['_id'] for id in x ]
        name_id = dict(zip(name, id))
        customer_name = st.selectbox('choose your customer', name)
        customer_id = name_id.get(customer_name)
        plot_names = [name['name'] for name in list(plot.find({'customer':customer_id},{'name':1,'_id':0}))]
        plot_choice = st.sidebar.radio('Select the One',('Choose your plot','all plot'))
        hourly = pd.DataFrame()
        if plot_choice == 'Choose your plot':
            plot_name_sel = st.selectbox('Choose your plot', plot_names)
            plot_id_sel = plot.find_one({'name':plot_name_sel},{'name':0,'_id':1}).get('_id')
            start_date = str(st.date_input('Data From'))
            end_date = str(st.date_input('Data Till'))        
            st.write(f'data for plotId : {plot_id_sel} from {start_date} to {end_date}')
            hourly, daily = data_plotwise(plot_id_sel, start_date, end_date)
        if plot_choice == 'all plot':      
            hourly, daily = data_customerwise(customer_id)
        weekly = weekly_data(hourly)
        st.write('weekly data: ', weekly)
    if choice == 'Sensor Data crop specific':
        sensor_data_calc(choice)
    elif choice == 'Sensor Data using plotId':
        sensor_data_calc(choice)
    elif choice == 'Analyse your dataset':
        data_analysis()
        # report = data_analyzer()
        # st_profile_report(report)
    elif choice == 'Sensor or forecast data using lat_long (3rdparty)':
        get_api_data_calc()
        
    elif choice == 'Weather forecast Data':
        weather_forecast_calc(choice)

    elif choice == 'GDD calculation' or choice =='GDD calculation using plotId':
        gdd_calc(choice)
    elif choice == 'Query with ChatGPT':
        chatgpt_query()
    elif choice == 'CropFrameworkV2 Data':
        crop_v2_data_extraction()
except Exception as e:
    
    exc_type, exc_obj, exc_tb = sys.exc_info()
    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
    logger.error(f"function {fname}, line no: {e.__traceback__.tb_lineno},error: {e}")


def hourly_data_customerwise_plot():
    """houly data clockwise
    """
    st.write('fkdvjfksjd')
    name, id, name_id = all_customer()
    customer_name = st.selectbox('choose your customer', name)
    customer_id = name_id.get(customer_name)
    plot_names = list(plot.find({'customer':customer_id}))
    plot_name = st.selectbox('Choose your plot', plot_names)




