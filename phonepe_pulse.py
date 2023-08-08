import git
import os
import json
import pandas as pd
import psycopg2
import requests
import streamlit as st
import plotly.express as px

st.set_page_config(page_title='PhonePe Pulse', page_icon=':bar_chart:', layout="wide")
st.markdown(f'<h1 style="text-align: center;">PhonePe Pulse Data Visualization \
            and Exploration</h1>', unsafe_allow_html=True)


def data_collection():
    try:
        git.Repo.clone_from("https://github.com/PhonePe/pulse.git", 'phonepe_pulse_git')
    except:
        pass


def data_insights():

    st.write('')
    st.write('PhonePe Pulse Data 2018-2022: Insights for India.')

    st.subheader('Aggregated Transaction:')
    st.write('Transaction data broken down by type of payments at state level.')
    st.write('- Recharge & bill payments')
    st.write('- Peer-to-peer payments')
    st.write('- Merchant payments')
    st.write('- Financial Services')
    st.write('- Others')

    st.subheader('Aggregated User:')
    st.write('Users data broken down by devices at state level.')
    col1,col2,col3,col4, col5, col6 = st.columns(6)
    with col1:
        st.write(':small_blue_diamond: Apple')
        st.write(':small_blue_diamond: Asus')
        st.write(':small_blue_diamond: Coolpad')
        st.write(':small_blue_diamond: Gionee')
        st.write(':small_blue_diamond: HMD Global')
    with col2:
        st.write(':small_blue_diamond: Huawei')
        st.write(':small_blue_diamond: Infinix')
        st.write(':small_blue_diamond: Lava')
        st.write(':small_blue_diamond: Lenovo')
        st.write(':small_blue_diamond: Lyf')
    with col3:
        st.write(':small_blue_diamond: Micromax')
        st.write(':small_blue_diamond: Motorola')
        st.write(':small_blue_diamond: OnePlus')
        st.write(':small_blue_diamond: Oppo')
        st.write(':small_blue_diamond: Realme')
    with col4:
        st.write(':small_blue_diamond: Samsung')
        st.write(':small_blue_diamond: Tecno')
        st.write(':small_blue_diamond: Vivo')
        st.write(':small_blue_diamond: Xiaomi')
        st.write(':small_blue_diamond: Others')

    st.subheader('Map Transaction:')
    st.write('- Total number of transactions at the state / district level.')
    st.write('- Total value of all transactions at the state / district level.')

    st.subheader('Map User:')
    st.write('- Total number of registered users at the state / district level.')
    st.write('- Total number of app opens by these registered users at the state / district level.')

    st.subheader('Top Transaction:')
    st.write('Explore the most number of the transactions happened for a selected Year-Quarter combination')
    st.write('- Top 10 Districts')
    st.write('- Top 10 Pin Codes')

    st.subheader('Top User:')
    st.write('Explore the most number of registered users for a selected Year-Quarter combination')
    st.write('- Top 10 Districts')
    st.write('- Top 10 Pin Codes')


class data_extraction:

    def aggregated_transaction():
        path = "phonepe_pulse_git/data/aggregated/transaction/country/india/state/"
        agg_state_list = os.listdir(path)

        data = {'State': [], 'Year': [], 'Quater': [], 'Transaction_type': [],
                'Transaction_count': [], 'Transaction_amount': []}

        for i in agg_state_list:
            path_i = path + i + '/'                 # india/state/delhi/
            agg_year_list = os.listdir(path_i)      # 2018,2019,2020,2021,2022

            for j in agg_year_list:
                path_j = path_i + j + '/'           # india/state/delhi/2018/
                agg_year_json = os.listdir(path_j)  # 1.json,2.json,3.json,4.json

                for k in agg_year_json:
                    path_k = path_j + k             # india/state/delhi/2018/1.json
                    f = open(path_k, 'r')
                    d = json.load(f)

                    for z in d['data']['transactionData']:
                        name = z['name']
                        count = z['paymentInstruments'][0]['count']
                        amount = z['paymentInstruments'][0]['amount']

                        data['State'].append(i)
                        data['Year'].append(int(j))
                        data['Quater'].append(int(k[0]))
                        data['Transaction_type'].append(name)
                        data['Transaction_count'].append(count)
                        data['Transaction_amount'].append(amount)

        return data

    def aggregated_user():
        path = "phonepe_pulse_git/data/aggregated/user/country/india/state/"
        agg_state_list = os.listdir(path)

        data = {'State': [], 'Year': [], 'Quater': [], 'User_brand': [], 'User_count': [], 'User_percentage': []}

        for i in agg_state_list:
            path_i = path + i + '/'
            agg_year_list = os.listdir(path_i)

            for j in agg_year_list:
                path_j = path_i + j + '/'
                agg_year_json = os.listdir(path_j)

                for k in agg_year_json:
                    path_k = path_j + k
                    f = open(path_k, 'r')
                    d = json.load(f)
                    
                    try:
                        for z in d['data']['usersByDevice']:
                            brand = z['brand']
                            count = z['count']
                            percentage = z['percentage']*100

                            data['State'].append(i)
                            data['Year'].append(int(j))
                            data['Quater'].append(int(k[0]))
                            data['User_brand'].append(brand)
                            data['User_count'].append(count)
                            data['User_percentage'].append(percentage)
                    except:
                        pass
                        
        return data

    def map_transaction():
        path = "phonepe_pulse_git/data/map/transaction/hover/country/india/state/"
        agg_state_list = os.listdir(path)

        data = {'State': [], 'Year': [], 'Quater': [], 'District': [],
                'Transaction_count': [], 'Transaction_amount': []}

        for i in agg_state_list:
            path_i = path + i + '/'                 
            agg_year_list = os.listdir(path_i)      
            for j in agg_year_list:
                path_j = path_i + j + '/'          
                agg_year_json = os.listdir(path_j)  
                for k in agg_year_json:
                    path_k = path_j + k             
                    f = open(path_k, 'r')
                    d = json.load(f)

                    for z in d['data']['hoverDataList']:
                        district = z['name'].split(' district')[0]
                        count = z['metric'][0]['count']
                        amount = z['metric'][0]['amount']

                        data['State'].append(i)
                        data['Year'].append(int(j))
                        data['Quater'].append(int(k[0]))
                        data['District'].append(district)
                        data['Transaction_count'].append(count)
                        data['Transaction_amount'].append(amount)

        return data

    def map_user():
        path = "phonepe_pulse_git/data/map/user/hover/country/india/state/"
        agg_state_list = os.listdir(path)

        data = {'State': [], 'Year': [], 'Quater': [], 'District': [], 'Registered_user': [], 'App_opens': []}

        for i in agg_state_list:
            path_i = path + i + '/'
            agg_year_list = os.listdir(path_i)

            for j in agg_year_list:
                path_j = path_i + j + '/'
                agg_year_json = os.listdir(path_j)

                for k in agg_year_json:
                    path_k = path_j + k
                    f = open(path_k, 'r')
                    d = json.load(f)

                    for z_key, z_value in d['data']['hoverData'].items():
                        district = z_key.split(' district')[0]
                        reg_user = z_value['registeredUsers']
                        app_opens = z_value['appOpens']

                        data['State'].append(i)
                        data['Year'].append(int(j))
                        data['Quater'].append(int(k[0]))
                        data['District'].append(district)
                        data['Registered_user'].append(reg_user)
                        data['App_opens'].append(app_opens)

        return data

    def top_transaction_district():
        path = "phonepe_pulse_git/data/top/transaction/country/india/state/"
        agg_state_list = os.listdir(path)

        data = {'State': [], 'Year': [], 'Quater': [], 'District': [],
                'Transaction_count': [], 'Transaction_amount': []}

        for i in agg_state_list:
            path_i = path + i + '/'                 
            agg_year_list = os.listdir(path_i)      

            for j in agg_year_list:
                path_j = path_i + j + '/'           
                agg_year_json = os.listdir(path_j)  

                for k in agg_year_json:
                    path_k = path_j + k             
                    f = open(path_k, 'r')
                    d = json.load(f)

                    for z in d['data']['districts']:
                        district = z['entityName']
                        count = z['metric']['count']
                        amount = z['metric']['amount']

                        data['State'].append(i)
                        data['Year'].append(int(j))
                        data['Quater'].append(int(k[0]))
                        data['District'].append(district)
                        data['Transaction_count'].append(count)
                        data['Transaction_amount'].append(amount)                        

        return data

    def find_district(pincode):
        url = "https://api.postalpincode.in/pincode/"
        response = requests.get(url + pincode)
        data = json.loads(response.text)
        district = data[0]['PostOffice'][0]['District']
        return district

    def top_transaction_pincode():
        path = "phonepe_pulse_git/data/top/transaction/country/india/state/"
        agg_state_list = os.listdir(path)

        data = {'State': [], 'Year': [], 'Quater': [], 'Pincode': [], 
                'Transaction_count': [], 'Transaction_amount': []}

        for i in agg_state_list:
            path_i = path + i + '/'                 
            agg_year_list = os.listdir(path_i)      

            for j in agg_year_list:
                path_j = path_i + j + '/'           
                agg_year_json = os.listdir(path_j)  

                for k in agg_year_json:
                    path_k = path_j + k             
                    f = open(path_k, 'r')
                    d = json.load(f)

                    for z in d['data']['pincodes']:
                        pincode = z['entityName']
                        count = z['metric']['count']
                        amount = z['metric']['amount']

                        data['State'].append(i)
                        data['Year'].append(int(j))
                        data['Quater'].append(int(k[0]))
                        data['Pincode'].append(pincode)
                        data['Transaction_count'].append(count)
                        data['Transaction_amount'].append(amount)                        

        return data

    def top_user_district():
        path = "phonepe_pulse_git/data/top/user/country/india/state/"
        agg_state_list = os.listdir(path)

        data = {'State': [], 'Year': [], 'Quater': [], 'District': [], 'Registered_user': []}

        for i in agg_state_list:
            path_i = path + i + '/'                 
            agg_year_list = os.listdir(path_i)      

            for j in agg_year_list:
                path_j = path_i + j + '/'           
                agg_year_json = os.listdir(path_j)  

                for k in agg_year_json:
                    path_k = path_j + k             
                    f = open(path_k, 'r')
                    d = json.load(f)

                    for z in d['data']['districts']:
                        district = z['name']
                        reg_user = z['registeredUsers']

                        data['State'].append(i)
                        data['Year'].append(int(j))
                        data['Quater'].append(int(k[0]))
                        data['District'].append(district)
                        data['Registered_user'].append(reg_user)                       

        return data

    def top_user_pincode():
        path = "phonepe_pulse_git/data/top/user/country/india/state/"
        agg_state_list = os.listdir(path)

        data = {'State': [], 'Year': [], 'Quater': [], 'Pincode': [], 'Registered_user': []}

        for i in agg_state_list:
            path_i = path + i + '/'                 
            agg_year_list = os.listdir(path_i)      

            for j in agg_year_list:
                path_j = path_i + j + '/'           
                agg_year_json = os.listdir(path_j)  

                for k in agg_year_json:
                    path_k = path_j + k             
                    f = open(path_k, 'r')
                    d = json.load(f)

                    for z in d['data']['pincodes']:
                        pincode = z['name']
                        reg_user = z['registeredUsers']

                        data['State'].append(i)
                        data['Year'].append(int(j))
                        data['Quater'].append(int(k[0]))
                        data['Pincode'].append(pincode)
                        data['Registered_user'].append(reg_user)                       

        return data


class data_transform:
    # pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)

    aggregated_transaction = pd.DataFrame(data_extraction.aggregated_transaction())
    aggregated_user = pd.DataFrame(data_extraction.aggregated_user())
    map_transaction = pd.DataFrame(data_extraction.map_transaction())
    map_user = pd.DataFrame(data_extraction.map_user())
    top_transaction_district = pd.DataFrame(data_extraction.top_transaction_district())
    top_transaction_pincode = pd.DataFrame(data_extraction.top_transaction_pincode())
    top_user_district = pd.DataFrame(data_extraction.top_user_district())
    top_user_pincode = pd.DataFrame(data_extraction.top_user_pincode())


class data_load:

    def sql_table_creation():
        gopi = psycopg2.connect(host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute("create table if not exists aggregated_transaction(\
                            state				varchar(255),\
                            year				int,\
                            quater				int,\
                            transaction_type	varchar(255),\
                            transaction_count	int,\
                            transaction_amount	float);\
                        create table if not exists aggregated_user(\
                            state				varchar(255),\
                            year				int,\
                            quater				int,\
                            user_brand			varchar(255),\
                            user_count			int,\
                            user_percentage		float);\
                        create table if not exists map_transaction(\
                            state				varchar(255),\
                            year				int,\
                            quater				int,\
                            district			varchar(255),\
                            transaction_count	int,\
                            transaction_amount	float);\
                        create table if not exists map_user(\
                            state				varchar(255),\
                            year				int,\
                            quater				int,\
                            district			varchar(255),\
                            registered_user		int,\
                            app_opens			int);\
                        create table if not exists top_transaction_district(\
                            state				varchar(255),\
                            year				int,\
                            quater				int,\
                            district			varchar(255),\
                            transaction_count	int,\
                            transaction_amount	float);	\
                        create table if not exists top_transaction_pincode(\
                            state				varchar(255),\
                            year				int,\
                            quater				int,\
                            pincode				varchar(255),\
                            transaction_count	int,\
                            transaction_amount	float);\
                        create table if not exists top_user_district(\
                            state				varchar(255),\
                            year				int,\
                            quater				int,\
                            district			varchar(255),\
                            registered_user		int);\
                        create table if not exists top_user_pincode(\
                            state				varchar(255),\
                            year				int,\
                            quater				int,\
                            pincode				varchar(255),\
                            registered_user		int);")
        gopi.commit()
        gopi.close()

    def data_migration():
        gopi = psycopg2.connect(host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()

        cursor.execute("delete from aggregated_transaction;\
                        delete from aggregated_user;\
                        delete from map_transaction;\
                        delete from map_user;\
                        delete from top_transaction_district;\
                        delete from top_transaction_pincode;\
                        delete from top_user_district;\
                        delete from top_user_pincode;")
        gopi.commit()

        cursor.executemany("insert into aggregated_transaction(state, year, quater, transaction_type,\
                            transaction_count, transaction_amount)\
                           values(%s,%s,%s,%s,%s,%s)", data_transform.aggregated_transaction.values.tolist())
        
        cursor.executemany("insert into aggregated_user(state, year, quater, user_brand,\
                            user_count, user_percentage)\
                           values(%s,%s,%s,%s,%s,%s)", data_transform.aggregated_user.values.tolist())
        
        cursor.executemany("insert into map_transaction(state, year, quater, district,\
                            transaction_count, transaction_amount)\
                           values(%s,%s,%s,%s,%s,%s)", data_transform.map_transaction.values.tolist())
        
        cursor.executemany("insert into map_user(state, year, quater, district,\
                            registered_user, app_opens)\
                           values(%s,%s,%s,%s,%s,%s)", data_transform.map_user.values.tolist())
        
        cursor.executemany("insert into top_transaction_district(state, year, quater, district,\
                            transaction_count, transaction_amount)\
                           values(%s,%s,%s,%s,%s,%s)", data_transform.top_transaction_district.values.tolist())
        
        cursor.executemany("insert into top_transaction_pincode(state, year, quater, pincode,\
                            transaction_count, transaction_amount)\
                           values(%s,%s,%s,%s,%s,%s)", data_transform.top_transaction_pincode.values.tolist())
        
        cursor.executemany("insert into top_user_district(state, year, quater, district,registered_user)\
                           values(%s,%s,%s,%s,%s)", data_transform.top_user_district.values.tolist())
        
        cursor.executemany("insert into top_user_pincode(state, year, quater, pincode,registered_user)\
                           values(%s,%s,%s,%s,%s)", data_transform.top_user_pincode.values.tolist())
        
        gopi.commit()
        gopi.close()


# st.title('Phonepe Pulse Data Visualization and Exploration')
st.subheader('Please select the option below:')
st.code('1 - Data Collection')
st.code('2 - Data Insights')
st.code('3 - Migrating Data to SQL Database')
st.code('4 - Data Analysis')
st.code('5 - Exit')

list_options = ['Select one', 'Data Collection', 'Data Insights',
                'Migrating Data to SQL Database', 'Data Analysis', 'Exit']
option = st.selectbox('', list_options)

if option:
    if option == 'Data Collection':
        data_collection()
        st.success('Data successfully cloned from the PhonePe Pulse Git repository')
        st.balloons()

    elif option == 'Data Insights':
        data_insights()

    elif option == 'Migrating Data to SQL Database':
        data_load.sql_table_creation()
        data_load.data_migration()
        st.success('Data successfully Migrated to the SQL Database')
        st.balloons()
    
    elif option == 'Data Analysis':
        pass

    elif option == 'Exit':
        st.success('Thank you for your time. Exiting the application')
        st.balloons()

