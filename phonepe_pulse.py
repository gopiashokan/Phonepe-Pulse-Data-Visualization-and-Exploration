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


def data_overview():

    st.write('')
    st.header('PhonePe Pulse Data: Insights for India')
    st.write('')

    st.subheader('Key Dimensions:')
    st.write('- State - All States in India')
    st.write('- Year -  2018 to 2022')
    st.write('- Quarter - Q1 (Jan to Mar), Q2 (Apr to June), Q3 (July to Sep), Q4 (Oct to Dec)')

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
    st.write('- Top 10 States')
    st.write('- Top 10 Districts')
    st.write('- Top 10 Pincodes')

    st.subheader('Top User:')
    st.write('Explore the most number of registered users for a selected Year-Quarter combination')
    st.write('- Top 10 States')
    st.write('- Top 10 Districts')
    st.write('- Top 10 Pincodes')


def state_list():
    gopi = psycopg2.connect(
        host='localhost', user='postgres', password='root', database='phonepe')
    cursor = gopi.cursor()
    cursor.execute(f"""select distinct state from aggregated_transaction
                        order by state asc;""")
    s = cursor.fetchall()
    state = [i[0] for i in s]
    return state


def brand_list():
    gopi = psycopg2.connect(
        host='localhost', user='postgres', password='root', database='phonepe')
    cursor = gopi.cursor()
    cursor.execute(f"""select distinct user_brand from aggregated_user
                        order by user_brand asc;""")
    s = cursor.fetchall()
    brand = [i[0] for i in s]
    return brand


def find_district(pincode):
    url = "https://api.postalpincode.in/pincode/"
    response = requests.get(url + pincode)
    data = json.loads(response.text)
    district = data[0]['PostOffice'][0]['District']
    return district


class data_extraction:

    def aggregated_transaction():
        try:
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
                            data['Year'].append(j)
                            data['Quater'].append('Q'+str(k[0]))
                            data['Transaction_type'].append(name)
                            data['Transaction_count'].append(count)
                            data['Transaction_amount'].append(amount)

            return data
        
        except:
            pass

    def aggregated_user():
        try:
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
                                data['Year'].append(j)
                                data['Quater'].append('Q'+str(k[0]))
                                data['User_brand'].append(brand)
                                data['User_count'].append(count)
                                data['User_percentage'].append(percentage)
                        except:
                            pass
                            
            return data
        
        except:
            pass

    def map_transaction():
        try:
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
                            data['Year'].append(j)
                            data['Quater'].append('Q'+str(k[0]))
                            data['District'].append(district)
                            data['Transaction_count'].append(count)
                            data['Transaction_amount'].append(amount)

            return data
        
        except:
            pass

    def map_user():
        try:
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
                            data['Year'].append(j)
                            data['Quater'].append('Q'+str(k[0]))
                            data['District'].append(district)
                            data['Registered_user'].append(reg_user)
                            data['App_opens'].append(app_opens)

            return data
        
        except:
            pass

    def top_transaction_district():
        try:
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
                            data['Year'].append(j)
                            data['Quater'].append('Q'+str(k[0]))
                            data['District'].append(district)
                            data['Transaction_count'].append(count)
                            data['Transaction_amount'].append(amount)                        

            return data
        
        except:
            pass

    def top_transaction_pincode():
        try:
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
                            data['Year'].append(j)
                            data['Quater'].append('Q'+str(k[0]))
                            data['Pincode'].append(pincode)
                            data['Transaction_count'].append(count)
                            data['Transaction_amount'].append(amount)                        

            return data

        except:
            pass

    def top_user_district():
        try:
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
                            data['Year'].append(j)
                            data['Quater'].append('Q'+str(k[0]))
                            data['District'].append(district)
                            data['Registered_user'].append(reg_user)                       

            return data
        
        except:
            pass

    def top_user_pincode():
        try:
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
                            data['Year'].append(j)
                            data['Quater'].append('Q'+str(k[0]))
                            data['Pincode'].append(pincode)
                            data['Registered_user'].append(reg_user)                       

            return data
        
        except:
            pass


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
                            quater				varchar(2),\
                            transaction_type	varchar(255),\
                            transaction_count	int,\
                            transaction_amount	float);\
                        create table if not exists aggregated_user(\
                            state				varchar(255),\
                            year				int,\
                            quater				varchar(2),\
                            user_brand			varchar(255),\
                            user_count			int,\
                            user_percentage		float);\
                        create table if not exists map_transaction(\
                            state				varchar(255),\
                            year				int,\
                            quater				varchar(2),\
                            district			varchar(255),\
                            transaction_count	int,\
                            transaction_amount	float);\
                        create table if not exists map_user(\
                            state				varchar(255),\
                            year				int,\
                            quater				varchar(2),\
                            district			varchar(255),\
                            registered_user		int,\
                            app_opens			int);\
                        create table if not exists top_transaction_district(\
                            state				varchar(255),\
                            year				int,\
                            quater				varchar(2),\
                            district			varchar(255),\
                            transaction_count	int,\
                            transaction_amount	float);	\
                        create table if not exists top_transaction_pincode(\
                            state				varchar(255),\
                            year				int,\
                            quater				varchar(2),\
                            pincode				varchar(255),\
                            transaction_count	int,\
                            transaction_amount	float);\
                        create table if not exists top_user_district(\
                            state				varchar(255),\
                            year				int,\
                            quater				varchar(2),\
                            district			varchar(255),\
                            registered_user		int);\
                        create table if not exists top_user_pincode(\
                            state				varchar(255),\
                            year				int,\
                            quater				varchar(2),\
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


class convert:
    def millions(transaction):
        a = transaction
        b = a.replace(',', '')
        c = int(b)/1000000
        d = '{:.2f}'.format(c)
        e = str(d) + 'M'
        return e

    def billions(transaction):
        a = transaction
        b = a.replace(',', '')
        c = int(b)/1000000000
        d = '{:.2f}'.format(c)
        e = str(d) + 'B'
        return e

    def trillions(transaction):
        a = transaction
        b = a.replace(',', '')
        c = int(b)/1000000000000
        d = '{:.2f}'.format(c)
        e = str(d) + 'T'
        return e

    def crores(transaction):
        a = transaction
        b = a.replace(',', '')
        c = int(b)/10000000
        d = '{:.2f}'.format(c)
        e = str(d) + 'Cr'
        return e

    def thousands(transaction):
        a = transaction
        b = a.replace(',', '')
        c = int(b)/1000
        d = '{:.2f}'.format(c)
        e = str(d) + 'K'
        return e

    def rupees(transaction):
        a = transaction
        b = a.replace(',', '')
        if len(b) <= 3:
            return b
        elif len(b) in (4, 5, 6):
            return convert.thousands(b)
        elif len(b) in (7, 8, 9):
            return convert.millions(b)
        elif len(b) in (10, 11, 12):
            return convert.billions(b)
        elif len(b) >= 13:
            return convert.trillions(b)
        

class state:
    def geo_state_list():
        url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
        response = requests.get(url)
        data = json.loads(response.content)
        geo_state = [i['properties']['ST_NM'] for i in data['features']]
        geo_state.sort(reverse=False)
        return geo_state

    def original_state_list():
        gopi = psycopg2.connect(host='localhost', user='postgres',
                                password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select distinct state 
                            from aggregated_transaction
                            order by state asc;""")
        s = cursor.fetchall()
        original_state = [i[0] for i in s]
        return original_state

    def state_dict():
        original = state.original_state_list()
        geo = state.geo_state_list()

        data = {}
        for i in range(0, len(original)):
            data[original[i]] = geo[i]
        return data

    def state_list(data):
        missed = set(state.original_state_list()
                     ).symmetric_difference(set(data))
        missed = list(missed)
        all_state = state.state_dict()
        if len(missed) > 0:
            for i in missed:
                del all_state[i]
        return list(all_state.values())


class plotly:

    def geo_map(data, locations_column, color_column, title, title_x=0.25):
        fig = px.choropleth(data,
                            geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                            featureidkey='properties.ST_NM',
                            locations=locations_column,
                            color=color_column,
                            color_continuous_scale=px.colors.diverging.RdYlGn,
                            title=title,
                            height=700)
        fig.update_geos(fitbounds='locations', visible=False)
        fig.update_layout(title=title,
                          title_x=title_x,
                          title_y=0.93,
                          title_font=dict(size=25))

        st.plotly_chart(fig, use_container_width=True)


    def pie_chart(df, x, y, title, title_x=0.20):

        fig = px.pie(df, names=x, values=y, hole=0.5, title=title)
        
        fig.update_layout(title_x=title_x, title_font_size=22)

        fig.update_traces(text=df[y], textinfo='percent+value', 
                          textposition='outside', 
                          textfont=dict(color='white'), 
                          texttemplate='%{value:.4s}<br>%{percent}' )

        st.plotly_chart(fig, use_container_width=True)


    def line_chart(df, x, y, text, textposition, color, title, title_x=0.25):

        fig = px.line(df, x=x, y=y, labels={x: '', y: ''}, title=title, text=df[text])

        fig.update_layout(title_x=title_x, title_font_size=22)

        fig.update_traces(line=dict(color=color, width=3.5),
                            marker=dict(symbol='diamond', size=10),
                            textfont=dict(size=13.5),
                            textposition=textposition,
                            hovertemplate='%{x}<br>%{y}')

        st.plotly_chart(fig, use_container_width=True, height=100)


    def multi_line_chart(df, x, y, colorcolumn, title, title_x=0.25, height=500):

        fig = px.line(df, x=x, y=y, color=colorcolumn, 
                      labels={x: '', y: ''}, title=title)

        fig.update_layout(title_x=title_x, 
                          title_font_size=22,
                          height=height)

        fig.update_traces(mode='lines+markers',
                        marker=dict(symbol='diamond', size=5),
                        hovertemplate='%{x}<br>%{y}')

        st.plotly_chart(fig, use_container_width=True)


    def marker_multi_line_chart(df, x, y, colorcolumn, text, textposition, title, title_x=0.25, height=500):

        fig = px.line(df, x=x, y=y, color=colorcolumn, labels={x: '', y: ''}, title=title, text=df[text])

        fig.update_layout(title_x=title_x, 
                          title_font_size=22,
                          height=height)

        fig.update_traces(marker=dict(symbol='diamond', size=10),
                          textposition=textposition,
                          hovertemplate='%{x}<br>%{y}')

        st.plotly_chart(fig, use_container_width=True, height=100)


    def horizontal_bar_chart(df, x, y, text, color, title):

        fig = px.bar(df, x=x, y=y, labels={x: '', y: ''}, title=title)

        fig.update_layout(title_x=0.35, title_font_size=22)

        text_position = ['inside' if val >= max(df[x]) * 0.75 else 'outside' for val in df[x]]

        fig.update_traces(marker_color=color, 
                          text=df[text], 
                          textposition=text_position,
                          textfont=dict(size=14),
                          insidetextfont=dict(color='white'),
                          textangle=0,
                          hovertemplate='%{x}<br>%{y}')
        
        st.plotly_chart(fig, use_container_width=True)


    def vertical_bar_chart(df, x, y, text, color, title, title_x=0.25):

        fig = px.bar(df, x=x, y=y, labels={x: '', y: ''}, title=title)

        fig.update_layout(title_x=title_x, title_font_size=22)

        text_position = ['inside' if val >= max(df[y]) * 0.90 else 'outside' for val in df[y]]

        fig.update_traces(marker_color=color, 
                          text=df[text], 
                          textposition=text_position,
                          textfont=dict(size=14),
                          insidetextfont=dict(color='white'),
                          textangle=0,
                          hovertemplate='%{x}<br>%{y}')
        
        st.plotly_chart(fig, use_container_width=True, height=100)


    def top10_transaction_state_vertical_bar_chart(df, x, y, text, color, title, title_x=0.25):
        
        fig = px.bar(df, x=x, y=y, labels={x: '', y: ''}, title=title,
                    custom_data=['Count', 'Avg. Transaction Value'])
        
        fig.update_layout(title_x=title_x, title_font_size=22)

        fig.update_xaxes(tickfont=dict(size=13.25))

        text_position = ['inside' if val >= max(df[y]) * 0.90 else 'outside' for val in df[y]]

        fig.update_traces(marker_color=color, 
                          text=df[text], 
                          textposition=text_position,
                          textfont=dict(size=14),
                          insidetextfont=dict(color='white'),
                          textangle=0,
                          hovertemplate='Transaction Count: <br>%{customdata[0]}<br>Avg. Transaction Value: <br>%{customdata[1]}')
        
        st.plotly_chart(fig, use_container_width=True)

    
    def top10_transaction_district_vertical_bar_chart(df, x, y, text, color, title, title_x=0.25):
        
        fig = px.bar(df, x=x, y=y, labels={x: '', y: ''}, title=title,
                    custom_data=['Count', 'Avg. Transaction Value'])
        
        fig.update_layout(title_x=title_x, title_font_size=22)

        new_text_xaxis = [district.replace('@@@','<br>') for district in df[x]]

        fig.update_xaxes(tickmode='array', tickvals=list(range(len(df))), ticktext=new_text_xaxis, tickfont=dict(size=13.25))

        text_position = ['inside' if val >= max(df[y]) * 0.90 else 'outside' for val in df[y]]

        fig.update_traces(marker_color=color, 
                          text=df[text], 
                          textposition=text_position,
                          textfont=dict(size=14),
                          insidetextfont=dict(color='white'),
                          textangle=0,
                          hovertemplate='Transaction Count: <br>%{customdata[0]}<br>Avg. Transaction Value: <br>%{customdata[1]}')
        
        st.plotly_chart(fig, use_container_width=True)

    
    def top10_transaction_pincode_vertical_bar_chart(df, x, y, text, color, title, title_x=0.25):
        
        fig = px.bar(df, x=x, y=y, labels={x: '', y: ''}, title=title,
                    custom_data=['Count', 'Avg. Transaction Value'])
        
        fig.update_layout(title_x=title_x, title_font_size=22)

        new_text_xaxis = [pincode.replace('@@@','<br>') for pincode in df[x]]

        fig.update_xaxes(tickmode='array', tickvals=list(range(len(df))), ticktext=new_text_xaxis, tickfont=dict(size=13.25))

        text_position = ['inside' if val >= max(df[y]) * 0.90 else 'outside' for val in df[y]]

        fig.update_traces(marker_color=color, 
                          text=df[text], 
                          textposition=text_position,
                          textfont=dict(size=14),
                          insidetextfont=dict(color='white'),
                          textangle=0,
                          hovertemplate='Transaction Count: <br>%{customdata[0]}<br>Avg. Transaction Value: <br>%{customdata[1]}')
        
        st.plotly_chart(fig, use_container_width=True)


    def top10_user_vertical_bar_chart(df, x, y, text, color, title, title_x=0.25):
        
        fig = px.bar(df, x=x, y=y, labels={x: '', y: ''}, title=title)
        
        fig.update_layout(title_x=title_x, title_font_size=22)

        new_text_xaxis = [user.replace('@@@','<br>') for user in df[x]]

        fig.update_xaxes(tickmode='array', tickvals=list(range(len(df))), ticktext=new_text_xaxis, tickfont=dict(size=13.25))

        text_position = ['inside' if val >= max(df[y]) * 0.90 else 'outside' for val in df[y]]

        fig.update_traces(marker_color=color, 
                          text=df[text], 
                          textposition=text_position,
                          textfont=dict(size=14),
                          insidetextfont=dict(color='white'),
                          textangle=0,
                          hovertemplate='%{y}')
        
        st.plotly_chart(fig, use_container_width=True)


class aggregated_transaction:

    # state wise

    def state_wise_total_transaction_count():
        gopi = psycopg2.connect(host='localhost', user='postgres',
                                password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, sum(transaction_count) as transaction_count 
                            from aggregated_transaction
                            group by state
                            order by state asc;""")
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['State Original', 'Transaction Count'], index=i)
        state_original = data['State Original'].tolist()
        data['State'] = state.state_list(state_original)
        data = data.sort_values(by='Transaction Count', ascending=False)
        data = data.rename_axis('S.No')
        data['Transaction'] = data['Transaction Count'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def state_wise_total_transaction_amount():
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, sum(transaction_amount) as transaction_amount 
                            from aggregated_transaction
                            group by state
                            order by state asc;""")
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['State Original', 'Transaction Amount'], index=i)
        state_original = data['State Original'].tolist()
        data['State'] = state.state_list(state_original)
        data = data.sort_values(by='Transaction Amount', ascending=False)
        data['Transaction Amount'] = data['Transaction Amount'].apply(
            lambda x: int(round(x, 0)))
        data = data.rename_axis('S.No')
        data['Transaction'] = data['Transaction Amount'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectstate_year_wise_total_transaction_count(state_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, year, sum(transaction_count) as transaction_count 
                            from aggregated_transaction
                            where state=%s
                            group by year, state
                            order by year asc;""", (state_option,))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['State', 'Year', 'Transaction Count'], index=i)
        data = data.rename_axis('S.No')
        data['Transaction'] = data['Transaction Count'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectstate_year_wise_total_transaction_amount(state_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, year, sum(transaction_amount) as transaction_amount 
                            from aggregated_transaction
                            where state=%s
                            group by year, state
                            order by year asc;""", (state_option,))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['State', 'Year', 'Transaction Amount'], index=i)
        data['Transaction Amount'] = data['Transaction Amount'].apply(
            lambda x: int(round(x, 0)))
        data = data.rename_axis('S.No')
        data['Transaction'] = data['Transaction Amount'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectstate_quater_wise_total_transaction_count(state_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, quater, sum(transaction_count) as transaction_count 
                        from aggregated_transaction
                        where state=%s
                        group by quater, state
                        order by quater asc;""", (state_option,))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['State', 'Quater', 'Transaction Count'], index=i)
        data = data.rename_axis('S.No')
        data['Transaction'] = data['Transaction Count'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectstate_quater_wise_total_transaction_amount(state_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, quater, sum(transaction_amount) as transaction_amount 
                            from aggregated_transaction
                            where state=%s
                            group by quater, state
                            order by quater asc;""", (state_option,))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['State', 'Quater', 'Transaction Amount'], index=i)
        data['Transaction Amount'] = data['Transaction Amount'].apply(
            lambda x: int(round(x, 0)))
        data = data.rename_axis('S.No')
        data['Transaction'] = data['Transaction Amount'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectstate_type_wise_total_transaction_count(state_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, transaction_type, sum(transaction_count) as transaction_count 
                        from aggregated_transaction
                        where state=%s
                        group by transaction_type, state
                        order by case when transaction_type = 'Others' then 1 else 0 end, transaction_count desc;""", (state_option,))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['State', 'Transaction Type', 'Transaction Count'], index=i)
        data = data.rename_axis('S.No')
        data['Transaction'] = data['Transaction Count'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectstate_type_wise_total_transaction_amount(state_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, transaction_type, sum(transaction_amount) as transaction_amount 
                        from aggregated_transaction
                        where state=%s
                        group by transaction_type, state
                        order by case when transaction_type = 'Others' then 1 else 0 end, transaction_amount desc;""", (state_option,))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['State', 'Transaction Type', 'Transaction Amount'], index=i)
        data['Transaction Amount'] = data['Transaction Amount'].apply(
            lambda x: int(round(x, 0)))
        data = data.rename_axis('S.No')
        data['Transaction'] = data['Transaction Amount'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectstate_year_quater_wise_total_transaction_count(state_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, year, quater, sum(transaction_count) as transaction_count 
                        from aggregated_transaction
                        where state=%s
                        group by quater, year, state
                        order by year, quater asc;""", (state_option,))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['State', 'Year', 'Quater', 'Transaction Count'], index=i)
        data = data.rename_axis('S.No')
        data['Transaction'] = data['Transaction Count'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectstate_year_quater_wise_total_transaction_amount(state_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, year, quater, sum(transaction_amount) as transaction_amount 
                            from aggregated_transaction
                            where state=%s
                            group by quater, year, state
                            order by year, quater asc;""", (state_option,))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['State', 'Year', 'Quater', 'Transaction Amount'], index=i)
        data['Transaction Amount'] = data['Transaction Amount'].apply(
            lambda x: int(round(x, 0)))
        data = data.rename_axis('S.No')
        data['Transaction'] = data['Transaction Amount'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectstate_year_type_wise_total_transaction_count(state_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, year, transaction_type, sum(transaction_count) as transaction_count 
                        from aggregated_transaction
                        where state=%s
                        group by transaction_type, year, state
                        order by year asc, case when transaction_type = 'Others' then 1 else 0 end, transaction_type asc;""", (state_option,))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['State', 'Year', 'Transaction Type', 'Transaction Count'], index=i)
        data = data.rename_axis('S.No')
        data['Transaction'] = data['Transaction Count'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectstate_year_type_wise_total_transaction_amount(state_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, year, transaction_type, sum(transaction_amount) as transaction_amount 
                            from aggregated_transaction
                            where state=%s
                            group by transaction_type, year, state
                            order by year asc, case when transaction_type = 'Others' then 1 else 0 end, transaction_type asc;""", (state_option,))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['State', 'Year', 'Transaction Type', 'Transaction Amount'], index=i)
        data['Transaction Amount'] = data['Transaction Amount'].apply(
            lambda x: int(round(x, 0)))
        data = data.rename_axis('S.No')
        data['Transaction'] = data['Transaction Amount'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectstate_quater_type_wise_total_transaction_count(state_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, quater, transaction_type, sum(transaction_count) as transaction_count 
                        from aggregated_transaction
                        where state=%s
                        group by transaction_type, quater, state
                        order by quater asc, case when transaction_type = 'Others' then 1 else 0 end, transaction_type asc;""", (state_option,))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['State', 'Quater', 'Transaction Type', 'Transaction Count'], index=i)
        data = data.rename_axis('S.No')
        data['Transaction'] = data['Transaction Count'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectstate_quater_type_wise_total_transaction_amount(state_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, quater, transaction_type, sum(transaction_amount) as transaction_amount 
                            from aggregated_transaction
                            where state=%s
                            group by transaction_type, quater, state
                            order by quater asc, case when transaction_type = 'Others' then 1 else 0 end, transaction_type asc;""", (state_option,))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['State', 'Quater', 'Transaction Type', 'Transaction Amount'], index=i)
        data['Transaction Amount'] = data['Transaction Amount'].apply(
            lambda x: int(round(x, 0)))
        data = data.rename_axis('S.No')
        data['Transaction'] = data['Transaction Amount'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectstate_selectyear_quater_wise_total_transaction_count(state_option, year_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, year, quater, sum(transaction_count) as transaction_count 
                        from aggregated_transaction
                        where state=%s and year=%s
                        group by quater, year, state
                        order by quater asc, transaction_count desc;""", (state_option, year_option))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['State', 'Year', 'Quater', 'Transaction Count'], index=i)
        data = data.rename_axis('S.No')
        data['Transaction'] = data['Transaction Count'].apply(
            lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectstate_selectyear_quater_wise_total_transaction_amount(state_option, year_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, year, quater, sum(transaction_amount) as transaction_amount 
                            from aggregated_transaction
                            where state=%s and year=%s
                            group by quater, year, state
                            order by quater asc, transaction_amount desc;""", (state_option, year_option))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['State', 'Year', 'Quater', 'Transaction Amount'], index=i)
        data['Transaction Amount'] = data['Transaction Amount'].apply(
            lambda x: int(round(x, 0)))
        data = data.rename_axis('S.No')
        data['Transaction'] = data['Transaction Amount'].apply(
            lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data


    def selectstate_selectyear_type_wise_total_transaction_count(state_option, year_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, year, transaction_type, sum(transaction_count) as transaction_count 
                        from aggregated_transaction
                        where state=%s and year=%s
                        group by transaction_type, year, state
                        order by case when transaction_type = 'Others' then 1 else 0 end, transaction_count desc;""", (state_option, year_option))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['State', 'Year', 'Transaction Type', 'Transaction Count'], index=i)
        data['Transaction'] = data['Transaction Count'].apply(lambda x: convert.rupees(str(x)))
        data = data.rename_axis('S.No')
        data['Transaction'] = data['Transaction Count'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectstate_selectyear_type_wise_total_transaction_amount(state_option, year_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, year, transaction_type, sum(transaction_amount) as transaction_amount 
                        from aggregated_transaction
                        where state=%s and year=%s
                        group by transaction_type, year, state
                        order by case when transaction_type = 'Others' then 1 else 0 end, transaction_amount desc;""", (state_option, year_option))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['State', 'Year', 'Transaction Type', 'Transaction Amount'], index=i)
        data['Transaction Amount'] = data['Transaction Amount'].apply(
            lambda x: int(round(x, 0)))
        data = data.rename_axis('S.No')
        data['Transaction'] = data['Transaction Amount'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectstate_selectyear_quater_type_wise_total_transaction_count(state_option, year_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, year, quater, transaction_type, sum(transaction_count) as transaction_count 
                        from aggregated_transaction
                        where state=%s and year=%s
                        group by transaction_type, quater, year, state
                        order by quater asc, case when transaction_type = 'Others' then 1 else 0 end, transaction_count desc;""", (state_option, year_option))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['State', 'Year', 'Quater', 'Transaction Type', 'Transaction Count'], index=i)
        data = data.rename_axis('S.No')
        data['Transaction'] = data['Transaction Count'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectstate_selectyear_quater_type_wise_total_transaction_amount(state_option, year_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, year, quater, transaction_type, sum(transaction_amount) as transaction_amount 
                            from aggregated_transaction
                            where state=%s and year=%s
                            group by transaction_type, quater, year, state
                            order by quater asc, case when transaction_type = 'Others' then 1 else 0 end, transaction_amount desc;""", (state_option, year_option))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['State', 'Year', 'Quater', 'Transaction Type', 'Transaction Amount'], index=i)
        data['Transaction Amount'] = data['Transaction Amount'].apply(
            lambda x: int(round(x, 0)))
        data = data.rename_axis('S.No')
        data['Transaction'] = data['Transaction Amount'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectstate_selectquater_year_wise_total_transaction_count(state_option, quater_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, year, quater, sum(transaction_count) as transaction_count 
                        from aggregated_transaction
                        where state=%s and quater=%s
                        group by quater, year, state
                        order by year asc;""", (state_option, quater_option))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['State', 'Year', 'Quater', 'Transaction Count'], index=i)
        data = data.rename_axis('S.No')
        data['Transaction'] = data['Transaction Count'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectstate_selectquater_year_wise_total_transaction_amount(state_option, quater_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, year, quater, sum(transaction_amount) as transaction_amount
                        from aggregated_transaction
                        where state=%s and quater=%s
                        group by quater, year, state
                        order by year asc;""", (state_option, quater_option))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['State', 'Year', 'Quater', 'Transaction Amount'], index=i)
        data['Transaction Amount'] = data['Transaction Amount'].apply(
            lambda x: int(round(x, 0)))
        data = data.rename_axis('S.No')
        data['Transaction'] = data['Transaction Amount'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectstate_selectquater_type_wise_total_transaction_count(state_option, quater_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, quater, transaction_type, sum(transaction_count) as transaction_count 
                        from aggregated_transaction
                        where state=%s and quater=%s
                        group by transaction_type, quater, state
                        order by case when transaction_type = 'Others' then 1 else 0 end, transaction_count desc;""", (state_option, quater_option))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['State', 'Quater', 'Transaction Type', 'Transaction Count'], index=i)
        data = data.rename_axis('S.No')
        data['Transaction'] = data['Transaction Count'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectstate_selectquater_type_wise_total_transaction_amount(state_option, quater_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, quater, transaction_type, sum(transaction_amount) as transaction_amount 
                        from aggregated_transaction
                        where state=%s and quater=%s
                        group by transaction_type, quater, state
                        order by case when transaction_type = 'Others' then 1 else 0 end, transaction_amount desc;""", (state_option, quater_option))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['State', 'Quater', 'Transaction Type', 'Transaction Amount'], index=i)
        data['Transaction Amount'] = data['Transaction Amount'].apply(
            lambda x: int(round(x, 0)))
        data = data.rename_axis('S.No')
        data['Transaction'] = data['Transaction Amount'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectstate_selectquater_year_type_wise_total_transaction_count(state_option, quater_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, year, quater, transaction_type, sum(transaction_count) as transaction_count 
                        from aggregated_transaction
                        where state=%s and quater=%s
                        group by transaction_type, quater, year, state
                        order by year asc, case when transaction_type = 'Others' then 1 else 0 end, transaction_count desc;""", (state_option, quater_option))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['State', 'Year', 'Quater', 'Transaction Type', 'Transaction Count'], index=i)
        data = data.rename_axis('S.No')
        data['Transaction'] = data['Transaction Count'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectstate_selectquater_year_type_wise_total_transaction_amount(state_option, quater_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, year, quater, transaction_type, sum(transaction_amount) as transaction_amount
                        from aggregated_transaction
                        where state=%s and quater=%s
                        group by transaction_type, quater, year, state
                        order by year asc, case when transaction_type = 'Others' then 1 else 0 end, transaction_amount desc;""", (state_option, quater_option))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['State', 'Year', 'Quater', 'Transaction Type', 'Transaction Amount'], index=i)
        data['Transaction Amount'] = data['Transaction Amount'].apply(
            lambda x: int(round(x, 0)))
        data = data.rename_axis('S.No')
        data['Transaction'] = data['Transaction Amount'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectstate_selectyear_selectquater_type_wise_total_transaction_count(state_option, year_option, quater_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, year, quater, transaction_type, sum(transaction_count) as transaction_count 
                        from aggregated_transaction
                        where state=%s and year=%s and quater=%s
                        group by transaction_type, quater, year, state
                        order by case when transaction_type = 'Others' then 1 else 0 end, transaction_count desc;""", (state_option, year_option, quater_option))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['State', 'Year', 'Quater', 'Transaction Type', 'Transaction Count'], index=i)
        data = data.rename_axis('S.No')
        data['Transaction'] = data['Transaction Count'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectstate_selectyear_selectquater_type_wise_total_transaction_amount(state_option, year_option, quater_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, year, quater, transaction_type, sum(transaction_amount) as transaction_amount 
                        from aggregated_transaction
                        where state=%s and year=%s and quater=%s
                        group by transaction_type, quater, year, state
                        order by case when transaction_type = 'Others' then 1 else 0 end, transaction_amount desc;""", (state_option, year_option, quater_option))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['State', 'Year', 'Quater', 'Transaction Type', 'Transaction Amount'], index=i)
        data['Transaction Amount'] = data['Transaction Amount'].apply(
            lambda x: int(round(x, 0)))
        data = data.rename_axis('S.No')
        data['Transaction'] = data['Transaction Amount'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    # year - wise

    def year_wise_total_transaction_count():
        gopi = psycopg2.connect(host='localhost', user='postgres',
                                password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select year, sum(transaction_count) as transaction_count 
                            from aggregated_transaction
                            group by year
                            order by year asc;""")
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(s, columns=['Year', 'Transaction Count'], index=i)
        data = data.rename_axis('S.No')
        data['Transaction'] = data['Transaction Count'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def year_wise_total_transaction_amount():
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select year, sum(transaction_amount) as transaction_amount 
                            from aggregated_transaction
                            group by year
                            order by year asc;""")
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['Year', 'Transaction Amount'], index=i)
        data['Transaction Amount'] = data['Transaction Amount'].apply(
            lambda x: int(round(x, 0)))
        data = data.rename_axis('S.No')
        data['Transaction'] = data['Transaction Amount'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def year_quater_wise_total_transaction_count():
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select year, quater, sum(transaction_count) as transaction_count 
                            from aggregated_transaction
                            group by quater, year
                            order by year, quater asc;""")
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['Year', 'Quater', 'Transaction Count'], index=i)
        data = data.rename_axis('S.No')
        data['Transaction'] = data['Transaction Count'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def year_quater_wise_total_transaction_amount():
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select year, quater, sum(transaction_amount) as transaction_amount 
                            from aggregated_transaction
                            group by quater, year
                            order by year, quater asc;""")
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['Year', 'Quater', 'Transaction Amount'], index=i)
        data['Transaction Amount'] = data['Transaction Amount'].apply(
            lambda x: int(round(x, 0)))
        data = data.rename_axis('S.No')
        data['Transaction'] = data['Transaction Amount'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def year_type_wise_total_transaction_count():
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select year, transaction_type, sum(transaction_count) as transaction_count 
                            from aggregated_transaction
                            group by transaction_type, year
                            order by year asc, case when transaction_type = 'Others' then 1 else 0 end, transaction_type asc;""")
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['Year', 'Transaction Type', 'Transaction Count'], index=i)
        data = data.rename_axis('S.No')
        data['Transaction'] = data['Transaction Count'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def year_type_wise_total_transaction_amount():
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select year, transaction_type, sum(transaction_amount) as transaction_amount 
                            from aggregated_transaction
                            group by transaction_type, year
                            order by year asc, case when transaction_type = 'Others' then 1 else 0 end, transaction_type asc;""")
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['Year', 'Transaction Type', 'Transaction Amount'], index=i)
        data['Transaction Amount'] = data['Transaction Amount'].apply(
            lambda x: int(round(x, 0)))
        data = data.rename_axis('S.No')
        data['Transaction'] = data['Transaction Amount'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectyear_state_wise_total_transaction_count(year_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, year, sum(transaction_count) as transaction_count 
                            from aggregated_transaction
                            where year=%s
                            group by year, state
                            order by state asc;""", (year_option,))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['State Original', 'Year', 'Transaction Count'], index=i)
        state_original = data['State Original'].tolist()
        data['State'] = state.state_list(state_original)
        data = data.sort_values(by='Transaction Count', ascending=False)
        data = data.rename_axis('S.No')
        data['Transaction'] = data['Transaction Count'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectyear_state_wise_total_transaction_amount(year_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, year, sum(transaction_amount) as transaction_amount 
                            from aggregated_transaction
                            where year=%s
                            group by year, state
                            order by state asc;""", (year_option,))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['State Original', 'Year', 'Transaction Amount'], index=i)
        state_original = data['State Original'].tolist()
        data['State'] = state.state_list(state_original)
        data = data.sort_values(by='Transaction Amount', ascending=False)
        data['Transaction Amount'] = data['Transaction Amount'].apply(
            lambda x: int(round(x, 0)))
        data = data.rename_axis('S.No')
        data['Transaction'] = data['Transaction Amount'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectyear_quater_wise_total_transaction_count(year_option):
        gopi = psycopg2.connect(host='localhost', user='postgres',
                                password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select year, quater, sum(transaction_count) as transaction_count 
                            from aggregated_transaction
                            where year=%s
                            group by quater, year
                            order by quater asc;""", (year_option,))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['Year', 'Quater', 'Transaction Count'], index=i)
        data = data.rename_axis('S.No')
        data['Transaction'] = data['Transaction Count'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectyear_quater_wise_total_transaction_amount(year_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select year, quater, sum(transaction_amount) as transaction_amount 
                            from aggregated_transaction
                            where year=%s
                            group by quater, year
                            order by year, quater asc;""", (year_option,))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['Year', 'Quater', 'Transaction Amount'], index=i)
        data['Transaction Amount'] = data['Transaction Amount'].apply(
            lambda x: int(round(x, 0)))
        data = data.rename_axis('S.No')
        data['Transaction'] = data['Transaction Amount'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectyear_type_wise_total_transaction_count(year_option):
        gopi = psycopg2.connect(host='localhost', user='postgres',
                                password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select year, transaction_type, sum(transaction_count) as transaction_count 
                            from aggregated_transaction
                            where year=%s
                            group by transaction_type, year
                            order by case when transaction_type = 'Others' then 1 else 0 end, transaction_count desc;""", (year_option,))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['Year', 'Transaction Type', 'Transaction Count'], index=i)
        data = data.rename_axis('S.No')
        data['Transaction'] = data['Transaction Count'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectyear_type_wise_total_transaction_amount(year_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select year, transaction_type, sum(transaction_amount) as transaction_amount 
                            from aggregated_transaction
                            where year=%s
                            group by transaction_type, year
                            order by case when transaction_type = 'Others' then 1 else 0 end, transaction_amount desc;""", (year_option,))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['Year', 'Transaction Type', 'Transaction Amount'], index=i)
        data['Transaction Amount'] = data['Transaction Amount'].apply(
            lambda x: int(round(x, 0)))
        data = data.rename_axis('S.No')
        data['Transaction'] = data['Transaction Amount'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectyear_quater_type_wise_total_transaction_count(year_option):
        gopi = psycopg2.connect(host='localhost', user='postgres',
                                password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select year, quater, transaction_type, sum(transaction_count) as transaction_count 
                            from aggregated_transaction
                            where year=%s
                            group by transaction_type, quater, year
                            order by quater asc, case when transaction_type = 'Others' then 1 else 0 end, transaction_type asc;""", (year_option,))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['Year', 'Quater', 'Transaction Type', 'Transaction Count'], index=i)
        data = data.rename_axis('S.No')
        data['Transaction'] = data['Transaction Count'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectyear_quater_type_wise_total_transaction_amount(year_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select year, quater, transaction_type, sum(transaction_amount) as transaction_amount 
                            from aggregated_transaction
                            where year=%s
                            group by transaction_type, quater, year
                            order by quater asc, case when transaction_type = 'Others' then 1 else 0 end, transaction_type asc;""", (year_option,))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['Year', 'Quater', 'Transaction Type', 'Transaction Amount'], index=i)
        data['Transaction Amount'] = data['Transaction Amount'].apply(
            lambda x: int(round(x, 0)))
        data = data.rename_axis('S.No')
        data['Transaction'] = data['Transaction Amount'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectyear_selectquater_state_wise_total_transaction_count(year_option, quater_option):
        gopi = psycopg2.connect(host='localhost', user='postgres',
                                password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, year, quater, sum(transaction_count) as transaction_count 
                            from aggregated_transaction
                            where year=%s and quater=%s
                            group by quater, year, state
                            order by state asc;""", (year_option, quater_option))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['State Original', 'Year', 'Quater', 'Transaction Count'], index=i)
        state_original = data['State Original'].tolist()
        data['State'] = state.state_list(state_original)
        data = data.sort_values(by='Transaction Count', ascending=False)
        data = data.rename_axis('S.No')
        data['Transaction'] = data['Transaction Count'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectyear_selectquater_state_wise_total_transaction_amount(year_option, quater_option):
        gopi = psycopg2.connect(host='localhost', user='postgres',
                                password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, year, quater, sum(transaction_amount) as transaction_amount 
                            from aggregated_transaction
                            where year=%s and quater=%s
                            group by quater, year, state
                            order by state asc;""", (year_option, quater_option))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['State Original', 'Year', 'Quater', 'Transaction Amount'], index=i)
        state_original = data['State Original'].tolist()
        data['State'] = state.state_list(state_original)
        data = data.sort_values(by='Transaction Amount', ascending=False)
        data['Transaction Amount'] = data['Transaction Amount'].apply(
            lambda x: int(round(x, 0)))
        data = data.rename_axis('S.No')
        data['Transaction'] = data['Transaction Amount'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectyear_selectquater_type_wise_total_transaction_count(year_option, quater_option):
        gopi = psycopg2.connect(host='localhost', user='postgres',
                                password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select year, quater, transaction_type, sum(transaction_count) as transaction_count 
                            from aggregated_transaction
                            where year=%s and quater=%s
                            group by transaction_type, quater, year
                            order by case when transaction_type = 'Others' then 1 else 0 end, transaction_count desc;""", (year_option, quater_option))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['Year', 'Quater', 'Transaction Type', 'Transaction Count'], index=i)
        data = data.rename_axis('S.No')
        data['Transaction'] = data['Transaction Count'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectyear_selectquater_type_wise_total_transaction_amount(year_option, quater_option):
        gopi = psycopg2.connect(host='localhost', user='postgres',
                                password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select year, quater, transaction_type, sum(transaction_amount) as transaction_amount 
                            from aggregated_transaction
                            where year=%s and quater=%s
                            group by transaction_type, quater, year
                            order by case when transaction_type = 'Others' then 1 else 0 end, transaction_amount desc;""", (year_option, quater_option))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['Year', 'Quater', 'Transaction Type', 'Transaction Amount'], index=i)
        data['Transaction Amount'] = data['Transaction Amount'].apply(
            lambda x: int(round(x, 0)))
        data = data.rename_axis('S.No')
        data['Transaction'] = data['Transaction Amount'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    # quater - wise

    def quater_wise_total_transaction_count():
        gopi = psycopg2.connect(host='localhost', user='postgres',
                                password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select quater, sum(transaction_count) as transaction_count 
                            from aggregated_transaction
                            group by quater
                            order by quater asc;""")
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['Quater', 'Transaction Count'], index=i)
        data = data.rename_axis('S.No')
        data['Transaction'] = data['Transaction Count'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def quater_wise_total_transaction_amount():
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select quater, sum(transaction_amount) as transaction_amount 
                            from aggregated_transaction
                            group by quater
                            order by quater asc;""")
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['Quater', 'Transaction Amount'], index=i)
        data['Transaction Amount'] = data['Transaction Amount'].apply(
            lambda x: int(round(x, 0)))
        data = data.rename_axis('S.No')
        data['Transaction'] = data['Transaction Amount'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def year_quater_wise_total_transaction_count():
        gopi = psycopg2.connect(host='localhost', user='postgres',
                                password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select year, quater, sum(transaction_count) as transaction_count 
                            from aggregated_transaction
                            group by quater, year
                            order by year, quater asc;""")
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['Year', 'Quater', 'Transaction Count'], index=i)
        data = data.rename_axis('S.No')
        data['Transaction'] = data['Transaction Count'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def year_quater_wise_total_transaction_amount():
        gopi = psycopg2.connect(host='localhost', user='postgres',
                                password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select year, quater, sum(transaction_amount) as transaction_amount 
                            from aggregated_transaction
                            group by quater, year
                            order by year, quater asc;""")
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['Year', 'Quater', 'Transaction Amount'], index=i)
        data['Transaction Amount'] = data['Transaction Amount'].apply(
            lambda x: int(round(x, 0)))
        data = data.rename_axis('S.No')
        data['Transaction'] = data['Transaction Amount'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def quater_type_wise_total_transaction_count():
        gopi = psycopg2.connect(host='localhost', user='postgres',
                                password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select quater, transaction_type, sum(transaction_count) as transaction_count 
                            from aggregated_transaction
                            group by transaction_type, quater
                            order by quater asc, case when transaction_type = 'Others' then 1 else 0 end, transaction_type asc;""")
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['Quater', 'Transaction Type', 'Transaction Count'], index=i)
        data = data.rename_axis('S.No')
        data['Transaction'] = data['Transaction Count'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def quater_type_wise_total_transaction_amount():
        gopi = psycopg2.connect(host='localhost', user='postgres',
                                password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select quater, transaction_type, sum(transaction_amount) as transaction_amount 
                            from aggregated_transaction
                            group by transaction_type, quater
                            order by quater asc, case when transaction_type = 'Others' then 1 else 0 end, transaction_type asc;""")
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['Quater', 'Transaction Type', 'Transaction Amount'], index=i)
        data['Transaction Amount'] = data['Transaction Amount'].apply(
            lambda x: int(round(x, 0)))
        data = data.rename_axis('S.No')
        data['Transaction'] = data['Transaction Amount'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data
    
    def selectquater_state_wise_total_transaction_count(quater_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, quater, sum(transaction_count) as transaction_count 
                            from aggregated_transaction
                            where quater=%s
                            group by quater, state
                            order by state asc;""", (quater_option,))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['State Original', 'Quater', 'Transaction Count'], index=i)
        state_original = data['State Original'].tolist()
        data['State'] = state.state_list(state_original)
        data = data.sort_values(by='Transaction Count', ascending=False)
        data = data.rename_axis('S.No')
        data['Transaction'] = data['Transaction Count'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectquater_state_wise_total_transaction_amount(quater_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, quater, sum(transaction_amount) as transaction_amount 
                            from aggregated_transaction
                            where quater=%s
                            group by quater, state
                            order by state asc;""", (quater_option,))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['State Original', 'Quater', 'Transaction Amount'], index=i)
        state_original = data['State Original'].tolist()
        data['State'] = state.state_list(state_original)
        data = data.sort_values(by='Transaction Amount', ascending=False)
        data['Transaction Amount'] = data['Transaction Amount'].apply(
            lambda x: int(round(x, 0)))
        data = data.rename_axis('S.No')
        data['Transaction'] = data['Transaction Amount'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectquater_year_wise_total_transaction_count(quater_option):
        gopi = psycopg2.connect(host='localhost', user='postgres',
                                password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select year, quater, sum(transaction_count) as transaction_count 
                            from aggregated_transaction
                            where quater=%s
                            group by quater, year
                            order by year asc;""", (quater_option,))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['Year', 'Quater', 'Transaction Count'], index=i)
        data = data.rename_axis('S.No')
        data['Transaction'] = data['Transaction Count'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectquater_year_wise_total_transaction_amount(quater_option):
        gopi = psycopg2.connect(host='localhost', user='postgres',
                                password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select year, quater, sum(transaction_amount) as transaction_amount 
                            from aggregated_transaction
                            where quater=%s
                            group by quater, year
                            order by year asc;""", (quater_option,))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['Year', 'Quater', 'Transaction Amount'], index=i)
        data['Transaction Amount'] = data['Transaction Amount'].apply(
            lambda x: int(round(x, 0)))
        data = data.rename_axis('S.No')
        data['Transaction'] = data['Transaction Amount'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectquater_type_wise_total_transaction_count(quater_option):
        gopi = psycopg2.connect(host='localhost', user='postgres',
                                password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select quater, transaction_type, sum(transaction_count) as transaction_count 
                            from aggregated_transaction
                            where quater=%s
                            group by transaction_type, quater
                            order by case when transaction_type = 'Others' then 1 else 0 end, transaction_count desc;""", (quater_option,))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['Quater', 'Transaction Type', 'Transaction Count'], index=i)
        data = data.rename_axis('S.No')
        data['Transaction'] = data['Transaction Count'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectquater_type_wise_total_transaction_amount(quater_option):
        gopi = psycopg2.connect(host='localhost', user='postgres',
                                password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select quater, transaction_type, sum(transaction_amount) as transaction_amount 
                            from aggregated_transaction
                            where quater=%s
                            group by transaction_type, quater
                            order by case when transaction_type = 'Others' then 1 else 0 end, transaction_amount desc;""", (quater_option,))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['Quater', 'Transaction Type', 'Transaction Amount'], index=i)
        data['Transaction Amount'] = data['Transaction Amount'].apply(
            lambda x: int(round(x, 0)))
        data = data.rename_axis('S.No')
        data['Transaction'] = data['Transaction Amount'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectquater_year_type_wise_total_transaction_count(quater_option):
        gopi = psycopg2.connect(host='localhost', user='postgres',
                                password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select year, quater, transaction_type, sum(transaction_count) as transaction_count 
                            from aggregated_transaction
                            where quater=%s
                            group by transaction_type, quater, year
                            order by year asc, case when transaction_type = 'Others' then 1 else 0 end, transaction_type asc;""", (quater_option,))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['Year', 'Quater', 'Transaction Type', 'Transaction Count'], index=i)
        data = data.rename_axis('S.No')
        data['Transaction'] = data['Transaction Count'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectquater_year_type_wise_total_transaction_amount(quater_option):
        gopi = psycopg2.connect(host='localhost', user='postgres',
                                password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select year, quater, transaction_type, sum(transaction_amount) as transaction_amount 
                            from aggregated_transaction
                            where quater=%s
                            group by transaction_type, quater, year
                            order by year asc, case when transaction_type = 'Others' then 1 else 0 end, transaction_type asc;""", (quater_option,))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['Year', 'Quater', 'Transaction Type', 'Transaction Amount'], index=i)
        data['Transaction Amount'] = data['Transaction Amount'].apply(
            lambda x: int(round(x, 0)))
        data = data.rename_axis('S.No')
        data['Transaction'] = data['Transaction Amount'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    # type - wise

    def type_wise_total_transaction_count():
        gopi = psycopg2.connect(host='localhost', user='postgres',
                                password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select transaction_type, sum(transaction_count) as transaction_count 
                            from aggregated_transaction
                            group by transaction_type
                            order by case when transaction_type = 'Others' then 1 else 0 end, transaction_count desc;""")
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['Transaction Type', 'Transaction Count'], index=i)
        data = data.rename_axis('S.No')
        data['Transaction'] = data['Transaction Count'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def type_wise_total_transaction_amount():
        gopi = psycopg2.connect(host='localhost', user='postgres',
                                password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select transaction_type, sum(transaction_amount) as transaction_amount 
                            from aggregated_transaction
                            group by transaction_type
                            order by case when transaction_type = 'Others' then 1 else 0 end, transaction_amount desc;""")
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['Transaction Type', 'Transaction Amount'], index=i)
        data['Transaction Amount'] = data['Transaction Amount'].apply(
            lambda x: int(round(x, 0)))
        data = data.rename_axis('S.No')
        data['Transaction'] = data['Transaction Amount'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def type_year_wise_total_transaction_count():
        gopi = psycopg2.connect(host='localhost', user='postgres',
                                password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select year, transaction_type, sum(transaction_count) as transaction_count 
                            from aggregated_transaction
                            group by transaction_type, year
                            order by year asc, case when transaction_type = 'Others' then 1 else 0 end, transaction_type;""")
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['Year', 'Transaction Type', 'Transaction Count'], index=i)
        data = data.rename_axis('S.No')
        data['Transaction'] = data['Transaction Count'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def type_year_wise_total_transaction_amount():
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select year, transaction_type, sum(transaction_amount) as transaction_amount 
                            from aggregated_transaction
                            group by transaction_type, year
                            order by year asc, case when transaction_type = 'Others' then 1 else 0 end, transaction_type;""")
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['Year', 'Transaction Type', 'Transaction Amount'], index=i)
        data['Transaction Amount'] = data['Transaction Amount'].apply(
            lambda x: int(round(x, 0)))
        data = data.rename_axis('S.No')
        data['Transaction'] = data['Transaction Amount'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def type_quater_wise_total_transaction_count():
        gopi = psycopg2.connect(host='localhost', user='postgres',
                                password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select quater, transaction_type, sum(transaction_count) as transaction_count 
                            from aggregated_transaction
                            group by transaction_type, quater
                            order by quater asc, case when transaction_type = 'Others' then 1 else 0 end, transaction_type;""")
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['Quater', 'Transaction Type', 'Transaction Count'], index=i)
        data = data.rename_axis('S.No')
        data['Transaction'] = data['Transaction Count'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def type_quater_wise_total_transaction_amount():
        gopi = psycopg2.connect(host='localhost', user='postgres',
                                password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select quater, transaction_type, sum(transaction_amount) as transaction_amount 
                            from aggregated_transaction
                            group by transaction_type, quater
                            order by quater asc, case when transaction_type = 'Others' then 1 else 0 end, transaction_type;""")
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['Quater', 'Transaction Type', 'Transaction Amount'], index=i)
        data['Transaction Amount'] = data['Transaction Amount'].apply(
            lambda x: int(round(x, 0)))
        data = data.rename_axis('S.No')
        data['Transaction'] = data['Transaction Amount'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def type_year_quater_wise_total_transaction_count(type_option):
        gopi = psycopg2.connect(host='localhost', user='postgres',
                                password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select year, quater, transaction_type, sum(transaction_count) as transaction_count 
                            from aggregated_transaction
                            where transaction_type=%s
                            group by transaction_type, quater, year
                            order by year, quater asc;""", (type_option,))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['Year', 'Quater', 'Transaction Type', 'Transaction Count'], index=i)
        data = data.rename_axis('S.No')
        data['Transaction'] = data['Transaction Count'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def type_year_quater_wise_total_transaction_amount(type_option):
        gopi = psycopg2.connect(host='localhost', user='postgres',
                                password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select year, quater, transaction_type, sum(transaction_amount) as transaction_amount 
                            from aggregated_transaction
                            where transaction_type=%s
                            group by transaction_type, quater, year
                            order by year, quater asc;""", (type_option,))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['Year', 'Quater', 'Transaction Type', 'Transaction Amount'], index=i)
        data['Transaction Amount'] = data['Transaction Amount'].apply(
            lambda x: int(round(x, 0)))
        data = data.rename_axis('S.No')
        data['Transaction'] = data['Transaction Amount'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selecttype_state_wise_total_transaction_count(type_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, transaction_type, sum(transaction_count) as transaction_count 
                            from aggregated_transaction
                            where transaction_type=%s
                            group by transaction_type, state
                            order by state asc;""", (type_option,))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['State Original', 'Transaction Type', 'Transaction Count'], index=i)
        state_original = data['State Original'].tolist()
        data['State'] = state.state_list(state_original)
        data = data.sort_values(by='Transaction Count', ascending=False)
        data = data.rename_axis('S.No')
        data['Transaction'] = data['Transaction Count'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selecttype_state_wise_total_transaction_amount(type_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, transaction_type, sum(transaction_amount) as transaction_amount 
                            from aggregated_transaction
                            where transaction_type=%s
                            group by transaction_type, state
                            order by state asc;""", (type_option,))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['State Original', 'Transaction Type', 'Transaction Amount'], index=i)
        state_original = data['State Original'].tolist()
        data['State'] = state.state_list(state_original)
        data = data.sort_values(by='Transaction Amount', ascending=False)
        data['Transaction Amount'] = data['Transaction Amount'].apply(
            lambda x: int(round(x, 0)))
        data = data.rename_axis('S.No')
        data['Transaction'] = data['Transaction Amount'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selecttype_year_wise_total_transaction_count(type_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select year, transaction_type, sum(transaction_count) as transaction_count 
                            from aggregated_transaction
                            where transaction_type=%s
                            group by transaction_type, year
                            order by year asc;""", (type_option,))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['Year', 'Transaction Type', 'Transaction Count'], index=i)
        data = data.rename_axis('S.No')
        data['Transaction'] = data['Transaction Count'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selecttype_year_wise_total_transaction_amount(type_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select year, transaction_type, sum(transaction_amount) as transaction_amount 
                            from aggregated_transaction
                            where transaction_type=%s
                            group by transaction_type, year
                            order by year asc;""", (type_option,))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['Year', 'Transaction Type', 'Transaction Amount'], index=i)
        data['Transaction Amount'] = data['Transaction Amount'].apply(
            lambda x: int(round(x, 0)))
        data = data.rename_axis('S.No')
        data['Transaction'] = data['Transaction Amount'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selecttype_quater_wise_total_transaction_count(type_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select quater, transaction_type, sum(transaction_count) as transaction_count 
                            from aggregated_transaction
                            where transaction_type=%s
                            group by transaction_type, quater
                            order by quater asc;""", (type_option,))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['Quater', 'Transaction Type', 'Transaction Count'], index=i)
        data = data.rename_axis('S.No')
        data['Transaction'] = data['Transaction Count'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selecttype_quater_wise_total_transaction_amount(type_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select quater, transaction_type, sum(transaction_amount) as transaction_amount 
                            from aggregated_transaction
                            where transaction_type=%s
                            group by transaction_type, quater
                            order by quater asc;""", (type_option,))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['Quater', 'Transaction Type', 'Transaction Amount'], index=i)
        data['Transaction Amount'] = data['Transaction Amount'].apply(
            lambda x: int(round(x, 0)))
        data = data.rename_axis('S.No')
        data['Transaction'] = data['Transaction Amount'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selecttype_year_quater_wise_total_transaction_count(type_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select year, quater, transaction_type, sum(transaction_count) as transaction_count 
                            from aggregated_transaction
                            where transaction_type=%s
                            group by transaction_type, quater, year
                            order by year, quater asc;""", (type_option,))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['Year', 'Quater', 'Transaction Type', 'Transaction Count'], index=i)
        data = data.rename_axis('S.No')
        data['Transaction'] = data['Transaction Count'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selecttype_year_quater_wise_total_transaction_amount(type_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select year, quater, transaction_type, sum(transaction_amount) as transaction_amount 
                            from aggregated_transaction
                            where transaction_type=%s
                            group by transaction_type, quater, year
                            order by year, quater asc;""", (type_option,))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['Year', 'Quater', 'Transaction Type', 'Transaction Amount'], index=i)
        data['Transaction Amount'] = data['Transaction Amount'].apply(
            lambda x: int(round(x, 0)))
        data = data.rename_axis('S.No')
        data['Transaction'] = data['Transaction Amount'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectstate_selecttype_year_wise_total_transaction_count(state_option, type_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, year, transaction_type, sum(transaction_count) as transaction_count 
                            from aggregated_transaction
                            where state=%s and transaction_type=%s
                            group by transaction_type, year, state
                            order by year asc;""", (state_option, type_option))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['State', 'Year', 'Transaction Type', 'Transaction Count'], index=i)
        data = data.rename_axis('S.No')
        data['Transaction'] = data['Transaction Count'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectstate_selecttype_year_wise_total_transaction_amount(state_option, type_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, year, transaction_type, sum(transaction_amount) as transaction_amount 
                            from aggregated_transaction
                            where state=%s and transaction_type=%s
                            group by transaction_type, year, state
                            order by year asc;""", (state_option, type_option))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['State', 'Year', 'Transaction Type', 'Transaction Amount'], index=i)
        data['Transaction Amount'] = data['Transaction Amount'].apply(
            lambda x: int(round(x, 0)))
        data = data.rename_axis('S.No')
        data['Transaction'] = data['Transaction Amount'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectstate_selecttype_quater_wise_total_transaction_count(state_option, type_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, quater, transaction_type, sum(transaction_count) as transaction_count 
                            from aggregated_transaction
                            where state=%s and transaction_type=%s
                            group by transaction_type, quater, state
                            order by quater asc;""", (state_option, type_option))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['State', 'Quater', 'Transaction Type', 'Transaction Count'], index=i)
        data = data.rename_axis('S.No')
        data['Transaction'] = data['Transaction Count'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectstate_selecttype_quater_wise_total_transaction_amount(state_option, type_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, quater, transaction_type, sum(transaction_amount) as transaction_amount 
                            from aggregated_transaction
                            where state=%s and transaction_type=%s
                            group by transaction_type, quater, state
                            order by quater asc;""", (state_option, type_option))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['State', 'Quater', 'Transaction Type', 'Transaction Amount'], index=i)
        data['Transaction Amount'] = data['Transaction Amount'].apply(
            lambda x: int(round(x, 0)))
        data = data.rename_axis('S.No')
        data['Transaction'] = data['Transaction Amount'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectstate_selecttype_year_quater_wise_total_transaction_count(state_option, type_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, year, quater, transaction_type, sum(transaction_count) as transaction_count 
                            from aggregated_transaction
                            where state=%s and transaction_type=%s
                            group by transaction_type, quater, year, state
                            order by year, quater asc;""", (state_option, type_option))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['State', 'Year', 'Quater', 'Transaction Type', 'Transaction Count'], index=i)
        data = data.rename_axis('S.No')
        data['Transaction'] = data['Transaction Count'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectstate_selecttype_year_quater_wise_total_transaction_amount(state_option, type_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, year, quater, transaction_type, sum(transaction_amount) as transaction_amount 
                            from aggregated_transaction
                            where state=%s and transaction_type=%s
                            group by transaction_type, quater, year, state
                            order by year, quater asc;""", (state_option, type_option))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['State', 'Year', 'Quater', 'Transaction Type', 'Transaction Amount'], index=i)
        data['Transaction Amount'] = data['Transaction Amount'].apply(
            lambda x: int(round(x, 0)))
        data = data.rename_axis('S.No')
        data['Transaction'] = data['Transaction Amount'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectyear_selecttype_state_wise_total_transaction_count(year_option, type_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, year, transaction_type, sum(transaction_count) as transaction_count 
                            from aggregated_transaction
                            where year=%s and transaction_type=%s
                            group by transaction_type, year, state
                            order by state asc;""", (year_option, type_option))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(s, columns=[
                            'State Original', 'Year', 'Transaction Type', 'Transaction Count'], index=i)
        state_original = data['State Original'].tolist()
        data['State'] = state.state_list(state_original)
        data = data.sort_values(by='Transaction Count', ascending=False)
        data = data.rename_axis('S.No')
        data['Transaction'] = data['Transaction Count'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectyear_selecttype_state_wise_total_transaction_amount(year_option, type_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, year, transaction_type, sum(transaction_amount) as transaction_amount 
                            from aggregated_transaction
                            where year=%s and transaction_type=%s
                            group by transaction_type, year, state
                            order by state asc;""", (year_option, type_option))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(s, columns=[
                            'State Original', 'Year', 'Transaction Type', 'Transaction Amount'], index=i)
        state_original = data['State Original'].tolist()
        data['State'] = state.state_list(state_original)
        data = data.sort_values(by='Transaction Amount', ascending=False)
        data['Transaction Amount'] = data['Transaction Amount'].apply(
            lambda x: int(round(x, 0)))
        data = data.rename_axis('S.No')
        data['Transaction'] = data['Transaction Amount'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectyear_selecttype_quater_wise_total_transaction_count(year_option, type_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select year, quater, transaction_type, sum(transaction_count) as transaction_count 
                            from aggregated_transaction
                            where year=%s and transaction_type=%s
                            group by transaction_type, quater, year
                            order by quater asc;""", (year_option, type_option))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['Year', 'Quater', 'Transaction Type', 'Transaction Count'], index=i)
        data = data.rename_axis('S.No')
        data['Transaction'] = data['Transaction Count'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectyear_selecttype_quater_wise_total_transaction_amount(year_option, type_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select year, quater, transaction_type, sum(transaction_amount) as transaction_amount 
                            from aggregated_transaction
                            where year=%s and transaction_type=%s
                            group by transaction_type, year, quater
                            order by quater asc;""", (year_option, type_option))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['Year', 'Quater', 'Transaction Type', 'Transaction Amount'], index=i)
        data['Transaction Amount'] = data['Transaction Amount'].apply(
            lambda x: int(round(x, 0)))
        data = data.rename_axis('S.No')
        data['Transaction'] = data['Transaction Amount'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectquater_selecttype_state_wise_total_transaction_count(quater_option, type_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, quater, transaction_type, sum(transaction_count) as transaction_count 
                            from aggregated_transaction
                            where quater=%s and transaction_type=%s
                            group by transaction_type, quater, state
                            order by state asc;""", (quater_option, type_option))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(s, columns=[
                            'State Original', 'Quater', 'Transaction Type', 'Transaction Count'], index=i)
        state_original = data['State Original'].tolist()
        data['State'] = state.state_list(state_original)
        data = data.sort_values(by='Transaction Count', ascending=False)
        data = data.rename_axis('S.No')
        data['Transaction'] = data['Transaction Count'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectquater_selecttype_state_wise_total_transaction_amount(quater_option, type_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, quater, transaction_type, sum(transaction_amount) as transaction_amount 
                            from aggregated_transaction
                            where quater=%s and transaction_type=%s
                            group by transaction_type, quater, state
                            order by state asc;""", (quater_option, type_option))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(s, columns=[
                            'State Original', 'Quater', 'Transaction Type', 'Transaction Amount'], index=i)
        state_original = data['State Original'].tolist()
        data['State'] = state.state_list(state_original)
        data = data.sort_values(by='Transaction Amount', ascending=False)
        data['Transaction Amount'] = data['Transaction Amount'].apply(
            lambda x: int(round(x, 0)))
        data = data.rename_axis('S.No')
        data['Transaction'] = data['Transaction Amount'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectquater_selecttype_year_wise_total_transaction_count(quater_option, type_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select year, quater, transaction_type, sum(transaction_count) as transaction_count 
                            from aggregated_transaction
                            where quater=%s and transaction_type=%s
                            group by transaction_type, quater, year
                            order by year asc;""", (quater_option, type_option))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['Year', 'Quater', 'Transaction Type', 'Transaction Count'], index=i)
        data = data.rename_axis('S.No')
        data['Transaction'] = data['Transaction Count'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectquater_selecttype_year_wise_total_transaction_amount(quater_option, type_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select year, quater, transaction_type, sum(transaction_amount) as transaction_amount 
                            from aggregated_transaction
                            where quater=%s and transaction_type=%s
                            group by transaction_type, year, quater
                            order by year asc;""", (quater_option, type_option))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['Year', 'Quater', 'Transaction Type', 'Transaction Amount'], index=i)
        data['Transaction Amount'] = data['Transaction Amount'].apply(
            lambda x: int(round(x, 0)))
        data = data.rename_axis('S.No')
        data['Transaction'] = data['Transaction Amount'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectstate_selectyear_selecttype_quater_wise_total_transaction_count(state_option, year_option, type_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, year, quater, transaction_type, sum(transaction_count) as transaction_count 
                            from aggregated_transaction
                            where state=%s and year=%s and transaction_type=%s
                            group by transaction_type, quater, year, state
                            order by quater asc;""", (state_option, year_option, type_option))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['State', 'Year', 'Quater', 'Transaction Type', 'Transaction Count'], index=i)
        data = data.rename_axis('S.No')
        data['Transaction'] = data['Transaction Count'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectstate_selectyear_selecttype_quater_wise_total_transaction_amount(state_option, year_option, type_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, year, quater, transaction_type, sum(transaction_amount) as transaction_amount 
                            from aggregated_transaction
                            where state=%s and year=%s and transaction_type=%s
                            group by transaction_type, quater, year, state
                            order by quater asc;""", (state_option, year_option, type_option))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['State', 'Year', 'Quater', 'Transaction Type', 'Transaction Amount'], index=i)
        data['Transaction Amount'] = data['Transaction Amount'].apply(
            lambda x: int(round(x, 0)))
        data = data.rename_axis('S.No')
        data['Transaction'] = data['Transaction Amount'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectstate_selectquater_selecttype_year_wise_total_transaction_count(state_option, quater_option, type_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, year, quater, transaction_type, sum(transaction_count) as transaction_count 
                            from aggregated_transaction
                            where state=%s and quater=%s and transaction_type=%s
                            group by transaction_type, quater, year, state
                            order by year asc;""", (state_option, quater_option, type_option))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['State', 'Year', 'Quater', 'Transaction Type', 'Transaction Count'], index=i)
        data = data.rename_axis('S.No')
        data['Transaction'] = data['Transaction Count'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectstate_selectquater_selecttype_year_wise_total_transaction_amount(state_option, quater_option, type_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, year, quater, transaction_type, sum(transaction_amount) as transaction_amount 
                            from aggregated_transaction
                            where state=%s and quater=%s and transaction_type=%s
                            group by transaction_type, quater, year, state
                            order by year asc;""", (state_option, quater_option, type_option))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['State', 'Year', 'Quater', 'Transaction Type', 'Transaction Amount'], index=i)
        data['Transaction Amount'] = data['Transaction Amount'].apply(
            lambda x: int(round(x, 0)))
        data = data.rename_axis('S.No')
        data['Transaction'] = data['Transaction Amount'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectyear_selectquater_selecttype_state_wise_total_transaction_count(year_option, quater_option, type_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, year, quater, transaction_type, sum(transaction_count) as transaction_count 
                            from aggregated_transaction
                            where year=%s and quater=%s and transaction_type=%s
                            group by transaction_type, quater, year, state
                            order by state asc;""", (year_option, quater_option, type_option))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(s, columns=[
                            'State Original', 'Year', 'Quater', 'Transaction Type', 'Transaction Count'], index=i)
        state_original = data['State Original'].tolist()
        data['State'] = state.state_list(state_original)
        data = data.sort_values(by='Transaction Count', ascending=False)
        data = data.rename_axis('S.No')
        data['Transaction'] = data['Transaction Count'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectyear_selectquater_selecttype_state_wise_total_transaction_amount(year_option, quater_option, type_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, year, quater, transaction_type, sum(transaction_amount) as transaction_amount 
                            from aggregated_transaction
                            where year=%s and quater=%s and transaction_type=%s
                            group by transaction_type, quater, year, state
                            order by state asc;""", (year_option, quater_option, type_option))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(s, columns=[
                            'State Original', 'Year', 'Quater', 'Transaction Type', 'Transaction Amount'], index=i)
        state_original = data['State Original'].tolist()
        data['State'] = state.state_list(state_original)
        data = data.sort_values(by='Transaction Amount', ascending=False)
        data['Transaction Amount'] = data['Transaction Amount'].apply(
            lambda x: int(round(x, 0)))
        data = data.rename_axis('S.No')
        data['Transaction'] = data['Transaction Amount'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectstate_selectyear_selectquater_selecttype_wise_total_transaction_count(state_option, year_option, quater_option, type_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, year, quater, transaction_type, sum(transaction_count) as transaction_count 
                        from aggregated_transaction
                        where state=%s and year=%s and quater=%s and transaction_type=%s
                        group by transaction_type, quater, year, state;""", (state_option, year_option, quater_option, type_option))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['State', 'Year', 'Quater', 'Transaction Type', 'Transaction Count'], index=i)
        data = data.rename_axis('S.No')
        data['Transaction'] = data['Transaction Count'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectstate_selectyear_selectquater_selecttype_wise_total_transaction_amount(state_option, year_option, quater_option, type_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, year, quater, transaction_type, sum(transaction_amount) as transaction_amount 
                        from aggregated_transaction
                        where state=%s and year=%s and quater=%s and transaction_type=%s
                        group by transaction_type, quater, year, state;""", (state_option, year_option, quater_option, type_option))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['State', 'Year', 'Quater', 'Transaction Type', 'Transaction Amount'], index=i)
        data['Transaction Amount'] = data['Transaction Amount'].apply(
            lambda x: int(round(x, 0)))
        data = data.rename_axis('S.No')
        data['Transaction'] = data['Transaction Amount'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data


class aggregated_user:

    # india map

    def state_wise_total_user_count():
        gopi = psycopg2.connect(host='localhost', user='postgres',
                                password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, sum(user_count) as user_count 
                            from aggregated_user
                            group by state
                            order by state asc;""")
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['State Original', 'User Count'], index=i)
        state_original = data['State Original'].tolist()
        data['State'] = state.state_list(state_original)
        data = data.sort_values(by='User Count', ascending=False)
        data = data.rename_axis('S.No')
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectyear_state_wise_total_user_count(year_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, year, sum(user_count) as user_count 
                                from aggregated_user
                                where year=%s
                                group by year, state
                                order by state asc;""", (year_option,))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['State Original', 'Year', 'User Count'], index=i)
        state_original = data['State Original'].tolist()
        data['State'] = state.state_list(state_original)
        data = data.sort_values(by='User Count', ascending=False)
        data = data.rename_axis('S.No')
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectquater_state_wise_total_user_count(quater_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, quater, sum(user_count) as user_count 
                                from aggregated_user
                                where quater=%s
                                group by quater, state
                                order by state asc;""", (quater_option,))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['State Original', 'Quater', 'User Count'], index=i)
        state_original = data['State Original'].tolist()
        data['State'] = state.state_list(state_original)
        data = data.sort_values(by='User Count', ascending=False)
        data = data.rename_axis('S.No')
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectbrand_state_wise_total_user_count(brand_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, user_brand, sum(user_count) as user_count 
                                from aggregated_user
                                where user_brand=%s
                                group by user_brand, state
                                order by state asc;""", (brand_option,))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['State Original', 'User Brand', 'User Count'], index=i)
        state_original = data['State Original'].tolist()
        data['State'] = state.state_list(state_original)
        data = data.sort_values(by='User Count', ascending=False)
        data = data.rename_axis('S.No')
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    # select - state

    def selectstate_year_wise_total_user_count(state_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, year, sum(user_count) as user_count 
                                from aggregated_user
                                where state=%s
                                group by year, state
                                order by year asc;""", (state_option,))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['State', 'Year', 'User Count'], index=i)
        data = data.rename_axis('S.No')
        data['User'] = data['User Count'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectstate_quater_wise_total_user_count(state_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, quater, sum(user_count) as user_count 
                            from aggregated_user
                            where state=%s
                            group by quater, state
                            order by quater asc;""", (state_option,))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['State', 'Quater', 'User Count'], index=i)
        data = data.rename_axis('S.No')
        data['User'] = data['User Count'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectstate_brand_wise_total_user_count(state_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, user_brand, sum(user_count) as user_count 
                            from aggregated_user
                            where state=%s
                            group by user_brand, state
                            order by case when user_brand = 'Others' then 1 else 0 end, user_count desc;""", (state_option,))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['State', 'User Brand', 'User Count'], index=i)
        data = data.rename_axis('S.No')
        data['User'] = data['User Count'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectstate_year_quater_wise_total_user_count(state_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, year, quater, sum(user_count) as user_count 
                            from aggregated_user
                            where state=%s
                            group by quater, year, state
                            order by year, quater asc;""", (state_option,))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['State', 'Year', 'Quater', 'User Count'], index=i)
        data = data.rename_axis('S.No')
        data['User'] = data['User Count'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectstate_selectyear_quater_wise_total_user_count(state_option, year_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, year, quater, sum(user_count) as user_count 
                            from aggregated_user
                            where state=%s and year=%s
                            group by quater, year, state
                            order by quater asc;""", (state_option, year_option))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['State', 'Year', 'Quater', 'User Count'], index=i)
        data = data.rename_axis('S.No')
        data['User'] = data['User Count'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectstate_selectyear_brand_wise_total_user_count(state_option, year_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, year, user_brand, sum(user_count) as user_count 
                            from aggregated_user
                            where state=%s and year=%s
                            group by user_brand, year, state
                            order by case when user_brand = 'Others' then 1 else 0 end, user_count desc;""", (state_option, year_option))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['State', 'Year', 'User Brand', 'User Count'], index=i)
        data = data.rename_axis('S.No')
        data['User'] = data['User Count'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectstate_selectquater_year_wise_total_user_count(state_option, quater_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, year, quater, sum(user_count) as user_count 
                            from aggregated_user
                            where state=%s and quater=%s
                            group by quater, year, state
                            order by year asc;""", (state_option, quater_option))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['State', 'Year', 'Quater', 'User Count'], index=i)
        data = data.rename_axis('S.No')
        data['User'] = data['User Count'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectstate_selectquater_brand_wise_total_user_count(state_option, quater_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, quater, user_brand, sum(user_count) as user_count 
                            from aggregated_user
                            where state=%s and quater=%s
                            group by user_brand, quater, state
                            order by case when user_brand = 'Others' then 1 else 0 end, user_count desc;""", (state_option, quater_option))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['State', 'Quater', 'User Brand', 'User Count'], index=i)
        data = data.rename_axis('S.No')
        data['User'] = data['User Count'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectstate_selectyear_selectquater_brand_wise_total_user_count(state_option, year_option, quater_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, year, quater, user_brand, user_count 
                            from aggregated_user
                            where state=%s and year=%s and quater=%s
                            order by case when user_brand = 'Others' then 1 else 0 end, user_count desc;""", (state_option, year_option, quater_option))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['State', 'Year', 'Quater', 'User Brand', 'User Count'], index=i)
        data = data.rename_axis('S.No')
        data['User'] = data['User Count'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectstate_selectyear_quater_brand_wise_total_user_count(state_option, year_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, year, quater, user_brand, user_count 
                            from aggregated_user
                            where state=%s and year=%s
                            order by quater asc, case when user_brand = 'Others' then 1 else 0 end, user_count desc;""", (state_option, year_option))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['State', 'Year', 'Quater', 'User Brand', 'User Count'], index=i)
        data = data.rename_axis('S.No')
        data['User'] = data['User Count'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectstate_selectquater_year_brand_wise_total_user_count(state_option, quater_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, year, quater, user_brand, user_count 
                            from aggregated_user
                            where state=%s and quater=%s
                            order by year, case when user_brand = 'Others' then 1 else 0 end, user_count desc;""", (state_option, quater_option))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['State', 'Year', 'Quater', 'User Brand', 'User Count'], index=i)
        data = data.rename_axis('S.No')
        data['User'] = data['User Count'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data



    # year - wise

    def year_wise_total_user_count():
        gopi = psycopg2.connect(host='localhost', user='postgres',
                                password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select year, sum(user_count) as user_count 
                                from aggregated_user
                                group by year
                                order by year asc;""")
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(s, columns=['Year', 'User Count'], index=i)
        data = data.rename_axis('S.No')
        data['User'] = data['User Count'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def year_quater_wise_total_user_count():
        gopi = psycopg2.connect(host='localhost', user='postgres',
                                password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select year, quater, sum(user_count) as user_count 
                            from aggregated_user
                            group by quater, year
                            order by year, quater asc;""")
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['Year', 'Quater', 'User Count'], index=i)
        data = data.rename_axis('S.No')
        data['User'] = data['User Count'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def year_brand_wise_total_user_count():
        gopi = psycopg2.connect(host='localhost', user='postgres',
                                password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select year, user_brand, sum(user_count) as user_count 
                            from aggregated_user
                            group by user_brand, year
                            order by year asc, case when user_brand = 'Others' then 1 else 0 end, user_count desc;""")
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['Year', 'User Brand', 'User Count'], index=i)
        data = data.rename_axis('S.No')
        data['User'] = data['User Count'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data
    
    def quater_brand_wise_total_user_count():
        gopi = psycopg2.connect(host='localhost', user='postgres',
                                password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select quater, user_brand, sum(user_count) as user_count 
                            from aggregated_user
                            group by user_brand, quater
                            order by quater asc, case when user_brand = 'Others' then 1 else 0 end, user_count desc;""")
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['Quater', 'User Brand', 'User Count'], index=i)
        data = data.rename_axis('S.No')
        data['User'] = data['User Count'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data


    def selectyear_quater_wise_total_user_count(year_option):
        gopi = psycopg2.connect(host='localhost', user='postgres',
                                password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select year, quater, sum(user_count) as user_count 
                            from aggregated_user
                            where year=%s
                            group by quater, year
                            order by quater asc;""", (year_option,))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['Year', 'Quater', 'User Count'], index=i)
        data = data.rename_axis('S.No')
        data['User'] = data['User Count'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectyear_brand_wise_total_user_count(year_option):
        gopi = psycopg2.connect(host='localhost', user='postgres',
                                password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select year, user_brand, sum(user_count) as user_count 
                            from aggregated_user
                            where year=%s
                            group by user_brand, year
                            order by case when user_brand = 'Others' then 1 else 0 end, user_count desc;""", (year_option,))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['Year', 'User Brand', 'User Count'], index=i)
        data = data.rename_axis('S.No')
        data['User'] = data['User Count'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectyear_quater_brand_wise_total_user_count(year_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select year, quater, user_brand, sum(user_count) as user_count 
                            from aggregated_user
                            where year=%s
                            group by user_brand, quater, year
                            order by quater asc, case when user_brand = 'Others' then 1 else 0 end, user_count desc;""", (year_option,))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['Year', 'Quater', 'User Brand', 'User Count'], index=i)
        data = data.rename_axis('S.No')
        data['User'] = data['User Count'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data


    def selectyear_selectstate_brand_wise_total_user_count(state_option, year_option):
        gopi = psycopg2.connect(host='localhost', user='postgres',
                                password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, year, user_brand, sum(user_count) as user_count 
                                from aggregated_user
                                where state=%s and year=%s
                                group by user_brand, year, state
                                order by case when user_brand = 'Others' then 1 else 0 end, user_count desc;""", (state_option, year_option))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['State', 'Year', 'User Brand', 'User Count'], index=i)
        data = data.rename_axis('S.No')
        data['User'] = data['User Count'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectyear_selectstate_quater_brand_wise_total_user_count(state_option, year_option, quater_option):
        gopi = psycopg2.connect(host='localhost', user='postgres',
                                password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, year, quater, user_brand, sum(user_count) as user_count 
                                from aggregated_user
                                where state=%s and year=%s and quater=%s
                                group by user_brand, quater, year, state
                                order by case when user_brand = 'Others' then 1 else 0 end, user_count desc;""", (state_option, year_option, quater_option))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['State', 'Year', 'Quater', 'User Brand', 'User Count'], index=i)
        data = data.rename_axis('S.No')
        data['User'] = data['User Count'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectyear_selectquater_state_wise_total_user_count(year_option, quater_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, year, quater, sum(user_count) as user_count 
                            from aggregated_user
                            where year=%s and quater=%s
                            group by quater, year, state
                            order by state asc;""", (year_option, quater_option))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['State Original', 'Year', 'Quater', 'User Count'], index=i)
        state_original = data['State Original'].tolist()
        data['State'] = state.state_list(state_original)
        data = data.sort_values(by='User Count', ascending=False)
        data = data.rename_axis('S.No')
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectyear_selectquater_brand_wise_total_user_count(year_option, quater_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select year, quater, user_brand, sum(user_count) as user_count 
                            from aggregated_user
                            where year=%s and quater=%s
                            group by user_brand, quater, year
                            order by case when user_brand = 'Others' then 1 else 0 end, user_count desc;""", (year_option, quater_option))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['Year', 'Quater', 'User Brand', 'User Count'], index=i)
        data = data.rename_axis('S.No')
        data['User'] = data['User Count'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    # quater - wise

    def quater_wise_total_user_count():
        gopi = psycopg2.connect(host='localhost', user='postgres',
                                password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select quater, sum(user_count) as user_count 
                                from aggregated_user
                                group by quater
                                order by quater asc;""")
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(s, columns=['Quater', 'User Count'], index=i)
        data = data.rename_axis('S.No')
        data['User'] = data['User Count'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectquater_year_wise_total_user_count(quater_option):
        gopi = psycopg2.connect(host='localhost', user='postgres',
                                password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select year, quater, sum(user_count) as user_count 
                            from aggregated_user
                            where quater=%s
                            group by quater, year
                            order by year asc;""", (quater_option,))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['Year', 'Quater', 'User Count'], index=i)
        data = data.rename_axis('S.No')
        data['User'] = data['User Count'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectquater_brand_wise_total_user_count(quater_option):
        gopi = psycopg2.connect(host='localhost', user='postgres',
                                password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select quater, user_brand, sum(user_count) as user_count 
                            from aggregated_user
                            where quater=%s
                            group by user_brand, quater
                            order by case when user_brand = 'Others' then 1 else 0 end, user_count desc;""", (quater_option,))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['Quater', 'User Brand', 'User Count'], index=i)
        data = data.rename_axis('S.No')
        data['User'] = data['User Count'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectquater_year_brand_wise_total_user_count(quater_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select year, quater, user_brand, sum(user_count) as user_count 
                            from aggregated_user
                            where quater=%s
                            group by user_brand, quater, year
                            order by year asc, case when user_brand = 'Others' then 1 else 0 end, user_count desc;""", (quater_option,))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['Year', 'Quater', 'User Brand', 'User Count'], index=i)
        data = data.rename_axis('S.No')
        data['User'] = data['User Count'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data


    # brand - wise

    def brand_wise_total_user_count():
        gopi = psycopg2.connect(host='localhost', user='postgres',
                                password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select user_brand, sum(user_count) as user_count 
                            from aggregated_user
                            group by user_brand
                            order by case when user_brand = 'Others' then 1 else 0 end, user_count desc;""")
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['User Brand', 'User Count'], index=i)
        data = data.rename_axis('S.No')
        data['User'] = data['User Count'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectbrand_state_wise_total_user_count(brand_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, user_brand, sum(user_count) as user_count 
                            from aggregated_user
                            where user_brand=%s
                            group by user_brand, state
                            order by state asc;""", (brand_option,))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['State Original', 'User Brand', 'User Count'], index=i)
        state_original = data['State Original'].tolist()
        data['State'] = state.state_list(state_original)
        data = data.sort_values(by='User Count', ascending=False)
        data = data.rename_axis('S.No')
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectbrand_year_wise_total_user_count(brand_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select year, user_brand, sum(user_count) as user_count 
                            from aggregated_user
                            where user_brand=%s
                            group by user_brand, year
                            order by year asc;""", (brand_option,))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['Year', 'User Brand', 'User Count'], index=i)
        data = data.rename_axis('S.No')
        data['User'] = data['User Count'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectbrand_quater_wise_total_user_count(brand_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select quater, user_brand, sum(user_count) as user_count 
                            from aggregated_user
                            where user_brand=%s
                            group by user_brand, quater
                            order by user_count asc;""", (brand_option,))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['Quater', 'User Brand', 'User Count'], index=i)
        data = data.rename_axis('S.No')
        data['User'] = data['User Count'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectbrand_year_quater_wise_total_user_count(brand_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select year, quater, user_brand, sum(user_count) as user_count 
                            from aggregated_user
                            where user_brand=%s
                            group by user_brand, quater, year
                            order by year, quater asc;""", (brand_option,))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['Year', 'Quater', 'User Brand', 'User Count'], index=i)
        data = data.rename_axis('S.No')
        data['User'] = data['User Count'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectstate_selectbrand_year_wise_total_user_count(state_option, brand_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, year, user_brand, sum(user_count) as user_count 
                            from aggregated_user
                            where state=%s and user_brand=%s
                            group by user_brand, year, state
                            order by year asc;""", (state_option, brand_option))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['State', 'Year', 'User Brand', 'User Count'], index=i)
        data = data.rename_axis('S.No')
        data['User'] = data['User Count'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectstate_selectbrand_quater_wise_total_user_count(state_option, brand_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, quater, user_brand, sum(user_count) as user_count 
                            from aggregated_user
                            where state=%s and user_brand=%s
                            group by user_brand, quater, state
                            order by quater asc;""", (state_option, brand_option))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['State', 'Quater', 'User Brand', 'User Count'], index=i)
        data = data.rename_axis('S.No')
        data['User'] = data['User Count'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectstate_selectbrand_year_quater_wise_total_user_count(state_option, brand_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, year, quater, user_brand, sum(user_count) as user_count 
                            from aggregated_user
                            where state=%s and user_brand=%s
                            group by user_brand, quater, year, state
                            order by year, quater asc;""", (state_option, brand_option))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['State', 'Year', 'Quater', 'User Brand', 'User Count'], index=i)
        data = data.rename_axis('S.No')
        data['User'] = data['User Count'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectyear_selectbrand_state_wise_total_user_count(year_option, brand_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, year, user_brand, sum(user_count) as user_count 
                            from aggregated_user
                            where year=%s and user_brand=%s
                            group by user_brand, year, state
                            order by state asc;""", (year_option, brand_option))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['State Original', 'Year', 'User Brand', 'User Count'], index=i)
        state_original = data['State Original'].tolist()
        data['State'] = state.state_list(state_original)
        data = data.sort_values(by='User Count', ascending=False)
        data = data.rename_axis('S.No')
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectyear_selectbrand_quater_wise_total_user_count(year_option, brand_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select year, quater, user_brand, sum(user_count) as user_count 
                            from aggregated_user
                            where year=%s and user_brand=%s
                            group by user_brand, quater, year
                            order by quater asc;""", (year_option, brand_option))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['Year', 'Quater', 'User Brand', 'User Count'], index=i)
        data = data.rename_axis('S.No')
        data['User'] = data['User Count'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectquater_selectbrand_state_wise_total_user_count(quater_option, brand_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, quater, user_brand, sum(user_count) as user_count 
                            from aggregated_user
                            where quater=%s and user_brand=%s
                            group by user_brand, quater, state
                            order by state asc;""", (quater_option, brand_option))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['State Original', 'Quater', 'User Brand', 'User Count'], index=i)
        state_original = data['State Original'].tolist()
        data['State'] = state.state_list(state_original)
        data = data.sort_values(by='User Count', ascending=False)
        data = data.rename_axis('S.No')
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectquater_selectbrand_year_wise_total_user_count(quater_option, brand_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select year, quater, user_brand, sum(user_count) as user_count 
                            from aggregated_user
                            where quater=%s and user_brand=%s
                            group by user_brand, quater, year
                            order by year asc;""", (quater_option, brand_option))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['Year', 'Quater', 'User Brand', 'User Count'], index=i)
        data = data.rename_axis('S.No')
        data['User'] = data['User Count'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectstate_selectyear_selectbrand_quater_wise_total_user_count(state_option, year_option, brand_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, year, quater, user_brand, sum(user_count) as user_count 
                            from aggregated_user
                            where state=%s and year=%s and user_brand=%s
                            group by user_brand, quater, year, state
                            order by quater asc;""", (state_option, year_option, brand_option))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['State', 'Year', 'Quater', 'User Brand', 'User Count'], index=i)
        data = data.rename_axis('S.No')
        data['User'] = data['User Count'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectstate_selectquater_selectbrand_year_wise_total_user_count(state_option, quater_option, brand_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, year, quater, user_brand, sum(user_count) as user_count 
                            from aggregated_user
                            where state=%s and quater=%s and user_brand=%s
                            group by user_brand, quater, year, state
                            order by year asc;""", (state_option, quater_option, brand_option))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['State', 'Year', 'Quater', 'User Brand', 'User Count'], index=i)
        data = data.rename_axis('S.No')
        data['User'] = data['User Count'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectyear_selectquater_selectbrand_state_wise_total_user_count(year_option, quater_option, brand_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, year, quater, user_brand, sum(user_count) as user_count 
                            from aggregated_user
                            where year=%s and quater=%s and user_brand=%s
                            group by user_brand, quater, year, state
                            order by state asc;""", (year_option, quater_option, brand_option))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(s, columns=[
                            'State Original', 'Year', 'Quater', 'User Brand', 'User Count'], index=i)
        state_original = data['State Original'].tolist()
        data['State'] = state.state_list(state_original)
        data = data.sort_values(by='User Count', ascending=False)
        data = data.rename_axis('S.No')
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectstate_selectyear_selectquater_selectbrand_wise_total_user_count(state_option, year_option, quater_option, brand_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, year, quater, user_brand, sum(user_count) as user_count 
                        from aggregated_user
                        where state=%s and year=%s and quater=%s and user_brand=%s
                        group by user_brand, quater, year, state;""", (state_option, year_option, quater_option, brand_option))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['State', 'Year', 'Quater', 'User Brand', 'User Count'], index=i)
        data = data.rename_axis('S.No')
        data['User'] = data['User Count'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data


class map_transaction:

    # map - transaction

    def selectstate_district_wise_total_transaction_count(state_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, district, sum(transaction_count) as transaction_count 
                            from map_transaction
                            where state=%s
                            group by district, state
                            order by transaction_count desc;""", (state_option,))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['State', 'District', 'Transaction Count'], index=i)
        data = data.rename_axis('S.No')
        data['Formatted Value'] = data['Transaction Count'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectstate_district_wise_total_transaction_amount(state_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, district, sum(transaction_amount) as transaction_amount 
                            from map_transaction
                            where state=%s
                            group by district, state
                            order by transaction_amount desc;""", (state_option,))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['State', 'District', 'Transaction Amount'], index=i)
        data['Transaction Amount'] = data['Transaction Amount'].apply(
            lambda x: int(round(x, 0)))
        data = data.rename_axis('S.No')
        data['Formatted Value'] = data['Transaction Amount'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectstate_selectyear_district_wise_total_transaction_count(state_option, year_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, year, district, sum(transaction_count) as transaction_count 
                            from map_transaction
                            where state=%s and year=%s
                            group by district, year, state
                            order by year asc, transaction_count desc;""", (state_option, year_option))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['State', 'Year', 'District', 'Transaction Count'], index=i)
        data = data.rename_axis('S.No')
        data['Formatted Value'] = data['Transaction Count'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectstate_selectyear_district_wise_total_transaction_amount(state_option, year_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, year, district, sum(transaction_amount) as transaction_amount 
                            from map_transaction
                            where state=%s and year=%s
                            group by district, year, state
                            order by year asc, transaction_amount desc;""", (state_option, year_option))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['State', 'Year', 'District', 'Transaction Amount'], index=i)
        data['Transaction Amount'] = data['Transaction Amount'].apply(
            lambda x: int(round(x, 0)))
        data = data.rename_axis('S.No')
        data['Formatted Value'] = data['Transaction Amount'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectstate_selectquater_district_wise_total_transaction_count(state_option, quater_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, quater, district, sum(transaction_count) as transaction_count 
                            from map_transaction
                            where state=%s and quater=%s
                            group by district, quater, state
                            order by quater asc, transaction_count desc;""", (state_option, quater_option))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['State', 'Quater', 'District', 'Transaction Count'], index=i)
        data = data.rename_axis('S.No')
        data['Formatted Value'] = data['Transaction Count'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectstate_selectquater_district_wise_total_transaction_amount(state_option, quater_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, quater, district, sum(transaction_amount) as transaction_amount 
                            from map_transaction
                            where state=%s and quater=%s
                            group by district, quater, state
                            order by quater asc, transaction_amount desc;""", (state_option, quater_option))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['State', 'Quater', 'District', 'Transaction Amount'], index=i)
        data['Transaction Amount'] = data['Transaction Amount'].apply(
            lambda x: int(round(x, 0)))
        data = data.rename_axis('S.No')
        data['Formatted Value'] = data['Transaction Amount'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectstate_selectyear_selectquater_district_wise_total_transaction_count(state_option, year_option, quater_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, year, quater, district, sum(transaction_count) as transaction_count 
                            from map_transaction
                            where state=%s and year=%s and quater=%s
                            group by district, quater, year, state
                            order by year, quater asc, transaction_count desc;""", (state_option, year_option, quater_option))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['State', 'Year', 'Quater', 'District', 'Transaction Count'], index=i)
        data = data.rename_axis('S.No')
        data['Formatted Value'] = data['Transaction Count'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectstate_selectyear_selectquater_district_wise_total_transaction_amount(state_option, year_option, quater_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, year, quater, district, sum(transaction_amount) as transaction_amount 
                            from map_transaction
                            where state=%s and year=%s and quater=%s
                            group by district, quater, year, state
                            order by year, quater asc, transaction_amount desc;""", (state_option, year_option, quater_option))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['State', 'Year', 'Quater', 'District', 'Transaction Amount'], index=i)
        data['Transaction Amount'] = data['Transaction Amount'].apply(
            lambda x: int(round(x, 0)))
        data = data.rename_axis('S.No')
        data['Formatted Value'] = data['Transaction Amount'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data


class map_user:
    # india map

    def state_wise_total_registered_user():
        gopi = psycopg2.connect(host='localhost', user='postgres',
                                password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, sum(registered_user) as registered_user 
                                from map_user
                                group by state
                                order by state asc;""")
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['State Original', 'Registered Users'], index=i)
        state_original = data['State Original'].tolist()
        data['State'] = state.state_list(state_original)
        data = data.sort_values(by='Registered Users', ascending=False)
        data = data.rename_axis('S.No')
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def state_wise_total_app_opens():
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, sum(app_opens) as app_opens 
                                from map_user
                                group by state
                                order by state asc;""")
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['State Original', 'App Opens'], index=i)
        state_original = data['State Original'].tolist()
        data['State'] = state.state_list(state_original)
        data = data.sort_values(by='App Opens', ascending=False)
        data = data.rename_axis('S.No')
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectyear_state_wise_total_registered_user(year_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, year, sum(registered_user) as registered_user 
                                from map_user
                                where year=%s
                                group by year, state
                                order by state asc;""", (year_option,))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['State Original', 'Year', 'Registered Users'], index=i)
        state_original = data['State Original'].tolist()
        data['State'] = state.state_list(state_original)
        data = data.sort_values(by='Registered Users', ascending=False)
        data = data.rename_axis('S.No')
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectyear_state_wise_total_app_opens(year_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, year, sum(app_opens) as app_opens 
                                from map_user
                                where year=%s
                                group by year, state
                                order by state asc;""", (year_option,))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['State Original', 'Year', 'App Opens'], index=i)
        state_original = data['State Original'].tolist()
        data['State'] = state.state_list(state_original)
        data = data.sort_values(by='App Opens', ascending=False)
        data = data.rename_axis('S.No')
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectquater_state_wise_total_registered_user(quater_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, quater, sum(registered_user) as registered_user 
                                from map_user
                                where quater=%s
                                group by quater, state
                                order by state asc;""", (quater_option,))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['State Original', 'Quater', 'Registered Users'], index=i)
        state_original = data['State Original'].tolist()
        data['State'] = state.state_list(state_original)
        data = data.sort_values(by='Registered Users', ascending=False)
        data = data.rename_axis('S.No')
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectquater_state_wise_total_app_opens(quater_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, quater, sum(app_opens) as app_opens 
                                from map_user
                                where quater=%s
                                group by quater, state
                                order by state asc;""", (quater_option,))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['State Original', 'Quater', 'App Opens'], index=i)
        state_original = data['State Original'].tolist()
        data['State'] = state.state_list(state_original)
        data = data.sort_values(by='App Opens', ascending=False)
        data = data.rename_axis('S.No')
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    # select - state

    def selectstate_year_wise_total_registered_user(state_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, year, sum(registered_user) as registered_user 
                                from map_user
                                where state=%s
                                group by year, state
                                order by year asc;""", (state_option,))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['State', 'Year', 'Registered Users'], index=i)
        data = data.rename_axis('S.No')
        data['Register'] = data['Registered Users'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectstate_year_wise_total_app_opens(state_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, year, sum(app_opens) as app_opens 
                                from map_user
                                where state=%s
                                group by year, state
                                order by year asc;""", (state_option,))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['State', 'Year', 'App Opens'], index=i)
        data = data.rename_axis('S.No')
        data['App'] = data['App Opens'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectstate_quater_wise_total_registered_user(state_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, quater, sum(registered_user) as registered_user 
                            from map_user
                            where state=%s
                            group by quater, state
                            order by quater asc;""", (state_option,))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['State', 'Quater', 'Registered Users'], index=i)
        data = data.rename_axis('S.No')
        data['Register'] = data['Registered Users'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectstate_quater_wise_total_app_opens(state_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, quater, sum(app_opens) as app_opens 
                                from map_user
                                where state=%s
                                group by quater, state
                                order by quater asc;""", (state_option,))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['State', 'Quater', 'App Opens'], index=i)
        data = data.rename_axis('S.No')
        data['App'] = data['App Opens'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectstate_year_quater_wise_total_registered_user(state_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, year, quater, sum(registered_user) as registered_user 
                            from map_user
                            where state=%s
                            group by quater, year, state
                            order by year, quater asc;""", (state_option,))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['State', 'Year', 'Quater', 'Registered Users'], index=i)
        data = data.rename_axis('S.No')
        data['Register'] = data['Registered Users'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectstate_year_quater_wise_total_app_opens(state_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, year, quater, sum(app_opens) as app_opens 
                                from map_user
                                where state=%s
                                group by quater, year, state
                                order by year, quater asc;""", (state_option,))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['State', 'Year', 'Quater', 'App Opens'], index=i)
        data = data.rename_axis('S.No')
        data['App'] = data['App Opens'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectstate_selectyear_quater_wise_total_registered_user(state_option, year_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, year, quater, sum(registered_user) as registered_user 
                            from map_user
                            where state=%s and year=%s
                            group by quater, year, state
                            order by quater asc;""", (state_option, year_option))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['State', 'Year', 'Quater', 'Registered Users'], index=i)
        data = data.rename_axis('S.No')
        data['Register'] = data['Registered Users'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectstate_selectyear_quater_wise_total_app_opens(state_option, year_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, year, quater, sum(app_opens) as app_opens 
                                from map_user
                                where state=%s and year=%s
                                group by quater, year, state
                                order by quater asc;""", (state_option, year_option))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['State', 'Year', 'Quater', 'App Opens'], index=i)
        data = data.rename_axis('S.No')
        data['App'] = data['App Opens'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectstate_selectquater_year_wise_total_registered_user(state_option, quater_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, year, quater, sum(registered_user) as registered_user 
                            from map_user
                            where state=%s and quater=%s
                            group by quater, year, state
                            order by year asc;""", (state_option, quater_option))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['State', 'Year', 'Quater', 'Registered Users'], index=i)
        data = data.rename_axis('S.No')
        data['Register'] = data['Registered Users'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectstate_selectquater_year_wise_total_app_opens(state_option, quater_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, year, quater, sum(app_opens) as app_opens 
                            from map_user
                            where state=%s and quater=%s
                            group by quater, year, state
                            order by year asc;""", (state_option, quater_option))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['State', 'Year', 'Quater', 'App Opens'], index=i)
        data = data.rename_axis('S.No')
        data['App'] = data['App Opens'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    # year - wise

    def year_wise_total_registered_user():
        gopi = psycopg2.connect(host='localhost', user='postgres',
                                password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select year, sum(registered_user) as registered_user 
                                from map_user
                                group by year
                                order by year asc;""")
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(s, columns=['Year', 'Registered Users'], index=i)
        data = data.rename_axis('S.No')
        data['Register'] = data['Registered Users'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def year_wise_total_app_opens():
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select year, sum(app_opens) as app_opens 
                                from map_user
                                group by year
                                order by year asc;""")
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['Year', 'App Opens'], index=i)
        data = data.rename_axis('S.No')
        data['App'] = data['App Opens'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def year_quater_wise_total_registered_user():
        gopi = psycopg2.connect(host='localhost', user='postgres',
                                password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select year, quater, sum(registered_user) as registered_user 
                                from map_user
                                group by quater, year
                                order by year, quater asc;""")
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['Year', 'Quater', 'Registered Users'], index=i)
        data = data.rename_axis('S.No')
        data['Register'] = data['Registered Users'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def year_quater_wise_total_app_opens():
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select year, quater, sum(app_opens) as app_opens 
                                from map_user
                                group by quater, year
                                order by year, quater asc;""")
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['Year', 'Quater', 'App Opens'], index=i)
        data = data.rename_axis('S.No')
        data['App'] = data['App Opens'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectyear_quater_wise_total_registered_user(year_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select year, quater, sum(registered_user) as registered_user 
                            from map_user
                            where year=%s
                            group by quater, year
                            order by quater asc;""", (year_option,))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['Year', 'Quater', 'Registered Users'], index=i)
        data = data.rename_axis('S.No')
        data['Register'] = data['Registered Users'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectyear_quater_wise_total_app_opens(year_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select year, quater, sum(app_opens) as app_opens 
                                from map_user
                                where year=%s
                                group by quater, year
                                order by quater asc;""", (year_option,))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['Year', 'Quater', 'App Opens'], index=i)
        data = data.rename_axis('S.No')
        data['App'] = data['App Opens'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectyear_selectquater_state_wise_total_registered_user(year_option, quater_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, year, quater, sum(registered_user) as registered_user 
                                from map_user
                                where year=%s and quater=%s
                                group by quater, year, state
                                order by state asc;""", (year_option, quater_option))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['State Original', 'Year', 'Quater', 'Registered Users'], index=i)
        state_original = data['State Original'].tolist()
        data['State'] = state.state_list(state_original)
        data = data.sort_values(by='Registered Users', ascending=False)
        data = data.rename_axis('S.No')
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectyear_selectquater_state_wise_total_app_opens(year_option, quater_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, year, quater, sum(app_opens) as app_opens 
                                from map_user
                                where year=%s and quater=%s
                                group by quater, year, state
                                order by state asc;""", (year_option, quater_option))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['State Original', 'Quater', 'Year', 'App Opens'], index=i)
        state_original = data['State Original'].tolist()
        data['State'] = state.state_list(state_original)
        data = data.sort_values(by='App Opens', ascending=False)
        data = data.rename_axis('S.No')
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    # quater - wise

    def quater_wise_total_registered_user():
        gopi = psycopg2.connect(host='localhost', user='postgres',
                                password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select quater, sum(registered_user) as registered_user 
                                from map_user
                                group by quater
                                order by quater asc;""")
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['Quater', 'Registered Users'], index=i)
        data = data.rename_axis('S.No')
        data['Register'] = data['Registered Users'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def quater_wise_total_app_opens():
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select quater, sum(app_opens) as app_opens 
                                from map_user
                                group by quater
                                order by quater asc;""")
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['Quater', 'App Opens'], index=i)
        data = data.rename_axis('S.No')
        data['App'] = data['App Opens'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectquater_year_wise_total_registered_user(quater_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select year, quater, sum(registered_user) as registered_user 
                            from map_user
                            where quater=%s
                            group by quater, year
                            order by year asc;""", (quater_option,))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['Year', 'Quater', 'Registered Users'], index=i)
        data = data.rename_axis('S.No')
        data['Register'] = data['Registered Users'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectquater_year_wise_total_app_opens(quater_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select year, quater, sum(app_opens) as app_opens 
                            from map_user
                            where quater=%s
                            group by quater, year
                            order by year asc;""", (quater_option,))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['Year', 'Quater', 'App Opens'], index=i)
        data = data.rename_axis('S.No')
        data['App'] = data['App Opens'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    # district - wise

    def selectstate_district_wise_total_registered_user(state_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, district, sum(registered_user) as registered_user 
                                from map_user
                                where state=%s
                                group by district, state
                                order by registered_user desc;""", (state_option,))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['State', 'District', 'Registered Users'], index=i)
        data = data.rename_axis('S.No')
        data['Formatted Value'] = data['Registered Users'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectstate_district_wise_total_app_opens(state_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, district, sum(app_opens) as app_opens 
                                from map_user
                                where state=%s
                                group by district, state
                                order by app_opens desc;""", (state_option,))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['State', 'District', 'App Opens'], index=i)
        data = data.rename_axis('S.No')
        data['Formatted Value'] = data['App Opens'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectstate_selectyear_district_wise_total_registered_user(state_option, year_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, year, district, sum(registered_user) as registered_user 
                                from map_user
                                where state=%s and year=%s
                                group by district, year, state
                                order by year asc, registered_user desc;""", (state_option, year_option))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['State', 'Year', 'District', 'Registered Users'], index=i)
        data = data.rename_axis('S.No')
        data['Formatted Value'] = data['Registered Users'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectstate_selectyear_district_wise_total_app_opens(state_option, year_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, year, district, sum(app_opens) as app_opens 
                                from map_user
                                where state=%s and year=%s
                                group by district, year, state
                                order by year asc, app_opens desc;""", (state_option, year_option))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['State', 'Year', 'District', 'App Opens'], index=i)
        data = data.rename_axis('S.No')
        data['Formatted Value'] = data['App Opens'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectstate_selectquater_district_wise_total_registered_user(state_option, quater_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, quater, district, sum(registered_user) as registered_user 
                                from map_user
                                where state=%s and quater=%s
                                group by district, quater, state
                                order by quater asc, registered_user desc;""", (state_option, quater_option))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['State', 'Quater', 'District', 'Registered Users'], index=i)
        data = data.rename_axis('S.No')
        data['Formatted Value'] = data['Registered Users'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectstate_selectquater_district_wise_total_app_opens(state_option, quater_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, quater, district, sum(app_opens) as app_opens 
                                from map_user
                                where state=%s and quater=%s
                                group by district, quater, state
                                order by quater asc, app_opens desc;""", (state_option, quater_option))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['State', 'Quater', 'District', 'App Opens'], index=i)
        data = data.rename_axis('S.No')
        data['Formatted Value'] = data['App Opens'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectstate_selectyear_selectquater_district_wise_total_registered_user(state_option, year_option, quater_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, year, quater, district, sum(registered_user) as registered_user 
                                from map_user
                                where state=%s and year=%s and quater=%s
                                group by district, quater, year, state
                                order by year, quater asc, registered_user desc;""", (state_option, year_option, quater_option))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['State', 'Year', 'Quater', 'District', 'Registered Users'], index=i)
        data = data.rename_axis('S.No')
        data['Formatted Value'] = data['Registered Users'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectstate_selectyear_selectquater_district_wise_total_app_opens(state_option, year_option, quater_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, year, quater, district, sum(app_opens) as app_opens 
                                from map_user
                                where state=%s and year=%s and quater=%s
                                group by district, quater, year, state
                                order by year, quater asc, app_opens desc;""", (state_option, year_option, quater_option))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['State', 'Year', 'Quater', 'District', 'App Opens'], index=i)
        data = data.rename_axis('S.No')
        data['Formatted Value'] = data['App Opens'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data


class top_transaction_and_user:

# transaction

    def selectyear_selectquater_state_wise_top10_transaction(year_option, quater_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, year, quater, sum(transaction_count) as transaction_count, sum(transaction_amount) as transaction_amount 
                            from aggregated_transaction
                            where year=%s and quater=%s
                            group by quater, year, state
                            order by transaction_amount desc
                            limit 10;""", (year_option, quater_option))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['State', 'Year', 'Quater', 'Transaction Count', 'Transaction Amount'], index=i)
        data['Transaction Amount'] = data['Transaction Amount'].apply(
            lambda x: int(round(x, 0)))
        data = data.rename_axis('S.No')
        data['Count'] = data['Transaction Count'].apply(lambda x: convert.rupees(str(x)))
        data['Amount'] = data['Transaction Amount'].apply(lambda x: convert.rupees(str(x)))
        data['Avg. Transaction Value'] = data['Transaction Amount'] // data['Transaction Count']
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectyear_selectquater_district_wise_top10_transaction(year_option, quater_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, year, quater, district, transaction_count, transaction_amount
                            from top_transaction_district
                            where year=%s and quater=%s
                            group by transaction_amount, transaction_count, district, quater, year, state
                            order by transaction_amount desc
                            limit 10;""", (year_option, quater_option))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['State', 'Year', 'Quater', 'District', 'Transaction Count', 'Transaction Amount'], index=i)
        data['Transaction Amount'] = data['Transaction Amount'].apply(
            lambda x: int(round(x, 0)))
        data = data.rename_axis('S.No')
        data['Count'] = data['Transaction Count'].apply(lambda x: convert.rupees(str(x)))
        data['Amount'] = data['Transaction Amount'].apply(lambda x: convert.rupees(str(x)))
        data['Avg. Transaction Value'] = data['Transaction Amount'] // data['Transaction Count']
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        data['District_State'] = data['District'] + '@@@(' + data['State'] + ')'
        return data

    def selectyear_selectquater_pincode_wise_top10_transaction(year_option, quater_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, year, quater, pincode, sum(transaction_count) as transaction_count, sum(transaction_amount) as transaction_amount
                            from top_transaction_pincode
                            where year=%s and quater=%s
                            group by pincode, quater, year, state
                            order by transaction_amount desc
                            limit 10;""", (year_option, quater_option))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(s, columns=['State', 'Year', 'Quater', 'Pincode', 'Transaction Count', 'Transaction Amount'], index=i)
        data['Transaction Amount'] = data['Transaction Amount'].apply(lambda x: int(round(x, 0)))
        data = data.rename_axis('S.No')
        data['Count'] = data['Transaction Count'].apply(lambda x: convert.rupees(str(x)))
        data['Amount'] = data['Transaction Amount'].apply(lambda x: convert.rupees(str(x)))
        data['Avg. Transaction Value'] = data['Transaction Amount'] // data['Transaction Count']
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        data['District'] = data['Pincode'].apply(lambda x: find_district(x))
        data['Pincode_District_State'] = data['Pincode'] + '@@@(' + data['District'] + ' -@@@' + data['State'] + ')'
        return data

    def selectstate_selectyear_selectquater_district_wise_top10_transaction(state_option, year_option, quater_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, year, quater, district, transaction_count, transaction_amount
                            from top_transaction_district
                            where state=%s and year=%s and quater=%s
                            group by transaction_amount, transaction_count, district, quater, year, state
                            order by transaction_amount desc
                            limit 10;""", (state_option, year_option, quater_option))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['State', 'Year', 'Quater', 'District', 'Transaction Count', 'Transaction Amount'], index=i)
        data['Transaction Amount'] = data['Transaction Amount'].apply(
            lambda x: int(round(x, 0)))
        data = data.rename_axis('S.No')
        data['Count'] = data['Transaction Count'].apply(lambda x: convert.rupees(str(x)))
        data['Amount'] = data['Transaction Amount'].apply(lambda x: convert.rupees(str(x)))
        data['Avg. Transaction Value'] = data['Transaction Amount'] // data['Transaction Count']
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectstate_selectyear_selectquater_pincode_wise_top10_transaction(state_option, year_option, quater_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, year, quater, pincode, sum(transaction_count) as transaction_count, sum(transaction_amount) as transaction_amount
                            from top_transaction_pincode
                            where state=%s and year=%s and quater=%s
                            group by pincode, quater, year, state
                            order by transaction_amount desc
                            limit 10;""", (state_option, year_option, quater_option))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(s, columns=['State', 'Year', 'Quater', 'Pincode', 'Transaction Count', 'Transaction Amount'], index=i)
        data['Transaction Amount'] = data['Transaction Amount'].apply(lambda x: int(round(x, 0)))
        data = data.rename_axis('S.No')
        data['Count'] = data['Transaction Count'].apply(lambda x: convert.rupees(str(x)))
        data['Amount'] = data['Transaction Amount'].apply(lambda x: convert.rupees(str(x)))
        data['Avg. Transaction Value'] = data['Transaction Amount'] // data['Transaction Count']
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        data['District'] = data['Pincode'].apply(lambda x: find_district(x))
        data['Pincode_District'] = data['Pincode'] + '@@@(' + data['District'] + ')'
        return data


# user

    def selectyear_selectquater_state_wise_top10_user(year_option, quater_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, year, quater, sum(user_count) as user_count 
                            from aggregated_user
                            where year=%s and quater=%s
                            group by quater, year, state
                            order by user_count desc
                            limit 10;""", (year_option, quater_option))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['State', 'Year', 'Quater', 'User Count'], index=i)
        data = data.rename_axis('S.No')
        data['User'] = data['User Count'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectyear_selectquater_district_wise_top10_user(year_option, quater_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, year, quater, district, registered_user
                            from top_user_district
                            where year=%s and quater=%s
                            group by registered_user, district, quater, year, state
                            order by registered_user desc
                            limit 10;""", (year_option, quater_option))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['State', 'Year', 'Quater', 'District', 'User Count'], index=i)
        data = data.rename_axis('S.No')
        data['User'] = data['User Count'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        data['District_State'] = data['District'] + '@@@(' + data['State'] + ')'
        return data

    def selectyear_selectquater_pincode_wise_top10_user(year_option, quater_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, year, quater, pincode, sum(registered_user) as registered_user
                            from top_user_pincode
                            where year=%s and quater=%s
                            group by pincode, quater, year, state
                            order by registered_user desc
                            limit 10;""", (year_option, quater_option))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['State', 'Year', 'Quater', 'Pincode', 'User Count'], index=i)
        data = data.rename_axis('S.No')
        data['User'] = data['User Count'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        data['District'] = data['Pincode'].apply(lambda x: find_district(x))
        data['Pincode_District_State'] = data['Pincode'] + '@@@(' + data['District'] + ' -@@@' + data['State'] + ')'
        return data

    def selectstate_selectyear_selectquater_district_wise_top10_user(state_option, year_option, quater_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, year, quater, district, registered_user
                            from top_user_district
                            where state=%s and year=%s and quater=%s
                            group by registered_user, district, quater, year, state
                            order by registered_user desc
                            limit 10;""", (state_option, year_option, quater_option))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['State', 'Year', 'Quater', 'District', 'User Count'], index=i)
        data = data.rename_axis('S.No')
        data['User'] = data['User Count'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def selectstate_selectyear_selectquater_pincode_wise_top10_user(state_option, year_option, quater_option):
        gopi = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='phonepe')
        cursor = gopi.cursor()
        cursor.execute(f"""select state, year, quater, pincode, sum(registered_user) as registered_user
                            from top_user_pincode
                            where state=%s and year=%s and quater=%s
                            group by pincode, quater, year, state
                            order by registered_user desc
                            limit 10;""", (state_option, year_option, quater_option))
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['State', 'Year', 'Quater', 'Pincode', 'User Count'], index=i)
        data = data.rename_axis('S.No')
        data['User'] = data['User Count'].apply(lambda x: convert.rupees(str(x)))
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        data['District'] = data['Pincode'].apply(lambda x: find_district(x))
        data['Pincode_District'] = data['Pincode'] + '@@@(' + data['District'] + ')'
        return data


def data_analysis():
    analysis = st.selectbox('', ['Select one', 'State', 'Year', 'Quater',
                            'District', 'Transaction Type', 'User Brand', 'Top 10 Insights'])
    st.write('')

    if analysis:

        if analysis == 'State':
            transactions, users = st.tabs(['Transactions', 'Users'])
            with transactions:
                option = st.radio('', ['Transaction Count', 'Transaction Amount'], horizontal=True)
                if option == 'Transaction Count':
                    state_wise_total_transaction_count = aggregated_transaction.state_wise_total_transaction_count()
                    plotly.geo_map(state_wise_total_transaction_count, 'State',
                                   'Transaction Count', 'State wise Transaction Count')

                elif option == 'Transaction Amount':
                    state_wise_total_transaction_amount = aggregated_transaction.state_wise_total_transaction_amount()
                    plotly.geo_map(state_wise_total_transaction_amount, 'State',
                                   'Transaction Amount', 'State wise Transaction Amount')

                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.write('')
                    state_option = st.selectbox('State: ', state_list())
                    advanced_filters = st.checkbox('Advanced Filters ')
                    st.write('')
                if advanced_filters:
                    col1, col2, col3, col4 = st.columns(4)
                    with col1:
                        year_option = st.selectbox(
                            'Year: ', ['Select One', '2022', '2021', '2020', '2019', '2018'])
                    with col2:
                        quater_option = st.selectbox(
                            'Quater: ', ['Select One', 'Q1', 'Q2', 'Q3', 'Q4'])
                    st.write('')

                if state_option and advanced_filters:
                    if year_option != 'Select One' and quater_option == 'Select One':
                        col1, col2 = st.columns(2)
                        # pie chart
                        selectstate_selectyear_quater_wise_total_transaction_count = aggregated_transaction.selectstate_selectyear_quater_wise_total_transaction_count(state_option, year_option)
                        selectstate_selectyear_quater_wise_total_transaction_amount = aggregated_transaction.selectstate_selectyear_quater_wise_total_transaction_amount(state_option, year_option)
                        with col1:
                            plotly.pie_chart(df=selectstate_selectyear_quater_wise_total_transaction_count, x='Quater', y='Transaction Count',
                                            title='Quater wise Transaction Count')
                        with col2:
                            plotly.pie_chart(df=selectstate_selectyear_quater_wise_total_transaction_amount, x='Quater', y='Transaction Amount',
                                            title='Quater wise Transaction Amount')
                        
                        col1, col2 = st.columns(2)
                        # h_bar chart
                        selectstate_selectyear_type_wise_total_transaction_count = aggregated_transaction.selectstate_selectyear_type_wise_total_transaction_count(
                                                                                                            state_option, year_option)
                        selectstate_selectyear_type_wise_total_transaction_amount = aggregated_transaction.selectstate_selectyear_type_wise_total_transaction_amount(
                                                                                                            state_option, year_option)
                        with col1:
                            plotly.horizontal_bar_chart(df=selectstate_selectyear_type_wise_total_transaction_count,y='Transaction Type',x='Transaction Count', 
                                                text='Transaction', color='#5D9A96', title='Type wise Transaction Count')
                        with col2:
                            plotly.horizontal_bar_chart(df=selectstate_selectyear_type_wise_total_transaction_amount,y='Transaction Type',x='Transaction Amount',
                                                text='Transaction', color='#5cb85c', title='Type wise Transaction Amount')
                        
                        col1, col2 = st.columns(2)
                        # multi line chart
                        selectstate_selectyear_quater_type_wise_total_transaction_count = aggregated_transaction.selectstate_selectyear_quater_type_wise_total_transaction_count(
                                                                                                                    state_option, year_option)
                        selectstate_selectyear_quater_type_wise_total_transaction_amount = aggregated_transaction.selectstate_selectyear_quater_type_wise_total_transaction_amount(
                                                                                                                    state_option, year_option)
                        plotly.multi_line_chart(df=selectstate_selectyear_quater_type_wise_total_transaction_count, x='Quater', y='Transaction Count',
                                                    colorcolumn='Transaction Type', title='Quater - Type wise Transaction Count', title_x=0.38)
                        plotly.multi_line_chart(df=selectstate_selectyear_quater_type_wise_total_transaction_amount, x='Quater', y='Transaction Amount',
                                                    colorcolumn='Transaction Type', title='Quater - Type wise Transaction Amount', title_x=0.38)

                    elif year_option == 'Select One' and quater_option != 'Select One':
                        # vertical_bar chart
                        col1, col2 = st.columns(2)
                        selectstate_selectquater_year_wise_total_transaction_count = aggregated_transaction.selectstate_selectquater_year_wise_total_transaction_count(
                                                                                                            state_option, quater_option)
                        selectstate_selectquater_year_wise_total_transaction_amount = aggregated_transaction.selectstate_selectquater_year_wise_total_transaction_amount(
                                                                                                            state_option, quater_option)
                        with col1:
                            plotly.pie_chart(df=selectstate_selectquater_year_wise_total_transaction_count, x='Year', y='Transaction Count',
                                            title='Year wise Transaction Count')
                        with col2:
                            plotly.pie_chart(df=selectstate_selectquater_year_wise_total_transaction_amount, x='Year', y='Transaction Amount',
                                            title='Year wise Transaction Amount')
                        
                        # pie chart
                        col1, col2 = st.columns(2)
                        selectstate_selectquater_type_wise_total_transaction_count = aggregated_transaction.selectstate_selectquater_type_wise_total_transaction_count(
                                                                                                            state_option, quater_option)
                        selectstate_selectquater_type_wise_total_transaction_amount = aggregated_transaction.selectstate_selectquater_type_wise_total_transaction_amount(
                                                                                                            state_option, quater_option)
                        with col1:
                            plotly.horizontal_bar_chart(df=selectstate_selectquater_type_wise_total_transaction_count,y='Transaction Type',x='Transaction Count', 
                                                        text='Transaction', color='#5D9A96', title='Type wise Transaction Count')
                        with col2:
                            plotly.horizontal_bar_chart(df=selectstate_selectquater_type_wise_total_transaction_amount,y='Transaction Type',x='Transaction Amount', 
                                                        text='Transaction', color='#5cb85c', title='Type wise Transaction Amount')

                        # multi line chart
                        
                        selectstate_selectquater_year_type_wise_total_transaction_count = aggregated_transaction.selectstate_selectquater_year_type_wise_total_transaction_count(
                                                                                                                    state_option, quater_option)
                        selectstate_selectquater_year_type_wise_total_transaction_amount = aggregated_transaction.selectstate_selectquater_year_type_wise_total_transaction_amount(
                                                                                                                    state_option, quater_option)
                        plotly.multi_line_chart(df=selectstate_selectquater_year_type_wise_total_transaction_count, x='Year', y='Transaction Count',
                                                    colorcolumn='Transaction Type', title='Year - Type wise Transaction Count', title_x=0.40)
                        plotly.multi_line_chart(df=selectstate_selectquater_year_type_wise_total_transaction_amount, x='Year', y='Transaction Amount',
                                                    colorcolumn='Transaction Type', title='Year - Type wise Transaction Amount', title_x=0.40)

                    elif year_option != 'Select One' and quater_option != 'Select One':
                        # line chart
                        col1, col2 = st.columns(2)
                        selectstate_selectyear_selectquater_type_wise_total_transaction_count = aggregated_transaction.selectstate_selectyear_selectquater_type_wise_total_transaction_count(
                                                                                                                        state_option, year_option, quater_option)
                        selectstate_selectyear_selectquater_type_wise_total_transaction_amount = aggregated_transaction.selectstate_selectyear_selectquater_type_wise_total_transaction_amount(
                                                                                                                        state_option, year_option, quater_option)
                        with col1:
                            plotly.line_chart(df=selectstate_selectyear_selectquater_type_wise_total_transaction_count,y='Transaction Type',x='Transaction Count', 
                                                        text='Transaction', textposition=['bottom left','bottom center','top center','top right','top right'], 
                                                        color='#5D9A96', title='Type wise Transaction Count')
                        with col2:
                            plotly.line_chart(df=selectstate_selectyear_selectquater_type_wise_total_transaction_amount,y='Transaction Type',x='Transaction Amount', 
                                                        text='Transaction', textposition=['top center','top center','top right','top right','top right'], 
                                                        color='#5cb85c', title='Type wise Transaction Amount')

                else:
                    st.write('')
                    # vertical_bar chart
                    col1, col2 = st.columns(2)
                    selectstate_year_wise_total_transaction_count = aggregated_transaction.selectstate_year_wise_total_transaction_count(state_option)
                    selectstate_year_wise_total_transaction_amount = aggregated_transaction.selectstate_year_wise_total_transaction_amount(state_option)
                    with col1:
                        plotly.vertical_bar_chart(df=selectstate_year_wise_total_transaction_count,x='Year',y='Transaction Count', 
                                                    text='Transaction', color='#5D9A96', title='Year wise Transaction Count')
                    with col2:
                        plotly.vertical_bar_chart(df=selectstate_year_wise_total_transaction_amount,x='Year',y='Transaction Amount', 
                                                    text='Transaction', color='#5cb85c', title='Year wise Transaction Amount')
                    
                    # pie chart
                    col1, col2 = st.columns(2)
                    selectstate_quater_wise_total_transaction_count = aggregated_transaction.selectstate_quater_wise_total_transaction_count(state_option)
                    selectstate_quater_wise_total_transaction_amount = aggregated_transaction.selectstate_quater_wise_total_transaction_amount(state_option)
                    with col1:
                        plotly.pie_chart(df=selectstate_quater_wise_total_transaction_count, x='Quater', y='Transaction Count',
                                        title='Quater wise Transaction Count')
                    with col2:
                        plotly.pie_chart(df=selectstate_quater_wise_total_transaction_amount, x='Quater', y='Transaction Amount',
                                        title='Quater wise Transaction Amount')
                    
                    # line chart
                    col1, col2 = st.columns(2)
                    selectstate_type_wise_total_transaction_count = aggregated_transaction.selectstate_type_wise_total_transaction_count(state_option)
                    selectstate_type_wise_total_transaction_amount = aggregated_transaction.selectstate_type_wise_total_transaction_amount(state_option)
                    with col1:
                        plotly.line_chart(df=selectstate_type_wise_total_transaction_count,y='Transaction Type',x='Transaction Count', 
                                            text='Transaction', textposition=['bottom left','top right','top right','top right','top right'],
                                              color='#ba6e77', title='Type wise Transaction Count', title_x=0.35)
                    with col2:
                        plotly.line_chart(df=selectstate_type_wise_total_transaction_amount,y='Transaction Type',x='Transaction Amount', 
                                            text='Transaction', textposition=['bottom left','top right','top right','top right','top right'], 
                                            color='#716cf8', title='Type wise Transaction Amount', title_x=0.35)

                    # multi line chart
                    col1, col2 = st.columns(2)
                    selectstate_year_quater_wise_total_transaction_count = aggregated_transaction.selectstate_year_quater_wise_total_transaction_count(state_option)
                    selectstate_year_quater_wise_total_transaction_amount = aggregated_transaction.selectstate_year_quater_wise_total_transaction_amount(state_option)
                    selectstate_year_type_wise_total_transaction_count = aggregated_transaction.selectstate_year_type_wise_total_transaction_count(state_option)
                    selectstate_year_type_wise_total_transaction_amount = aggregated_transaction.selectstate_year_type_wise_total_transaction_amount(state_option)
                    selectstate_quater_type_wise_total_transaction_count = aggregated_transaction.selectstate_quater_type_wise_total_transaction_count(state_option)
                    selectstate_quater_type_wise_total_transaction_amount = aggregated_transaction.selectstate_quater_type_wise_total_transaction_amount(state_option)
                    with col1:
                        plotly.multi_line_chart(df=selectstate_year_quater_wise_total_transaction_count, x='Quater', y='Transaction Count',
                                                colorcolumn='Year', title='Year - Quater wise Transaction Count')
                        plotly.multi_line_chart(df=selectstate_year_type_wise_total_transaction_count, y='Transaction Count', x='Year',
                                                colorcolumn='Transaction Type', title='Year - Type wise Transaction Count')
                        plotly.multi_line_chart(df=selectstate_quater_type_wise_total_transaction_count, x='Quater', y='Transaction Count',
                                                colorcolumn='Transaction Type', title='Quater - Type wise Transaction Count')
                    with col2:
                        plotly.multi_line_chart(df=selectstate_year_quater_wise_total_transaction_amount, x='Quater', y='Transaction Amount',
                                                colorcolumn='Year', title='Year - Quater wise Transaction Amount')
                        plotly.multi_line_chart(df=selectstate_year_type_wise_total_transaction_amount, y='Transaction Amount', x='Year',
                                                colorcolumn='Transaction Type', title='Year - Type wise Transaction Amount')
                        plotly.multi_line_chart(df=selectstate_quater_type_wise_total_transaction_amount, x='Quater', y='Transaction Amount',
                                                colorcolumn='Transaction Type', title='Quater - Type wise Transaction Amount')                 

            with users:
                option = st.radio(
                    '', ['User Count', 'Registered Users', 'App Opens'], horizontal=True)
                if option == 'User Count':
                    state_wise_total_user_count = aggregated_user.state_wise_total_user_count()
                    plotly.geo_map(state_wise_total_user_count,
                                   'State', 'User Count', 'State wise User Count',0.30)

                elif option == 'Registered Users':
                    state_wise_total_registered_user = map_user.state_wise_total_registered_user()
                    plotly.geo_map(state_wise_total_registered_user, 'State',
                                   'Registered Users', 'State wise Registered Users')

                elif option == 'App Opens':
                    state_wise_total_app_opens = map_user.state_wise_total_app_opens()
                    plotly.geo_map(state_wise_total_app_opens,
                                   'State', 'App Opens', 'State wise App Opens',0.30)

                # chart
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.write('')
                    state_option = st.selectbox('State:  ', state_list())
                    advanced_filters = st.checkbox('Advanced Filters  ')
                    st.write('')
                if advanced_filters:
                    col1, col2, col3, col4 = st.columns(4)
                    with col1:
                        year_option = st.selectbox(
                            'Year:  ', ['Select One', '2022', '2021', '2020', '2019', '2018'])
                    with col2:
                        quater_option = st.selectbox(
                            'Quater:  ', ['Select One', 'Q1', 'Q2', 'Q3', 'Q4'])
                    st.write('')

                if state_option and advanced_filters:
                    if year_option != 'Select One' and quater_option == 'Select One':
                        # pie chart
                        selectstate_selectyear_quater_wise_total_user_count = aggregated_user.selectstate_selectyear_quater_wise_total_user_count(
                                                                                                state_option, year_option)
                        plotly.pie_chart(df=selectstate_selectyear_quater_wise_total_user_count, x='Quater', y='User Count',
                                            title='Quater wise User Count', title_x=0.35)
                        
                        # vertical_bar chart
                        selectstate_selectyear_brand_wise_total_user_count = aggregated_user.selectstate_selectyear_brand_wise_total_user_count(
                                                                                                state_option, year_option)
                        plotly.vertical_bar_chart(df=selectstate_selectyear_brand_wise_total_user_count,x='User Brand',y='User Count', 
                                                    text='User', color='#5D9A96', title='Brand wise User Count', title_x=0.45)
                        
                        # multi line chart
                        selectstate_selectyear_quater_brand_wise_total_user_count = aggregated_user.selectstate_selectyear_quater_brand_wise_total_user_count(
                                                                                                        state_option, year_option)
                        plotly.multi_line_chart(df=selectstate_selectyear_quater_brand_wise_total_user_count, x='Quater', y='User Count',
                                                colorcolumn='User Brand', title='Quater - Brand wise User Count', title_x=0.40)

                        # line chart
                        col1, col2 = st.columns(2)
                        selectstate_selectyear_quater_wise_total_registered_user = map_user.selectstate_selectyear_quater_wise_total_registered_user(
                                                                                            state_option, year_option)
                        selectstate_selectyear_quater_wise_total_app_opens = map_user.selectstate_selectyear_quater_wise_total_app_opens(
                                                                                        state_option, year_option)
                        with col1:
                            plotly.line_chart(df=selectstate_selectyear_quater_wise_total_registered_user,x='Quater',y='Registered Users', 
                                                text='Register', textposition=['top center','top left','top left','top left'], 
                                                color='#ba6e77', title='Quater wise Registered Users', title_x=0.35)
                        with col2:
                            plotly.line_chart(df=selectstate_selectyear_quater_wise_total_app_opens,x='Quater',y='App Opens', 
                                                text='App', textposition=['top center','top left','top left','top left'], 
                                                color='#716cf8', title='Quater wise App Opens', title_x=0.35)

                    elif year_option == 'Select One' and quater_option != 'Select One':
                        # pie chart
                        selectstate_selectquater_year_wise_total_user_count = aggregated_user.selectstate_selectquater_year_wise_total_user_count(
                                                                                                state_option, quater_option)
                        plotly.pie_chart(df=selectstate_selectquater_year_wise_total_user_count, x='Year', y='User Count',
                                            title='Year wise User Count', title_x=0.35)
                        
                        # vertical_bar chart
                        selectstate_selectquater_brand_wise_total_user_count = aggregated_user.selectstate_selectquater_brand_wise_total_user_count(
                                                                                                state_option, quater_option)
                        plotly.vertical_bar_chart(df=selectstate_selectquater_brand_wise_total_user_count,x='User Brand',y='User Count', 
                                                    text='User', color='#5D9A96', title='Brand wise User Count', title_x=0.45)

                        # multi line chart
                        selectstate_selectquater_year_brand_wise_total_user_count = aggregated_user.selectstate_selectquater_year_brand_wise_total_user_count(
                                                                                                    state_option, quater_option)
                        plotly.multi_line_chart(df=selectstate_selectquater_year_brand_wise_total_user_count, x='Year', y='User Count',
                                                colorcolumn='User Brand', title='Year - Brand wise User Count', title_x=0.40)
                        
                        # line chart
                        col1, col2 = st.columns(2)
                        selectstate_selectquater_year_wise_total_registered_user = map_user.selectstate_selectquater_year_wise_total_registered_user(
                                                                                            state_option, quater_option)
                        selectstate_selectquater_year_wise_total_app_opens = map_user.selectstate_selectquater_year_wise_total_app_opens(
                                                                                        state_option, quater_option)
                        
                        with col1:
                            plotly.line_chart(df=selectstate_selectquater_year_wise_total_registered_user,x='Year',y='Registered Users', 
                                                text='Register', textposition=['top center','top left','top left','top left', 'top left'], 
                                                color='#ba6e77', title='Year wise Registered Users', title_x=0.35)
                        with col2:
                            plotly.line_chart(df=selectstate_selectquater_year_wise_total_app_opens,x='Year',y='App Opens', 
                                                text='App', textposition=['top center','top left','top left','top left','top left'], 
                                                color='#716cf8', title='Year wise App Opens', title_x=0.35)

                    elif year_option != 'Select One' and quater_option != 'Select One':
                        # vertical_bar chart
                        selectstate_selectyear_selectquater_brand_wise_total_user_count = aggregated_user.selectstate_selectyear_selectquater_brand_wise_total_user_count(
                                                                                                state_option, year_option, quater_option)
                        plotly.vertical_bar_chart(df=selectstate_selectyear_selectquater_brand_wise_total_user_count,x='User Brand',y='User Count', 
                                                    text='User', color='#5D9A96', title='Brand wise User Count', title_x=0.45)

                else:
                    st.write('')
                    col1, col2 = st.columns(2)
                    with col1:
                    # line chart
                        selectstate_quater_wise_total_user_count = aggregated_user.selectstate_quater_wise_total_user_count(state_option)
                        plotly.line_chart(df=selectstate_quater_wise_total_user_count,y='Quater',x='User Count', 
                                        text='User', textposition=['top center','bottom center','top left','top left'], 
                                        color='#716cf8', title='Quater wise User Count', title_x=0.35)
                    with col2:
                    # pie chart
                        selectstate_year_wise_total_user_count = aggregated_user.selectstate_year_wise_total_user_count(state_option)
                        plotly.pie_chart(df=selectstate_year_wise_total_user_count, x='Year', y='User Count',
                                        title='Year wise User Count', title_x=0.26)
                    
                    # vertical_bar chart
                    selectstate_brand_wise_total_user_count = aggregated_user.selectstate_brand_wise_total_user_count(state_option)
                    plotly.vertical_bar_chart(df=selectstate_brand_wise_total_user_count,x='User Brand',y='User Count', 
                                text='User', color='#5cb85c', title='Brand wise User Count', title_x=0.45)
                    
                    # multi line chart
                    selectstate_year_quater_wise_total_user_count = aggregated_user.selectstate_year_quater_wise_total_user_count(state_option)
                    plotly.marker_multi_line_chart(df=selectstate_year_quater_wise_total_user_count, x='Quater', y='User Count',
                                            colorcolumn='Year', text='User', textposition='top center',
                                            title='Year - Quater wise User Count', title_x=0.35)

                    # pie chart
                    col1, col2 = st.columns(2)
                    selectstate_year_wise_total_registered_user = map_user.selectstate_year_wise_total_registered_user(state_option)
                    selectstate_year_wise_total_app_opens = map_user.selectstate_year_wise_total_app_opens(state_option)
                    with col1:
                        plotly.pie_chart(df=selectstate_year_wise_total_registered_user, x='Year', y='Registered Users',
                                        title='Year wise Registered Users')
                    with col2:
                        plotly.pie_chart(df=selectstate_year_wise_total_app_opens, x='Year', y='App Opens',
                                        title='Year wise App Opens')
                        
                    # vertical_bar chart
                    col1, col2 = st.columns(2)
                    selectstate_quater_wise_total_registered_user = map_user.selectstate_quater_wise_total_registered_user(state_option)
                    selectstate_quater_wise_total_app_opens = map_user.selectstate_quater_wise_total_app_opens(state_option)
                    with col1:
                        plotly.vertical_bar_chart(df=selectstate_quater_wise_total_registered_user,x='Quater',y='Registered Users', 
                                                    text='Register', color='#5D9A96', title='Quater wise Registered Users', title_x=0.30)
                    with col2:
                        plotly.vertical_bar_chart(df=selectstate_quater_wise_total_app_opens,x='Quater',y='App Opens', 
                                                    text='App', color='#5cb85c', title='Quater wise App Opens', title_x=0.35)
                    
                    # multi line chart
                    col1, col2 = st.columns(2)
                    selectstate_year_quater_wise_total_registered_user = map_user.selectstate_year_quater_wise_total_registered_user(state_option)
                    selectstate_year_quater_wise_total_app_opens = map_user.selectstate_year_quater_wise_total_app_opens(state_option)
                    with col1:
                        plotly.multi_line_chart(df=selectstate_year_quater_wise_total_registered_user, x='Quater', y='Registered Users',
                                                colorcolumn='Year',title='Year - Quater wise Registered Users')

                    with col2:
                        plotly.multi_line_chart(df=selectstate_year_quater_wise_total_app_opens, x='Quater', y='App Opens',
                                                colorcolumn='Year', title='Year - Quater wise App Opens')

        elif analysis == 'Year':
            transactions, users = st.tabs(['Transactions', 'Users'])
            with transactions:
                # vertical_bar chart
                col1, col2 = st.columns(2)
                year_wise_total_transaction_count = aggregated_transaction.year_wise_total_transaction_count()
                year_wise_total_transaction_amount = aggregated_transaction.year_wise_total_transaction_amount()
                with col1:
                    plotly.vertical_bar_chart(df=year_wise_total_transaction_count,x='Year',y='Transaction Count', 
                                                text='Transaction', color='#5D9A96', title='Year wise Transaction Count')
                with col2:
                    plotly.vertical_bar_chart(df=year_wise_total_transaction_amount,x='Year',y='Transaction Amount', 
                                                text='Transaction', color='#5cb85c', title='Year wise Transaction Amount')

                # multi line chart
                col1, col2 = st.columns(2)
                year_quater_wise_total_transaction_count = aggregated_transaction.year_quater_wise_total_transaction_count()
                year_quater_wise_total_transaction_amount = aggregated_transaction.year_quater_wise_total_transaction_amount()
                with col1:
                    plotly.multi_line_chart(df=year_quater_wise_total_transaction_count, x='Quater', y='Transaction Count',
                                                colorcolumn='Year', title='Year - Quater wise Transaction Count')
                with col2:
                    plotly.multi_line_chart(df=year_quater_wise_total_transaction_amount, x='Quater', y='Transaction Amount',
                                            colorcolumn='Year', title='Year - Quater wise Transaction Amount')
                
                # multi line chart
                year_type_wise_total_transaction_count = aggregated_transaction.year_type_wise_total_transaction_count()
                year_type_wise_total_transaction_amount = aggregated_transaction.year_type_wise_total_transaction_amount()
                plotly.multi_line_chart(df=year_type_wise_total_transaction_count, y='Transaction Count', x='Year',
                                        colorcolumn='Transaction Type', title='Year - Type wise Transaction Count', title_x=0.35)
                plotly.multi_line_chart(df=year_type_wise_total_transaction_amount, y='Transaction Amount', x='Year',
                                        colorcolumn='Transaction Type', title='Year - Type wise Transaction Amount', title_x=0.35)
            

                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.write('')
                    year_option = st.selectbox(
                        'Year:   ', ['2022', '2021', '2020', '2019', '2018'])
                    advanced_filters = st.checkbox('Advanced Filters   ')
                    st.write('')
                if advanced_filters:
                    col1, col2, col3, col4 = st.columns(4)
                    with col1:
                        list_state = ['Select One']
                        list_state.extend(state_list())
                        state_option = st.selectbox('State:   ', list_state)
                    with col2:
                        quater_option = st.selectbox(
                            'Quater:   ', ['Select One', 'Q1', 'Q2', 'Q3', 'Q4'])
                    st.write('')

                if year_option and advanced_filters:
                    if state_option != 'Select One' and quater_option == 'Select One':
                        # pie chart
                        col1, col2 = st.columns(2)
                        selectstate_selectyear_quater_wise_total_transaction_count = aggregated_transaction.selectstate_selectyear_quater_wise_total_transaction_count(
                                                                                                            state_option, year_option)
                        selectstate_selectyear_quater_wise_total_transaction_amount = aggregated_transaction.selectstate_selectyear_quater_wise_total_transaction_amount(
                                                                                                            state_option, year_option)
                        with col1:
                            plotly.pie_chart(df=selectstate_selectyear_quater_wise_total_transaction_count, x='Quater', y='Transaction Count',
                                            title='Quater wise Transaction Count')
                        with col2:
                            plotly.pie_chart(df=selectstate_selectyear_quater_wise_total_transaction_amount, x='Quater', y='Transaction Amount',
                                            title='Quater wise Transaction Amount')
                        
                        # horizontal_bar chart
                        col1, col2 = st.columns(2)
                        selectstate_selectyear_type_wise_total_transaction_count = aggregated_transaction.selectstate_selectyear_type_wise_total_transaction_count(
                                                                                                            state_option, year_option)
                        selectstate_selectyear_type_wise_total_transaction_amount = aggregated_transaction.selectstate_selectyear_type_wise_total_transaction_amount(
                                                                                                            state_option, year_option)
                        with col1:
                            plotly.horizontal_bar_chart(df=selectstate_selectyear_type_wise_total_transaction_count,y='Transaction Type',x='Transaction Count', 
                                                        text='Transaction', color='#5D9A96', title='Type wise Transaction Count')
                        with col2:
                            plotly.horizontal_bar_chart(df=selectstate_selectyear_type_wise_total_transaction_amount,y='Transaction Type',x='Transaction Amount', 
                                                        text='Transaction', color='#5cb85c', title='Type wise Transaction Amount')
                        
                        # multi line chart
                        selectstate_selectyear_quater_type_wise_total_transaction_count = aggregated_transaction.selectstate_selectyear_quater_type_wise_total_transaction_count(
                                                                                                                    state_option, year_option)
                        selectstate_selectyear_quater_type_wise_total_transaction_amount = aggregated_transaction.selectstate_selectyear_quater_type_wise_total_transaction_amount(
                                                                                                                    state_option, year_option)
                        plotly.multi_line_chart(df=selectstate_selectyear_quater_type_wise_total_transaction_count, x='Quater', y='Transaction Count',
                                                    colorcolumn='Transaction Type', title='Quater - Type wise Transaction Count', title_x=0.35)
                        plotly.multi_line_chart(df=selectstate_selectyear_quater_type_wise_total_transaction_amount, x='Quater', y='Transaction Amount',
                                                    colorcolumn='Transaction Type', title='Quater - Type wise Transaction Amount', title_x=0.35)

                    elif state_option == 'Select One' and quater_option != 'Select One':
                        option = st.radio(
                            '', ['Transaction Count', 'Transaction Amount'], horizontal=True)
                        if option == 'Transaction Count':
                            selectyear_selectquater_state_wise_total_transaction_count = aggregated_transaction.selectyear_selectquater_state_wise_total_transaction_count(
                                year_option, quater_option)
                            plotly.geo_map(selectyear_selectquater_state_wise_total_transaction_count,
                                           'State', 'Transaction Count', 'State wise Transaction Count')
                        elif option == 'Transaction Amount':
                            selectyear_selectquater_state_wise_total_transaction_amount = aggregated_transaction.selectyear_selectquater_state_wise_total_transaction_amount(
                                year_option, quater_option)
                            plotly.geo_map(selectyear_selectquater_state_wise_total_transaction_amount,
                                           'State', 'Transaction Amount', 'State wise Transaction Amount')

                        # line chart
                        col1, col2 = st.columns(2)
                        selectyear_selectquater_type_wise_total_transaction_count = aggregated_transaction.selectyear_selectquater_type_wise_total_transaction_count(
                                                                                                            year_option, quater_option)
                        selectyear_selectquater_type_wise_total_transaction_amount = aggregated_transaction.selectyear_selectquater_type_wise_total_transaction_amount(
                                                                                                            year_option, quater_option)
                        with col1:
                            plotly.line_chart(df=selectyear_selectquater_type_wise_total_transaction_count,y='Transaction Type',x='Transaction Count', 
                                                text='Transaction', textposition=['bottom left','top right','top right','top right','top right'], 
                                                color='#ba6e77', title='Type wise Transaction Count', title_x=0.35)
                        with col2:
                            plotly.line_chart(df=selectyear_selectquater_type_wise_total_transaction_amount,y='Transaction Type',x='Transaction Amount', 
                                                text='Transaction', textposition=['bottom left','top right','top right','top right','top right'], 
                                                color='#716cf8', title='Type wise Transaction Amount', title_x=0.35)

                    elif year_option != 'Select One' and quater_option != 'Select One':
                        # line chart
                        col1, col2 = st.columns(2)
                        selectstate_selectyear_selectquater_type_wise_total_transaction_count = aggregated_transaction.selectstate_selectyear_selectquater_type_wise_total_transaction_count(
                                                                                                            state_option, year_option, quater_option)
                        selectstate_selectyear_selectquater_type_wise_total_transaction_amount = aggregated_transaction.selectstate_selectyear_selectquater_type_wise_total_transaction_amount(
                                                                                                            state_option, year_option, quater_option)
                        with col1:
                            plotly.line_chart(df=selectstate_selectyear_selectquater_type_wise_total_transaction_count,y='Transaction Type',x='Transaction Count', 
                                                text='Transaction', textposition=['bottom left','bottom center','top center','top right','top right'], 
                                                color='#ba6e77', title='Type wise Transaction Count', title_x=0.35)
                        with col2:
                            plotly.line_chart(df=selectstate_selectyear_selectquater_type_wise_total_transaction_amount,y='Transaction Type',x='Transaction Amount', 
                                                text='Transaction', textposition=['bottom left','bottom center','top right','top right','top right'], 
                                                color='#716cf8', title='Type wise Transaction Amount', title_x=0.35)

                else:
                    option = st.radio(
                        '', ['Transaction Count', 'Transaction Amount'], horizontal=True)
                    if option == 'Transaction Count':
                        selectyear_state_wise_total_transaction_count = aggregated_transaction.selectyear_state_wise_total_transaction_count(
                            year_option)
                        plotly.geo_map(selectyear_state_wise_total_transaction_count,
                                       'State', 'Transaction Count', 'State wise Transaction Count')
                    elif option == 'Transaction Amount':
                        selectyear_state_wise_total_transaction_amount = aggregated_transaction.selectyear_state_wise_total_transaction_amount(
                            year_option)
                        plotly.geo_map(selectyear_state_wise_total_transaction_amount,
                                       'State', 'Transaction Amount', 'State wise Transaction Amount')

                        # chart
                    
                    # pie chart
                    col1, col2 = st.columns(2)
                    selectyear_quater_wise_total_transaction_count = aggregated_transaction.selectyear_quater_wise_total_transaction_count(year_option)
                    selectyear_quater_wise_total_transaction_amount = aggregated_transaction.selectyear_quater_wise_total_transaction_amount(year_option)
                    with col1:
                        plotly.pie_chart(df=selectyear_quater_wise_total_transaction_count, x='Quater', y='Transaction Count',
                                        title='Quater wise Transaction Count')
                    with col2:
                        plotly.pie_chart(df=selectyear_quater_wise_total_transaction_amount, x='Quater', y='Transaction Amount',
                                        title='Quater wise Transaction Amount')
                    
                    # horizontal_bar chart
                    col1, col2 = st.columns(2)
                    selectyear_type_wise_total_transaction_count = aggregated_transaction.selectyear_type_wise_total_transaction_count(year_option)
                    selectyear_type_wise_total_transaction_amount = aggregated_transaction.selectyear_type_wise_total_transaction_amount(year_option)
                    with col1:
                        plotly.horizontal_bar_chart(df=selectyear_type_wise_total_transaction_count,y='Transaction Type',x='Transaction Count', 
                                                    text='Transaction', color='#5D9A96', title='Type wise Transaction Count')
                    with col2:
                        plotly.horizontal_bar_chart(df=selectyear_type_wise_total_transaction_amount,y='Transaction Type',x='Transaction Amount', 
                                                    text='Transaction', color='#5cb85c', title='Type wise Transaction Amount')
                    
                    # multi line chart
                    selectyear_quater_type_wise_total_transaction_count = aggregated_transaction.selectyear_quater_type_wise_total_transaction_count(year_option)
                    selectyear_quater_type_wise_total_transaction_amount = aggregated_transaction.selectyear_quater_type_wise_total_transaction_amount(year_option) 
                    plotly.multi_line_chart(df=selectyear_quater_type_wise_total_transaction_count, y='Transaction Count',x='Quater',
                                                colorcolumn='Transaction Type', title='Quater - Type wise Transaction Count', title_x=0.35)
                    plotly.multi_line_chart(df=selectyear_quater_type_wise_total_transaction_amount, y='Transaction Amount',x='Quater',
                                                colorcolumn='Transaction Type', title='Quater - Type wise Transaction Amount', title_x=0.35)
                    
            with users:
                # vertical_bar chart
                year_wise_total_user_count = aggregated_user.year_wise_total_user_count()
                plotly.vertical_bar_chart(df=year_wise_total_user_count,x='Year',y='User Count', 
                                            text='User', color='#5D9A96', title='Year wise User Count', title_x=0.45)
                
                # marker multi line chart
                year_quater_wise_total_user_count = aggregated_user.year_quater_wise_total_user_count()
                plotly.marker_multi_line_chart(df=year_quater_wise_total_user_count, x='Quater', y='User Count',
                                            colorcolumn='Year', title='Year - Quater wise User Count', text='User', textposition='top center', title_x=0.35)
                
                # pie chart
                col1, col2 = st.columns(2)
                year_wise_total_registered_user = map_user.year_wise_total_registered_user()
                year_wise_total_app_opens = map_user.year_wise_total_app_opens()
                with col1:
                    plotly.pie_chart(df=year_wise_total_registered_user, x='Year', y='Registered Users',
                                    title='Year wise Registered Users')
                with col2:
                    plotly.pie_chart(df=year_wise_total_app_opens, x='Year', y='App Opens',
                                    title='Year wise App Opens')

                # multi line chart
                col1, col2 = st.columns(2)
                year_quater_wise_total_registered_user = map_user.year_quater_wise_total_registered_user()
                year_quater_wise_total_app_opens = map_user.year_quater_wise_total_app_opens()
                with col1:
                    plotly.multi_line_chart(df=year_quater_wise_total_registered_user, x='Quater', y='Registered Users',
                                                colorcolumn='Year', title='Year - Quater wise Registered Users')
                with col2:
                    plotly.multi_line_chart(df=year_quater_wise_total_app_opens, x='Quater', y='App Opens',
                                            colorcolumn='Year', title='Year - Quater wise App Opens')
                
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.write('')
                    year_option = st.selectbox(
                        'Year:    ', ['2022', '2021', '2020', '2019', '2018'])
                    advanced_filters = st.checkbox('Advanced Filters    ')
                    st.write('')
                if advanced_filters:
                    col1, col2, col3, col4 = st.columns(4)
                    with col1:
                        list_state = ['Select One']
                        list_state.extend(state_list())
                        state_option = st.selectbox('State:    ', list_state)
                    with col2:
                        quater_option = st.selectbox(
                            'Quater:    ', ['Select One', 'Q1', 'Q2', 'Q3', 'Q4'])
                    st.write('')

                if year_option and advanced_filters:
                    if state_option != 'Select One' and quater_option == 'Select One':
                        # line chart
                        selectstate_selectyear_quater_wise_total_user_count = aggregated_user.selectstate_selectyear_quater_wise_total_user_count(
                                                                                            state_option, year_option)
                        plotly.line_chart(df=selectstate_selectyear_quater_wise_total_user_count,x='Quater',y='User Count', 
                                            text='User', textposition='top left', color='#ba6e77', title='Quater wise User Count', title_x=0.40)
                    
                        # vertical_bar chart
                        selectyear_selectstate_brand_wise_total_user_count = aggregated_user.selectyear_selectstate_brand_wise_total_user_count(
                                                                                                state_option, year_option)
                        plotly.vertical_bar_chart(df=selectyear_selectstate_brand_wise_total_user_count,x='User Brand',y='User Count', 
                                                    text='User', color='#5D9A96', title='Brand wise User Count', title_x=0.40)

                        # multi line chart
                        selectstate_selectyear_quater_brand_wise_total_user_count = aggregated_user.selectstate_selectyear_quater_brand_wise_total_user_count(
                                                                                                    state_option, year_option)
                        plotly.multi_line_chart(df=selectstate_selectyear_quater_brand_wise_total_user_count, x='Quater', y='User Count',
                                                    colorcolumn='User Brand', title='Quater - Brand wise User Count', title_x=0.40)

                        # pie chart
                        col1, col2 = st.columns(2)
                        selectstate_selectyear_quater_wise_total_registered_user = map_user.selectstate_selectyear_quater_wise_total_registered_user(
                                                                                            state_option, year_option)
                        selectstate_selectyear_quater_wise_total_app_opens = map_user.selectstate_selectyear_quater_wise_total_app_opens(
                                                                                        state_option, year_option)
                        with col1:
                            plotly.pie_chart(df=selectstate_selectyear_quater_wise_total_registered_user, x='Quater', y='Registered Users',
                                            title='Quater wise Registered Users')
                        with col2:
                            plotly.pie_chart(df=selectstate_selectyear_quater_wise_total_app_opens, x='Quater', y='App Opens',
                                            title='Quater wise App Opens')
                        
                    elif state_option == 'Select One' and quater_option != 'Select One':
                        option = st.radio(
                            '', ['User Count', 'Registered Users', 'App Opens'], horizontal=True)
                        if option == 'User Count':
                            selectyear_selectquater_state_wise_total_user_count = aggregated_user.selectyear_selectquater_state_wise_total_user_count(
                                year_option, quater_option)
                            plotly.geo_map(selectyear_selectquater_state_wise_total_user_count,
                                           'State', 'User Count', 'State wise User Count')

                        elif option == 'Registered Users':
                            selectyear_selectquater_state_wise_total_registered_user = map_user.selectyear_selectquater_state_wise_total_registered_user(
                                year_option, quater_option)
                            plotly.geo_map(selectyear_selectquater_state_wise_total_registered_user,
                                           'State', 'Registered Users', 'State wise Registered Users')

                        elif option == 'App Opens':
                            selectyear_selectquater_state_wise_total_app_opens = map_user.selectyear_selectquater_state_wise_total_app_opens(
                                year_option, quater_option)
                            plotly.geo_map(selectyear_selectquater_state_wise_total_app_opens,
                                           'State', 'App Opens', 'State wise App Opens')

                        # vertical_bar chart
                        selectyear_selectquater_brand_wise_total_user_count = aggregated_user.selectyear_selectquater_brand_wise_total_user_count(
                                                                                                year_option, quater_option)
                        plotly.vertical_bar_chart(df=selectyear_selectquater_brand_wise_total_user_count, x='User Brand', y='User Count', 
                                                    text='User', color='#5D9A96', title='Brand wise User Count', title_x=0.40)


                    elif year_option != 'Select One' and quater_option != 'Select One':
                        # vertical_bar chart
                        selectstate_selectyear_selectquater_brand_wise_total_user_count = aggregated_user.selectstate_selectyear_selectquater_brand_wise_total_user_count(
                                                                                                            state_option, year_option, quater_option)
                        plotly.vertical_bar_chart(df=selectstate_selectyear_selectquater_brand_wise_total_user_count, x='User Brand', y='User Count', 
                                                    text='User', color='#5D9A96', title='Brand wise User Count', title_x=0.40)

                else:
                    option = st.radio(
                        '', ['User Count', 'Registered Users', 'App Opens'], horizontal=True)
                    if option == 'User Count':
                        selectyear_state_wise_total_user_count = aggregated_user.selectyear_state_wise_total_user_count(
                                                                                year_option)
                        plotly.geo_map(selectyear_state_wise_total_user_count,
                                       'State', 'User Count', 'State wise User Count', title_x=0.30)

                    elif option == 'Registered Users':
                        selectyear_state_wise_total_registered_user = map_user.selectyear_state_wise_total_registered_user(
                                                                                year_option)
                        plotly.geo_map(selectyear_state_wise_total_registered_user,
                                       'State', 'Registered Users', 'State wise Registered Users')

                    elif option == 'App Opens':
                        selectyear_state_wise_total_app_opens = map_user.selectyear_state_wise_total_app_opens(
                                                                        year_option)
                        plotly.geo_map(selectyear_state_wise_total_app_opens,
                                       'State', 'App Opens', 'State wise App Opens', title_x=0.30)

                    # line chart
                    selectyear_quater_wise_total_user_count = aggregated_user.selectyear_quater_wise_total_user_count(year_option)
                    plotly.line_chart(df=selectyear_quater_wise_total_user_count,x='Quater',y='User Count', 
                                        text='User', textposition='top left', color='#ba6e77', title='Quater wise User Count', title_x=0.40)
                    
                    # vertical_bar chart
                    selectyear_brand_wise_total_user_count = aggregated_user.selectyear_brand_wise_total_user_count(year_option)
                    plotly.vertical_bar_chart(df=selectyear_brand_wise_total_user_count,x='User Brand',y='User Count', 
                                                text='User', color='#5D9A96', title='Brand wise User Count', title_x=0.40)
                    
                    # multi line chart
                    selectyear_quater_brand_wise_total_user_count = aggregated_user.selectyear_quater_brand_wise_total_user_count(year_option)
                    plotly.multi_line_chart(df=selectyear_quater_brand_wise_total_user_count, y='User Count', x='Quater',
                                                colorcolumn='User Brand', title='Quater - Brand wise User Count', title_x=0.40)

                    # pie chart
                    col1, col2 = st.columns(2)
                    selectyear_quater_wise_total_registered_user = map_user.selectyear_quater_wise_total_registered_user(year_option)
                    selectyear_quater_wise_total_app_opens = map_user.selectyear_quater_wise_total_app_opens(year_option)
                    with col1:
                        plotly.pie_chart(df=selectyear_quater_wise_total_registered_user, x='Quater', y='Registered Users',
                                        title='Quater wise Registered Users')
                    with col2:
                        plotly.pie_chart(df=selectyear_quater_wise_total_app_opens, x='Quater', y='App Opens',
                                        title='Quater wise App Opens')
    
        elif analysis == 'Quater':
            transactions, users = st.tabs(['Transactions', 'Users'])
            with transactions:
                # vertical_bar chart
                col1, col2 = st.columns(2)
                quater_wise_total_transaction_count = aggregated_transaction.quater_wise_total_transaction_count()
                quater_wise_total_transaction_amount = aggregated_transaction.quater_wise_total_transaction_amount()
                with col1:
                    plotly.vertical_bar_chart(df=quater_wise_total_transaction_count,x='Quater',y='Transaction Count', 
                                                text='Transaction', color='#5D9A96', title='Quater wise Transaction Count')
                with col2:
                    plotly.vertical_bar_chart(df=quater_wise_total_transaction_amount,x='Quater',y='Transaction Amount', 
                                                text='Transaction', color='#5cb85c', title='Quater wise Transaction Amount')

                # multi line chart
                col1, col2 = st.columns(2)
                year_quater_wise_total_transaction_count = aggregated_transaction.year_quater_wise_total_transaction_count()
                year_quater_wise_total_transaction_amount = aggregated_transaction.year_quater_wise_total_transaction_amount()
                with col1:
                    plotly.multi_line_chart(df=year_quater_wise_total_transaction_count, x='Year', y='Transaction Count',
                                                colorcolumn='Quater', title='Year - Quater wise Transaction Count')
                with col2:
                    plotly.multi_line_chart(df=year_quater_wise_total_transaction_amount, x='Year', y='Transaction Amount',
                                            colorcolumn='Quater', title='Year - Quater wise Transaction Amount')
                
                # multi line chart
                quater_type_wise_total_transaction_count = aggregated_transaction.quater_type_wise_total_transaction_count()
                quater_type_wise_total_transaction_amount = aggregated_transaction.quater_type_wise_total_transaction_amount()
                plotly.multi_line_chart(df=quater_type_wise_total_transaction_count, y='Transaction Count', x='Quater',
                                        colorcolumn='Transaction Type', title='Quater - Type wise Transaction Count', title_x=0.35)
                plotly.multi_line_chart(df=quater_type_wise_total_transaction_amount, y='Transaction Amount', x='Quater',
                                        colorcolumn='Transaction Type', title='Quater - Type wise Transaction Amount', title_x=0.35)

                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.write('')
                    quater_option = st.selectbox('Quater:     ', ['Q1', 'Q2', 'Q3', 'Q4'])
                    advanced_filters = st.checkbox('Advanced Filters     ')
                    st.write('')
                if advanced_filters:
                    col1, col2, col3, col4 = st.columns(4)
                    with col1:
                        list_state = ['Select One']
                        list_state.extend(state_list())
                        state_option = st.selectbox('State:     ', list_state)
                    with col2:
                        year_option = st.selectbox(
                            'Year:     ', ['Select One', '2022', '2021', '2020', '2019', '2018'])
                    st.write('')

                if quater_option and advanced_filters:
                    if state_option != 'Select One' and year_option == 'Select One':
                        # pie chart
                        col1, col2 = st.columns(2)
                        selectstate_selectquater_year_wise_total_transaction_count = aggregated_transaction.selectstate_selectquater_year_wise_total_transaction_count(
                                                                                                            state_option, quater_option)
                        selectstate_selectquater_year_wise_total_transaction_amount = aggregated_transaction.selectstate_selectquater_year_wise_total_transaction_amount(
                                                                                                            state_option, quater_option)
                        with col1:
                            plotly.pie_chart(df=selectstate_selectquater_year_wise_total_transaction_count, x='Year', y='Transaction Count',
                                            title='Year wise Transaction Count')
                        with col2:
                            plotly.pie_chart(df=selectstate_selectquater_year_wise_total_transaction_amount, x='Year', y='Transaction Amount',
                                            title='Year wise Transaction Amount')
                        
                        # horizontal_bar chart
                        col1, col2 = st.columns(2)
                        selectstate_selectquater_type_wise_total_transaction_count = aggregated_transaction.selectstate_selectquater_type_wise_total_transaction_count(
                                                                                                            state_option, quater_option)
                        selectstate_selectquater_type_wise_total_transaction_amount = aggregated_transaction.selectstate_selectquater_type_wise_total_transaction_amount(
                                                                                                            state_option, quater_option)
                        with col1:
                            plotly.horizontal_bar_chart(df=selectstate_selectquater_type_wise_total_transaction_count,y='Transaction Type',x='Transaction Count', 
                                                        text='Transaction', color='#5D9A96', title='Type wise Transaction Count')
                        with col2:
                            plotly.horizontal_bar_chart(df=selectstate_selectquater_type_wise_total_transaction_amount,y='Transaction Type',x='Transaction Amount', 
                                                        text='Transaction', color='#5cb85c', title='Type wise Transaction Amount')

                        # multi line chart
                        selectstate_selectquater_year_type_wise_total_transaction_count = aggregated_transaction.selectstate_selectquater_year_type_wise_total_transaction_count(
                                                                                                                    state_option, quater_option)
                        selectstate_selectquater_year_type_wise_total_transaction_amount = aggregated_transaction.selectstate_selectquater_year_type_wise_total_transaction_amount(
                                                                                                                    state_option, quater_option)
                        plotly.multi_line_chart(df=selectstate_selectquater_year_type_wise_total_transaction_count, y='Transaction Count', x='Year',
                                                colorcolumn='Transaction Type', title='Year - Type wise Transaction Count', title_x=0.35)
                        plotly.multi_line_chart(df=selectstate_selectquater_year_type_wise_total_transaction_amount, y='Transaction Amount', x='Year',
                                                colorcolumn='Transaction Type', title='Year - Type wise Transaction Amount', title_x=0.35)
                
                    elif state_option == 'Select One' and year_option != 'Select One':
                        option = st.radio('', ['Transaction Count', 'Transaction Amount'], horizontal=True)
                        if option == 'Transaction Count':
                            selectyear_selectquater_state_wise_total_transaction_count = aggregated_transaction.selectyear_selectquater_state_wise_total_transaction_count(
                                year_option, quater_option)
                            plotly.geo_map(selectyear_selectquater_state_wise_total_transaction_count,
                                           'State', 'Transaction Count', 'State wise Transaction Count')

                        elif option == 'Transaction Amount':
                            selectyear_selectquater_state_wise_total_transaction_amount = aggregated_transaction.selectyear_selectquater_state_wise_total_transaction_amount(
                                year_option, quater_option)
                            plotly.geo_map(selectyear_selectquater_state_wise_total_transaction_amount,
                                           'State', 'Transaction Amount', 'State wise Transaction Amount')

                        # horizontal_bar chart
                        col1, col2 = st.columns(2)
                        selectyear_selectquater_type_wise_total_transaction_count = aggregated_transaction.selectyear_selectquater_type_wise_total_transaction_count(
                                                                                                            year_option, quater_option)
                        selectyear_selectquater_type_wise_total_transaction_amount = aggregated_transaction.selectyear_selectquater_type_wise_total_transaction_amount(
                            year_option, quater_option)
                        with col1:
                            plotly.horizontal_bar_chart(df=selectyear_selectquater_type_wise_total_transaction_count,y='Transaction Type',x='Transaction Count', 
                                                        text='Transaction', color='#5D9A96', title='Type wise Transaction Count')
                        with col2:
                            plotly.horizontal_bar_chart(df=selectyear_selectquater_type_wise_total_transaction_amount,y='Transaction Type',x='Transaction Amount', 
                                                        text='Transaction', color='#5cb85c', title='Type wise Transaction Amount')
                            
                    elif year_option != 'Select One' and quater_option != 'Select One':
                        # horizontal_bar chart
                        col1, col2 = st.columns(2)
                        selectstate_selectyear_selectquater_type_wise_total_transaction_count = aggregated_transaction.selectstate_selectyear_selectquater_type_wise_total_transaction_count(
                                                                                                                        state_option, year_option, quater_option)
                        selectstate_selectyear_selectquater_type_wise_total_transaction_amount = aggregated_transaction.selectstate_selectyear_selectquater_type_wise_total_transaction_amount(
                                                                                                                        state_option, year_option, quater_option)
                        with col1:
                            plotly.horizontal_bar_chart(df=selectstate_selectyear_selectquater_type_wise_total_transaction_count,y='Transaction Type',x='Transaction Count', 
                                                        text='Transaction', color='#5D9A96', title='Type wise Transaction Count')
                        with col2:
                            plotly.horizontal_bar_chart(df=selectstate_selectyear_selectquater_type_wise_total_transaction_amount,y='Transaction Type',x='Transaction Amount', 
                                                        text='Transaction', color='#5cb85c', title='Type wise Transaction Amount')
                            
                else:
                    option = st.radio('', ['Transaction Count', 'Transaction Amount'], horizontal=True)
                    if option == 'Transaction Count':
                        selectquater_state_wise_total_transaction_count = aggregated_transaction.selectquater_state_wise_total_transaction_count(
                            quater_option)
                        plotly.geo_map(selectquater_state_wise_total_transaction_count,
                                       'State', 'Transaction Count', 'State wise Transaction Count')

                    elif option == 'Transaction Amount':
                        selectquater_state_wise_total_transaction_amount = aggregated_transaction.selectquater_state_wise_total_transaction_amount(
                            quater_option)
                        plotly.geo_map(selectquater_state_wise_total_transaction_amount,
                                       'State', 'Transaction Amount', 'State wise Transaction Amount')
                    # vertical_bar chart
                    col1, col2 = st.columns(2)
                    selectquater_year_wise_total_transaction_count = aggregated_transaction.selectquater_year_wise_total_transaction_count(quater_option)
                    selectquater_year_wise_total_transaction_amount = aggregated_transaction.selectquater_year_wise_total_transaction_amount(quater_option)
                    with col1:
                        plotly.vertical_bar_chart(df=selectquater_year_wise_total_transaction_count,x='Year',y='Transaction Count', 
                                                    text='Transaction', color='#5D9A96', title='Year wise Transaction Count')
                    with col2:
                        plotly.vertical_bar_chart(df=selectquater_year_wise_total_transaction_amount,x='Year',y='Transaction Amount', 
                                                    text='Transaction', color='#5cb85c', title='Year wise Transaction Amount')
                                        
                    # line chart
                    col1, col2 = st.columns(2)
                    selectquater_type_wise_total_transaction_count = aggregated_transaction.selectquater_type_wise_total_transaction_count(quater_option)
                    selectquater_type_wise_total_transaction_amount = aggregated_transaction.selectquater_type_wise_total_transaction_amount(quater_option)
                    with col1:
                        plotly.line_chart(df=selectquater_type_wise_total_transaction_count,y='Transaction Type',x='Transaction Count', 
                                            text='Transaction', textposition=['bottom left','bottom left','top right','top right','top right'], 
                                            color='#ba6e77', title='Type wise Transaction Count')
                    with col2:
                        plotly.line_chart(df=selectquater_type_wise_total_transaction_amount,y='Transaction Type',x='Transaction Amount', 
                                            text='Transaction', textposition=['top center','top right','top right','top right','top right'], 
                                            color='#716cf8', title='Type wise Transaction Amount')

                    # multi line chart
                    selectquater_year_type_wise_total_transaction_count = aggregated_transaction.selectquater_year_type_wise_total_transaction_count(
                                                                                                    quater_option)
                    selectquater_year_type_wise_total_transaction_amount = aggregated_transaction.selectquater_year_type_wise_total_transaction_amount(
                                                                                                    quater_option)
                    plotly.multi_line_chart(df=selectquater_year_type_wise_total_transaction_count, y='Transaction Count',x='Year',
                                            colorcolumn='Transaction Type', title='Year - Type wise Transaction Count', title_x=0.35)
                    plotly.multi_line_chart(df=selectquater_year_type_wise_total_transaction_amount, y='Transaction Amount',x='Year',
                                            colorcolumn='Transaction Type', title='Year - Type wise Transaction Amount', title_x=0.35)

            with users:
                col1, col2 = st.columns(2)
                with col1:
                    # vertical_bar chart
                    quater_wise_total_user_count = aggregated_user.quater_wise_total_user_count()
                    plotly.vertical_bar_chart(df=quater_wise_total_user_count,x='Quater',y='User Count', 
                                                text='User', color='#5D9A96', title='Quater wise User Count', title_x=0.40)
                with col2:
                    # marker multi line chart
                    year_quater_wise_total_user_count = aggregated_user.year_quater_wise_total_user_count()
                    plotly.multi_line_chart(df=year_quater_wise_total_user_count, x='Year', y='User Count',
                                                colorcolumn='Quater', title='Year - Quater wise User Count')
                
                # pie chart
                col1, col2 = st.columns(2)
                quater_wise_total_registered_user = map_user.quater_wise_total_registered_user()
                quater_wise_total_app_opens = map_user.quater_wise_total_app_opens()
                with col1:
                    plotly.pie_chart(df=quater_wise_total_registered_user, x='Quater', y='Registered Users',
                                    title='Quater wise Registered Users')
                with col2:
                    plotly.pie_chart(df=quater_wise_total_app_opens, x='Quater', y='App Opens',
                                    title='Quater wise App Opens')

                # multi line chart
                col1, col2 = st.columns(2)
                year_quater_wise_total_registered_user = map_user.year_quater_wise_total_registered_user()
                year_quater_wise_total_app_opens = map_user.year_quater_wise_total_app_opens()
                with col1:
                    plotly.multi_line_chart(df=year_quater_wise_total_registered_user, x='Year', y='Registered Users',
                                                colorcolumn='Quater', title='Year - Quater wise Registered Users')
                with col2:
                    plotly.multi_line_chart(df=year_quater_wise_total_app_opens, x='Year', y='App Opens',
                                            colorcolumn='Quater', title='Year - Quater wise App Opens')
                
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.write('')
                    quater_option = st.selectbox(
                        'Quater:      ', ['Q1', 'Q2', 'Q3', 'Q4'])
                    advanced_filters = st.checkbox('Advanced Filters      ')
                    st.write('')
                if advanced_filters:
                    col1, col2, col3, col4 = st.columns(4)
                    with col1:
                        list_state = ['Select One']
                        list_state.extend(state_list())
                        state_option = st.selectbox('State:      ', list_state)
                    with col2:
                        year_option = st.selectbox(
                            'Year:      ', ['Select One', '2022', '2021', '2020', '2019', '2018'])
                    st.write('')

                if quater_option and advanced_filters:
                    if state_option != 'Select One' and year_option == 'Select One':
                        # line chart
                        selectstate_selectquater_year_wise_total_user_count = aggregated_user.selectstate_selectquater_year_wise_total_user_count(
                                                                                                state_option, quater_option)
                        plotly.line_chart(df=selectstate_selectquater_year_wise_total_user_count,x='Year',y='User Count', 
                                            text='User', textposition='top left', color='#ba6e77', title='Year wise User Count', title_x=0.40)
                    
                        # vertical_bar chart
                        selectstate_selectquater_brand_wise_total_user_count = aggregated_user.selectstate_selectquater_brand_wise_total_user_count(
                                                                                                state_option, quater_option)
                        plotly.vertical_bar_chart(df=selectstate_selectquater_brand_wise_total_user_count,x='User Brand',y='User Count', 
                                                    text='User', color='#5D9A96', title='Brand wise User Count', title_x=0.40)
                    
                        # multi line chart
                        selectstate_selectquater_year_brand_wise_total_user_count = aggregated_user.selectstate_selectquater_year_brand_wise_total_user_count(
                                                                                                    state_option, quater_option)
                        plotly.multi_line_chart(df=selectstate_selectquater_year_brand_wise_total_user_count, y='User Count', x='Year',
                                                    colorcolumn='User Brand', title='Year - Brand wise User Count', title_x=0.40)

                        # pie chart
                        col1, col2 = st.columns(2)
                        selectstate_selectquater_year_wise_total_registered_user = map_user.selectstate_selectquater_year_wise_total_registered_user(
                                                                                            state_option, quater_option)
                        selectstate_selectquater_year_wise_total_app_opens = map_user.selectstate_selectquater_year_wise_total_app_opens(
                                                                                        state_option, quater_option)
                        with col1:
                            plotly.pie_chart(df=selectstate_selectquater_year_wise_total_registered_user, x='Year', y='Registered Users',
                                            title='Year wise Registered Users')
                        with col2:
                            plotly.pie_chart(df=selectstate_selectquater_year_wise_total_app_opens, x='Year', y='App Opens',
                                            title='Year wise App Opens')

                    elif state_option == 'Select One' and year_option != 'Select One':
                        option = st.radio(
                            '', ['User Count', 'Registered Users', 'App Opens'], horizontal=True)
                        if option == 'User Count':
                            selectyear_selectquater_state_wise_total_user_count = aggregated_user.selectyear_selectquater_state_wise_total_user_count(
                                year_option, quater_option)
                            plotly.geo_map(selectyear_selectquater_state_wise_total_user_count,
                                           'State', 'User Count', 'State wise User Count')

                        elif option == 'Registered Users':
                            selectyear_selectquater_state_wise_total_registered_user = map_user.selectyear_selectquater_state_wise_total_registered_user(
                                year_option, quater_option)
                            plotly.geo_map(selectyear_selectquater_state_wise_total_registered_user,
                                           'State', 'Registered Users', 'State wise Registered Users')

                        elif option == 'App Opens':
                            selectyear_selectquater_state_wise_total_app_opens = map_user.selectyear_selectquater_state_wise_total_app_opens(
                                year_option, quater_option)
                            plotly.geo_map(selectyear_selectquater_state_wise_total_app_opens,
                                           'State', 'App Opens', 'State wise App Opens')

                        # vertical_bar chart
                        selectyear_selectquater_brand_wise_total_user_count = aggregated_user.selectyear_selectquater_brand_wise_total_user_count(
                                                                                                year_option, quater_option)
                        plotly.vertical_bar_chart(df=selectyear_selectquater_brand_wise_total_user_count,x='User Brand',y='User Count', 
                                                    text='User', color='#5D9A96', title='Brand wise User Count', title_x=0.40)
                    
                    elif year_option != 'Select One' and quater_option != 'Select One':
                        # vertical_bar chart
                        selectstate_selectyear_selectquater_brand_wise_total_user_count = aggregated_user.selectstate_selectyear_selectquater_brand_wise_total_user_count(
                                                                                                            state_option, year_option, quater_option)
                        plotly.vertical_bar_chart(df=selectstate_selectyear_selectquater_brand_wise_total_user_count,x='User Brand',y='User Count', 
                                                    text='User', color='#5D9A96', title='Brand wise User Count', title_x=0.40)
                    
                else:
                    option = st.radio('', ['User Count', 'Registered Users', 'App Opens'], horizontal=True)
                    if option == 'User Count':
                        selectquater_state_wise_total_user_count = aggregated_user.selectquater_state_wise_total_user_count(
                            quater_option)
                        plotly.geo_map(selectquater_state_wise_total_user_count,
                                       'State', 'User Count', 'State wise User Count')

                    elif option == 'Registered Users':
                        selectquater_state_wise_total_registered_user = map_user.selectquater_state_wise_total_registered_user(
                            quater_option)
                        plotly.geo_map(selectquater_state_wise_total_registered_user,
                                       'State', 'Registered Users', 'State wise Registered Users')

                    elif option == 'App Opens':
                        selectquater_state_wise_total_app_opens = map_user.selectquater_state_wise_total_app_opens(
                            quater_option)
                        plotly.geo_map(selectquater_state_wise_total_app_opens,
                                       'State', 'App Opens', 'State wise App Opens')

                    # line chart
                    selectquater_year_wise_total_user_count = aggregated_user.selectquater_year_wise_total_user_count(quater_option)
                    plotly.line_chart(df=selectquater_year_wise_total_user_count,x='Year',y='User Count', 
                                        text='User', textposition='top left', color='#ba6e77', title='Year wise User Count', title_x=0.40)
                    
                    # vertical_bar chart
                    selectquater_brand_wise_total_user_count = aggregated_user.selectquater_brand_wise_total_user_count(quater_option)
                    plotly.vertical_bar_chart(df=selectquater_brand_wise_total_user_count,x='User Brand',y='User Count', 
                                                text='User', color='#5D9A96', title='Brand wise User Count', title_x=0.40)
                    
                    # multi line chart
                    selectquater_year_brand_wise_total_user_count = aggregated_user.selectquater_year_brand_wise_total_user_count(quater_option)
                    plotly.multi_line_chart(df=selectquater_year_brand_wise_total_user_count, y='User Count', x='Year',
                                                colorcolumn='User Brand', title='Year - Brand wise User Count', title_x=0.40)

                    # pie chart
                    col1, col2 = st.columns(2)
                    selectquater_year_wise_total_registered_user = map_user.selectquater_year_wise_total_registered_user(quater_option)
                    selectquater_year_wise_total_app_opens = map_user.selectquater_year_wise_total_app_opens(quater_option)
                    with col1:
                        plotly.pie_chart(df=selectquater_year_wise_total_registered_user, x='Year', y='Registered Users',
                                        title='Year wise Registered Users')
                    with col2:
                        plotly.pie_chart(df=selectquater_year_wise_total_app_opens, x='Year', y='App Opens',
                                        title='Year wise App Opens')
    
        elif analysis == 'District':
            transactions, users = st.tabs(['Transactions', 'Users'])
            with transactions:
                # map
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    state_option = st.selectbox('State:       ', state_list())
                with col2:
                    year_option = st.selectbox(
                        'Year:       ', ['Over All', '2022', '2021', '2020', '2019', '2018'])
                with col3:
                    quater_option = st.selectbox(
                        'Quater:       ', ['Over All', 'Q1', 'Q2', 'Q3', 'Q4'])

                if state_option:
                    if year_option == 'Over All' and quater_option == 'Over All':
                        st.dataframe(map_transaction.selectstate_district_wise_total_transaction_count(state_option))
                        st.dataframe(map_transaction.selectstate_district_wise_total_transaction_amount(state_option))

                    elif year_option != 'Over All' and quater_option == 'Over All':
                        st.dataframe(map_transaction.selectstate_selectyear_district_wise_total_transaction_count(
                            state_option, year_option))
                        st.dataframe(map_transaction.selectstate_selectyear_district_wise_total_transaction_amount(
                            state_option, year_option))

                    elif year_option == 'Over All' and quater_option != 'Over All':
                        st.dataframe(map_transaction.selectstate_selectquater_district_wise_total_transaction_count(
                            state_option, quater_option))
                        st.dataframe(map_transaction.selectstate_selectquater_district_wise_total_transaction_amount(
                            state_option, quater_option))

                    elif year_option != 'Over All' and quater_option != 'Over All':
                        st.dataframe(map_transaction.selectstate_selectyear_selectquater_district_wise_total_transaction_count(
                            state_option, year_option, quater_option))
                        st.dataframe(map_transaction.selectstate_selectyear_selectquater_district_wise_total_transaction_amount(
                            state_option, year_option, quater_option))

            with users:
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    state_option = st.selectbox('State:        ', state_list())
                with col2:
                    year_option = st.selectbox(
                        'Year:        ', ['Over All', '2022', '2021', '2020', '2019', '2018'])
                with col3:
                    quater_option = st.selectbox(
                        'Quater:        ', ['Over All', 'Q1', 'Q2', 'Q3', 'Q4'])

                if state_option:
                    if year_option == 'Over All' and quater_option == 'Over All':
                        st.dataframe(
                            map_user.selectstate_district_wise_total_registered_user(state_option))
                        st.dataframe(
                            map_user.selectstate_district_wise_total_app_opens(state_option))

                    elif year_option != 'Over All' and quater_option == 'Over All':
                        st.dataframe(map_user.selectstate_selectyear_district_wise_total_registered_user(
                            state_option, year_option))
                        st.dataframe(map_user.selectstate_selectyear_district_wise_total_app_opens(
                            state_option, year_option))

                    elif year_option == 'Over All' and quater_option != 'Over All':
                        st.dataframe(map_user.selectstate_selectquater_district_wise_total_registered_user(
                            state_option, quater_option))
                        st.dataframe(map_user.selectstate_selectquater_district_wise_total_app_opens(
                            state_option, quater_option))

                    elif year_option != 'Over All' and quater_option != 'Over All':
                        st.dataframe(map_user.selectstate_selectyear_selectquater_district_wise_total_registered_user(
                            state_option, year_option, quater_option))
                        st.dataframe(map_user.selectstate_selectyear_selectquater_district_wise_total_app_opens(
                            state_option, year_option, quater_option))

        elif analysis == 'Transaction Type':
            # horizontal_bar chart
            col1, col2 = st.columns(2)
            type_wise_total_transaction_count = aggregated_transaction.type_wise_total_transaction_count()
            type_wise_total_transaction_amount = aggregated_transaction.type_wise_total_transaction_amount()
            with col1:
                plotly.horizontal_bar_chart(df=type_wise_total_transaction_count,y='Transaction Type',x='Transaction Count', 
                                            text='Transaction', color='#5D9A96', title='Type wise Transaction Count')
            with col2:
                plotly.horizontal_bar_chart(df=type_wise_total_transaction_amount,y='Transaction Type',x='Transaction Amount', 
                                            text='Transaction', color='#5cb85c', title='Type wise Transaction Amount')

            # multi line chart
            type_year_wise_total_transaction_count = aggregated_transaction.type_year_wise_total_transaction_count()
            type_year_wise_total_transaction_amount = aggregated_transaction.type_year_wise_total_transaction_amount()
            plotly.multi_line_chart(df=type_year_wise_total_transaction_count, y='Transaction Count',x='Year',
                                        colorcolumn='Transaction Type', title='Year - Type wise Transaction Count', title_x=0.35)
            plotly.multi_line_chart(df=type_year_wise_total_transaction_amount, y='Transaction Amount',x='Year',
                                        colorcolumn='Transaction Type', title='Year - Type wise Transaction Amount', title_x=0.35)

            # multi line chart
            col1, col2 = st.columns(2)
            type_quater_wise_total_transaction_count = aggregated_transaction.type_quater_wise_total_transaction_count()
            type_quater_wise_total_transaction_amount = aggregated_transaction.type_quater_wise_total_transaction_amount()
            with col1:
                plotly.multi_line_chart(df=type_quater_wise_total_transaction_count, y='Transaction Count',x='Quater',
                                        colorcolumn='Transaction Type', title='Quater - Type wise Transaction Count')
            with col2:
                plotly.multi_line_chart(df=type_quater_wise_total_transaction_amount, y='Transaction Amount',x='Quater',
                                        colorcolumn='Transaction Type', title='Quater - Type wise Transaction Amount')
    

            type_list = ['Recharge & bill payments', 'Peer-to-peer payments',
                         'Merchant payments', 'Financial Services', 'Others']
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.write('')
                type_option = st.selectbox(
                    'Transaction Type        ', type_list)
                advanced_filters = st.checkbox('Advanced Filters     ')
                st.write('')
            if advanced_filters:
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    list_state = ['Select One']
                    list_state.extend(state_list())
                    state_option = st.selectbox('State:        ', list_state)
                with col2:
                    year_option = st.selectbox(
                        'Year:        ', ['Select One', '2022', '2021', '2020', '2019', '2018'])
                with col3:
                    quater_option = st.selectbox(
                        'Quater:        ', ['Select One', 'Q1', 'Q2', 'Q3', 'Q4'])
                    st.write('')

            if type_option and advanced_filters:

                if state_option != 'Select One' and year_option == 'Select One' and quater_option == 'Select One':
                    # vertical_bar chart
                    col1, col2 = st.columns(2)
                    selectstate_selecttype_year_wise_total_transaction_count = aggregated_transaction.selectstate_selecttype_year_wise_total_transaction_count(
                                                                                                        state_option, type_option)
                    selectstate_selecttype_year_wise_total_transaction_amount = aggregated_transaction.selectstate_selecttype_year_wise_total_transaction_amount(
                                                                                                        state_option, type_option)
                    with col1:
                        plotly.vertical_bar_chart(df=selectstate_selecttype_year_wise_total_transaction_count,x='Year',y='Transaction Count', 
                                                    text='Transaction', color='#5D9A96', title='Year wise Transaction Count')
                    with col2:
                        plotly.vertical_bar_chart(df=selectstate_selecttype_year_wise_total_transaction_amount,x='Year',y='Transaction Amount', 
                                                    text='Transaction', color='#5cb85c', title='Year wise Transaction Amount')

                    # pie chart
                    col1, col2 = st.columns(2)
                    selectstate_selecttype_quater_wise_total_transaction_count = aggregated_transaction.selectstate_selecttype_quater_wise_total_transaction_count(
                                                                                                        state_option, type_option)
                    selectstate_selecttype_quater_wise_total_transaction_amount = aggregated_transaction.selectstate_selecttype_quater_wise_total_transaction_amount(
                                                                                                            state_option, type_option)
                    with col1:
                        plotly.pie_chart(df=selectstate_selecttype_quater_wise_total_transaction_count, x='Quater', y='Transaction Count',
                                        title='Quater wise Transaction Count')
                    with col2:
                        plotly.pie_chart(df=selectstate_selecttype_quater_wise_total_transaction_amount, x='Quater', y='Transaction Amount',
                                        title='Quater wise Transaction Amount') 
                        
                    # multi line chart
                    col1, col2 = st.columns(2)
                    selectstate_selecttype_year_quater_wise_total_transaction_count = aggregated_transaction.selectstate_selecttype_year_quater_wise_total_transaction_count(
                                                                                                                state_option, type_option)
                    selectstate_selecttype_year_quater_wise_total_transaction_amount = aggregated_transaction.selectstate_selecttype_year_quater_wise_total_transaction_amount(
                                                                                                                state_option, type_option)
                    with col1:
                        plotly.multi_line_chart(df=selectstate_selecttype_year_quater_wise_total_transaction_count, y='Transaction Count',x='Quater',
                                                colorcolumn='Year', title='Year - Quater wise Transaction Count')
                    with col2:
                        plotly.multi_line_chart(df=selectstate_selecttype_year_quater_wise_total_transaction_amount, y='Transaction Amount',x='Quater',
                                                colorcolumn='Year', title='Year - Quater wise Transaction Amount')

                elif state_option == 'Select One' and year_option != 'Select One' and quater_option == 'Select One':
                    option = st.radio(
                        '', ['Transaction Count', 'Transaction Amount'], horizontal=True)
                    if option == 'Transaction Count':
                        selectyear_selecttype_state_wise_total_transaction_count = aggregated_transaction.selectyear_selecttype_state_wise_total_transaction_count(
                            year_option, type_option)
                        plotly.geo_map(selectyear_selecttype_state_wise_total_transaction_count,
                                       'State', 'Transaction Count', 'State wise Transaction Count')

                    elif option == 'Transaction Amount':
                        selectyear_selecttype_state_wise_total_transaction_amount = aggregated_transaction.selectyear_selecttype_state_wise_total_transaction_amount(
                            year_option, type_option)
                        plotly.geo_map(selectyear_selecttype_state_wise_total_transaction_amount,
                                       'State', 'Transaction Amount', 'State wise Transaction Amount')

                    # vertical_bar chart
                    col1, col2 = st.columns(2)
                    selectyear_selecttype_quater_wise_total_transaction_count = aggregated_transaction.selectyear_selecttype_quater_wise_total_transaction_count(
                                                                                                        year_option, type_option)
                    selectyear_selecttype_quater_wise_total_transaction_amount = aggregated_transaction.selectyear_selecttype_quater_wise_total_transaction_amount(
                                                                                                        year_option, type_option)
                    with col1:
                        plotly.vertical_bar_chart(df=selectyear_selecttype_quater_wise_total_transaction_count,x='Quater',y='Transaction Count', 
                                                    text='Transaction', color='#5D9A96', title='Quater wise Transaction Count')
                    with col2:
                        plotly.vertical_bar_chart(df=selectyear_selecttype_quater_wise_total_transaction_amount,x='Quater',y='Transaction Amount', 
                                                    text='Transaction', color='#5cb85c', title='Quater wise Transaction Amount')
                        
                elif state_option == 'Select One' and year_option == 'Select One' and quater_option != 'Select One':
                    option = st.radio(
                        '', ['Transaction Count', 'Transaction Amount'], horizontal=True)
                    if option == 'Transaction Count':
                        selectquater_selecttype_state_wise_total_transaction_count = aggregated_transaction.selectquater_selecttype_state_wise_total_transaction_count(
                            quater_option, type_option)
                        plotly.geo_map(selectquater_selecttype_state_wise_total_transaction_count,
                                       'State', 'Transaction Count', 'State wise Transaction Count')

                    elif option == 'Transaction Amount':
                        selectquater_selecttype_state_wise_total_transaction_amount = aggregated_transaction.selectquater_selecttype_state_wise_total_transaction_amount(
                            quater_option, type_option)
                        plotly.geo_map(selectquater_selecttype_state_wise_total_transaction_amount,
                                       'State', 'Transaction Amount', 'State wise Transaction Amount')

                    # vertical_bar chart
                    col1, col2 = st.columns(2)
                    selectquater_selecttype_year_wise_total_transaction_count = aggregated_transaction.selectquater_selecttype_year_wise_total_transaction_count(
                                                                                                        quater_option, type_option)
                    selectquater_selecttype_year_wise_total_transaction_amount = aggregated_transaction.selectquater_selecttype_year_wise_total_transaction_amount(
                                                                                                        quater_option, type_option)
                    with col1:
                        plotly.vertical_bar_chart(df=selectquater_selecttype_year_wise_total_transaction_count,x='Year',y='Transaction Count', 
                                                    text='Transaction', color='#5D9A96', title='Year wise Transaction Count')
                    with col2:
                        plotly.vertical_bar_chart(df=selectquater_selecttype_year_wise_total_transaction_amount,x='Year',y='Transaction Amount', 
                                                    text='Transaction', color='#5cb85c', title='Year wise Transaction Amount')

                elif state_option != 'Select One' and year_option != 'Select One' and quater_option == 'Select One':
                    # vertical_bar chart
                    col1, col2 = st.columns(2)
                    selectstate_selectyear_selecttype_quater_wise_total_transaction_count = aggregated_transaction.selectstate_selectyear_selecttype_quater_wise_total_transaction_count(
                                                                                                                    state_option, year_option, type_option)
                    selectstate_selectyear_selecttype_quater_wise_total_transaction_amount = aggregated_transaction.selectstate_selectyear_selecttype_quater_wise_total_transaction_amount(
                                                                                                                    state_option, year_option, type_option)
                    with col1:
                        plotly.vertical_bar_chart(df=selectstate_selectyear_selecttype_quater_wise_total_transaction_count,x='Quater',y='Transaction Count', 
                                                    text='Transaction', color='#5D9A96', title='Quater wise Transaction Count')
                    with col2:
                        plotly.vertical_bar_chart(df=selectstate_selectyear_selecttype_quater_wise_total_transaction_amount,x='Quater',y='Transaction Amount', 
                                                    text='Transaction', color='#5cb85c', title='Quater wise Transaction Amount')
                        
                elif state_option != 'Select One' and year_option == 'Select One' and quater_option != 'Select One':
                    # vertical_bar chart
                    col1, col2 = st.columns(2)
                    selectstate_selectquater_selecttype_year_wise_total_transaction_count = aggregated_transaction.selectstate_selectquater_selecttype_year_wise_total_transaction_count(
                                                                                                                    state_option, quater_option, type_option)
                    selectstate_selectquater_selecttype_year_wise_total_transaction_amount = aggregated_transaction.selectstate_selectquater_selecttype_year_wise_total_transaction_amount(
                                                                                                                    state_option, quater_option, type_option)
                    with col1:
                        plotly.vertical_bar_chart(df=selectstate_selectquater_selecttype_year_wise_total_transaction_count,x='Year',y='Transaction Count', 
                                                    text='Transaction', color='#5D9A96', title='Year wise Transaction Count')
                    with col2:
                        plotly.vertical_bar_chart(df=selectstate_selectquater_selecttype_year_wise_total_transaction_amount,x='Year',y='Transaction Amount', 
                                                    text='Transaction', color='#5cb85c', title='Year wise Transaction Amount')

                elif state_option == 'Select One' and year_option != 'Select One' and quater_option != 'Select One':
                    option = st.radio(
                        '', ['Transaction Count', 'Transaction Amount'], horizontal=True)
                    if option == 'Transaction Count':
                        selectyear_selectquater_selecttype_state_wise_total_transaction_count = aggregated_transaction.selectyear_selectquater_selecttype_state_wise_total_transaction_count(
                            year_option, quater_option, type_option)
                        plotly.geo_map(selectyear_selectquater_selecttype_state_wise_total_transaction_count,
                                       'State', 'Transaction Count', 'State wise Transaction Count')

                    elif option == 'Transaction Amount':
                        selectyear_selectquater_selecttype_state_wise_total_transaction_amount = aggregated_transaction.selectyear_selectquater_selecttype_state_wise_total_transaction_amount(
                            year_option, quater_option, type_option)
                        plotly.geo_map(selectyear_selectquater_selecttype_state_wise_total_transaction_amount,
                                       'State', 'Transaction Amount', 'State wise Transaction Amount')

            else:
                option = st.radio('', ['Transaction Count', 'Transaction Amount'], horizontal=True)
                if option == 'Transaction Count':
                    selecttype_state_wise_total_transaction_count = aggregated_transaction.selecttype_state_wise_total_transaction_count(
                        type_option)
                    plotly.geo_map(selecttype_state_wise_total_transaction_count,
                                   'State', 'Transaction Count', 'State wise Transaction Count')

                elif option == 'Transaction Amount':
                    selecttype_state_wise_total_transaction_amount = aggregated_transaction.selecttype_state_wise_total_transaction_amount(
                        type_option)
                    plotly.geo_map(selecttype_state_wise_total_transaction_amount,
                                   'State', 'Transaction Amount', 'State wise Transaction Amount')

                # vertical_bar chart
                col1, col2 = st.columns(2)
                selecttype_year_wise_total_transaction_count = aggregated_transaction.selecttype_year_wise_total_transaction_count(type_option)
                selecttype_year_wise_total_transaction_amount = aggregated_transaction.selecttype_year_wise_total_transaction_amount(type_option)
                with col1:
                    plotly.vertical_bar_chart(df=selecttype_year_wise_total_transaction_count,x='Year',y='Transaction Count', 
                                                text='Transaction', color='#5D9A96', title='Year wise Transaction Count')
                with col2:
                    plotly.vertical_bar_chart(df=selecttype_year_wise_total_transaction_amount,x='Year',y='Transaction Amount', 
                                                text='Transaction', color='#5cb85c', title='Year wise Transaction Amount')

                # pie chart
                col1, col2 = st.columns(2)
                selecttype_quater_wise_total_transaction_count = aggregated_transaction.selecttype_quater_wise_total_transaction_count(type_option)
                selecttype_quater_wise_total_transaction_amount = aggregated_transaction.selecttype_quater_wise_total_transaction_amount(type_option)
                with col1:
                    plotly.pie_chart(df=selecttype_quater_wise_total_transaction_count, x='Quater', y='Transaction Count',
                                    title='Quater wise Transaction Count')
                with col2:
                    plotly.pie_chart(df=selecttype_quater_wise_total_transaction_amount, x='Quater', y='Transaction Amount',
                                    title='Quater wise Transaction Amount') 

                # multi line chart
                col1, col2 = st.columns(2)
                selecttype_year_quater_wise_total_transaction_count = aggregated_transaction.selecttype_year_quater_wise_total_transaction_count(type_option)
                selecttype_year_quater_wise_total_transaction_amount = aggregated_transaction.selecttype_year_quater_wise_total_transaction_amount(type_option)
                with col1:
                    plotly.multi_line_chart(df=selecttype_year_quater_wise_total_transaction_count, y='Transaction Count',x='Quater',
                                            colorcolumn='Year', title='Year - Quater wise Transaction Count')
                with col2:
                    plotly.multi_line_chart(df=selecttype_year_quater_wise_total_transaction_amount, y='Transaction Amount',x='Quater',
                                            colorcolumn='Year', title='Year - Quater wise Transaction Amount')
    
        elif analysis == 'User Brand':
            # vertical_bar chart
            brand_wise_total_user_count = aggregated_user.brand_wise_total_user_count()
            plotly.vertical_bar_chart(df=brand_wise_total_user_count,x='User Brand',y='User Count', 
                                        text='User', color='#5D9A96', title='Brand wise User Count', title_x=0.42)

            # multi line chart
            year_brand_wise_total_user_count = aggregated_user.year_brand_wise_total_user_count()
            quater_brand_wise_total_user_count = aggregated_user.quater_brand_wise_total_user_count()
            plotly.multi_line_chart(df=year_brand_wise_total_user_count, y='User Count',x='Year',
                                    colorcolumn='User Brand', title='Year - Brand wise User Count', height=600, title_x=0.42)
            plotly.multi_line_chart(df=quater_brand_wise_total_user_count, y='User Count',x='Quater',
                                    colorcolumn='User Brand', title='Quater - Brand wise User Count', height=600, title_x=0.42)
    
            # map
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.write('')
                brand_option = st.selectbox('User Brand         ', brand_list())
                advanced_filters = st.checkbox('Advanced Filters     ')
                st.write('')
            
            if advanced_filters:
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    list_state = ['Select One']
                    list_state.extend(state_list())
                    state_option = st.selectbox('State:         ', list_state)
                with col2:
                    year_option = st.selectbox('Year:         ', ['Select One', '2022', '2021', '2020', '2019', '2018'])
                with col3:
                    quater_option = st.selectbox('Quater:         ', ['Select One', 'Q1', 'Q2', 'Q3', 'Q4'])
                    st.write('')

            if brand_option and advanced_filters:
                if state_option != 'Select One' and year_option == 'Select One' and quater_option == 'Select One':
                    col1, col2 = st.columns(2)
                    with col1:
                        # vertical_bar chart
                        selectstate_selectbrand_year_wise_total_user_count = aggregated_user.selectstate_selectbrand_year_wise_total_user_count(
                                                                                            state_option, brand_option)
                        plotly.vertical_bar_chart(df=selectstate_selectbrand_year_wise_total_user_count,x='Year',y='User Count', 
                                                    text='User', color='#5D9A96', title='Year wise User Count', title_x=0.30)
                    with col2:
                        # pie chart
                        selectstate_selectbrand_quater_wise_total_user_count = aggregated_user.selectstate_selectbrand_quater_wise_total_user_count(
                                                                                                state_option, brand_option)
                        plotly.pie_chart(df=selectstate_selectbrand_quater_wise_total_user_count, x='Quater', y='User Count',
                                                    title='Quater wise User Count')
                    
                    # multi line chart
                    selectstate_selectbrand_year_quater_wise_total_user_count = aggregated_user.selectstate_selectbrand_year_quater_wise_total_user_count(
                                                                                                state_option, brand_option)
                    plotly.marker_multi_line_chart(df=selectstate_selectbrand_year_quater_wise_total_user_count, y='User Count',x='Quater', colorcolumn='Year',
                                                    title='Year - Quater wise User Count', title_x=0.35, text='User', textposition='top center')
                
                elif state_option == 'Select One' and year_option != 'Select One' and quater_option == 'Select One':
                    # geo map
                    selectyear_selectbrand_state_wise_total_user_count = aggregated_user.selectyear_selectbrand_state_wise_total_user_count(
                        year_option, brand_option)
                    plotly.geo_map(selectyear_selectbrand_state_wise_total_user_count,
                                   'State', 'User Count', 'State wise User Count')

                    # vertical_bar chart
                    selectyear_selectbrand_quater_wise_total_user_count = aggregated_user.selectyear_selectbrand_quater_wise_total_user_count(
                                                                                            year_option, brand_option)
                    plotly.vertical_bar_chart(df=selectyear_selectbrand_quater_wise_total_user_count,x='Quater',y='User Count', 
                                                    text='User', color='#5D9A96', title='Quater wise User Count', title_x=0.40)

                elif state_option == 'Select One' and year_option == 'Select One' and quater_option != 'Select One':
                    # geo map
                    selectquater_selectbrand_state_wise_total_user_count = aggregated_user.selectquater_selectbrand_state_wise_total_user_count(
                        quater_option, brand_option)
                    plotly.geo_map(selectquater_selectbrand_state_wise_total_user_count,
                                   'State', 'User Count', 'State wise User Count')

                    # pie chart
                    selectquater_selectbrand_year_wise_total_user_count = aggregated_user.selectquater_selectbrand_year_wise_total_user_count(
                                                                                            quater_option, brand_option)
                    plotly.pie_chart(df=selectquater_selectbrand_year_wise_total_user_count, x='Year', y='User Count',
                                    title='Year wise User Count', title_x=0.37)

                elif state_option != 'Select One' and year_option != 'Select One' and quater_option == 'Select One':
                    # vertical_bar chart
                    selectstate_selectyear_selectbrand_quater_wise_total_user_count = aggregated_user.selectstate_selectyear_selectbrand_quater_wise_total_user_count(
                                                                                                        state_option, year_option, brand_option)
                    plotly.vertical_bar_chart(df=selectstate_selectyear_selectbrand_quater_wise_total_user_count,x='Quater',y='User Count', 
                                                    text='User', color='#5D9A96', title='Quater wise User Count', title_x=0.40)

                elif state_option != 'Select One' and year_option == 'Select One' and quater_option != 'Select One':
                    # pie chart
                    selectstate_selectquater_selectbrand_year_wise_total_user_count = aggregated_user.selectstate_selectquater_selectbrand_year_wise_total_user_count(
                                                                                                        state_option, quater_option, brand_option)
                    plotly.pie_chart(df=selectstate_selectquater_selectbrand_year_wise_total_user_count, x='Year', y='User Count',
                                    title='Year wise User Count', title_x=0.37)
                    
                elif state_option == 'Select One' and year_option != 'Select One' and quater_option != 'Select One':
                    # geo map
                    selectyear_selectquater_selectbrand_state_wise_total_user_count = aggregated_user.selectyear_selectquater_selectbrand_state_wise_total_user_count(
                        year_option, quater_option, brand_option)
                    plotly.geo_map(selectyear_selectquater_selectbrand_state_wise_total_user_count,
                                   'State', 'User Count', 'State wise User Count')

            else:
                # geo map
                selectbrand_state_wise_total_user_count = aggregated_user.selectbrand_state_wise_total_user_count(
                                                                            brand_option)
                plotly.geo_map(selectbrand_state_wise_total_user_count,'State', 'User Count', 'State wise User Count')

                col1, col2 = st.columns(2)
                with col1:
                    # vertical_bar chart
                    selectbrand_year_wise_total_user_count = aggregated_user.selectbrand_year_wise_total_user_count(brand_option)
                    plotly.vertical_bar_chart(df=selectbrand_year_wise_total_user_count,x='Year',y='User Count', 
                                                text='User', color='#5D9A96', title='Year wise User Count', title_x=0.30)
                with col2:
                    # pie chart
                    selectbrand_quater_wise_total_user_count = aggregated_user.selectbrand_quater_wise_total_user_count(brand_option)
                    plotly.pie_chart(df=selectbrand_quater_wise_total_user_count, x='Quater', y='User Count',
                                        title='Quater wise User Count')
                
                # multi line chart
                selectbrand_year_quater_wise_total_user_count = aggregated_user.selectbrand_year_quater_wise_total_user_count(brand_option)
                plotly.marker_multi_line_chart(df=selectbrand_year_quater_wise_total_user_count, y='User Count',x='Quater', colorcolumn='Year',
                                                title='Year - Quater wise User Count', title_x=0.35, text='User', textposition='top center')

        elif analysis == 'Top 10 Insights':
            transactions, users = st.tabs(['Transactions', 'Users'])
            with transactions:
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    year_option = st.selectbox(
                        'Year:         ', ['2022', '2021', '2020', '2019', '2018'])
                with col2:
                    quater_option = st.selectbox(
                        'Quater:         ', ['Q1', 'Q2', 'Q3', 'Q4'])

                if year_option and quater_option:
                    states, districts, pincodes = st.tabs(['States', 'Districts', 'Pincodes'])
                    with states:
                        # vertical_bar chart
                        selectyear_selectquater_state_wise_top10_transaction = top_transaction_and_user.selectyear_selectquater_state_wise_top10_transaction(
                                                                                                        year_option, quater_option)
                        plotly.top10_transaction_state_vertical_bar_chart(df=selectyear_selectquater_state_wise_top10_transaction,x='State',y='Transaction Amount', 
                                text='Amount', color='#5D9A96', title='Top 10 States', title_x=0.42)
                    
                    with districts:
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            list_state = ['Over All']
                            list_state.extend(state_list())
                            state_option = st.selectbox(
                                'State:         ', list_state)
                        if state_option == 'Over All':
                            # vertical_bar chart
                            selectyear_selectquater_district_wise_top10_transaction = top_transaction_and_user.selectyear_selectquater_district_wise_top10_transaction(
                                                                                                                year_option, quater_option)
                            plotly.top10_transaction_district_vertical_bar_chart(df=selectyear_selectquater_district_wise_top10_transaction,x='District_State',y='Transaction Amount', 
                                    text='Amount', color='#5D9A96', title='Top 10 Districts', title_x=0.45)

                        else:
                            # vertical_bar chart
                            selectstate_selectyear_selectquater_district_wise_top10_transaction = top_transaction_and_user.selectstate_selectyear_selectquater_district_wise_top10_transaction(
                                                                                                                            state_option, year_option, quater_option)
                            plotly.top10_transaction_district_vertical_bar_chart(df=selectstate_selectyear_selectquater_district_wise_top10_transaction,x='District',y='Transaction Amount', 
                                    text='Amount', color='#5D9A96', title='Top 10 Districts', title_x=0.45)
                                                
                    with pincodes:
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            list_state = ['Over All']
                            list_state.extend(state_list())
                            state_option = st.selectbox(
                                'State:          ', list_state)
                        if state_option == 'Over All':
                            # vertical_bar chart
                            selectyear_selectquater_pincode_wise_top10_transaction = top_transaction_and_user.selectyear_selectquater_pincode_wise_top10_transaction(
                                                                                                                year_option, quater_option)
                            plotly.top10_transaction_pincode_vertical_bar_chart(df=selectyear_selectquater_pincode_wise_top10_transaction,x='Pincode_District_State',y='Transaction Amount', 
                                    text='Amount', color='#5D9A96', title='Top 10 Pincodes', title_x=0.42)

                        else:
                            # vertical_bar chart
                            selectstate_selectyear_selectquater_pincode_wise_top10_transaction = top_transaction_and_user.selectstate_selectyear_selectquater_pincode_wise_top10_transaction(
                                                                                                                            state_option, year_option, quater_option)
                            plotly.top10_transaction_pincode_vertical_bar_chart(df=selectstate_selectyear_selectquater_pincode_wise_top10_transaction,x='Pincode_District',y='Transaction Amount', 
                                    text='Amount', color='#5D9A96', title='Top 10 Pincodes', title_x=0.42)

            with users:
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    year_option = st.selectbox(
                        'Year:           ', ['2022', '2021', '2020', '2019', '2018'])
                with col2:
                    quater_option = st.selectbox('Quater:           ', [
                                                 'Q1', 'Q2', 'Q3', 'Q4'])

                if year_option and quater_option:
                    states, districts, pincodes = st.tabs(['States', 'Districts', 'Pincodes'])
                    with states:
                        # vertical_bar chart
                        selectyear_selectquater_state_wise_top10_user = top_transaction_and_user.selectyear_selectquater_state_wise_top10_user(
                                                                                                    year_option, quater_option)
                        plotly.top10_user_vertical_bar_chart(df=selectyear_selectquater_state_wise_top10_user,x='State',y='User Count', 
                                text='User', color='#5D9A96', title='Top 10 States', title_x=0.42)
                    
                    with districts:
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            list_state = ['Over All']
                            list_state.extend(state_list())
                            state_option = st.selectbox(
                                'State:           ', list_state)
                        if state_option == 'Over All':
                            # vertical_bar chart
                            selectyear_selectquater_district_wise_top10_user = top_transaction_and_user.selectyear_selectquater_district_wise_top10_user(
                                                                                                        year_option, quater_option)
                            plotly.top10_user_vertical_bar_chart(df=selectyear_selectquater_district_wise_top10_user,x='District_State',y='User Count', 
                                    text='User', color='#5D9A96', title='Top 10 Districts', title_x=0.45)

                        else:
                            # vertical_bar chart
                            selectstate_selectyear_selectquater_district_wise_top10_user = top_transaction_and_user.selectstate_selectyear_selectquater_district_wise_top10_user(
                                                                                                        state_option, year_option, quater_option)
                            plotly.top10_user_vertical_bar_chart(df=selectstate_selectyear_selectquater_district_wise_top10_user,x='District',y='User Count', 
                                    text='User', color='#5D9A96', title='Top 10 Districts', title_x=0.45)
                    
                    with pincodes:
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            list_state = ['Over All']
                            list_state.extend(state_list())
                            state_option = st.selectbox(
                                'State:            ', list_state)
                        if state_option == 'Over All':
                            # vertical_bar chart
                            selectyear_selectquater_pincode_wise_top10_user = top_transaction_and_user.selectyear_selectquater_pincode_wise_top10_user(
                                                                                                        year_option, quater_option)
                            plotly.top10_user_vertical_bar_chart(df=selectyear_selectquater_pincode_wise_top10_user,x='Pincode_District_State',y='User Count', 
                                    text='User', color='#5D9A96', title='Top 10 Pincodes', title_x=0.42)

                        else:
                            # vertical_bar chart
                            selectstate_selectyear_selectquater_pincode_wise_top10_user = top_transaction_and_user.selectstate_selectyear_selectquater_pincode_wise_top10_user(
                                                                                                                    state_option, year_option, quater_option)
                            plotly.top10_user_vertical_bar_chart(df=selectstate_selectyear_selectquater_pincode_wise_top10_user,x='Pincode_District',y='User Count', 
                                    text='User', color='#5D9A96', title='Top 10 Pincodes', title_x=0.42)


st.subheader('Please select the option below:')
st.code('1 - Data Collection')
st.code('2 - Data Overview')
st.code('3 - Migrating Data to SQL Database')
st.code('4 - Data Insights and Exploration')
st.code('5 - Exit')

list_options = ['Select one', 'Data Collection', 'Data Overview',
                'Migrating Data to SQL Database', 'Data Insights and Exploration', 'Exit']
option = st.selectbox('', list_options)

if option:
    if option == 'Data Collection':
        data_collection()
        st.success('Data successfully cloned from the PhonePe Pulse Git repository')
        st.balloons()

    elif option == 'Data Overview':
        data_overview()

    elif option == 'Migrating Data to SQL Database':
        try:
            data_load.sql_table_creation()
            data_load.data_migration()
            st.success('Data successfully Migrated to the SQL Database')
            st.balloons()
        except:
            st.warning('No PhonePe Pulse data found in your local directory')
    
    elif option == 'Data Insights and Exploration':
        try:
            data_analysis()
        except:
            st.warning('The SQL database is currently empty')

    elif option == 'Exit':
        st.success('Thank you for your time. Exiting the application')
        st.balloons()

