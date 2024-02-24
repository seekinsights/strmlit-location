import streamlit as st
import geopandas as gpd
import pandas as pd
import googlemaps
import altair as alt
from snowflake.snowpark import functions as F
import uuid
from streamlit_searchbox import st_searchbox

conn = st.connection("snowflake",ttl=3600*3)

@st.cache_data(ttl=3600*6)
def load_geo():
    gdf_all_loc = gpd.read_parquet('static_files/geo_census_tr.parquet')
    return gdf_all_loc

@st.cache_data(ttl=3600*6)
def load_weather():
    session = conn.session()
    forecast = session.table("SHORT_RANGE_FORECAST").filter(F.col('DATE')>=F.current_date()).to_pandas()
    desc = pd.read_csv('static_files/weather_desc.csv',index_col=0)
    forecast = forecast.merge(desc, on=['WX_DESCRIPTION'])
    return forecast

@st.cache_data(ttl=3600*6)
def load_nn():
    nn = pd.read_parquet('static_files/nn_with_local.parquet')
    return nn

gdf_all_loc = load_geo()
forecast_df = load_weather()
nn_df = load_nn()

class locations():
    def __init__(self,fine_granularity):
        self.granularity=fine_granularity

    def with_gmaps(self,query):
        self.map_client = googlemaps.Client(key=st.secrets.gconnect.mkey)
        geocode_result = self.map_client.geocode(query,components={'country':'US'})
        return geocode_result

    def connect(self,map_data):

        self.map_data = map_data
        gdf_loc = gpd.GeoDataFrame(map_data,geometry=gpd.points_from_xy(map_data.lon, map_data.lat), crs="EPSG:4326" )
        self.gdf_loc = gdf_loc.sjoin_nearest(gdf_all_loc,max_distance=100)
        self.main_id = int(self.gdf_loc['GEOID'].iloc[0])
        self.main_cluster = int(self.gdf_loc['Cluster'].iloc[0])
        self.main_name = str(self.gdf_loc['name'].iloc[0])
        self.data_read()
        st.session_state['zip_code']=int(self.gdf_loc['ZIP_CODE'].iloc[0])

        return

    def data_read(self):
        session = conn.session()
        self.qloo_loc = session.table("seek_solutions.beth_sandbox.ct_qloo_rnks")\
                          .filter(F.col('GEOID') == self.main_id).to_pandas()

        self.eps_w_main = session.table("seek_solutions.beth_sandbox.ct_wide")\
                          .filter(F.col('GEOID') == self.main_id).to_pandas().merge(pd.DataFrame(gdf_all_loc.drop(columns='geometry')),
                                                    on=['GEOID'])

        self.ep_rank_main = session.table("seek_solutions.beth_sandbox.ct_rnks")\
                          .filter(F.col('GEOID') == self.main_id).to_pandas()
        self.weather_main = self.eps_w_main[['GEOID','AVE_WINTER_HIGH','AVE_WINTER_LOW',
                                             'WINTER_PRECIPITATION','AVE_SPRING_HIGH','AVE_SPRING_LOW',
                                             'SPRING_PRECIPITATION','AVE_SUMMER_HIGH',
                                             'AVE_SUMMER_LOW','SUMMER_PRECIPITATION','AVE_FALL_HIGH','AVE_FALL_LOW',
                                             'FALL_PRECIPITATION']]

        self.main_nn = nn_df[nn_df['GID']==self.main_id]
        return

    def top_text(self):
        f1 = self.ep_rank_main[self.ep_rank_main['CATEGORY_OUTLIER_RANK'] == 1].sort_values('OUTLIER_SCORE',
                                                            ascending=False).iloc[:10][['CATEGORY', 'SUBCATEGORY']]
        self.people = []
        for c, s in f1.values:
            if c == 'LANGUAGE':
                self.people.append('Speak' + s.split('-')[1])
            if c == 'RELIGION':
                self.people.append('Identify as ' + s.capitalize())
            if c == 'DWELLING_TYPE':
                self.people.append('Live in ' + s.lower().split('_')[-1]+' classified housing')
            if c == 'ETHNIC_GROUP':
                self.people.append('Have a ' + s.capitalize() + ' background')
            if c == 'AGE_GROUP':
                try:
                    _, _, start, end = s.split('_')
                    if end == '25':
                        self.people.append('Be part of the under 25 age group')
                    else:
                        self.people.append('Be part of the age group ' + start + ' to ' + end)
                except:
                    self.people.append('Be part of the age group 65 and up')
            if c == 'NET_WORTH':
                self.people.append('Have a ' + s.lower())
            if c == 'INCOME':
                self.people.append('Have an ' + s.lower())
            if c == 'EDUCATION':
                self.people.append('Have an education at ' + s + ' level')

        self.top_q_type = self.qloo_loc[self.qloo_loc['LOCAL_RANK']==1].SUBTYPE.iloc[0]
        self.top_q_name = self.qloo_loc[self.qloo_loc['LOCAL_RANK']==1].NAME.iloc[0]
        m_text = f"has a high affinity for the {self.top_q_type} :blue[{self.top_q_name}]"
        return m_text

    def new_summary(self,text):
        st.markdown(f'<p style="background-color:#1A237E;color:#FFFFFF;font-size:20px;border-radius:4%;">{text}</p>',
                    unsafe_allow_html=True)
        return

    def bullets(self):
        for i in range(3):
            st.markdown(f"- {self.people[i]}")

        st.markdown('''
            <style>
            [data-testid="stMarkdownContainer"] li{
                list-style-position: inside;
                background-color:#FFFFFF;color:black;
            }
            </style>
            ''', unsafe_allow_html=True)

         #   st.write(f'<p style="background-color:#1A237E;color:#FFFFFF;font-size:20px;border-radius:4%;"* {summary_bullets[0]}</p>',
         #           unsafe_allow_html=True)
        return
    def add_metric(self,label,value):

        st.metric(label=label, value=value)
        return

    def aff_frame(self):
        aff_df = self.qloo_loc[['NAME','LOCAL_RANK','SUBTYPE']].drop_duplicates(['NAME'])
        aff_df['LOCAL_RANK']=aff_df['LOCAL_RANK'].rank(ascending=True)
        aff_df = aff_df.sort_values(by='LOCAL_RANK', ascending=True).head(5)
        return st.dataframe(aff_df, use_container_width=True,hide_index=True)

    def bubble(self):

        return alt.Chart(self.qloo_loc, title='Top Cultural Affinities').mark_circle().encode(
            alt.X('NAME', axis=alt.Axis(labelLimit=200, labelAngle=-90)),
            alt.Y('LOCAL_BOOST_FACTOR', scale=alt.Scale(reverse=False)),
            color=alt.Color('SUBTYPE',legend=alt.Legend(orient="bottom")
        ))
    def map_clusters(self):
        d = gdf_all_loc[gdf_all_loc['Cluster']==self.main_cluster][['INTPTLONG','INTPTLAT','Cluster','GEOID','ZIP_CODE']]
        d = d.merge(self.main_nn[['GID','RANK','LOCALITY','STATE']],left_on='GEOID',right_on='GID',how='outer')
        d['most_similar_locations']=pd.notnull(d['RANK']).astype('int')
        cluster_data = d[d['most_similar_locations']==0]
        cluster_points = alt.Chart(cluster_data,title='Related Locations').mark_circle( color="#5885AF"
                ).encode(longitude='INTPTLONG',latitude='INTPTLAT').project( "albersUsa" )
        sim_data = d[d['most_similar_locations']==1]
        sim_points = alt.Chart(sim_data).mark_circle(color='#d4322c'
                ).encode(longitude='INTPTLONG',latitude='INTPTLAT',
                        )

        st.altair_chart(cluster_points +sim_points, use_container_width=True)
        st.write('Highest similarity shown in red')
        d = d.drop_duplicates(['LOCALITY','STATE','Cluster'])
        d['RANK']=d['RANK'].rank(ascending=True)
        d = d[['LOCALITY','STATE','RANK','ZIP_CODE']]
        st.divider()
        st.write('Most Similar Location Details')
        st.dataframe(d[d['RANK']<=10],use_container_width=True,hide_index=True)

        return

    def weather_section(self):

        col1, col2 = st.columns(2, gap='small')
        tile1 = col1.container(height=400)
        tile2 = col2.container(height=400)
        tile1.map(self.map_data, zoom=12, use_container_width=True)
        with tile2:
            upcoming_weather()

def deeper_chart(data1,data2,cats):
    range_ = ['#d4322c', "#f78d53", "#fed183", "#f9f7ae", "#cae986", "#84ca68", "#22964f"]
    domain = ['Extremely Low', 'Atypically Low', 'Low Average', 'Average', 'High Average', 'Atypically High',
              'Extremely High']
    chart1 = alt.Chart(data1, title = f'{cats[0].capitalize()} Comparisons to Typical US Area').mark_bar().encode(
    x = alt.X('SUBCATEGORY', axis=alt.Axis(labelLimit=200, labelAngle=-90)),
    y = alt.Y('OUTLIER_SCORE', title='Relative Importance'),
        color = alt.Color('CATEGORY_DESC',scale=alt.Scale(domain=domain, range=range_),
                          legend=alt.Legend(orient="right"))).properties(
        height=300)

    chart2 = alt.Chart(data2, title = f'{cats[1].capitalize()} Comparisons to Typical US Area').mark_bar().encode(
    x = alt.X('SUBCATEGORY', axis=alt.Axis(labelLimit=200, labelAngle=-90)),
    y = alt.Y('OUTLIER_SCORE', title='Relative Importance'),
        color = alt.Color('CATEGORY_DESC',scale=alt.Scale(domain=domain, range=range_))).properties(
        height=300)
    return alt.hconcat(chart1, chart2)

def create_layout(location_instance):
    location_instance.top_text()
    location_instance.new_summary(f'''Hi, were you looking for me? I'm {location_instance.main_name}, and I live really close to
      {st.session_state['f_address']}! I'm personally a little obsessed with {location_instance.top_q_name}, which might be
    unusual if you aren't from around here. The people here are more likely than in most U.S. areas to: ''')
    location_instance.bullets()
    st.divider()
    mcol1, mcol2,mcol3 = st.columns(3,gap='medium')
    with mcol1:
        location_instance.add_metric('Households with Children (%)',round(100*location_instance.eps_w_main['CHILDREN_HH'].iloc[0],2))
    with mcol2:
        location_instance.add_metric('Households Married (%)',round(100*location_instance.eps_w_main['MARRIAGE'].iloc[0],2))
    with mcol3:
        location_instance.add_metric('Adults, Women (%)',round(100*location_instance.eps_w_main['WOMEN'].iloc[0],2))
    st.divider()
    mcol4, mcol5, mcol6 = st.columns(3, gap='medium')
    with mcol4:
        location_instance.add_metric('Average Time in Residence',round(location_instance.eps_w_main['LENGTH_RESIDENCE'].iloc[0],2))
    with mcol5:
        location_instance.add_metric('Average Adult Age',round(location_instance.eps_w_main['AGE'].iloc[0],2))
    with mcol6:
        location_instance.add_metric('Average Household Size',round(location_instance.eps_w_main['HOUSEHOLD_SIZE'].iloc[0],1))
    st.divider()
    st.subheader('People in my Neighborhood like:')
    location_instance.aff_frame()
    location_instance.weather_section()

    st.subheader('Where else can I find more people like ' + location_instance.main_name + '?')
    st.divider()

    location_instance.map_clusters()

    exp_details = st.expander('Dive Deeper', expanded=False)
    with exp_details:

        categories = [('AGE_GROUP','RELIGION'),('EDUCATION','WEATHER'),('ETHNIC_GROUP','DWELLING_TYPE'),
                      ('INCOME','NET_WORTH')]
        for each in categories:
            ds1 = location_instance.ep_rank_main[location_instance.ep_rank_main['CATEGORY']==each[0]]
            ds2 = location_instance.ep_rank_main[location_instance.ep_rank_main['CATEGORY']==each[1]]

            st.altair_chart(deeper_chart(ds1,ds2,each),use_container_width=False)


def upcoming_weather():
    forecast = forecast_df[forecast_df['ZIPCODE']==st.session_state['zipcode']]

    points = alt.Chart(forecast,title='Upcoming Weather Forecast').mark_point(filled=True,color='black').encode(
        x=alt.X('DATE:T'),
        y=alt.Y('AVG_TEMP:Q',axis=alt.Axis(title='Daily Temperature Span (F)')),
                tooltip=['WX_DESCRIPTION:N','image:N'])

    error_bars = points.mark_rule().encode(y='MAX_TEMP',y2='MIN_TEMP' )
    st.altair_chart(points + error_bars  ,use_container_width=True)
    return
def execute():
    st.session_state['is_expanded'] = False
    initial = locations(fine_granularity=True)
    result = initial.with_gmaps(st.session_state['address'])
    location = result[0]['geometry']['location']
    initial.connect(pd.DataFrame({'lat': [location['lat']], 'lon': [location['lng']]}))
    try:
        st.session_state['f_address'] = result[0]['formatted_address']
        st.session_state['zipcode'] = result[0]['formatted_address'].split(',')[-2].split(' ')[-1]
        create_layout(initial)
    except:
        pass

    return
def search_address(searchaddress):

    if not searchaddress:
        return ['No results']
    map_client = googlemaps.Client(key=st.secrets.gconnect.mkey)
    result = map_client.places_autocomplete(input_text=searchaddress, components={'country': 'US'},
                                            session_token=uuid.uuid4())

    return [(x['description'], x['description']) for x in result]
linkcol1, linkcol2 = st.columns([.7, .3])
with linkcol2:
    st.page_link('https://seekinsights.com/', label='https://seekinsights.com/',
                 icon=None, disabled=False, use_container_width=True)
if 'is_expanded' not in st.session_state:
    st.session_state['is_expanded'] = True
top_container = st.container()
with top_container:
    st.subheader('Understand a Location')
    col1, col2, col3 = st.columns([.25,.5,.25])

    with col1:
        st.write(' ')
    with col2:
        st.image('images/Seek Logo fix-01.svg',use_column_width=True)
    with col3:
        st.write(' ')

    selected_value = st_searchbox(**dict(
    search_function=search_address,
    placeholder="Enter an Address Here",
    label=None,
    default="",
    clear_on_submit=True,
    key="searchbox"))

    if selected_value:
        st.info(f"{selected_value}")
        st.session_state['address']=selected_value

        execute()

