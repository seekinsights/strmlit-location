import streamlit as st
import geopandas as gpd
import pandas as pd
import googlemaps
import altair as alt
from snowflake.snowpark import functions as F
import uuid
from streamlit_searchbox import st_searchbox
from typing import Any, List

#st.set_page_config(layout="wide")
# Initialize connection.
conn = st.connection("snowflake",ttl=3600*3)

@st.cache_data(ttl=3600*6)
def load_weather():
    session = conn.session()
    forecast = session.table("SHORT_RANGE_FORECAST").filter(F.col('DATE')>=F.current_date()).to_pandas()
    desc = pd.read_csv('static_files/weather_desc.csv',index_col=0)
    forecast = forecast.merge(desc, on=['WX_DESCRIPTION'])
    return forecast

forecast_df = load_weather()

class locations():
    def __init__(self,fine_granularity):
        self.granularity=fine_granularity
        self.gdf_all_loc = gpd.read_parquet('static_files/geo_census_tr.parquet')

    def with_gmaps(self,query):
        self.map_client = googlemaps.Client(key=st.secrets.gconnect.mkey)
        geocode_result = self.map_client.geocode(query,components={'country':'US'})
        return geocode_result

    def connect(self,map_data):

        self.map_data = map_data
        gdf_loc = gpd.GeoDataFrame(map_data,geometry=gpd.points_from_xy(map_data.lon, map_data.lat), crs="EPSG:4326" )
        self.gdf_loc = gdf_loc.sjoin_nearest(self.gdf_all_loc,max_distance=100)
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
                          .filter(F.col('GEOID') == self.main_id).to_pandas().merge(pd.DataFrame(self.gdf_all_loc.drop(columns='geometry')),
                                                    on=['GEOID'])

        self.ep_rank_main = session.table("seek_solutions.beth_sandbox.ct_rnks")\
                          .filter(F.col('GEOID') == self.main_id).to_pandas()
        self.weather_main = self.eps_w_main[['GEOID','AVE_WINTER_HIGH','AVE_WINTER_LOW',
                                             'WINTER_PRECIPITATION','AVE_SPRING_HIGH','AVE_SPRING_LOW',
                                             'SPRING_PRECIPITATION','AVE_SUMMER_HIGH',
                                             'AVE_SUMMER_LOW','SUMMER_PRECIPITATION','AVE_FALL_HIGH','AVE_FALL_LOW',
                                             'FALL_PRECIPITATION']]

        return

    def top_text(self):
        f1 = self.ep_rank_main[self.ep_rank_main['CATEGORY_OUTLIER_RANK'] == 1].sort_values('OUTLIER_SCORE',
                                                            ascending=False).iloc[:10][['CATEGORY', 'SUBCATEGORY']]
        people = []
        for c, s in f1.values:
            if c == 'LANGUAGE':
                people.append('speak' + s.split('-')[1])
            if c == 'RELIGION':
                people.append('identify as ' + s.capitalize())
            if c == 'DWELLING_TYPE':
                people.append('live in a ' + s.lower().replace('_', ' '))
            if c == 'ETHNIC_GROUP':
                people.append('have a ' + s.capitalize() + ' background')
            if c == 'AGE_GROUP':
                try:
                    _, _, start, end = s.split('_')
                    people.append('be part of age group ' + start + ' to ' + end)
                except:
                    pass
            if c == 'NET_WORTH':
                people.append('have a ' + s.lower())
            if c == 'INCOME':
                people.append('have an ' + s.lower())
            if c == 'EDUCATION':
                people.append('have an education at ' + s + ' level')
        self.people_summary = f'''The people here are more likely than those in most areas to {people[0]}, {people[1]}, {people[2]}, and {people[4]}.'''
        self.top_q_type = self.qloo_loc[self.qloo_loc['LOCAL_RANK']==1].SUBTYPE.iloc[0]
        self.top_q_name = self.qloo_loc[self.qloo_loc['LOCAL_RANK']==1].NAME.iloc[0]
        m_text = f"has a high affinity for the {self.top_q_type} :blue[{self.top_q_name}]"
        return m_text

    def new_summary(self,text):
        st.markdown(f'<p style="background-color:#1A237E;color:#FFFFFF;font-size:20px;border-radius:2%;">{text}</p>',
                    unsafe_allow_html=True)
        return

    def add_metric(self,label,value):
        st.metric(label=label, value=value)
        return
    def aff_frame(self):
        return st.dataframe(self.qloo_loc[['NAME','LOCAL_RANK','SUBTYPE']].sort_values(by='LOCAL_RANK', ascending=True).head(5), use_container_width=True)
    def bubble(self):

        return alt.Chart(self.qloo_loc, title='Top Cultural Affinities').mark_circle().encode(
            alt.X('NAME', axis=alt.Axis(labelLimit=200, labelAngle=-90)),
            alt.Y('LOCAL_BOOST_FACTOR', scale=alt.Scale(reverse=False)),
            color=alt.Color('SUBTYPE',legend=alt.Legend(orient="bottom")
        ))
    def map_clusters(self):
        d = self.gdf_all_loc[self.gdf_all_loc['Cluster']==self.main_cluster][['INTPTLONG','INTPTLAT','Cluster','ZIP_CODE']]
        return alt.Chart(d,title='Related Locations',
                         ).mark_circle().encode(longitude='INTPTLONG',latitude='INTPTLAT',tooltip=['ZIP_CODE:N']
           ).project( "albersUsa" )#.properties(width=800,height=800)


def create_layout(location_instance):
    location_instance.top_text()
    location_instance.new_summary(f'''Hi, were you looking for me? I'm {location_instance.main_name}, and I live really close to
      {st.session_state['f_address']}! I'm personally a little obsessed with {location_instance.top_q_name}, which might be
    unusual if you aren't from around here. {location_instance.people_summary}''')
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
    st.write('Local Cultural Affinities')
    location_instance.aff_frame()
    exp_map = st.expander('On a map',expanded=True)
    with exp_map:
        exp_col1,exp_col2 = st.columns(2,gap='small')
        exp_col1.map(location_instance.map_data, zoom=10, use_container_width=True)
        with exp_col2:
            upcoming_weather()
#
    st.subheader('Where else can I find more people like ' + location_instance.main_name + '?')
    st.divider()
    st.altair_chart(location_instance.map_clusters(), use_container_width=True)

    exp_details = st.expander('Dive Deeper', expanded=False)
    with exp_details:
        range_ = ['#d4322c', "#f78d53", "#fed183", "#f9f7ae", "#cae986", "#84ca68", "#22964f"]
        domain = ['Extremely Low', 'Atypically Low', 'Low Average', 'Average', 'High Average', 'Atypically High',
                  'Extremely High']
        st.altair_chart(alt.Chart(location_instance.ep_rank_main[location_instance.ep_rank_main['CATEGORY']!='LANGUAGE'],title='Area Comparisons to Typical US Area').mark_bar().encode(
            x=alt.X('SUBCATEGORY', axis=alt.Axis(labelLimit=200, labelAngle=-90)),
            y=alt.Y('OUTLIER_SCORE',title='Relative Importance'),color=alt.Color('CATEGORY_DESC',
                                                                                 scale=alt.Scale(domain=domain,range=range_),
                                                                                 legend=alt.Legend(orient="top",title=None)),
            facet=alt.Facet('CATEGORY',columns=3)
        ).resolve_scale(x='independent'),use_container_width=False)


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
    st.session_state['f_address'] = result[0]['formatted_address']
   # st.write(result[0]['formatted_address'])
    st.session_state['zipcode'] = result[0]['formatted_address'].split(',')[-2].split(' ')[-1]

   # st.markdown(initial.top_text())
    container1 = st.container()
    create_layout(initial)

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
    placeholder="Address",
    label=None,
    default="Address",
    clear_on_submit=True,
    key="searchbox"))

    if selected_value:
        st.info(f"{selected_value}")
        st.session_state['address']=selected_value

execute()

