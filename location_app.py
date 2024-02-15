import streamlit as st
import geopandas as gpd
import pandas as pd
import googlemaps
import altair as alt
from snowflake.snowpark import functions as F


# Initialize connection.
conn = st.connection("snowflake")

@st.cache_data
def load_weather():
    session = conn.session()
    forecast = session.table("SHORT_RANGE_FORECAST").filter(F.col('DATE')>=F.current_date()).to_pandas()
    desc = pd.read_csv('static_files/weather_desc.csv',index_col=0)
    forecast = forecast.merge(desc, on=['WX_DESCRIPTION'])
    return forecast

forecast_df = load_weather()
st.write(forecast_df.head())

def upcoming_weather():

    forecast = forecast_df[forecast_df['ZIPCODE']==zipcode]

    points = alt.Chart(forecast,title='Upcoming Weather Forecast').mark_point(filled=True,color='black').encode(
        x=alt.X('DATE:T'),
        y=alt.Y('AVG_TEMP:Q',axis=alt.Axis(title='Daily Temperature Span (F)')),
                tooltip=['WX_DESCRIPTION:N','image:N'])

    error_bars = points.mark_rule().encode(y='MAX_TEMP',y2='MIN_TEMP' )
    images = points.mark_image().encode(y='image')
    st.altair_chart(points + error_bars  ,use_container_width=True)
    return

with st.sidebar:
    col1, col2, col3 = st.columns([.25,.5,.25])

    with col1:
        st.write(' ')
    with col2:
        st.image('images/Seek Logo fix-01.svg',use_column_width=True)
    with col3:
        st.write(' ')

    with st.form(key="address_form"):
        address = st.text_input("Address", key="address")
        city = st.text_input("City", key="city")
        state = st.text_input("State", key="state")
        zipcode = st.text_input('Zip',key='zipcode')
        st.form_submit_button("Get Location Details!", on_click=upcoming_weather)