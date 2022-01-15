import streamlit as st
import requests
import os

API_URL = os.environ.get("API_URL")

def main():
    st.header("ADSB Dashboard")
    page = st.sidebar.selectbox("Choose a page", ["Homepage", "Models", "Owners"])

    if page == "Homepage":
        st.write("Please select a page on the left, or lookup a plane below.")
        icao = st.text_input("ICAO Lookup", "c0243e")
        st.write(load_icao(icao))
    elif page == "Models":
        st.title("Aircraft Models")
        num = st.slider('Number of rare models', min_value=1, max_value=100, value=15)
        st.table(load_overall_data(num, "models"))
    elif page == "Owners":
        st.title("Aircraft Owners")
        num = st.slider('Number of rare owners', min_value=1, max_value=100, value=15)
        st.table(load_overall_data(num, "owners"))

@st.cache
def load_icao(icao):
    response = requests.get(API_URL + "aircraft/" + icao)
    print(response)
    return response.json()

@st.cache
def load_overall_data(num, type):
    response = requests.get(API_URL + type + "?end=" + str(num -1))
    print(response)
    return response.json()

@st.cache
def load_daily_data(num, type, day):
    response = requests.get(API_URL + type + "?end=" + str(num -1))
    print(response)
    return response.json()

if __name__ == "__main__":
    main()
