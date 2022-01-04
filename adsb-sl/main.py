import streamlit as st
import requests

#API_URL = "http://docker1:8080/"
API_URL = "http://localhost:8005/"

def main():
    st.header("ADSB Dashboard")
    page = st.sidebar.selectbox("Choose a page", ["Homepage", "Models", "Owners"])

    if page == "Homepage":
        st.write("Please select a page on the left.")
    elif page == "Models":
        st.title("Aircraft Models")
        st.table(load_model_data())
    elif page == "Owners":
        st.title("Aircraft Owners")
        st.table(load_owner_data())

@st.cache
def load_model_data():
    response = requests.get(API_URL + "models?end=10")
    print(response)
    return response.json()

@st.cache
def load_owner_data():
    response = requests.get(API_URL + "owners?end=10")
    return response.json()

if __name__ == "__main__":
    main()
