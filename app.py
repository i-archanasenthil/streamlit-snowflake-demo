import streamlit as st
import pandas as pd


st.set_page_config(page_title= "Streamlit Snowflake Demo", layout = "wide", initial_sidebar_state = "expanded")
st.title("Streamlit Snowflake Demo")
st.write("This is a simple Streamlit app connected to Snowflake.")

st.sidebar.image("assets/Logo.bmp")
st.sidebar.title("Navigation")
st.sidebar.subheader("Select a page")

#initialize the session state for df to avoid losing data during re-runs
if 'original_df' not in st.session_state:
    st.session_state.original_df = pd.read_csv("vgsales.csv")

if 'editable_df' not in st.session_state:
    st.session_state.editable_df = st.session_state.original_df.copy()

#Initializing session state to track actve page
if 'active_page' not in st.session_state:
    st.session_state.active_page = "Table"

#Sidebar button 
if st.sidebar.button("Sales Table"):
    st.session_state.active_page = "Table"
if st.sidebar.button("Dashboard"):
    st.session_state.active_page = "Dashboard"

if 'show_uploader' not in st.session_state:
    st.session_state.show_uploader = False

if 'uploaded_df' not in st.session_state:
    st.session_state.uploaded_df = None

#Display content based on active page
if st.session_state.active_page == "Table":
    st.header("Sales Table")
    edited_df = st.data_editor(st.session_state.editable_df, num_rows= "dynamic")
    st.session_state.editable_df = edited_df
    st.info("Edit cells or add new rows to the table.")

    if st.button("Add Row"):
        st.session_state.editable_df.loc[len(st.session_state.editable_df)] = ["","","","","","","","","","",""]

    if st.button("Append CSV"):
        st.session_state.show_uploader = True

    if st.session_state.show_uploader:
        uploaded_file = st.file_uploader("Upload CSV to Append", type = ["csv"])
        if uploaded_file is not None:
            st.session_state.uploaded_df = pd.read_csv(uploaded_file)
            st.dataframe(st.session_state.uploaded_df)

            if st.session_state.uploaded_df is not None and st.button("Add to the table"):
                st.session_state.editable_df = pd.concat([st.session_state.editable_df, st.session_state.uploaded_df], ignore_index = True)
                st.success("data appended to the table")
                st.session_state.uploaded_df = None
                st.session_state.show_uploader = False

    if st.button("Save Changes"):
        st.session_state.original_df = st.session_state.editable_df.copy()
        st.session_state.original_df.to_csv("vgsales.csv", index = False)
        st.success("Changes Saved")
        