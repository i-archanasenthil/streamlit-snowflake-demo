import streamlit as st
import pandas as pd
import plotly.express as px
from PIL import Image
import io
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col, sum as ssum, max as smax

from helping_functions import build_column_config, get_dropdown_options, edit_dropdowns, add_new_dialog, select_tables_dialog, preview_changes_dialog

session = get_active_session()

st.set_page_config(page_title= "Streamlit Snowflake Demo ", layout = "wide", initial_sidebar_state = "expanded")
st.title("Streamlit Snowflake Demo")
st.write("This is a simple Streamlit app connected to Snowflake.")

logo = session.file.get_stream("@DEMO_STREAMLIT_APP.PUBLIC.ASSETS/l1.jpg", decompress=False).read()

st.sidebar.image(logo)
st.sidebar.title("Navigation")
st.sidebar.subheader("Select a page ↔️")

if 'original_df' not in st.session_state:
    st.session_state.original_df = session.table("DEMO_STREAMLIT_APP.PUBLIC.SALES")

if 'editable_df' not in st.session_state:
    st.session_state.editable_df = st.session_state.original_df.to_pandas().copy()

if 'active_page' not in st.session_state:
    st.session_state.active_page = "Table"

if "dropdown_df" not in st.session_state:
    st.session_state.dropdown_df = session.table("DEMO_STREAMLIT_APP.PUBLIC.DROPDOWN_OPTIONS").to_pandas()

    
#Sidebar button 
if st.sidebar.button("Sales Table 🗒️"):
    st.session_state.active_page = "Table"
if st.sidebar.button("Dashboard 📈"):
    st.session_state.active_page = "Dashboard"


st.sidebar.header("Filters 🔽")

selected_platform = "All"
selected_genre = "All"

if st.session_state.active_page == "Table":
    col1, col2 = st.columns([4,1])
    col1.header("Sales Table 📋")

    if col2.button("⚙️ Manage Dropdown Options"):
        edit_dropdowns(st.session_state.dropdown_df,session)

    dropdown_options = get_dropdown_options(st.session_state.dropdown_df)
    column_config = build_column_config(dropdown_options, st.session_state.editable_df)
    edited_df = st.data_editor(st.session_state.editable_df, column_config = column_config, num_rows= "dynamic")
    st.info("Edit cells or add new rows to the table.")

    primary_keys = edited_df[["METRIC", "FORECAST", "PRODUCT", "YEAR"]]
    duplicates = primary_keys[primary_keys.duplicated(keep=False)]

    if not duplicates.empty:
        st.error("Duplicate primary keys detected!  The combination has to be unique. Please edit the existing cell.")
        st.dataframe(duplicates, use_container_width=True)
    else:
        st.session_state.editable_df = edited_df

    c1,spacer,c2,spacer,c3 = st.columns([1,2,1,2,1])

    if c1.button("➕ Add new row"):
        add_new_dialog(st.session_state.editable_df, dropdown_options)

    if c2.button("🗂️ Append CSV File"):
        select_tables_dialog(st.session_state.editable_df, session)
                        
                        
    if c3.button("🔍 Preview Changes"):
        preview_changes_dialog(session)

if st.session_state.active_page == "Dashboard":
    st.header("Sales Analysis")

    #Snowpark DataFrame
    sp_df = session.table("DEMO_STREAMLIT_APP.PUBLIC.VGSALES")
    if selected_genre != "All":
        sp_df = sp_df.filter(col("Genre") == selected_genre)
    if selected_platform != "All":
        sp_df = sp_df.filter(col("Platform") == selected_platform)

    totals_df = sp_df.agg(
        ssum(col("NA_Sales")).alias("NA_Sales"),
        ssum(col("EU_Sales")).alias("EU_Sales"),
        ssum(col("JP_Sales")).alias("JP_Sales"),
        ssum(col("Global_Sales")).alias("Global_Sales")
    ).to_pandas()
    
    total_NA_sales = totals_df["NA_SALES"].iloc[0]
    total_EU_sales = totals_df["EU_SALES"].iloc[0]
    total_JP_sales = totals_df["JP_SALES"].iloc[0]
    total_global_sales = totals_df["GLOBAL_SALES"].iloc[0]
    
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Sales to Date", f"${total_global_sales:,.2f}")
    c2.metric("North America Sales to Date", f"${total_NA_sales:,.2f}")
    c3.metric("European Union Sales to Date", f"${total_EU_sales:,.2f}")
    c4.metric("Japan Sales to Date", f"${total_JP_sales:,.2f}")

    pdf = sp_df.to_pandas().copy()
    
    
    st.subheader("Total sales per year")
    pdf['YEAR'] = pd.to_numeric(pdf['YEAR'], errors = 'coerce')
    pdf = pdf.dropna(subset = ["YEAR"])
    pdf = pdf.sort_values('YEAR')

    yearly_sales = (
        sp_df.group_by("YEAR")
        .agg(ssum(col("Global_Sales")).alias("Global_Sales"))
        .to_pandas()
        .sort_values("YEAR")
    )
    
    fig = px.line(yearly_sales, x="YEAR", y="GLOBAL_SALES",markers= True)
    fig.update_layout(yaxis_title="Sales ($)", xaxis_title= "Year")
    st.plotly_chart(fig, use_container_width= True)

    
        

    
        