import streamlit as st
import pandas as pd
import plotly.express as px


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

st.sidebar.header("Filters")
if "Platform" in st.session_state.original_df.columns:
    selected_platform = st.sidebar.selectbox("Select Publisher", options= ["All"] + sorted(st.session_state.original_df["Platform"].dropna().unique().tolist()))
else:
    selected_platform = "All"
if "Genre" in st.session_state.original_df.columns:
    selected_genre = st.sidebar.selectbox("Select Genre", options = ["All"] + sorted(st.session_state.original_df["Genre"].dropna().unique().tolist()))
else:
    selected_genre = "All"


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

#Display for the dashboard
if st.session_state.active_page == "Dashboard":

    st.header("Sales Analysis")
    filtered_df = st.session_state.original_df.copy()
    

    if selected_genre != "All":
        filtered_df = filtered_df[filtered_df["Genre"] == selected_genre]
    if selected_platform != "All":
        filtered_df = filtered_df[filtered_df["Platform"] == selected_platform]
      
    total_NA_sales =  filtered_df["NA_Sales"].sum()
    total_EU_sales = filtered_df["EU_Sales"].sum()
    total_JP_sales = filtered_df["JP_Sales"].sum()
    total_global_sales = filtered_df["Global_Sales"].sum()
    
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Sales to Date", f"${total_global_sales:,.2f}")
    c2.metric("North America Sales to Date", f"${total_NA_sales:,.2f}")
    c3.metric("European Union Sales to Date", f"${total_EU_sales:,.2f}")
    c4.metric("Japan Sales to Date", f"${total_JP_sales:,.2f}")

    col1, col2 = st.columns(2)
    col1.subheader("Total sales per year")
    fig = px.line(filtered_df, x="Year", y="Global_Sales")
    fig.update_layout(yaxis_title="Sales ($)", xaxis_title= "Year")
    col1.plotly_chart(fig, use_container_width= True)

    col2.subheader("Distribution of Sales (Last 5 Years)")
    last_year = filtered_df["Year"]
    last5_df = filtered_df[filtered_df['Year'] >= last_year - 4]
    melted_df = last5_df.melt(
        id_vars='Year',
        value_vars = ['NA_Sales','EU_Sales', 'JP_Sales','Other_Sales'],
        var_name= 'Region',
        value_name= 'Sales'
    )

    fig2 = px.bar(
        melted_df,
        x = 'Year',
        y = 'Sales',
        color = 'Region',
        barmode = 'group',
        text = 'Sales'
    )

    fig2.update_layout(
        yaxis_title = "Total Sales ($)",
        xaxis_title = "Year",
        uniformtext_minsize = 8,
        uniformtext_mode = 'hide'
    )
    col2.plotly_chart(fig2, use_container_width= True)