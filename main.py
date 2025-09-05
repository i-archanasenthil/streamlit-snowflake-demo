# Import python packages


import streamlit as st
import pandas as pd
import plotly.express as px
from PIL import Image
import io
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col, sum as ssum, max as smax

    
session = get_active_session()

st.set_page_config(page_title= "Streamlit Snowflake Demo", layout = "wide", initial_sidebar_state = "expanded")
st.title("Streamlit Snowflake Demo")
st.write("This is a simple Streamlit app connected to Snowflake.")

logo = session.file.get_stream("@DEMO_STREAMLIT_APP.PUBLIC.ASSETS/l1.jpg", decompress=False).read()

st.sidebar.image(logo)
st.sidebar.title("Navigation")
st.sidebar.subheader("Select a page")

#initialize the session state for df to avoid losing data during re-runs
if 'original_df' not in st.session_state:
    st.session_state.original_df = session.table("DEMO_STREAMLIT_APP.PUBLIC.SALES")

if 'editable_df' not in st.session_state:
    st.session_state.editable_df = st.session_state.original_df.to_pandas().copy()

#Initializing session state to track actve page
if 'active_page' not in st.session_state:
    st.session_state.active_page = "Table"

if 'save_success' not in st.session_state:
    st.session_state.save_success = False

if "saved_df" not in st.session_state:
    st.session_state.saved_df = st.session_state.editable_df.copy()

if "changes_preview" not in st.session_state:
    st.session_state.changes_preview = None


#Sidebar button 
if st.sidebar.button("Sales Table"):
    st.session_state.active_page = "Table"
if st.sidebar.button("Dashboard"):
    st.session_state.active_page = "Dashboard"

st.sidebar.header("Filters")


selected_platform = "All"
selected_genre = "All"


if 'show_uploader' not in st.session_state:
    st.session_state.show_uploader = False

if 'uploaded_df' not in st.session_state:
    st.session_state.uploaded_df = None

def get_last10_sales(sp_df):
    last_year = sp_df.select(col("Year")).agg(smax(col("Year"))).collect()[0][0] 
    
    last10_df = (
        sp_df.filter(col("Year") >= last_year - 10)
        .group_by("Year")
        .agg(
            ssum(col("NA_Sales")).alias("NA_Sales"),
            ssum(col("EU_Sales")).alias("EU_Sales"),
            ssum(col("JP_Sales")).alias("JP_Sales"),
            ssum(col("Other_Sales")).alias("Other_Sales")
        )
        .sort(col("Year"))
        .to_pandas()
    )

    return last10_df

if "dropdown_df" not in st.session_state:
    st.session_state.dropdown_df = session.table("DEMO_STREAMLIT_APP.PUBLIC.DROPDOWN_OPTIONS").to_pandas()

@st.dialog("Edit Dropdown Options")
def edit_dropdowns():
    st.write("Update dropdown values here:")

    dropdown_df = session.table("DEMO_STREAMLIT_APP.PUBLIC.DROPDOWN_OPTIONS").to_pandas()
    primary_keys = ["METRIC", "FORECAST", "PRODUCT", "YEAR"]

    updated_values = []

    for key in primary_keys:
        with st.expander(f"{key} Values"):
            key_df = dropdown_df[dropdown_df["COLUMN_NAME"] == key][["VALUE"]].copy()
            key_df.reset_index(drop=True, inplace=True)

            edited_key_df = st.data_editor(
                key_df,
                num_rows = "dynamic",
                use_container_width = True
            )

            edited_key_df["COLUMN_NAME"] = key
            updated_values.append(edited_key_df)

    updated_df = pd.concat(updated_values, ignore_index= True)[["COLUMN_NAME", "VALUE"]]

    if st.button("Update Dropdowns"):
        st.session_state.dropdown_df = updated_df
        save_dropdown_options(updated_df)
        st.success("Dropdown options updated successfully!")
        st.rerun()

def save_dropdown_options(df):
    """
    Overwrites the DROPDOWN_OPTIONS with the values in the edited DataFrame 
    """
    df = df[['COLUMN_NAME', 'VALUE']].copy()

    records = [tuple(x) for x in df.to_numpy()]
    session.sql("TRUNCATE TABLE DEMO_STREAMLIT_APP.PUBLIC.DROPDOWN_OPTIONS").collect()

    for record in records:
        session.sql(f"""
            INSERT INTO DEMO_STREAMLIT_APP.PUBLIC.DROPDOWN_OPTIONS(COLUMN_NAME, VALUE)
            VALUES ('{record[0]}', '{record[1]}')
        """).collect()

    st.success("Dropdown table successfully updated!")

def get_dropdown_options():
    dropdown_df = session.table("DEMO_STREAMLIT_APP.PUBLIC.DROPDOWN_OPTIONS").to_pandas()
    dropdown_dict = {}
    for column in ["YEAR", "PRODUCT", "FORECAST", "METRIC"]:
        dropdown_dict[column] = dropdown_df[dropdown_df['COLUMN_NAME'] == column]['VALUE'].tolist()
    return dropdown_dict

dropdown_options = get_dropdown_options()

column_config = {}
for col_name, options in dropdown_options.items():
    if col_name in st.session_state.editable_df.columns:
        column_config[col_name] = st.column_config.SelectboxColumn(
            col_name,
            options = options,
            help= f"Select a {col_name}"
        )

@st.dialog("Add New Row")
def add_new_dialog():
    st.write("Enter values for a new rows:")

    with st.form("new_row_form"):
        metric = st.selectbox("Metric", dropdown_options.get("METRIC", []), key = "new_metric")
        forecast = st.selectbox("Forecast", dropdown_options.get("FORECAST", []), key = "new_forecast")
        product = st.selectbox("Product", dropdown_options.get("PRODUCT", []), key = "new_product")
        year = st.selectbox("Year", dropdown_options.get("YEAR", []), key = "new-year")

        st.subheader("Monthly values")
        monthly_values = {}
        cols = st.columns(3)
        months = ["JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"]

        for i, month in enumerate(months):
            with cols[i%3]:
                monthly_values[month] = st.number_input(month, min_value=0.0, value = None, step=0.1,  key=month)

        submitted = st.form_submit_button("Add Row")

        if submitted:
            new_row = {
                "METRIC": metric,
                "FORECAST": forecast,
                "PRODUCT": product,
                "YEAR": year,
            }
            new_row.update(monthly_values)

            new_row_df = pd.DataFrame([new_row])

            temp_df= pd.concat([st.session_state.temp_editable_df, new_row_df], ignore_index= True)
            primary_keys = temp_df[["METRIC","FORECAST", "PRODUCT","YEAR"]]
            
            if primary_keys.duplicated(keep=False).iloc[-1]:
                st.error("Duplicate primary keys detected! The combination has to be unique. Please edit the existing cell.")
                existing_df = st.session_state.editable_df
                duplicate_match = existing_df[
                    (existing_df["METRIC"] == metric) &
                    (existing_df["FORECAST"] == forecast) &
                    (existing_df["PRODUCT"] == product) &
                    (existing_df["YEAR"] == year)
                    ]
                if not duplicate_match.empty:
                    st.dataframe(duplicate_match)
        
            else:
                st.session_state.editable_df = temp_df.copy()
                st.success("Table updated successfully.")

#Display content based on active page
if st.session_state.active_page == "Table":
    col1, col2 = st.columns([3,1])
    col1.header("Sales Table")

    if col2.button("Manage Dropdown Options"):
        edit_dropdowns()

    if "original_df" not in st.session_state or not isinstance(st.session_state.original_df, pd.DataFrame):
        st.session_state.original_df = st.session_state.editable_df.copy()
    
    edited_df = st.data_editor(st.session_state.editable_df, column_config = column_config, num_rows= "dynamic")
    st.info("Edit cells or add new rows to the table.")
    
    primary_keys = edited_df[["METRIC", "FORECAST", "PRODUCT", "YEAR"]]
    duplicates = primary_keys[primary_keys.duplicated(keep=False)]

    if not duplicates.empty:
        st.error("Duplicate primary keys detected!  The combination has to be unique. Please edit the existing cell.")
        st.dataframe(duplicates, use_container_width=True)
    else:
        st.session_state.temp_editable_df = edited_df
        if st.session_state.save_success:
            st.success("Table updated successfully")
            st.session_state.save_changes = False
    
    if not st.session_state.temp_editable_df.equals(st.session_state.original_df):
        st.session_state.save_success = False

    c1,c2,c3 = st.columns(3)
    if c1.button("Add new row"):
        add_new_dialog()
        

    if c2.button("Append CSV"):
        st.session_state.show_uploader = True

    if st.session_state.show_uploader:
        uploaded_file = st.file_uploader("Upload CSV to Append", type = ["csv"])
        if uploaded_file is not None:
            st.session_state.uploaded_df = pd.read_csv(uploaded_file)
            st.dataframe(st.session_state.uploaded_df)

            if st.button("Add to the table"):
                if "uploaded_df" in st.session_state and st.session_state.uploaded_df is not None:

                    uploaded_cols = [col.strip().upper() for col in st.session_state.uploaded_df.columns]
                    st.session_state.uploaded_df.columns = uploaded_cols
                    
                    st.session_state.temp_editable_df = pd.concat([st.session_state.temp_editable_df, st.session_state.uploaded_df], ignore_index = True)
                    st.success("data appended to the table")
                    st.session_state.uploaded_df = None
                    st.session_state.show_uploader = False

    if c3.button("Preview Changes"):
        pk_cols = ["METRIC", "FORECAST", "PRODUCT", "YEAR"]
    
        temp_df = st.session_state.temp_editable_df.copy()
        original_df = st.session_state.original_df.copy()

        for col_name in pk_cols:
            temp_df[col_name] = temp_df[col_name].astype(str).str.strip()
            original_df[col_name] = original_df[col_name].astype(str).str.strip()

        temp_df["_pk"] = temp_df[pk_cols].apply(lambda row: tuple(row), axis=1)
        original_df["_pk"] = original_df[pk_cols].apply(lambda row: tuple(row), axis=1)
        
        added_rows = temp_df[~temp_df["_pk"].isin(original_df["_pk"])].drop(columns="_pk")
        removed_rows = original_df[~original_df["_pk"].isin(temp_df["_pk"])].drop(columns="_pk")
        common_pks = temp_df["_pk"].isin(original_df["_pk"])
        
        updated_rows = pd.DataFrame()

        for pk in temp_df.loc[common_pks,"_pk"]:
            row_temp = temp_df[temp_df["_pk"] == pk].drop(columns="_pk").iloc[0]
            row_orig = original_df[original_df["_pk"] == pk].drop(columns="_pk").iloc[0]
            if not row_temp.equals(row_orig):
                updated_rows = pd.concat([updated_rows, pd.DataFrame([row_temp])], ignore_index=True)

        st.subheader("Changes Preview")
        if not added_rows.empty:
            st.success("Added Rows:")
            st.dataframe(added_rows, use_container_width = True)

        if not removed_rows.empty:
            st.error("Removed Rows:")
            st.dataframe(removed_rows, use_container_width= True)

        if not updated_rows.empty:
            st.warning("Updated Rows")
            st.dataframe(updated_rows, use_container_width= True)

        if added_rows.empty and removed_rows.empty and updated_rows.empty:
            st.info("No Changes detected.")

        if st.button("Save Changes"):
            try:
                df_to_save = st.session_state.temp_editable_df.copy()
                df_to_save.columns = [c.upper() for c in df_to_save.columns]

                session.create_dataframe(df_to_save).write.save_as_table("TMP_SALES_STAGE", mode = "overwrite")

                merge_condition = " AND ".join([f"taget.{col} = source.{col}" for col in pk_cols])
                update_clause = ", ".join([f"{col} = source.{col}" for col in month_cols])
                insert_columns = ", ".join(df_to_save.columns)
                insert_values = ", ".join([f"source.{c}" for c in df_to_save.columns])

                session.sql(f"""
                    MERGE INTO DEMO_STREAMLIT_APP.PUBLIC.SALES AS target
                    USING TMP_SALES_STAGE AS source
                    ON {merge_condition}
                    WHEN MATCHED THEN
                        UPDATE SET {update_clause}
                    WHEN NOT MATCHED THEN
                        INSERT ({insert_columns})
                        VALUES ({insert_values})
                """).collect()
                
                refreshed_df = session.table("DEMO_STREAMLIT_APP.PUBLIC.SALES").to_pandas()
                st.session_state.editable_df = refreshed_df.copy()
                st.session_state.original_df = refreshed_df.copy()
            
                st.success("Changes saved")
                st.rerun()

            except Exception as e:
                st.error(f"Save Failed")

#Display for the dashboard
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
    
    
    col1, col2 = st.columns(2)
    col1.subheader("Total sales per year")
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
    col1.plotly_chart(fig, use_container_width= True)

    col2.subheader("Distribution of Sales (Last 10 Years)")
      
    
    last10_df = get_last10_sales(sp_df)
    
    melted_df = last10_df.melt(
        id_vars='YEAR',
        value_vars = ['NA_SALES','EU_SALES','JP_SALES','OTHER_SALES'],
        var_name= 'REGION',
        value_name= 'SALES'
    )
    fig2 = px.bar(
        melted_df,
        x = 'YEAR',
        y = 'SALES',
        color = 'REGION',
        barmode = 'group',
        text = 'SALES'
    )
    fig2.update_layout(
        yaxis_title = "Total Sales ($)",
        xaxis_title = "Year",
        uniformtext_minsize = 8,
        uniformtext_mode = 'hide'
    )
    col2.plotly_chart(fig2, use_container_width= True)