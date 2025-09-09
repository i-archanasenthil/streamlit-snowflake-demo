import pandas as pd
import streamlit as st
from snowflake.snowpark import Session

def build_column_config(dropdown_options, df):
    """
    Build Streamlit column_config for st.data_editor based on dropdown_options.

    Args:
        dropdown_options (dict) : Dictionary mapping column names -> list of options
        df (pd.DataFrame) : The dataframe whose columns we want to configure

    Returns:
        dict : A column_config dictionary in st.data_editor
    """
    column_config = {}
    for col_name, options in dropdown_options.items():
        if col_name in df.columns:
            column_config[col_name] = st.column_config.SelectboxColumn(
                col_name,
                options = options,
                help = f"Select a {col_name}"
            )
    return column_config


def get_dropdown_options(dropdown_df):
    """
    Gets the dropdown options for adding to the column config

    Args:
        dropdown_df (pd.DataFrame) : The dataframe from which we can retrive the unique values

    Returns:
        dict : A dropdown options as a dictionary to use in column config
    """
    dropdown_dict = {}
    for column in ["YEAR", "PRODUCT", "FORECAST", "METRIC"]:
        dropdown_dict[column] = dropdown_df[dropdown_df['COLUMN_NAME'] == column]['VALUE'].tolist()
    return dropdown_dict

def save_dropdown_options(df, session):
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

@st.dialog("Edit Dropdown Options ✏️")
def edit_dropdowns(dropdown_df, session):
    st.write("Update dropdown values here:")
    primary_keys = ["METRIC", "FORECAST", "PRODUCT", "YEAR"]

    updated_values = []

    for key in primary_keys:
        with st.expander(f"{key} Values"):
            key_df = dropdown_df[dropdown_df["COLUMN_NAME"] == key][["VALUE"]].copy()
            key_df.reset_index(drop=True, inplace = True)

            edited_key_df = st.data_editor(
                key_df,
                num_rows = "dynamic",
                use_container_width = True
            )

            edited_key_df["COLUMN_NAME"] = key
            updated_values.append(edited_key_df)

    updated_df = pd.concat(updated_values, ignore_index = True)[["COLUMN_NAME", "VALUE"]]
    updated_df = updated_df[updated_df["VALUE"].notnull() & (updated_df["VALUE"] != "")]

    if st.button("📤 Update Dropdowns"):
        if updated_df.empty:
            st.error("No values entered. Please fill atleast one value before saving")
        else:
            st.session_state.dropdown_df = updated_df
            save_dropdown_options(updated_df, session)
            st.success("Dropdown options updated successfully!")
            st.rerun()


@st.dialog("Add New Row ✚")
def add_new_dialog(df, dropdown_options):
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

        submitted = st.form_submit_button("✅ Add Row")

        if submitted:
            new_row = {
                "METRIC": metric,
                "FORECAST": forecast,
                "PRODUCT": product,
                "YEAR": year,
            }
            new_row.update(monthly_values)

            new_row_df = pd.DataFrame([new_row])

            temp_df= pd.concat([df, new_row_df], ignore_index= True)
            primary_keys = temp_df[["METRIC","FORECAST", "PRODUCT","YEAR"]]

            if primary_keys.duplicated(keep=False).iloc[-1]:
                st.error("Duplicate primary keys detected! The combination has to be unique. Please edit the existing cell.")
                existing_df = df
                duplicate_match = existing_df[
                    (existing_df["METRIC"] == metric) &
                    (existing_df["FORECAST"] == forecast) &
                    (existing_df["PRODUCT"] == product) &
                    (existing_df["YEAR"] == year)
                    ]
                if not duplicate_match.empty:
                    st.dataframe(duplicate_match)

            else:
                st.session_state.editable_df = temp_df
                st.success("Table updated successfully")
                st.rerun()


    
@st.dialog("Upload File 📎")
def select_tables_dialog(edit_df, session):
    uploaded_file = st.file_uploader("🗂️ Upload CSV file ", type = ["csv"])
    
    if uploaded_file is not None:
        df_csv = pd.read_csv(uploaded_file)
        st.write("Preview of the uploaded file:")
        st.dataframe(df_csv.head(20))

        if st.button(f"📤 Add to the table"):
            full_df = df_csv.copy()

            editable_cols = set(edit_df.columns)
            new_cols = set(full_df.columns)

            if editable_cols == new_cols:
                temp_df = pd.concat(
                    [edit_df, full_df], ignore_index = True
                )

                primary_keys = temp_df[["METRIC","FORECAST", "PRODUCT","YEAR"]]

                duplicates = primary_keys[primary_keys.duplicated(keep=False)]
                
                if not duplicates.empty:
                    st.error("Duplicate primary keys detected! The combination has to be unique. Please edit the existing cell.")
                    st.dataframe(temp_df.loc[duplicates.index]) 
                         
                else:
                    st.session_state.editable_df = temp_df
                    st.success("Table updated successfully")
                    st.rerun()

            else:
                st.error(
                    f"Cannot append {df_csv}.Column mismatch.\n\n"
                    f"Original Table Columns : {list(editable_cols)}. \n\n"
                    f"{df_csv} columsn: {list(new_cols)}"
                )

@st.dialog("Preview and Save Changes ✅")
def preview_changes_dialog(session):
    """
    Dialog to preview and save changes between editable_df and original_df
    """
    pk_cols = ["METRIC", "FORECAST", "PRODUCT", "YEAR"]

    temp_df = pd.DataFrame(st.session_state.editable_df)
    original_df = pd.DataFrame(st.session_state.original_df.to_pandas())

    for col_name in pk_cols:
        temp_df[col_name] = temp_df[col_name].astype(str).str.strip()
        original_df[col_name] = original_df[col_name].astype(str).str.strip()


    temp_df["_pk"] = temp_df[pk_cols].apply(lambda row: tuple(row), axis = 1)
    original_df["_pk"] = original_df[pk_cols].apply(lambda row: tuple(row), axis = 1)

    added_rows = temp_df[~temp_df["_pk"].isin(original_df["_pk"])].drop(columns= "_pk")
    removed_rows = original_df[~original_df["_pk"].isin(temp_df["_pk"])].drop(columns = "_pk")

    updated_rows = pd.DataFrame()
    original_rows = pd.DataFrame()
    common_pks = temp_df["_pk"].isin(original_df["_pk"])
    for pk in temp_df.loc[common_pks, "_pk"]:
        row_temp = temp_df[temp_df["_pk"] == pk].drop(columns="_pk").iloc[0]
        row_orig = original_df[original_df["_pk"] == pk].drop(columns = "_pk").iloc[0]

        if not row_temp.equals(row_orig):
            updated_rows = pd.concat([updated_rows, pd.DataFrame([row_temp])], ignore_index = False)
            original_rows = pd.concat([original_rows, pd.DataFrame([row_orig])],ignore_index = False)

    st.subheader("Changes Preview")

    if not added_rows.empty:
        st.success("Addd Rows:")
        st.dataframe(added_rows, use_container_width = True)

    if not removed_rows.empty:
        st.error("Removed Rows:")
        st.dataframe(removed_rows, use_container_width = True)

    if not updated_rows.empty:
        st.warning("Updated Rows:")
        st.dataframe(updated_rows, use_container_width = True)

    if added_rows.empty and removed_rows.empty and updated_rows.empty:
        st.info("No Changes Detected.")

    if st.button("💾 Save Changes to the Table"):
        try:
            df_to_save = st.session_state.editable_df
            df_to_save = df_to_save.dropna(subset=pk_cols)
            df_to_save.columns = [c.upper() for c in df_to_save.columns]

            month_cols = [c for c in df_to_save.columns if c not in pk_cols]

            session.create_dataframe(df_to_save).write.save_as_table(
                "TMP_SALES_STAGE", mode = "overwrite"
            )

            merge_condition = " AND ".join([f"target.{col} = source.{col}" for col in pk_cols])
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

            if not removed_rows.empty:
                for idx, row in removed_rows.iterrows():
                    condition = " AND ".join([f"{col} = '{row[col]}'" for col in pk_cols])
                    session.sql(f"DELETE FROM DEMO_STREAMLIT_APP.PUBLIC.SALES WHERE {condition}").collect()

            refreshed_df = session.table("DEMO_STREAMLIT_APP.PUBLIC.SALES")
            st.session_state.editable_df = refreshed_df.to_pandas()
            st.session_state.original_df = refreshed_df

            st.success("Changes Saved Successfully!")
            st.rerun()

        except Exception as e:
            st.error(f"Save Failed: {e}")



    

    

    
                    

            
            

        
    
        

    
            


 