import streamlit as st
import pandas as pd
import ast
import numpy as np
from sqlalchemy import create_engine

st.set_page_config(page_title="Course Eligibility and Recommendation System", layout="wide")
st.image("gust.png",width=400)
navigation = st.sidebar.radio("Go To", ["User Guide", "Course Eligibility and Recommendation System","Quick Check"])

# Add custom CSS to the Streamlit app
st.markdown(
    """
    <style>
    /* Ensure the font-family applies to all text elements */
    @font-face {
        font-family: 'Times New Roman';
        src: url('https://fonts.cdnfonts.com/s/15292/Times_New_Roman.woff') format('woff');
    }
    body, div, p, h1, h2, h3, h4, h5, h6, span, td, th, li, label, input, button, select, textarea, .stMarkdown, .stTextInput, .stTextArea, .stRadio, .stCheckbox, .stSelectbox, .stMultiSelect, .stButton, .stSlider, .stDataFrame, .stTable, .stExpander, .stTabs, .stAccordion, .stDownloadButton {
        font-family: 'Times New Roman', serif !important;
    }
    </style>
    """,
    unsafe_allow_html=True
)

def fetch_data_from_db(query):
    # Define connection parameters
    server = '192.168.8.11'
    database = 'GUST-DW-Staging'
    
    # Create the connection URL for SQLAlchemy
    connection_url = f'mssql+pyodbc://{server}/{database}?trusted_connection=yes&driver=ODBC+Driver+17+for+SQL+Server'
    
    # Create the SQLAlchemy engine
    engine = create_engine(connection_url,pool_size=1, max_overflow=0)
    conn = None  # Initialize the connection variable

    try:
        # Connect to the database
        conn = engine.connect()
        print("Connection successful")
        
        # Fetch data into a DataFrame
        df = pd.read_sql(query, conn)
        
        return df  # Return the DataFrame

    except Exception as e:
        print(f"Error: {e}")
        
    finally:
        # Close the connection if it was successfully created
        if conn:
            conn.close()
            engine.dispose()
            print("Connection closed")

def st_data_cleaning(st_enrollment_data, transfer_credit_data):
    ac_st_enrollment_data = st_enrollment_data
    tc_data = transfer_credit_data
    
    # Rename columns
    rename_columns_dict = {"EMPLID": "Student_ID", "STRM": "Semester", "Course": "Course_ID",
                           "Level": "Student_Level", "Plan": "Major",
                           "COURSE": "Course_ID", "chosen_semester": "Semester",
                          "STUDENT_ID" : "Student_ID","TRANSFER_TERM":"Semester",
                          "UNT_TRNSFR":"CREDITS","CUM_GPA":"GPA"}
    tc_data.rename(columns=rename_columns_dict, inplace=True)
    ac_st_enrollment_data.rename(columns=rename_columns_dict, inplace=True)
    ac_st_enrollment_data = ac_st_enrollment_data.fillna(0)
    ac_st_enrollment_data = ac_st_enrollment_data.astype(str)
    tc_data = tc_data.astype(str)
    
    # Strip whitespace from all columns
    for df in [tc_data, ac_st_enrollment_data]:
        for column in df.columns:
            df[column] = df[column].str.strip()
            
    # Convert relevant columns to appropriate types
    ac_st_enrollment_data["Student_Level"] = ac_st_enrollment_data["Student_Level"].str.extract('(\d+)', expand=False)
    ac_st_enrollment_data["Semester"] = ac_st_enrollment_data["Semester"].astype(int)
    ac_st_enrollment_data["Student_Level"] = ac_st_enrollment_data["Student_Level"].astype(int)
    ac_st_enrollment_data["CREDITS"] = ac_st_enrollment_data["CREDITS"].astype(float)
    ac_st_enrollment_data["Passed Credits"] = ac_st_enrollment_data["Passed Credits"].astype(float)
    ac_st_enrollment_data["GPA"] = ac_st_enrollment_data["GPA"].astype(float)
    ac_st_enrollment_data["MPA"] = ac_st_enrollment_data["MPA"].astype(float)
    tc_data["Semester"] = tc_data["Semester"].astype(int)
    tc_data["CREDITS"] = tc_data["CREDITS"].astype(float)
    
    ac_st_enrollment_data["CREDITS"] = ac_st_enrollment_data["CREDITS"].astype(int)
    ac_st_enrollment_data["Passed Credits"] = ac_st_enrollment_data["Passed Credits"].astype(int)
    tc_data["CREDITS"] = tc_data["CREDITS"].astype(int)
    
    max_semester_index = ac_st_enrollment_data.groupby('Student_ID')['Semester'].idxmax()
    latest_major_df = ac_st_enrollment_data.loc[max_semester_index, ['Student_ID', 'Semester',
                                                              "College", "Program", 'Major']]
    
    
    ac_st_enrollment_data = pd.merge(ac_st_enrollment_data.drop(columns=["College", "Program", 'Major']),
                          latest_major_df[["Student_ID", "College", "Program", 'Major']],
                          left_on="Student_ID", right_on="Student_ID", how='inner')

    tc_data = pd.merge(tc_data, latest_major_df[["Student_ID", "College", "Program", 'Major']],
                           left_on="Student_ID", right_on="Student_ID", how='inner')
    
    # Define a function to determine the chosen semester
    def determine_chosen_semester(row):
        if row['Semester'] == row['min']:
            return row['Semester']
        else:
            return row['min']
        
    semester_stats = ac_st_enrollment_data.groupby('Student_ID')['Semester'].agg(['min', 'max']).reset_index()
    tc_data = tc_data.merge(semester_stats, on='Student_ID', how='left')
    
    tc_data['chosen_semester'] = tc_data.apply(determine_chosen_semester, axis=1)
    tc_data = tc_data.drop(columns=["min", "max", "Semester", "SUBJECT","CATALOG_NBR"])
    tc_data.rename(columns=rename_columns_dict, inplace=True)
    grouped_data = ac_st_enrollment_data.groupby(['Student_ID', 'Semester']).agg({
        'Student_Level': 'first',
        'ADMIT_TERM': 'first',
        'Passed Credits': 'first',
        'Status': 'first',
        'GPA': 'first',
        'MPA': 'first'
    }).reset_index()
    
    tc_data = pd.merge(tc_data, grouped_data, on=['Student_ID', 'Semester'], how='inner')

    # Select and reorder columns
    tc_data = tc_data[['Student_ID', 'Semester', 'Status', 'Student_Level',
                       'Course_ID',"CREDITS", 'College', 'Program', 'Major', 'ADMIT_TERM',
                       'Passed Credits', 'GPA', 'MPA']]

    # Filter out unwanted records
    values_to_delete = ['Visit', 'Non-Degree', 'Undeclared - English',
                        'FA', 'F', 'I', 'S', 'NP', 'WA']

    ac_st_enrollment_data = ac_st_enrollment_data[~ac_st_enrollment_data["Major"].isin(values_to_delete)]
    ac_st_enrollment_data = ac_st_enrollment_data[['Student_ID', 'Semester','GRADE', 'Status', 'Student_Level',
                                     'Course_ID',"CREDITS", 'College', 'Program', 'Major', 'ADMIT_TERM',
                                     'Passed Credits', 'GPA', 'MPA']]
    
    # Combine data
    combined_data = pd.concat([ac_st_enrollment_data, tc_data], axis=0)
    combined_data["Major"] = combined_data['Major'].replace('Radio / TV', 'Digital Media Production')
    
    # Identify the latest semester for each student
    latest_semester = combined_data.groupby('Student_ID')['Semester'].max().reset_index()
    latest_semester.columns = ['Student_ID', 'Latest_Semester']

    # Merge this information back with the original dataframe
    combined_data = pd.merge(combined_data, latest_semester, on='Student_ID')

    # Filter rows for each student where the semester is their latest semester
    latest_semester_data = combined_data[combined_data['Semester'] == combined_data['Latest_Semester']]

    # Extract the passed credits for each student from the latest semester data
    latest_semester_passed_credits = latest_semester_data[['Student_ID', 'Passed Credits']].drop_duplicates()
    # Summing the CREDITS for the latest semester
    latest_semester_credits_sum = latest_semester_data.groupby('Student_ID')['CREDITS'].sum().reset_index()
    latest_semester_credits_sum.columns = ['Student_ID', 'Latest_Semester_Credits']


    previous_semesters_data = combined_data[combined_data['Semester'] != combined_data['Latest_Semester']]
    # Identify the latest semester for each student
    latest_semester_previous = previous_semesters_data.groupby('Student_ID')['Semester'].max().reset_index()
    latest_semester_previous.columns = ['Student_ID', 'Latest_Semester_Previous']

    # Merge this information back with the original dataframe
    combined_data = pd.merge(combined_data, latest_semester_previous, on='Student_ID',how = "left")
    combined_data = combined_data.fillna(0)
    combined_data["Latest_Semester_Previous"] = combined_data["Latest_Semester_Previous"].astype(int)

    # Filter rows for each student where the semester is their latest semester
    latest_semester_previous_data = combined_data[combined_data['Semester'] == combined_data['Latest_Semester_Previous']]

    # Extract the passed credits for each student from the latest semester data
    latest_semester_previous_passed_credits = latest_semester_previous_data[['Student_ID', 'Passed Credits']].drop_duplicates()

    latest_semester_passed_credits = latest_semester_passed_credits.rename(columns={"Passed Credits":"Passed_Credits_Latest"})
    latest_semester_previous_passed_credits = latest_semester_previous_passed_credits.rename(columns={"Passed Credits":"Passed_Credits_Previous"})

    total_pcr_previous_latest = pd.merge(latest_semester_passed_credits,latest_semester_previous_passed_credits, on='Student_ID',how = "left")
    total_pcr_previous_latest = total_pcr_previous_latest.fillna(0)
    total_pcr_previous_latest["Passed_Credits_Previous"] = total_pcr_previous_latest["Passed_Credits_Previous"].astype(int)

    total_pcr_previous_latest = pd.merge(latest_semester_credits_sum, total_pcr_previous_latest, on='Student_ID')
    total_pcr_previous_latest['Incoming_PCR'] = total_pcr_previous_latest.apply(
        lambda row: row['Latest_Semester_Credits'] + row['Passed_Credits_Latest'] 
        if row['Passed_Credits_Previous'] == row['Passed_Credits_Latest'] 
        else row['Passed_Credits_Latest'],
        axis=1)
    combined_data = pd.merge(combined_data, total_pcr_previous_latest[['Student_ID', 'Incoming_PCR']], on='Student_ID')
    combined_data['Incoming_PCR'] = combined_data.apply(
        lambda row: 0 if row['Semester'] != row['Latest_Semester'] else row['Incoming_PCR'],
        axis=1)
    combined_data = combined_data.drop(columns=["Latest_Semester_Previous","Latest_Semester"])
    
    return combined_data

# Eligibility Functions
def is_eligible(course, taken_courses, prerequisites):
    prereqs = prerequisites.get(course, [])
    return all(prereq in taken_courses for prereq in prereqs)


def is_eligible_special_acc(course, taken_courses, student_info,prerequisites,conditions):
    prereqs = prerequisites.get(course, [])
    condition = conditions.get(course, "")
    
    if condition == "OR":
        return any(prereq in taken_courses for prereq in prereqs)
    elif condition == "AND":
        return all(prereq in taken_courses for prereq in prereqs)
    elif condition == "AND_NOT_CS":
        return all(prereq in taken_courses for prereq in prereqs) and student_info['Major'] != "Computer Science"
    elif condition == "OR_AND_NOT_CS":
        return any(prereq in taken_courses for prereq in prereqs) and student_info['Major'] != "Computer Science"
    elif condition == "Credits":
        return (student_info['Passed Credits'] >= 81) or (int(student_info['Incoming_PCR']) >= 81)
    elif condition == "Credits_College":
        return (student_info['Passed Credits'] >= 81 and student_info['College'] == "CBA") or (int(student_info['Incoming_PCR']) >= 81 and student_info['College'] == "CBA") 
    elif condition == "AND_OR":
        return prereqs and prereqs[0] in taken_courses and any(prereq in taken_courses for prereq in prereqs[1:])
    elif condition == "AND_Senior":
        return all(prereq in taken_courses for prereq in prereqs) and student_info['Student_Level'] == 4
    elif condition == "Junior_AND_Major_ACC":
        return student_info['Student_Level'] == 3 and student_info['Major'] == "Accounting"
    elif condition == "AND_Major_ACC":
        return all(prereq in taken_courses for prereq in prereqs) and student_info['Major'] == "Accounting"
    elif condition == "Senior":
        return student_info['Student_Level'] == 4
    elif condition == "Any_Two":
        return sum(prereq in taken_courses for prereq in prereqs) >= 2
    elif condition == "AND_NOT_ENGLISH":
        return all(prereq in taken_courses for prereq in prereqs) and student_info['Program'] != "English"
    else:
        return False

def is_eligible_special_ib(course, taken_courses, student_info,prerequisites,conditions):
    prereqs = prerequisites.get(course, [])
    condition = conditions.get(course, "")
    
    if condition == "OR":
        return any(prereq in taken_courses for prereq in prereqs)
    elif condition == "AND_Major_MG_IB":
        return all(prereq in taken_courses for prereq in prereqs) and (student_info['Major'] == "International Business" or student_info['Major'] == "Mgmt & Organizational Behavior")
    elif condition == "AND_Major_MG_IB_MRKT":
        return all(prereq in taken_courses for prereq in prereqs) and (student_info['Major'] == "International Business" or student_info['Major'] == "Mgmt & Organizational Behavior" or student_info['Major'] == "Marketing")
    elif condition == "AND_Major_MG_IB_MRKT_MIS":
        return all(prereq in taken_courses for prereq in prereqs) and (student_info['Major'] == "International Business" or student_info['Major'] == "Mgmt & Organizational Behavior" or student_info['Major'] == "Marketing" or student_info['Major'] == "Management Information Systems")
    elif condition == "AND":
        return all(prereq in taken_courses for prereq in prereqs)
    elif condition == "AND_NOT_CS":
        return all(prereq in taken_courses for prereq in prereqs) and student_info['Major'] != "Computer Science"
    elif condition == "OR_AND_NOT_CS":
        return any(prereq in taken_courses for prereq in prereqs) and student_info['Major'] != "Computer Science"
    elif condition == "Credits":
        return (student_info['Passed Credits'] >= 81) or (int(student_info['Incoming_PCR']) >= 81)
    elif condition == "Credits_College":
        return (student_info['Passed Credits'] >= 81 and student_info['College'] == "CBA") or (int(student_info['Incoming_PCR']) >= 81 and student_info['College'] == "CBA")
    elif condition == "AND_OR":
        return prereqs and prereqs[0] in taken_courses and any(prereq in taken_courses for prereq in prereqs[1:])
    elif condition == "Senior_And_Major_MG_IB":
        return student_info['Student_Level'] == 4 and (student_info['Major'] == "International Business" or student_info['Major'] == "Mgmt & Organizational Behavior")
    elif condition == "Junior_And_Major_IB":
        return student_info['Student_Level'] == 3 and student_info['Major'] == "International Business"
    elif condition == "Senior":
        return student_info['Student_Level'] == 4
    elif condition == "Any_Two":
        return sum(prereq in taken_courses for prereq in prereqs) >= 2
    elif condition == "AND_NOT_ENGLISH":
        return all(prereq in taken_courses for prereq in prereqs) and student_info['Program'] != "English"
    else:
        return False
    
def is_eligible_special_mob(course, taken_courses, student_info,prerequisites,conditions):
    prereqs = prerequisites.get(course, [])
    condition = conditions.get(course, "")
    
    if condition == "OR":
        return any(prereq in taken_courses for prereq in prereqs)
    elif condition == "AND_Major_MG_IB":
        return all(prereq in taken_courses for prereq in prereqs) and (student_info['Major'] == "International Business" or student_info['Major'] == "Mgmt & Organizational Behavior")
    elif condition == "AND_Major_MG_IB_MRKT":
        return all(prereq in taken_courses for prereq in prereqs) and (student_info['Major'] == "International Business" or student_info['Major'] == "Mgmt & Organizational Behavior" or student_info['Major'] == "Marketing")
    elif condition == "AND_Major_MG_IB_MRKT_MIS":
        return all(prereq in taken_courses for prereq in prereqs) and (student_info['Major'] == "International Business" or student_info['Major'] == "Mgmt & Organizational Behavior" or student_info['Major'] == "Marketing" or student_info['Major'] == "Management Information Systems")
    elif condition == "AND":
        return all(prereq in taken_courses for prereq in prereqs)
    elif condition == "AND_NOT_CS":
        return all(prereq in taken_courses for prereq in prereqs) and student_info['Major'] != "Computer Science"
    elif condition == "OR_AND_NOT_CS":
        return any(prereq in taken_courses for prereq in prereqs) and student_info['Major'] != "Computer Science"
    elif condition == "Credits":
        return (student_info['Passed Credits'] >= 81) or (int(student_info['Incoming_PCR']) >= 81)
    elif condition == "Credits_College":
        return (student_info['Passed Credits'] >= 81 and student_info['College'] == "CBA") or (int(student_info['Incoming_PCR']) >= 81 and student_info['College'] == "CBA")
    elif condition == "AND_OR":
        return prereqs and prereqs[0] in taken_courses and any(prereq in taken_courses for prereq in prereqs[1:])
    elif condition == "Senior_And_Major_MG_IB":
        return student_info['Student_Level'] == 4 and (student_info['Major'] == "International Business" or student_info['Major'] == "Mgmt & Organizational Behavior")
    elif condition == "Junior_And_Major_MOB":
        return student_info['Student_Level'] == 3 and student_info['Major'] == "Mgmt & Organizational Behavior"
    elif condition == "Senior":
        return student_info['Student_Level'] == 4
    elif condition == "Any_Two":
        return sum(prereq in taken_courses for prereq in prereqs) >= 2
    elif condition == "AND_NOT_ENGLISH":
        return all(prereq in taken_courses for prereq in prereqs) and student_info['Program'] != "English"
    else:
        return False

def is_eligible_special_mis(course, taken_courses, student_info,prerequisites,conditions):
    prereqs = prerequisites.get(course, [])
    condition = conditions.get(course, "")

    if condition == "OR":
        return any(prereq in taken_courses for prereq in prereqs)
    elif condition == "AND":
        return all(prereq in taken_courses for prereq in prereqs)
    elif condition == "AND_Major_MG_IB_MRKT_MIS":
        return all(prereq in taken_courses for prereq in prereqs) and (student_info['Major'] == "International Business" or student_info['Major'] == "Mgmt & Organizational Behavior" or student_info['Major'] == "Marketing" or student_info['Major'] == "Management Information Systems")
    elif condition == "AND_NOT_CS":
        return all(prereq in taken_courses for prereq in prereqs) and student_info['Major'] != "Computer Science"
    elif condition == "OR_AND_NOT_CS":
        return any(prereq in taken_courses for prereq in prereqs) and student_info['Major'] != "Computer Science"
    elif condition == "Credits":
        return (student_info['Passed Credits'] >= 81) or (int(student_info['Incoming_PCR']) >= 81)
    elif condition == "Credits_College":
        return (student_info['Passed Credits'] >= 81 and student_info['College'] == "CBA") or (int(student_info['Incoming_PCR']) >= 81 and student_info['College'] == "CBA")
    elif condition == "AND_OR":
        return prereqs and prereqs[0] in taken_courses and any(prereq in taken_courses for prereq in prereqs[1:])
    elif condition == "Senior_AND_Major_MIS":
        return student_info['Student_Level'] == 4 and student_info['Major'] == "Management Information Systems" 
    elif condition == "Junior_AND_Major_MIS":
        return student_info['Student_Level'] == 3 and student_info['Major'] == "Management Information Systems"
    elif condition == "AND_Major_MIS":
        return all(prereq in taken_courses for prereq in prereqs) and student_info['Major'] == "Management Information Systems"
    elif condition == "Senior":
        return student_info['Student_Level'] == 4
    elif condition == "Any_Two":
        return sum(prereq in taken_courses for prereq in prereqs) >= 2
    elif condition == "AND_NOT_ENGLISH":
        return all(prereq in taken_courses for prereq in prereqs) and student_info['Program'] != "English"
    elif condition == "AND_Credits_MIS_CS":
        return all(prereq in taken_courses for prereq in prereqs) and student_info['Passed Credits'] < 45 and (student_info['Major'] == "Management Information Systems" or student_info['Major'] == "Computer Science")
    else:
        return False
    

def is_eligible_special_mrkt(course, taken_courses, student_info,prerequisites,conditions):
    prereqs = prerequisites.get(course, [])
    condition = conditions.get(course, "")

    if condition == "OR":
        return any(prereq in taken_courses for prereq in prereqs)
    elif condition == "AND_Major_MG_IB_MRKT":
        return all(prereq in taken_courses for prereq in prereqs) and (student_info['Major'] == "International Business" or student_info['Major'] == "Mgmt & Organizational Behavior" or student_info['Major'] == "Marketing")
    elif condition == "AND_Major_MG_IB_MRKT_MIS":
        return all(prereq in taken_courses for prereq in prereqs) and (student_info['Major'] == "International Business" or student_info['Major'] == "Mgmt & Organizational Behavior" or student_info['Major'] == "Marketing" or student_info['Major'] == "Management Information Systems")
    elif condition == "AND":
        return all(prereq in taken_courses for prereq in prereqs)
    elif condition == "AND_NOT_CS":
        return all(prereq in taken_courses for prereq in prereqs) and student_info['Major'] != "Computer Science"
    elif condition == "OR_AND_NOT_CS":
        return any(prereq in taken_courses for prereq in prereqs) and student_info['Major'] != "Computer Science"
    elif condition == "Credits":
        return (student_info['Passed Credits'] >= 81) or (int(student_info['Incoming_PCR']) >= 81)
    elif condition == "Credits_College":
        return (student_info['Passed Credits'] >= 81 and student_info['College'] == "CBA") or (int(student_info['Incoming_PCR']) >= 81 and student_info['College'] == "CBA")
    elif condition == "AND_OR":
        return prereqs and prereqs[0] in taken_courses and any(prereq in taken_courses for prereq in prereqs[1:])
    elif condition == "Senior_AND_Major_MRKT":
        return student_info['Student_Level'] == 4 and student_info['Major'] == "Marketing"
    elif condition == "Junior_AND_Major_MRKT":
        return student_info['Student_Level'] == 3 and student_info['Major'] == "Marketing"
    elif condition == "AND_Major_MRKT":
        return all(prereq in taken_courses for prereq in prereqs) and student_info['Major'] == "Marketing"
    elif condition == "Senior":
        return student_info['Student_Level'] == 4
    elif condition == "Any_Two":
        return sum(prereq in taken_courses for prereq in prereqs) >= 2
    elif condition == "AND_NOT_ENGLISH":
        return all(prereq in taken_courses for prereq in prereqs) and student_info['Program'] != "English"
    else:
        return False

def is_eligible_special_fin(course, taken_courses, student_info,prerequisites,conditions):
    prereqs = prerequisites.get(course, [])
    condition = conditions.get(course, "")
    
    if condition == "OR":
        return any(prereq in taken_courses for prereq in prereqs)
    elif condition == "AND":
        return all(prereq in taken_courses for prereq in prereqs)
    elif condition == "AND_NOT_CS":
        return all(prereq in taken_courses for prereq in prereqs) and student_info['Major'] != "Computer Science"
    elif condition == "OR_AND_NOT_CS":
        return any(prereq in taken_courses for prereq in prereqs) and student_info['Major'] != "Computer Science"
    elif condition == "Credits":
        return (student_info['Passed Credits'] >= 81) or (int(student_info['Incoming_PCR']) >= 81)
    elif condition == "Credits_College":
        return (student_info['Passed Credits'] >= 81 and student_info['College'] == "CBA") or (int(student_info['Incoming_PCR']) >= 81 and student_info['College'] == "CBA") 
    elif condition == "AND_OR":
        return prereqs and prereqs[0] in taken_courses and any(prereq in taken_courses for prereq in prereqs[1:])
    elif condition == "OR_AND":
        return all(prereq in taken_courses for prereq in prereqs[:2]) or any(prereq in taken_courses for prereq in prereqs[2:])
    elif condition == "Senior_AND_Major_FIN":
        return student_info['Student_Level'] == 4 and student_info['Major'] == "Finance"
    elif condition == "AND_Major_FIN":
        return all(prereq in taken_courses for prereq in prereqs) and student_info['Major'] == "Finance"
    elif condition == "AND_Senior":
        return all(prereq in taken_courses for prereq in prereqs) and student_info['Student_Level'] == 4
    elif condition == "Senior":
        return student_info['Student_Level'] == 4
    elif condition == "Any_Two":
        return sum(prereq in taken_courses for prereq in prereqs) >= 2
    elif condition == "AND_NOT_ENGLISH":
        return all(prereq in taken_courses for prereq in prereqs) and student_info['Program'] != "English"
    else:
        return False
    

def is_eligible_special_cs(course, taken_courses, student_info,prerequisites,conditions):
    prereqs = prerequisites.get(course, [])
    condition = conditions.get(course, "")
    
    if condition == "OR":
        return any(prereq in taken_courses for prereq in prereqs)
    elif condition == "AND":
        return all(prereq in taken_courses for prereq in prereqs)
    elif condition == "AND_OR":
        return prereqs and prereqs[0] in taken_courses and any(prereq in taken_courses for prereq in prereqs[1:])
    elif condition == "AND_College_OR":
        return all(prereq in taken_courses for prereq in prereqs) and (student_info['Major'] == "Computer Science" or student_info['College'] == "COE")
    elif condition == "OR_CS":
        return any(prereq in taken_courses for prereq in prereqs) and student_info['Major'] == "Computer Science"
    elif condition == "Junior_CS":
        return student_info['Student_Level'] == 3 and student_info['Major'] == "Computer Science"
    elif condition == "Senior_CS":
        return student_info['Student_Level'] == 4 and student_info['Major'] == "Computer Science"
    elif condition == "Any_Two":
        return sum(prereq in taken_courses for prereq in prereqs) >= 2
    elif condition == "OR_AND_College_OR":
        return any(prereq in taken_courses for prereq in prereqs) and (student_info['Major'] == "Computer Science" or student_info['College'] == "COE")
    elif condition == "AND_Credits_MIS_CS":
        return (all(prereq in taken_courses for prereq in prereqs) and student_info['Passed Credits'] < 45 and (student_info['Major'] == "Management Information Systems" or student_info['Major'] == "Computer Science")) or (all(prereq in taken_courses for prereq in prereqs) and int(student_info['Incoming_PCR']) < 45 and (student_info['Major'] == "Management Information Systems" or student_info['Major'] == "Computer Science"))
    else:
        return False

def is_eligible_special_dmp(course, taken_courses, student_info,prerequisites,conditions):
    prereqs = prerequisites.get(course, [])
    condition = conditions.get(course, "")
    if condition == "OR":
        return any(prereq in taken_courses for prereq in prereqs)
    elif condition == "AND":
        return all(prereq in taken_courses for prereq in prereqs)
    elif condition == "OR_MCOM":
        return any(prereq in taken_courses for prereq in prereqs) and student_info['Program'] == "Mass Communication"
    elif condition == "AND_MCOM":
        return all(prereq in taken_courses for prereq in prereqs) and student_info['Program'] == "Mass Communication"
    elif condition == "AND_Credits_MCOM":
        return (all(prereq in taken_courses for prereq in prereqs) and student_info['Passed Credits'] >= 54 and student_info['Program'] == "Mass Communication") or (all(prereq in taken_courses for prereq in prereqs) and int(student_info['Incoming_PCR']) >= 54 and student_info['Program'] == "Mass Communication")
    elif condition == "AND_Credits_MCOM_2":
        return (all(prereq in taken_courses for prereq in prereqs) and student_info['Passed Credits'] >= 60 and student_info['Program'] == "Mass Communication") or (all(prereq in taken_courses for prereq in prereqs) and int(student_info['Incoming_PCR']) >= 60 and student_info['Program'] == "Mass Communication")
    elif condition == "AND_OR_2":
        return prereqs and prereqs[0] in taken_courses and any(prereq in taken_courses for prereq in prereqs[1:3]) and any(prereq in taken_courses for prereq in prereqs[3:])
    elif condition == "AND_OR_PR":
        return (all(prereq in taken_courses for prereq in prereqs[:3]) and student_info['Major'] == "Public relations & Advertising") or all(prereq in taken_courses for prereq in prereqs[4:]) 
    elif condition == "AND_OR_Junior_Program":
        return prereqs and prereqs[0] in taken_courses and any(prereq in taken_courses for prereq in prereqs[1:]) and student_info['Student_Level'] == 3 and student_info['Program'] == "Mass Communication"
    elif condition == "OR_AND_Program_OR":
        return any(prereq in taken_courses for prereq in prereqs) and (student_info['Program'] == "Mass Communication" or student_info['Program'] == "English")
    elif condition == "AND_Junior":
        return student_info['Student_Level'] == 3 and all(prereq in taken_courses for prereq in prereqs)
    elif condition == "Junior_Program":
        return student_info['Student_Level'] == 3 and student_info['Program'] == "Mass Communication"
    elif condition == "Senior_MCOM":
        return student_info['Student_Level'] == 4 and student_info['Program'] == "Mass Communication"
    elif condition == "AND_Junior_Program":
        return student_info['Student_Level'] == 3 and all(prereq in taken_courses for prereq in prereqs) and student_info['Program'] == "Mass Communication"
    elif condition == "Any_Two":
        return sum(prereq in taken_courses for prereq in prereqs) >= 2
    elif condition == "AND_NOT_ENGLISH":
        return all(prereq in taken_courses for prereq in prereqs) and student_info['Program'] != "English"
    elif condition == "AND_NOT_CS":
        return all(prereq in taken_courses for prereq in prereqs) and student_info['Major'] != "Computer Science"
    elif condition == "OR_AND_NOT_CS":
        return any(prereq in taken_courses for prereq in prereqs) and student_info['Major'] != "Computer Science"
    else:
        return False

def is_eligible_special_eng_lin(course, taken_courses, student_info,prerequisites,conditions):
    prereqs = prerequisites.get(course, [])
    condition = conditions.get(course, "")
    
    if condition == "OR":
        return any(prereq in taken_courses for prereq in prereqs)
    elif condition == "AND":
        return all(prereq in taken_courses for prereq in prereqs)
    elif condition == "AND_UENG":
        return all(prereq in taken_courses for prereq in prereqs) and student_info['Program'] == "English"
    elif condition == "Senior_Lingusitics":
        return student_info['Student_Level'] == 4 and student_info['Major'] == "Eng- Linguistics - Translation"
    elif condition == "OR_AND_Program_OR":
        return any(prereq in taken_courses for prereq in prereqs) and (student_info['Program'] == "Mass Communication" or student_info['Program'] == "English")
    elif condition == "Senior_AND_UENG":
        return student_info['Student_Level'] == 4 and student_info['Program'] == "English"
    elif condition == "Any_Two":
        return sum(prereq in taken_courses for prereq in prereqs) >= 2
    elif condition == "OR_AND_NOT_CS":
        return any(prereq in taken_courses for prereq in prereqs) and student_info['Major'] != "Computer Science"
    elif condition == "AND_LIN_LIT":
        return all(prereq in taken_courses for prereq in prereqs) and (student_info['Major'] == "Eng- Linguistics - Translation" or student_info['Major'] == "English Literature")
    else:
        return False
    
def is_eligible_special_eng_edu(course, taken_courses, student_info,prerequisites,conditions):
    prereqs = prerequisites.get(course, [])
    condition = conditions.get(course, "")
    
    if condition == "OR":
        return any(prereq in taken_courses for prereq in prereqs)
    elif condition == "AND":
        return all(prereq in taken_courses for prereq in prereqs)
    elif condition == "AND_EDU":
        return all(prereq in taken_courses for prereq in prereqs) and student_info['Major'] == "English Education"
    elif condition == "AND_UENG":
        return all(prereq in taken_courses for prereq in prereqs) and student_info['Program'] == "English"
    elif condition == "OR_AND_Program_OR":
        return any(prereq in taken_courses for prereq in prereqs) and (student_info['Program'] == "Mass Communication" or student_info['Program'] == "English")
    elif condition == "Any_Three":
        return sum(prereq in taken_courses for prereq in prereqs) >= 3
    elif condition == "Any_Two":
        return sum(prereq in taken_courses for prereq in prereqs) >= 2
    elif condition == "OR_AND_NOT_CS":
        return any(prereq in taken_courses for prereq in prereqs) and student_info['Major'] != "Computer Science"
    elif condition == "Senior_Lingusitics":
        return student_info['Student_Level'] == 4 and student_info['Major'] == "Eng- Linguistics - Translation"
    elif condition == "Senior_AND_UENG":
        return student_info['Student_Level'] == 4 and student_info['Program'] == "English"
    else:
        return False
    
def is_eligible_special_eng_lit(course, taken_courses, student_info,prerequisites,conditions):
    prereqs = prerequisites.get(course, [])
    condition = conditions.get(course, "")
    
    if condition == "OR":
        return any(prereq in taken_courses for prereq in prereqs)
    elif condition == "AND_UENG":
        return all(prereq in taken_courses for prereq in prereqs) and student_info['Program'] == "English"
    elif condition == "OR_AND_Program_OR":
        return any(prereq in taken_courses for prereq in prereqs) and (student_info['Program'] == "Mass Communication" or student_info['Program'] == "English")
    elif condition == "Any_Two":
        return sum(prereq in taken_courses for prereq in prereqs) >= 2
    elif condition == "OR_AND_NOT_CS":
        return any(prereq in taken_courses for prereq in prereqs) and student_info['Major'] != "Computer Science"
    elif condition == "Senior_Lingusitics":
        return student_info['Student_Level'] == 4 and student_info['Major'] == "Eng- Linguistics - Translation"
    elif condition == "Senior_AND_UENG":
        return student_info['Student_Level'] == 4 and student_info['Program'] == "English"
    elif condition == "AND":
        return all(prereq in taken_courses for prereq in prereqs)
    elif condition == "AND_LIN_LIT":
        return all(prereq in taken_courses for prereq in prereqs) and (student_info['Major'] == "Eng- Linguistics - Translation" or student_info['Major'] == "English Literature")
    else:
        return False
    
def is_eligible_special_pr(course, taken_courses, student_info,prerequisites,conditions):
    prereqs = prerequisites.get(course, [])
    condition = conditions.get(course, "")
    
    if condition == "AND":
        return all(prereq in taken_courses for prereq in prereqs)
    elif condition == "OR":
        return any(prereq in taken_courses for prereq in prereqs)
    elif condition == "OR_MCOM":
        return any(prereq in taken_courses for prereq in prereqs) and student_info['Program'] == "Mass Communication"
    elif condition == "AND_MCOM":
        return all(prereq in taken_courses for prereq in prereqs) and student_info['Program'] == "Mass Communication"
    elif condition == "AND_Credits_MCOM":
        return (all(prereq in taken_courses for prereq in prereqs) and student_info['Passed Credits'] >= 54 and student_info['Program'] == "Mass Communication") or (all(prereq in taken_courses for prereq in prereqs) and int(student_info['Incoming_PCR']) >= 54 and student_info['Program'] == "Mass Communication")
    elif condition == "AND_Credits_MCOM_2":
        return (all(prereq in taken_courses for prereq in prereqs) and student_info['Passed Credits'] >= 60 and student_info['Program'] == "Mass Communication") or (all(prereq in taken_courses for prereq in prereqs) and int(student_info['Incoming_PCR']) >= 60 and student_info['Program'] == "Mass Communication")
    elif condition == "AND_OR_2":
        return prereqs and prereqs[0] in taken_courses and any(prereq in taken_courses for prereq in prereqs[1:3]) and any(prereq in taken_courses for prereq in prereqs[3:])
    elif condition == "AND_OR_PR":
        return (all(prereq in taken_courses for prereq in prereqs[:3]) and student_info['Major'] == "Public relations & Advertising") or all(prereq in taken_courses for prereq in prereqs[4:])
    elif condition == "AND_OR_Junior_Program":
        return prereqs and prereqs[0] in taken_courses and any(prereq in taken_courses for prereq in prereqs[1:]) and student_info['Student_Level'] == 3 and student_info['Program'] == "Mass Communication"
    elif condition == "OR_AND_Program_OR":
        return any(prereq in taken_courses for prereq in prereqs) and (student_info['Program'] == "Mass Communication" or student_info['Program'] == "English")
    elif condition == "Junior_Program":
        return student_info['Student_Level'] == 3 and student_info['Program'] == "Mass Communication"
    elif condition == "Senior_MCOM":
        return student_info['Student_Level'] == 4 and student_info['Program'] == "Mass Communication"
    elif condition == "AND_Junior":
        return student_info['Student_Level'] == 3 and all(prereq in taken_courses for prereq in prereqs)
    elif condition == "AND_Junior_Program":
        return student_info['Student_Level'] == 3 and all(prereq in taken_courses for prereq in prereqs) and student_info['Program'] == "Mass Communication"
    elif condition == "Any_Two":
        return sum(prereq in taken_courses for prereq in prereqs) >= 2
    elif condition == "OR_AND_NOT_CS":
        return any(prereq in taken_courses for prereq in prereqs) and student_info['Major'] != "Computer Science"
    elif condition == "AND_NOT_ENGLISH":
        return all(prereq in taken_courses for prereq in prereqs) and student_info['Program'] != "English"
    elif condition == "AND_NOT_CS":
        return all(prereq in taken_courses for prereq in prereqs) and student_info['Major'] != "Computer Science"
    else:
        return False
    
def is_eligible_special_vc(course, taken_courses, student_info,prerequisites,conditions):
    prereqs = prerequisites.get(course, [])
    condition = conditions.get(course, "")
    
    if condition == "OR":
        return any(prereq in taken_courses for prereq in prereqs)
    elif condition == "AND":
        return all(prereq in taken_courses for prereq in prereqs)
    elif condition == "AND_MCOM":
        return all(prereq in taken_courses for prereq in prereqs) and student_info['Program'] == "Mass Communication"
    elif condition == "OR_MCOM":
        return any(prereq in taken_courses for prereq in prereqs) and student_info['Program'] == "Mass Communication"
    elif condition == "AND_Credits_MCOM":
        return (all(prereq in taken_courses for prereq in prereqs) and student_info['Passed Credits'] >= 54 and student_info['Program'] == "Mass Communication") or (all(prereq in taken_courses for prereq in prereqs) and int(student_info['Incoming_PCR']) >= 54 and student_info['Program'] == "Mass Communication")
    elif condition == "AND_Credits_MCOM_2":
        return (all(prereq in taken_courses for prereq in prereqs) and student_info['Passed Credits'] >= 60 and student_info['Program'] == "Mass Communication") or (all(prereq in taken_courses for prereq in prereqs) and int(student_info['Incoming_PCR']) >= 60 and student_info['Program'] == "Mass Communication")
    elif condition == "AND_OR_Junior_Program":
        return prereqs and prereqs[0] in taken_courses and any(prereq in taken_courses for prereq in prereqs[1:]) and student_info['Student_Level'] == 3 and student_info['Program'] == "Mass Communication"
    elif condition == "AND_OR_2":
        return prereqs and prereqs[0] in taken_courses and any(prereq in taken_courses for prereq in prereqs[1:3]) and any(prereq in taken_courses for prereq in prereqs[3:])
    elif condition == "AND_OR_PR":
        return (all(prereq in taken_courses for prereq in prereqs[:3]) and student_info['Major'] == "Public relations & Advertising") or all(prereq in taken_courses for prereq in prereqs[4:])
    elif condition == "Senior_MCOM":
        return student_info['Student_Level'] == 4 and student_info['Program'] == "Mass Communication"
    elif condition == "OR_AND_Program_OR":
        return any(prereq in taken_courses for prereq in prereqs) and (student_info['Program'] == "Mass Communication" or student_info['Program'] == "English")
    elif condition == "Junior_Program":
        return student_info['Student_Level'] == 3 and student_info['Program'] == "Mass Communication"
    elif condition == "AND_Junior":
        return student_info['Student_Level'] == 3 and all(prereq in taken_courses for prereq in prereqs)
    elif condition == "AND_Junior_Program":
        return student_info['Student_Level'] == 3 and all(prereq in taken_courses for prereq in prereqs) and student_info['Program'] == "Mass Communication"
    elif condition == "Any_Two":
        return sum(prereq in taken_courses for prereq in prereqs) >= 2
    elif condition == "OR_AND_NOT_CS":
        return any(prereq in taken_courses for prereq in prereqs) and student_info['Major'] != "Computer Science"
    elif condition == "AND_NOT_ENGLISH":
        return all(prereq in taken_courses for prereq in prereqs) and student_info['Program'] != "English"
    elif condition == "AND_NOT_CS":
        return all(prereq in taken_courses for prereq in prereqs) and student_info['Major'] != "Computer Science"
    else:
        return False

def is_eligible_special_mgmt(course, taken_courses, student_info,prerequisites,conditions):
    prereqs = prerequisites.get(course, [])
    condition = conditions.get(course, "")
    
    if condition == "OR":
        return any(prereq in taken_courses for prereq in prereqs)
    elif condition == "AND":
        return all(prereq in taken_courses for prereq in prereqs)
    elif condition == "AND_College":
        return all(prereq in taken_courses for prereq in prereqs) and student_info['College'] == "COE"
    elif condition == "AND_College_OR":
        return all(prereq in taken_courses for prereq in prereqs) and (student_info['Major'] == "Computer Science" or student_info['College'] == "COE")
    elif condition == "AND_Senior":
        return student_info['Student_Level'] == 4 and all(prereq in taken_courses for prereq in prereqs)
    elif condition == "AND_OR_2":
        return all(prereq in taken_courses for prereq in prereqs[:2]) and any(prereq in taken_courses for prereq in prereqs[3:])
    elif condition == "Any_Two":
        return sum(prereq in taken_courses for prereq in prereqs) >= 2
    else:
        return False

def is_eligible_special_elec(course, taken_courses, student_info,prerequisites,conditions):
    prereqs = prerequisites.get(course, [])
    condition = conditions.get(course, "")
    
    if condition == "OR":
        return any(prereq in taken_courses for prereq in prereqs)
    elif condition == "AND":
        return all(prereq in taken_courses for prereq in prereqs)
    elif condition == "AND_College":
        return all(prereq in taken_courses for prereq in prereqs) and student_info['College'] == "COE"
    elif condition == "OR_AND_College_OR":
        return any(prereq in taken_courses for prereq in prereqs) and (student_info['Major'] == "Computer Science" or student_info['College'] == "COE")
    elif condition == "AND_College_OR":
        return all(prereq in taken_courses for prereq in prereqs) and (student_info['Major'] == "Computer Science" or student_info['College'] == "COE")
    elif condition == "AND_Senior":
        return student_info['Student_Level'] == 4 and all(prereq in taken_courses for prereq in prereqs)
    elif condition == "AND_OR_2":
        return all(prereq in taken_courses for prereq in prereqs[:2]) and any(prereq in taken_courses for prereq in prereqs[3:])
    elif condition == "AND_3_Courses":
        return all(prereq in taken_courses for prereq in prereqs[:3]) and sum(prereq in taken_courses for prereq in prereqs[3:]) >= 3
    elif condition == "Any_Two":
        return sum(prereq in taken_courses for prereq in prereqs) >= 2
    else:
        return False

def is_eligible_special_comp(course, taken_courses, student_info,prerequisites,conditions):
    prereqs = prerequisites.get(course, [])
    condition = conditions.get(course, "")
    
    if condition == "OR":
        return any(prereq in taken_courses for prereq in prereqs)
    elif condition == "AND":
        return all(prereq in taken_courses for prereq in prereqs)
    elif condition == "AND_College":
        return all(prereq in taken_courses for prereq in prereqs) and student_info['College'] == "COE"
    elif condition == "OR_AND_College_OR":
        return any(prereq in taken_courses for prereq in prereqs) and (student_info['Major'] == "Computer Science" or student_info['College'] == "COE")
    elif condition == "AND_OR":
        return prereqs and prereqs[0] in taken_courses and any(prereq in taken_courses for prereq in prereqs[1:])
    elif condition == "Junior_ECOM":
        return student_info['Student_Level'] == 3 and student_info['Program'] == "Computer Engineering"
    elif condition == "Senior_ECOM":
        return student_info['Student_Level'] == 4 and student_info['Program'] == "Computer Engineering"
    elif condition == "AND_OR_2":
        return all(prereq in taken_courses for prereq in prereqs[:2]) and any(prereq in taken_courses for prereq in prereqs[2:])
    elif condition == "AND_OR_3":
        return any(prereq in taken_courses for prereq in prereqs[:2]) and all(prereq in taken_courses for prereq in prereqs[2:])
    elif condition == "AND_College_OR":
        return all(prereq in taken_courses for prereq in prereqs) and (student_info['Major'] == "Computer Science" or student_info['College'] == "COE")
    elif condition == "AND_3_Courses":
        return all(prereq in taken_courses for prereq in prereqs[:3]) and sum(prereq in taken_courses for prereq in prereqs[3:]) >= 3
    elif condition == "Any_Two":
        return sum(prereq in taken_courses for prereq in prereqs) >= 2
    elif condition == "AND_Senior":
        return student_info['Student_Level'] == 4 and all(prereq in taken_courses for prereq in prereqs)
    else:
        return False
    
def is_eligible_special_acc_(course, taken_courses, student_info,prerequisites,conditions):
    prereqs = prerequisites.get(course, [])
    condition = conditions.get(course, "")
    
    if condition == "OR":
        return any(prereq in taken_courses for prereq in prereqs)
    elif condition == "AND_NOT_CS":
        return all(prereq in taken_courses for prereq in prereqs) and student_info['Major'] != "Computer Science"
    elif condition == "OR_AND_NOT_CS":
        return any(prereq in taken_courses for prereq in prereqs) and student_info['Major'] != "Computer Science"
    elif condition == "AND_Senior":
        return all(prereq in taken_courses for prereq in prereqs) and student_info['Student_Level'] == 4
    elif condition == "AND_Major_ACC":
        return all(prereq in taken_courses for prereq in prereqs) and student_info['Major'] == "Accounting"
    elif condition == "AND_NOT_ENGLISH":
        return all(prereq in taken_courses for prereq in prereqs) and student_info['Program'] != "English"
    else:
        return False
    
def is_eligible_special_ib_(course, taken_courses, student_info,prerequisites,conditions):
    prereqs = prerequisites.get(course, [])
    condition = conditions.get(course, "")
    
    if condition == "OR":
        return any(prereq in taken_courses for prereq in prereqs)
    elif condition == "AND_Major_MG_IB":
        return all(prereq in taken_courses for prereq in prereqs) and (student_info['Major'] == "International Business" or student_info['Major'] == "Mgmt & Organizational Behavior")
    elif condition == "AND_Major_MG_IB_MRKT":
        return all(prereq in taken_courses for prereq in prereqs) and (student_info['Major'] == "International Business" or student_info['Major'] == "Mgmt & Organizational Behavior" or student_info['Major'] == "Marketing")
    elif condition == "AND_Major_MG_IB_MRKT_MIS":
        return all(prereq in taken_courses for prereq in prereqs) and (student_info['Major'] == "International Business" or student_info['Major'] == "Mgmt & Organizational Behavior" or student_info['Major'] == "Marketing" or student_info['Major'] == "Management Information Systems")
    elif condition == "AND_NOT_CS":
        return all(prereq in taken_courses for prereq in prereqs) and student_info['Major'] != "Computer Science"
    elif condition == "OR_AND_NOT_CS":
        return any(prereq in taken_courses for prereq in prereqs) and student_info['Major'] != "Computer Science"
    elif condition == "AND_NOT_ENGLISH":
        return all(prereq in taken_courses for prereq in prereqs) and student_info['Program'] != "English"
    else:
        return False
    
def is_eligible_special_mob_(course, taken_courses, student_info,prerequisites,conditions):
    prereqs = prerequisites.get(course, [])
    condition = conditions.get(course, "")

    if condition == "OR":
        return any(prereq in taken_courses for prereq in prereqs)
    elif condition == "AND_Major_MG_IB":
        return all(prereq in taken_courses for prereq in prereqs) and (student_info['Major'] == "International Business" or student_info['Major'] == "Mgmt & Organizational Behavior")
    elif condition == "AND_Major_MG_IB_MRKT":
        return all(prereq in taken_courses for prereq in prereqs) and (student_info['Major'] == "International Business" or student_info['Major'] == "Mgmt & Organizational Behavior" or student_info['Major'] == "Marketing")
    elif condition == "AND_Major_MG_IB_MRKT_MIS":
        return all(prereq in taken_courses for prereq in prereqs) and (student_info['Major'] == "International Business" or student_info['Major'] == "Mgmt & Organizational Behavior" or student_info['Major'] == "Marketing" or student_info['Major'] == "Management Information Systems")
    elif condition == "AND_NOT_CS":
        return all(prereq in taken_courses for prereq in prereqs) and student_info['Major'] != "Computer Science"
    elif condition == "OR_AND_NOT_CS":
        return any(prereq in taken_courses for prereq in prereqs) and student_info['Major'] != "Computer Science"
    elif condition == "AND_NOT_ENGLISH":
        return all(prereq in taken_courses for prereq in prereqs) and student_info['Program'] != "English"
    else:
        return False
    
def is_eligible_special_mis_(course, taken_courses, student_info,prerequisites,conditions):
    prereqs = prerequisites.get(course, [])
    condition = conditions.get(course, "")

    if condition == "OR":
        return any(prereq in taken_courses for prereq in prereqs)
    elif condition == "AND_Major_MG_IB_MRKT_MIS":
        return all(prereq in taken_courses for prereq in prereqs) and (student_info['Major'] == "International Business" or student_info['Major'] == "Mgmt & Organizational Behavior" or student_info['Major'] == "Marketing" or student_info['Major'] == "Management Information Systems")
    elif condition == "AND_NOT_CS":
        return all(prereq in taken_courses for prereq in prereqs) and student_info['Major'] != "Computer Science"
    elif condition == "OR_AND_NOT_CS":
        return any(prereq in taken_courses for prereq in prereqs) and student_info['Major'] != "Computer Science"
    elif condition == "AND_Major_MIS":
        return all(prereq in taken_courses for prereq in prereqs) and student_info['Major'] == "Management Information Systems"
    elif condition == "AND_NOT_ENGLISH":
        return all(prereq in taken_courses for prereq in prereqs) and student_info['Program'] != "English"
    elif condition == "AND_Credits_MIS_CS":
        return all(prereq in taken_courses for prereq in prereqs) and student_info['Passed Credits'] < 45 and (student_info['Major'] == "Management Information Systems" or student_info['Major'] == "Computer Science")
    else:
        return False
    
def is_eligible_special_mrkt_(course, taken_courses, student_info,prerequisites,conditions):
    prereqs = prerequisites.get(course, [])
    condition = conditions.get(course, "")
    
    if condition == "OR":
        return any(prereq in taken_courses for prereq in prereqs)
    elif condition == "AND_Major_MG_IB_MRKT":
        return all(prereq in taken_courses for prereq in prereqs) and (student_info['Major'] == "International Business" or student_info['Major'] == "Mgmt & Organizational Behavior" or student_info['Major'] == "Marketing")
    elif condition == "AND_Major_MG_IB_MRKT_MIS":
        return all(prereq in taken_courses for prereq in prereqs) and (student_info['Major'] == "International Business" or student_info['Major'] == "Mgmt & Organizational Behavior" or student_info['Major'] == "Marketing" or student_info['Major'] == "Management Information Systems")
    elif condition == "AND_NOT_CS":
        return all(prereq in taken_courses for prereq in prereqs) and student_info['Major'] != "Computer Science"
    elif condition == "OR_AND_NOT_CS":
        return any(prereq in taken_courses for prereq in prereqs) and student_info['Major'] != "Computer Science"
    elif condition == "AND_Major_MRKT":
        return all(prereq in taken_courses for prereq in prereqs) and student_info['Major'] == "Marketing"
    elif condition == "AND_NOT_ENGLISH":
        return all(prereq in taken_courses for prereq in prereqs) and student_info['Program'] != "English"
    else:
        return False
    
def is_eligible_special_fin_(course, taken_courses, student_info,prerequisites,conditions):
    prereqs = prerequisites.get(course, [])
    condition = conditions.get(course, "")
    
    if condition == "OR":
        return any(prereq in taken_courses for prereq in prereqs)
    elif condition == "AND_NOT_CS":
        return all(prereq in taken_courses for prereq in prereqs) and student_info['Major'] != "Computer Science"
    elif condition == "OR_AND_NOT_CS":
        return any(prereq in taken_courses for prereq in prereqs) and student_info['Major'] != "Computer Science"
    elif condition == "AND_Major_FIN":
        return all(prereq in taken_courses for prereq in prereqs) and student_info['Major'] == "Finance"
    elif condition == "AND_Senior":
        return all(prereq in taken_courses for prereq in prereqs) and student_info['Student_Level'] == 4
    elif condition == "AND_NOT_ENGLISH":
        return all(prereq in taken_courses for prereq in prereqs) and student_info['Program'] != "English"
    else:
        return False

def is_eligible_special_cs_(course, taken_courses, student_info,prerequisites,conditions):
    prereqs = prerequisites.get(course, [])
    condition = conditions.get(course, "")
    
    if condition == "OR":
        return any(prereq in taken_courses for prereq in prereqs)
    elif condition == "AND_College_OR":
        return all(prereq in taken_courses for prereq in prereqs) and (student_info['Major'] == "Computer Science" or student_info['College'] == "COE")
    elif condition == "OR_CS":
        return any(prereq in taken_courses for prereq in prereqs) and student_info['Major'] == "Computer Science"
    elif condition == "OR_AND_College_OR":
        return any(prereq in taken_courses for prereq in prereqs) and (student_info['Major'] == "Computer Science" or student_info['College'] == "COE")
    elif condition == "AND_Credits_MIS_CS":
        return all(prereq in taken_courses for prereq in prereqs) and student_info['Passed Credits'] < 45 and (student_info['Major'] == "Management Information Systems" or student_info['Major'] == "Computer Science")
    else:
        return False
    
def is_eligible_special_dmp_(course, taken_courses, student_info,prerequisites,conditions):
    prereqs = prerequisites.get(course, [])
    condition = conditions.get(course, "")
    
    if condition == "OR":
        return any(prereq in taken_courses for prereq in prereqs)
    elif condition == "OR_MCOM":
        return any(prereq in taken_courses for prereq in prereqs) and student_info['Program'] == "Mass Communication"
    elif condition == "AND_MCOM":
        return all(prereq in taken_courses for prereq in prereqs) and student_info['Program'] == "Mass Communication"
    elif condition == "AND_Credits_MCOM":
        return all(prereq in taken_courses for prereq in prereqs) and student_info['Passed Credits'] >= 54 and student_info['Program'] == "Mass Communication"
    elif condition == "AND_Credits_MCOM_2":
        return all(prereq in taken_courses for prereq in prereqs) and student_info['Passed Credits'] >= 60 and student_info['Program'] == "Mass Communication"
    elif condition == "OR_AND_Program_OR":
        return any(prereq in taken_courses for prereq in prereqs) and (student_info['Program'] == "Mass Communication" or student_info['Program'] == "English")
    elif condition == "AND_Junior":
        return student_info['Student_Level'] == 3 and all(prereq in taken_courses for prereq in prereqs)
    elif condition == "OR_MCOM":
        return any(prereq in taken_courses for prereq in prereqs) and student_info['Program'] == "Mass Communication"
    elif condition == "AND_Junior_Program":
        return student_info['Student_Level'] == 3 and all(prereq in taken_courses for prereq in prereqs) and student_info['Program'] == "Mass Communication"
    elif condition == "AND_NOT_ENGLISH":
        return all(prereq in taken_courses for prereq in prereqs) and student_info['Program'] != "English"
    elif condition == "AND_NOT_CS":
        return all(prereq in taken_courses for prereq in prereqs) and student_info['Major'] != "Computer Science"
    elif condition == "OR_AND_NOT_CS":
        return any(prereq in taken_courses for prereq in prereqs) and student_info['Major'] != "Computer Science"
    else:
        return False
    
def is_eligible_special_eng_lin_(course, taken_courses, student_info,prerequisites,conditions):
    prereqs = prerequisites.get(course, [])
    condition = conditions.get(course, "")
    
    if condition == "OR":
        return any(prereq in taken_courses for prereq in prereqs)
    elif condition == "AND_UENG":
        return all(prereq in taken_courses for prereq in prereqs) and student_info['Program'] == "English"
    elif condition == "OR_AND_Program_OR":
        return any(prereq in taken_courses for prereq in prereqs) and (student_info['Program'] == "Mass Communication" or student_info['Program'] == "English")
    elif condition == "OR_AND_NOT_CS":
        return any(prereq in taken_courses for prereq in prereqs) and student_info['Major'] != "Computer Science"
    elif condition == "AND_LIN_LIT":
        return all(prereq in taken_courses for prereq in prereqs) and (student_info['Major'] == "Eng- Linguistics - Translation" or student_info['Major'] == "English Literature")
    else:
        return False
    
def is_eligible_special_eng_edu_(course, taken_courses, student_info,prerequisites,conditions):
    prereqs = prerequisites.get(course, [])
    condition = conditions.get(course, "")
    
    if condition == "OR":
        return any(prereq in taken_courses for prereq in prereqs)
    elif condition == "AND_EDU":
        return all(prereq in taken_courses for prereq in prereqs) and student_info['Major'] == "English Education"
    elif condition == "AND_UENG":
        return all(prereq in taken_courses for prereq in prereqs) and student_info['Program'] == "English"
    elif condition == "OR_AND_Program_OR":
        return any(prereq in taken_courses for prereq in prereqs) and (student_info['Program'] == "Mass Communication" or student_info['Program'] == "English")
    elif condition == "OR_AND_NOT_CS":
        return any(prereq in taken_courses for prereq in prereqs) and student_info['Major'] != "Computer Science"
    else:
        return False
    
def is_eligible_special_eng_lit_(course, taken_courses, student_info,prerequisites,conditions):
    prereqs = prerequisites.get(course, [])
    condition = conditions.get(course, "")
    
    if condition == "OR":
        return any(prereq in taken_courses for prereq in prereqs)
    elif condition == "AND_UENG":
        return all(prereq in taken_courses for prereq in prereqs) and student_info['Program'] == "English"
    elif condition == "OR_AND_Program_OR":
        return any(prereq in taken_courses for prereq in prereqs) and (student_info['Program'] == "Mass Communication" or student_info['Program'] == "English")
    elif condition == "OR_AND_NOT_CS":
        return any(prereq in taken_courses for prereq in prereqs) and student_info['Major'] != "Computer Science"
    elif condition == "AND_LIN_LIT":
        return all(prereq in taken_courses for prereq in prereqs) and (student_info['Major'] == "Eng- Linguistics - Translation" or student_info['Major'] == "English Literature")
    else:
        return False
    
def is_eligible_special_pr_(course, taken_courses, student_info,prerequisites,conditions):
    prereqs = prerequisites.get(course, [])
    condition = conditions.get(course, "")
    
    if condition == "OR":
        return any(prereq in taken_courses for prereq in prereqs)
    elif condition == "OR_MCOM":
        return any(prereq in taken_courses for prereq in prereqs) and student_info['Program'] == "Mass Communication"
    elif condition == "AND_MCOM":
        return all(prereq in taken_courses for prereq in prereqs) and student_info['Program'] == "Mass Communication"
    elif condition == "AND_Credits_MCOM":
        return all(prereq in taken_courses for prereq in prereqs) and student_info['Passed Credits'] >= 54 and student_info['Program'] == "Mass Communication"
    elif condition == "AND_Credits_MCOM_2":
        return all(prereq in taken_courses for prereq in prereqs) and student_info['Passed Credits'] >= 60 and student_info['Program'] == "Mass Communication"
    elif condition == "OR_AND_Program_OR":
        return any(prereq in taken_courses for prereq in prereqs) and (student_info['Program'] == "Mass Communication" or student_info['Program'] == "English")
    elif condition == "AND_Junior":
        return student_info['Student_Level'] == 3 and all(prereq in taken_courses for prereq in prereqs)
    elif condition == "AND_Junior_Program":
        return student_info['Student_Level'] == 3 and all(prereq in taken_courses for prereq in prereqs) and student_info['Program'] == "Mass Communication"
    elif condition == "OR_AND_NOT_CS":
        return any(prereq in taken_courses for prereq in prereqs) and student_info['Major'] != "Computer Science"
    elif condition == "AND_NOT_ENGLISH":
        return all(prereq in taken_courses for prereq in prereqs) and student_info['Program'] != "English"
    elif condition == "AND_NOT_CS":
        return all(prereq in taken_courses for prereq in prereqs) and student_info['Major'] != "Computer Science"
    else:
        return False
    
def is_eligible_special_vc_(course, taken_courses, student_info,prerequisites,conditions):
    prereqs = prerequisites.get(course, [])
    condition = conditions.get(course, "")
    
    if condition == "OR":
        return any(prereq in taken_courses for prereq in prereqs)
    elif condition == "AND_MCOM":
        return all(prereq in taken_courses for prereq in prereqs) and student_info['Program'] == "Mass Communication"
    elif condition == "OR_MCOM":
        return any(prereq in taken_courses for prereq in prereqs) and student_info['Program'] == "Mass Communication"
    elif condition == "AND_Credits_MCOM":
        return all(prereq in taken_courses for prereq in prereqs) and student_info['Passed Credits'] >= 54 and student_info['Program'] == "Mass Communication"
    elif condition == "AND_Credits_MCOM_2":
        return all(prereq in taken_courses for prereq in prereqs) and student_info['Passed Credits'] >= 60 and student_info['Program'] == "Mass Communication"
    elif condition == "OR_AND_Program_OR":
        return any(prereq in taken_courses for prereq in prereqs) and (student_info['Program'] == "Mass Communication" or student_info['Program'] == "English")
    elif condition == "AND_Junior":
        return student_info['Student_Level'] == 3 and all(prereq in taken_courses for prereq in prereqs)
    elif condition == "AND_Junior_Program":
        return student_info['Student_Level'] == 3 and all(prereq in taken_courses for prereq in prereqs) and student_info['Program'] == "Mass Communication"
    elif condition == "OR_AND_NOT_CS":
        return any(prereq in taken_courses for prereq in prereqs) and student_info['Major'] != "Computer Science"
    elif condition == "AND_NOT_ENGLISH":
        return all(prereq in taken_courses for prereq in prereqs) and student_info['Program'] != "English"
    elif condition == "AND_NOT_CS":
        return all(prereq in taken_courses for prereq in prereqs) and student_info['Major'] != "Computer Science"
    else:
        return False
    
def is_eligible_special_mgmt_(course, taken_courses, student_info,prerequisites,conditions):
    prereqs = prerequisites.get(course, [])
    condition = conditions.get(course, "")
    
    if condition == "OR":
        return any(prereq in taken_courses for prereq in prereqs)
    elif condition == "AND_College":
        return all(prereq in taken_courses for prereq in prereqs) and student_info['College'] == "COE"
    elif condition == "AND_College_OR":
        return all(prereq in taken_courses for prereq in prereqs) and (student_info['Major'] == "Computer Science" or student_info['College'] == "COE")
    else:
        return False
    
def is_eligible_special_elec_(course, taken_courses, student_info,prerequisites,conditions):
    prereqs = prerequisites.get(course, [])
    condition = conditions.get(course, "")
    
    if condition == "OR":
        return any(prereq in taken_courses for prereq in prereqs)
    elif condition == "AND_College":
        return all(prereq in taken_courses for prereq in prereqs) and student_info['College'] == "COE"
    elif condition == "OR_AND_College_OR":
        return any(prereq in taken_courses for prereq in prereqs) and (student_info['Major'] == "Computer Science" or student_info['College'] == "COE")
    elif condition == "AND_College_OR":
        return all(prereq in taken_courses for prereq in prereqs) and (student_info['Major'] == "Computer Science" or student_info['College'] == "COE")
    elif condition == "AND_Senior":
        return student_info['Student_Level'] == 4 and all(prereq in taken_courses for prereq in prereqs)
    else:
        return False
    
def is_eligible_special_comp_(course, taken_courses, student_info,prerequisites,conditions):
    prereqs = prerequisites.get(course, [])
    condition = conditions.get(course, "")
    
    if condition == "OR":
        return any(prereq in taken_courses for prereq in prereqs)
    elif condition == "AND_College":
        return all(prereq in taken_courses for prereq in prereqs) and student_info['College'] == "COE"
    elif condition == "OR_AND_College_OR":
        return any(prereq in taken_courses for prereq in prereqs) and (student_info['Major'] == "Computer Science" or student_info['College'] == "COE")
    elif condition == "AND_College_OR":
        return all(prereq in taken_courses for prereq in prereqs) and (student_info['Major'] == "Computer Science" or student_info['College'] == "COE")
    elif condition == "AND_Senior":
        return student_info['Student_Level'] == 4 and all(prereq in taken_courses for prereq in prereqs)
    else:
        return False
    
# Helper Functions from provided logic
def combine_eligible_courses(df1, df2):
    if df1.shape != df2.shape:
        raise ValueError("Dataframes do not have the same shape.")
    
    if list(df1.columns) != list(df2.columns):
        raise ValueError("Dataframes do not have the same headers.")
    
    combined_data = []
    for index, row in df1.iterrows():
        combined_row = row.copy()
        combined_courses = list(set(row['Eligible_Courses'] + df2.loc[index, 'Eligible_Courses']))
        combined_row['Eligible_Courses'] = combined_courses
        combined_data.append(combined_row)
    
    combined_df = pd.DataFrame(combined_data)
    
    return combined_df

def find_course_combinations(student_courses, requisites_data):
    combinations = []
    for _, row in requisites_data.iterrows():
        requisites_list = row['REQUISITES_LIST']
        course_id = row['Course_ID']
        if all(course in student_courses for course in requisites_list):
            combination = requisites_list + [course_id]
            combinations.append(combination)
    return combinations

def create_combined_courses(row, co):
    eligible_courses = row['Eligible_Courses']
    combined_courses = eligible_courses[:]
    co_requisite_courses = []
    combinations = find_course_combinations(eligible_courses, co)
    for combination in combinations:
        combined_courses += combination
        co_requisite_courses.append(combination)
    row['Co_Requisite_Courses'] = co_requisite_courses
    row['Eligible_Courses_CO'] = list(set(combined_courses))
    return row

def find_additional_eligibilities(courses, taken_courses, prerequisites):
    additional_eligibilities = set()
    for course in courses:
        hypothetical_courses = taken_courses.copy()
        hypothetical_courses.add(course)
        for c in prerequisites.keys():
            if is_eligible(c, hypothetical_courses, prerequisites) and c not in hypothetical_courses:
                additional_eligibilities.add(c)
    return list(additional_eligibilities)

def find_additional_eligibilities_special(courses, taken_courses, student_info, prerequisites_special, conditions, is_eligible_special):
    additional_eligibilities = set()
    hypothetical_courses = taken_courses.copy()
    for course in courses:
        hypothetical_courses.add(course)
        for c in prerequisites_special.keys():
            if is_eligible_special(c, hypothetical_courses, student_info, prerequisites_special, conditions) and c not in hypothetical_courses:
                additional_eligibilities.add(c)
    return list(additional_eligibilities)

# Function to remove matches
def remove_matches(row):
    eligible_courses = set(row["Eligible_Courses_CO"])
    course_id = set(row["Course_ID"])
    unmatched_courses = eligible_courses - course_id  # Set difference to find unmatched courses
    return list(unmatched_courses)  # Return as list for compatibility

# Function to process each row based on multiple course categories
def process_row(row):
    course_data = [
        (['MATH100','MATH131','MATH132','MATH231','MATH140','MATH221','MATH211',
          'MATH330','MATH111','MATH130','MATH121','MATH400','MATH331','MATH342',
          'MATH232','MATH122','MATH120'],
         ['MATH094','MATH095','MATH096','MATH098']),
        
        (['ENGL100','ENGL110','ENGL112'],
         ['ENGL097','ENGL098']),
         (['MATH096'],
         ['MATH095','MATH094']),
         (['MATH098'],
         ['MATH096','MATH095','MATH094'])
    ]

    for check_courses, remove_courses in course_data:
        if any(course in check_courses for course in row['Course_ID']):
            row['Eligible_Courses_CO'] = [course for course in row['Eligible_Courses_CO'] if course not in remove_courses]
    
    return row


def find_best_courses(group):
    sorted_courses = group.sort_values(by='Course_Score', ascending=False)
    return sorted_courses['Eligible_Courses_CO'].tolist()[:5]

def find_best_courses_v2(group):
    sorted_courses = group.sort_values(by='Course_Score', ascending=False)
    return sorted_courses['Eligible_Courses_CO'].tolist()[:5]

# Function to normalize the scores per student for each eligible course using Max Normalization
def normalize_by_student(group):
    # Normalize the Course Score by Max Normalization
    if group['Course_Score'].max() > 0:  # Avoid division by zero
        group['Normalized_Course_Score'] = group['Course_Score'] / group['Course_Score'].max()
    else:
        group['Normalized_Course_Score'] = 0  # Assign a default value if all scores are the same
    
    # Normalize the Remaining Weight Score by Max Normalization
    if group['Remaining_Courses_Weight_Score'].max() > 0:
        group['Normalized_Remaining_Courses_Weight'] = group['Remaining_Courses_Weight_Score'] / group['Remaining_Courses_Weight_Score'].max()
    else:
        group['Normalized_Remaining_Courses_Weight'] = 0  # Assign a default value if all scores are the same

    # Normalize the Course Level by Max Normalization and invert it
    if group['Course_Level'].max() > 0:
        group['Normalized_Course_Level'] = 1 - (group['Course_Level'] / group['Course_Level'].max())
    else:
        group['Normalized_Course_Level'] = 0  # Assign a default value if all levels are the same

    return group

def find_best_courses_cea_v2(group):
    sorted_courses = group.sort_values(by='Final_Score', ascending=False)
    return sorted_courses['Eligible_Courses_CO'].tolist()[:7]
def process_data_acc(st_hist_data,major_data, requirements_weights_path):
    
    values_to_delete = ['FA', 'F', 'I', 'S', 'NP', 'WA']
    failed_grades = ['F','FA','NP']
    failed_data = st_hist_data[st_hist_data["GRADE"].isin(failed_grades)]
    st_hist_data = st_hist_data[~st_hist_data["GRADE"].isin(values_to_delete)]
    
    # Filtering and Sorting Data
    failed_data = failed_data[failed_data['Major'] == 'Accounting']
    failed_data = failed_data.sort_values(by=['Student_ID', 'Semester'])

    grouped_data_failed = failed_data.groupby(['Student_ID'])['Course_ID'].apply(list).reset_index()
    
    # Filtering and Sorting Data
    acc_data = st_hist_data[st_hist_data['Major'] == 'Accounting']
    acc_data = acc_data.sort_values(by=['Student_ID', 'Semester'])

    major = major_data["All_Courses"]
    courses_co = major_data["CO_Courses"]
    
    major["AREA_OF_STUDY"] = major["AREA_OF_STUDY"].fillna("NA")
    # Dropping records where AREA_OF_STUDY is 'N' and COURSE_OF_STUDY is 'Z'
    major_filtered = major[~((major['AREA_OF_STUDY'] == 'NA') & (major['COURSE_OF_STUDY'] == 'Z'))]
    
    major_filtered = major_filtered.copy()
    # Apply replacements directly to the specific columns to avoid SettingWithCopyWarning
    major_filtered['AREA_OF_STUDY'] = major_filtered['AREA_OF_STUDY'].replace("NA","GE")
    major_filtered['COURSE_OF_STUDY'] = major_filtered['COURSE_OF_STUDY'].replace("N","E")
    
    # Defining the major lists
    cba_majors = ['ACCOUNTING', 'INTL BUSIN', 'MANAGEMENT', 'FINANCE', 'MIS', 'MARKETING2']
    # Filtering the DataFrame based on each major list
    df_cba = major_filtered[(major_filtered['Major'].isin(cba_majors)) & (major_filtered['COURSE_OF_STUDY'].isin(['R', 'RE']))]
    
    list_conditions = ['-', 'ONE_COURSE']

    cba_list = df_cba[df_cba['Condition'].isin(list_conditions)]
    cba_special_cases = df_cba[~df_cba['Condition'].isin(list_conditions)]
    cba_co = courses_co[courses_co['Major'].isin(cba_majors)]
    
    acc_list = cba_list[cba_list["Major"] == "ACCOUNTING"]
    acc_special_cases = cba_special_cases[cba_special_cases["Major"] == "ACCOUNTING"]
    acc_co = cba_co[cba_co["Major"] == "ACCOUNTING"]

    # Process 'REQUISITES_LIST'
    acc_co = acc_co.copy()
    acc_co.loc[:, 'REQUISITES_LIST'] = acc_co['REQUISITES_LIST'].apply(ast.literal_eval)

    # CBA Courses
    cba_courses = major_filtered[major_filtered['Major'].isin(cba_majors)]
    courses_acc = cba_courses[cba_courses["Major"] == "ACCOUNTING"]
    
    grouped_data_acc = acc_data.groupby(['Student_ID'])['Course_ID'].apply(list).reset_index()

    # Merge dataframes
    merged_df = grouped_data_failed.merge(grouped_data_acc, on=['Student_ID'], how='outer', suffixes=('_failed', '_all'))
    # Replace NaN with empty lists to avoid errors
    merged_df['Course_ID_all'] = merged_df['Course_ID_all'].apply(lambda x: x if isinstance(x, list) else [])
    merged_df['Course_ID_failed'] = merged_df['Course_ID_failed'].apply(lambda x: x if isinstance(x, list) else [])

    merged_df['Failed_Courses'] = merged_df.apply(
        lambda row: list(set(row['Course_ID_failed']) - set(row['Course_ID_all'])),
        axis=1)
    # Keep only relevant columns
    merged_df = merged_df[['Student_ID', 'Failed_Courses']]

    # Extract Accounting specific requirements and weights from respective DataFrames
    requirements_df = pd.read_excel(requirements_weights_path,sheet_name="requirements")
    weights_df = pd.read_excel(requirements_weights_path,sheet_name="weights")
    requirements_acc = requirements_df[requirements_df["Major"] == "Accounting"]
    requirements_acc_ = requirements_acc.pivot_table(index="Major",columns="AREA_OF_STUDY",values ='Required_Courses' ,aggfunc='sum',fill_value=0).reset_index()
    weights_acc = weights_df[weights_df["Major"] == "Accounting"]

    student_courses = acc_data[["Student_ID", "Course_ID"]]

    # Map AREA_OF_STUDY and COURSE_OF_STUDY to acc_data
    student_courses = student_courses.merge(courses_acc[['Course_ID', 'AREA_OF_STUDY', 'COURSE_OF_STUDY', "Course_Level"]],
                                            on='Course_ID', how='left').drop_duplicates()

    # Create summary DataFrames for taken courses
    student_progress = student_courses.groupby(['Student_ID', 'AREA_OF_STUDY']).size().reset_index(name='Total_Taken_Courses')
    student_progress = student_progress.merge(requirements_acc, on='AREA_OF_STUDY', how='left')
    student_progress["Remaining_Courses"] = student_progress["Required_Courses"] - student_progress["Total_Taken_Courses"]
    student_progress["Remaining_Courses"] = student_progress["Remaining_Courses"].apply(lambda x: max(x, 0))

    free_elective_taken_counts = student_courses[(student_courses['AREA_OF_STUDY'] == "GE") & (student_courses['COURSE_OF_STUDY'] == "E")].groupby('Student_ID').size().reset_index(name='Total_Free_Electives_Taken')

    # Update progress by including the free elective data
    student_progress["Student_Progress"] = (student_progress["Total_Taken_Courses"] / student_progress["Required_Courses"]) * 100
    student_progress["Student_Progress"].replace([np.inf, -np.inf], 100, inplace=True)

    summary_area_of_study_taken = student_progress.pivot_table(index="Student_ID", columns="AREA_OF_STUDY", values="Total_Taken_Courses", fill_value=0)
    summary_area_of_study_taken = summary_area_of_study_taken.merge(free_elective_taken_counts, on="Student_ID", how="left").fillna(0).rename(columns={"Total_Free_Electives_Taken": "FE"})

    # Create a copy of summary_area_of_study_taken to work on remaining courses calculation
    remaining_courses_df = summary_area_of_study_taken.copy()

    # Loop through each AREA_OF_STUDY and calculate remaining courses by subtracting from the requirements
    for column in remaining_courses_df.columns:
        if column in requirements_acc['AREA_OF_STUDY'].values:
            required_courses = requirements_acc.loc[requirements_acc['AREA_OF_STUDY'] == column, 'Required_Courses'].values[0]
            remaining_courses_df[column] = required_courses - remaining_courses_df[column]
            remaining_courses_df[column] = remaining_courses_df[column].clip(lower=0)

    # Calculate weighted remaining courses
    weighted_remaining_courses_df = remaining_courses_df.copy()
    for column in weighted_remaining_courses_df.columns:
        if column in weights_acc['AREA_OF_STUDY'].values:
            weight_value = weights_acc.loc[weights_acc['AREA_OF_STUDY'] == column, 'Weight'].values[0]
            weighted_remaining_courses_df[column] = weighted_remaining_courses_df[column] * weight_value

    # Prepare weighted remaining courses for merge
    weighted_remaining_courses_df = weighted_remaining_courses_df.reset_index().melt(id_vars=['Student_ID'],
                                                                                      var_name='AREA_OF_STUDY',
                                                                                      value_name='Remaining_Courses_Weight_Score')
    weighted_remaining_courses_df = weighted_remaining_courses_df[weighted_remaining_courses_df["AREA_OF_STUDY"] != "index"]

    # Eligibility Calculation for Standard and Special Cases
    prerequisites_acc = acc_list.set_index('Course_ID')['REQUISITES_LIST'].apply(eval).to_dict()
    prerequisites_special_acc = acc_special_cases.set_index('Course_ID')['REQUISITES_LIST'].apply(eval).to_dict()
    conditions_acc = acc_special_cases.set_index('Course_ID')['Condition'].to_dict()

    final_results_acc = []  # Standard eligibility results
    final_results_special_acc = []  # Special eligibility results

    for student_id, group in acc_data.groupby('Student_ID'):
        cumulative_courses = set()
        for semester, semester_group in group.groupby('Semester'):
            taken_courses = set(semester_group['Course_ID'].tolist())
            cumulative_courses.update(taken_courses)

            # Determine Standard Eligible Courses
            student_info = semester_group.iloc[0].to_dict()
            eligible_courses = {course for course in prerequisites_acc.keys() if all(req in cumulative_courses for req in prerequisites_acc[course])}
            final_results_acc.append({
                'Student_ID': student_id,
                'Semester': semester,
                'Major': student_info['Major'],
                'College': student_info['College'],
                'Program': student_info['Program'],
                'Passed Credits': student_info['Passed Credits'],
                'Student_Level': student_info['Student_Level'],
                'Eligible_Courses': list(eligible_courses - cumulative_courses)
            })

            # Determine Special Eligible Courses
            special_eligible_courses = {
                course for course in prerequisites_special_acc.keys()
                if is_eligible_special_acc(course, cumulative_courses, student_info, prerequisites_special_acc, conditions_acc)
            }
            final_results_special_acc.append({
                'Student_ID': student_id,
                'Semester': semester,
                'Major': student_info['Major'],
                'College': student_info['College'],
                'Program': student_info['Program'],
                'Passed Credits': student_info['Passed Credits'],
                'Student_Level': student_info['Student_Level'],
                'Eligible_Courses': list(special_eligible_courses - cumulative_courses)
            })

    # Convert Results to DataFrames
    final_results_df_acc = pd.DataFrame(final_results_acc)
    final_results_special_df_acc = pd.DataFrame(final_results_special_acc)
    
    # Combine Eligible Courses from Both DataFrames
    combined_acc_list = combine_eligible_courses(final_results_df_acc, final_results_special_df_acc)
    # Find Course Combinations for Co-requisites
    combined_acc_list = combined_acc_list.apply(create_combined_courses, axis=1, co=acc_co)
    latest_eligible_courses = combined_acc_list.sort_values(by='Semester', ascending=False)
    latest_eligible_courses = latest_eligible_courses.groupby('Student_ID').first().reset_index()
    latest_eligible_courses = latest_eligible_courses.merge(grouped_data_acc,on = "Student_ID",how = "inner")
    latest_eligible_courses["Eligible_Courses_CO"] = latest_eligible_courses.apply(remove_matches, axis=1)
    latest_eligible_courses = latest_eligible_courses.apply(process_row, axis=1)
    latest_eligible_courses.drop(columns=["Course_ID"], inplace=True)

    latest_eligible_courses = latest_eligible_courses.merge(merged_df, on='Student_ID', how='outer')
    latest_eligible_courses['Failed_Courses'] = latest_eligible_courses['Failed_Courses'].apply(lambda x: x if isinstance(x, list) else [])
    latest_eligible_courses['Eligible_Courses_CO'] = latest_eligible_courses['Eligible_Courses_CO'].apply(lambda x: x if isinstance(x, list) else [])
    latest_eligible_courses['Eligible_Courses_CO'] = latest_eligible_courses.apply(
        lambda row: list(set(row['Eligible_Courses_CO']) | (set(row['Failed_Courses']) - set(row['Eligible_Courses_CO']))),axis=1)
    latest_eligible_courses = latest_eligible_courses.drop(columns=['Failed_Courses'])

    latest_info_failed = failed_data.loc[failed_data.groupby("Student_ID")["Semester"].idxmax()]
    missing_semester_df = latest_eligible_courses[latest_eligible_courses['Semester'].isna()]
    latest_eligible_courses.dropna(inplace=True)
    columns_to_fill = ['Semester', 'Major', 'College', 'Program', 'Passed Credits', 'Student_Level']

    for col in columns_to_fill:
        missing_semester_df.loc[missing_semester_df[col].isna(), col] = missing_semester_df.loc[
            missing_semester_df[col].isna(), 'Student_ID'
        ].map(latest_info_failed.set_index('Student_ID')[col])

    columns_to_convert = ['Semester', 'Student_Level', 'Passed Credits']
    for col in columns_to_convert:
        latest_eligible_courses.loc[:, col] = pd.to_numeric(latest_eligible_courses[col], errors='coerce').astype('Int64')
        
    latest_eligible_courses = pd.concat([latest_eligible_courses, missing_semester_df], ignore_index=True)


    max_semester_index = acc_data.groupby('Student_ID')['Semester'].idxmax()
    max_semester_data = acc_data.loc[max_semester_index, ['Student_ID', 'Semester']]

    last_semester_courses = pd.merge(max_semester_data, acc_data, on=['Student_ID', 'Semester'])
    eng097_fpu_students = last_semester_courses[last_semester_courses['Course_ID'] == 'ENGL097']
    # Target course list
    target_courses = ['ENGL098', 'MATH094', 'MATH095', 'MATH096', 'MATH098', 'MATH100', 'MATH111', 'MATH120', 'MATH121', 'MATH131', 'MATH140']

    eng097_fpu_students_eligible = latest_eligible_courses[latest_eligible_courses['Student_ID']
                                                       .isin(eng097_fpu_students['Student_ID'])].copy()
    eng097_fpu_students_eligible.loc[:, 'Eligible_Courses_CO'] = eng097_fpu_students_eligible['Eligible_Courses_CO'].apply(
    lambda courses: [course for course in courses if course in target_courses])

    latest_eligible_courses = latest_eligible_courses.merge(
    eng097_fpu_students_eligible[['Student_ID', 'Eligible_Courses_CO']],  # Relevant columns from filtered_students
    on='Student_ID',
    how='left',  # Keep all rows in students_df
    suffixes=('', '_updated'))  # Suffix to differentiate new column)

    latest_eligible_courses['Eligible_Courses_CO'] = latest_eligible_courses['Eligible_Courses_CO_updated'].combine_first(latest_eligible_courses['Eligible_Courses_CO'])
    latest_eligible_courses = latest_eligible_courses.drop(columns=['Eligible_Courses_CO_updated'])
    latest_eligible_courses = latest_eligible_courses.merge(grouped_data_acc,on = "Student_ID",how = "outer")
    latest_eligible_courses['Course_ID'] = latest_eligible_courses['Course_ID'].apply(lambda x: x if isinstance(x, list) else [])
    latest_eligible_courses = latest_eligible_courses.apply(process_row, axis=1)
    latest_eligible_courses.drop(columns=["Course_ID"], inplace=True)

    # Exploding DataFrame and mapping course details
    eligible_courses_comprehensive_data = latest_eligible_courses.explode("Eligible_Courses_CO")
    eligible_courses_comprehensive_data = eligible_courses_comprehensive_data.merge(courses_acc[['Course_ID', 'AREA_OF_STUDY', 'COURSE_OF_STUDY', 'Course_Level']],
                                                                                    left_on='Eligible_Courses_CO', right_on='Course_ID', how='left').drop(columns="Course_ID")
    eligible_courses_comprehensive_data['Eligible_Courses_CO'] = eligible_courses_comprehensive_data['Eligible_Courses_CO'].apply(lambda x: x if isinstance(x, list) else ([] if pd.isna(x) else [x]))

    # Find Additional Eligibilities
    eligible_courses_comprehensive_data['Future_Eligible_Courses_List'] = eligible_courses_comprehensive_data.apply(lambda row: find_additional_eligibilities(row['Eligible_Courses_CO'], set(row['Eligible_Courses_CO']), prerequisites_acc), axis=1)
    eligible_courses_per_student = eligible_courses_comprehensive_data.groupby('Student_ID')['Eligible_Courses_CO'].agg(lambda x: list(set([item for sublist in x for item in sublist if isinstance(sublist, list)]))).reset_index()

    # Merge aggregated list back to the comprehensive DataFrame
    eligible_courses_comprehensive_data = eligible_courses_comprehensive_data.merge(eligible_courses_per_student.rename(columns={'Eligible_Courses_CO': 'Eligible_Courses_List_All'}), on='Student_ID', how='left')

    # Filter matching courses from future eligible lists
    eligible_courses_comprehensive_data['Future_Eligible_Courses_List'] = eligible_courses_comprehensive_data.apply(lambda row: [course for course in row['Future_Eligible_Courses_List'] if course not in row['Eligible_Courses_List_All']], axis=1)
    eligible_courses_comprehensive_data['Total_Future_Eligible_Courses_List'] = eligible_courses_comprehensive_data['Future_Eligible_Courses_List'].apply(len)

    # Special eligibility courses
    eligible_courses_comprehensive_data['Future_Eligible_Courses_Special'] = eligible_courses_comprehensive_data.apply(lambda row: find_additional_eligibilities_special(row['Eligible_Courses_CO'], set(row['Eligible_Courses_CO']), row, prerequisites_special_acc, conditions_acc, is_eligible_special_acc_), axis=1)
    eligible_courses_comprehensive_data['Future_Eligible_Courses_Special'] = eligible_courses_comprehensive_data.apply(lambda row: [course for course in row['Future_Eligible_Courses_Special'] if course not in row['Eligible_Courses_List_All']], axis=1)
    eligible_courses_comprehensive_data['Total_Future_Eligible_Courses_Special'] = eligible_courses_comprehensive_data['Future_Eligible_Courses_Special'].apply(len)

    # Combine Future Eligible Courses and calculate the score
    eligible_courses_comprehensive_data["Future_Eligible_Courses"] = eligible_courses_comprehensive_data["Future_Eligible_Courses_List"] + eligible_courses_comprehensive_data["Future_Eligible_Courses_Special"]
    eligible_courses_comprehensive_data['Course_Score'] = eligible_courses_comprehensive_data['Future_Eligible_Courses'].apply(len)

    # Find Best Courses
    recommended_courses_acc = eligible_courses_comprehensive_data.groupby(['Student_ID', 'Semester']).apply(lambda group: pd.Series({'Recommended_Courses': find_best_courses(group)})).reset_index()


    eligible_courses_comprehensive_data = eligible_courses_comprehensive_data.merge(recommended_courses_acc, on=['Student_ID', 'Semester'])
    eligible_courses_comprehensive_data = eligible_courses_comprehensive_data.merge(weighted_remaining_courses_df, on=['Student_ID', 'AREA_OF_STUDY'], how='left')


    eligible_courses_comprehensive_data = eligible_courses_comprehensive_data.groupby('Student_ID', group_keys=False).apply(normalize_by_student)
    eligible_courses_comprehensive_data['Final_Score'] = (
        (eligible_courses_comprehensive_data['Normalized_Course_Score'] * 0.4) +
        (eligible_courses_comprehensive_data['Normalized_Remaining_Courses_Weight'] * 0.4) +
        (eligible_courses_comprehensive_data['Normalized_Course_Level'] * 0.2))

    # Find Best Courses
    recommended_courses_acc_v2 = eligible_courses_comprehensive_data.groupby(['Student_ID', 'Semester']).apply(lambda group: pd.Series({'Recommended_Courses_V2': find_best_courses_v2(group)})).reset_index()
    eligible_courses_comprehensive_data = eligible_courses_comprehensive_data.merge(recommended_courses_acc_v2, on=['Student_ID', 'Semester'])

    recommended_courses = recommended_courses_acc.merge(recommended_courses_acc_v2,on=['Student_ID', 'Semester'])

    # Create summary DataFrames for eligible courses
    summary_area_of_study_eligible = eligible_courses_comprehensive_data.groupby(['Student_ID', 'AREA_OF_STUDY']).size().reset_index(name='Total_Eligible_Courses')
    summary_course_of_study_eligible = eligible_courses_comprehensive_data.groupby(['Student_ID', 'COURSE_OF_STUDY']).size().reset_index(name='Total_Eligible_Courses')
    summary_area_of_study_eligible = summary_area_of_study_eligible.pivot_table(index="Student_ID", columns="AREA_OF_STUDY", values="Total_Eligible_Courses", fill_value=0).reset_index()

    return requirements_acc_,student_progress,summary_area_of_study_taken,remaining_courses_df,latest_eligible_courses,eligible_courses_comprehensive_data,recommended_courses,summary_area_of_study_eligible

def process_data_ib(st_hist_data,major_data, requirements_weights_path):
    
    values_to_delete = ['FA', 'F', 'I', 'S', 'NP', 'WA']
    failed_grades = ['F','FA','NP']
    failed_data = st_hist_data[st_hist_data["GRADE"].isin(failed_grades)]
    st_hist_data = st_hist_data[~st_hist_data["GRADE"].isin(values_to_delete)]
    
    # Filtering and Sorting Data
    failed_data = failed_data[failed_data['Major'] == 'International Business']
    failed_data = failed_data.sort_values(by=['Student_ID', 'Semester'])

    grouped_data_failed = failed_data.groupby(['Student_ID'])['Course_ID'].apply(list).reset_index()
    
    # Filtering and Sorting Data
    ib_data = st_hist_data[st_hist_data['Major'] == 'International Business']
    ib_data = ib_data.sort_values(by=['Student_ID', 'Semester'])

    major = major_data["All_Courses"]
    courses_co = major_data["CO_Courses"]
    
    major["AREA_OF_STUDY"] = major["AREA_OF_STUDY"].fillna("NA")
    # Dropping records where AREA_OF_STUDY is 'N' and COURSE_OF_STUDY is 'Z'
    major_filtered = major[~((major['AREA_OF_STUDY'] == 'NA') & (major['COURSE_OF_STUDY'] == 'Z'))]
    
    major_filtered = major_filtered.copy()
    # Apply replacements directly to the specific columns to avoid SettingWithCopyWarning
    major_filtered['AREA_OF_STUDY'] = major_filtered['AREA_OF_STUDY'].replace("NA","GE")
    major_filtered['COURSE_OF_STUDY'] = major_filtered['COURSE_OF_STUDY'].replace("N","E")
    
    # Defining the major lists
    cba_majors = ['ACCOUNTING', 'INTL BUSIN', 'MANAGEMENT', 'FINANCE', 'MIS', 'MARKETING2']
    # Filtering the DataFrame based on each major list
    df_cba = major_filtered[(major_filtered['Major'].isin(cba_majors)) & (major_filtered['COURSE_OF_STUDY'].isin(['R', 'RE']))]
    
    list_conditions = ['-', 'ONE_COURSE']

    cba_list = df_cba[df_cba['Condition'].isin(list_conditions)]
    cba_special_cases = df_cba[~df_cba['Condition'].isin(list_conditions)]
    cba_co = courses_co[courses_co['Major'].isin(cba_majors)]
    
    ib_list = cba_list[cba_list["Major"] == "INTL BUSIN"]
    ib_special_cases = cba_special_cases[cba_special_cases["Major"] == "INTL BUSIN"]
    ib_co = cba_co[cba_co["Major"] == "INTL BUSIN'"]

    # Process 'REQUISITES_LIST'
    ib_co = ib_co.copy()
    ib_co.loc[:, 'REQUISITES_LIST'] = ib_co['REQUISITES_LIST'].apply(ast.literal_eval)

    # CBA Courses
    cba_courses = major_filtered[major_filtered['Major'].isin(cba_majors)]
    courses_ib = cba_courses[cba_courses["Major"] == "INTL BUSIN"]
    
    grouped_data_ib = ib_data.groupby(['Student_ID'])['Course_ID'].apply(list).reset_index()

    # Merge dataframes
    merged_df = grouped_data_failed.merge(grouped_data_ib, on=['Student_ID'], how='outer', suffixes=('_failed', '_all'))
    # Replace NaN with empty lists to avoid errors
    merged_df['Course_ID_all'] = merged_df['Course_ID_all'].apply(lambda x: x if isinstance(x, list) else [])
    merged_df['Course_ID_failed'] = merged_df['Course_ID_failed'].apply(lambda x: x if isinstance(x, list) else [])

    merged_df['Failed_Courses'] = merged_df.apply(
        lambda row: list(set(row['Course_ID_failed']) - set(row['Course_ID_all'])),
        axis=1)
    # Keep only relevant columns
    merged_df = merged_df[['Student_ID', 'Failed_Courses']]

    # Extract Accounting specific requirements and weights from respective DataFrames
    requirements_df = pd.read_excel(requirements_weights_path,sheet_name="requirements")
    weights_df = pd.read_excel(requirements_weights_path,sheet_name="weights")
    requirements_ib = requirements_df[requirements_df["Major"] == "International Business"]
    requirements_ib_ = requirements_ib.pivot_table(index="Major",columns="AREA_OF_STUDY",values ='Required_Courses' ,aggfunc='sum',fill_value=0).reset_index()
    weights_ib = weights_df[weights_df["Major"] == "International Business"]

    student_courses = ib_data[["Student_ID", "Course_ID"]]

    # Map AREA_OF_STUDY and COURSE_OF_STUDY to ib_data
    student_courses = student_courses.merge(courses_ib[['Course_ID', 'AREA_OF_STUDY', 'COURSE_OF_STUDY', "Course_Level"]],
                                            on='Course_ID', how='left').drop_duplicates()

    # Create summary DataFrames for taken courses
    student_progress = student_courses.groupby(['Student_ID', 'AREA_OF_STUDY']).size().reset_index(name='Total_Taken_Courses')
    student_progress = student_progress.merge(requirements_ib, on='AREA_OF_STUDY', how='left')
    student_progress["Remaining_Courses"] = student_progress["Required_Courses"] - student_progress["Total_Taken_Courses"]
    student_progress["Remaining_Courses"] = student_progress["Remaining_Courses"].apply(lambda x: max(x, 0))

    free_elective_taken_counts = student_courses[(student_courses['AREA_OF_STUDY'] == "GE") & (student_courses['COURSE_OF_STUDY'] == "E")].groupby('Student_ID').size().reset_index(name='Total_Free_Electives_Taken')

    # Update progress by including the free elective data
    student_progress["Student_Progress"] = (student_progress["Total_Taken_Courses"] / student_progress["Required_Courses"]) * 100
    student_progress["Student_Progress"].replace([np.inf, -np.inf], 100, inplace=True)

    summary_area_of_study_taken = student_progress.pivot_table(index="Student_ID", columns="AREA_OF_STUDY", values="Total_Taken_Courses", fill_value=0)
    summary_area_of_study_taken = summary_area_of_study_taken.merge(free_elective_taken_counts, on="Student_ID", how="left").fillna(0).rename(columns={"Total_Free_Electives_Taken": "FE"})

    # Create a copy of summary_area_of_study_taken to work on remaining courses calculation
    remaining_courses_df = summary_area_of_study_taken.copy()

    # Loop through each AREA_OF_STUDY and calculate remaining courses by subtracting from the requirements
    for column in remaining_courses_df.columns:
        if column in requirements_ib['AREA_OF_STUDY'].values:
            required_courses = requirements_ib.loc[requirements_ib['AREA_OF_STUDY'] == column, 'Required_Courses'].values[0]
            remaining_courses_df[column] = required_courses - remaining_courses_df[column]
            remaining_courses_df[column] = remaining_courses_df[column].clip(lower=0)

    # Calculate weighted remaining courses
    weighted_remaining_courses_df = remaining_courses_df.copy()
    for column in weighted_remaining_courses_df.columns:
        if column in weights_ib['AREA_OF_STUDY'].values:
            weight_value = weights_ib.loc[weights_ib['AREA_OF_STUDY'] == column, 'Weight'].values[0]
            weighted_remaining_courses_df[column] = weighted_remaining_courses_df[column] * weight_value

    # Prepare weighted remaining courses for merge
    weighted_remaining_courses_df = weighted_remaining_courses_df.reset_index().melt(id_vars=['Student_ID'],
                                                                                      var_name='AREA_OF_STUDY',
                                                                                      value_name='Remaining_Courses_Weight_Score')
    weighted_remaining_courses_df = weighted_remaining_courses_df[weighted_remaining_courses_df["AREA_OF_STUDY"] != "index"]

    # Eligibility Calculation for Standard and Special Cases
    prerequisites_ib = ib_list.set_index('Course_ID')['REQUISITES_LIST'].apply(eval).to_dict()
    prerequisites_special_ib = ib_special_cases.set_index('Course_ID')['REQUISITES_LIST'].apply(eval).to_dict()
    conditions_ib = ib_special_cases.set_index('Course_ID')['Condition'].to_dict()

    final_results_ib = []  # Standard eligibility results
    final_results_special_ib = []  # Special eligibility results

    for student_id, group in ib_data.groupby('Student_ID'):
        cumulative_courses = set()
        for semester, semester_group in group.groupby('Semester'):
            taken_courses = set(semester_group['Course_ID'].tolist())
            cumulative_courses.update(taken_courses)

            # Determine Standard Eligible Courses
            student_info = semester_group.iloc[0].to_dict()
            eligible_courses = {course for course in prerequisites_ib.keys() if all(req in cumulative_courses for req in prerequisites_ib[course])}
            final_results_ib.append({
                'Student_ID': student_id,
                'Semester': semester,
                'Major': student_info['Major'],
                'College': student_info['College'],
                'Program': student_info['Program'],
                'Passed Credits': student_info['Passed Credits'],
                'Student_Level': student_info['Student_Level'],
                'Eligible_Courses': list(eligible_courses - cumulative_courses)
            })

            # Determine Special Eligible Courses
            special_eligible_courses = {
                course for course in prerequisites_special_ib.keys()
                if is_eligible_special_ib(course, cumulative_courses, student_info, prerequisites_special_ib, conditions_ib)
            }
            final_results_special_ib.append({
                'Student_ID': student_id,
                'Semester': semester,
                'Major': student_info['Major'],
                'College': student_info['College'],
                'Program': student_info['Program'],
                'Passed Credits': student_info['Passed Credits'],
                'Student_Level': student_info['Student_Level'],
                'Eligible_Courses': list(special_eligible_courses - cumulative_courses)
            })

    # Convert Results to DataFrames
    final_results_df_ib = pd.DataFrame(final_results_ib)
    final_results_special_df_ib = pd.DataFrame(final_results_special_ib)
    
    # Combine Eligible Courses from Both DataFrames
    combined_ib_list = combine_eligible_courses(final_results_df_ib, final_results_special_df_ib)
    # Find Course Combinations for Co-requisites
    combined_ib_list = combined_ib_list.apply(create_combined_courses, axis=1, co=ib_co)
    latest_eligible_courses = combined_ib_list.sort_values(by='Semester', ascending=False)
    latest_eligible_courses = latest_eligible_courses.groupby('Student_ID').first().reset_index()
    latest_eligible_courses = latest_eligible_courses.merge(grouped_data_ib,on = "Student_ID",how = "inner")
    latest_eligible_courses["Eligible_Courses_CO"] = latest_eligible_courses.apply(remove_matches, axis=1)
    latest_eligible_courses = latest_eligible_courses.apply(process_row, axis=1)
    latest_eligible_courses.drop(columns=["Course_ID"], inplace=True)

    latest_eligible_courses = latest_eligible_courses.merge(merged_df, on='Student_ID', how='outer')
    latest_eligible_courses['Failed_Courses'] = latest_eligible_courses['Failed_Courses'].apply(lambda x: x if isinstance(x, list) else [])
    latest_eligible_courses['Eligible_Courses_CO'] = latest_eligible_courses['Eligible_Courses_CO'].apply(lambda x: x if isinstance(x, list) else [])
    latest_eligible_courses['Eligible_Courses_CO'] = latest_eligible_courses.apply(
        lambda row: list(set(row['Eligible_Courses_CO']) | (set(row['Failed_Courses']) - set(row['Eligible_Courses_CO']))),axis=1)
    latest_eligible_courses = latest_eligible_courses.drop(columns=['Failed_Courses'])

    latest_info_failed = failed_data.loc[failed_data.groupby("Student_ID")["Semester"].idxmax()]
    missing_semester_df = latest_eligible_courses[latest_eligible_courses['Semester'].isna()]
    latest_eligible_courses.dropna(inplace=True)
    columns_to_fill = ['Semester', 'Major', 'College', 'Program', 'Passed Credits', 'Student_Level']

    for col in columns_to_fill:
        missing_semester_df.loc[missing_semester_df[col].isna(), col] = missing_semester_df.loc[
            missing_semester_df[col].isna(), 'Student_ID'
        ].map(latest_info_failed.set_index('Student_ID')[col])

    columns_to_convert = ['Semester', 'Student_Level', 'Passed Credits']
    for col in columns_to_convert:
        latest_eligible_courses.loc[:, col] = pd.to_numeric(latest_eligible_courses[col], errors='coerce').astype('Int64')
        
    latest_eligible_courses = pd.concat([latest_eligible_courses, missing_semester_df], ignore_index=True)

    max_semester_index = ib_data.groupby('Student_ID')['Semester'].idxmax()
    max_semester_data = ib_data.loc[max_semester_index, ['Student_ID', 'Semester']]

    last_semester_courses = pd.merge(max_semester_data, ib_data, on=['Student_ID', 'Semester'])
    eng097_fpu_students = last_semester_courses[last_semester_courses['Course_ID'] == 'ENGL097']
    # Target course list
    target_courses = ['ENGL098', 'MATH094', 'MATH095', 'MATH096', 'MATH098', 'MATH100', 'MATH111', 'MATH120', 'MATH121', 'MATH131', 'MATH140']

    eng097_fpu_students_eligible = latest_eligible_courses[latest_eligible_courses['Student_ID']
                                                       .isin(eng097_fpu_students['Student_ID'])].copy()
    eng097_fpu_students_eligible.loc[:, 'Eligible_Courses_CO'] = eng097_fpu_students_eligible['Eligible_Courses_CO'].apply(
    lambda courses: [course for course in courses if course in target_courses])

    latest_eligible_courses = latest_eligible_courses.merge(
    eng097_fpu_students_eligible[['Student_ID', 'Eligible_Courses_CO']],  # Relevant columns from filtered_students
    on='Student_ID',
    how='left',  # Keep all rows in students_df
    suffixes=('', '_updated'))  # Suffix to differentiate new column)

    latest_eligible_courses['Eligible_Courses_CO'] = latest_eligible_courses['Eligible_Courses_CO_updated'].combine_first(latest_eligible_courses['Eligible_Courses_CO'])
    latest_eligible_courses = latest_eligible_courses.drop(columns=['Eligible_Courses_CO_updated'])
    latest_eligible_courses = latest_eligible_courses.merge(grouped_data_ib,on = "Student_ID",how = "outer")
    latest_eligible_courses['Course_ID'] = latest_eligible_courses['Course_ID'].apply(lambda x: x if isinstance(x, list) else [])
    latest_eligible_courses = latest_eligible_courses.apply(process_row, axis=1)
    latest_eligible_courses.drop(columns=["Course_ID"], inplace=True)

    # Exploding DataFrame and mapping course details
    eligible_courses_comprehensive_data = latest_eligible_courses.explode("Eligible_Courses_CO")
    eligible_courses_comprehensive_data = eligible_courses_comprehensive_data.merge(courses_ib[['Course_ID', 'AREA_OF_STUDY', 'COURSE_OF_STUDY', 'Course_Level']],
                                                                                    left_on='Eligible_Courses_CO', right_on='Course_ID', how='left').drop(columns="Course_ID")
    eligible_courses_comprehensive_data['Eligible_Courses_CO'] = eligible_courses_comprehensive_data['Eligible_Courses_CO'].apply(lambda x: x if isinstance(x, list) else ([] if pd.isna(x) else [x]))

    # Find Additional Eligibilities
    eligible_courses_comprehensive_data['Future_Eligible_Courses_List'] = eligible_courses_comprehensive_data.apply(lambda row: find_additional_eligibilities(row['Eligible_Courses_CO'], set(row['Eligible_Courses_CO']), prerequisites_ib), axis=1)
    eligible_courses_per_student = eligible_courses_comprehensive_data.groupby('Student_ID')['Eligible_Courses_CO'].agg(lambda x: list(set([item for sublist in x for item in sublist if isinstance(sublist, list)]))).reset_index()

    # Merge aggregated list back to the comprehensive DataFrame
    eligible_courses_comprehensive_data = eligible_courses_comprehensive_data.merge(eligible_courses_per_student.rename(columns={'Eligible_Courses_CO': 'Eligible_Courses_List_All'}), on='Student_ID', how='left')

    # Filter matching courses from future eligible lists
    eligible_courses_comprehensive_data['Future_Eligible_Courses_List'] = eligible_courses_comprehensive_data.apply(lambda row: [course for course in row['Future_Eligible_Courses_List'] if course not in row['Eligible_Courses_List_All']], axis=1)
    eligible_courses_comprehensive_data['Total_Future_Eligible_Courses_List'] = eligible_courses_comprehensive_data['Future_Eligible_Courses_List'].apply(len)

    # Special eligibility courses
    eligible_courses_comprehensive_data['Future_Eligible_Courses_Special'] = eligible_courses_comprehensive_data.apply(lambda row: find_additional_eligibilities_special(row['Eligible_Courses_CO'], set(row['Eligible_Courses_CO']), row, prerequisites_special_ib, conditions_ib, is_eligible_special_ib_), axis=1)
    eligible_courses_comprehensive_data['Future_Eligible_Courses_Special'] = eligible_courses_comprehensive_data.apply(lambda row: [course for course in row['Future_Eligible_Courses_Special'] if course not in row['Eligible_Courses_List_All']], axis=1)
    eligible_courses_comprehensive_data['Total_Future_Eligible_Courses_Special'] = eligible_courses_comprehensive_data['Future_Eligible_Courses_Special'].apply(len)

    # Combine Future Eligible Courses and calculate the score
    eligible_courses_comprehensive_data["Future_Eligible_Courses"] = eligible_courses_comprehensive_data["Future_Eligible_Courses_List"] + eligible_courses_comprehensive_data["Future_Eligible_Courses_Special"]
    eligible_courses_comprehensive_data['Course_Score'] = eligible_courses_comprehensive_data['Future_Eligible_Courses'].apply(len)

    # Find Best Courses
    recommended_courses_ib = eligible_courses_comprehensive_data.groupby(['Student_ID', 'Semester']).apply(lambda group: pd.Series({'Recommended_Courses': find_best_courses(group)})).reset_index()


    eligible_courses_comprehensive_data = eligible_courses_comprehensive_data.merge(recommended_courses_ib, on=['Student_ID', 'Semester'])
    eligible_courses_comprehensive_data = eligible_courses_comprehensive_data.merge(weighted_remaining_courses_df, on=['Student_ID', 'AREA_OF_STUDY'], how='left')


    eligible_courses_comprehensive_data = eligible_courses_comprehensive_data.groupby('Student_ID', group_keys=False).apply(normalize_by_student)
    eligible_courses_comprehensive_data['Final_Score'] = (
        (eligible_courses_comprehensive_data['Normalized_Course_Score'] * 0.4) +
        (eligible_courses_comprehensive_data['Normalized_Remaining_Courses_Weight'] * 0.4) +
        (eligible_courses_comprehensive_data['Normalized_Course_Level'] * 0.2))

    # Find Best Courses
    recommended_courses_ib_v2 = eligible_courses_comprehensive_data.groupby(['Student_ID', 'Semester']).apply(lambda group: pd.Series({'Recommended_Courses_V2': find_best_courses_v2(group)})).reset_index()
    eligible_courses_comprehensive_data = eligible_courses_comprehensive_data.merge(recommended_courses_ib_v2, on=['Student_ID', 'Semester'])

    recommended_courses = recommended_courses_ib.merge(recommended_courses_ib_v2,on=['Student_ID', 'Semester'])

    # Create summary DataFrames for eligible courses
    summary_area_of_study_eligible = eligible_courses_comprehensive_data.groupby(['Student_ID', 'AREA_OF_STUDY']).size().reset_index(name='Total_Eligible_Courses')
    summary_course_of_study_eligible = eligible_courses_comprehensive_data.groupby(['Student_ID', 'COURSE_OF_STUDY']).size().reset_index(name='Total_Eligible_Courses')
    summary_area_of_study_eligible = summary_area_of_study_eligible.pivot_table(index="Student_ID", columns="AREA_OF_STUDY", values="Total_Eligible_Courses", fill_value=0).reset_index()

    return requirements_ib_,student_progress,summary_area_of_study_taken,remaining_courses_df,latest_eligible_courses,eligible_courses_comprehensive_data,recommended_courses,summary_area_of_study_eligible

def process_data_mob(st_hist_data,major_data, requirements_weights_path):
    
    values_to_delete = ['FA', 'F', 'I', 'S', 'NP', 'WA']
    failed_grades = ['F','FA','NP']
    failed_data = st_hist_data[st_hist_data["GRADE"].isin(failed_grades)]
    st_hist_data = st_hist_data[~st_hist_data["GRADE"].isin(values_to_delete)]
    
    # Filtering and Sorting Data
    failed_data = failed_data[failed_data['Major'] == 'Mgmt & Organizational Behavior']
    failed_data = failed_data.sort_values(by=['Student_ID', 'Semester'])

    grouped_data_failed = failed_data.groupby(['Student_ID'])['Course_ID'].apply(list).reset_index()
    
    # Filtering and Sorting Data
    mob_data = st_hist_data[st_hist_data['Major'] == "Mgmt & Organizational Behavior"]
    mob_data = mob_data.sort_values(by=['Student_ID', 'Semester'])

    major = major_data["All_Courses"]
    courses_co = major_data["CO_Courses"]
    
    major["AREA_OF_STUDY"] = major["AREA_OF_STUDY"].fillna("NA")
    # Dropping records where AREA_OF_STUDY is 'N' and COURSE_OF_STUDY is 'Z'
    major_filtered = major[~((major['AREA_OF_STUDY'] == 'NA') & (major['COURSE_OF_STUDY'] == 'Z'))]
    
    major_filtered = major_filtered.copy()
    # Apply replacements directly to the specific columns to avoid SettingWithCopyWarning
    major_filtered['AREA_OF_STUDY'] = major_filtered['AREA_OF_STUDY'].replace("NA","GE")
    major_filtered['COURSE_OF_STUDY'] = major_filtered['COURSE_OF_STUDY'].replace("N","E")
    
    # Defining the major lists
    cba_majors = ['ACCOUNTING', 'INTL BUSIN', 'MANAGEMENT', 'FINANCE', 'MIS', 'MARKETING2']
    # Filtering the DataFrame based on each major list
    df_cba = major_filtered[(major_filtered['Major'].isin(cba_majors)) & (major_filtered['COURSE_OF_STUDY'].isin(['R', 'RE']))]
    
    list_conditions = ['-', 'ONE_COURSE']

    cba_list = df_cba[df_cba['Condition'].isin(list_conditions)]
    cba_special_cases = df_cba[~df_cba['Condition'].isin(list_conditions)]
    cba_co = courses_co[courses_co['Major'].isin(cba_majors)]
    
    mob_list = cba_list[cba_list["Major"] == "MANAGEMENT"]
    mob_special_cases = cba_special_cases[cba_special_cases["Major"] == "MANAGEMENT"]
    mob_co = cba_co[cba_co["Major"] == "MANAGEMENT"]

    # Process 'REQUISITES_LIST'
    mob_co = mob_co.copy()
    mob_co.loc[:, 'REQUISITES_LIST'] = mob_co['REQUISITES_LIST'].apply(ast.literal_eval)

    # CBA Courses
    cba_courses = major_filtered[major_filtered['Major'].isin(cba_majors)]
    courses_mob = cba_courses[cba_courses["Major"] == "MANAGEMENT"]
    
    grouped_data_mob = mob_data.groupby(['Student_ID'])['Course_ID'].apply(list).reset_index()

    # Merge dataframes
    merged_df = grouped_data_failed.merge(grouped_data_mob, on=['Student_ID'], how='outer', suffixes=('_failed', '_all'))
    # Replace NaN with empty lists to avoid errors
    merged_df['Course_ID_all'] = merged_df['Course_ID_all'].apply(lambda x: x if isinstance(x, list) else [])
    merged_df['Course_ID_failed'] = merged_df['Course_ID_failed'].apply(lambda x: x if isinstance(x, list) else [])

    merged_df['Failed_Courses'] = merged_df.apply(
        lambda row: list(set(row['Course_ID_failed']) - set(row['Course_ID_all'])),
        axis=1)
    # Keep only relevant columns
    merged_df = merged_df[['Student_ID', 'Failed_Courses']]

    # Extract Accounting specific requirements and weights from respective DataFrames
    requirements_df = pd.read_excel(requirements_weights_path,sheet_name="requirements")
    weights_df = pd.read_excel(requirements_weights_path,sheet_name="weights")
    requirements_mob = requirements_df[requirements_df["Major"] == "Mgmt & Organizational Behavior"]
    requirements_mob_ = requirements_mob.pivot_table(index="Major",columns="AREA_OF_STUDY",values ='Required_Courses' ,aggfunc='sum',fill_value=0).reset_index()
    weights_mob = weights_df[weights_df["Major"] == "Mgmt & Organizational Behavior"]

    student_courses = mob_data[["Student_ID", "Course_ID"]]

    # Map AREA_OF_STUDY and COURSE_OF_STUDY to mob_data
    student_courses = student_courses.merge(courses_mob[['Course_ID', 'AREA_OF_STUDY', 'COURSE_OF_STUDY', "Course_Level"]],
                                            on='Course_ID', how='left').drop_duplicates()

    # Create summary DataFrames for taken courses
    student_progress = student_courses.groupby(['Student_ID', 'AREA_OF_STUDY']).size().reset_index(name='Total_Taken_Courses')
    student_progress = student_progress.merge(requirements_mob, on='AREA_OF_STUDY', how='left')
    student_progress["Remaining_Courses"] = student_progress["Required_Courses"] - student_progress["Total_Taken_Courses"]
    student_progress["Remaining_Courses"] = student_progress["Remaining_Courses"].apply(lambda x: max(x, 0))

    free_elective_taken_counts = student_courses[(student_courses['AREA_OF_STUDY'] == "GE") & (student_courses['COURSE_OF_STUDY'] == "E")].groupby('Student_ID').size().reset_index(name='Total_Free_Electives_Taken')

    # Update progress by including the free elective data
    student_progress["Student_Progress"] = (student_progress["Total_Taken_Courses"] / student_progress["Required_Courses"]) * 100
    student_progress["Student_Progress"].replace([np.inf, -np.inf], 100, inplace=True)

    summary_area_of_study_taken = student_progress.pivot_table(index="Student_ID", columns="AREA_OF_STUDY", values="Total_Taken_Courses", fill_value=0)
    summary_area_of_study_taken = summary_area_of_study_taken.merge(free_elective_taken_counts, on="Student_ID", how="left").fillna(0).rename(columns={"Total_Free_Electives_Taken": "FE"})

    # Create a copy of summary_area_of_study_taken to work on remaining courses calculation
    remaining_courses_df = summary_area_of_study_taken.copy()

    # Loop through each AREA_OF_STUDY and calculate remaining courses by subtracting from the requirements
    for column in remaining_courses_df.columns:
        if column in requirements_mob['AREA_OF_STUDY'].values:
            required_courses = requirements_mob.loc[requirements_mob['AREA_OF_STUDY'] == column, 'Required_Courses'].values[0]
            remaining_courses_df[column] = required_courses - remaining_courses_df[column]
            remaining_courses_df[column] = remaining_courses_df[column].clip(lower=0)

    # Calculate weighted remaining courses
    weighted_remaining_courses_df = remaining_courses_df.copy()
    for column in weighted_remaining_courses_df.columns:
        if column in weights_mob['AREA_OF_STUDY'].values:
            weight_value = weights_mob.loc[weights_mob['AREA_OF_STUDY'] == column, 'Weight'].values[0]
            weighted_remaining_courses_df[column] = weighted_remaining_courses_df[column] * weight_value

    # Prepare weighted remaining courses for merge
    weighted_remaining_courses_df = weighted_remaining_courses_df.reset_index().melt(id_vars=['Student_ID'],
                                                                                      var_name='AREA_OF_STUDY',
                                                                                      value_name='Remaining_Courses_Weight_Score')
    weighted_remaining_courses_df = weighted_remaining_courses_df[weighted_remaining_courses_df["AREA_OF_STUDY"] != "index"]

    # Eligibility Calculation for Standard and Special Cases
    prerequisites_mob = mob_list.set_index('Course_ID')['REQUISITES_LIST'].apply(eval).to_dict()
    prerequisites_special_mob = mob_special_cases.set_index('Course_ID')['REQUISITES_LIST'].apply(eval).to_dict()
    conditions_mob = mob_special_cases.set_index('Course_ID')['Condition'].to_dict()

    final_results_mob = []  # Standard eligibility results
    final_results_special_mob = []  # Special eligibility results

    for student_id, group in mob_data.groupby('Student_ID'):
        cumulative_courses = set()
        for semester, semester_group in group.groupby('Semester'):
            taken_courses = set(semester_group['Course_ID'].tolist())
            cumulative_courses.update(taken_courses)

            # Determine Standard Eligible Courses
            student_info = semester_group.iloc[0].to_dict()
            eligible_courses = {course for course in prerequisites_mob.keys() if all(req in cumulative_courses for req in prerequisites_mob[course])}
            final_results_mob.append({
                'Student_ID': student_id,
                'Semester': semester,
                'Major': student_info['Major'],
                'College': student_info['College'],
                'Program': student_info['Program'],
                'Passed Credits': student_info['Passed Credits'],
                'Student_Level': student_info['Student_Level'],
                'Eligible_Courses': list(eligible_courses - cumulative_courses)
            })

            # Determine Special Eligible Courses
            special_eligible_courses = {
                course for course in prerequisites_special_mob.keys()
                if is_eligible_special_mob(course, cumulative_courses, student_info, prerequisites_special_mob, conditions_mob)
            }
            final_results_special_mob.append({
                'Student_ID': student_id,
                'Semester': semester,
                'Major': student_info['Major'],
                'College': student_info['College'],
                'Program': student_info['Program'],
                'Passed Credits': student_info['Passed Credits'],
                'Student_Level': student_info['Student_Level'],
                'Eligible_Courses': list(special_eligible_courses - cumulative_courses)
            })

    # Convert Results to DataFrames
    final_results_df_mob = pd.DataFrame(final_results_mob)
    final_results_special_df_mob = pd.DataFrame(final_results_special_mob)
    
    # Combine Eligible Courses from Both DataFrames
    combined_mob_list = combine_eligible_courses(final_results_df_mob, final_results_special_df_mob)
    # Find Course Combinations for Co-requisites
    combined_mob_list = combined_mob_list.apply(create_combined_courses, axis=1, co=mob_co)
    latest_eligible_courses = combined_mob_list.sort_values(by='Semester', ascending=False)
    latest_eligible_courses = latest_eligible_courses.groupby('Student_ID').first().reset_index()
    latest_eligible_courses = latest_eligible_courses.merge(grouped_data_mob,on = "Student_ID",how = "inner")
    latest_eligible_courses["Eligible_Courses_CO"] = latest_eligible_courses.apply(remove_matches, axis=1)
    latest_eligible_courses = latest_eligible_courses.apply(process_row, axis=1)
    latest_eligible_courses.drop(columns=["Course_ID"], inplace=True)

    latest_eligible_courses = latest_eligible_courses.merge(merged_df, on='Student_ID', how='outer')
    latest_eligible_courses['Failed_Courses'] = latest_eligible_courses['Failed_Courses'].apply(lambda x: x if isinstance(x, list) else [])
    latest_eligible_courses['Eligible_Courses_CO'] = latest_eligible_courses['Eligible_Courses_CO'].apply(lambda x: x if isinstance(x, list) else [])
    latest_eligible_courses['Eligible_Courses_CO'] = latest_eligible_courses.apply(
        lambda row: list(set(row['Eligible_Courses_CO']) | (set(row['Failed_Courses']) - set(row['Eligible_Courses_CO']))),axis=1)
    latest_eligible_courses = latest_eligible_courses.drop(columns=['Failed_Courses'])

    latest_info_failed = failed_data.loc[failed_data.groupby("Student_ID")["Semester"].idxmax()]
    missing_semester_df = latest_eligible_courses[latest_eligible_courses['Semester'].isna()]
    latest_eligible_courses.dropna(inplace=True)
    columns_to_fill = ['Semester', 'Major', 'College', 'Program', 'Passed Credits', 'Student_Level']

    for col in columns_to_fill:
        missing_semester_df.loc[missing_semester_df[col].isna(), col] = missing_semester_df.loc[
            missing_semester_df[col].isna(), 'Student_ID'
        ].map(latest_info_failed.set_index('Student_ID')[col])

    columns_to_convert = ['Semester', 'Student_Level', 'Passed Credits']
    for col in columns_to_convert:
        latest_eligible_courses.loc[:, col] = pd.to_numeric(latest_eligible_courses[col], errors='coerce').astype('Int64')
        
    latest_eligible_courses = pd.concat([latest_eligible_courses, missing_semester_df], ignore_index=True)

    max_semester_index = mob_data.groupby('Student_ID')['Semester'].idxmax()
    max_semester_data = mob_data.loc[max_semester_index, ['Student_ID', 'Semester']]

    last_semester_courses = pd.merge(max_semester_data, mob_data, on=['Student_ID', 'Semester'])
    eng097_fpu_students = last_semester_courses[last_semester_courses['Course_ID'] == 'ENGL097']
    # Target course list
    target_courses = ['ENGL098', 'MATH094', 'MATH095', 'MATH096', 'MATH098', 'MATH100', 'MATH111', 'MATH120', 'MATH121', 'MATH131', 'MATH140']

    eng097_fpu_students_eligible = latest_eligible_courses[latest_eligible_courses['Student_ID']
                                                       .isin(eng097_fpu_students['Student_ID'])].copy()
    eng097_fpu_students_eligible.loc[:, 'Eligible_Courses_CO'] = eng097_fpu_students_eligible['Eligible_Courses_CO'].apply(
    lambda courses: [course for course in courses if course in target_courses])

    latest_eligible_courses = latest_eligible_courses.merge(
    eng097_fpu_students_eligible[['Student_ID', 'Eligible_Courses_CO']],  # Relevant columns from filtered_students
    on='Student_ID',
    how='left',  # Keep all rows in students_df
    suffixes=('', '_updated'))  # Suffix to differentiate new column)

    latest_eligible_courses['Eligible_Courses_CO'] = latest_eligible_courses['Eligible_Courses_CO_updated'].combine_first(latest_eligible_courses['Eligible_Courses_CO'])
    latest_eligible_courses = latest_eligible_courses.drop(columns=['Eligible_Courses_CO_updated'])
    latest_eligible_courses = latest_eligible_courses.merge(grouped_data_mob,on = "Student_ID",how = "outer")
    latest_eligible_courses['Course_ID'] = latest_eligible_courses['Course_ID'].apply(lambda x: x if isinstance(x, list) else [])
    latest_eligible_courses = latest_eligible_courses.apply(process_row, axis=1)
    latest_eligible_courses.drop(columns=["Course_ID"], inplace=True)

    # Exploding DataFrame and mapping course details
    eligible_courses_comprehensive_data = latest_eligible_courses.explode("Eligible_Courses_CO")
    eligible_courses_comprehensive_data = eligible_courses_comprehensive_data.merge(courses_mob[['Course_ID', 'AREA_OF_STUDY', 'COURSE_OF_STUDY', 'Course_Level']],
                                                                                    left_on='Eligible_Courses_CO', right_on='Course_ID', how='left').drop(columns="Course_ID")
    eligible_courses_comprehensive_data['Eligible_Courses_CO'] = eligible_courses_comprehensive_data['Eligible_Courses_CO'].apply(lambda x: x if isinstance(x, list) else ([] if pd.isna(x) else [x]))

    # Find Additional Eligibilities
    eligible_courses_comprehensive_data['Future_Eligible_Courses_List'] = eligible_courses_comprehensive_data.apply(lambda row: find_additional_eligibilities(row['Eligible_Courses_CO'], set(row['Eligible_Courses_CO']), prerequisites_mob), axis=1)
    eligible_courses_per_student = eligible_courses_comprehensive_data.groupby('Student_ID')['Eligible_Courses_CO'].agg(lambda x: list(set([item for sublist in x for item in sublist if isinstance(sublist, list)]))).reset_index()

    # Merge aggregated list back to the comprehensive DataFrame
    eligible_courses_comprehensive_data = eligible_courses_comprehensive_data.merge(eligible_courses_per_student.rename(columns={'Eligible_Courses_CO': 'Eligible_Courses_List_All'}), on='Student_ID', how='left')

    # Filter matching courses from future eligible lists
    eligible_courses_comprehensive_data['Future_Eligible_Courses_List'] = eligible_courses_comprehensive_data.apply(lambda row: [course for course in row['Future_Eligible_Courses_List'] if course not in row['Eligible_Courses_List_All']], axis=1)
    eligible_courses_comprehensive_data['Total_Future_Eligible_Courses_List'] = eligible_courses_comprehensive_data['Future_Eligible_Courses_List'].apply(len)

    # Special eligibility courses
    eligible_courses_comprehensive_data['Future_Eligible_Courses_Special'] = eligible_courses_comprehensive_data.apply(lambda row: find_additional_eligibilities_special(row['Eligible_Courses_CO'], set(row['Eligible_Courses_CO']), row, prerequisites_special_mob, conditions_mob, is_eligible_special_mob_), axis=1)
    eligible_courses_comprehensive_data['Future_Eligible_Courses_Special'] = eligible_courses_comprehensive_data.apply(lambda row: [course for course in row['Future_Eligible_Courses_Special'] if course not in row['Eligible_Courses_List_All']], axis=1)
    eligible_courses_comprehensive_data['Total_Future_Eligible_Courses_Special'] = eligible_courses_comprehensive_data['Future_Eligible_Courses_Special'].apply(len)

    # Combine Future Eligible Courses and calculate the score
    eligible_courses_comprehensive_data["Future_Eligible_Courses"] = eligible_courses_comprehensive_data["Future_Eligible_Courses_List"] + eligible_courses_comprehensive_data["Future_Eligible_Courses_Special"]
    eligible_courses_comprehensive_data['Course_Score'] = eligible_courses_comprehensive_data['Future_Eligible_Courses'].apply(len)

    # Find Best Courses
    recommended_courses_mob = eligible_courses_comprehensive_data.groupby(['Student_ID', 'Semester']).apply(lambda group: pd.Series({'Recommended_Courses': find_best_courses(group)})).reset_index()


    eligible_courses_comprehensive_data = eligible_courses_comprehensive_data.merge(recommended_courses_mob, on=['Student_ID', 'Semester'])
    eligible_courses_comprehensive_data = eligible_courses_comprehensive_data.merge(weighted_remaining_courses_df, on=['Student_ID', 'AREA_OF_STUDY'], how='left')


    eligible_courses_comprehensive_data = eligible_courses_comprehensive_data.groupby('Student_ID', group_keys=False).apply(normalize_by_student)
    eligible_courses_comprehensive_data['Final_Score'] = (
        (eligible_courses_comprehensive_data['Normalized_Course_Score'] * 0.4) +
        (eligible_courses_comprehensive_data['Normalized_Remaining_Courses_Weight'] * 0.4) +
        (eligible_courses_comprehensive_data['Normalized_Course_Level'] * 0.2))

    # Find Best Courses
    recommended_courses_mob_v2 = eligible_courses_comprehensive_data.groupby(['Student_ID', 'Semester']).apply(lambda group: pd.Series({'Recommended_Courses_V2': find_best_courses_v2(group)})).reset_index()
    eligible_courses_comprehensive_data = eligible_courses_comprehensive_data.merge(recommended_courses_mob_v2, on=['Student_ID', 'Semester'])

    recommended_courses = recommended_courses_mob.merge(recommended_courses_mob_v2,on=['Student_ID', 'Semester'])

    # Create summary DataFrames for eligible courses
    summary_area_of_study_eligible = eligible_courses_comprehensive_data.groupby(['Student_ID', 'AREA_OF_STUDY']).size().reset_index(name='Total_Eligible_Courses')
    summary_course_of_study_eligible = eligible_courses_comprehensive_data.groupby(['Student_ID', 'COURSE_OF_STUDY']).size().reset_index(name='Total_Eligible_Courses')
    summary_area_of_study_eligible = summary_area_of_study_eligible.pivot_table(index="Student_ID", columns="AREA_OF_STUDY", values="Total_Eligible_Courses", fill_value=0).reset_index()

    return requirements_mob_,student_progress,summary_area_of_study_taken,remaining_courses_df,latest_eligible_courses,eligible_courses_comprehensive_data,recommended_courses,summary_area_of_study_eligible

def process_data_mis(st_hist_data,major_data, requirements_weights_path):
    
    values_to_delete = ['FA', 'F', 'I', 'S', 'NP', 'WA']
    failed_grades = ['F','FA','NP']
    failed_data = st_hist_data[st_hist_data["GRADE"].isin(failed_grades)]
    st_hist_data = st_hist_data[~st_hist_data["GRADE"].isin(values_to_delete)]
    
    # Filtering and Sorting Data
    failed_data = failed_data[failed_data['Major'] == 'Management Information Systems']
    failed_data = failed_data.sort_values(by=['Student_ID', 'Semester'])

    grouped_data_failed = failed_data.groupby(['Student_ID'])['Course_ID'].apply(list).reset_index()
    
    # Filtering and Sorting Data
    mis_data = st_hist_data[st_hist_data['Major'] == "Management Information Systems"]
    mis_data = mis_data.sort_values(by=['Student_ID', 'Semester'])

    major = major_data["All_Courses"]
    courses_co = major_data["CO_Courses"]
    
    major["AREA_OF_STUDY"] = major["AREA_OF_STUDY"].fillna("NA")
    # Dropping records where AREA_OF_STUDY is 'N' and COURSE_OF_STUDY is 'Z'
    major_filtered = major[~((major['AREA_OF_STUDY'] == 'NA') & (major['COURSE_OF_STUDY'] == 'Z'))]
    
    major_filtered = major_filtered.copy()
    # Apply replacements directly to the specific columns to avoid SettingWithCopyWarning
    major_filtered['AREA_OF_STUDY'] = major_filtered['AREA_OF_STUDY'].replace("NA","GE")
    major_filtered['COURSE_OF_STUDY'] = major_filtered['COURSE_OF_STUDY'].replace("N","E")
    
    # Defining the major lists
    cba_majors = ['ACCOUNTING', 'INTL BUSIN', 'MANAGEMENT', 'FINANCE', 'MIS', 'MARKETING2']
    # Filtering the DataFrame based on each major list
    df_cba = major_filtered[(major_filtered['Major'].isin(cba_majors)) & (major_filtered['COURSE_OF_STUDY'].isin(['R', 'RE']))]
    
    list_conditions = ['-', 'ONE_COURSE']

    cba_list = df_cba[df_cba['Condition'].isin(list_conditions)]
    cba_special_cases = df_cba[~df_cba['Condition'].isin(list_conditions)]
    cba_co = courses_co[courses_co['Major'].isin(cba_majors)]
    
    mis_list = cba_list[cba_list["Major"] == "MIS"]
    mis_special_cases = cba_special_cases[cba_special_cases["Major"] == "MIS"]
    mis_co = cba_co[cba_co["Major"] == "MIS"]

    # Process 'REQUISITES_LIST'
    mis_co = mis_co.copy()
    mis_co.loc[:, 'REQUISITES_LIST'] = mis_co['REQUISITES_LIST'].apply(ast.literal_eval)

    # CBA Courses
    cba_courses = major_filtered[major_filtered['Major'].isin(cba_majors)]
    courses_mis = cba_courses[cba_courses["Major"] == "MIS"]
    
    grouped_data_mis = mis_data.groupby(['Student_ID'])['Course_ID'].apply(list).reset_index()

    # Merge dataframes
    merged_df = grouped_data_failed.merge(grouped_data_mis, on=['Student_ID'], how='outer', suffixes=('_failed', '_all'))
    # Replace NaN with empty lists to avoid errors
    merged_df['Course_ID_all'] = merged_df['Course_ID_all'].apply(lambda x: x if isinstance(x, list) else [])
    merged_df['Course_ID_failed'] = merged_df['Course_ID_failed'].apply(lambda x: x if isinstance(x, list) else [])

    merged_df['Failed_Courses'] = merged_df.apply(
        lambda row: list(set(row['Course_ID_failed']) - set(row['Course_ID_all'])),
        axis=1)
    # Keep only relevant columns
    merged_df = merged_df[['Student_ID', 'Failed_Courses']]

    # Extract Accounting specific requirements and weights from respective DataFrames
    requirements_df = pd.read_excel(requirements_weights_path,sheet_name="requirements")
    weights_df = pd.read_excel(requirements_weights_path,sheet_name="weights")
    requirements_mis = requirements_df[requirements_df["Major"] == "Management Information Systems"]
    requirements_mis_ = requirements_mis.pivot_table(index="Major",columns="AREA_OF_STUDY",values ='Required_Courses' ,aggfunc='sum',fill_value=0).reset_index()
    weights_mis = weights_df[weights_df["Major"] == "Management Information Systems"]

    student_courses = mis_data[["Student_ID", "Course_ID"]]

    # Map AREA_OF_STUDY and COURSE_OF_STUDY to mis_data
    student_courses = student_courses.merge(courses_mis[['Course_ID', 'AREA_OF_STUDY', 'COURSE_OF_STUDY', "Course_Level"]],
                                            on='Course_ID', how='left').drop_duplicates()

    # Create summary DataFrames for taken courses
    student_progress = student_courses.groupby(['Student_ID', 'AREA_OF_STUDY']).size().reset_index(name='Total_Taken_Courses')
    student_progress = student_progress.merge(requirements_mis, on='AREA_OF_STUDY', how='left')
    student_progress["Remaining_Courses"] = student_progress["Required_Courses"] - student_progress["Total_Taken_Courses"]
    student_progress["Remaining_Courses"] = student_progress["Remaining_Courses"].apply(lambda x: max(x, 0))

    free_elective_taken_counts = student_courses[(student_courses['AREA_OF_STUDY'] == "GE") & (student_courses['COURSE_OF_STUDY'] == "E")].groupby('Student_ID').size().reset_index(name='Total_Free_Electives_Taken')

    # Update progress by including the free elective data
    student_progress["Student_Progress"] = (student_progress["Total_Taken_Courses"] / student_progress["Required_Courses"]) * 100
    student_progress["Student_Progress"].replace([np.inf, -np.inf], 100, inplace=True)

    summary_area_of_study_taken = student_progress.pivot_table(index="Student_ID", columns="AREA_OF_STUDY", values="Total_Taken_Courses", fill_value=0)
    summary_area_of_study_taken = summary_area_of_study_taken.merge(free_elective_taken_counts, on="Student_ID", how="left").fillna(0).rename(columns={"Total_Free_Electives_Taken": "FE"})

    # Create a copy of summary_area_of_study_taken to work on remaining courses calculation
    remaining_courses_df = summary_area_of_study_taken.copy()

    # Loop through each AREA_OF_STUDY and calculate remaining courses by subtracting from the requirements
    for column in remaining_courses_df.columns:
        if column in requirements_mis['AREA_OF_STUDY'].values:
            required_courses = requirements_mis.loc[requirements_mis['AREA_OF_STUDY'] == column, 'Required_Courses'].values[0]
            remaining_courses_df[column] = required_courses - remaining_courses_df[column]
            remaining_courses_df[column] = remaining_courses_df[column].clip(lower=0)

    # Calculate weighted remaining courses
    weighted_remaining_courses_df = remaining_courses_df.copy()
    for column in weighted_remaining_courses_df.columns:
        if column in weights_mis['AREA_OF_STUDY'].values:
            weight_value = weights_mis.loc[weights_mis['AREA_OF_STUDY'] == column, 'Weight'].values[0]
            weighted_remaining_courses_df[column] = weighted_remaining_courses_df[column] * weight_value

    # Prepare weighted remaining courses for merge
    weighted_remaining_courses_df = weighted_remaining_courses_df.reset_index().melt(id_vars=['Student_ID'],
                                                                                      var_name='AREA_OF_STUDY',
                                                                                      value_name='Remaining_Courses_Weight_Score')
    weighted_remaining_courses_df = weighted_remaining_courses_df[weighted_remaining_courses_df["AREA_OF_STUDY"] != "index"]

    # Eligibility Calculation for Standard and Special Cases
    prerequisites_mis = mis_list.set_index('Course_ID')['REQUISITES_LIST'].apply(eval).to_dict()
    prerequisites_special_mis = mis_special_cases.set_index('Course_ID')['REQUISITES_LIST'].apply(eval).to_dict()
    conditions_mis = mis_special_cases.set_index('Course_ID')['Condition'].to_dict()

    final_results_mis = []  # Standard eligibility results
    final_results_special_mis = []  # Special eligibility results

    for student_id, group in mis_data.groupby('Student_ID'):
        cumulative_courses = set()
        for semester, semester_group in group.groupby('Semester'):
            taken_courses = set(semester_group['Course_ID'].tolist())
            cumulative_courses.update(taken_courses)

            # Determine Standard Eligible Courses
            student_info = semester_group.iloc[0].to_dict()
            eligible_courses = {course for course in prerequisites_mis.keys() if all(req in cumulative_courses for req in prerequisites_mis[course])}
            final_results_mis.append({
                'Student_ID': student_id,
                'Semester': semester,
                'Major': student_info['Major'],
                'College': student_info['College'],
                'Program': student_info['Program'],
                'Passed Credits': student_info['Passed Credits'],
                'Student_Level': student_info['Student_Level'],
                'Eligible_Courses': list(eligible_courses - cumulative_courses)
            })

            # Determine Special Eligible Courses
            special_eligible_courses = {
                course for course in prerequisites_special_mis.keys()
                if is_eligible_special_mis(course, cumulative_courses, student_info, prerequisites_special_mis, conditions_mis)
            }
            final_results_special_mis.append({
                'Student_ID': student_id,
                'Semester': semester,
                'Major': student_info['Major'],
                'College': student_info['College'],
                'Program': student_info['Program'],
                'Passed Credits': student_info['Passed Credits'],
                'Student_Level': student_info['Student_Level'],
                'Eligible_Courses': list(special_eligible_courses - cumulative_courses)
            })

    # Convert Results to DataFrames
    final_results_df_mis = pd.DataFrame(final_results_mis)
    final_results_special_df_mis = pd.DataFrame(final_results_special_mis)
    
    # Combine Eligible Courses from Both DataFrames
    combined_mis_list = combine_eligible_courses(final_results_df_mis, final_results_special_df_mis)
    # Find Course Combinations for Co-requisites
    combined_mis_list = combined_mis_list.apply(create_combined_courses, axis=1, co=mis_co)
    latest_eligible_courses = combined_mis_list.sort_values(by='Semester', ascending=False)
    latest_eligible_courses = latest_eligible_courses.groupby('Student_ID').first().reset_index()
    latest_eligible_courses = latest_eligible_courses.merge(grouped_data_mis,on = "Student_ID",how = "inner")
    latest_eligible_courses["Eligible_Courses_CO"] = latest_eligible_courses.apply(remove_matches, axis=1)
    latest_eligible_courses = latest_eligible_courses.apply(process_row, axis=1)
    latest_eligible_courses.drop(columns=["Course_ID"], inplace=True)

    latest_eligible_courses = latest_eligible_courses.merge(merged_df, on='Student_ID', how='outer')
    latest_eligible_courses['Failed_Courses'] = latest_eligible_courses['Failed_Courses'].apply(lambda x: x if isinstance(x, list) else [])
    latest_eligible_courses['Eligible_Courses_CO'] = latest_eligible_courses['Eligible_Courses_CO'].apply(lambda x: x if isinstance(x, list) else [])
    latest_eligible_courses['Eligible_Courses_CO'] = latest_eligible_courses.apply(
        lambda row: list(set(row['Eligible_Courses_CO']) | (set(row['Failed_Courses']) - set(row['Eligible_Courses_CO']))),axis=1)
    latest_eligible_courses = latest_eligible_courses.drop(columns=['Failed_Courses'])

    latest_info_failed = failed_data.loc[failed_data.groupby("Student_ID")["Semester"].idxmax()]
    missing_semester_df = latest_eligible_courses[latest_eligible_courses['Semester'].isna()]
    latest_eligible_courses.dropna(inplace=True)
    columns_to_fill = ['Semester', 'Major', 'College', 'Program', 'Passed Credits', 'Student_Level']

    for col in columns_to_fill:
        missing_semester_df.loc[missing_semester_df[col].isna(), col] = missing_semester_df.loc[
            missing_semester_df[col].isna(), 'Student_ID'
        ].map(latest_info_failed.set_index('Student_ID')[col])

    columns_to_convert = ['Semester', 'Student_Level', 'Passed Credits']
    for col in columns_to_convert:
        latest_eligible_courses.loc[:, col] = pd.to_numeric(latest_eligible_courses[col], errors='coerce').astype('Int64')
        
    latest_eligible_courses = pd.concat([latest_eligible_courses, missing_semester_df], ignore_index=True)

    max_semester_index = mis_data.groupby('Student_ID')['Semester'].idxmax()
    max_semester_data = mis_data.loc[max_semester_index, ['Student_ID', 'Semester']]

    last_semester_courses = pd.merge(max_semester_data, mis_data, on=['Student_ID', 'Semester'])
    eng097_fpu_students = last_semester_courses[last_semester_courses['Course_ID'] == 'ENGL097']
    # Target course list
    target_courses = ['ENGL098', 'MATH094', 'MATH095', 'MATH096', 'MATH098', 'MATH100', 'MATH111', 'MATH120', 'MATH121', 'MATH131', 'MATH140']

    eng097_fpu_students_eligible = latest_eligible_courses[latest_eligible_courses['Student_ID']
                                                       .isin(eng097_fpu_students['Student_ID'])].copy()
    eng097_fpu_students_eligible.loc[:, 'Eligible_Courses_CO'] = eng097_fpu_students_eligible['Eligible_Courses_CO'].apply(
    lambda courses: [course for course in courses if course in target_courses])

    latest_eligible_courses = latest_eligible_courses.merge(
    eng097_fpu_students_eligible[['Student_ID', 'Eligible_Courses_CO']],  # Relevant columns from filtered_students
    on='Student_ID',
    how='left',  # Keep all rows in students_df
    suffixes=('', '_updated'))  # Suffix to differentiate new column)

    latest_eligible_courses['Eligible_Courses_CO'] = latest_eligible_courses['Eligible_Courses_CO_updated'].combine_first(latest_eligible_courses['Eligible_Courses_CO'])
    latest_eligible_courses = latest_eligible_courses.drop(columns=['Eligible_Courses_CO_updated'])
    latest_eligible_courses = latest_eligible_courses.merge(grouped_data_mis,on = "Student_ID",how = "outer")
    latest_eligible_courses['Course_ID'] = latest_eligible_courses['Course_ID'].apply(lambda x: x if isinstance(x, list) else [])
    latest_eligible_courses = latest_eligible_courses.apply(process_row, axis=1)
    latest_eligible_courses.drop(columns=["Course_ID"], inplace=True)

    # Exploding DataFrame and mapping course details
    eligible_courses_comprehensive_data = latest_eligible_courses.explode("Eligible_Courses_CO")
    eligible_courses_comprehensive_data = eligible_courses_comprehensive_data.merge(courses_mis[['Course_ID', 'AREA_OF_STUDY', 'COURSE_OF_STUDY', 'Course_Level']],
                                                                                    left_on='Eligible_Courses_CO', right_on='Course_ID', how='left').drop(columns="Course_ID")
    eligible_courses_comprehensive_data['Eligible_Courses_CO'] = eligible_courses_comprehensive_data['Eligible_Courses_CO'].apply(lambda x: x if isinstance(x, list) else ([] if pd.isna(x) else [x]))

    # Find Additional Eligibilities
    eligible_courses_comprehensive_data['Future_Eligible_Courses_List'] = eligible_courses_comprehensive_data.apply(lambda row: find_additional_eligibilities(row['Eligible_Courses_CO'], set(row['Eligible_Courses_CO']), prerequisites_mis), axis=1)
    eligible_courses_per_student = eligible_courses_comprehensive_data.groupby('Student_ID')['Eligible_Courses_CO'].agg(lambda x: list(set([item for sublist in x for item in sublist if isinstance(sublist, list)]))).reset_index()

    # Merge aggregated list back to the comprehensive DataFrame
    eligible_courses_comprehensive_data = eligible_courses_comprehensive_data.merge(eligible_courses_per_student.rename(columns={'Eligible_Courses_CO': 'Eligible_Courses_List_All'}), on='Student_ID', how='left')

    # Filter matching courses from future eligible lists
    eligible_courses_comprehensive_data['Future_Eligible_Courses_List'] = eligible_courses_comprehensive_data.apply(lambda row: [course for course in row['Future_Eligible_Courses_List'] if course not in row['Eligible_Courses_List_All']], axis=1)
    eligible_courses_comprehensive_data['Total_Future_Eligible_Courses_List'] = eligible_courses_comprehensive_data['Future_Eligible_Courses_List'].apply(len)

    # Special eligibility courses
    eligible_courses_comprehensive_data['Future_Eligible_Courses_Special'] = eligible_courses_comprehensive_data.apply(lambda row: find_additional_eligibilities_special(row['Eligible_Courses_CO'], set(row['Eligible_Courses_CO']), row, prerequisites_special_mis, conditions_mis, is_eligible_special_mis_), axis=1)
    eligible_courses_comprehensive_data['Future_Eligible_Courses_Special'] = eligible_courses_comprehensive_data.apply(lambda row: [course for course in row['Future_Eligible_Courses_Special'] if course not in row['Eligible_Courses_List_All']], axis=1)
    eligible_courses_comprehensive_data['Total_Future_Eligible_Courses_Special'] = eligible_courses_comprehensive_data['Future_Eligible_Courses_Special'].apply(len)

    # Combine Future Eligible Courses and calculate the score
    eligible_courses_comprehensive_data["Future_Eligible_Courses"] = eligible_courses_comprehensive_data["Future_Eligible_Courses_List"] + eligible_courses_comprehensive_data["Future_Eligible_Courses_Special"]
    eligible_courses_comprehensive_data['Course_Score'] = eligible_courses_comprehensive_data['Future_Eligible_Courses'].apply(len)

    # Find Best Courses
    recommended_courses_mis = eligible_courses_comprehensive_data.groupby(['Student_ID', 'Semester']).apply(lambda group: pd.Series({'Recommended_Courses': find_best_courses(group)})).reset_index()


    eligible_courses_comprehensive_data = eligible_courses_comprehensive_data.merge(recommended_courses_mis, on=['Student_ID', 'Semester'])
    eligible_courses_comprehensive_data = eligible_courses_comprehensive_data.merge(weighted_remaining_courses_df, on=['Student_ID', 'AREA_OF_STUDY'], how='left')


    eligible_courses_comprehensive_data = eligible_courses_comprehensive_data.groupby('Student_ID', group_keys=False).apply(normalize_by_student)
    eligible_courses_comprehensive_data['Final_Score'] = (
        (eligible_courses_comprehensive_data['Normalized_Course_Score'] * 0.4) +
        (eligible_courses_comprehensive_data['Normalized_Remaining_Courses_Weight'] * 0.4) +
        (eligible_courses_comprehensive_data['Normalized_Course_Level'] * 0.2))

    # Find Best Courses
    recommended_courses_mis_v2 = eligible_courses_comprehensive_data.groupby(['Student_ID', 'Semester']).apply(lambda group: pd.Series({'Recommended_Courses_V2': find_best_courses_v2(group)})).reset_index()
    eligible_courses_comprehensive_data = eligible_courses_comprehensive_data.merge(recommended_courses_mis_v2, on=['Student_ID', 'Semester'])

    recommended_courses = recommended_courses_mis.merge(recommended_courses_mis_v2,on=['Student_ID', 'Semester'])

    # Create summary DataFrames for eligible courses
    summary_area_of_study_eligible = eligible_courses_comprehensive_data.groupby(['Student_ID', 'AREA_OF_STUDY']).size().reset_index(name='Total_Eligible_Courses')
    summary_course_of_study_eligible = eligible_courses_comprehensive_data.groupby(['Student_ID', 'COURSE_OF_STUDY']).size().reset_index(name='Total_Eligible_Courses')
    summary_area_of_study_eligible = summary_area_of_study_eligible.pivot_table(index="Student_ID", columns="AREA_OF_STUDY", values="Total_Eligible_Courses", fill_value=0).reset_index()

    return requirements_mis_,student_progress,summary_area_of_study_taken,remaining_courses_df,latest_eligible_courses,eligible_courses_comprehensive_data,recommended_courses,summary_area_of_study_eligible

def process_data_mrkt(st_hist_data,major_data, requirements_weights_path):
    
    values_to_delete = ['FA', 'F', 'I', 'S', 'NP', 'WA']
    failed_grades = ['F','FA','NP']
    failed_data = st_hist_data[st_hist_data["GRADE"].isin(failed_grades)]
    st_hist_data = st_hist_data[~st_hist_data["GRADE"].isin(values_to_delete)]
    
    # Filtering and Sorting Data
    failed_data = failed_data[failed_data['Major'] == 'Marketing']
    failed_data = failed_data.sort_values(by=['Student_ID', 'Semester'])

    grouped_data_failed = failed_data.groupby(['Student_ID'])['Course_ID'].apply(list).reset_index()
    
    # Filtering and Sorting Data
    mrkt_data = st_hist_data[st_hist_data['Major'] == "Marketing"]
    mrkt_data = mrkt_data.sort_values(by=['Student_ID', 'Semester'])

    major = major_data["All_Courses"]
    courses_co = major_data["CO_Courses"]
    
    major["AREA_OF_STUDY"] = major["AREA_OF_STUDY"].fillna("NA")
    # Dropping records where AREA_OF_STUDY is 'N' and COURSE_OF_STUDY is 'Z'
    major_filtered = major[~((major['AREA_OF_STUDY'] == 'NA') & (major['COURSE_OF_STUDY'] == 'Z'))]
    
    major_filtered = major_filtered.copy()
    # Apply replacements directly to the specific columns to avoid SettingWithCopyWarning
    major_filtered['AREA_OF_STUDY'] = major_filtered['AREA_OF_STUDY'].replace("NA","GE")
    major_filtered['COURSE_OF_STUDY'] = major_filtered['COURSE_OF_STUDY'].replace("N","E")
    
    # Defining the major lists
    cba_majors = ['ACCOUNTING', 'INTL BUSIN', 'MANAGEMENT', 'FINANCE', 'MIS', 'MARKETING2']
    # Filtering the DataFrame based on each major list
    df_cba = major_filtered[(major_filtered['Major'].isin(cba_majors)) & (major_filtered['COURSE_OF_STUDY'].isin(['R', 'RE']))]
    
    list_conditions = ['-', 'ONE_COURSE']

    cba_list = df_cba[df_cba['Condition'].isin(list_conditions)]
    cba_special_cases = df_cba[~df_cba['Condition'].isin(list_conditions)]
    cba_co = courses_co[courses_co['Major'].isin(cba_majors)]
    
    mrkt_list = cba_list[cba_list["Major"] == "MARKETING2"]
    mrkt_special_cases = cba_special_cases[cba_special_cases["Major"] == "MARKETING2"]
    mrkt_co = cba_co[cba_co["Major"] == "MARKETING2"]

    # Process 'REQUISITES_LIST'
    mrkt_co = mrkt_co.copy()
    mrkt_co.loc[:, 'REQUISITES_LIST'] = mrkt_co['REQUISITES_LIST'].apply(ast.literal_eval)

    # CBA Courses
    cba_courses = major_filtered[major_filtered['Major'].isin(cba_majors)]
    courses_mrkt = cba_courses[cba_courses["Major"] == "MARKETING2"]
    
    grouped_data_mrkt = mrkt_data.groupby(['Student_ID'])['Course_ID'].apply(list).reset_index()

    # Merge dataframes
    merged_df = grouped_data_failed.merge(grouped_data_mrkt, on=['Student_ID'], how='outer', suffixes=('_failed', '_all'))
    # Replace NaN with empty lists to avoid errors
    merged_df['Course_ID_all'] = merged_df['Course_ID_all'].apply(lambda x: x if isinstance(x, list) else [])
    merged_df['Course_ID_failed'] = merged_df['Course_ID_failed'].apply(lambda x: x if isinstance(x, list) else [])

    merged_df['Failed_Courses'] = merged_df.apply(
        lambda row: list(set(row['Course_ID_failed']) - set(row['Course_ID_all'])),
        axis=1)
    # Keep only relevant columns
    merged_df = merged_df[['Student_ID', 'Failed_Courses']]

    # Extract Accounting specific requirements and weights from respective DataFrames
    requirements_df = pd.read_excel(requirements_weights_path,sheet_name="requirements")
    weights_df = pd.read_excel(requirements_weights_path,sheet_name="weights")
    requirements_mrkt = requirements_df[requirements_df["Major"] == "Marketing"]
    requirements_mrkt_ = requirements_mrkt.pivot_table(index="Major",columns="AREA_OF_STUDY",values ='Required_Courses' ,aggfunc='sum',fill_value=0).reset_index()
    weights_mrkt = weights_df[weights_df["Major"] == "Marketing"]

    student_courses = mrkt_data[["Student_ID", "Course_ID"]]

    # Map AREA_OF_STUDY and COURSE_OF_STUDY to mrkt_data
    student_courses = student_courses.merge(courses_mrkt[['Course_ID', 'AREA_OF_STUDY', 'COURSE_OF_STUDY', "Course_Level"]],
                                            on='Course_ID', how='left').drop_duplicates()

    # Create summary DataFrames for taken courses
    student_progress = student_courses.groupby(['Student_ID', 'AREA_OF_STUDY']).size().reset_index(name='Total_Taken_Courses')
    student_progress = student_progress.merge(requirements_mrkt, on='AREA_OF_STUDY', how='left')
    student_progress["Remaining_Courses"] = student_progress["Required_Courses"] - student_progress["Total_Taken_Courses"]
    student_progress["Remaining_Courses"] = student_progress["Remaining_Courses"].apply(lambda x: max(x, 0))

    free_elective_taken_counts = student_courses[(student_courses['AREA_OF_STUDY'] == "GE") & (student_courses['COURSE_OF_STUDY'] == "E")].groupby('Student_ID').size().reset_index(name='Total_Free_Electives_Taken')

    # Update progress by including the free elective data
    student_progress["Student_Progress"] = (student_progress["Total_Taken_Courses"] / student_progress["Required_Courses"]) * 100
    student_progress["Student_Progress"].replace([np.inf, -np.inf], 100, inplace=True)

    summary_area_of_study_taken = student_progress.pivot_table(index="Student_ID", columns="AREA_OF_STUDY", values="Total_Taken_Courses", fill_value=0)
    summary_area_of_study_taken = summary_area_of_study_taken.merge(free_elective_taken_counts, on="Student_ID", how="left").fillna(0).rename(columns={"Total_Free_Electives_Taken": "FE"})

    # Create a copy of summary_area_of_study_taken to work on remaining courses calculation
    remaining_courses_df = summary_area_of_study_taken.copy()

    # Loop through each AREA_OF_STUDY and calculate remaining courses by subtracting from the requirements
    for column in remaining_courses_df.columns:
        if column in requirements_mrkt['AREA_OF_STUDY'].values:
            required_courses = requirements_mrkt.loc[requirements_mrkt['AREA_OF_STUDY'] == column, 'Required_Courses'].values[0]
            remaining_courses_df[column] = required_courses - remaining_courses_df[column]
            remaining_courses_df[column] = remaining_courses_df[column].clip(lower=0)

    # Calculate weighted remaining courses
    weighted_remaining_courses_df = remaining_courses_df.copy()
    for column in weighted_remaining_courses_df.columns:
        if column in weights_mrkt['AREA_OF_STUDY'].values:
            weight_value = weights_mrkt.loc[weights_mrkt['AREA_OF_STUDY'] == column, 'Weight'].values[0]
            weighted_remaining_courses_df[column] = weighted_remaining_courses_df[column] * weight_value

    # Prepare weighted remaining courses for merge
    weighted_remaining_courses_df = weighted_remaining_courses_df.reset_index().melt(id_vars=['Student_ID'],
                                                                                      var_name='AREA_OF_STUDY',
                                                                                      value_name='Remaining_Courses_Weight_Score')
    weighted_remaining_courses_df = weighted_remaining_courses_df[weighted_remaining_courses_df["AREA_OF_STUDY"] != "index"]

    # Eligibility Calculation for Standard and Special Cases
    prerequisites_mrkt = mrkt_list.set_index('Course_ID')['REQUISITES_LIST'].apply(eval).to_dict()
    prerequisites_special_mrkt = mrkt_special_cases.set_index('Course_ID')['REQUISITES_LIST'].apply(eval).to_dict()
    conditions_mrkt = mrkt_special_cases.set_index('Course_ID')['Condition'].to_dict()

    final_results_mrkt = []  # Standard eligibility results
    final_results_special_mrkt = []  # Special eligibility results

    for student_id, group in mrkt_data.groupby('Student_ID'):
        cumulative_courses = set()
        for semester, semester_group in group.groupby('Semester'):
            taken_courses = set(semester_group['Course_ID'].tolist())
            cumulative_courses.update(taken_courses)

            # Determine Standard Eligible Courses
            student_info = semester_group.iloc[0].to_dict()
            eligible_courses = {course for course in prerequisites_mrkt.keys() if all(req in cumulative_courses for req in prerequisites_mrkt[course])}
            final_results_mrkt.append({
                'Student_ID': student_id,
                'Semester': semester,
                'Major': student_info['Major'],
                'College': student_info['College'],
                'Program': student_info['Program'],
                'Passed Credits': student_info['Passed Credits'],
                'Student_Level': student_info['Student_Level'],
                'Eligible_Courses': list(eligible_courses - cumulative_courses)
            })

            # Determine Special Eligible Courses
            special_eligible_courses = {
                course for course in prerequisites_special_mrkt.keys()
                if is_eligible_special_mrkt(course, cumulative_courses, student_info, prerequisites_special_mrkt, conditions_mrkt)
            }
            final_results_special_mrkt.append({
                'Student_ID': student_id,
                'Semester': semester,
                'Major': student_info['Major'],
                'College': student_info['College'],
                'Program': student_info['Program'],
                'Passed Credits': student_info['Passed Credits'],
                'Student_Level': student_info['Student_Level'],
                'Eligible_Courses': list(special_eligible_courses - cumulative_courses)
            })

    # Convert Results to DataFrames
    final_results_df_mrkt = pd.DataFrame(final_results_mrkt)
    final_results_special_df_mrkt = pd.DataFrame(final_results_special_mrkt)
    
    # Combine Eligible Courses from Both DataFrames
    combined_mrkt_list = combine_eligible_courses(final_results_df_mrkt, final_results_special_df_mrkt)
    # Find Course Combinations for Co-requisites
    combined_mrkt_list = combined_mrkt_list.apply(create_combined_courses, axis=1, co=mrkt_co)
    latest_eligible_courses = combined_mrkt_list.sort_values(by='Semester', ascending=False)
    latest_eligible_courses = latest_eligible_courses.groupby('Student_ID').first().reset_index()
    latest_eligible_courses = latest_eligible_courses.merge(grouped_data_mrkt,on = "Student_ID",how = "inner")
    latest_eligible_courses["Eligible_Courses_CO"] = latest_eligible_courses.apply(remove_matches, axis=1)
    latest_eligible_courses = latest_eligible_courses.apply(process_row, axis=1)
    latest_eligible_courses.drop(columns=["Course_ID"], inplace=True)

    latest_eligible_courses = latest_eligible_courses.merge(merged_df, on='Student_ID', how='outer')
    latest_eligible_courses['Failed_Courses'] = latest_eligible_courses['Failed_Courses'].apply(lambda x: x if isinstance(x, list) else [])
    latest_eligible_courses['Eligible_Courses_CO'] = latest_eligible_courses['Eligible_Courses_CO'].apply(lambda x: x if isinstance(x, list) else [])
    latest_eligible_courses['Eligible_Courses_CO'] = latest_eligible_courses.apply(
        lambda row: list(set(row['Eligible_Courses_CO']) | (set(row['Failed_Courses']) - set(row['Eligible_Courses_CO']))),axis=1)
    latest_eligible_courses = latest_eligible_courses.drop(columns=['Failed_Courses'])

    latest_info_failed = failed_data.loc[failed_data.groupby("Student_ID")["Semester"].idxmax()]
    missing_semester_df = latest_eligible_courses[latest_eligible_courses['Semester'].isna()]
    latest_eligible_courses.dropna(inplace=True)
    columns_to_fill = ['Semester', 'Major', 'College', 'Program', 'Passed Credits', 'Student_Level']

    for col in columns_to_fill:
        missing_semester_df.loc[missing_semester_df[col].isna(), col] = missing_semester_df.loc[
            missing_semester_df[col].isna(), 'Student_ID'
        ].map(latest_info_failed.set_index('Student_ID')[col])

    columns_to_convert = ['Semester', 'Student_Level', 'Passed Credits']
    for col in columns_to_convert:
        latest_eligible_courses.loc[:, col] = pd.to_numeric(latest_eligible_courses[col], errors='coerce').astype('Int64')
        
    latest_eligible_courses = pd.concat([latest_eligible_courses, missing_semester_df], ignore_index=True)

    max_semester_index = mrkt_data.groupby('Student_ID')['Semester'].idxmax()
    max_semester_data = mrkt_data.loc[max_semester_index, ['Student_ID', 'Semester']]

    last_semester_courses = pd.merge(max_semester_data, mrkt_data, on=['Student_ID', 'Semester'])
    eng097_fpu_students = last_semester_courses[last_semester_courses['Course_ID'] == 'ENGL097']
    # Target course list
    target_courses = ['ENGL098', 'MATH094', 'MATH095', 'MATH096', 'MATH098', 'MATH100', 'MATH111', 'MATH120', 'MATH121', 'MATH131', 'MATH140']

    eng097_fpu_students_eligible = latest_eligible_courses[latest_eligible_courses['Student_ID']
                                                       .isin(eng097_fpu_students['Student_ID'])].copy()
    eng097_fpu_students_eligible.loc[:, 'Eligible_Courses_CO'] = eng097_fpu_students_eligible['Eligible_Courses_CO'].apply(
    lambda courses: [course for course in courses if course in target_courses])

    latest_eligible_courses = latest_eligible_courses.merge(
    eng097_fpu_students_eligible[['Student_ID', 'Eligible_Courses_CO']],  # Relevant columns from filtered_students
    on='Student_ID',
    how='left',  # Keep all rows in students_df
    suffixes=('', '_updated'))  # Suffix to differentiate new column)

    latest_eligible_courses['Eligible_Courses_CO'] = latest_eligible_courses['Eligible_Courses_CO_updated'].combine_first(latest_eligible_courses['Eligible_Courses_CO'])
    latest_eligible_courses = latest_eligible_courses.drop(columns=['Eligible_Courses_CO_updated'])
    latest_eligible_courses = latest_eligible_courses.merge(grouped_data_mrkt,on = "Student_ID",how = "outer")
    latest_eligible_courses['Course_ID'] = latest_eligible_courses['Course_ID'].apply(lambda x: x if isinstance(x, list) else [])
    latest_eligible_courses = latest_eligible_courses.apply(process_row, axis=1)
    latest_eligible_courses.drop(columns=["Course_ID"], inplace=True)

    # Exploding DataFrame and mapping course details
    eligible_courses_comprehensive_data = latest_eligible_courses.explode("Eligible_Courses_CO")
    eligible_courses_comprehensive_data = eligible_courses_comprehensive_data.merge(courses_mrkt[['Course_ID', 'AREA_OF_STUDY', 'COURSE_OF_STUDY', 'Course_Level']],
                                                                                    left_on='Eligible_Courses_CO', right_on='Course_ID', how='left').drop(columns="Course_ID")
    eligible_courses_comprehensive_data['Eligible_Courses_CO'] = eligible_courses_comprehensive_data['Eligible_Courses_CO'].apply(lambda x: x if isinstance(x, list) else ([] if pd.isna(x) else [x]))

    # Find Additional Eligibilities
    eligible_courses_comprehensive_data['Future_Eligible_Courses_List'] = eligible_courses_comprehensive_data.apply(lambda row: find_additional_eligibilities(row['Eligible_Courses_CO'], set(row['Eligible_Courses_CO']), prerequisites_mrkt), axis=1)
    eligible_courses_per_student = eligible_courses_comprehensive_data.groupby('Student_ID')['Eligible_Courses_CO'].agg(lambda x: list(set([item for sublist in x for item in sublist if isinstance(sublist, list)]))).reset_index()

    # Merge aggregated list back to the comprehensive DataFrame
    eligible_courses_comprehensive_data = eligible_courses_comprehensive_data.merge(eligible_courses_per_student.rename(columns={'Eligible_Courses_CO': 'Eligible_Courses_List_All'}), on='Student_ID', how='left')

    # Filter matching courses from future eligible lists
    eligible_courses_comprehensive_data['Future_Eligible_Courses_List'] = eligible_courses_comprehensive_data.apply(lambda row: [course for course in row['Future_Eligible_Courses_List'] if course not in row['Eligible_Courses_List_All']], axis=1)
    eligible_courses_comprehensive_data['Total_Future_Eligible_Courses_List'] = eligible_courses_comprehensive_data['Future_Eligible_Courses_List'].apply(len)

    # Special eligibility courses
    eligible_courses_comprehensive_data['Future_Eligible_Courses_Special'] = eligible_courses_comprehensive_data.apply(lambda row: find_additional_eligibilities_special(row['Eligible_Courses_CO'], set(row['Eligible_Courses_CO']), row, prerequisites_special_mrkt, conditions_mrkt, is_eligible_special_mrkt_), axis=1)
    eligible_courses_comprehensive_data['Future_Eligible_Courses_Special'] = eligible_courses_comprehensive_data.apply(lambda row: [course for course in row['Future_Eligible_Courses_Special'] if course not in row['Eligible_Courses_List_All']], axis=1)
    eligible_courses_comprehensive_data['Total_Future_Eligible_Courses_Special'] = eligible_courses_comprehensive_data['Future_Eligible_Courses_Special'].apply(len)

    # Combine Future Eligible Courses and calculate the score
    eligible_courses_comprehensive_data["Future_Eligible_Courses"] = eligible_courses_comprehensive_data["Future_Eligible_Courses_List"] + eligible_courses_comprehensive_data["Future_Eligible_Courses_Special"]
    eligible_courses_comprehensive_data['Course_Score'] = eligible_courses_comprehensive_data['Future_Eligible_Courses'].apply(len)

    # Find Best Courses
    recommended_courses_mrkt = eligible_courses_comprehensive_data.groupby(['Student_ID', 'Semester']).apply(lambda group: pd.Series({'Recommended_Courses': find_best_courses(group)})).reset_index()


    eligible_courses_comprehensive_data = eligible_courses_comprehensive_data.merge(recommended_courses_mrkt, on=['Student_ID', 'Semester'])
    eligible_courses_comprehensive_data = eligible_courses_comprehensive_data.merge(weighted_remaining_courses_df, on=['Student_ID', 'AREA_OF_STUDY'], how='left')


    eligible_courses_comprehensive_data = eligible_courses_comprehensive_data.groupby('Student_ID', group_keys=False).apply(normalize_by_student)
    eligible_courses_comprehensive_data['Final_Score'] = (
        (eligible_courses_comprehensive_data['Normalized_Course_Score'] * 0.4) +
        (eligible_courses_comprehensive_data['Normalized_Remaining_Courses_Weight'] * 0.4) +
        (eligible_courses_comprehensive_data['Normalized_Course_Level'] * 0.2))

    # Find Best Courses
    recommended_courses_mrkt_v2 = eligible_courses_comprehensive_data.groupby(['Student_ID', 'Semester']).apply(lambda group: pd.Series({'Recommended_Courses_V2': find_best_courses_v2(group)})).reset_index()
    eligible_courses_comprehensive_data = eligible_courses_comprehensive_data.merge(recommended_courses_mrkt_v2, on=['Student_ID', 'Semester'])

    recommended_courses = recommended_courses_mrkt.merge(recommended_courses_mrkt_v2,on=['Student_ID', 'Semester'])

    # Create summary DataFrames for eligible courses
    summary_area_of_study_eligible = eligible_courses_comprehensive_data.groupby(['Student_ID', 'AREA_OF_STUDY']).size().reset_index(name='Total_Eligible_Courses')
    summary_course_of_study_eligible = eligible_courses_comprehensive_data.groupby(['Student_ID', 'COURSE_OF_STUDY']).size().reset_index(name='Total_Eligible_Courses')
    summary_area_of_study_eligible = summary_area_of_study_eligible.pivot_table(index="Student_ID", columns="AREA_OF_STUDY", values="Total_Eligible_Courses", fill_value=0).reset_index()

    return requirements_mrkt_,student_progress,summary_area_of_study_taken,remaining_courses_df,latest_eligible_courses,eligible_courses_comprehensive_data,recommended_courses,summary_area_of_study_eligible

def process_data_fin(st_hist_data,major_data, requirements_weights_path):
    
    values_to_delete = ['FA', 'F', 'I', 'S', 'NP', 'WA']
    failed_grades = ['F','FA','NP']
    failed_data = st_hist_data[st_hist_data["GRADE"].isin(failed_grades)]
    st_hist_data = st_hist_data[~st_hist_data["GRADE"].isin(values_to_delete)]
    
    # Filtering and Sorting Data
    failed_data = failed_data[failed_data['Major'] == 'Finance']
    failed_data = failed_data.sort_values(by=['Student_ID', 'Semester'])

    grouped_data_failed = failed_data.groupby(['Student_ID'])['Course_ID'].apply(list).reset_index()
    
    # Filtering and Sorting Data
    fin_data = st_hist_data[st_hist_data['Major'] == "Finance"]
    fin_data = fin_data.sort_values(by=['Student_ID', 'Semester'])

    major = major_data["All_Courses"]
    courses_co = major_data["CO_Courses"]
    
    major["AREA_OF_STUDY"] = major["AREA_OF_STUDY"].fillna("NA")
    # Dropping records where AREA_OF_STUDY is 'N' and COURSE_OF_STUDY is 'Z'
    major_filtered = major[~((major['AREA_OF_STUDY'] == 'NA') & (major['COURSE_OF_STUDY'] == 'Z'))]
    
    major_filtered = major_filtered.copy()
    # Apply replacements directly to the specific columns to avoid SettingWithCopyWarning
    major_filtered['AREA_OF_STUDY'] = major_filtered['AREA_OF_STUDY'].replace("NA","GE")
    major_filtered['COURSE_OF_STUDY'] = major_filtered['COURSE_OF_STUDY'].replace("N","E")
    
    # Defining the major lists
    cba_majors = ['ACCOUNTING', 'INTL BUSIN', 'MANAGEMENT', 'FINANCE', 'MIS', 'MARKETING2']
    # Filtering the DataFrame based on each major list
    df_cba = major_filtered[(major_filtered['Major'].isin(cba_majors)) & (major_filtered['COURSE_OF_STUDY'].isin(['R', 'RE']))]
    
    list_conditions = ['-', 'ONE_COURSE']

    cba_list = df_cba[df_cba['Condition'].isin(list_conditions)]
    cba_special_cases = df_cba[~df_cba['Condition'].isin(list_conditions)]
    cba_co = courses_co[courses_co['Major'].isin(cba_majors)]
    
    fin_list = cba_list[cba_list["Major"] == "FINANCE"]
    fin_special_cases = cba_special_cases[cba_special_cases["Major"] == "FINANCE"]
    fin_co = cba_co[cba_co["Major"] == "FINANCE"]

    # Process 'REQUISITES_LIST'
    fin_co = fin_co.copy()
    fin_co.loc[:, 'REQUISITES_LIST'] = fin_co['REQUISITES_LIST'].apply(ast.literal_eval)

    # CBA Courses
    cba_courses = major_filtered[major_filtered['Major'].isin(cba_majors)]
    courses_fin = cba_courses[cba_courses["Major"] == "FINANCE"]
    
    grouped_data_fin = fin_data.groupby(['Student_ID'])['Course_ID'].apply(list).reset_index()

    # Merge dataframes
    merged_df = grouped_data_failed.merge(grouped_data_fin, on=['Student_ID'], how='outer', suffixes=('_failed', '_all'))
    # Replace NaN with empty lists to avoid errors
    merged_df['Course_ID_all'] = merged_df['Course_ID_all'].apply(lambda x: x if isinstance(x, list) else [])
    merged_df['Course_ID_failed'] = merged_df['Course_ID_failed'].apply(lambda x: x if isinstance(x, list) else [])

    merged_df['Failed_Courses'] = merged_df.apply(
        lambda row: list(set(row['Course_ID_failed']) - set(row['Course_ID_all'])),
        axis=1)
    # Keep only relevant columns
    merged_df = merged_df[['Student_ID', 'Failed_Courses']]

    # Extract Accounting specific requirements and weights from respective DataFrames
    requirements_df = pd.read_excel(requirements_weights_path,sheet_name="requirements")
    weights_df = pd.read_excel(requirements_weights_path,sheet_name="weights")
    requirements_fin = requirements_df[requirements_df["Major"] == "Finance"]
    requirements_fin_ = requirements_fin.pivot_table(index="Major",columns="AREA_OF_STUDY",values ='Required_Courses' ,aggfunc='sum',fill_value=0).reset_index()
    weights_fin = weights_df[weights_df["Major"] == "Finance"]

    student_courses = fin_data[["Student_ID", "Course_ID"]]

    # Map AREA_OF_STUDY and COURSE_OF_STUDY to fin_data
    student_courses = student_courses.merge(courses_fin[['Course_ID', 'AREA_OF_STUDY', 'COURSE_OF_STUDY', "Course_Level"]],
                                            on='Course_ID', how='left').drop_duplicates()

    # Create summary DataFrames for taken courses
    student_progress = student_courses.groupby(['Student_ID', 'AREA_OF_STUDY']).size().reset_index(name='Total_Taken_Courses')
    student_progress = student_progress.merge(requirements_fin, on='AREA_OF_STUDY', how='left')
    student_progress["Remaining_Courses"] = student_progress["Required_Courses"] - student_progress["Total_Taken_Courses"]
    student_progress["Remaining_Courses"] = student_progress["Remaining_Courses"].apply(lambda x: max(x, 0))

    free_elective_taken_counts = student_courses[(student_courses['AREA_OF_STUDY'] == "GE") & (student_courses['COURSE_OF_STUDY'] == "E")].groupby('Student_ID').size().reset_index(name='Total_Free_Electives_Taken')

    # Update progress by including the free elective data
    student_progress["Student_Progress"] = (student_progress["Total_Taken_Courses"] / student_progress["Required_Courses"]) * 100
    student_progress["Student_Progress"].replace([np.inf, -np.inf], 100, inplace=True)

    summary_area_of_study_taken = student_progress.pivot_table(index="Student_ID", columns="AREA_OF_STUDY", values="Total_Taken_Courses", fill_value=0)
    summary_area_of_study_taken = summary_area_of_study_taken.merge(free_elective_taken_counts, on="Student_ID", how="left").fillna(0).rename(columns={"Total_Free_Electives_Taken": "FE"})

    # Create a copy of summary_area_of_study_taken to work on remaining courses calculation
    remaining_courses_df = summary_area_of_study_taken.copy()

    # Loop through each AREA_OF_STUDY and calculate remaining courses by subtracting from the requirements
    for column in remaining_courses_df.columns:
        if column in requirements_fin['AREA_OF_STUDY'].values:
            required_courses = requirements_fin.loc[requirements_fin['AREA_OF_STUDY'] == column, 'Required_Courses'].values[0]
            remaining_courses_df[column] = required_courses - remaining_courses_df[column]
            remaining_courses_df[column] = remaining_courses_df[column].clip(lower=0)

    # Calculate weighted remaining courses
    weighted_remaining_courses_df = remaining_courses_df.copy()
    for column in weighted_remaining_courses_df.columns:
        if column in weights_fin['AREA_OF_STUDY'].values:
            weight_value = weights_fin.loc[weights_fin['AREA_OF_STUDY'] == column, 'Weight'].values[0]
            weighted_remaining_courses_df[column] = weighted_remaining_courses_df[column] * weight_value

    # Prepare weighted remaining courses for merge
    weighted_remaining_courses_df = weighted_remaining_courses_df.reset_index().melt(id_vars=['Student_ID'],
                                                                                      var_name='AREA_OF_STUDY',
                                                                                      value_name='Remaining_Courses_Weight_Score')
    weighted_remaining_courses_df = weighted_remaining_courses_df[weighted_remaining_courses_df["AREA_OF_STUDY"] != "index"]

    # Eligibility Calculation for Standard and Special Cases
    prerequisites_fin = fin_list.set_index('Course_ID')['REQUISITES_LIST'].apply(eval).to_dict()
    prerequisites_special_fin = fin_special_cases.set_index('Course_ID')['REQUISITES_LIST'].apply(eval).to_dict()
    conditions_fin = fin_special_cases.set_index('Course_ID')['Condition'].to_dict()

    final_results_fin = []  # Standard eligibility results
    final_results_special_fin = []  # Special eligibility results

    for student_id, group in fin_data.groupby('Student_ID'):
        cumulative_courses = set()
        for semester, semester_group in group.groupby('Semester'):
            taken_courses = set(semester_group['Course_ID'].tolist())
            cumulative_courses.update(taken_courses)

            # Determine Standard Eligible Courses
            student_info = semester_group.iloc[0].to_dict()
            eligible_courses = {course for course in prerequisites_fin.keys() if all(req in cumulative_courses for req in prerequisites_fin[course])}
            final_results_fin.append({
                'Student_ID': student_id,
                'Semester': semester,
                'Major': student_info['Major'],
                'College': student_info['College'],
                'Program': student_info['Program'],
                'Passed Credits': student_info['Passed Credits'],
                'Student_Level': student_info['Student_Level'],
                'Eligible_Courses': list(eligible_courses - cumulative_courses)
            })

            # Determine Special Eligible Courses
            special_eligible_courses = {
                course for course in prerequisites_special_fin.keys()
                if is_eligible_special_fin(course, cumulative_courses, student_info, prerequisites_special_fin, conditions_fin)
            }
            final_results_special_fin.append({
                'Student_ID': student_id,
                'Semester': semester,
                'Major': student_info['Major'],
                'College': student_info['College'],
                'Program': student_info['Program'],
                'Passed Credits': student_info['Passed Credits'],
                'Student_Level': student_info['Student_Level'],
                'Eligible_Courses': list(special_eligible_courses - cumulative_courses)
            })

    # Convert Results to DataFrames
    final_results_df_fin = pd.DataFrame(final_results_fin)
    final_results_special_df_fin = pd.DataFrame(final_results_special_fin)
    
    # Combine Eligible Courses from Both DataFrames
    combined_fin_list = combine_eligible_courses(final_results_df_fin, final_results_special_df_fin)
    # Find Course Combinations for Co-requisites
    combined_fin_list = combined_fin_list.apply(create_combined_courses, axis=1, co=fin_co)
    latest_eligible_courses = combined_fin_list.sort_values(by='Semester', ascending=False)
    latest_eligible_courses = latest_eligible_courses.groupby('Student_ID').first().reset_index()
    latest_eligible_courses = latest_eligible_courses.merge(grouped_data_fin,on = "Student_ID",how = "inner")
    latest_eligible_courses["Eligible_Courses_CO"] = latest_eligible_courses.apply(remove_matches, axis=1)
    latest_eligible_courses = latest_eligible_courses.apply(process_row, axis=1)
    latest_eligible_courses.drop(columns=["Course_ID"], inplace=True)

    latest_eligible_courses = latest_eligible_courses.merge(merged_df, on='Student_ID', how='outer')
    latest_eligible_courses['Failed_Courses'] = latest_eligible_courses['Failed_Courses'].apply(lambda x: x if isinstance(x, list) else [])
    latest_eligible_courses['Eligible_Courses_CO'] = latest_eligible_courses['Eligible_Courses_CO'].apply(lambda x: x if isinstance(x, list) else [])
    latest_eligible_courses['Eligible_Courses_CO'] = latest_eligible_courses.apply(
        lambda row: list(set(row['Eligible_Courses_CO']) | (set(row['Failed_Courses']) - set(row['Eligible_Courses_CO']))),axis=1)
    latest_eligible_courses = latest_eligible_courses.drop(columns=['Failed_Courses'])

    latest_info_failed = failed_data.loc[failed_data.groupby("Student_ID")["Semester"].idxmax()]
    missing_semester_df = latest_eligible_courses[latest_eligible_courses['Semester'].isna()]
    latest_eligible_courses.dropna(inplace=True)
    columns_to_fill = ['Semester', 'Major', 'College', 'Program', 'Passed Credits', 'Student_Level']

    for col in columns_to_fill:
        missing_semester_df.loc[missing_semester_df[col].isna(), col] = missing_semester_df.loc[
            missing_semester_df[col].isna(), 'Student_ID'
        ].map(latest_info_failed.set_index('Student_ID')[col])

    columns_to_convert = ['Semester', 'Student_Level', 'Passed Credits']
    for col in columns_to_convert:
        latest_eligible_courses.loc[:, col] = pd.to_numeric(latest_eligible_courses[col], errors='coerce').astype('Int64')
        
    latest_eligible_courses = pd.concat([latest_eligible_courses, missing_semester_df], ignore_index=True)

    max_semester_index = fin_data.groupby('Student_ID')['Semester'].idxmax()
    max_semester_data = fin_data.loc[max_semester_index, ['Student_ID', 'Semester']]

    last_semester_courses = pd.merge(max_semester_data, fin_data, on=['Student_ID', 'Semester'])
    eng097_fpu_students = last_semester_courses[last_semester_courses['Course_ID'] == 'ENGL097']
    # Target course list
    target_courses = ['ENGL098', 'MATH094', 'MATH095', 'MATH096', 'MATH098', 'MATH100', 'MATH111', 'MATH120', 'MATH121', 'MATH131', 'MATH140']

    eng097_fpu_students_eligible = latest_eligible_courses[latest_eligible_courses['Student_ID']
                                                       .isin(eng097_fpu_students['Student_ID'])].copy()
    eng097_fpu_students_eligible.loc[:, 'Eligible_Courses_CO'] = eng097_fpu_students_eligible['Eligible_Courses_CO'].apply(
    lambda courses: [course for course in courses if course in target_courses])

    latest_eligible_courses = latest_eligible_courses.merge(
    eng097_fpu_students_eligible[['Student_ID', 'Eligible_Courses_CO']],  # Relevant columns from filtered_students
    on='Student_ID',
    how='left',  # Keep all rows in students_df
    suffixes=('', '_updated'))  # Suffix to differentiate new column)

    latest_eligible_courses['Eligible_Courses_CO'] = latest_eligible_courses['Eligible_Courses_CO_updated'].combine_first(latest_eligible_courses['Eligible_Courses_CO'])
    latest_eligible_courses = latest_eligible_courses.drop(columns=['Eligible_Courses_CO_updated'])
    latest_eligible_courses = latest_eligible_courses.merge(grouped_data_fin,on = "Student_ID",how = "outer")
    latest_eligible_courses['Course_ID'] = latest_eligible_courses['Course_ID'].apply(lambda x: x if isinstance(x, list) else [])
    latest_eligible_courses = latest_eligible_courses.apply(process_row, axis=1)
    latest_eligible_courses.drop(columns=["Course_ID"], inplace=True)

    # Exploding DataFrame and mapping course details
    eligible_courses_comprehensive_data = latest_eligible_courses.explode("Eligible_Courses_CO")
    eligible_courses_comprehensive_data = eligible_courses_comprehensive_data.merge(courses_fin[['Course_ID', 'AREA_OF_STUDY', 'COURSE_OF_STUDY', 'Course_Level']],
                                                                                    left_on='Eligible_Courses_CO', right_on='Course_ID', how='left').drop(columns="Course_ID")
    eligible_courses_comprehensive_data['Eligible_Courses_CO'] = eligible_courses_comprehensive_data['Eligible_Courses_CO'].apply(lambda x: x if isinstance(x, list) else ([] if pd.isna(x) else [x]))

    # Find Additional Eligibilities
    eligible_courses_comprehensive_data['Future_Eligible_Courses_List'] = eligible_courses_comprehensive_data.apply(lambda row: find_additional_eligibilities(row['Eligible_Courses_CO'], set(row['Eligible_Courses_CO']), prerequisites_fin), axis=1)
    eligible_courses_per_student = eligible_courses_comprehensive_data.groupby('Student_ID')['Eligible_Courses_CO'].agg(lambda x: list(set([item for sublist in x for item in sublist if isinstance(sublist, list)]))).reset_index()

    # Merge aggregated list back to the comprehensive DataFrame
    eligible_courses_comprehensive_data = eligible_courses_comprehensive_data.merge(eligible_courses_per_student.rename(columns={'Eligible_Courses_CO': 'Eligible_Courses_List_All'}), on='Student_ID', how='left')

    # Filter matching courses from future eligible lists
    eligible_courses_comprehensive_data['Future_Eligible_Courses_List'] = eligible_courses_comprehensive_data.apply(lambda row: [course for course in row['Future_Eligible_Courses_List'] if course not in row['Eligible_Courses_List_All']], axis=1)
    eligible_courses_comprehensive_data['Total_Future_Eligible_Courses_List'] = eligible_courses_comprehensive_data['Future_Eligible_Courses_List'].apply(len)

    # Special eligibility courses
    eligible_courses_comprehensive_data['Future_Eligible_Courses_Special'] = eligible_courses_comprehensive_data.apply(lambda row: find_additional_eligibilities_special(row['Eligible_Courses_CO'], set(row['Eligible_Courses_CO']), row, prerequisites_special_fin, conditions_fin, is_eligible_special_fin_), axis=1)
    eligible_courses_comprehensive_data['Future_Eligible_Courses_Special'] = eligible_courses_comprehensive_data.apply(lambda row: [course for course in row['Future_Eligible_Courses_Special'] if course not in row['Eligible_Courses_List_All']], axis=1)
    eligible_courses_comprehensive_data['Total_Future_Eligible_Courses_Special'] = eligible_courses_comprehensive_data['Future_Eligible_Courses_Special'].apply(len)

    # Combine Future Eligible Courses and calculate the score
    eligible_courses_comprehensive_data["Future_Eligible_Courses"] = eligible_courses_comprehensive_data["Future_Eligible_Courses_List"] + eligible_courses_comprehensive_data["Future_Eligible_Courses_Special"]
    eligible_courses_comprehensive_data['Course_Score'] = eligible_courses_comprehensive_data['Future_Eligible_Courses'].apply(len)

    # Find Best Courses
    recommended_courses_fin = eligible_courses_comprehensive_data.groupby(['Student_ID', 'Semester']).apply(lambda group: pd.Series({'Recommended_Courses': find_best_courses(group)})).reset_index()


    eligible_courses_comprehensive_data = eligible_courses_comprehensive_data.merge(recommended_courses_fin, on=['Student_ID', 'Semester'])
    eligible_courses_comprehensive_data = eligible_courses_comprehensive_data.merge(weighted_remaining_courses_df, on=['Student_ID', 'AREA_OF_STUDY'], how='left')


    eligible_courses_comprehensive_data = eligible_courses_comprehensive_data.groupby('Student_ID', group_keys=False).apply(normalize_by_student)
    eligible_courses_comprehensive_data['Final_Score'] = (
        (eligible_courses_comprehensive_data['Normalized_Course_Score'] * 0.4) +
        (eligible_courses_comprehensive_data['Normalized_Remaining_Courses_Weight'] * 0.4) +
        (eligible_courses_comprehensive_data['Normalized_Course_Level'] * 0.2))

    # Find Best Courses
    recommended_courses_fin_v2 = eligible_courses_comprehensive_data.groupby(['Student_ID', 'Semester']).apply(lambda group: pd.Series({'Recommended_Courses_V2': find_best_courses_v2(group)})).reset_index()
    eligible_courses_comprehensive_data = eligible_courses_comprehensive_data.merge(recommended_courses_fin_v2, on=['Student_ID', 'Semester'])

    recommended_courses = recommended_courses_fin.merge(recommended_courses_fin_v2,on=['Student_ID', 'Semester'])

    # Create summary DataFrames for eligible courses
    summary_area_of_study_eligible = eligible_courses_comprehensive_data.groupby(['Student_ID', 'AREA_OF_STUDY']).size().reset_index(name='Total_Eligible_Courses')
    summary_course_of_study_eligible = eligible_courses_comprehensive_data.groupby(['Student_ID', 'COURSE_OF_STUDY']).size().reset_index(name='Total_Eligible_Courses')
    summary_area_of_study_eligible = summary_area_of_study_eligible.pivot_table(index="Student_ID", columns="AREA_OF_STUDY", values="Total_Eligible_Courses", fill_value=0).reset_index()

    return requirements_fin_,student_progress,summary_area_of_study_taken,remaining_courses_df,latest_eligible_courses,eligible_courses_comprehensive_data,recommended_courses,summary_area_of_study_eligible

def process_data_cs(st_hist_data,major_data, requirements_weights_path):
    
    values_to_delete = ['FA', 'F', 'I', 'S', 'NP', 'WA']
    failed_grades = ['F','FA','NP']
    failed_data = st_hist_data[st_hist_data["GRADE"].isin(failed_grades)]
    st_hist_data = st_hist_data[~st_hist_data["GRADE"].isin(values_to_delete)]
    
    # Filtering and Sorting Data
    failed_data = failed_data[failed_data['Major'] == 'Computer Science']
    failed_data = failed_data.sort_values(by=['Student_ID', 'Semester'])

    grouped_data_failed = failed_data.groupby(['Student_ID'])['Course_ID'].apply(list).reset_index()

    # Filtering and Sorting Data
    cs_data = st_hist_data[st_hist_data['Major'] == "Computer Science"]
    cs_data = cs_data.sort_values(by=['Student_ID', 'Semester'])

    major = major_data["All_Courses"]
    courses_co = major_data["CO_Courses"]
    
    major["AREA_OF_STUDY"] = major["AREA_OF_STUDY"].fillna("NA")
    # Dropping records where AREA_OF_STUDY is 'N' and COURSE_OF_STUDY is 'Z'
    major_filtered = major[~((major['AREA_OF_STUDY'] == 'NA') & (major['COURSE_OF_STUDY'] == 'Z'))]
    
    major_filtered = major_filtered.copy()
    # Apply replacements directly to the specific columns to avoid SettingWithCopyWarning
    major_filtered['AREA_OF_STUDY'] = major_filtered['AREA_OF_STUDY'].replace("NA","GE")
    major_filtered['COURSE_OF_STUDY'] = major_filtered['COURSE_OF_STUDY'].replace("N","E")
    
    # Defining the major lists
    cas_majors = ['COMSCIENCE', 'ENGLISH', 'LINGUISTIC', 'LITERATURE', 'DIGITALMED', 'PR / ADV', 'VISUAL COM']
    df_cas = major_filtered[major_filtered['Major'].isin(cas_majors)]
    df_cas = df_cas[~((df_cas['AREA_OF_STUDY'] == 'GE') & (df_cas['COURSE_OF_STUDY'] == 'E'))]
    
    list_conditions = ['-', 'ONE_COURSE']

    cas_list = df_cas[df_cas['Condition'].isin(list_conditions)]
    cas_special_cases = df_cas[~df_cas['Condition'].isin(list_conditions)]
    cas_co = courses_co[courses_co['Major'].isin(cas_majors)]
    
    cs_list = cas_list[cas_list["Major"] == "COMSCIENCE"]
    cs_special_cases = cas_special_cases[cas_special_cases["Major"] == "COMSCIENCE"]
    cs_co = cas_co[cas_co["Major"] == "COMSCIENCE"]

    # Process 'REQUISITES_LIST'
    cs_co = cs_co.copy()
    cs_co.loc[:, 'REQUISITES_LIST'] = cs_co['REQUISITES_LIST'].apply(ast.literal_eval)

    # CAS Courses
    cas_courses = major_filtered[major_filtered['Major'].isin(cas_majors)]
    courses_cs = cas_courses[cas_courses["Major"] == "COMSCIENCE"]
    
    grouped_data_cs = cs_data.groupby(['Student_ID'])['Course_ID'].apply(list).reset_index()

    # Merge dataframes
    merged_df = grouped_data_failed.merge(grouped_data_cs, on=['Student_ID'], how='outer', suffixes=('_failed', '_all'))
    # Replace NaN with empty lists to avoid errors
    merged_df['Course_ID_all'] = merged_df['Course_ID_all'].apply(lambda x: x if isinstance(x, list) else [])
    merged_df['Course_ID_failed'] = merged_df['Course_ID_failed'].apply(lambda x: x if isinstance(x, list) else [])

    merged_df['Failed_Courses'] = merged_df.apply(
        lambda row: list(set(row['Course_ID_failed']) - set(row['Course_ID_all'])),
        axis=1)
    # Keep only relevant columns
    merged_df = merged_df[['Student_ID', 'Failed_Courses']]

    # Extract Accounting specific requirements and weights from respective DataFrames
    requirements_df = pd.read_excel(requirements_weights_path,sheet_name="requirements")
    weights_df = pd.read_excel(requirements_weights_path,sheet_name="weights")
    requirements_cs = requirements_df[requirements_df["Major"] == "Computer Science"]
    requirements_cs_ = requirements_cs.pivot_table(index="Major",columns="AREA_OF_STUDY",values ='Required_Courses' ,aggfunc='sum',fill_value=0).reset_index()
    weights_cs = weights_df[weights_df["Major"] == "Computer Science"]

    student_courses = cs_data[["Student_ID", "Course_ID"]]

    # Map AREA_OF_STUDY and COURSE_OF_STUDY to cs_data
    student_courses = student_courses.merge(courses_cs[['Course_ID', 'AREA_OF_STUDY', 'COURSE_OF_STUDY', "Course_Level"]],
                                            on='Course_ID', how='left').drop_duplicates()

    # Create summary DataFrames for taken courses
    student_progress = student_courses.groupby(['Student_ID', 'AREA_OF_STUDY']).size().reset_index(name='Total_Taken_Courses')
    student_progress = student_progress.merge(requirements_cs, on='AREA_OF_STUDY', how='left')
    student_progress["Remaining_Courses"] = student_progress["Required_Courses"] - student_progress["Total_Taken_Courses"]
    student_progress["Remaining_Courses"] = student_progress["Remaining_Courses"].apply(lambda x: max(x, 0))

    free_elective_taken_counts = student_courses[(student_courses['AREA_OF_STUDY'] == "GE") & (student_courses['COURSE_OF_STUDY'] == "E")].groupby('Student_ID').size().reset_index(name='Total_Free_Electives_Taken')

    # Update progress by including the free elective data
    student_progress["Student_Progress"] = (student_progress["Total_Taken_Courses"] / student_progress["Required_Courses"]) * 100
    student_progress["Student_Progress"].replace([np.inf, -np.inf], 100, inplace=True)

    summary_area_of_study_taken = student_progress.pivot_table(index="Student_ID", columns="AREA_OF_STUDY", values="Total_Taken_Courses", fill_value=0)
    summary_area_of_study_taken = summary_area_of_study_taken.merge(free_elective_taken_counts, on="Student_ID", how="left").fillna(0).rename(columns={"Total_Free_Electives_Taken": "FE"})

    # Create a copy of summary_area_of_study_taken to work on remaining courses calculation
    remaining_courses_df = summary_area_of_study_taken.copy()

    # Loop through each AREA_OF_STUDY and calculate remaining courses by subtracting from the requirements
    for column in remaining_courses_df.columns:
        if column in requirements_cs['AREA_OF_STUDY'].values:
            required_courses = requirements_cs.loc[requirements_cs['AREA_OF_STUDY'] == column, 'Required_Courses'].values[0]
            remaining_courses_df[column] = required_courses - remaining_courses_df[column]
            remaining_courses_df[column] = remaining_courses_df[column].clip(lower=0)

    # Calculate weighted remaining courses
    weighted_remaining_courses_df = remaining_courses_df.copy()
    for column in weighted_remaining_courses_df.columns:
        if column in weights_cs['AREA_OF_STUDY'].values:
            weight_value = weights_cs.loc[weights_cs['AREA_OF_STUDY'] == column, 'Weight'].values[0]
            weighted_remaining_courses_df[column] = weighted_remaining_courses_df[column] * weight_value

    # Prepare weighted remaining courses for merge
    weighted_remaining_courses_df = weighted_remaining_courses_df.reset_index().melt(id_vars=['Student_ID'],
                                                                                      var_name='AREA_OF_STUDY',
                                                                                      value_name='Remaining_Courses_Weight_Score')
    weighted_remaining_courses_df = weighted_remaining_courses_df[weighted_remaining_courses_df["AREA_OF_STUDY"] != "index"]

    # Eligibility Calculation for Standard and Special Cases
    prerequisites_cs = cs_list.set_index('Course_ID')['REQUISITES_LIST'].apply(eval).to_dict()
    prerequisites_special_cs = cs_special_cases.set_index('Course_ID')['REQUISITES_LIST'].apply(eval).to_dict()
    conditions_cs = cs_special_cases.set_index('Course_ID')['Condition'].to_dict()

    final_results_cs = []  # Standard eligibility results
    final_results_special_cs = []  # Special eligibility results

    for student_id, group in cs_data.groupby('Student_ID'):
        cumulative_courses = set()
        for semester, semester_group in group.groupby('Semester'):
            taken_courses = set(semester_group['Course_ID'].tolist())
            cumulative_courses.update(taken_courses)

            # Determine Standard Eligible Courses
            student_info = semester_group.iloc[0].to_dict()
            eligible_courses = {course for course in prerequisites_cs.keys() if all(req in cumulative_courses for req in prerequisites_cs[course])}
            final_results_cs.append({
                'Student_ID': student_id,
                'Semester': semester,
                'Major': student_info['Major'],
                'College': student_info['College'],
                'Program': student_info['Program'],
                'Passed Credits': student_info['Passed Credits'],
                'Student_Level': student_info['Student_Level'],
                'Eligible_Courses': list(eligible_courses - cumulative_courses)
            })

            # Determine Special Eligible Courses
            special_eligible_courses = {
                course for course in prerequisites_special_cs.keys()
                if is_eligible_special_cs(course, cumulative_courses, student_info, prerequisites_special_cs, conditions_cs)
            }
            final_results_special_cs.append({
                'Student_ID': student_id,
                'Semester': semester,
                'Major': student_info['Major'],
                'College': student_info['College'],
                'Program': student_info['Program'],
                'Passed Credits': student_info['Passed Credits'],
                'Student_Level': student_info['Student_Level'],
                'Eligible_Courses': list(special_eligible_courses - cumulative_courses)
            })

    # Convert Results to DataFrames
    final_results_df_cs = pd.DataFrame(final_results_cs)
    final_results_special_df_cs = pd.DataFrame(final_results_special_cs)
    
    # Combine Eligible Courses from Both DataFrames
    combined_cs_list = combine_eligible_courses(final_results_df_cs, final_results_special_df_cs)
    # Find Course Combinations for Co-requisites
    combined_cs_list = combined_cs_list.apply(create_combined_courses, axis=1, co=cs_co)
    latest_eligible_courses = combined_cs_list.sort_values(by='Semester', ascending=False)
    latest_eligible_courses = latest_eligible_courses.groupby('Student_ID').first().reset_index()
    latest_eligible_courses = latest_eligible_courses.merge(grouped_data_cs,on = "Student_ID",how = "inner")
    latest_eligible_courses["Eligible_Courses_CO"] = latest_eligible_courses.apply(remove_matches, axis=1)
    latest_eligible_courses = latest_eligible_courses.apply(process_row, axis=1)
    latest_eligible_courses.drop(columns=["Course_ID"], inplace=True)

    latest_eligible_courses = latest_eligible_courses.merge(merged_df, on='Student_ID', how='outer')
    latest_eligible_courses['Failed_Courses'] = latest_eligible_courses['Failed_Courses'].apply(lambda x: x if isinstance(x, list) else [])
    latest_eligible_courses['Eligible_Courses_CO'] = latest_eligible_courses['Eligible_Courses_CO'].apply(lambda x: x if isinstance(x, list) else [])
    latest_eligible_courses['Eligible_Courses_CO'] = latest_eligible_courses.apply(
        lambda row: list(set(row['Eligible_Courses_CO']) | (set(row['Failed_Courses']) - set(row['Eligible_Courses_CO']))),axis=1)
    latest_eligible_courses = latest_eligible_courses.drop(columns=['Failed_Courses'])

    latest_info_failed = failed_data.loc[failed_data.groupby("Student_ID")["Semester"].idxmax()]
    missing_semester_df = latest_eligible_courses[latest_eligible_courses['Semester'].isna()]
    latest_eligible_courses.dropna(inplace=True)
    columns_to_fill = ['Semester', 'Major', 'College', 'Program', 'Passed Credits', 'Student_Level']

    for col in columns_to_fill:
        missing_semester_df.loc[missing_semester_df[col].isna(), col] = missing_semester_df.loc[
            missing_semester_df[col].isna(), 'Student_ID'
        ].map(latest_info_failed.set_index('Student_ID')[col])

    columns_to_convert = ['Semester', 'Student_Level', 'Passed Credits']
    for col in columns_to_convert:
        latest_eligible_courses.loc[:, col] = pd.to_numeric(latest_eligible_courses[col], errors='coerce').astype('Int64')
        
    latest_eligible_courses = pd.concat([latest_eligible_courses, missing_semester_df], ignore_index=True)

    max_semester_index = cs_data.groupby('Student_ID')['Semester'].idxmax()
    max_semester_data = cs_data.loc[max_semester_index, ['Student_ID', 'Semester']]

    last_semester_courses = pd.merge(max_semester_data, cs_data, on=['Student_ID', 'Semester'])
    eng097_fpu_students = last_semester_courses[last_semester_courses['Course_ID'] == 'ENGL097']
    # Target course list
    target_courses = ['ENGL098', 'MATH094', 'MATH095', 'MATH096', 'MATH098', 'MATH100', 'MATH111', 'MATH120', 'MATH121', 'MATH131', 'MATH140']

    eng097_fpu_students_eligible = latest_eligible_courses[latest_eligible_courses['Student_ID']
                                                       .isin(eng097_fpu_students['Student_ID'])].copy()
    eng097_fpu_students_eligible.loc[:, 'Eligible_Courses_CO'] = eng097_fpu_students_eligible['Eligible_Courses_CO'].apply(
    lambda courses: [course for course in courses if course in target_courses])

    latest_eligible_courses = latest_eligible_courses.merge(
    eng097_fpu_students_eligible[['Student_ID', 'Eligible_Courses_CO']],  # Relevant columns from filtered_students
    on='Student_ID',
    how='left',  # Keep all rows in students_df
    suffixes=('', '_updated'))  # Suffix to differentiate new column)

    latest_eligible_courses['Eligible_Courses_CO'] = latest_eligible_courses['Eligible_Courses_CO_updated'].combine_first(latest_eligible_courses['Eligible_Courses_CO'])
    latest_eligible_courses = latest_eligible_courses.drop(columns=['Eligible_Courses_CO_updated'])
    latest_eligible_courses = latest_eligible_courses.merge(grouped_data_cs,on = "Student_ID",how = "outer")
    latest_eligible_courses['Course_ID'] = latest_eligible_courses['Course_ID'].apply(lambda x: x if isinstance(x, list) else [])
    latest_eligible_courses = latest_eligible_courses.apply(process_row, axis=1)
    latest_eligible_courses.drop(columns=["Course_ID"], inplace=True)

    # Exploding DataFrame and mapping course details
    eligible_courses_comprehensive_data = latest_eligible_courses.explode("Eligible_Courses_CO")
    eligible_courses_comprehensive_data = eligible_courses_comprehensive_data.merge(courses_cs[['Course_ID', 'AREA_OF_STUDY', 'COURSE_OF_STUDY', 'Course_Level']],
                                                                                    left_on='Eligible_Courses_CO', right_on='Course_ID', how='left').drop(columns="Course_ID")
    eligible_courses_comprehensive_data['Eligible_Courses_CO'] = eligible_courses_comprehensive_data['Eligible_Courses_CO'].apply(lambda x: x if isinstance(x, list) else ([] if pd.isna(x) else [x]))

    # Find Additional Eligibilities
    eligible_courses_comprehensive_data['Future_Eligible_Courses_List'] = eligible_courses_comprehensive_data.apply(lambda row: find_additional_eligibilities(row['Eligible_Courses_CO'], set(row['Eligible_Courses_CO']), prerequisites_cs), axis=1)
    eligible_courses_per_student = eligible_courses_comprehensive_data.groupby('Student_ID')['Eligible_Courses_CO'].agg(lambda x: list(set([item for sublist in x for item in sublist if isinstance(sublist, list)]))).reset_index()

    # Merge aggregated list back to the comprehensive DataFrame
    eligible_courses_comprehensive_data = eligible_courses_comprehensive_data.merge(eligible_courses_per_student.rename(columns={'Eligible_Courses_CO': 'Eligible_Courses_List_All'}), on='Student_ID', how='left')

    # Filter matching courses from future eligible lists
    eligible_courses_comprehensive_data['Future_Eligible_Courses_List'] = eligible_courses_comprehensive_data.apply(lambda row: [course for course in row['Future_Eligible_Courses_List'] if course not in row['Eligible_Courses_List_All']], axis=1)
    eligible_courses_comprehensive_data['Total_Future_Eligible_Courses_List'] = eligible_courses_comprehensive_data['Future_Eligible_Courses_List'].apply(len)

    # Special eligibility courses
    eligible_courses_comprehensive_data['Future_Eligible_Courses_Special'] = eligible_courses_comprehensive_data.apply(lambda row: find_additional_eligibilities_special(row['Eligible_Courses_CO'], set(row['Eligible_Courses_CO']), row, prerequisites_special_cs, conditions_cs, is_eligible_special_cs_), axis=1)
    eligible_courses_comprehensive_data['Future_Eligible_Courses_Special'] = eligible_courses_comprehensive_data.apply(lambda row: [course for course in row['Future_Eligible_Courses_Special'] if course not in row['Eligible_Courses_List_All']], axis=1)
    eligible_courses_comprehensive_data['Total_Future_Eligible_Courses_Special'] = eligible_courses_comprehensive_data['Future_Eligible_Courses_Special'].apply(len)

    # Combine Future Eligible Courses and calculate the score
    eligible_courses_comprehensive_data["Future_Eligible_Courses"] = eligible_courses_comprehensive_data["Future_Eligible_Courses_List"] + eligible_courses_comprehensive_data["Future_Eligible_Courses_Special"]
    eligible_courses_comprehensive_data['Course_Score'] = eligible_courses_comprehensive_data['Future_Eligible_Courses'].apply(len)

    # Find Best Courses
    recommended_courses_cs = eligible_courses_comprehensive_data.groupby(['Student_ID', 'Semester']).apply(lambda group: pd.Series({'Recommended_Courses': find_best_courses(group)})).reset_index()


    eligible_courses_comprehensive_data = eligible_courses_comprehensive_data.merge(recommended_courses_cs, on=['Student_ID', 'Semester'])
    eligible_courses_comprehensive_data = eligible_courses_comprehensive_data.merge(weighted_remaining_courses_df, on=['Student_ID', 'AREA_OF_STUDY'], how='left')


    eligible_courses_comprehensive_data = eligible_courses_comprehensive_data.groupby('Student_ID', group_keys=False).apply(normalize_by_student)
    eligible_courses_comprehensive_data['Final_Score'] = (
        (eligible_courses_comprehensive_data['Normalized_Course_Score'] * 0.4) +
        (eligible_courses_comprehensive_data['Normalized_Remaining_Courses_Weight'] * 0.4) +
        (eligible_courses_comprehensive_data['Normalized_Course_Level'] * 0.2))

    # Find Best Courses
    recommended_courses_cs_v2 = eligible_courses_comprehensive_data.groupby(['Student_ID', 'Semester']).apply(lambda group: pd.Series({'Recommended_Courses_V2': find_best_courses_v2(group)})).reset_index()
    eligible_courses_comprehensive_data = eligible_courses_comprehensive_data.merge(recommended_courses_cs_v2, on=['Student_ID', 'Semester'])

    recommended_courses = recommended_courses_cs.merge(recommended_courses_cs_v2,on=['Student_ID', 'Semester'])

    # Create summary DataFrames for eligible courses
    summary_area_of_study_eligible = eligible_courses_comprehensive_data.groupby(['Student_ID', 'AREA_OF_STUDY']).size().reset_index(name='Total_Eligible_Courses')
    summary_course_of_study_eligible = eligible_courses_comprehensive_data.groupby(['Student_ID', 'COURSE_OF_STUDY']).size().reset_index(name='Total_Eligible_Courses')
    summary_area_of_study_eligible = summary_area_of_study_eligible.pivot_table(index="Student_ID", columns="AREA_OF_STUDY", values="Total_Eligible_Courses", fill_value=0).reset_index()

    return requirements_cs_,student_progress,summary_area_of_study_taken,remaining_courses_df,latest_eligible_courses,eligible_courses_comprehensive_data,recommended_courses,summary_area_of_study_eligible

def process_data_dmp(st_hist_data,major_data, requirements_weights_path):
    
    values_to_delete = ['FA', 'F', 'I', 'S', 'NP', 'WA']
    failed_grades = ['F','FA','NP']
    failed_data = st_hist_data[st_hist_data["GRADE"].isin(failed_grades)]
    st_hist_data = st_hist_data[~st_hist_data["GRADE"].isin(values_to_delete)]
    
    # Filtering and Sorting Data
    failed_data = failed_data[failed_data['Major'] == 'Digital Media Production']
    failed_data = failed_data.sort_values(by=['Student_ID', 'Semester'])

    grouped_data_failed = failed_data.groupby(['Student_ID'])['Course_ID'].apply(list).reset_index()
    
    # Filtering and Sorting Data
    dmp_data = st_hist_data[st_hist_data['Major'] == "Digital Media Production"]
    dmp_data = dmp_data.sort_values(by=['Student_ID', 'Semester'])

    major = major_data["All_Courses"]
    courses_co = major_data["CO_Courses"]
    
    major["AREA_OF_STUDY"] = major["AREA_OF_STUDY"].fillna("NA")
    # Dropping records where AREA_OF_STUDY is 'N' and COURSE_OF_STUDY is 'Z'
    major_filtered = major[~((major['AREA_OF_STUDY'] == 'NA') & (major['COURSE_OF_STUDY'] == 'Z'))]
    
    major_filtered = major_filtered.copy()
    # Apply replacements directly to the specific columns to avoid SettingWithCopyWarning
    major_filtered['AREA_OF_STUDY'] = major_filtered['AREA_OF_STUDY'].replace("NA","GE")
    major_filtered['COURSE_OF_STUDY'] = major_filtered['COURSE_OF_STUDY'].replace("N","E")
    
    # Defining the major lists
    cas_majors = ['COMSCIENCE', 'ENGLISH', 'LINGUISTIC', 'LITERATURE', 'DIGITALMED', 'PR / ADV', 'VISUAL COM']
    df_cas = major_filtered[major_filtered['Major'].isin(cas_majors)]
    df_cas = df_cas[~((df_cas['AREA_OF_STUDY'] == 'GE') & (df_cas['COURSE_OF_STUDY'] == 'E'))]
    
    list_conditions = ['-', 'ONE_COURSE']

    cas_list = df_cas[df_cas['Condition'].isin(list_conditions)]
    cas_special_cases = df_cas[~df_cas['Condition'].isin(list_conditions)]
    cas_co = courses_co[courses_co['Major'].isin(cas_majors)]
    
    dmp_list = cas_list[cas_list["Major"] == "DIGITALMED"]
    dmp_special_cases = cas_special_cases[cas_special_cases["Major"] == "DIGITALMED"]
    dmp_co = cas_co[cas_co["Major"] == "DIGITALMED"]

    # Process 'REQUISITES_LIST'
    dmp_co = dmp_co.copy()
    dmp_co.loc[:, 'REQUISITES_LIST'] = dmp_co['REQUISITES_LIST'].apply(ast.literal_eval)

    # CAS Courses
    cas_courses = major_filtered[major_filtered['Major'].isin(cas_majors)]
    courses_dmp = cas_courses[cas_courses["Major"] == "DIGITALMED"]
    
    grouped_data_dmp = dmp_data.groupby(['Student_ID'])['Course_ID'].apply(list).reset_index()

    # Merge dataframes
    merged_df = grouped_data_failed.merge(grouped_data_dmp, on=['Student_ID'], how='outer', suffixes=('_failed', '_all'))
    # Replace NaN with empty lists to avoid errors
    merged_df['Course_ID_all'] = merged_df['Course_ID_all'].apply(lambda x: x if isinstance(x, list) else [])
    merged_df['Course_ID_failed'] = merged_df['Course_ID_failed'].apply(lambda x: x if isinstance(x, list) else [])

    merged_df['Failed_Courses'] = merged_df.apply(
        lambda row: list(set(row['Course_ID_failed']) - set(row['Course_ID_all'])),
        axis=1)
    # Keep only relevant columns
    merged_df = merged_df[['Student_ID', 'Failed_Courses']]

    # Extract Accounting specific requirements and weights from respective DataFrames
    requirements_df = pd.read_excel(requirements_weights_path,sheet_name="requirements")
    weights_df = pd.read_excel(requirements_weights_path,sheet_name="weights")
    requirements_dmp = requirements_df[requirements_df["Major"] == "Digital Media Production"]
    requirements_dmp_ = requirements_dmp.pivot_table(index="Major",columns="AREA_OF_STUDY",values ='Required_Courses' ,aggfunc='sum',fill_value=0).reset_index()
    weights_dmp = weights_df[weights_df["Major"] == "Digital Media Production"]

    student_courses = dmp_data[["Student_ID", "Course_ID"]]

    # Map AREA_OF_STUDY and COURSE_OF_STUDY to dmp_data
    student_courses = student_courses.merge(courses_dmp[['Course_ID', 'AREA_OF_STUDY', 'COURSE_OF_STUDY', "Course_Level"]],
                                            on='Course_ID', how='left').drop_duplicates()

    # Create summary DataFrames for taken courses
    student_progress = student_courses.groupby(['Student_ID', 'AREA_OF_STUDY']).size().reset_index(name='Total_Taken_Courses')
    student_progress = student_progress.merge(requirements_dmp, on='AREA_OF_STUDY', how='left')
    student_progress["Remaining_Courses"] = student_progress["Required_Courses"] - student_progress["Total_Taken_Courses"]
    student_progress["Remaining_Courses"] = student_progress["Remaining_Courses"].apply(lambda x: max(x, 0))

    free_elective_taken_counts = student_courses[(student_courses['AREA_OF_STUDY'] == "GE") & (student_courses['COURSE_OF_STUDY'] == "E")].groupby('Student_ID').size().reset_index(name='Total_Free_Electives_Taken')

    # Update progress by including the free elective data
    student_progress["Student_Progress"] = (student_progress["Total_Taken_Courses"] / student_progress["Required_Courses"]) * 100
    student_progress["Student_Progress"].replace([np.inf, -np.inf], 100, inplace=True)

    summary_area_of_study_taken = student_progress.pivot_table(index="Student_ID", columns="AREA_OF_STUDY", values="Total_Taken_Courses", fill_value=0)
    summary_area_of_study_taken = summary_area_of_study_taken.merge(free_elective_taken_counts, on="Student_ID", how="left").fillna(0).rename(columns={"Total_Free_Electives_Taken": "FE"})

    # Create a copy of summary_area_of_study_taken to work on remaining courses calculation
    remaining_courses_df = summary_area_of_study_taken.copy()

    # Loop through each AREA_OF_STUDY and calculate remaining courses by subtracting from the requirements
    for column in remaining_courses_df.columns:
        if column in requirements_dmp['AREA_OF_STUDY'].values:
            required_courses = requirements_dmp.loc[requirements_dmp['AREA_OF_STUDY'] == column, 'Required_Courses'].values[0]
            remaining_courses_df[column] = required_courses - remaining_courses_df[column]
            remaining_courses_df[column] = remaining_courses_df[column].clip(lower=0)

    # Calculate weighted remaining courses
    weighted_remaining_courses_df = remaining_courses_df.copy()
    for column in weighted_remaining_courses_df.columns:
        if column in weights_dmp['AREA_OF_STUDY'].values:
            weight_value = weights_dmp.loc[weights_dmp['AREA_OF_STUDY'] == column, 'Weight'].values[0]
            weighted_remaining_courses_df[column] = weighted_remaining_courses_df[column] * weight_value

    # Prepare weighted remaining courses for merge
    weighted_remaining_courses_df = weighted_remaining_courses_df.reset_index().melt(id_vars=['Student_ID'],
                                                                                      var_name='AREA_OF_STUDY',
                                                                                      value_name='Remaining_Courses_Weight_Score')
    weighted_remaining_courses_df = weighted_remaining_courses_df[weighted_remaining_courses_df["AREA_OF_STUDY"] != "index"]

    # Eligibility Calculation for Standard and Special Cases
    prerequisites_dmp = dmp_list.set_index('Course_ID')['REQUISITES_LIST'].apply(eval).to_dict()
    prerequisites_special_dmp = dmp_special_cases.set_index('Course_ID')['REQUISITES_LIST'].apply(eval).to_dict()
    conditions_dmp = dmp_special_cases.set_index('Course_ID')['Condition'].to_dict()

    final_results_dmp = []  # Standard eligibility results
    final_results_special_dmp = []  # Special eligibility results

    for student_id, group in dmp_data.groupby('Student_ID'):
        cumulative_courses = set()
        for semester, semester_group in group.groupby('Semester'):
            taken_courses = set(semester_group['Course_ID'].tolist())
            cumulative_courses.update(taken_courses)

            # Determine Standard Eligible Courses
            student_info = semester_group.iloc[0].to_dict()
            eligible_courses = {course for course in prerequisites_dmp.keys() if all(req in cumulative_courses for req in prerequisites_dmp[course])}
            final_results_dmp.append({
                'Student_ID': student_id,
                'Semester': semester,
                'Major': student_info['Major'],
                'College': student_info['College'],
                'Program': student_info['Program'],
                'Passed Credits': student_info['Passed Credits'],
                'Student_Level': student_info['Student_Level'],
                'Eligible_Courses': list(eligible_courses - cumulative_courses)
            })

            # Determine Special Eligible Courses
            special_eligible_courses = {
                course for course in prerequisites_special_dmp.keys()
                if is_eligible_special_dmp(course, cumulative_courses, student_info, prerequisites_special_dmp, conditions_dmp)
            }
            final_results_special_dmp.append({
                'Student_ID': student_id,
                'Semester': semester,
                'Major': student_info['Major'],
                'College': student_info['College'],
                'Program': student_info['Program'],
                'Passed Credits': student_info['Passed Credits'],
                'Student_Level': student_info['Student_Level'],
                'Eligible_Courses': list(special_eligible_courses - cumulative_courses)
            })

    # Convert Results to DataFrames
    final_results_df_dmp = pd.DataFrame(final_results_dmp)
    final_results_special_df_dmp = pd.DataFrame(final_results_special_dmp)
    
    # Combine Eligible Courses from Both DataFrames
    combined_dmp_list = combine_eligible_courses(final_results_df_dmp, final_results_special_df_dmp)
    # Find Course Combinations for Co-requisites
    combined_dmp_list = combined_dmp_list.apply(create_combined_courses, axis=1, co=dmp_co)
    latest_eligible_courses = combined_dmp_list.sort_values(by='Semester', ascending=False)
    latest_eligible_courses = latest_eligible_courses.groupby('Student_ID').first().reset_index()
    latest_eligible_courses = latest_eligible_courses.merge(grouped_data_dmp,on = "Student_ID",how = "inner")
    latest_eligible_courses["Eligible_Courses_CO"] = latest_eligible_courses.apply(remove_matches, axis=1)
    latest_eligible_courses = latest_eligible_courses.apply(process_row, axis=1)
    latest_eligible_courses.drop(columns=["Course_ID"], inplace=True)

    latest_eligible_courses = latest_eligible_courses.merge(merged_df, on='Student_ID', how='outer')
    latest_eligible_courses['Failed_Courses'] = latest_eligible_courses['Failed_Courses'].apply(lambda x: x if isinstance(x, list) else [])
    latest_eligible_courses['Eligible_Courses_CO'] = latest_eligible_courses['Eligible_Courses_CO'].apply(lambda x: x if isinstance(x, list) else [])
    latest_eligible_courses['Eligible_Courses_CO'] = latest_eligible_courses.apply(
        lambda row: list(set(row['Eligible_Courses_CO']) | (set(row['Failed_Courses']) - set(row['Eligible_Courses_CO']))),axis=1)
    latest_eligible_courses = latest_eligible_courses.drop(columns=['Failed_Courses'])

    latest_info_failed = failed_data.loc[failed_data.groupby("Student_ID")["Semester"].idxmax()]
    missing_semester_df = latest_eligible_courses[latest_eligible_courses['Semester'].isna()]
    latest_eligible_courses.dropna(inplace=True)
    columns_to_fill = ['Semester', 'Major', 'College', 'Program', 'Passed Credits', 'Student_Level']

    for col in columns_to_fill:
        missing_semester_df.loc[missing_semester_df[col].isna(), col] = missing_semester_df.loc[
            missing_semester_df[col].isna(), 'Student_ID'
        ].map(latest_info_failed.set_index('Student_ID')[col])

    columns_to_convert = ['Semester', 'Student_Level', 'Passed Credits']
    for col in columns_to_convert:
        latest_eligible_courses.loc[:, col] = pd.to_numeric(latest_eligible_courses[col], errors='coerce').astype('Int64')
        
    latest_eligible_courses = pd.concat([latest_eligible_courses, missing_semester_df], ignore_index=True)

    max_semester_index = dmp_data.groupby('Student_ID')['Semester'].idxmax()
    max_semester_data = dmp_data.loc[max_semester_index, ['Student_ID', 'Semester']]

    last_semester_courses = pd.merge(max_semester_data, dmp_data, on=['Student_ID', 'Semester'])
    eng097_fpu_students = last_semester_courses[last_semester_courses['Course_ID'] == 'ENGL097']
    # Target course list
    target_courses = ['ENGL098', 'MATH094', 'MATH095', 'MATH096', 'MATH098', 'MATH100', 'MATH111', 'MATH120', 'MATH121', 'MATH131', 'MATH140']

    eng097_fpu_students_eligible = latest_eligible_courses[latest_eligible_courses['Student_ID']
                                                       .isin(eng097_fpu_students['Student_ID'])].copy()
    eng097_fpu_students_eligible.loc[:, 'Eligible_Courses_CO'] = eng097_fpu_students_eligible['Eligible_Courses_CO'].apply(
    lambda courses: [course for course in courses if course in target_courses])

    latest_eligible_courses = latest_eligible_courses.merge(
    eng097_fpu_students_eligible[['Student_ID', 'Eligible_Courses_CO']],  # Relevant columns from filtered_students
    on='Student_ID',
    how='left',  # Keep all rows in students_df
    suffixes=('', '_updated'))  # Suffix to differentiate new column)

    latest_eligible_courses['Eligible_Courses_CO'] = latest_eligible_courses['Eligible_Courses_CO_updated'].combine_first(latest_eligible_courses['Eligible_Courses_CO'])
    latest_eligible_courses = latest_eligible_courses.drop(columns=['Eligible_Courses_CO_updated'])
    latest_eligible_courses = latest_eligible_courses.merge(grouped_data_dmp,on = "Student_ID",how = "outer")
    latest_eligible_courses['Course_ID'] = latest_eligible_courses['Course_ID'].apply(lambda x: x if isinstance(x, list) else [])
    latest_eligible_courses = latest_eligible_courses.apply(process_row, axis=1)
    latest_eligible_courses.drop(columns=["Course_ID"], inplace=True)

    # Exploding DataFrame and mapping course details
    eligible_courses_comprehensive_data = latest_eligible_courses.explode("Eligible_Courses_CO")
    eligible_courses_comprehensive_data = eligible_courses_comprehensive_data.merge(courses_dmp[['Course_ID', 'AREA_OF_STUDY', 'COURSE_OF_STUDY', 'Course_Level']],
                                                                                    left_on='Eligible_Courses_CO', right_on='Course_ID', how='left').drop(columns="Course_ID")
    eligible_courses_comprehensive_data['Eligible_Courses_CO'] = eligible_courses_comprehensive_data['Eligible_Courses_CO'].apply(lambda x: x if isinstance(x, list) else ([] if pd.isna(x) else [x]))

    # Find Additional Eligibilities
    eligible_courses_comprehensive_data['Future_Eligible_Courses_List'] = eligible_courses_comprehensive_data.apply(lambda row: find_additional_eligibilities(row['Eligible_Courses_CO'], set(row['Eligible_Courses_CO']), prerequisites_dmp), axis=1)
    eligible_courses_per_student = eligible_courses_comprehensive_data.groupby('Student_ID')['Eligible_Courses_CO'].agg(lambda x: list(set([item for sublist in x for item in sublist if isinstance(sublist, list)]))).reset_index()

    # Merge aggregated list back to the comprehensive DataFrame
    eligible_courses_comprehensive_data = eligible_courses_comprehensive_data.merge(eligible_courses_per_student.rename(columns={'Eligible_Courses_CO': 'Eligible_Courses_List_All'}), on='Student_ID', how='left')

    # Filter matching courses from future eligible lists
    eligible_courses_comprehensive_data['Future_Eligible_Courses_List'] = eligible_courses_comprehensive_data.apply(lambda row: [course for course in row['Future_Eligible_Courses_List'] if course not in row['Eligible_Courses_List_All']], axis=1)
    eligible_courses_comprehensive_data['Total_Future_Eligible_Courses_List'] = eligible_courses_comprehensive_data['Future_Eligible_Courses_List'].apply(len)

    # Special eligibility courses
    eligible_courses_comprehensive_data['Future_Eligible_Courses_Special'] = eligible_courses_comprehensive_data.apply(lambda row: find_additional_eligibilities_special(row['Eligible_Courses_CO'], set(row['Eligible_Courses_CO']), row, prerequisites_special_dmp, conditions_dmp, is_eligible_special_dmp_), axis=1)
    eligible_courses_comprehensive_data['Future_Eligible_Courses_Special'] = eligible_courses_comprehensive_data.apply(lambda row: [course for course in row['Future_Eligible_Courses_Special'] if course not in row['Eligible_Courses_List_All']], axis=1)
    eligible_courses_comprehensive_data['Total_Future_Eligible_Courses_Special'] = eligible_courses_comprehensive_data['Future_Eligible_Courses_Special'].apply(len)

    # Combine Future Eligible Courses and calculate the score
    eligible_courses_comprehensive_data["Future_Eligible_Courses"] = eligible_courses_comprehensive_data["Future_Eligible_Courses_List"] + eligible_courses_comprehensive_data["Future_Eligible_Courses_Special"]
    eligible_courses_comprehensive_data['Course_Score'] = eligible_courses_comprehensive_data['Future_Eligible_Courses'].apply(len)

    # Find Best Courses
    recommended_courses_dmp = eligible_courses_comprehensive_data.groupby(['Student_ID', 'Semester']).apply(lambda group: pd.Series({'Recommended_Courses': find_best_courses(group)})).reset_index()


    eligible_courses_comprehensive_data = eligible_courses_comprehensive_data.merge(recommended_courses_dmp, on=['Student_ID', 'Semester'])
    eligible_courses_comprehensive_data = eligible_courses_comprehensive_data.merge(weighted_remaining_courses_df, on=['Student_ID', 'AREA_OF_STUDY'], how='left')


    eligible_courses_comprehensive_data = eligible_courses_comprehensive_data.groupby('Student_ID', group_keys=False).apply(normalize_by_student)
    eligible_courses_comprehensive_data['Final_Score'] = (
        (eligible_courses_comprehensive_data['Normalized_Course_Score'] * 0.4) +
        (eligible_courses_comprehensive_data['Normalized_Remaining_Courses_Weight'] * 0.4) +
        (eligible_courses_comprehensive_data['Normalized_Course_Level'] * 0.2))

    # Find Best Courses
    recommended_courses_dmp_v2 = eligible_courses_comprehensive_data.groupby(['Student_ID', 'Semester']).apply(lambda group: pd.Series({'Recommended_Courses_V2': find_best_courses_v2(group)})).reset_index()
    eligible_courses_comprehensive_data = eligible_courses_comprehensive_data.merge(recommended_courses_dmp_v2, on=['Student_ID', 'Semester'])

    recommended_courses = recommended_courses_dmp.merge(recommended_courses_dmp_v2,on=['Student_ID', 'Semester'])

    # Create summary DataFrames for eligible courses
    summary_area_of_study_eligible = eligible_courses_comprehensive_data.groupby(['Student_ID', 'AREA_OF_STUDY']).size().reset_index(name='Total_Eligible_Courses')
    summary_course_of_study_eligible = eligible_courses_comprehensive_data.groupby(['Student_ID', 'COURSE_OF_STUDY']).size().reset_index(name='Total_Eligible_Courses')
    summary_area_of_study_eligible = summary_area_of_study_eligible.pivot_table(index="Student_ID", columns="AREA_OF_STUDY", values="Total_Eligible_Courses", fill_value=0).reset_index()

    return requirements_dmp_,student_progress,summary_area_of_study_taken,remaining_courses_df,latest_eligible_courses,eligible_courses_comprehensive_data,recommended_courses,summary_area_of_study_eligible

def process_data_eng_lin(st_hist_data,major_data, requirements_weights_path):
    
    values_to_delete = ['FA', 'F', 'I', 'S', 'NP', 'WA']
    failed_grades = ['F','FA','NP']
    failed_data = st_hist_data[st_hist_data["GRADE"].isin(failed_grades)]
    st_hist_data = st_hist_data[~st_hist_data["GRADE"].isin(values_to_delete)]
    
    # Filtering and Sorting Data
    failed_data = failed_data[failed_data['Major'] == 'Eng- Linguistics - Translation']
    failed_data = failed_data.sort_values(by=['Student_ID', 'Semester'])

    grouped_data_failed = failed_data.groupby(['Student_ID'])['Course_ID'].apply(list).reset_index()
    
    # Filtering and Sorting Data
    eng_lin_data = st_hist_data[st_hist_data['Major'] == "Eng- Linguistics - Translation"]
    eng_lin_data = eng_lin_data.sort_values(by=['Student_ID', 'Semester'])

    major = major_data["All_Courses"]
    courses_co = major_data["CO_Courses"]
    
    major["AREA_OF_STUDY"] = major["AREA_OF_STUDY"].fillna("NA")
    # Dropping records where AREA_OF_STUDY is 'N' and COURSE_OF_STUDY is 'Z'
    major_filtered = major[~((major['AREA_OF_STUDY'] == 'NA') & (major['COURSE_OF_STUDY'] == 'Z'))]
    
    major_filtered = major_filtered.copy()
    # Apply replacements directly to the specific columns to avoid SettingWithCopyWarning
    major_filtered['AREA_OF_STUDY'] = major_filtered['AREA_OF_STUDY'].replace("NA","GE")
    major_filtered['COURSE_OF_STUDY'] = major_filtered['COURSE_OF_STUDY'].replace("N","E")
    
    # Defining the major lists
    cas_majors = ['COMSCIENCE', 'ENGLISH', 'LINGUISTIC', 'LITERATURE', 'DIGITALMED', 'PR / ADV', 'VISUAL COM']
    df_cas = major_filtered[major_filtered['Major'].isin(cas_majors)]
    df_cas = df_cas[~((df_cas['AREA_OF_STUDY'] == 'GE') & (df_cas['COURSE_OF_STUDY'] == 'E'))]
    
    list_conditions = ['-', 'ONE_COURSE']

    cas_list = df_cas[df_cas['Condition'].isin(list_conditions)]
    cas_special_cases = df_cas[~df_cas['Condition'].isin(list_conditions)]
    cas_co = courses_co[courses_co['Major'].isin(cas_majors)]
    
    eng_lin_list = cas_list[cas_list["Major"] == "LINGUISTIC"]
    eng_lin_special_cases = cas_special_cases[cas_special_cases["Major"] == "LINGUISTIC"]
    eng_lin_co = cas_co[cas_co["Major"] == "LINGUISTIC"]

    # Process 'REQUISITES_LIST'
    eng_lin_co = eng_lin_co.copy()
    eng_lin_co.loc[:, 'REQUISITES_LIST'] = eng_lin_co['REQUISITES_LIST'].apply(ast.literal_eval)

    # CAS Courses
    cas_courses = major_filtered[major_filtered['Major'].isin(cas_majors)]
    courses_eng_lin = cas_courses[cas_courses["Major"] == "LINGUISTIC"]
    
    grouped_data_eng_lin = eng_lin_data.groupby(['Student_ID'])['Course_ID'].apply(list).reset_index()

    # Merge dataframes
    merged_df = grouped_data_failed.merge(grouped_data_eng_lin, on=['Student_ID'], how='outer', suffixes=('_failed', '_all'))
    # Replace NaN with empty lists to avoid errors
    merged_df['Course_ID_all'] = merged_df['Course_ID_all'].apply(lambda x: x if isinstance(x, list) else [])
    merged_df['Course_ID_failed'] = merged_df['Course_ID_failed'].apply(lambda x: x if isinstance(x, list) else [])

    merged_df['Failed_Courses'] = merged_df.apply(
        lambda row: list(set(row['Course_ID_failed']) - set(row['Course_ID_all'])),
        axis=1)
    # Keep only relevant columns
    merged_df = merged_df[['Student_ID', 'Failed_Courses']]

    # Extract Accounting specific requirements and weights from respective DataFrames
    requirements_df = pd.read_excel(requirements_weights_path,sheet_name="requirements")
    weights_df = pd.read_excel(requirements_weights_path,sheet_name="weights")
    requirements_eng_lin = requirements_df[requirements_df["Major"] == "Eng- Linguistics - Translation"]
    requirements_eng_lin_ = requirements_eng_lin.pivot_table(index="Major",columns="AREA_OF_STUDY",values ='Required_Courses' ,aggfunc='sum',fill_value=0).reset_index()
    weights_eng_lin = weights_df[weights_df["Major"] == "Eng- Linguistics - Translation"]

    student_courses = eng_lin_data[["Student_ID", "Course_ID"]]

    # Map AREA_OF_STUDY and COURSE_OF_STUDY to eng_lin_data
    student_courses = student_courses.merge(courses_eng_lin[['Course_ID', 'AREA_OF_STUDY', 'COURSE_OF_STUDY', "Course_Level"]],
                                            on='Course_ID', how='left').drop_duplicates()

    # Create summary DataFrames for taken courses
    student_progress = student_courses.groupby(['Student_ID', 'AREA_OF_STUDY']).size().reset_index(name='Total_Taken_Courses')
    student_progress = student_progress.merge(requirements_eng_lin, on='AREA_OF_STUDY', how='left')
    student_progress["Remaining_Courses"] = student_progress["Required_Courses"] - student_progress["Total_Taken_Courses"]
    student_progress["Remaining_Courses"] = student_progress["Remaining_Courses"].apply(lambda x: max(x, 0))

    free_elective_taken_counts = student_courses[(student_courses['AREA_OF_STUDY'] == "GE") & (student_courses['COURSE_OF_STUDY'] == "E")].groupby('Student_ID').size().reset_index(name='Total_Free_Electives_Taken')

    # Update progress by including the free elective data
    student_progress["Student_Progress"] = (student_progress["Total_Taken_Courses"] / student_progress["Required_Courses"]) * 100
    student_progress["Student_Progress"].replace([np.inf, -np.inf], 100, inplace=True)

    summary_area_of_study_taken = student_progress.pivot_table(index="Student_ID", columns="AREA_OF_STUDY", values="Total_Taken_Courses", fill_value=0)
    summary_area_of_study_taken = summary_area_of_study_taken.merge(free_elective_taken_counts, on="Student_ID", how="left").fillna(0).rename(columns={"Total_Free_Electives_Taken": "FE"})

    # Create a copy of summary_area_of_study_taken to work on remaining courses calculation
    remaining_courses_df = summary_area_of_study_taken.copy()

    # Loop through each AREA_OF_STUDY and calculate remaining courses by subtracting from the requirements
    for column in remaining_courses_df.columns:
        if column in requirements_eng_lin['AREA_OF_STUDY'].values:
            required_courses = requirements_eng_lin.loc[requirements_eng_lin['AREA_OF_STUDY'] == column, 'Required_Courses'].values[0]
            remaining_courses_df[column] = required_courses - remaining_courses_df[column]
            remaining_courses_df[column] = remaining_courses_df[column].clip(lower=0)

    # Calculate weighted remaining courses
    weighted_remaining_courses_df = remaining_courses_df.copy()
    for column in weighted_remaining_courses_df.columns:
        if column in weights_eng_lin['AREA_OF_STUDY'].values:
            weight_value = weights_eng_lin.loc[weights_eng_lin['AREA_OF_STUDY'] == column, 'Weight'].values[0]
            weighted_remaining_courses_df[column] = weighted_remaining_courses_df[column] * weight_value

    # Prepare weighted remaining courses for merge
    weighted_remaining_courses_df = weighted_remaining_courses_df.reset_index().melt(id_vars=['Student_ID'],
                                                                                      var_name='AREA_OF_STUDY',
                                                                                      value_name='Remaining_Courses_Weight_Score')
    weighted_remaining_courses_df = weighted_remaining_courses_df[weighted_remaining_courses_df["AREA_OF_STUDY"] != "index"]

    # Eligibility Calculation for Standard and Special Cases
    prerequisites_eng_lin = eng_lin_list.set_index('Course_ID')['REQUISITES_LIST'].apply(eval).to_dict()
    prerequisites_special_eng_lin = eng_lin_special_cases.set_index('Course_ID')['REQUISITES_LIST'].apply(eval).to_dict()
    conditions_eng_lin = eng_lin_special_cases.set_index('Course_ID')['Condition'].to_dict()

    final_results_eng_lin = []  # Standard eligibility results
    final_results_special_eng_lin = []  # Special eligibility results

    for student_id, group in eng_lin_data.groupby('Student_ID'):
        cumulative_courses = set()
        for semester, semester_group in group.groupby('Semester'):
            taken_courses = set(semester_group['Course_ID'].tolist())
            cumulative_courses.update(taken_courses)

            # Determine Standard Eligible Courses
            student_info = semester_group.iloc[0].to_dict()
            eligible_courses = {course for course in prerequisites_eng_lin.keys() if all(req in cumulative_courses for req in prerequisites_eng_lin[course])}
            final_results_eng_lin.append({
                'Student_ID': student_id,
                'Semester': semester,
                'Major': student_info['Major'],
                'College': student_info['College'],
                'Program': student_info['Program'],
                'Passed Credits': student_info['Passed Credits'],
                'Student_Level': student_info['Student_Level'],
                'Eligible_Courses': list(eligible_courses - cumulative_courses)
            })

            # Determine Special Eligible Courses
            special_eligible_courses = {
                course for course in prerequisites_special_eng_lin.keys()
                if is_eligible_special_eng_lin(course, cumulative_courses, student_info, prerequisites_special_eng_lin, conditions_eng_lin)
            }
            final_results_special_eng_lin.append({
                'Student_ID': student_id,
                'Semester': semester,
                'Major': student_info['Major'],
                'College': student_info['College'],
                'Program': student_info['Program'],
                'Passed Credits': student_info['Passed Credits'],
                'Student_Level': student_info['Student_Level'],
                'Eligible_Courses': list(special_eligible_courses - cumulative_courses)
            })

    # Convert Results to DataFrames
    final_results_df_eng_lin = pd.DataFrame(final_results_eng_lin)
    final_results_special_df_eng_lin = pd.DataFrame(final_results_special_eng_lin)
    
    # Combine Eligible Courses from Both DataFrames
    combined_eng_lin_list = combine_eligible_courses(final_results_df_eng_lin, final_results_special_df_eng_lin)
    # Find Course Combinations for Co-requisites
    combined_eng_lin_list = combined_eng_lin_list.apply(create_combined_courses, axis=1, co=eng_lin_co)
    latest_eligible_courses = combined_eng_lin_list.sort_values(by='Semester', ascending=False)
    latest_eligible_courses = latest_eligible_courses.groupby('Student_ID').first().reset_index()
    latest_eligible_courses = latest_eligible_courses.merge(grouped_data_eng_lin,on = "Student_ID",how = "inner")
    latest_eligible_courses["Eligible_Courses_CO"] = latest_eligible_courses.apply(remove_matches, axis=1)
    latest_eligible_courses = latest_eligible_courses.apply(process_row, axis=1)
    latest_eligible_courses.drop(columns=["Course_ID"], inplace=True)

    latest_eligible_courses = latest_eligible_courses.merge(merged_df, on='Student_ID', how='outer')
    latest_eligible_courses['Failed_Courses'] = latest_eligible_courses['Failed_Courses'].apply(lambda x: x if isinstance(x, list) else [])
    latest_eligible_courses['Eligible_Courses_CO'] = latest_eligible_courses['Eligible_Courses_CO'].apply(lambda x: x if isinstance(x, list) else [])
    latest_eligible_courses['Eligible_Courses_CO'] = latest_eligible_courses.apply(
        lambda row: list(set(row['Eligible_Courses_CO']) | (set(row['Failed_Courses']) - set(row['Eligible_Courses_CO']))),axis=1)
    latest_eligible_courses = latest_eligible_courses.drop(columns=['Failed_Courses'])

    latest_info_failed = failed_data.loc[failed_data.groupby("Student_ID")["Semester"].idxmax()]
    missing_semester_df = latest_eligible_courses[latest_eligible_courses['Semester'].isna()]
    latest_eligible_courses.dropna(inplace=True)
    columns_to_fill = ['Semester', 'Major', 'College', 'Program', 'Passed Credits', 'Student_Level']

    for col in columns_to_fill:
        missing_semester_df.loc[missing_semester_df[col].isna(), col] = missing_semester_df.loc[
            missing_semester_df[col].isna(), 'Student_ID'
        ].map(latest_info_failed.set_index('Student_ID')[col])

    columns_to_convert = ['Semester', 'Student_Level', 'Passed Credits']
    for col in columns_to_convert:
        latest_eligible_courses.loc[:, col] = pd.to_numeric(latest_eligible_courses[col], errors='coerce').astype('Int64')
        
    latest_eligible_courses = pd.concat([latest_eligible_courses, missing_semester_df], ignore_index=True)
    
    max_semester_index = eng_lin_data.groupby('Student_ID')['Semester'].idxmax()
    max_semester_data = eng_lin_data.loc[max_semester_index, ['Student_ID', 'Semester']]

    last_semester_courses = pd.merge(max_semester_data, eng_lin_data, on=['Student_ID', 'Semester'])
    eng097_fpu_students = last_semester_courses[last_semester_courses['Course_ID'] == 'ENGL097']
    # Target course list
    target_courses = ['ENGL098', 'MATH094', 'MATH095', 'MATH096', 'MATH098', 'MATH100', 'MATH111', 'MATH120', 'MATH121', 'MATH131', 'MATH140']

    eng097_fpu_students_eligible = latest_eligible_courses[latest_eligible_courses['Student_ID']
                                                       .isin(eng097_fpu_students['Student_ID'])].copy()
    eng097_fpu_students_eligible.loc[:, 'Eligible_Courses_CO'] = eng097_fpu_students_eligible['Eligible_Courses_CO'].apply(
    lambda courses: [course for course in courses if course in target_courses])

    latest_eligible_courses = latest_eligible_courses.merge(
    eng097_fpu_students_eligible[['Student_ID', 'Eligible_Courses_CO']],  # Relevant columns from filtered_students
    on='Student_ID',
    how='left',  # Keep all rows in students_df
    suffixes=('', '_updated'))  # Suffix to differentiate new column)

    latest_eligible_courses['Eligible_Courses_CO'] = latest_eligible_courses['Eligible_Courses_CO_updated'].combine_first(latest_eligible_courses['Eligible_Courses_CO'])
    latest_eligible_courses = latest_eligible_courses.drop(columns=['Eligible_Courses_CO_updated'])
    latest_eligible_courses = latest_eligible_courses.merge(grouped_data_eng_lin,on = "Student_ID",how = "outer")
    latest_eligible_courses['Course_ID'] = latest_eligible_courses['Course_ID'].apply(lambda x: x if isinstance(x, list) else [])
    latest_eligible_courses = latest_eligible_courses.apply(process_row, axis=1)
    latest_eligible_courses.drop(columns=["Course_ID"], inplace=True)

    # Exploding DataFrame and mapping course details
    eligible_courses_comprehensive_data = latest_eligible_courses.explode("Eligible_Courses_CO")
    eligible_courses_comprehensive_data = eligible_courses_comprehensive_data.merge(courses_eng_lin[['Course_ID', 'AREA_OF_STUDY', 'COURSE_OF_STUDY', 'Course_Level']],
                                                                                    left_on='Eligible_Courses_CO', right_on='Course_ID', how='left').drop(columns="Course_ID")
    eligible_courses_comprehensive_data['Eligible_Courses_CO'] = eligible_courses_comprehensive_data['Eligible_Courses_CO'].apply(lambda x: x if isinstance(x, list) else ([] if pd.isna(x) else [x]))

    # Find Additional Eligibilities
    eligible_courses_comprehensive_data['Future_Eligible_Courses_List'] = eligible_courses_comprehensive_data.apply(lambda row: find_additional_eligibilities(row['Eligible_Courses_CO'], set(row['Eligible_Courses_CO']), prerequisites_eng_lin), axis=1)
    eligible_courses_per_student = eligible_courses_comprehensive_data.groupby('Student_ID')['Eligible_Courses_CO'].agg(lambda x: list(set([item for sublist in x for item in sublist if isinstance(sublist, list)]))).reset_index()

    # Merge aggregated list back to the comprehensive DataFrame
    eligible_courses_comprehensive_data = eligible_courses_comprehensive_data.merge(eligible_courses_per_student.rename(columns={'Eligible_Courses_CO': 'Eligible_Courses_List_All'}), on='Student_ID', how='left')

    # Filter matching courses from future eligible lists
    eligible_courses_comprehensive_data['Future_Eligible_Courses_List'] = eligible_courses_comprehensive_data.apply(lambda row: [course for course in row['Future_Eligible_Courses_List'] if course not in row['Eligible_Courses_List_All']], axis=1)
    eligible_courses_comprehensive_data['Total_Future_Eligible_Courses_List'] = eligible_courses_comprehensive_data['Future_Eligible_Courses_List'].apply(len)

    # Special eligibility courses
    eligible_courses_comprehensive_data['Future_Eligible_Courses_Special'] = eligible_courses_comprehensive_data.apply(lambda row: find_additional_eligibilities_special(row['Eligible_Courses_CO'], set(row['Eligible_Courses_CO']), row, prerequisites_special_eng_lin, conditions_eng_lin, is_eligible_special_eng_lin_), axis=1)
    eligible_courses_comprehensive_data['Future_Eligible_Courses_Special'] = eligible_courses_comprehensive_data.apply(lambda row: [course for course in row['Future_Eligible_Courses_Special'] if course not in row['Eligible_Courses_List_All']], axis=1)
    eligible_courses_comprehensive_data['Total_Future_Eligible_Courses_Special'] = eligible_courses_comprehensive_data['Future_Eligible_Courses_Special'].apply(len)

    # Combine Future Eligible Courses and calculate the score
    eligible_courses_comprehensive_data["Future_Eligible_Courses"] = eligible_courses_comprehensive_data["Future_Eligible_Courses_List"] + eligible_courses_comprehensive_data["Future_Eligible_Courses_Special"]
    eligible_courses_comprehensive_data['Course_Score'] = eligible_courses_comprehensive_data['Future_Eligible_Courses'].apply(len)

    # Find Best Courses
    recommended_courses_eng_lin = eligible_courses_comprehensive_data.groupby(['Student_ID', 'Semester']).apply(lambda group: pd.Series({'Recommended_Courses': find_best_courses(group)})).reset_index()


    eligible_courses_comprehensive_data = eligible_courses_comprehensive_data.merge(recommended_courses_eng_lin, on=['Student_ID', 'Semester'])
    eligible_courses_comprehensive_data = eligible_courses_comprehensive_data.merge(weighted_remaining_courses_df, on=['Student_ID', 'AREA_OF_STUDY'], how='left')


    eligible_courses_comprehensive_data = eligible_courses_comprehensive_data.groupby('Student_ID', group_keys=False).apply(normalize_by_student)
    eligible_courses_comprehensive_data['Final_Score'] = (
        (eligible_courses_comprehensive_data['Normalized_Course_Score'] * 0.4) +
        (eligible_courses_comprehensive_data['Normalized_Remaining_Courses_Weight'] * 0.4) +
        (eligible_courses_comprehensive_data['Normalized_Course_Level'] * 0.2))

    # Find Best Courses
    recommended_courses_eng_lin_v2 = eligible_courses_comprehensive_data.groupby(['Student_ID', 'Semester']).apply(lambda group: pd.Series({'Recommended_Courses_V2': find_best_courses_v2(group)})).reset_index()
    eligible_courses_comprehensive_data = eligible_courses_comprehensive_data.merge(recommended_courses_eng_lin_v2, on=['Student_ID', 'Semester'])

    recommended_courses = recommended_courses_eng_lin.merge(recommended_courses_eng_lin_v2,on=['Student_ID', 'Semester'])

    # Create summary DataFrames for eligible courses
    summary_area_of_study_eligible = eligible_courses_comprehensive_data.groupby(['Student_ID', 'AREA_OF_STUDY']).size().reset_index(name='Total_Eligible_Courses')
    summary_course_of_study_eligible = eligible_courses_comprehensive_data.groupby(['Student_ID', 'COURSE_OF_STUDY']).size().reset_index(name='Total_Eligible_Courses')
    summary_area_of_study_eligible = summary_area_of_study_eligible.pivot_table(index="Student_ID", columns="AREA_OF_STUDY", values="Total_Eligible_Courses", fill_value=0).reset_index()

    return requirements_eng_lin_,student_progress,summary_area_of_study_taken,remaining_courses_df,latest_eligible_courses,eligible_courses_comprehensive_data,recommended_courses,summary_area_of_study_eligible

def process_data_eng_edu(st_hist_data,major_data, requirements_weights_path):
    
    values_to_delete = ['FA', 'F', 'I', 'S', 'NP', 'WA']
    failed_grades = ['F','FA','NP']
    failed_data = st_hist_data[st_hist_data["GRADE"].isin(failed_grades)]
    st_hist_data = st_hist_data[~st_hist_data["GRADE"].isin(values_to_delete)]
    
    # Filtering and Sorting Data
    failed_data = failed_data[failed_data['Major'] == 'English Education']
    failed_data = failed_data.sort_values(by=['Student_ID', 'Semester'])

    grouped_data_failed = failed_data.groupby(['Student_ID'])['Course_ID'].apply(list).reset_index()
    
    # Filtering and Sorting Data
    eng_edu_data = st_hist_data[st_hist_data['Major'] == "English Education"]
    eng_edu_data = eng_edu_data.sort_values(by=['Student_ID', 'Semester'])

    major = major_data["All_Courses"]
    courses_co = major_data["CO_Courses"]
    
    major["AREA_OF_STUDY"] = major["AREA_OF_STUDY"].fillna("NA")
    # Dropping records where AREA_OF_STUDY is 'N' and COURSE_OF_STUDY is 'Z'
    major_filtered = major[~((major['AREA_OF_STUDY'] == 'NA') & (major['COURSE_OF_STUDY'] == 'Z'))]
    
    major_filtered = major_filtered.copy()
    # Apply replacements directly to the specific columns to avoid SettingWithCopyWarning
    major_filtered['AREA_OF_STUDY'] = major_filtered['AREA_OF_STUDY'].replace("NA","GE")
    major_filtered['COURSE_OF_STUDY'] = major_filtered['COURSE_OF_STUDY'].replace("N","E")
    
    # Defining the major lists
    cas_majors = ['COMSCIENCE', 'ENGLISH', 'LINGUISTIC', 'LITERATURE', 'DIGITALMED', 'PR / ADV', 'VISUAL COM']
    df_cas = major_filtered[major_filtered['Major'].isin(cas_majors)]
    df_cas = df_cas[~((df_cas['AREA_OF_STUDY'] == 'GE') & (df_cas['COURSE_OF_STUDY'] == 'E'))]
    
    list_conditions = ['-', 'ONE_COURSE']

    cas_list = df_cas[df_cas['Condition'].isin(list_conditions)]
    cas_special_cases = df_cas[~df_cas['Condition'].isin(list_conditions)]
    cas_co = courses_co[courses_co['Major'].isin(cas_majors)]
    
    eng_edu_list = cas_list[cas_list["Major"] == "ENGLISH"]
    eng_edu_special_cases = cas_special_cases[cas_special_cases["Major"] == "ENGLISH"]
    eng_edu_co = cas_co[cas_co["Major"] == "ENGLISH"]

    # Process 'REQUISITES_LIST'
    eng_edu_co = eng_edu_co.copy()
    eng_edu_co.loc[:, 'REQUISITES_LIST'] = eng_edu_co['REQUISITES_LIST'].apply(ast.literal_eval)

    # CAS Courses
    cas_courses = major_filtered[major_filtered['Major'].isin(cas_majors)]
    courses_eng_edu = cas_courses[cas_courses["Major"] == "ENGLISH"]
    
    grouped_data_eng_edu = eng_edu_data.groupby(['Student_ID'])['Course_ID'].apply(list).reset_index()

    # Merge dataframes
    merged_df = grouped_data_failed.merge(grouped_data_eng_edu, on=['Student_ID'], how='outer', suffixes=('_failed', '_all'))
    # Replace NaN with empty lists to avoid errors
    merged_df['Course_ID_all'] = merged_df['Course_ID_all'].apply(lambda x: x if isinstance(x, list) else [])
    merged_df['Course_ID_failed'] = merged_df['Course_ID_failed'].apply(lambda x: x if isinstance(x, list) else [])

    merged_df['Failed_Courses'] = merged_df.apply(
        lambda row: list(set(row['Course_ID_failed']) - set(row['Course_ID_all'])),
        axis=1)
    # Keep only relevant columns
    merged_df = merged_df[['Student_ID', 'Failed_Courses']]

    # Extract Accounting specific requirements and weights from respective DataFrames
    requirements_df = pd.read_excel(requirements_weights_path,sheet_name="requirements")
    weights_df = pd.read_excel(requirements_weights_path,sheet_name="weights")
    requirements_eng_edu = requirements_df[requirements_df["Major"] == "English Education"]
    requirements_eng_edu_ = requirements_eng_edu.pivot_table(index="Major",columns="AREA_OF_STUDY",values ='Required_Courses' ,aggfunc='sum',fill_value=0).reset_index()
    weights_eng_edu = weights_df[weights_df["Major"] == "English Education"]

    student_courses = eng_edu_data[["Student_ID", "Course_ID"]]

    # Map AREA_OF_STUDY and COURSE_OF_STUDY to eng_edu_data
    student_courses = student_courses.merge(courses_eng_edu[['Course_ID', 'AREA_OF_STUDY', 'COURSE_OF_STUDY', "Course_Level"]],
                                            on='Course_ID', how='left').drop_duplicates()

    # Create summary DataFrames for taken courses
    student_progress = student_courses.groupby(['Student_ID', 'AREA_OF_STUDY']).size().reset_index(name='Total_Taken_Courses')
    student_progress = student_progress.merge(requirements_eng_edu, on='AREA_OF_STUDY', how='left')
    student_progress["Remaining_Courses"] = student_progress["Required_Courses"] - student_progress["Total_Taken_Courses"]
    student_progress["Remaining_Courses"] = student_progress["Remaining_Courses"].apply(lambda x: max(x, 0))

    free_elective_taken_counts = student_courses[(student_courses['AREA_OF_STUDY'] == "GE") & (student_courses['COURSE_OF_STUDY'] == "E")].groupby('Student_ID').size().reset_index(name='Total_Free_Electives_Taken')

    # Update progress by including the free elective data
    student_progress["Student_Progress"] = (student_progress["Total_Taken_Courses"] / student_progress["Required_Courses"]) * 100
    student_progress["Student_Progress"].replace([np.inf, -np.inf], 100, inplace=True)

    summary_area_of_study_taken = student_progress.pivot_table(index="Student_ID", columns="AREA_OF_STUDY", values="Total_Taken_Courses", fill_value=0)
    summary_area_of_study_taken = summary_area_of_study_taken.merge(free_elective_taken_counts, on="Student_ID", how="left").fillna(0).rename(columns={"Total_Free_Electives_Taken": "FE"})

    # Create a copy of summary_area_of_study_taken to work on remaining courses calculation
    remaining_courses_df = summary_area_of_study_taken.copy()

    # Loop through each AREA_OF_STUDY and calculate remaining courses by subtracting from the requirements
    for column in remaining_courses_df.columns:
        if column in requirements_eng_edu['AREA_OF_STUDY'].values:
            required_courses = requirements_eng_edu.loc[requirements_eng_edu['AREA_OF_STUDY'] == column, 'Required_Courses'].values[0]
            remaining_courses_df[column] = required_courses - remaining_courses_df[column]
            remaining_courses_df[column] = remaining_courses_df[column].clip(lower=0)

    # Calculate weighted remaining courses
    weighted_remaining_courses_df = remaining_courses_df.copy()
    for column in weighted_remaining_courses_df.columns:
        if column in weights_eng_edu['AREA_OF_STUDY'].values:
            weight_value = weights_eng_edu.loc[weights_eng_edu['AREA_OF_STUDY'] == column, 'Weight'].values[0]
            weighted_remaining_courses_df[column] = weighted_remaining_courses_df[column] * weight_value

    # Prepare weighted remaining courses for merge
    weighted_remaining_courses_df = weighted_remaining_courses_df.reset_index().melt(id_vars=['Student_ID'],
                                                                                      var_name='AREA_OF_STUDY',
                                                                                      value_name='Remaining_Courses_Weight_Score')
    weighted_remaining_courses_df = weighted_remaining_courses_df[weighted_remaining_courses_df["AREA_OF_STUDY"] != "index"]

    # Eligibility Calculation for Standard and Special Cases
    prerequisites_eng_edu = eng_edu_list.set_index('Course_ID')['REQUISITES_LIST'].apply(eval).to_dict()
    prerequisites_special_eng_edu = eng_edu_special_cases.set_index('Course_ID')['REQUISITES_LIST'].apply(eval).to_dict()
    conditions_eng_edu = eng_edu_special_cases.set_index('Course_ID')['Condition'].to_dict()

    final_results_eng_edu = []  # Standard eligibility results
    final_results_special_eng_edu = []  # Special eligibility results

    for student_id, group in eng_edu_data.groupby('Student_ID'):
        cumulative_courses = set()
        for semester, semester_group in group.groupby('Semester'):
            taken_courses = set(semester_group['Course_ID'].tolist())
            cumulative_courses.update(taken_courses)

            # Determine Standard Eligible Courses
            student_info = semester_group.iloc[0].to_dict()
            eligible_courses = {course for course in prerequisites_eng_edu.keys() if all(req in cumulative_courses for req in prerequisites_eng_edu[course])}
            final_results_eng_edu.append({
                'Student_ID': student_id,
                'Semester': semester,
                'Major': student_info['Major'],
                'College': student_info['College'],
                'Program': student_info['Program'],
                'Passed Credits': student_info['Passed Credits'],
                'Student_Level': student_info['Student_Level'],
                'Eligible_Courses': list(eligible_courses - cumulative_courses)
            })

            # Determine Special Eligible Courses
            special_eligible_courses = {
                course for course in prerequisites_special_eng_edu.keys()
                if is_eligible_special_eng_edu(course, cumulative_courses, student_info, prerequisites_special_eng_edu, conditions_eng_edu)
            }
            final_results_special_eng_edu.append({
                'Student_ID': student_id,
                'Semester': semester,
                'Major': student_info['Major'],
                'College': student_info['College'],
                'Program': student_info['Program'],
                'Passed Credits': student_info['Passed Credits'],
                'Student_Level': student_info['Student_Level'],
                'Eligible_Courses': list(special_eligible_courses - cumulative_courses)
            })

    # Convert Results to DataFrames
    final_results_df_eng_edu = pd.DataFrame(final_results_eng_edu)
    final_results_special_df_eng_edu = pd.DataFrame(final_results_special_eng_edu)
    
    # Combine Eligible Courses from Both DataFrames
    combined_eng_edu_list = combine_eligible_courses(final_results_df_eng_edu, final_results_special_df_eng_edu)
    # Find Course Combinations for Co-requisites
    combined_eng_edu_list = combined_eng_edu_list.apply(create_combined_courses, axis=1, co=eng_edu_co)
    latest_eligible_courses = combined_eng_edu_list.sort_values(by='Semester', ascending=False)
    latest_eligible_courses = latest_eligible_courses.groupby('Student_ID').first().reset_index()
    latest_eligible_courses = latest_eligible_courses.merge(grouped_data_eng_edu,on = "Student_ID",how = "inner")
    latest_eligible_courses["Eligible_Courses_CO"] = latest_eligible_courses.apply(remove_matches, axis=1)
    latest_eligible_courses = latest_eligible_courses.apply(process_row, axis=1)
    latest_eligible_courses.drop(columns=["Course_ID"], inplace=True)

    latest_eligible_courses = latest_eligible_courses.merge(merged_df, on='Student_ID', how='outer')
    latest_eligible_courses['Failed_Courses'] = latest_eligible_courses['Failed_Courses'].apply(lambda x: x if isinstance(x, list) else [])
    latest_eligible_courses['Eligible_Courses_CO'] = latest_eligible_courses['Eligible_Courses_CO'].apply(lambda x: x if isinstance(x, list) else [])
    latest_eligible_courses['Eligible_Courses_CO'] = latest_eligible_courses.apply(
        lambda row: list(set(row['Eligible_Courses_CO']) | (set(row['Failed_Courses']) - set(row['Eligible_Courses_CO']))),axis=1)
    latest_eligible_courses = latest_eligible_courses.drop(columns=['Failed_Courses'])

    latest_info_failed = failed_data.loc[failed_data.groupby("Student_ID")["Semester"].idxmax()]
    missing_semester_df = latest_eligible_courses[latest_eligible_courses['Semester'].isna()]
    latest_eligible_courses.dropna(inplace=True)
    columns_to_fill = ['Semester', 'Major', 'College', 'Program', 'Passed Credits', 'Student_Level']

    for col in columns_to_fill:
        missing_semester_df.loc[missing_semester_df[col].isna(), col] = missing_semester_df.loc[
            missing_semester_df[col].isna(), 'Student_ID'
        ].map(latest_info_failed.set_index('Student_ID')[col])

    columns_to_convert = ['Semester', 'Student_Level', 'Passed Credits']
    for col in columns_to_convert:
        latest_eligible_courses.loc[:, col] = pd.to_numeric(latest_eligible_courses[col], errors='coerce').astype('Int64')
        
    latest_eligible_courses = pd.concat([latest_eligible_courses, missing_semester_df], ignore_index=True)

    max_semester_index = eng_edu_data.groupby('Student_ID')['Semester'].idxmax()
    max_semester_data = eng_edu_data.loc[max_semester_index, ['Student_ID', 'Semester']]

    last_semester_courses = pd.merge(max_semester_data, eng_edu_data, on=['Student_ID', 'Semester'])
    eng097_fpu_students = last_semester_courses[last_semester_courses['Course_ID'] == 'ENGL097']
    # Target course list
    target_courses = ['ENGL098', 'MATH094', 'MATH095', 'MATH096', 'MATH098', 'MATH100', 'MATH111', 'MATH120', 'MATH121', 'MATH131', 'MATH140']

    eng097_fpu_students_eligible = latest_eligible_courses[latest_eligible_courses['Student_ID']
                                                       .isin(eng097_fpu_students['Student_ID'])].copy()
    eng097_fpu_students_eligible.loc[:, 'Eligible_Courses_CO'] = eng097_fpu_students_eligible['Eligible_Courses_CO'].apply(
    lambda courses: [course for course in courses if course in target_courses])

    latest_eligible_courses = latest_eligible_courses.merge(
    eng097_fpu_students_eligible[['Student_ID', 'Eligible_Courses_CO']],  # Relevant columns from filtered_students
    on='Student_ID',
    how='left',  # Keep all rows in students_df
    suffixes=('', '_updated'))  # Suffix to differentiate new column)

    latest_eligible_courses['Eligible_Courses_CO'] = latest_eligible_courses['Eligible_Courses_CO_updated'].combine_first(latest_eligible_courses['Eligible_Courses_CO'])
    latest_eligible_courses = latest_eligible_courses.drop(columns=['Eligible_Courses_CO_updated'])
    latest_eligible_courses = latest_eligible_courses.merge(grouped_data_eng_edu,on = "Student_ID",how = "outer")
    latest_eligible_courses['Course_ID'] = latest_eligible_courses['Course_ID'].apply(lambda x: x if isinstance(x, list) else [])
    latest_eligible_courses = latest_eligible_courses.apply(process_row, axis=1)
    latest_eligible_courses.drop(columns=["Course_ID"], inplace=True)

    # Exploding DataFrame and mapping course details
    eligible_courses_comprehensive_data = latest_eligible_courses.explode("Eligible_Courses_CO")
    eligible_courses_comprehensive_data = eligible_courses_comprehensive_data.merge(courses_eng_edu[['Course_ID', 'AREA_OF_STUDY', 'COURSE_OF_STUDY', 'Course_Level']],
                                                                                    left_on='Eligible_Courses_CO', right_on='Course_ID', how='left').drop(columns="Course_ID")
    eligible_courses_comprehensive_data['Eligible_Courses_CO'] = eligible_courses_comprehensive_data['Eligible_Courses_CO'].apply(lambda x: x if isinstance(x, list) else ([] if pd.isna(x) else [x]))

    # Find Additional Eligibilities
    eligible_courses_comprehensive_data['Future_Eligible_Courses_List'] = eligible_courses_comprehensive_data.apply(lambda row: find_additional_eligibilities(row['Eligible_Courses_CO'], set(row['Eligible_Courses_CO']), prerequisites_eng_edu), axis=1)
    eligible_courses_per_student = eligible_courses_comprehensive_data.groupby('Student_ID')['Eligible_Courses_CO'].agg(lambda x: list(set([item for sublist in x for item in sublist if isinstance(sublist, list)]))).reset_index()

    # Merge aggregated list back to the comprehensive DataFrame
    eligible_courses_comprehensive_data = eligible_courses_comprehensive_data.merge(eligible_courses_per_student.rename(columns={'Eligible_Courses_CO': 'Eligible_Courses_List_All'}), on='Student_ID', how='left')

    # Filter matching courses from future eligible lists
    eligible_courses_comprehensive_data['Future_Eligible_Courses_List'] = eligible_courses_comprehensive_data.apply(lambda row: [course for course in row['Future_Eligible_Courses_List'] if course not in row['Eligible_Courses_List_All']], axis=1)
    eligible_courses_comprehensive_data['Total_Future_Eligible_Courses_List'] = eligible_courses_comprehensive_data['Future_Eligible_Courses_List'].apply(len)

    # Special eligibility courses
    eligible_courses_comprehensive_data['Future_Eligible_Courses_Special'] = eligible_courses_comprehensive_data.apply(lambda row: find_additional_eligibilities_special(row['Eligible_Courses_CO'], set(row['Eligible_Courses_CO']), row, prerequisites_special_eng_edu, conditions_eng_edu, is_eligible_special_eng_edu_), axis=1)
    eligible_courses_comprehensive_data['Future_Eligible_Courses_Special'] = eligible_courses_comprehensive_data.apply(lambda row: [course for course in row['Future_Eligible_Courses_Special'] if course not in row['Eligible_Courses_List_All']], axis=1)
    eligible_courses_comprehensive_data['Total_Future_Eligible_Courses_Special'] = eligible_courses_comprehensive_data['Future_Eligible_Courses_Special'].apply(len)

    # Combine Future Eligible Courses and calculate the score
    eligible_courses_comprehensive_data["Future_Eligible_Courses"] = eligible_courses_comprehensive_data["Future_Eligible_Courses_List"] + eligible_courses_comprehensive_data["Future_Eligible_Courses_Special"]
    eligible_courses_comprehensive_data['Course_Score'] = eligible_courses_comprehensive_data['Future_Eligible_Courses'].apply(len)

    # Find Best Courses
    recommended_courses_eng_edu = eligible_courses_comprehensive_data.groupby(['Student_ID', 'Semester']).apply(lambda group: pd.Series({'Recommended_Courses': find_best_courses(group)})).reset_index()


    eligible_courses_comprehensive_data = eligible_courses_comprehensive_data.merge(recommended_courses_eng_edu, on=['Student_ID', 'Semester'])
    eligible_courses_comprehensive_data = eligible_courses_comprehensive_data.merge(weighted_remaining_courses_df, on=['Student_ID', 'AREA_OF_STUDY'], how='left')


    eligible_courses_comprehensive_data = eligible_courses_comprehensive_data.groupby('Student_ID', group_keys=False).apply(normalize_by_student)
    eligible_courses_comprehensive_data['Final_Score'] = (
        (eligible_courses_comprehensive_data['Normalized_Course_Score'] * 0.4) +
        (eligible_courses_comprehensive_data['Normalized_Remaining_Courses_Weight'] * 0.4) +
        (eligible_courses_comprehensive_data['Normalized_Course_Level'] * 0.2))

    # Find Best Courses
    recommended_courses_eng_edu_v2 = eligible_courses_comprehensive_data.groupby(['Student_ID', 'Semester']).apply(lambda group: pd.Series({'Recommended_Courses_V2': find_best_courses_v2(group)})).reset_index()
    eligible_courses_comprehensive_data = eligible_courses_comprehensive_data.merge(recommended_courses_eng_edu_v2, on=['Student_ID', 'Semester'])

    recommended_courses = recommended_courses_eng_edu.merge(recommended_courses_eng_edu_v2,on=['Student_ID', 'Semester'])

    # Create summary DataFrames for eligible courses
    summary_area_of_study_eligible = eligible_courses_comprehensive_data.groupby(['Student_ID', 'AREA_OF_STUDY']).size().reset_index(name='Total_Eligible_Courses')
    summary_course_of_study_eligible = eligible_courses_comprehensive_data.groupby(['Student_ID', 'COURSE_OF_STUDY']).size().reset_index(name='Total_Eligible_Courses')
    summary_area_of_study_eligible = summary_area_of_study_eligible.pivot_table(index="Student_ID", columns="AREA_OF_STUDY", values="Total_Eligible_Courses", fill_value=0).reset_index()

    return requirements_eng_edu_,student_progress,summary_area_of_study_taken,remaining_courses_df,latest_eligible_courses,eligible_courses_comprehensive_data,recommended_courses,summary_area_of_study_eligible

def process_data_eng_lit(st_hist_data,major_data, requirements_weights_path):
    
    values_to_delete = ['FA', 'F', 'I', 'S', 'NP', 'WA']
    failed_grades = ['F','FA','NP']
    failed_data = st_hist_data[st_hist_data["GRADE"].isin(failed_grades)]
    st_hist_data = st_hist_data[~st_hist_data["GRADE"].isin(values_to_delete)]
    
    # Filtering and Sorting Data
    failed_data = failed_data[failed_data['Major'] == 'English Literature']
    failed_data = failed_data.sort_values(by=['Student_ID', 'Semester'])

    grouped_data_failed = failed_data.groupby(['Student_ID'])['Course_ID'].apply(list).reset_index()
    
    # Filtering and Sorting Data
    eng_lit_data = st_hist_data[st_hist_data['Major'] == "English Literature"]
    eng_lit_data = eng_lit_data.sort_values(by=['Student_ID', 'Semester'])

    major = major_data["All_Courses"]
    courses_co = major_data["CO_Courses"]
    
    major["AREA_OF_STUDY"] = major["AREA_OF_STUDY"].fillna("NA")
    # Dropping records where AREA_OF_STUDY is 'N' and COURSE_OF_STUDY is 'Z'
    major_filtered = major[~((major['AREA_OF_STUDY'] == 'NA') & (major['COURSE_OF_STUDY'] == 'Z'))]
    
    major_filtered = major_filtered.copy()
    # Apply replacements directly to the specific columns to avoid SettingWithCopyWarning
    major_filtered['AREA_OF_STUDY'] = major_filtered['AREA_OF_STUDY'].replace("NA","GE")
    major_filtered['COURSE_OF_STUDY'] = major_filtered['COURSE_OF_STUDY'].replace("N","E")
    
    # Defining the major lists
    cas_majors = ['COMSCIENCE', 'ENGLISH', 'LINGUISTIC', 'LITERATURE', 'DIGITALMED', 'PR / ADV', 'VISUAL COM']
    df_cas = major_filtered[major_filtered['Major'].isin(cas_majors)]
    df_cas = df_cas[~((df_cas['AREA_OF_STUDY'] == 'GE') & (df_cas['COURSE_OF_STUDY'] == 'E'))]
    
    list_conditions = ['-', 'ONE_COURSE']

    cas_list = df_cas[df_cas['Condition'].isin(list_conditions)]
    cas_special_cases = df_cas[~df_cas['Condition'].isin(list_conditions)]
    cas_co = courses_co[courses_co['Major'].isin(cas_majors)]
    
    eng_lit_list = cas_list[cas_list["Major"] == "LITERATURE"]
    eng_lit_special_cases = cas_special_cases[cas_special_cases["Major"] == "LITERATURE"]
    eng_lit_co = cas_co[cas_co["Major"] == "LITERATURE"]

    # Process 'REQUISITES_LIST'
    eng_lit_co = eng_lit_co.copy()
    eng_lit_co.loc[:, 'REQUISITES_LIST'] = eng_lit_co['REQUISITES_LIST'].apply(ast.literal_eval)

    # CAS Courses
    cas_courses = major_filtered[major_filtered['Major'].isin(cas_majors)]
    courses_eng_lit = cas_courses[cas_courses["Major"] == "LITERATURE"]
    
    grouped_data_eng_lit = eng_lit_data.groupby(['Student_ID'])['Course_ID'].apply(list).reset_index()

    # Merge dataframes
    merged_df = grouped_data_failed.merge(grouped_data_eng_lit, on=['Student_ID'], how='outer', suffixes=('_failed', '_all'))
    # Replace NaN with empty lists to avoid errors
    merged_df['Course_ID_all'] = merged_df['Course_ID_all'].apply(lambda x: x if isinstance(x, list) else [])
    merged_df['Course_ID_failed'] = merged_df['Course_ID_failed'].apply(lambda x: x if isinstance(x, list) else [])

    merged_df['Failed_Courses'] = merged_df.apply(
        lambda row: list(set(row['Course_ID_failed']) - set(row['Course_ID_all'])),
        axis=1)
    # Keep only relevant columns
    merged_df = merged_df[['Student_ID', 'Failed_Courses']]

    # Extract Accounting specific requirements and weights from respective DataFrames
    requirements_df = pd.read_excel(requirements_weights_path,sheet_name="requirements")
    weights_df = pd.read_excel(requirements_weights_path,sheet_name="weights")
    requirements_eng_lit = requirements_df[requirements_df["Major"] == "English Literature"]
    requirements_eng_lit_ = requirements_eng_lit.pivot_table(index="Major",columns="AREA_OF_STUDY",values ='Required_Courses' ,aggfunc='sum',fill_value=0).reset_index()
    weights_eng_lit = weights_df[weights_df["Major"] == "English Literature"]

    student_courses = eng_lit_data[["Student_ID", "Course_ID"]]

    # Map AREA_OF_STUDY and COURSE_OF_STUDY to eng_lit_data
    student_courses = student_courses.merge(courses_eng_lit[['Course_ID', 'AREA_OF_STUDY', 'COURSE_OF_STUDY', "Course_Level"]],
                                            on='Course_ID', how='left').drop_duplicates()

    # Create summary DataFrames for taken courses
    student_progress = student_courses.groupby(['Student_ID', 'AREA_OF_STUDY']).size().reset_index(name='Total_Taken_Courses')
    student_progress = student_progress.merge(requirements_eng_lit, on='AREA_OF_STUDY', how='left')
    student_progress["Remaining_Courses"] = student_progress["Required_Courses"] - student_progress["Total_Taken_Courses"]
    student_progress["Remaining_Courses"] = student_progress["Remaining_Courses"].apply(lambda x: max(x, 0))

    free_elective_taken_counts = student_courses[(student_courses['AREA_OF_STUDY'] == "GE") & (student_courses['COURSE_OF_STUDY'] == "E")].groupby('Student_ID').size().reset_index(name='Total_Free_Electives_Taken')

    # Update progress by including the free elective data
    student_progress["Student_Progress"] = (student_progress["Total_Taken_Courses"] / student_progress["Required_Courses"]) * 100
    student_progress["Student_Progress"].replace([np.inf, -np.inf], 100, inplace=True)

    summary_area_of_study_taken = student_progress.pivot_table(index="Student_ID", columns="AREA_OF_STUDY", values="Total_Taken_Courses", fill_value=0)
    summary_area_of_study_taken = summary_area_of_study_taken.merge(free_elective_taken_counts, on="Student_ID", how="left").fillna(0).rename(columns={"Total_Free_Electives_Taken": "FE"})

    # Create a copy of summary_area_of_study_taken to work on remaining courses calculation
    remaining_courses_df = summary_area_of_study_taken.copy()

    # Loop through each AREA_OF_STUDY and calculate remaining courses by subtracting from the requirements
    for column in remaining_courses_df.columns:
        if column in requirements_eng_lit['AREA_OF_STUDY'].values:
            required_courses = requirements_eng_lit.loc[requirements_eng_lit['AREA_OF_STUDY'] == column, 'Required_Courses'].values[0]
            remaining_courses_df[column] = required_courses - remaining_courses_df[column]
            remaining_courses_df[column] = remaining_courses_df[column].clip(lower=0)

    # Calculate weighted remaining courses
    weighted_remaining_courses_df = remaining_courses_df.copy()
    for column in weighted_remaining_courses_df.columns:
        if column in weights_eng_lit['AREA_OF_STUDY'].values:
            weight_value = weights_eng_lit.loc[weights_eng_lit['AREA_OF_STUDY'] == column, 'Weight'].values[0]
            weighted_remaining_courses_df[column] = weighted_remaining_courses_df[column] * weight_value

    # Prepare weighted remaining courses for merge
    weighted_remaining_courses_df = weighted_remaining_courses_df.reset_index().melt(id_vars=['Student_ID'],
                                                                                      var_name='AREA_OF_STUDY',
                                                                                      value_name='Remaining_Courses_Weight_Score')
    weighted_remaining_courses_df = weighted_remaining_courses_df[weighted_remaining_courses_df["AREA_OF_STUDY"] != "index"]

    # Eligibility Calculation for Standard and Special Cases
    prerequisites_eng_lit = eng_lit_list.set_index('Course_ID')['REQUISITES_LIST'].apply(eval).to_dict()
    prerequisites_special_eng_lit = eng_lit_special_cases.set_index('Course_ID')['REQUISITES_LIST'].apply(eval).to_dict()
    conditions_eng_lit = eng_lit_special_cases.set_index('Course_ID')['Condition'].to_dict()

    final_results_eng_lit = []  # Standard eligibility results
    final_results_special_eng_lit = []  # Special eligibility results

    for student_id, group in eng_lit_data.groupby('Student_ID'):
        cumulative_courses = set()
        for semester, semester_group in group.groupby('Semester'):
            taken_courses = set(semester_group['Course_ID'].tolist())
            cumulative_courses.update(taken_courses)

            # Determine Standard Eligible Courses
            student_info = semester_group.iloc[0].to_dict()
            eligible_courses = {course for course in prerequisites_eng_lit.keys() if all(req in cumulative_courses for req in prerequisites_eng_lit[course])}
            final_results_eng_lit.append({
                'Student_ID': student_id,
                'Semester': semester,
                'Major': student_info['Major'],
                'College': student_info['College'],
                'Program': student_info['Program'],
                'Passed Credits': student_info['Passed Credits'],
                'Student_Level': student_info['Student_Level'],
                'Eligible_Courses': list(eligible_courses - cumulative_courses)
            })

            # Determine Special Eligible Courses
            special_eligible_courses = {
                course for course in prerequisites_special_eng_lit.keys()
                if is_eligible_special_eng_lit(course, cumulative_courses, student_info, prerequisites_special_eng_lit, conditions_eng_lit)
            }
            final_results_special_eng_lit.append({
                'Student_ID': student_id,
                'Semester': semester,
                'Major': student_info['Major'],
                'College': student_info['College'],
                'Program': student_info['Program'],
                'Passed Credits': student_info['Passed Credits'],
                'Student_Level': student_info['Student_Level'],
                'Eligible_Courses': list(special_eligible_courses - cumulative_courses)
            })

    # Convert Results to DataFrames
    final_results_df_eng_lit = pd.DataFrame(final_results_eng_lit)
    final_results_special_df_eng_lit = pd.DataFrame(final_results_special_eng_lit)
    
    # Combine Eligible Courses from Both DataFrames
    combined_eng_lit_list = combine_eligible_courses(final_results_df_eng_lit, final_results_special_df_eng_lit)
    # Find Course Combinations for Co-requisites
    combined_eng_lit_list = combined_eng_lit_list.apply(create_combined_courses, axis=1, co=eng_lit_co)
    latest_eligible_courses = combined_eng_lit_list.sort_values(by='Semester', ascending=False)
    latest_eligible_courses = latest_eligible_courses.groupby('Student_ID').first().reset_index()
    latest_eligible_courses = latest_eligible_courses.merge(grouped_data_eng_lit,on = "Student_ID",how = "inner")
    latest_eligible_courses["Eligible_Courses_CO"] = latest_eligible_courses.apply(remove_matches, axis=1)
    latest_eligible_courses = latest_eligible_courses.apply(process_row, axis=1)
    latest_eligible_courses.drop(columns=["Course_ID"], inplace=True)

    latest_eligible_courses = latest_eligible_courses.merge(merged_df, on='Student_ID', how='outer')
    latest_eligible_courses['Failed_Courses'] = latest_eligible_courses['Failed_Courses'].apply(lambda x: x if isinstance(x, list) else [])
    latest_eligible_courses['Eligible_Courses_CO'] = latest_eligible_courses['Eligible_Courses_CO'].apply(lambda x: x if isinstance(x, list) else [])
    latest_eligible_courses['Eligible_Courses_CO'] = latest_eligible_courses.apply(
        lambda row: list(set(row['Eligible_Courses_CO']) | (set(row['Failed_Courses']) - set(row['Eligible_Courses_CO']))),axis=1)
    latest_eligible_courses = latest_eligible_courses.drop(columns=['Failed_Courses'])

    latest_info_failed = failed_data.loc[failed_data.groupby("Student_ID")["Semester"].idxmax()]
    missing_semester_df = latest_eligible_courses[latest_eligible_courses['Semester'].isna()]
    latest_eligible_courses.dropna(inplace=True)
    columns_to_fill = ['Semester', 'Major', 'College', 'Program', 'Passed Credits', 'Student_Level']

    for col in columns_to_fill:
        missing_semester_df.loc[missing_semester_df[col].isna(), col] = missing_semester_df.loc[
            missing_semester_df[col].isna(), 'Student_ID'
        ].map(latest_info_failed.set_index('Student_ID')[col])

    columns_to_convert = ['Semester', 'Student_Level', 'Passed Credits']
    for col in columns_to_convert:
        latest_eligible_courses.loc[:, col] = pd.to_numeric(latest_eligible_courses[col], errors='coerce').astype('Int64')
        
    latest_eligible_courses = pd.concat([latest_eligible_courses, missing_semester_df], ignore_index=True)

    max_semester_index = eng_lit_data.groupby('Student_ID')['Semester'].idxmax()
    max_semester_data = eng_lit_data.loc[max_semester_index, ['Student_ID', 'Semester']]

    last_semester_courses = pd.merge(max_semester_data, eng_lit_data, on=['Student_ID', 'Semester'])
    eng097_fpu_students = last_semester_courses[last_semester_courses['Course_ID'] == 'ENGL097']
    # Target course list
    target_courses = ['ENGL098', 'MATH094', 'MATH095', 'MATH096', 'MATH098', 'MATH100', 'MATH111', 'MATH120', 'MATH121', 'MATH131', 'MATH140']

    eng097_fpu_students_eligible = latest_eligible_courses[latest_eligible_courses['Student_ID']
                                                       .isin(eng097_fpu_students['Student_ID'])].copy()
    eng097_fpu_students_eligible.loc[:, 'Eligible_Courses_CO'] = eng097_fpu_students_eligible['Eligible_Courses_CO'].apply(
    lambda courses: [course for course in courses if course in target_courses])

    latest_eligible_courses = latest_eligible_courses.merge(
    eng097_fpu_students_eligible[['Student_ID', 'Eligible_Courses_CO']],  # Relevant columns from filtered_students
    on='Student_ID',
    how='left',  # Keep all rows in students_df
    suffixes=('', '_updated'))  # Suffix to differentiate new column)

    latest_eligible_courses['Eligible_Courses_CO'] = latest_eligible_courses['Eligible_Courses_CO_updated'].combine_first(latest_eligible_courses['Eligible_Courses_CO'])
    latest_eligible_courses = latest_eligible_courses.drop(columns=['Eligible_Courses_CO_updated'])
    latest_eligible_courses = latest_eligible_courses.merge(grouped_data_eng_lit,on = "Student_ID",how = "outer")
    latest_eligible_courses['Course_ID'] = latest_eligible_courses['Course_ID'].apply(lambda x: x if isinstance(x, list) else [])
    latest_eligible_courses = latest_eligible_courses.apply(process_row, axis=1)
    latest_eligible_courses.drop(columns=["Course_ID"], inplace=True)

    # Exploding DataFrame and mapping course details
    eligible_courses_comprehensive_data = latest_eligible_courses.explode("Eligible_Courses_CO")
    eligible_courses_comprehensive_data = eligible_courses_comprehensive_data.merge(courses_eng_lit[['Course_ID', 'AREA_OF_STUDY', 'COURSE_OF_STUDY', 'Course_Level']],
                                                                                    left_on='Eligible_Courses_CO', right_on='Course_ID', how='left').drop(columns="Course_ID")
    eligible_courses_comprehensive_data['Eligible_Courses_CO'] = eligible_courses_comprehensive_data['Eligible_Courses_CO'].apply(lambda x: x if isinstance(x, list) else ([] if pd.isna(x) else [x]))

    # Find Additional Eligibilities
    eligible_courses_comprehensive_data['Future_Eligible_Courses_List'] = eligible_courses_comprehensive_data.apply(lambda row: find_additional_eligibilities(row['Eligible_Courses_CO'], set(row['Eligible_Courses_CO']), prerequisites_eng_lit), axis=1)
    eligible_courses_per_student = eligible_courses_comprehensive_data.groupby('Student_ID')['Eligible_Courses_CO'].agg(lambda x: list(set([item for sublist in x for item in sublist if isinstance(sublist, list)]))).reset_index()

    # Merge aggregated list back to the comprehensive DataFrame
    eligible_courses_comprehensive_data = eligible_courses_comprehensive_data.merge(eligible_courses_per_student.rename(columns={'Eligible_Courses_CO': 'Eligible_Courses_List_All'}), on='Student_ID', how='left')

    # Filter matching courses from future eligible lists
    eligible_courses_comprehensive_data['Future_Eligible_Courses_List'] = eligible_courses_comprehensive_data.apply(lambda row: [course for course in row['Future_Eligible_Courses_List'] if course not in row['Eligible_Courses_List_All']], axis=1)
    eligible_courses_comprehensive_data['Total_Future_Eligible_Courses_List'] = eligible_courses_comprehensive_data['Future_Eligible_Courses_List'].apply(len)

    # Special eligibility courses
    eligible_courses_comprehensive_data['Future_Eligible_Courses_Special'] = eligible_courses_comprehensive_data.apply(lambda row: find_additional_eligibilities_special(row['Eligible_Courses_CO'], set(row['Eligible_Courses_CO']), row, prerequisites_special_eng_lit, conditions_eng_lit, is_eligible_special_eng_lit_), axis=1)
    eligible_courses_comprehensive_data['Future_Eligible_Courses_Special'] = eligible_courses_comprehensive_data.apply(lambda row: [course for course in row['Future_Eligible_Courses_Special'] if course not in row['Eligible_Courses_List_All']], axis=1)
    eligible_courses_comprehensive_data['Total_Future_Eligible_Courses_Special'] = eligible_courses_comprehensive_data['Future_Eligible_Courses_Special'].apply(len)

    # Combine Future Eligible Courses and calculate the score
    eligible_courses_comprehensive_data["Future_Eligible_Courses"] = eligible_courses_comprehensive_data["Future_Eligible_Courses_List"] + eligible_courses_comprehensive_data["Future_Eligible_Courses_Special"]
    eligible_courses_comprehensive_data['Course_Score'] = eligible_courses_comprehensive_data['Future_Eligible_Courses'].apply(len)

    # Find Best Courses
    recommended_courses_eng_lit = eligible_courses_comprehensive_data.groupby(['Student_ID', 'Semester']).apply(lambda group: pd.Series({'Recommended_Courses': find_best_courses(group)})).reset_index()


    eligible_courses_comprehensive_data = eligible_courses_comprehensive_data.merge(recommended_courses_eng_lit, on=['Student_ID', 'Semester'])
    eligible_courses_comprehensive_data = eligible_courses_comprehensive_data.merge(weighted_remaining_courses_df, on=['Student_ID', 'AREA_OF_STUDY'], how='left')


    eligible_courses_comprehensive_data = eligible_courses_comprehensive_data.groupby('Student_ID', group_keys=False).apply(normalize_by_student)
    eligible_courses_comprehensive_data['Final_Score'] = (
        (eligible_courses_comprehensive_data['Normalized_Course_Score'] * 0.4) +
        (eligible_courses_comprehensive_data['Normalized_Remaining_Courses_Weight'] * 0.4) +
        (eligible_courses_comprehensive_data['Normalized_Course_Level'] * 0.2))

    # Find Best Courses
    recommended_courses_eng_lit_v2 = eligible_courses_comprehensive_data.groupby(['Student_ID', 'Semester']).apply(lambda group: pd.Series({'Recommended_Courses_V2': find_best_courses_v2(group)})).reset_index()
    eligible_courses_comprehensive_data = eligible_courses_comprehensive_data.merge(recommended_courses_eng_lit_v2, on=['Student_ID', 'Semester'])

    recommended_courses = recommended_courses_eng_lit.merge(recommended_courses_eng_lit_v2,on=['Student_ID', 'Semester'])

    # Create summary DataFrames for eligible courses
    summary_area_of_study_eligible = eligible_courses_comprehensive_data.groupby(['Student_ID', 'AREA_OF_STUDY']).size().reset_index(name='Total_Eligible_Courses')
    summary_course_of_study_eligible = eligible_courses_comprehensive_data.groupby(['Student_ID', 'COURSE_OF_STUDY']).size().reset_index(name='Total_Eligible_Courses')
    summary_area_of_study_eligible = summary_area_of_study_eligible.pivot_table(index="Student_ID", columns="AREA_OF_STUDY", values="Total_Eligible_Courses", fill_value=0).reset_index()

    return requirements_eng_lit_,student_progress,summary_area_of_study_taken,remaining_courses_df,latest_eligible_courses,eligible_courses_comprehensive_data,recommended_courses,summary_area_of_study_eligible

def process_data_pr(st_hist_data,major_data, requirements_weights_path):
    
    values_to_delete = ['FA', 'F', 'I', 'S', 'NP', 'WA']
    failed_grades = ['F','FA','NP']
    failed_data = st_hist_data[st_hist_data["GRADE"].isin(failed_grades)]
    st_hist_data = st_hist_data[~st_hist_data["GRADE"].isin(values_to_delete)]
    
    # Filtering and Sorting Data
    failed_data = failed_data[failed_data['Major'] == 'Public relations & Advertising']
    failed_data = failed_data.sort_values(by=['Student_ID', 'Semester'])

    grouped_data_failed = failed_data.groupby(['Student_ID'])['Course_ID'].apply(list).reset_index()
    
    # Filtering and Sorting Data
    pr_data = st_hist_data[st_hist_data['Major'] == "Public relations & Advertising"]
    pr_data = pr_data.sort_values(by=['Student_ID', 'Semester'])

    major = major_data["All_Courses"]
    courses_co = major_data["CO_Courses"]
    
    major["AREA_OF_STUDY"] = major["AREA_OF_STUDY"].fillna("NA")
    # Dropping records where AREA_OF_STUDY is 'N' and COURSE_OF_STUDY is 'Z'
    major_filtered = major[~((major['AREA_OF_STUDY'] == 'NA') & (major['COURSE_OF_STUDY'] == 'Z'))]
    
    major_filtered = major_filtered.copy()
    # Apply replacements directly to the specific columns to avoid SettingWithCopyWarning
    major_filtered['AREA_OF_STUDY'] = major_filtered['AREA_OF_STUDY'].replace("NA","GE")
    major_filtered['COURSE_OF_STUDY'] = major_filtered['COURSE_OF_STUDY'].replace("N","E")
    
    # Defining the major lists
    cas_majors = ['COMSCIENCE', 'ENGLISH', 'LINGUISTIC', 'LITERATURE', 'DIGITALMED', 'PR / ADV', 'VISUAL COM']
    df_cas = major_filtered[major_filtered['Major'].isin(cas_majors)]
    df_cas = df_cas[~((df_cas['AREA_OF_STUDY'] == 'GE') & (df_cas['COURSE_OF_STUDY'] == 'E'))]
    
    list_conditions = ['-', 'ONE_COURSE']

    cas_list = df_cas[df_cas['Condition'].isin(list_conditions)]
    cas_special_cases = df_cas[~df_cas['Condition'].isin(list_conditions)]
    cas_co = courses_co[courses_co['Major'].isin(cas_majors)]
    
    pr_list = cas_list[cas_list["Major"] == "PR / ADV"]
    pr_special_cases = cas_special_cases[cas_special_cases["Major"] == "PR / ADV"]
    pr_co = cas_co[cas_co["Major"] == "PR / ADV"]

    # Process 'REQUISITES_LIST'
    pr_co = pr_co.copy()
    pr_co.loc[:, 'REQUISITES_LIST'] = pr_co['REQUISITES_LIST'].apply(ast.literal_eval)

    # CAS Courses
    cas_courses = major_filtered[major_filtered['Major'].isin(cas_majors)]
    courses_pr = cas_courses[cas_courses["Major"] == "PR / ADV"]
    
    grouped_data_pr = pr_data.groupby(['Student_ID'])['Course_ID'].apply(list).reset_index()

    # Merge dataframes
    merged_df = grouped_data_failed.merge(grouped_data_pr, on=['Student_ID'], how='outer', suffixes=('_failed', '_all'))
    # Replace NaN with empty lists to avoid errors
    merged_df['Course_ID_all'] = merged_df['Course_ID_all'].apply(lambda x: x if isinstance(x, list) else [])
    merged_df['Course_ID_failed'] = merged_df['Course_ID_failed'].apply(lambda x: x if isinstance(x, list) else [])

    merged_df['Failed_Courses'] = merged_df.apply(
        lambda row: list(set(row['Course_ID_failed']) - set(row['Course_ID_all'])),
        axis=1)
    # Keep only relevant columns
    merged_df = merged_df[['Student_ID', 'Failed_Courses']]

    # Extract Accounting specific requirements and weights from respective DataFrames
    requirements_df = pd.read_excel(requirements_weights_path,sheet_name="requirements")
    weights_df = pd.read_excel(requirements_weights_path,sheet_name="weights")
    requirements_pr = requirements_df[requirements_df["Major"] == "Public relations & Advertising"]
    requirements_pr_ = requirements_pr.pivot_table(index="Major",columns="AREA_OF_STUDY",values ='Required_Courses' ,aggfunc='sum',fill_value=0).reset_index()
    weights_pr = weights_df[weights_df["Major"] == "Public relations & Advertising"]

    student_courses = pr_data[["Student_ID", "Course_ID"]]

    # Map AREA_OF_STUDY and COURSE_OF_STUDY to pr_data
    student_courses = student_courses.merge(courses_pr[['Course_ID', 'AREA_OF_STUDY', 'COURSE_OF_STUDY', "Course_Level"]],
                                            on='Course_ID', how='left').drop_duplicates()

    # Create summary DataFrames for taken courses
    student_progress = student_courses.groupby(['Student_ID', 'AREA_OF_STUDY']).size().reset_index(name='Total_Taken_Courses')
    student_progress = student_progress.merge(requirements_pr, on='AREA_OF_STUDY', how='left')
    student_progress["Remaining_Courses"] = student_progress["Required_Courses"] - student_progress["Total_Taken_Courses"]
    student_progress["Remaining_Courses"] = student_progress["Remaining_Courses"].apply(lambda x: max(x, 0))

    free_elective_taken_counts = student_courses[(student_courses['AREA_OF_STUDY'] == "GE") & (student_courses['COURSE_OF_STUDY'] == "E")].groupby('Student_ID').size().reset_index(name='Total_Free_Electives_Taken')

    # Update progress by including the free elective data
    student_progress["Student_Progress"] = (student_progress["Total_Taken_Courses"] / student_progress["Required_Courses"]) * 100
    student_progress["Student_Progress"].replace([np.inf, -np.inf], 100, inplace=True)

    summary_area_of_study_taken = student_progress.pivot_table(index="Student_ID", columns="AREA_OF_STUDY", values="Total_Taken_Courses", fill_value=0)
    summary_area_of_study_taken = summary_area_of_study_taken.merge(free_elective_taken_counts, on="Student_ID", how="left").fillna(0).rename(columns={"Total_Free_Electives_Taken": "FE"})

    # Create a copy of summary_area_of_study_taken to work on remaining courses calculation
    remaining_courses_df = summary_area_of_study_taken.copy()

    # Loop through each AREA_OF_STUDY and calculate remaining courses by subtracting from the requirements
    for column in remaining_courses_df.columns:
        if column in requirements_pr['AREA_OF_STUDY'].values:
            required_courses = requirements_pr.loc[requirements_pr['AREA_OF_STUDY'] == column, 'Required_Courses'].values[0]
            remaining_courses_df[column] = required_courses - remaining_courses_df[column]
            remaining_courses_df[column] = remaining_courses_df[column].clip(lower=0)

    # Calculate weighted remaining courses
    weighted_remaining_courses_df = remaining_courses_df.copy()
    for column in weighted_remaining_courses_df.columns:
        if column in weights_pr['AREA_OF_STUDY'].values:
            weight_value = weights_pr.loc[weights_pr['AREA_OF_STUDY'] == column, 'Weight'].values[0]
            weighted_remaining_courses_df[column] = weighted_remaining_courses_df[column] * weight_value

    # Prepare weighted remaining courses for merge
    weighted_remaining_courses_df = weighted_remaining_courses_df.reset_index().melt(id_vars=['Student_ID'],
                                                                                      var_name='AREA_OF_STUDY',
                                                                                      value_name='Remaining_Courses_Weight_Score')
    weighted_remaining_courses_df = weighted_remaining_courses_df[weighted_remaining_courses_df["AREA_OF_STUDY"] != "index"]

    # Eligibility Calculation for Standard and Special Cases
    prerequisites_pr = pr_list.set_index('Course_ID')['REQUISITES_LIST'].apply(eval).to_dict()
    prerequisites_special_pr = pr_special_cases.set_index('Course_ID')['REQUISITES_LIST'].apply(eval).to_dict()
    conditions_pr = pr_special_cases.set_index('Course_ID')['Condition'].to_dict()

    final_results_pr = []  # Standard eligibility results
    final_results_special_pr = []  # Special eligibility results

    for student_id, group in pr_data.groupby('Student_ID'):
        cumulative_courses = set()
        for semester, semester_group in group.groupby('Semester'):
            taken_courses = set(semester_group['Course_ID'].tolist())
            cumulative_courses.update(taken_courses)

            # Determine Standard Eligible Courses
            student_info = semester_group.iloc[0].to_dict()
            eligible_courses = {course for course in prerequisites_pr.keys() if all(req in cumulative_courses for req in prerequisites_pr[course])}
            final_results_pr.append({
                'Student_ID': student_id,
                'Semester': semester,
                'Major': student_info['Major'],
                'College': student_info['College'],
                'Program': student_info['Program'],
                'Passed Credits': student_info['Passed Credits'],
                'Student_Level': student_info['Student_Level'],
                'Eligible_Courses': list(eligible_courses - cumulative_courses)
            })

            # Determine Special Eligible Courses
            special_eligible_courses = {
                course for course in prerequisites_special_pr.keys()
                if is_eligible_special_pr(course, cumulative_courses, student_info, prerequisites_special_pr, conditions_pr)
            }
            final_results_special_pr.append({
                'Student_ID': student_id,
                'Semester': semester,
                'Major': student_info['Major'],
                'College': student_info['College'],
                'Program': student_info['Program'],
                'Passed Credits': student_info['Passed Credits'],
                'Student_Level': student_info['Student_Level'],
                'Eligible_Courses': list(special_eligible_courses - cumulative_courses)
            })

    # Convert Results to DataFrames
    final_results_df_pr = pd.DataFrame(final_results_pr)
    final_results_special_df_pr = pd.DataFrame(final_results_special_pr)
    
    # Combine Eligible Courses from Both DataFrames
    combined_pr_list = combine_eligible_courses(final_results_df_pr, final_results_special_df_pr)
    # Find Course Combinations for Co-requisites
    combined_pr_list = combined_pr_list.apply(create_combined_courses, axis=1, co=pr_co)
    latest_eligible_courses = combined_pr_list.sort_values(by='Semester', ascending=False)
    latest_eligible_courses = latest_eligible_courses.groupby('Student_ID').first().reset_index()
    latest_eligible_courses = latest_eligible_courses.merge(grouped_data_pr,on = "Student_ID",how = "inner")
    latest_eligible_courses["Eligible_Courses_CO"] = latest_eligible_courses.apply(remove_matches, axis=1)
    latest_eligible_courses = latest_eligible_courses.apply(process_row, axis=1)
    latest_eligible_courses.drop(columns=["Course_ID"], inplace=True)

    latest_eligible_courses = latest_eligible_courses.merge(merged_df, on='Student_ID', how='outer')
    latest_eligible_courses['Failed_Courses'] = latest_eligible_courses['Failed_Courses'].apply(lambda x: x if isinstance(x, list) else [])
    latest_eligible_courses['Eligible_Courses_CO'] = latest_eligible_courses['Eligible_Courses_CO'].apply(lambda x: x if isinstance(x, list) else [])
    latest_eligible_courses['Eligible_Courses_CO'] = latest_eligible_courses.apply(
        lambda row: list(set(row['Eligible_Courses_CO']) | (set(row['Failed_Courses']) - set(row['Eligible_Courses_CO']))),axis=1)
    latest_eligible_courses = latest_eligible_courses.drop(columns=['Failed_Courses'])

    latest_info_failed = failed_data.loc[failed_data.groupby("Student_ID")["Semester"].idxmax()]
    missing_semester_df = latest_eligible_courses[latest_eligible_courses['Semester'].isna()]
    latest_eligible_courses.dropna(inplace=True)
    columns_to_fill = ['Semester', 'Major', 'College', 'Program', 'Passed Credits', 'Student_Level']

    for col in columns_to_fill:
        missing_semester_df.loc[missing_semester_df[col].isna(), col] = missing_semester_df.loc[
            missing_semester_df[col].isna(), 'Student_ID'
        ].map(latest_info_failed.set_index('Student_ID')[col])

    columns_to_convert = ['Semester', 'Student_Level', 'Passed Credits']
    for col in columns_to_convert:
        latest_eligible_courses.loc[:, col] = pd.to_numeric(latest_eligible_courses[col], errors='coerce').astype('Int64')
        
    latest_eligible_courses = pd.concat([latest_eligible_courses, missing_semester_df], ignore_index=True)

    max_semester_index = pr_data.groupby('Student_ID')['Semester'].idxmax()
    max_semester_data = pr_data.loc[max_semester_index, ['Student_ID', 'Semester']]

    last_semester_courses = pd.merge(max_semester_data, pr_data, on=['Student_ID', 'Semester'])
    eng097_fpu_students = last_semester_courses[last_semester_courses['Course_ID'] == 'ENGL097']
    # Target course list
    target_courses = ['ENGL098', 'MATH094', 'MATH095', 'MATH096', 'MATH098', 'MATH100', 'MATH111', 'MATH120', 'MATH121', 'MATH131', 'MATH140']

    eng097_fpu_students_eligible = latest_eligible_courses[latest_eligible_courses['Student_ID']
                                                       .isin(eng097_fpu_students['Student_ID'])].copy()
    eng097_fpu_students_eligible.loc[:, 'Eligible_Courses_CO'] = eng097_fpu_students_eligible['Eligible_Courses_CO'].apply(
    lambda courses: [course for course in courses if course in target_courses])

    latest_eligible_courses = latest_eligible_courses.merge(
    eng097_fpu_students_eligible[['Student_ID', 'Eligible_Courses_CO']],  # Relevant columns from filtered_students
    on='Student_ID',
    how='left',  # Keep all rows in students_df
    suffixes=('', '_updated'))  # Suffix to differentiate new column)

    latest_eligible_courses['Eligible_Courses_CO'] = latest_eligible_courses['Eligible_Courses_CO_updated'].combine_first(latest_eligible_courses['Eligible_Courses_CO'])
    latest_eligible_courses = latest_eligible_courses.drop(columns=['Eligible_Courses_CO_updated'])
    latest_eligible_courses = latest_eligible_courses.merge(grouped_data_pr,on = "Student_ID",how = "outer")
    latest_eligible_courses['Course_ID'] = latest_eligible_courses['Course_ID'].apply(lambda x: x if isinstance(x, list) else [])
    latest_eligible_courses = latest_eligible_courses.apply(process_row, axis=1)
    latest_eligible_courses.drop(columns=["Course_ID"], inplace=True)

    # Exploding DataFrame and mapping course details
    eligible_courses_comprehensive_data = latest_eligible_courses.explode("Eligible_Courses_CO")
    eligible_courses_comprehensive_data = eligible_courses_comprehensive_data.merge(courses_pr[['Course_ID', 'AREA_OF_STUDY', 'COURSE_OF_STUDY', 'Course_Level']],
                                                                                    left_on='Eligible_Courses_CO', right_on='Course_ID', how='left').drop(columns="Course_ID")
    eligible_courses_comprehensive_data['Eligible_Courses_CO'] = eligible_courses_comprehensive_data['Eligible_Courses_CO'].apply(lambda x: x if isinstance(x, list) else ([] if pd.isna(x) else [x]))

    # Find Additional Eligibilities
    eligible_courses_comprehensive_data['Future_Eligible_Courses_List'] = eligible_courses_comprehensive_data.apply(lambda row: find_additional_eligibilities(row['Eligible_Courses_CO'], set(row['Eligible_Courses_CO']), prerequisites_pr), axis=1)
    eligible_courses_per_student = eligible_courses_comprehensive_data.groupby('Student_ID')['Eligible_Courses_CO'].agg(lambda x: list(set([item for sublist in x for item in sublist if isinstance(sublist, list)]))).reset_index()

    # Merge aggregated list back to the comprehensive DataFrame
    eligible_courses_comprehensive_data = eligible_courses_comprehensive_data.merge(eligible_courses_per_student.rename(columns={'Eligible_Courses_CO': 'Eligible_Courses_List_All'}), on='Student_ID', how='left')

    # Filter matching courses from future eligible lists
    eligible_courses_comprehensive_data['Future_Eligible_Courses_List'] = eligible_courses_comprehensive_data.apply(lambda row: [course for course in row['Future_Eligible_Courses_List'] if course not in row['Eligible_Courses_List_All']], axis=1)
    eligible_courses_comprehensive_data['Total_Future_Eligible_Courses_List'] = eligible_courses_comprehensive_data['Future_Eligible_Courses_List'].apply(len)

    # Special eligibility courses
    eligible_courses_comprehensive_data['Future_Eligible_Courses_Special'] = eligible_courses_comprehensive_data.apply(lambda row: find_additional_eligibilities_special(row['Eligible_Courses_CO'], set(row['Eligible_Courses_CO']), row, prerequisites_special_pr, conditions_pr, is_eligible_special_pr_), axis=1)
    eligible_courses_comprehensive_data['Future_Eligible_Courses_Special'] = eligible_courses_comprehensive_data.apply(lambda row: [course for course in row['Future_Eligible_Courses_Special'] if course not in row['Eligible_Courses_List_All']], axis=1)
    eligible_courses_comprehensive_data['Total_Future_Eligible_Courses_Special'] = eligible_courses_comprehensive_data['Future_Eligible_Courses_Special'].apply(len)

    # Combine Future Eligible Courses and calculate the score
    eligible_courses_comprehensive_data["Future_Eligible_Courses"] = eligible_courses_comprehensive_data["Future_Eligible_Courses_List"] + eligible_courses_comprehensive_data["Future_Eligible_Courses_Special"]
    eligible_courses_comprehensive_data['Course_Score'] = eligible_courses_comprehensive_data['Future_Eligible_Courses'].apply(len)

    # Find Best Courses
    recommended_courses_pr = eligible_courses_comprehensive_data.groupby(['Student_ID', 'Semester']).apply(lambda group: pd.Series({'Recommended_Courses': find_best_courses(group)})).reset_index()


    eligible_courses_comprehensive_data = eligible_courses_comprehensive_data.merge(recommended_courses_pr, on=['Student_ID', 'Semester'])
    eligible_courses_comprehensive_data = eligible_courses_comprehensive_data.merge(weighted_remaining_courses_df, on=['Student_ID', 'AREA_OF_STUDY'], how='left')


    eligible_courses_comprehensive_data = eligible_courses_comprehensive_data.groupby('Student_ID', group_keys=False).apply(normalize_by_student)
    eligible_courses_comprehensive_data['Final_Score'] = (
        (eligible_courses_comprehensive_data['Normalized_Course_Score'] * 0.4) +
        (eligible_courses_comprehensive_data['Normalized_Remaining_Courses_Weight'] * 0.4) +
        (eligible_courses_comprehensive_data['Normalized_Course_Level'] * 0.2))

    # Find Best Courses
    recommended_courses_pr_v2 = eligible_courses_comprehensive_data.groupby(['Student_ID', 'Semester']).apply(lambda group: pd.Series({'Recommended_Courses_V2': find_best_courses_v2(group)})).reset_index()
    eligible_courses_comprehensive_data = eligible_courses_comprehensive_data.merge(recommended_courses_pr_v2, on=['Student_ID', 'Semester'])

    recommended_courses = recommended_courses_pr.merge(recommended_courses_pr_v2,on=['Student_ID', 'Semester'])

    # Create summary DataFrames for eligible courses
    summary_area_of_study_eligible = eligible_courses_comprehensive_data.groupby(['Student_ID', 'AREA_OF_STUDY']).size().reset_index(name='Total_Eligible_Courses')
    summary_course_of_study_eligible = eligible_courses_comprehensive_data.groupby(['Student_ID', 'COURSE_OF_STUDY']).size().reset_index(name='Total_Eligible_Courses')
    summary_area_of_study_eligible = summary_area_of_study_eligible.pivot_table(index="Student_ID", columns="AREA_OF_STUDY", values="Total_Eligible_Courses", fill_value=0).reset_index()

    return requirements_pr_,student_progress,summary_area_of_study_taken,remaining_courses_df,latest_eligible_courses,eligible_courses_comprehensive_data,recommended_courses,summary_area_of_study_eligible

def process_data_vc(st_hist_data,major_data, requirements_weights_path):
    
    values_to_delete = ['FA', 'F', 'I', 'S', 'NP', 'WA']
    failed_grades = ['F','FA','NP']
    failed_data = st_hist_data[st_hist_data["GRADE"].isin(failed_grades)]
    st_hist_data = st_hist_data[~st_hist_data["GRADE"].isin(values_to_delete)]
    
    # Filtering and Sorting Data
    failed_data = failed_data[failed_data['Major'] == 'Visual Communication']
    failed_data = failed_data.sort_values(by=['Student_ID', 'Semester'])

    grouped_data_failed = failed_data.groupby(['Student_ID'])['Course_ID'].apply(list).reset_index()
    
    # Filtering and Sorting Data
    vc_data = st_hist_data[st_hist_data['Major'] == "Visual Communication"]
    vc_data = vc_data.sort_values(by=['Student_ID', 'Semester'])

    major = major_data["All_Courses"]
    courses_co = major_data["CO_Courses"]
    
    major["AREA_OF_STUDY"] = major["AREA_OF_STUDY"].fillna("NA")
    # Dropping records where AREA_OF_STUDY is 'N' and COURSE_OF_STUDY is 'Z'
    major_filtered = major[~((major['AREA_OF_STUDY'] == 'NA') & (major['COURSE_OF_STUDY'] == 'Z'))]
    
    major_filtered = major_filtered.copy()
    # Apply replacements directly to the specific columns to avoid SettingWithCopyWarning
    major_filtered['AREA_OF_STUDY'] = major_filtered['AREA_OF_STUDY'].replace("NA","GE")
    major_filtered['COURSE_OF_STUDY'] = major_filtered['COURSE_OF_STUDY'].replace("N","E")
    
    # Defining the major lists
    cas_majors = ['COMSCIENCE', 'ENGLISH', 'LINGUISTIC', 'LITERATURE', 'DIGITALMED', 'PR / ADV', 'VISUAL COM']
    df_cas = major_filtered[major_filtered['Major'].isin(cas_majors)]
    df_cas = df_cas[~((df_cas['AREA_OF_STUDY'] == 'GE') & (df_cas['COURSE_OF_STUDY'] == 'E'))]
    
    list_conditions = ['-', 'ONE_COURSE']

    cas_list = df_cas[df_cas['Condition'].isin(list_conditions)]
    cas_special_cases = df_cas[~df_cas['Condition'].isin(list_conditions)]
    cas_co = courses_co[courses_co['Major'].isin(cas_majors)]
    
    vc_list = cas_list[cas_list["Major"] == "VISUAL COM"]
    vc_special_cases = cas_special_cases[cas_special_cases["Major"] == "VISUAL COM"]
    vc_co = cas_co[cas_co["Major"] == "VISUAL COM"]

    # Process 'REQUISITES_LIST'
    vc_co = vc_co.copy()
    vc_co.loc[:, 'REQUISITES_LIST'] = vc_co['REQUISITES_LIST'].apply(ast.literal_eval)

    # CAS Courses
    cas_courses = major_filtered[major_filtered['Major'].isin(cas_majors)]
    courses_vc = cas_courses[cas_courses["Major"] == "VISUAL COM"]
    
    grouped_data_vc = vc_data.groupby(['Student_ID'])['Course_ID'].apply(list).reset_index()

    # Merge dataframes
    merged_df = grouped_data_failed.merge(grouped_data_vc, on=['Student_ID'], how='outer', suffixes=('_failed', '_all'))
    # Replace NaN with empty lists to avoid errors
    merged_df['Course_ID_all'] = merged_df['Course_ID_all'].apply(lambda x: x if isinstance(x, list) else [])
    merged_df['Course_ID_failed'] = merged_df['Course_ID_failed'].apply(lambda x: x if isinstance(x, list) else [])

    merged_df['Failed_Courses'] = merged_df.apply(
        lambda row: list(set(row['Course_ID_failed']) - set(row['Course_ID_all'])),
        axis=1)
    # Keep only relevant columns
    merged_df = merged_df[['Student_ID', 'Failed_Courses']]

    # Extract Accounting specific requirements and weights from respective DataFrames
    requirements_df = pd.read_excel(requirements_weights_path,sheet_name="requirements")
    weights_df = pd.read_excel(requirements_weights_path,sheet_name="weights")
    requirements_vc = requirements_df[requirements_df["Major"] == "Visual Communication"]
    requirements_vc_ = requirements_vc.pivot_table(index="Major",columns="AREA_OF_STUDY",values ='Required_Courses' ,aggfunc='sum',fill_value=0).reset_index()
    weights_vc = weights_df[weights_df["Major"] == "Visual Communication"]

    student_courses = vc_data[["Student_ID", "Course_ID"]]

    # Map AREA_OF_STUDY and COURSE_OF_STUDY to vc_data
    student_courses = student_courses.merge(courses_vc[['Course_ID', 'AREA_OF_STUDY', 'COURSE_OF_STUDY', "Course_Level"]],
                                            on='Course_ID', how='left').drop_duplicates()

    # Create summary DataFrames for taken courses
    student_vcogress = student_courses.groupby(['Student_ID', 'AREA_OF_STUDY']).size().reset_index(name='Total_Taken_Courses')
    student_vcogress = student_vcogress.merge(requirements_vc, on='AREA_OF_STUDY', how='left')
    student_vcogress["Remaining_Courses"] = student_vcogress["Required_Courses"] - student_vcogress["Total_Taken_Courses"]
    student_vcogress["Remaining_Courses"] = student_vcogress["Remaining_Courses"].apply(lambda x: max(x, 0))

    free_elective_taken_counts = student_courses[(student_courses['AREA_OF_STUDY'] == "GE") & (student_courses['COURSE_OF_STUDY'] == "E")].groupby('Student_ID').size().reset_index(name='Total_Free_Electives_Taken')

    # Update progress by including the free elective data
    student_vcogress["Student_vcogress"] = (student_vcogress["Total_Taken_Courses"] / student_vcogress["Required_Courses"]) * 100
    student_vcogress["Student_vcogress"].replace([np.inf, -np.inf], 100, inplace=True)

    summary_area_of_study_taken = student_vcogress.pivot_table(index="Student_ID", columns="AREA_OF_STUDY", values="Total_Taken_Courses", fill_value=0)
    summary_area_of_study_taken = summary_area_of_study_taken.merge(free_elective_taken_counts, on="Student_ID", how="left").fillna(0).rename(columns={"Total_Free_Electives_Taken": "FE"})

    # Create a copy of summary_area_of_study_taken to work on remaining courses calculation
    remaining_courses_df = summary_area_of_study_taken.copy()

    # Loop through each AREA_OF_STUDY and calculate remaining courses by subtracting from the requirements
    for column in remaining_courses_df.columns:
        if column in requirements_vc['AREA_OF_STUDY'].values:
            required_courses = requirements_vc.loc[requirements_vc['AREA_OF_STUDY'] == column, 'Required_Courses'].values[0]
            remaining_courses_df[column] = required_courses - remaining_courses_df[column]
            remaining_courses_df[column] = remaining_courses_df[column].clip(lower=0)

    # Calculate weighted remaining courses
    weighted_remaining_courses_df = remaining_courses_df.copy()
    for column in weighted_remaining_courses_df.columns:
        if column in weights_vc['AREA_OF_STUDY'].values:
            weight_value = weights_vc.loc[weights_vc['AREA_OF_STUDY'] == column, 'Weight'].values[0]
            weighted_remaining_courses_df[column] = weighted_remaining_courses_df[column] * weight_value

    # Prepare weighted remaining courses for merge
    weighted_remaining_courses_df = weighted_remaining_courses_df.reset_index().melt(id_vars=['Student_ID'],
                                                                                      var_name='AREA_OF_STUDY',
                                                                                      value_name='Remaining_Courses_Weight_Score')
    weighted_remaining_courses_df = weighted_remaining_courses_df[weighted_remaining_courses_df["AREA_OF_STUDY"] != "index"]

    # Eligibility Calculation for Standard and Special Cases
    prerequisites_vc = vc_list.set_index('Course_ID')['REQUISITES_LIST'].apply(eval).to_dict()
    prerequisites_special_vc = vc_special_cases.set_index('Course_ID')['REQUISITES_LIST'].apply(eval).to_dict()
    conditions_vc = vc_special_cases.set_index('Course_ID')['Condition'].to_dict()

    final_results_vc = []  # Standard eligibility results
    final_results_special_vc = []  # Special eligibility results

    for student_id, group in vc_data.groupby('Student_ID'):
        cumulative_courses = set()
        for semester, semester_group in group.groupby('Semester'):
            taken_courses = set(semester_group['Course_ID'].tolist())
            cumulative_courses.update(taken_courses)

            # Determine Standard Eligible Courses
            student_info = semester_group.iloc[0].to_dict()
            eligible_courses = {course for course in prerequisites_vc.keys() if all(req in cumulative_courses for req in prerequisites_vc[course])}
            final_results_vc.append({
                'Student_ID': student_id,
                'Semester': semester,
                'Major': student_info['Major'],
                'College': student_info['College'],
                'Program': student_info['Program'],
                'Passed Credits': student_info['Passed Credits'],
                'Student_Level': student_info['Student_Level'],
                'Eligible_Courses': list(eligible_courses - cumulative_courses)
            })

            # Determine Special Eligible Courses
            special_eligible_courses = {
                course for course in prerequisites_special_vc.keys()
                if is_eligible_special_vc(course, cumulative_courses, student_info, prerequisites_special_vc, conditions_vc)
            }
            final_results_special_vc.append({
                'Student_ID': student_id,
                'Semester': semester,
                'Major': student_info['Major'],
                'College': student_info['College'],
                'Program': student_info['Program'],
                'Passed Credits': student_info['Passed Credits'],
                'Student_Level': student_info['Student_Level'],
                'Eligible_Courses': list(special_eligible_courses - cumulative_courses)
            })

    # Convert Results to DataFrames
    final_results_df_vc = pd.DataFrame(final_results_vc)
    final_results_special_df_vc = pd.DataFrame(final_results_special_vc)
    
    # Combine Eligible Courses from Both DataFrames
    combined_vc_list = combine_eligible_courses(final_results_df_vc, final_results_special_df_vc)
    # Find Course Combinations for Co-requisites
    combined_vc_list = combined_vc_list.apply(create_combined_courses, axis=1, co=vc_co)
    latest_eligible_courses = combined_vc_list.sort_values(by='Semester', ascending=False)
    latest_eligible_courses = latest_eligible_courses.groupby('Student_ID').first().reset_index()
    latest_eligible_courses = latest_eligible_courses.merge(grouped_data_vc,on = "Student_ID",how = "inner")
    latest_eligible_courses["Eligible_Courses_CO"] = latest_eligible_courses.apply(remove_matches, axis=1)
    latest_eligible_courses = latest_eligible_courses.apply(process_row, axis=1)
    latest_eligible_courses.drop(columns=["Course_ID"], inplace=True)

    latest_eligible_courses = latest_eligible_courses.merge(merged_df, on='Student_ID', how='outer')
    latest_eligible_courses['Failed_Courses'] = latest_eligible_courses['Failed_Courses'].apply(lambda x: x if isinstance(x, list) else [])
    latest_eligible_courses['Eligible_Courses_CO'] = latest_eligible_courses['Eligible_Courses_CO'].apply(lambda x: x if isinstance(x, list) else [])
    latest_eligible_courses['Eligible_Courses_CO'] = latest_eligible_courses.apply(
        lambda row: list(set(row['Eligible_Courses_CO']) | (set(row['Failed_Courses']) - set(row['Eligible_Courses_CO']))),axis=1)
    latest_eligible_courses = latest_eligible_courses.drop(columns=['Failed_Courses'])

    latest_info_failed = failed_data.loc[failed_data.groupby("Student_ID")["Semester"].idxmax()]
    missing_semester_df = latest_eligible_courses[latest_eligible_courses['Semester'].isna()]
    latest_eligible_courses.dropna(inplace=True)
    columns_to_fill = ['Semester', 'Major', 'College', 'Program', 'Passed Credits', 'Student_Level']

    for col in columns_to_fill:
        missing_semester_df.loc[missing_semester_df[col].isna(), col] = missing_semester_df.loc[
            missing_semester_df[col].isna(), 'Student_ID'
        ].map(latest_info_failed.set_index('Student_ID')[col])

    columns_to_convert = ['Semester', 'Student_Level', 'Passed Credits']
    for col in columns_to_convert:
        latest_eligible_courses.loc[:, col] = pd.to_numeric(latest_eligible_courses[col], errors='coerce').astype('Int64')
        
    latest_eligible_courses = pd.concat([latest_eligible_courses, missing_semester_df], ignore_index=True)

    max_semester_index = vc_data.groupby('Student_ID')['Semester'].idxmax()
    max_semester_data = vc_data.loc[max_semester_index, ['Student_ID', 'Semester']]

    last_semester_courses = pd.merge(max_semester_data, vc_data, on=['Student_ID', 'Semester'])
    eng097_fpu_students = last_semester_courses[last_semester_courses['Course_ID'] == 'ENGL097']
    # Target course list
    target_courses = ['ENGL098', 'MATH094', 'MATH095', 'MATH096', 'MATH098', 'MATH100', 'MATH111', 'MATH120', 'MATH121', 'MATH131', 'MATH140']

    eng097_fpu_students_eligible = latest_eligible_courses[latest_eligible_courses['Student_ID']
                                                       .isin(eng097_fpu_students['Student_ID'])].copy()
    eng097_fpu_students_eligible.loc[:, 'Eligible_Courses_CO'] = eng097_fpu_students_eligible['Eligible_Courses_CO'].apply(
    lambda courses: [course for course in courses if course in target_courses])

    latest_eligible_courses = latest_eligible_courses.merge(
    eng097_fpu_students_eligible[['Student_ID', 'Eligible_Courses_CO']],  # Relevant columns from filtered_students
    on='Student_ID',
    how='left',  # Keep all rows in students_df
    suffixes=('', '_updated'))  # Suffix to differentiate new column)

    latest_eligible_courses['Eligible_Courses_CO'] = latest_eligible_courses['Eligible_Courses_CO_updated'].combine_first(latest_eligible_courses['Eligible_Courses_CO'])
    latest_eligible_courses = latest_eligible_courses.drop(columns=['Eligible_Courses_CO_updated'])
    latest_eligible_courses = latest_eligible_courses.merge(grouped_data_vc,on = "Student_ID",how = "outer")
    latest_eligible_courses['Course_ID'] = latest_eligible_courses['Course_ID'].apply(lambda x: x if isinstance(x, list) else [])
    latest_eligible_courses = latest_eligible_courses.apply(process_row, axis=1)
    latest_eligible_courses.drop(columns=["Course_ID"], inplace=True)

    # Exploding DataFrame and mapping course details
    eligible_courses_comprehensive_data = latest_eligible_courses.explode("Eligible_Courses_CO")
    eligible_courses_comprehensive_data = eligible_courses_comprehensive_data.merge(courses_vc[['Course_ID', 'AREA_OF_STUDY', 'COURSE_OF_STUDY', 'Course_Level']],
                                                                                    left_on='Eligible_Courses_CO', right_on='Course_ID', how='left').drop(columns="Course_ID")
    eligible_courses_comprehensive_data['Eligible_Courses_CO'] = eligible_courses_comprehensive_data['Eligible_Courses_CO'].apply(lambda x: x if isinstance(x, list) else ([] if pd.isna(x) else [x]))

    # Find Additional Eligibilities
    eligible_courses_comprehensive_data['Future_Eligible_Courses_List'] = eligible_courses_comprehensive_data.apply(lambda row: find_additional_eligibilities(row['Eligible_Courses_CO'], set(row['Eligible_Courses_CO']), prerequisites_vc), axis=1)
    eligible_courses_per_student = eligible_courses_comprehensive_data.groupby('Student_ID')['Eligible_Courses_CO'].agg(lambda x: list(set([item for sublist in x for item in sublist if isinstance(sublist, list)]))).reset_index()

    # Merge aggregated list back to the comprehensive DataFrame
    eligible_courses_comprehensive_data = eligible_courses_comprehensive_data.merge(eligible_courses_per_student.rename(columns={'Eligible_Courses_CO': 'Eligible_Courses_List_All'}), on='Student_ID', how='left')

    # Filter matching courses from future eligible lists
    eligible_courses_comprehensive_data['Future_Eligible_Courses_List'] = eligible_courses_comprehensive_data.apply(lambda row: [course for course in row['Future_Eligible_Courses_List'] if course not in row['Eligible_Courses_List_All']], axis=1)
    eligible_courses_comprehensive_data['Total_Future_Eligible_Courses_List'] = eligible_courses_comprehensive_data['Future_Eligible_Courses_List'].apply(len)

    # Special eligibility courses
    eligible_courses_comprehensive_data['Future_Eligible_Courses_Special'] = eligible_courses_comprehensive_data.apply(lambda row: find_additional_eligibilities_special(row['Eligible_Courses_CO'], set(row['Eligible_Courses_CO']), row, prerequisites_special_vc, conditions_vc, is_eligible_special_vc_), axis=1)
    eligible_courses_comprehensive_data['Future_Eligible_Courses_Special'] = eligible_courses_comprehensive_data.apply(lambda row: [course for course in row['Future_Eligible_Courses_Special'] if course not in row['Eligible_Courses_List_All']], axis=1)
    eligible_courses_comprehensive_data['Total_Future_Eligible_Courses_Special'] = eligible_courses_comprehensive_data['Future_Eligible_Courses_Special'].apply(len)

    # Combine Future Eligible Courses and calculate the score
    eligible_courses_comprehensive_data["Future_Eligible_Courses"] = eligible_courses_comprehensive_data["Future_Eligible_Courses_List"] + eligible_courses_comprehensive_data["Future_Eligible_Courses_Special"]
    eligible_courses_comprehensive_data['Course_Score'] = eligible_courses_comprehensive_data['Future_Eligible_Courses'].apply(len)

    # Find Best Courses
    recommended_courses_vc = eligible_courses_comprehensive_data.groupby(['Student_ID', 'Semester']).apply(lambda group: pd.Series({'Recommended_Courses': find_best_courses(group)})).reset_index()


    eligible_courses_comprehensive_data = eligible_courses_comprehensive_data.merge(recommended_courses_vc, on=['Student_ID', 'Semester'])
    eligible_courses_comprehensive_data = eligible_courses_comprehensive_data.merge(weighted_remaining_courses_df, on=['Student_ID', 'AREA_OF_STUDY'], how='left')


    eligible_courses_comprehensive_data = eligible_courses_comprehensive_data.groupby('Student_ID', group_keys=False).apply(normalize_by_student)
    eligible_courses_comprehensive_data['Final_Score'] = (
        (eligible_courses_comprehensive_data['Normalized_Course_Score'] * 0.4) +
        (eligible_courses_comprehensive_data['Normalized_Remaining_Courses_Weight'] * 0.4) +
        (eligible_courses_comprehensive_data['Normalized_Course_Level'] * 0.2))

    # Find Best Courses
    recommended_courses_vc_v2 = eligible_courses_comprehensive_data.groupby(['Student_ID', 'Semester']).apply(lambda group: pd.Series({'Recommended_Courses_V2': find_best_courses_v2(group)})).reset_index()
    eligible_courses_comprehensive_data = eligible_courses_comprehensive_data.merge(recommended_courses_vc_v2, on=['Student_ID', 'Semester'])

    recommended_courses = recommended_courses_vc.merge(recommended_courses_vc_v2,on=['Student_ID', 'Semester'])

    # Create summary DataFrames for eligible courses
    summary_area_of_study_eligible = eligible_courses_comprehensive_data.groupby(['Student_ID', 'AREA_OF_STUDY']).size().reset_index(name='Total_Eligible_Courses')
    summary_course_of_study_eligible = eligible_courses_comprehensive_data.groupby(['Student_ID', 'COURSE_OF_STUDY']).size().reset_index(name='Total_Eligible_Courses')
    summary_area_of_study_eligible = summary_area_of_study_eligible.pivot_table(index="Student_ID", columns="AREA_OF_STUDY", values="Total_Eligible_Courses", fill_value=0).reset_index()

    return requirements_vc_,student_vcogress,summary_area_of_study_taken,remaining_courses_df,latest_eligible_courses,eligible_courses_comprehensive_data,recommended_courses,summary_area_of_study_eligible

def process_data_mgmt(st_hist_data,major_data, requirements_weights_path):
    
    values_to_delete = ['FA', 'F', 'I', 'S', 'NP', 'WA']
    failed_grades = ['F','FA','NP']
    failed_data = st_hist_data[st_hist_data["GRADE"].isin(failed_grades)]
    st_hist_data = st_hist_data[~st_hist_data["GRADE"].isin(values_to_delete)]
    
    # Filtering and Sorting Data
    failed_data = failed_data[failed_data['Major'] == 'Engineering Management']
    failed_data = failed_data.sort_values(by=['Student_ID', 'Semester'])

    grouped_data_failed = failed_data.groupby(['Student_ID'])['Course_ID'].apply(list).reset_index()
    
    # Filtering and Sorting Data
    mgmt_data = st_hist_data[st_hist_data['Major'] == "Engineering Management"]
    mgmt_data = mgmt_data.sort_values(by=['Student_ID', 'Semester'])

    major = major_data["All_Courses"]
    courses_co = major_data["CO_Courses"]
    
    major["AREA_OF_STUDY"] = major["AREA_OF_STUDY"].fillna("NA")
    # Dropping records where AREA_OF_STUDY is 'N' and COURSE_OF_STUDY is 'Z'
    major_filtered = major[~((major['AREA_OF_STUDY'] == 'NA') & (major['COURSE_OF_STUDY'] == 'Z'))]
    
    major_filtered = major_filtered.copy()
    # Apply replacements directly to the specific columns to avoid SettingWithCopyWarning
    major_filtered['AREA_OF_STUDY'] = major_filtered['AREA_OF_STUDY'].replace("NA","GE")
    major_filtered['COURSE_OF_STUDY'] = major_filtered['COURSE_OF_STUDY'].replace("N","E")
    
    # Defining the major lists
    cea_majors = ['COMPENG', 'ELECENG', 'MGMTENG']
    df_cea = major_filtered[(major_filtered['Major'].isin(cea_majors)) & (major_filtered['COURSE_OF_STUDY'].isin(['R', 'RE']))]   
    
    list_conditions = ['-', 'ONE_COURSE']

    cea_list = df_cea[df_cea['Condition'].isin(list_conditions)]
    cea_special_cases = df_cea[~df_cea['Condition'].isin(list_conditions)]
    cea_co = courses_co[courses_co['Major'].isin(cea_majors)]
    
    mgmt_list = cea_list[cea_list["Major"] == "MGMTENG"]
    mgmt_special_cases = cea_special_cases[cea_special_cases["Major"] == "MGMTENG"]
    mgmt_co = cea_co[cea_co["Major"] == "MGMTENG"]

    # Process 'REQUISITES_LIST'
    mgmt_co = mgmt_co.copy()
    mgmt_co.loc[:, 'REQUISITES_LIST'] = mgmt_co['REQUISITES_LIST'].apply(ast.literal_eval)

    # CAS Courses
    cea_courses = major_filtered[major_filtered['Major'].isin(cea_majors)]
    courses_mgmt = cea_courses[cea_courses["Major"] == "MGMTENG"]
    
    grouped_data_mgmt = mgmt_data.groupby(['Student_ID'])['Course_ID'].apply(list).reset_index()

    # Merge dataframes
    merged_df = grouped_data_failed.merge(grouped_data_mgmt, on=['Student_ID'], how='outer', suffixes=('_failed', '_all'))
    # Replace NaN with empty lists to avoid errors
    merged_df['Course_ID_all'] = merged_df['Course_ID_all'].apply(lambda x: x if isinstance(x, list) else [])
    merged_df['Course_ID_failed'] = merged_df['Course_ID_failed'].apply(lambda x: x if isinstance(x, list) else [])

    merged_df['Failed_Courses'] = merged_df.apply(
        lambda row: list(set(row['Course_ID_failed']) - set(row['Course_ID_all'])),
        axis=1)
    # Keep only relevant columns
    merged_df = merged_df[['Student_ID', 'Failed_Courses']]

    # Extract Accounting specific requirements and weights from respective DataFrames
    requirements_df = pd.read_excel(requirements_weights_path,sheet_name="requirements")
    weights_df = pd.read_excel(requirements_weights_path,sheet_name="weights")
    requirements_mgmt = requirements_df[requirements_df["Major"] == "Engineering Management"]
    requirements_mgmt_ = requirements_mgmt.pivot_table(index="Major",columns="AREA_OF_STUDY",values ='Required_Courses' ,aggfunc='sum',fill_value=0).reset_index()
    weights_mgmt = weights_df[weights_df["Major"] == "Engineering Management"]

    student_courses = mgmt_data[["Student_ID", "Course_ID"]]

    # Map AREA_OF_STUDY and COURSE_OF_STUDY to mgmt_data
    student_courses = student_courses.merge(courses_mgmt[['Course_ID', 'AREA_OF_STUDY', 'COURSE_OF_STUDY', "Course_Level"]],
                                            on='Course_ID', how='left').drop_duplicates()

    # Create summary DataFrames for taken courses
    student_mgmtogress = student_courses.groupby(['Student_ID', 'AREA_OF_STUDY']).size().reset_index(name='Total_Taken_Courses')
    student_mgmtogress = student_mgmtogress.merge(requirements_mgmt, on='AREA_OF_STUDY', how='left')
    student_mgmtogress["Remaining_Courses"] = student_mgmtogress["Required_Courses"] - student_mgmtogress["Total_Taken_Courses"]
    student_mgmtogress["Remaining_Courses"] = student_mgmtogress["Remaining_Courses"].apply(lambda x: max(x, 0))

    free_elective_taken_counts = student_courses[(student_courses['AREA_OF_STUDY'] == "GE") & (student_courses['COURSE_OF_STUDY'] == "E")].groupby('Student_ID').size().reset_index(name='Total_Free_Electives_Taken')

    # Update progress by including the free elective data
    student_mgmtogress["Student_mgmtogress"] = (student_mgmtogress["Total_Taken_Courses"] / student_mgmtogress["Required_Courses"]) * 100
    student_mgmtogress["Student_mgmtogress"].replace([np.inf, -np.inf], 100, inplace=True)

    summary_area_of_study_taken = student_mgmtogress.pivot_table(index="Student_ID", columns="AREA_OF_STUDY", values="Total_Taken_Courses", fill_value=0)
    summary_area_of_study_taken = summary_area_of_study_taken.merge(free_elective_taken_counts, on="Student_ID", how="left").fillna(0).rename(columns={"Total_Free_Electives_Taken": "FE"})

    # Create a copy of summary_area_of_study_taken to work on remaining courses calculation
    remaining_courses_df = summary_area_of_study_taken.copy()

    # Loop through each AREA_OF_STUDY and calculate remaining courses by subtracting from the requirements
    for column in remaining_courses_df.columns:
        if column in requirements_mgmt['AREA_OF_STUDY'].values:
            required_courses = requirements_mgmt.loc[requirements_mgmt['AREA_OF_STUDY'] == column, 'Required_Courses'].values[0]
            remaining_courses_df[column] = required_courses - remaining_courses_df[column]
            remaining_courses_df[column] = remaining_courses_df[column].clip(lower=0)

    # Calculate weighted remaining courses
    weighted_remaining_courses_df = remaining_courses_df.copy()
    for column in weighted_remaining_courses_df.columns:
        if column in weights_mgmt['AREA_OF_STUDY'].values:
            weight_value = weights_mgmt.loc[weights_mgmt['AREA_OF_STUDY'] == column, 'Weight'].values[0]
            weighted_remaining_courses_df[column] = weighted_remaining_courses_df[column] * weight_value

    # Prepare weighted remaining courses for merge
    weighted_remaining_courses_df = weighted_remaining_courses_df.reset_index().melt(id_vars=['Student_ID'],
                                                                                      var_name='AREA_OF_STUDY',
                                                                                      value_name='Remaining_Courses_Weight_Score')
    weighted_remaining_courses_df = weighted_remaining_courses_df[weighted_remaining_courses_df["AREA_OF_STUDY"] != "index"]

    # Eligibility Calculation for Standard and Special Cases
    prerequisites_mgmt = mgmt_list.set_index('Course_ID')['REQUISITES_LIST'].apply(eval).to_dict()
    prerequisites_special_mgmt = mgmt_special_cases.set_index('Course_ID')['REQUISITES_LIST'].apply(eval).to_dict()
    conditions_mgmt = mgmt_special_cases.set_index('Course_ID')['Condition'].to_dict()

    final_results_mgmt = []  # Standard eligibility results
    final_results_special_mgmt = []  # Special eligibility results

    for student_id, group in mgmt_data.groupby('Student_ID'):
        cumulative_courses = set()
        for semester, semester_group in group.groupby('Semester'):
            taken_courses = set(semester_group['Course_ID'].tolist())
            cumulative_courses.update(taken_courses)

            # Determine Standard Eligible Courses
            student_info = semester_group.iloc[0].to_dict()
            eligible_courses = {course for course in prerequisites_mgmt.keys() if all(req in cumulative_courses for req in prerequisites_mgmt[course])}
            final_results_mgmt.append({
                'Student_ID': student_id,
                'Semester': semester,
                'Major': student_info['Major'],
                'College': student_info['College'],
                'Program': student_info['Program'],
                'Passed Credits': student_info['Passed Credits'],
                'Student_Level': student_info['Student_Level'],
                'Eligible_Courses': list(eligible_courses - cumulative_courses)
            })

            # Determine Special Eligible Courses
            special_eligible_courses = {
                course for course in prerequisites_special_mgmt.keys()
                if is_eligible_special_mgmt(course, cumulative_courses, student_info, prerequisites_special_mgmt, conditions_mgmt)
            }
            final_results_special_mgmt.append({
                'Student_ID': student_id,
                'Semester': semester,
                'Major': student_info['Major'],
                'College': student_info['College'],
                'Program': student_info['Program'],
                'Passed Credits': student_info['Passed Credits'],
                'Student_Level': student_info['Student_Level'],
                'Eligible_Courses': list(special_eligible_courses - cumulative_courses)
            })

    # Convert Results to DataFrames
    final_results_df_mgmt = pd.DataFrame(final_results_mgmt)
    final_results_special_df_mgmt = pd.DataFrame(final_results_special_mgmt)
    
    # Combine Eligible Courses from Both DataFrames
    combined_mgmt_list = combine_eligible_courses(final_results_df_mgmt, final_results_special_df_mgmt)
    # Find Course Combinations for Co-requisites
    combined_mgmt_list = combined_mgmt_list.apply(create_combined_courses, axis=1, co=mgmt_co)
    latest_eligible_courses = combined_mgmt_list.sort_values(by='Semester', ascending=False)
    latest_eligible_courses = latest_eligible_courses.groupby('Student_ID').first().reset_index()
    latest_eligible_courses = latest_eligible_courses.merge(grouped_data_mgmt,on = "Student_ID",how = "inner")
    latest_eligible_courses["Eligible_Courses_CO"] = latest_eligible_courses.apply(remove_matches, axis=1)
    latest_eligible_courses = latest_eligible_courses.apply(process_row, axis=1)
    latest_eligible_courses.drop(columns=["Course_ID"], inplace=True)

    latest_eligible_courses = latest_eligible_courses.merge(merged_df, on='Student_ID', how='outer')
    latest_eligible_courses['Failed_Courses'] = latest_eligible_courses['Failed_Courses'].apply(lambda x: x if isinstance(x, list) else [])
    latest_eligible_courses['Eligible_Courses_CO'] = latest_eligible_courses['Eligible_Courses_CO'].apply(lambda x: x if isinstance(x, list) else [])
    latest_eligible_courses['Eligible_Courses_CO'] = latest_eligible_courses.apply(
        lambda row: list(set(row['Eligible_Courses_CO']) | (set(row['Failed_Courses']) - set(row['Eligible_Courses_CO']))),axis=1)
    latest_eligible_courses = latest_eligible_courses.drop(columns=['Failed_Courses'])

    latest_info_failed = failed_data.loc[failed_data.groupby("Student_ID")["Semester"].idxmax()]
    missing_semester_df = latest_eligible_courses[latest_eligible_courses['Semester'].isna()]
    latest_eligible_courses.dropna(inplace=True)
    columns_to_fill = ['Semester', 'Major', 'College', 'Program', 'Passed Credits', 'Student_Level']

    for col in columns_to_fill:
        missing_semester_df.loc[missing_semester_df[col].isna(), col] = missing_semester_df.loc[
            missing_semester_df[col].isna(), 'Student_ID'
        ].map(latest_info_failed.set_index('Student_ID')[col])

    columns_to_convert = ['Semester', 'Student_Level', 'Passed Credits']
    for col in columns_to_convert:
        latest_eligible_courses.loc[:, col] = pd.to_numeric(latest_eligible_courses[col], errors='coerce').astype('Int64')
        
    latest_eligible_courses = pd.concat([latest_eligible_courses, missing_semester_df], ignore_index=True)

    max_semester_index = mgmt_data.groupby('Student_ID')['Semester'].idxmax()
    max_semester_data = mgmt_data.loc[max_semester_index, ['Student_ID', 'Semester']]

    last_semester_courses = pd.merge(max_semester_data, mgmt_data, on=['Student_ID', 'Semester'])
    eng097_fpu_students = last_semester_courses[last_semester_courses['Course_ID'] == 'ENGL097']
    # Target course list
    target_courses = ['ENGL098', 'MATH094', 'MATH095', 'MATH096', 'MATH098', 'MATH100', 'MATH111', 'MATH120', 'MATH121', 'MATH131', 'MATH140']

    eng097_fpu_students_eligible = latest_eligible_courses[latest_eligible_courses['Student_ID']
                                                       .isin(eng097_fpu_students['Student_ID'])].copy()
    eng097_fpu_students_eligible.loc[:, 'Eligible_Courses_CO'] = eng097_fpu_students_eligible['Eligible_Courses_CO'].apply(
    lambda courses: [course for course in courses if course in target_courses])

    latest_eligible_courses = latest_eligible_courses.merge(
    eng097_fpu_students_eligible[['Student_ID', 'Eligible_Courses_CO']],  # Relevant columns from filtered_students
    on='Student_ID',
    how='left',  # Keep all rows in students_df
    suffixes=('', '_updated'))  # Suffix to differentiate new column)

    latest_eligible_courses['Eligible_Courses_CO'] = latest_eligible_courses['Eligible_Courses_CO_updated'].combine_first(latest_eligible_courses['Eligible_Courses_CO'])
    latest_eligible_courses = latest_eligible_courses.drop(columns=['Eligible_Courses_CO_updated'])
    latest_eligible_courses = latest_eligible_courses.merge(grouped_data_mgmt,on = "Student_ID",how = "outer")
    latest_eligible_courses['Course_ID'] = latest_eligible_courses['Course_ID'].apply(lambda x: x if isinstance(x, list) else [])
    latest_eligible_courses = latest_eligible_courses.apply(process_row, axis=1)
    latest_eligible_courses.drop(columns=["Course_ID"], inplace=True)

    # Exploding DataFrame and mapping course details
    eligible_courses_comprehensive_data = latest_eligible_courses.explode("Eligible_Courses_CO")
    eligible_courses_comprehensive_data = eligible_courses_comprehensive_data.merge(courses_mgmt[['Course_ID', 'AREA_OF_STUDY', 'COURSE_OF_STUDY', 'Course_Level']],
                                                                                    left_on='Eligible_Courses_CO', right_on='Course_ID', how='left').drop(columns="Course_ID")
    eligible_courses_comprehensive_data['Eligible_Courses_CO'] = eligible_courses_comprehensive_data['Eligible_Courses_CO'].apply(lambda x: x if isinstance(x, list) else ([] if pd.isna(x) else [x]))

    # Find Additional Eligibilities
    eligible_courses_comprehensive_data['Future_Eligible_Courses_List'] = eligible_courses_comprehensive_data.apply(lambda row: find_additional_eligibilities(row['Eligible_Courses_CO'], set(row['Eligible_Courses_CO']), prerequisites_mgmt), axis=1)
    eligible_courses_per_student = eligible_courses_comprehensive_data.groupby('Student_ID')['Eligible_Courses_CO'].agg(lambda x: list(set([item for sublist in x for item in sublist if isinstance(sublist, list)]))).reset_index()

    # Merge aggregated list back to the comprehensive DataFrame
    eligible_courses_comprehensive_data = eligible_courses_comprehensive_data.merge(eligible_courses_per_student.rename(columns={'Eligible_Courses_CO': 'Eligible_Courses_List_All'}), on='Student_ID', how='left')

    # Filter matching courses from future eligible lists
    eligible_courses_comprehensive_data['Future_Eligible_Courses_List'] = eligible_courses_comprehensive_data.apply(lambda row: [course for course in row['Future_Eligible_Courses_List'] if course not in row['Eligible_Courses_List_All']], axis=1)
    eligible_courses_comprehensive_data['Total_Future_Eligible_Courses_List'] = eligible_courses_comprehensive_data['Future_Eligible_Courses_List'].apply(len)

    # Special eligibility courses
    eligible_courses_comprehensive_data['Future_Eligible_Courses_Special'] = eligible_courses_comprehensive_data.apply(lambda row: find_additional_eligibilities_special(row['Eligible_Courses_CO'], set(row['Eligible_Courses_CO']), row, prerequisites_special_mgmt, conditions_mgmt, is_eligible_special_mgmt_), axis=1)
    eligible_courses_comprehensive_data['Future_Eligible_Courses_Special'] = eligible_courses_comprehensive_data.apply(lambda row: [course for course in row['Future_Eligible_Courses_Special'] if course not in row['Eligible_Courses_List_All']], axis=1)
    eligible_courses_comprehensive_data['Total_Future_Eligible_Courses_Special'] = eligible_courses_comprehensive_data['Future_Eligible_Courses_Special'].apply(len)

    # Combine Future Eligible Courses and calculate the score
    eligible_courses_comprehensive_data["Future_Eligible_Courses"] = eligible_courses_comprehensive_data["Future_Eligible_Courses_List"] + eligible_courses_comprehensive_data["Future_Eligible_Courses_Special"]
    eligible_courses_comprehensive_data['Course_Score'] = eligible_courses_comprehensive_data['Future_Eligible_Courses'].apply(len)

    # Find Best Courses
    recommended_courses_mgmt = eligible_courses_comprehensive_data.groupby(['Student_ID', 'Semester']).apply(lambda group: pd.Series({'Recommended_Courses': find_best_courses(group)})).reset_index()


    eligible_courses_comprehensive_data = eligible_courses_comprehensive_data.merge(recommended_courses_mgmt, on=['Student_ID', 'Semester'])
    eligible_courses_comprehensive_data = eligible_courses_comprehensive_data.merge(weighted_remaining_courses_df, on=['Student_ID', 'AREA_OF_STUDY'], how='left')


    eligible_courses_comprehensive_data = eligible_courses_comprehensive_data.groupby('Student_ID', group_keys=False).apply(normalize_by_student)
    eligible_courses_comprehensive_data['Final_Score'] = (
        (eligible_courses_comprehensive_data['Normalized_Course_Score'] * 0.4) +
        (eligible_courses_comprehensive_data['Normalized_Remaining_Courses_Weight'] * 0.4) +
        (eligible_courses_comprehensive_data['Normalized_Course_Level'] * 0.2))

    # Find Best Courses
    recommended_courses_mgmt_v2 = eligible_courses_comprehensive_data.groupby(['Student_ID', 'Semester']).apply(lambda group: pd.Series({'Recommended_Courses_V2': find_best_courses_cea_v2(group)})).reset_index()
    eligible_courses_comprehensive_data = eligible_courses_comprehensive_data.merge(recommended_courses_mgmt_v2, on=['Student_ID', 'Semester'])

    recommended_courses = recommended_courses_mgmt.merge(recommended_courses_mgmt_v2,on=['Student_ID', 'Semester'])

    # Create summary DataFrames for eligible courses
    summary_area_of_study_eligible = eligible_courses_comprehensive_data.groupby(['Student_ID', 'AREA_OF_STUDY']).size().reset_index(name='Total_Eligible_Courses')
    summary_course_of_study_eligible = eligible_courses_comprehensive_data.groupby(['Student_ID', 'COURSE_OF_STUDY']).size().reset_index(name='Total_Eligible_Courses')
    summary_area_of_study_eligible = summary_area_of_study_eligible.pivot_table(index="Student_ID", columns="AREA_OF_STUDY", values="Total_Eligible_Courses", fill_value=0).reset_index()

    return requirements_mgmt_,student_mgmtogress,summary_area_of_study_taken,remaining_courses_df,latest_eligible_courses,eligible_courses_comprehensive_data,recommended_courses,summary_area_of_study_eligible

def process_data_elec(st_hist_data,major_data, requirements_weights_path):
    
    values_to_delete = ['FA', 'F', 'I', 'S', 'NP', 'WA']
    failed_grades = ['F','FA','NP']
    failed_data = st_hist_data[st_hist_data["GRADE"].isin(failed_grades)]
    st_hist_data = st_hist_data[~st_hist_data["GRADE"].isin(values_to_delete)]
    
    # Filtering and Sorting Data
    failed_data = failed_data[failed_data['Major'] == 'Electrical Engineering']
    failed_data = failed_data.sort_values(by=['Student_ID', 'Semester'])

    grouped_data_failed = failed_data.groupby(['Student_ID'])['Course_ID'].apply(list).reset_index()
    
    # Filtering and Sorting Data
    elec_data = st_hist_data[st_hist_data['Major'] == "Electrical Engineering"]
    elec_data = elec_data.sort_values(by=['Student_ID', 'Semester'])

    major = major_data["All_Courses"]
    courses_co = major_data["CO_Courses"]
    
    major["AREA_OF_STUDY"] = major["AREA_OF_STUDY"].fillna("NA")
    # Dropping records where AREA_OF_STUDY is 'N' and COURSE_OF_STUDY is 'Z'
    major_filtered = major[~((major['AREA_OF_STUDY'] == 'NA') & (major['COURSE_OF_STUDY'] == 'Z'))]
    
    major_filtered = major_filtered.copy()
    # Apply replacements directly to the specific columns to avoid SettingWithCopyWarning
    major_filtered['AREA_OF_STUDY'] = major_filtered['AREA_OF_STUDY'].replace("NA","GE")
    major_filtered['COURSE_OF_STUDY'] = major_filtered['COURSE_OF_STUDY'].replace("N","E")
    
    # Defining the major lists
    cea_majors = ['COMPENG', 'ELECENG', 'MGMTENG']
    df_cea = major_filtered[(major_filtered['Major'].isin(cea_majors)) & (major_filtered['COURSE_OF_STUDY'].isin(['R', 'RE']))]   
    
    list_conditions = ['-', 'ONE_COURSE']

    cea_list = df_cea[df_cea['Condition'].isin(list_conditions)]
    cea_special_cases = df_cea[~df_cea['Condition'].isin(list_conditions)]
    cea_co = courses_co[courses_co['Major'].isin(cea_majors)]
    
    elec_list = cea_list[cea_list["Major"] == "ELECENG"]
    elec_special_cases = cea_special_cases[cea_special_cases["Major"] == "ELECENG"]
    elec_co = cea_co[cea_co["Major"] == "ELECENG"]

    # Process 'REQUISITES_LIST'
    elec_co = elec_co.copy()
    elec_co.loc[:, 'REQUISITES_LIST'] = elec_co['REQUISITES_LIST'].apply(ast.literal_eval)

    # CAS Courses
    cea_courses = major_filtered[major_filtered['Major'].isin(cea_majors)]
    courses_elec = cea_courses[cea_courses["Major"] == "ELECENG"]
    
    grouped_data_elec = elec_data.groupby(['Student_ID'])['Course_ID'].apply(list).reset_index()

    # Merge dataframes
    merged_df = grouped_data_failed.merge(grouped_data_elec, on=['Student_ID'], how='outer', suffixes=('_failed', '_all'))
    # Replace NaN with empty lists to avoid errors
    merged_df['Course_ID_all'] = merged_df['Course_ID_all'].apply(lambda x: x if isinstance(x, list) else [])
    merged_df['Course_ID_failed'] = merged_df['Course_ID_failed'].apply(lambda x: x if isinstance(x, list) else [])

    merged_df['Failed_Courses'] = merged_df.apply(
        lambda row: list(set(row['Course_ID_failed']) - set(row['Course_ID_all'])),
        axis=1)
    # Keep only relevant columns
    merged_df = merged_df[['Student_ID', 'Failed_Courses']]

    # Extract Accounting specific requirements and weights from respective DataFrames
    requirements_df = pd.read_excel(requirements_weights_path,sheet_name="requirements")
    weights_df = pd.read_excel(requirements_weights_path,sheet_name="weights")
    requirements_elec = requirements_df[requirements_df["Major"] == "Electrical Engineering"]
    requirements_elec_ = requirements_elec.pivot_table(index="Major",columns="AREA_OF_STUDY",values ='Required_Courses' ,aggfunc='sum',fill_value=0).reset_index()
    weights_elec = weights_df[weights_df["Major"] == "Electrical Engineering"]

    student_courses = elec_data[["Student_ID", "Course_ID"]]

    # Map AREA_OF_STUDY and COURSE_OF_STUDY to elec_data
    student_courses = student_courses.merge(courses_elec[['Course_ID', 'AREA_OF_STUDY', 'COURSE_OF_STUDY', "Course_Level"]],
                                            on='Course_ID', how='left').drop_duplicates()

    # Create summary DataFrames for taken courses
    student_elecogress = student_courses.groupby(['Student_ID', 'AREA_OF_STUDY']).size().reset_index(name='Total_Taken_Courses')
    student_elecogress = student_elecogress.merge(requirements_elec, on='AREA_OF_STUDY', how='left')
    student_elecogress["Remaining_Courses"] = student_elecogress["Required_Courses"] - student_elecogress["Total_Taken_Courses"]
    student_elecogress["Remaining_Courses"] = student_elecogress["Remaining_Courses"].apply(lambda x: max(x, 0))

    free_elective_taken_counts = student_courses[(student_courses['AREA_OF_STUDY'] == "GE") & (student_courses['COURSE_OF_STUDY'] == "E")].groupby('Student_ID').size().reset_index(name='Total_Free_Electives_Taken')

    # Update progress by including the free elective data
    student_elecogress["Student_elecogress"] = (student_elecogress["Total_Taken_Courses"] / student_elecogress["Required_Courses"]) * 100
    student_elecogress["Student_elecogress"].replace([np.inf, -np.inf], 100, inplace=True)

    summary_area_of_study_taken = student_elecogress.pivot_table(index="Student_ID", columns="AREA_OF_STUDY", values="Total_Taken_Courses", fill_value=0)
    summary_area_of_study_taken = summary_area_of_study_taken.merge(free_elective_taken_counts, on="Student_ID", how="left").fillna(0).rename(columns={"Total_Free_Electives_Taken": "FE"})

    # Create a copy of summary_area_of_study_taken to work on remaining courses calculation
    remaining_courses_df = summary_area_of_study_taken.copy()

    # Loop through each AREA_OF_STUDY and calculate remaining courses by subtracting from the requirements
    for column in remaining_courses_df.columns:
        if column in requirements_elec['AREA_OF_STUDY'].values:
            required_courses = requirements_elec.loc[requirements_elec['AREA_OF_STUDY'] == column, 'Required_Courses'].values[0]
            remaining_courses_df[column] = required_courses - remaining_courses_df[column]
            remaining_courses_df[column] = remaining_courses_df[column].clip(lower=0)

    # Calculate weighted remaining courses
    weighted_remaining_courses_df = remaining_courses_df.copy()
    for column in weighted_remaining_courses_df.columns:
        if column in weights_elec['AREA_OF_STUDY'].values:
            weight_value = weights_elec.loc[weights_elec['AREA_OF_STUDY'] == column, 'Weight'].values[0]
            weighted_remaining_courses_df[column] = weighted_remaining_courses_df[column] * weight_value

    # Prepare weighted remaining courses for merge
    weighted_remaining_courses_df = weighted_remaining_courses_df.reset_index().melt(id_vars=['Student_ID'],
                                                                                      var_name='AREA_OF_STUDY',
                                                                                      value_name='Remaining_Courses_Weight_Score')
    weighted_remaining_courses_df = weighted_remaining_courses_df[weighted_remaining_courses_df["AREA_OF_STUDY"] != "index"]

    # Eligibility Calculation for Standard and Special Cases
    prerequisites_elec = elec_list.set_index('Course_ID')['REQUISITES_LIST'].apply(eval).to_dict()
    prerequisites_special_elec = elec_special_cases.set_index('Course_ID')['REQUISITES_LIST'].apply(eval).to_dict()
    conditions_elec = elec_special_cases.set_index('Course_ID')['Condition'].to_dict()

    final_results_elec = []  # Standard eligibility results
    final_results_special_elec = []  # Special eligibility results

    for student_id, group in elec_data.groupby('Student_ID'):
        cumulative_courses = set()
        for semester, semester_group in group.groupby('Semester'):
            taken_courses = set(semester_group['Course_ID'].tolist())
            cumulative_courses.update(taken_courses)

            # Determine Standard Eligible Courses
            student_info = semester_group.iloc[0].to_dict()
            eligible_courses = {course for course in prerequisites_elec.keys() if all(req in cumulative_courses for req in prerequisites_elec[course])}
            final_results_elec.append({
                'Student_ID': student_id,
                'Semester': semester,
                'Major': student_info['Major'],
                'College': student_info['College'],
                'Program': student_info['Program'],
                'Passed Credits': student_info['Passed Credits'],
                'Student_Level': student_info['Student_Level'],
                'Eligible_Courses': list(eligible_courses - cumulative_courses)
            })

            # Determine Special Eligible Courses
            special_eligible_courses = {
                course for course in prerequisites_special_elec.keys()
                if is_eligible_special_elec(course, cumulative_courses, student_info, prerequisites_special_elec, conditions_elec)
            }
            final_results_special_elec.append({
                'Student_ID': student_id,
                'Semester': semester,
                'Major': student_info['Major'],
                'College': student_info['College'],
                'Program': student_info['Program'],
                'Passed Credits': student_info['Passed Credits'],
                'Student_Level': student_info['Student_Level'],
                'Eligible_Courses': list(special_eligible_courses - cumulative_courses)
            })

    # Convert Results to DataFrames
    final_results_df_elec = pd.DataFrame(final_results_elec)
    final_results_special_df_elec = pd.DataFrame(final_results_special_elec)
    
    # Combine Eligible Courses from Both DataFrames
    combined_elec_list = combine_eligible_courses(final_results_df_elec, final_results_special_df_elec)
    # Find Course Combinations for Co-requisites
    combined_elec_list = combined_elec_list.apply(create_combined_courses, axis=1, co=elec_co)
    latest_eligible_courses = combined_elec_list.sort_values(by='Semester', ascending=False)
    latest_eligible_courses = latest_eligible_courses.groupby('Student_ID').first().reset_index()
    latest_eligible_courses = latest_eligible_courses.merge(grouped_data_elec,on = "Student_ID",how = "inner")
    latest_eligible_courses["Eligible_Courses_CO"] = latest_eligible_courses.apply(remove_matches, axis=1)
    latest_eligible_courses = latest_eligible_courses.apply(process_row, axis=1)
    latest_eligible_courses.drop(columns=["Course_ID"], inplace=True)

    latest_eligible_courses = latest_eligible_courses.merge(merged_df, on='Student_ID', how='outer')
    latest_eligible_courses['Failed_Courses'] = latest_eligible_courses['Failed_Courses'].apply(lambda x: x if isinstance(x, list) else [])
    latest_eligible_courses['Eligible_Courses_CO'] = latest_eligible_courses['Eligible_Courses_CO'].apply(lambda x: x if isinstance(x, list) else [])
    latest_eligible_courses['Eligible_Courses_CO'] = latest_eligible_courses.apply(
        lambda row: list(set(row['Eligible_Courses_CO']) | (set(row['Failed_Courses']) - set(row['Eligible_Courses_CO']))),axis=1)
    latest_eligible_courses = latest_eligible_courses.drop(columns=['Failed_Courses'])

    latest_info_failed = failed_data.loc[failed_data.groupby("Student_ID")["Semester"].idxmax()]
    missing_semester_df = latest_eligible_courses[latest_eligible_courses['Semester'].isna()]
    latest_eligible_courses.dropna(inplace=True)
    columns_to_fill = ['Semester', 'Major', 'College', 'Program', 'Passed Credits', 'Student_Level']

    for col in columns_to_fill:
        missing_semester_df.loc[missing_semester_df[col].isna(), col] = missing_semester_df.loc[
            missing_semester_df[col].isna(), 'Student_ID'
        ].map(latest_info_failed.set_index('Student_ID')[col])

    columns_to_convert = ['Semester', 'Student_Level', 'Passed Credits']
    for col in columns_to_convert:
        latest_eligible_courses.loc[:, col] = pd.to_numeric(latest_eligible_courses[col], errors='coerce').astype('Int64')
        
    latest_eligible_courses = pd.concat([latest_eligible_courses, missing_semester_df], ignore_index=True)

    max_semester_index = elec_data.groupby('Student_ID')['Semester'].idxmax()
    max_semester_data = elec_data.loc[max_semester_index, ['Student_ID', 'Semester']]

    last_semester_courses = pd.merge(max_semester_data, elec_data, on=['Student_ID', 'Semester'])
    eng097_fpu_students = last_semester_courses[last_semester_courses['Course_ID'] == 'ENGL097']
    # Target course list
    target_courses = ['ENGL098', 'MATH094', 'MATH095', 'MATH096', 'MATH098', 'MATH100', 'MATH111', 'MATH120', 'MATH121', 'MATH131', 'MATH140']

    eng097_fpu_students_eligible = latest_eligible_courses[latest_eligible_courses['Student_ID']
                                                       .isin(eng097_fpu_students['Student_ID'])].copy()
    eng097_fpu_students_eligible.loc[:, 'Eligible_Courses_CO'] = eng097_fpu_students_eligible['Eligible_Courses_CO'].apply(
    lambda courses: [course for course in courses if course in target_courses])

    latest_eligible_courses = latest_eligible_courses.merge(
    eng097_fpu_students_eligible[['Student_ID', 'Eligible_Courses_CO']],  # Relevant columns from filtered_students
    on='Student_ID',
    how='left',  # Keep all rows in students_df
    suffixes=('', '_updated'))  # Suffix to differentiate new column)

    latest_eligible_courses['Eligible_Courses_CO'] = latest_eligible_courses['Eligible_Courses_CO_updated'].combine_first(latest_eligible_courses['Eligible_Courses_CO'])
    latest_eligible_courses = latest_eligible_courses.drop(columns=['Eligible_Courses_CO_updated'])
    latest_eligible_courses = latest_eligible_courses.merge(grouped_data_elec,on = "Student_ID",how = "outer")
    latest_eligible_courses['Course_ID'] = latest_eligible_courses['Course_ID'].apply(lambda x: x if isinstance(x, list) else [])
    latest_eligible_courses = latest_eligible_courses.apply(process_row, axis=1)
    latest_eligible_courses.drop(columns=["Course_ID"], inplace=True)

    # Exploding DataFrame and mapping course details
    eligible_courses_comprehensive_data = latest_eligible_courses.explode("Eligible_Courses_CO")
    eligible_courses_comprehensive_data = eligible_courses_comprehensive_data.merge(courses_elec[['Course_ID', 'AREA_OF_STUDY', 'COURSE_OF_STUDY', 'Course_Level']],
                                                                                    left_on='Eligible_Courses_CO', right_on='Course_ID', how='left').drop(columns="Course_ID")
    eligible_courses_comprehensive_data['Eligible_Courses_CO'] = eligible_courses_comprehensive_data['Eligible_Courses_CO'].apply(lambda x: x if isinstance(x, list) else ([] if pd.isna(x) else [x]))

    # Find Additional Eligibilities
    eligible_courses_comprehensive_data['Future_Eligible_Courses_List'] = eligible_courses_comprehensive_data.apply(lambda row: find_additional_eligibilities(row['Eligible_Courses_CO'], set(row['Eligible_Courses_CO']), prerequisites_elec), axis=1)
    eligible_courses_per_student = eligible_courses_comprehensive_data.groupby('Student_ID')['Eligible_Courses_CO'].agg(lambda x: list(set([item for sublist in x for item in sublist if isinstance(sublist, list)]))).reset_index()

    # Merge aggregated list back to the comprehensive DataFrame
    eligible_courses_comprehensive_data = eligible_courses_comprehensive_data.merge(eligible_courses_per_student.rename(columns={'Eligible_Courses_CO': 'Eligible_Courses_List_All'}), on='Student_ID', how='left')

    # Filter matching courses from future eligible lists
    eligible_courses_comprehensive_data['Future_Eligible_Courses_List'] = eligible_courses_comprehensive_data.apply(lambda row: [course for course in row['Future_Eligible_Courses_List'] if course not in row['Eligible_Courses_List_All']], axis=1)
    eligible_courses_comprehensive_data['Total_Future_Eligible_Courses_List'] = eligible_courses_comprehensive_data['Future_Eligible_Courses_List'].apply(len)

    # Special eligibility courses
    eligible_courses_comprehensive_data['Future_Eligible_Courses_Special'] = eligible_courses_comprehensive_data.apply(lambda row: find_additional_eligibilities_special(row['Eligible_Courses_CO'], set(row['Eligible_Courses_CO']), row, prerequisites_special_elec, conditions_elec, is_eligible_special_elec_), axis=1)
    eligible_courses_comprehensive_data['Future_Eligible_Courses_Special'] = eligible_courses_comprehensive_data.apply(lambda row: [course for course in row['Future_Eligible_Courses_Special'] if course not in row['Eligible_Courses_List_All']], axis=1)
    eligible_courses_comprehensive_data['Total_Future_Eligible_Courses_Special'] = eligible_courses_comprehensive_data['Future_Eligible_Courses_Special'].apply(len)

    # Combine Future Eligible Courses and calculate the score
    eligible_courses_comprehensive_data["Future_Eligible_Courses"] = eligible_courses_comprehensive_data["Future_Eligible_Courses_List"] + eligible_courses_comprehensive_data["Future_Eligible_Courses_Special"]
    eligible_courses_comprehensive_data['Course_Score'] = eligible_courses_comprehensive_data['Future_Eligible_Courses'].apply(len)

    # Find Best Courses
    recommended_courses_elec = eligible_courses_comprehensive_data.groupby(['Student_ID', 'Semester']).apply(lambda group: pd.Series({'Recommended_Courses': find_best_courses(group)})).reset_index()


    eligible_courses_comprehensive_data = eligible_courses_comprehensive_data.merge(recommended_courses_elec, on=['Student_ID', 'Semester'])
    eligible_courses_comprehensive_data = eligible_courses_comprehensive_data.merge(weighted_remaining_courses_df, on=['Student_ID', 'AREA_OF_STUDY'], how='left')


    eligible_courses_comprehensive_data = eligible_courses_comprehensive_data.groupby('Student_ID', group_keys=False).apply(normalize_by_student)
    eligible_courses_comprehensive_data['Final_Score'] = (
        (eligible_courses_comprehensive_data['Normalized_Course_Score'] * 0.4) +
        (eligible_courses_comprehensive_data['Normalized_Remaining_Courses_Weight'] * 0.4) +
        (eligible_courses_comprehensive_data['Normalized_Course_Level'] * 0.2))

    # Find Best Courses
    recommended_courses_elec_v2 = eligible_courses_comprehensive_data.groupby(['Student_ID', 'Semester']).apply(lambda group: pd.Series({'Recommended_Courses_V2': find_best_courses_cea_v2(group)})).reset_index()
    eligible_courses_comprehensive_data = eligible_courses_comprehensive_data.merge(recommended_courses_elec_v2, on=['Student_ID', 'Semester'])

    recommended_courses = recommended_courses_elec.merge(recommended_courses_elec_v2,on=['Student_ID', 'Semester'])

    # Create summary DataFrames for eligible courses
    summary_area_of_study_eligible = eligible_courses_comprehensive_data.groupby(['Student_ID', 'AREA_OF_STUDY']).size().reset_index(name='Total_Eligible_Courses')
    summary_course_of_study_eligible = eligible_courses_comprehensive_data.groupby(['Student_ID', 'COURSE_OF_STUDY']).size().reset_index(name='Total_Eligible_Courses')
    summary_area_of_study_eligible = summary_area_of_study_eligible.pivot_table(index="Student_ID", columns="AREA_OF_STUDY", values="Total_Eligible_Courses", fill_value=0).reset_index()

    return requirements_elec_,student_elecogress,summary_area_of_study_taken,remaining_courses_df,latest_eligible_courses,eligible_courses_comprehensive_data,recommended_courses,summary_area_of_study_eligible

def process_data_comp(st_hist_data,major_data, requirements_weights_path):
    
    values_to_delete = ['FA', 'F', 'I', 'S', 'NP', 'WA']
    failed_grades = ['F','FA','NP']
    failed_data = st_hist_data[st_hist_data["GRADE"].isin(failed_grades)]
    st_hist_data = st_hist_data[~st_hist_data["GRADE"].isin(values_to_delete)]
    
    # Filtering and Sorting Data
    failed_data = failed_data[failed_data['Major'] == 'Computer Engineering']
    failed_data = failed_data.sort_values(by=['Student_ID', 'Semester'])

    grouped_data_failed = failed_data.groupby(['Student_ID'])['Course_ID'].apply(list).reset_index()
    
    # Filtering and Sorting Data
    comp_data = st_hist_data[st_hist_data['Major'] == "Computer Engineering"]
    comp_data = comp_data.sort_values(by=['Student_ID', 'Semester'])

    major = major_data["All_Courses"]
    courses_co = major_data["CO_Courses"]
    
    major["AREA_OF_STUDY"] = major["AREA_OF_STUDY"].fillna("NA")
    # Dropping records where AREA_OF_STUDY is 'N' and COURSE_OF_STUDY is 'Z'
    major_filtered = major[~((major['AREA_OF_STUDY'] == 'NA') & (major['COURSE_OF_STUDY'] == 'Z'))]
    
    major_filtered = major_filtered.copy()
    # Apply replacements directly to the specific columns to avoid SettingWithCopyWarning
    major_filtered['AREA_OF_STUDY'] = major_filtered['AREA_OF_STUDY'].replace("NA","GE")
    major_filtered['COURSE_OF_STUDY'] = major_filtered['COURSE_OF_STUDY'].replace("N","E")
    
    # Defining the major lists
    cea_majors = ['COMPENG', 'ELECENG', 'MGMTENG']
    df_cea = major_filtered[(major_filtered['Major'].isin(cea_majors)) & (major_filtered['COURSE_OF_STUDY'].isin(['R', 'RE']))]   
    
    list_conditions = ['-', 'ONE_COURSE']

    cea_list = df_cea[df_cea['Condition'].isin(list_conditions)]
    cea_special_cases = df_cea[~df_cea['Condition'].isin(list_conditions)]
    cea_co = courses_co[courses_co['Major'].isin(cea_majors)]
    
    comp_list = cea_list[cea_list["Major"] == "COMPENG"]
    comp_special_cases = cea_special_cases[cea_special_cases["Major"] == "COMPENG"]
    comp_co = cea_co[cea_co["Major"] == "COMPENG"]

    # Process 'REQUISITES_LIST'
    comp_co = comp_co.copy()
    comp_co.loc[:, 'REQUISITES_LIST'] = comp_co['REQUISITES_LIST'].apply(ast.literal_eval)

    # CAS Courses
    cea_courses = major_filtered[major_filtered['Major'].isin(cea_majors)]
    courses_comp = cea_courses[cea_courses["Major"] == "COMPENG"]
    
    grouped_data_comp = comp_data.groupby(['Student_ID'])['Course_ID'].apply(list).reset_index()

    # Merge dataframes
    merged_df = grouped_data_failed.merge(grouped_data_comp, on=['Student_ID'], how='outer', suffixes=('_failed', '_all'))
    # Replace NaN with empty lists to avoid errors
    merged_df['Course_ID_all'] = merged_df['Course_ID_all'].apply(lambda x: x if isinstance(x, list) else [])
    merged_df['Course_ID_failed'] = merged_df['Course_ID_failed'].apply(lambda x: x if isinstance(x, list) else [])

    merged_df['Failed_Courses'] = merged_df.apply(
        lambda row: list(set(row['Course_ID_failed']) - set(row['Course_ID_all'])),
        axis=1)
    # Keep only relevant columns
    merged_df = merged_df[['Student_ID', 'Failed_Courses']]

    # Extract Accounting specific requirements and weights from respective DataFrames
    requirements_df = pd.read_excel(requirements_weights_path,sheet_name="requirements")
    weights_df = pd.read_excel(requirements_weights_path,sheet_name="weights")
    requirements_comp = requirements_df[requirements_df["Major"] == "Computer Engineering"]
    requirements_comp_ = requirements_comp.pivot_table(index="Major",columns="AREA_OF_STUDY",values ='Required_Courses' ,aggfunc='sum',fill_value=0).reset_index()
    weights_comp = weights_df[weights_df["Major"] == "Computer Engineering"]

    student_courses = comp_data[["Student_ID", "Course_ID"]]

    # Map AREA_OF_STUDY and COURSE_OF_STUDY to comp_data
    student_courses = student_courses.merge(courses_comp[['Course_ID', 'AREA_OF_STUDY', 'COURSE_OF_STUDY', "Course_Level"]],
                                            on='Course_ID', how='left').drop_duplicates()

    # Create summary DataFrames for taken courses
    student_compogress = student_courses.groupby(['Student_ID', 'AREA_OF_STUDY']).size().reset_index(name='Total_Taken_Courses')
    student_compogress = student_compogress.merge(requirements_comp, on='AREA_OF_STUDY', how='left')
    student_compogress["Remaining_Courses"] = student_compogress["Required_Courses"] - student_compogress["Total_Taken_Courses"]
    student_compogress["Remaining_Courses"] = student_compogress["Remaining_Courses"].apply(lambda x: max(x, 0))

    free_comptive_taken_counts = student_courses[(student_courses['AREA_OF_STUDY'] == "GE") & (student_courses['COURSE_OF_STUDY'] == "E")].groupby('Student_ID').size().reset_index(name='Total_Free_comptives_Taken')

    # Update progress by including the free elective data
    student_compogress["Student_compogress"] = (student_compogress["Total_Taken_Courses"] / student_compogress["Required_Courses"]) * 100
    student_compogress["Student_compogress"].replace([np.inf, -np.inf], 100, inplace=True)

    summary_area_of_study_taken = student_compogress.pivot_table(index="Student_ID", columns="AREA_OF_STUDY", values="Total_Taken_Courses", fill_value=0)
    summary_area_of_study_taken = summary_area_of_study_taken.merge(free_comptive_taken_counts, on="Student_ID", how="left").fillna(0).rename(columns={"Total_Free_comptives_Taken": "FE"})

    # Create a copy of summary_area_of_study_taken to work on remaining courses calculation
    remaining_courses_df = summary_area_of_study_taken.copy()

    # Loop through each AREA_OF_STUDY and calculate remaining courses by subtracting from the requirements
    for column in remaining_courses_df.columns:
        if column in requirements_comp['AREA_OF_STUDY'].values:
            required_courses = requirements_comp.loc[requirements_comp['AREA_OF_STUDY'] == column, 'Required_Courses'].values[0]
            remaining_courses_df[column] = required_courses - remaining_courses_df[column]
            remaining_courses_df[column] = remaining_courses_df[column].clip(lower=0)

    # Calculate weighted remaining courses
    weighted_remaining_courses_df = remaining_courses_df.copy()
    for column in weighted_remaining_courses_df.columns:
        if column in weights_comp['AREA_OF_STUDY'].values:
            weight_value = weights_comp.loc[weights_comp['AREA_OF_STUDY'] == column, 'Weight'].values[0]
            weighted_remaining_courses_df[column] = weighted_remaining_courses_df[column] * weight_value

    # Prepare weighted remaining courses for merge
    weighted_remaining_courses_df = weighted_remaining_courses_df.reset_index().melt(id_vars=['Student_ID'],
                                                                                      var_name='AREA_OF_STUDY',
                                                                                      value_name='Remaining_Courses_Weight_Score')
    weighted_remaining_courses_df = weighted_remaining_courses_df[weighted_remaining_courses_df["AREA_OF_STUDY"] != "index"]

    # Eligibility Calculation for Standard and Special Cases
    prerequisites_comp = comp_list.set_index('Course_ID')['REQUISITES_LIST'].apply(eval).to_dict()
    prerequisites_special_comp = comp_special_cases.set_index('Course_ID')['REQUISITES_LIST'].apply(eval).to_dict()
    conditions_comp = comp_special_cases.set_index('Course_ID')['Condition'].to_dict()

    final_results_comp = []  # Standard eligibility results
    final_results_special_comp = []  # Special eligibility results

    for student_id, group in comp_data.groupby('Student_ID'):
        cumulative_courses = set()
        for semester, semester_group in group.groupby('Semester'):
            taken_courses = set(semester_group['Course_ID'].tolist())
            cumulative_courses.update(taken_courses)

            # Determine Standard Eligible Courses
            student_info = semester_group.iloc[0].to_dict()
            eligible_courses = {course for course in prerequisites_comp.keys() if all(req in cumulative_courses for req in prerequisites_comp[course])}
            final_results_comp.append({
                'Student_ID': student_id,
                'Semester': semester,
                'Major': student_info['Major'],
                'College': student_info['College'],
                'Program': student_info['Program'],
                'Passed Credits': student_info['Passed Credits'],
                'Student_Level': student_info['Student_Level'],
                'Eligible_Courses': list(eligible_courses - cumulative_courses)
            })

            # Determine Special Eligible Courses
            special_eligible_courses = {
                course for course in prerequisites_special_comp.keys()
                if is_eligible_special_comp(course, cumulative_courses, student_info, prerequisites_special_comp, conditions_comp)
            }
            final_results_special_comp.append({
                'Student_ID': student_id,
                'Semester': semester,
                'Major': student_info['Major'],
                'College': student_info['College'],
                'Program': student_info['Program'],
                'Passed Credits': student_info['Passed Credits'],
                'Student_Level': student_info['Student_Level'],
                'Eligible_Courses': list(special_eligible_courses - cumulative_courses)
            })

    # Convert Results to DataFrames
    final_results_df_comp = pd.DataFrame(final_results_comp)
    final_results_special_df_comp = pd.DataFrame(final_results_special_comp)
    
    # Combine Eligible Courses from Both DataFrames
    combined_comp_list = combine_eligible_courses(final_results_df_comp, final_results_special_df_comp)
    # Find Course Combinations for Co-requisites
    combined_comp_list = combined_comp_list.apply(create_combined_courses, axis=1, co=comp_co)
    latest_eligible_courses = combined_comp_list.sort_values(by='Semester', ascending=False)
    latest_eligible_courses = latest_eligible_courses.groupby('Student_ID').first().reset_index()
    latest_eligible_courses = latest_eligible_courses.merge(grouped_data_comp,on = "Student_ID",how = "inner")
    latest_eligible_courses["Eligible_Courses_CO"] = latest_eligible_courses.apply(remove_matches, axis=1)
    latest_eligible_courses = latest_eligible_courses.apply(process_row, axis=1)
    latest_eligible_courses.drop(columns=["Course_ID"], inplace=True)

    latest_eligible_courses = latest_eligible_courses.merge(merged_df, on='Student_ID', how='outer')
    latest_eligible_courses['Failed_Courses'] = latest_eligible_courses['Failed_Courses'].apply(lambda x: x if isinstance(x, list) else [])
    latest_eligible_courses['Eligible_Courses_CO'] = latest_eligible_courses['Eligible_Courses_CO'].apply(lambda x: x if isinstance(x, list) else [])
    latest_eligible_courses['Eligible_Courses_CO'] = latest_eligible_courses.apply(
        lambda row: list(set(row['Eligible_Courses_CO']) | (set(row['Failed_Courses']) - set(row['Eligible_Courses_CO']))),axis=1)
    latest_eligible_courses = latest_eligible_courses.drop(columns=['Failed_Courses'])

    latest_info_failed = failed_data.loc[failed_data.groupby("Student_ID")["Semester"].idxmax()]
    missing_semester_df = latest_eligible_courses[latest_eligible_courses['Semester'].isna()]
    latest_eligible_courses.dropna(inplace=True)
    columns_to_fill = ['Semester', 'Major', 'College', 'Program', 'Passed Credits', 'Student_Level']

    for col in columns_to_fill:
        missing_semester_df.loc[missing_semester_df[col].isna(), col] = missing_semester_df.loc[
            missing_semester_df[col].isna(), 'Student_ID'
        ].map(latest_info_failed.set_index('Student_ID')[col])

    columns_to_convert = ['Semester', 'Student_Level', 'Passed Credits']
    for col in columns_to_convert:
        latest_eligible_courses.loc[:, col] = pd.to_numeric(latest_eligible_courses[col], errors='coerce').astype('Int64')
        
    latest_eligible_courses = pd.concat([latest_eligible_courses, missing_semester_df], ignore_index=True)

    max_semester_index = comp_data.groupby('Student_ID')['Semester'].idxmax()
    max_semester_data = comp_data.loc[max_semester_index, ['Student_ID', 'Semester']]

    last_semester_courses = pd.merge(max_semester_data, comp_data, on=['Student_ID', 'Semester'])
    eng097_fpu_students = last_semester_courses[last_semester_courses['Course_ID'] == 'ENGL097']
    # Target course list
    target_courses = ['ENGL098', 'MATH094', 'MATH095', 'MATH096', 'MATH098', 'MATH100', 'MATH111', 'MATH120', 'MATH121', 'MATH131', 'MATH140']

    eng097_fpu_students_eligible = latest_eligible_courses[latest_eligible_courses['Student_ID']
                                                       .isin(eng097_fpu_students['Student_ID'])].copy()
    eng097_fpu_students_eligible.loc[:, 'Eligible_Courses_CO'] = eng097_fpu_students_eligible['Eligible_Courses_CO'].apply(
    lambda courses: [course for course in courses if course in target_courses])

    latest_eligible_courses = latest_eligible_courses.merge(
    eng097_fpu_students_eligible[['Student_ID', 'Eligible_Courses_CO']],  # Relevant columns from filtered_students
    on='Student_ID',
    how='left',  # Keep all rows in students_df
    suffixes=('', '_updated'))  # Suffix to differentiate new column)

    latest_eligible_courses['Eligible_Courses_CO'] = latest_eligible_courses['Eligible_Courses_CO_updated'].combine_first(latest_eligible_courses['Eligible_Courses_CO'])
    latest_eligible_courses = latest_eligible_courses.drop(columns=['Eligible_Courses_CO_updated'])
    latest_eligible_courses = latest_eligible_courses.merge(grouped_data_comp,on = "Student_ID",how = "outer")
    latest_eligible_courses['Course_ID'] = latest_eligible_courses['Course_ID'].apply(lambda x: x if isinstance(x, list) else [])
    latest_eligible_courses = latest_eligible_courses.apply(process_row, axis=1)
    latest_eligible_courses.drop(columns=["Course_ID"], inplace=True)

    # Exploding DataFrame and mapping course details
    eligible_courses_comprehensive_data = latest_eligible_courses.explode("Eligible_Courses_CO")
    eligible_courses_comprehensive_data = eligible_courses_comprehensive_data.merge(courses_comp[['Course_ID', 'AREA_OF_STUDY', 'COURSE_OF_STUDY', 'Course_Level']],
                                                                                    left_on='Eligible_Courses_CO', right_on='Course_ID', how='left').drop(columns="Course_ID")
    eligible_courses_comprehensive_data['Eligible_Courses_CO'] = eligible_courses_comprehensive_data['Eligible_Courses_CO'].apply(lambda x: x if isinstance(x, list) else ([] if pd.isna(x) else [x]))

    # Find Additional Eligibilities
    eligible_courses_comprehensive_data['Future_Eligible_Courses_List'] = eligible_courses_comprehensive_data.apply(lambda row: find_additional_eligibilities(row['Eligible_Courses_CO'], set(row['Eligible_Courses_CO']), prerequisites_comp), axis=1)
    eligible_courses_per_student = eligible_courses_comprehensive_data.groupby('Student_ID')['Eligible_Courses_CO'].agg(lambda x: list(set([item for sublist in x for item in sublist if isinstance(sublist, list)]))).reset_index()

    # Merge aggregated list back to the comprehensive DataFrame
    eligible_courses_comprehensive_data = eligible_courses_comprehensive_data.merge(eligible_courses_per_student.rename(columns={'Eligible_Courses_CO': 'Eligible_Courses_List_All'}), on='Student_ID', how='left')

    # Filter matching courses from future eligible lists
    eligible_courses_comprehensive_data['Future_Eligible_Courses_List'] = eligible_courses_comprehensive_data.apply(lambda row: [course for course in row['Future_Eligible_Courses_List'] if course not in row['Eligible_Courses_List_All']], axis=1)
    eligible_courses_comprehensive_data['Total_Future_Eligible_Courses_List'] = eligible_courses_comprehensive_data['Future_Eligible_Courses_List'].apply(len)

    # Special eligibility courses
    eligible_courses_comprehensive_data['Future_Eligible_Courses_Special'] = eligible_courses_comprehensive_data.apply(lambda row: find_additional_eligibilities_special(row['Eligible_Courses_CO'], set(row['Eligible_Courses_CO']), row, prerequisites_special_comp, conditions_comp, is_eligible_special_comp_), axis=1)
    eligible_courses_comprehensive_data['Future_Eligible_Courses_Special'] = eligible_courses_comprehensive_data.apply(lambda row: [course for course in row['Future_Eligible_Courses_Special'] if course not in row['Eligible_Courses_List_All']], axis=1)
    eligible_courses_comprehensive_data['Total_Future_Eligible_Courses_Special'] = eligible_courses_comprehensive_data['Future_Eligible_Courses_Special'].apply(len)

    # Combine Future Eligible Courses and calculate the score
    eligible_courses_comprehensive_data["Future_Eligible_Courses"] = eligible_courses_comprehensive_data["Future_Eligible_Courses_List"] + eligible_courses_comprehensive_data["Future_Eligible_Courses_Special"]
    eligible_courses_comprehensive_data['Course_Score'] = eligible_courses_comprehensive_data['Future_Eligible_Courses'].apply(len)

    # Find Best Courses
    recommended_courses_comp = eligible_courses_comprehensive_data.groupby(['Student_ID', 'Semester']).apply(lambda group: pd.Series({'Recommended_Courses': find_best_courses(group)})).reset_index()


    eligible_courses_comprehensive_data = eligible_courses_comprehensive_data.merge(recommended_courses_comp, on=['Student_ID', 'Semester'])
    eligible_courses_comprehensive_data = eligible_courses_comprehensive_data.merge(weighted_remaining_courses_df, on=['Student_ID', 'AREA_OF_STUDY'], how='left')


    eligible_courses_comprehensive_data = eligible_courses_comprehensive_data.groupby('Student_ID', group_keys=False).apply(normalize_by_student)
    eligible_courses_comprehensive_data['Final_Score'] = (
        (eligible_courses_comprehensive_data['Normalized_Course_Score'] * 0.4) +
        (eligible_courses_comprehensive_data['Normalized_Remaining_Courses_Weight'] * 0.4) +
        (eligible_courses_comprehensive_data['Normalized_Course_Level'] * 0.2))

    # Find Best Courses
    recommended_courses_comp_v2 = eligible_courses_comprehensive_data.groupby(['Student_ID', 'Semester']).apply(lambda group: pd.Series({'Recommended_Courses_V2': find_best_courses_cea_v2(group)})).reset_index()
    eligible_courses_comprehensive_data = eligible_courses_comprehensive_data.merge(recommended_courses_comp_v2, on=['Student_ID', 'Semester'])

    recommended_courses = recommended_courses_comp.merge(recommended_courses_comp_v2,on=['Student_ID', 'Semester'])

    # Create summary DataFrames for eligible courses
    summary_area_of_study_eligible = eligible_courses_comprehensive_data.groupby(['Student_ID', 'AREA_OF_STUDY']).size().reset_index(name='Total_Eligible_Courses')
    summary_course_of_study_eligible = eligible_courses_comprehensive_data.groupby(['Student_ID', 'COURSE_OF_STUDY']).size().reset_index(name='Total_Eligible_Courses')
    summary_area_of_study_eligible = summary_area_of_study_eligible.pivot_table(index="Student_ID", columns="AREA_OF_STUDY", values="Total_Eligible_Courses", fill_value=0).reset_index()

    return requirements_comp_,student_compogress,summary_area_of_study_taken,remaining_courses_df,latest_eligible_courses,eligible_courses_comprehensive_data,recommended_courses,summary_area_of_study_eligible


# Define the query
st_enrollment_query = """
SELECT 
    q1.EMPLID,
    q1.Status,
    q1.STRM,
    q1.Level,
    q1.Course,
    q1.GRADE,
    q1.CREDITS,
    q1.Course_Department,
    q1.College,
    q1.Program,
    q1.[Plan],
    q1.ADMIT_TERM,
    q1.ACAD_PROG,
    q1.ACAD_PLAN,
    q1.[Passed Credits],
    q1.CUM_GPA,
    q1.MPA,
    q2.PROG_STATUS,
    q2.ACAD_CAREER
FROM 
    (
        -- Begin query1
        SELECT DISTINCT 
            A.EMPLID,
            CASE WHEN AP.PROG_STATUS = 'AC' THEN 'Active'
                 WHEN AP.PROG_STATUS = 'CN' THEN 'University Withdrawal'
                 WHEN AP.PROG_STATUS = 'CM' THEN 'Graduated'
                 WHEN AP.PROG_STATUS = 'DM' THEN 'Dismissed'
                 WHEN AP.PROG_STATUS = 'DC' THEN 'Discontinue'
                 WHEN AP.PROG_STATUS = 'LA' THEN 'Leave of Absence'
                 WHEN AP.PROG_STATUS = 'SP' THEN 'Suspended'
                 ELSE X.XLATLONGNAME END AS Status,   
            A.STRM,
            CASE WHEN T.ACAD_LEVEL_BOT = '10' THEN '1- Freshman'
                 WHEN T.ACAD_LEVEL_BOT = '20' THEN '2- Sophomore'
                 WHEN T.ACAD_LEVEL_BOT = '30' THEN '3- Junior'
                 WHEN T.ACAD_LEVEL_BOT = '40' THEN '4- Senior'
                 ELSE 'NA' END AS Level,
            LTRIM(RTRIM(A.SUBJECT)) + LTRIM(RTRIM(A.CATALOG_NBR)) AS Course,
            A.CRSE_GRADE_OFF AS GRADE,
            A.UNT_TAKEN AS CREDITS,
            G.DESCR AS Course_Department,
            PR.ACAD_GROUP AS College,
            PR.DESCR AS Program,
            PL.DESCR AS [Plan],
            AP.ADMIT_TERM,
            AP.ACAD_PROG,
            APL.ACAD_PLAN,
            S.TOT_CUMULATIVE AS [Passed Credits],
            S.CUM_GPA,
            S.GU_MPA AS MPA
        FROM 
            [MFPS09].[SIS89PRD].dbo.PS_STDNT_ENRL_L_VW A 
        LEFT JOIN 
            [MFPS09].[SIS89PRD].dbo.PS_ACAD_PROG AP ON AP.EMPLID = A.EMPLID 
            AND AP.EFFDT = (SELECT MAX(AP1.EFFDT) FROM [MFPS09].[SIS89PRD].dbo.PS_ACAD_PROG AP1 WHERE AP1.EMPLID = AP.EMPLID)
            AND AP.EFFSEQ = (SELECT MAX(AP1.EFFSEQ) FROM [MFPS09].[SIS89PRD].dbo.PS_ACAD_PROG AP1 WHERE AP1.EMPLID = AP.EMPLID AND AP.EFFDT = AP1.EFFDT)
        LEFT JOIN 
            [MFPS09].[SIS89PRD].dbo.PS_ACAD_PLAN APL ON APL.EMPLID = AP.EMPLID AND APL.EFFDT = AP.EFFDT AND APL.EFFSEQ = AP.EFFSEQ
        LEFT JOIN 
            [MFPS09].[SIS89PRD].dbo.PSXLATITEM X ON X.FIELDNAME = 'PROG_STATUS' AND X.FIELDVALUE = AP.PROG_STATUS
        LEFT JOIN 
            [MFPS09].[SIS89PRD].dbo.PS_STDNT_CAR_TERM T ON A.EMPLID = T.EMPLID AND T.STRM = A.STRM
        LEFT JOIN 
            [MFPS09].[SIS89PRD].dbo.PS_GU_STDNT_STATS S ON A.EMPLID = S.EMPLID AND S.STRM = (SELECT MAX(S1.STRM) FROM [MFPS09].[SIS89PRD].dbo.PS_GU_STDNT_STATS S1 WHERE S1.EMPLID = S.EMPLID AND S1.STRM <= A.STRM)
        LEFT JOIN 
            [MFPS09].[SIS89PRD].dbo.PS_GU_CRSE_DEPT D ON A.CRSE_ID = D.CRSE_ID AND D.EFFDT = (SELECT MAX(D1.EFFDT) FROM [MFPS09].[SIS89PRD].dbo.PS_GU_CRSE_DEPT D1 WHERE D1.CRSE_ID = D.CRSE_ID)
        LEFT JOIN 
            [MFPS09].[SIS89PRD].dbo.PS_GU_DEPT_TBL G ON D.DEPTID = G.DEPTID
        LEFT JOIN 
            [MFPS09].[SIS89PRD].dbo.PS_ACAD_PROG_TBL PR ON PR.ACAD_PROG = AP.ACAD_PROG AND PR.EFFDT = (SELECT MAX(PR1.EFFDT) FROM [MFPS09].[SIS89PRD].dbo.PS_ACAD_PROG_TBL PR1 WHERE PR1.ACAD_PROG = PR.ACAD_PROG)
        LEFT JOIN 
            [MFPS09].[SIS89PRD].dbo.PS_ACAD_PLAN_TBL PL ON LTRIM(RTRIM(PL.ACAD_PLAN)) = APL.ACAD_PLAN AND PL.EFFDT = (SELECT MAX(PL1.EFFDT) FROM [MFPS09].[SIS89PRD].dbo.PS_ACAD_PLAN_TBL PL1 WHERE PL1.ACAD_PLAN = PL.ACAD_PLAN)
        WHERE 
            A.STRM >= '0801' 
            AND A.STDNT_ENRL_STATUS = 'E' 
            AND A.ENRL_STATUS_REASON IN ('ENRL', 'EWAT')
            AND A.GRADING_BASIS_ENRL <> 'NON'
            AND A.ACAD_CAREER = 'UGRD'
        -- End query1
    ) AS q1
INNER JOIN 
    (
        -- Begin query2
        SELECT *
        FROM SIS_STUDENT_STATUS 
        WHERE PROG_STATUS IN ('AC', 'LA')
          AND ACAD_CAREER = 'UGRD'
        -- End query2
    ) AS q2
ON q1.EMPLID COLLATE SQL_Latin1_General_CP1_CI_AS = q2.STUDENT_ID COLLATE SQL_Latin1_General_CP1_CI_AS
ORDER BY q1.STRM, q1.EMPLID;

"""

tc_query = """
SELECT 
    CONCAT(TRIM(SUBJECT), TRIM(CATALOG_NBR)) AS Course_ID, 
    * 
FROM 
    SIS_TRANSFER_COURSES WHERE 
    STUDENT_ID COLLATE Latin1_General_BIN IN (
        SELECT STUDENT_ID 
        FROM SIS_STUDENT_STATUS 
        WHERE PROG_STATUS IN ('AC', 'LA') 
          AND ACAD_CAREER = 'UGRD'
    );
    
"""


major_processing_functions = {
    "Accounting": process_data_acc,
    "International Business": process_data_ib,
    "Mgmt & Organizational Behavior": process_data_mob,
    "Management Information Systems": process_data_mis,
    "Marketing": process_data_mrkt,
    "Finance": process_data_fin,
    "Computer Science": process_data_cs,
    "Digital Media Production": process_data_dmp,
    "Eng- Linguistics - Translation": process_data_eng_lin,
    "English Education": process_data_eng_edu,
    "English Literature": process_data_eng_lit,
    "Public relations & Advertising": process_data_pr,
    "Visual Communication": process_data_vc,
    "Engineering Management": process_data_mgmt,
    "Electrical Engineering": process_data_elec,
    "Computer Engineering": process_data_comp
}



if navigation == "User Guide":
    st.title("User Guide")
    st.write("Welcome to the User Guide. Please choose an option below to learn more:")
    
    guide_option = st.selectbox("Choose an option:", ["Please select the required page!","Course Eligibility and Recommendation System", "Quick Check"])
    
    
    if guide_option == "Please select the required page!":
        st.info("No Page selected")

    if guide_option == "Course Eligibility and Recommendation System":
        with st.expander("Steps for Course Eligibility and Recommendation System"):
            st.markdown("""
                ### Select College and Major

                #### Choose a College:
                Use the dropdown labeled **"Select College"** to pick from the available colleges: **CBA**, **CAS**, or **CEA**.

                #### Choose Major(s):
                Once a college is selected, available majors will be displayed in a multi-select box. Select one or more majors to proceed.

                ### View Eligible and Recommended Courses

                #### Select Data Type:
                Use the **"Select Data to Display"** dropdown to choose the type of report you want:

                - **Major Sheet Requirements Data**: Shows course requirements for the selected major(s).
                - **Student Progress Report**: Displays the student's progress by area of study.
                - **Summary of Taken Courses by AREA_OF_STUDY**: Summarizes courses completed in each area of study.
                - **Remaining Courses by AREA_OF_STUDY**: Lists remaining courses needed.
                - **Latest Eligible Courses**: Shows the latest courses that the student is eligible to take.
                - **Comprehensive Eligible Courses Data**: Provides a comprehensive view of all eligible courses.
                - **Recommended Courses Report**: Recommends courses based on eligibility.
                - **Summary of Eligible Courses by AREA_OF_STUDY**: Summarizes eligible courses by study area.

                #### Viewing and Downloading Data

                ##### Viewing Data:
                Processed data will display in a table format.

                ##### Download Data as CSV:
                For each report, a download button is available to save the data as a **CSV file** for further use.
                """)

        
    elif guide_option == "Quick Check":
        with st.expander("Steps for Quick Check"):
            st.markdown("""
                ### Quick Check - Course Eligibility and Recommendation System

                #### 1. Input Semester Details
                You can specify multiple semesters, each with its own set of details:
                - **Number of Semesters to Add**: Use the number input to define how many semesters to include.
                - For each semester:
                - **Student ID**: Enter the unique ID for the student.
                - **Semester**: Specify the semester number.
                - **College**: Select the college from a dropdown (options: "CBA", "CAS", "CEA").
                - **Program**: Once a college is selected, choose the program (e.g., "Accounting" or "Finance").
                - **Major**: Based on the chosen program, select the specific major.
                - **Passed Credits**: Input the number of credits the student has already completed.
                - **Student Level**: Select the student’s current level (Freshman, Sophomore, Junior, Senior).
                - **Course ID**: Use the multi-select box to choose the courses from the loaded course list.
                - **Incoming PCR**: Enter the student's incoming PCR value.

                #### 2. Process Data
                - **Process Manual Input Data**: Check this box to begin processing the entered student data.

                #### 3. Select Data to Display
                After processing, choose from the following report options to view and download:

                - **Major Sheet Requirements Data**: Displays the course requirements for each major.
                - **Student Progress Report**: Shows student progress by area of study.
                - **Summary of Taken Courses by AREA_OF_STUDY**: Summarizes completed courses by study area.
                - **Remaining Courses by AREA_OF_STUDY**: Lists courses still required by study area.
                - **Latest Eligible Courses**: Shows the most recent courses the student is eligible for.
                - **Comprehensive Eligible Courses Data**: Provides a detailed view of all eligible courses.
                - **Recommended Courses Report**: Offers course recommendations based on eligibility.
                - **Summary of Eligible Courses by AREA_OF_STUDY**: Summarizes eligible courses in each study area.

                #### 4. Downloading Reports
                Each report is available for download in **CSV format**. Use the provided download button to save any displayed report as a CSV file.

                #### Note
                Ensure all required fields are correctly filled out before processing, or error messages will display.
                """)



elif navigation == "Course Eligibility and Recommendation System":
    st.title("Course Eligibility and Recommendation System")
    

    st.header("Select College & Major")
    selected_college = st.selectbox("Select College:", ["Please Select The Required College!", "CBA", "CAS", "CEA"])

    if selected_college == "Please Select The Required College!":
        st.warning("No College Selected!")
        majors = ["No College Selected!"]
    elif selected_college == "CBA":
        majors = ["Accounting", "International Business", "Mgmt & Organizational Behavior", "Management Information Systems", "Marketing", "Finance"]
    elif selected_college == "CAS":
        majors = ["Computer Science", "Digital Media Production", "Eng- Linguistics - Translation", "English Education", "English Literature", "Public relations & Advertising", "Visual Communication"]
    elif selected_college == "CEA":
        majors = ["Electrical Engineering", "Computer Engineering", "Engineering Management"]

    selected_major = st.multiselect("Select Major:", majors)

    st.header("Eligible & Recommended Courses")
    
    if selected_college != "Please Select The Required College!" and selected_major:

        section = st.selectbox("Select Data to Display", [
            "None", "Major Sheet Requirements Data","Student Progress Report","Summary of Taken Courses by AREA_OF_STUDY","Remaining Courses by AREA_OF_STUDY","Latest Eligible Courses",
            "Comprehensive Eligible Courses Data", "Recommended Courses Report", "Summary of Eligible Courses by AREA_OF_STUDY"])

        if section != "None":
            # Load Major Data, Requirements, and Weights
            try:
                major_data = pd.read_excel("Updated_MajorSheet_.xlsx", sheet_name=None)
                # Call the function with the query as an argument
                ac_st_enrollment_data = fetch_data_from_db(st_enrollment_query)
                tc_data = fetch_data_from_db(tc_query)
                st_hist_data = st_data_cleaning(ac_st_enrollment_data,tc_data)
                st_hist_data = st_hist_data[st_hist_data["Semester"] != 2402]
            except Exception as e:
                st.error(f"Error loading sheets: {e}")

            # Reading student data
            data = st_hist_data
            # Check for major mismatch
            majors_in_data = data['Major'].unique()
            college_major_map = {
                "CBA": ["Accounting", "International Business", "Mgmt & Organizational Behavior", "Management Information Systems", "Marketing", "Finance"],
                "CAS": ["Computer Science", "Digital Media Production", "Eng- Linguistics - Translation", "English Education", "English Literature", "Public relations & Advertising", "Visual Communication"],
                "CEA": ["Electrical Engineering", "Computer Engineering", "Engineering Management"]
            }


            requirement_df_list = []
            student_progrsss_list = []
            summary_taken_list = []
            remaining_courses_list = []
            latest_eligible_courses_list = []
            comprehensive_eligible_list = []
            recommended_courses_list = []
            summary_eligible_list = []

            for major in selected_major:
                st.write(f"Processing data for major: {major}")
                major_data_subset = data[data['Major'] == major]

                process_function = major_processing_functions.get(major)

                if process_function:
                    with st.spinner(f"Processing data for major: {major}..."):
                        requirements_df,student_progress,summary_area_of_study_taken,remaining_courses_df,latest_eligible_courses,eligible_courses_comprehensive_data,recommended_courses,summary_area_of_study_eligible = process_function(
                            major_data_subset, major_data, "Requierments_Weights.xlsx"
                        )

                    requirement_df_list.append(requirements_df)
                    student_progrsss_list.append(student_progress)
                    summary_taken_list.append(summary_area_of_study_taken)
                    remaining_courses_list.append(remaining_courses_df)
                    latest_eligible_courses_list.append(latest_eligible_courses)
                    comprehensive_eligible_list.append(eligible_courses_comprehensive_data)
                    recommended_courses_list.append(recommended_courses)
                    summary_eligible_list.append(summary_area_of_study_eligible)
                else:
                    st.error(f"No processing function found for major: {major}")

            # Combine the processed dataframes for all majors
            if requirement_df_list:
                requirements_df = pd.concat(requirement_df_list, ignore_index=True)
                student_progress = pd.concat(student_progrsss_list, ignore_index=True)
                summary_area_of_study_taken = pd.concat(summary_taken_list, ignore_index=True)
                remaining_courses_df = pd.concat(remaining_courses_list, ignore_index=True)
                latest_eligible_courses = pd.concat(latest_eligible_courses_list, ignore_index=True)
                eligible_courses_comprehensive_data = pd.concat(comprehensive_eligible_list, ignore_index=True)
                recommended_courses = pd.concat(recommended_courses_list, ignore_index=True)
                summary_area_of_study_eligible = pd.concat(summary_eligible_list, ignore_index=True)

                st.success("Data processed successfully for all majors!")

                if section == "Major Sheet Requirements Data":
                    st.header("Major Sheet Requirements Data")
                    st.dataframe(requirements_df)

                    # Download the DataFrame as CSV
                    st.header("Download Major Sheet Requirements Data")
                    csv = requirements_df.to_csv(index=False)
                    st.download_button(
                        label="Download data as CSV",
                        data=csv,
                        file_name='requirements_df.csv',
                        mime='text/csv',
                    )
                elif section == "Student Progress Report":
                    st.header("Student Progress Report")
                    st.dataframe(student_progress)

                    # Download the DataFrame as CSV
                    st.header("Download Student Progress Report")
                    csv = student_progress.to_csv(index=False)
                    st.download_button(
                        label="Download data as CSV",
                        data=csv,
                        file_name='student_progress.csv',
                        mime='text/csv',
                    )
                elif section == "Summary of Taken Courses by AREA_OF_STUDY":
                    st.header("Summary of Taken Courses by AREA_OF_STUDY")
                    st.dataframe(summary_area_of_study_taken)

                    # Download the DataFrame as CSV
                    st.header("Download Recommended Courses Data")
                    csv = summary_area_of_study_taken.to_csv(index=False)
                    st.download_button(
                        label="Download data as CSV",
                        data=csv,
                        file_name='summary_area_of_study_taken.csv',
                        mime='text/csv',
                    )
                elif section == "Remaining Courses by AREA_OF_STUDY":
                    st.header("Remaining Courses by AREA_OF_STUDY")
                    st.dataframe(remaining_courses_df)

                    # Download the DataFrame as CSV
                    st.header("Download Remaining Courses by AREA_OF_STUDY")
                    csv = remaining_courses_df.to_csv(index=False)
                    st.download_button(
                        label="Download data as CSV",
                        data=csv,
                        file_name='remaining_courses_df.csv',
                        mime='text/csv',
                    )
                elif section == "Latest Eligible Courses":
                    st.header("Latest Eligible Courses Report")
                    st.dataframe(latest_eligible_courses)
                    csv = latest_eligible_courses.to_csv(index=False)
                    st.download_button("Download data as CSV", data=csv, file_name='latest_eligible_courses.csv', mime='text/csv'
                    )
                elif section == "Comprehensive Eligible Courses Data":
                    st.header("Comprehensive Eligible Courses Data")
                    st.dataframe(eligible_courses_comprehensive_data)
                    csv = eligible_courses_comprehensive_data.to_csv(index=False)
                    st.download_button("Download data as CSV", data=csv, file_name='eligible_courses_comprehensive_data.csv', mime='text/csv'
                    )
                elif section == "Recommended Courses Report":   
                    st.header("Recommended Courses Report")
                    st.dataframe(recommended_courses)
                    csv = recommended_courses.to_csv(index=False)
                    st.download_button("Download data as CSV", data=csv, file_name='recommended_courses.csv', mime='text/csv'
                    )
                elif section == "Summary of Eligible Courses by AREA_OF_STUDY":
                    st.header("Summary of Eligible Courses by AREA_OF_STUDY")
                    st.dataframe(summary_area_of_study_eligible)
                    csv = summary_area_of_study_eligible.to_csv(index=False)
                    st.download_button("Download data as CSV", data=csv, file_name='summary_area_of_study_eligible.csv', mime='text/csv')
        else:
            st.warning("Please Choose the required Data!")

if navigation == "Quick Check":
    st.title("Course Eligibility and Recommendation System")
    
    num_semesters = st.number_input("Number of Semesters to Add:", min_value=1, value=1, step=1)
    student_info_list = []

    for i in range(num_semesters):
        st.subheader(f"Semester {i + 1} Information")

        student_id = st.text_input(f"Student ID (Semester {i + 1}):")
        semester = st.number_input(f"Semester (Semester {i + 1}):", min_value=1, value=1, step=1)
        college = st.selectbox(f"College (Semester {i + 1}):", ["Please Select The Required College!", "CBA", "CAS", "CEA"], key=f"college_{i}")

        if college == "Please Select The Required College!":
            st.warning("No College Selected!")
            programs = ["No College Selected!"]
            majors = ["No Program Selected!"]
        elif college == "CBA":
            programs = ["Please Choose the required program!", "Accounting", "Finance", "Marketing", "Management Information Systems", "Business Administration"]
        elif college == "CAS":
            programs = ["Please Choose the required program!", "Computer Science", "English", "Mass Communication"]
        elif college == "CEA":
            programs = ["Please Choose the required program!", "Computer Engineering", "Electrical Engineering", "Engineering Management"]

        program = st.selectbox(f"Program (Semester {i + 1}):", programs, key=f"program_{i}")

        if college == "CBA":
            if program == "Please Choose the required program!":
                st.warning("Please Choose the required program!")
                majors = ["No Program Selected!"]
            elif program == "Accounting":
                majors = ["Accounting"]
            elif program == "Finance":
                majors = ["Finance"]
            elif program == "Marketing":
                majors = ["Marketing"]
            elif program == "Management Information Systems":
                majors = ["Management Information Systems"]
            elif program == "Business Administration":
                majors = ["Mgmt & Organizational Behavior", "International Business"]
        elif college == "CAS":
            if program == "Please Choose the required program!":
                st.warning("Please Choose the required program!")
                majors = ["No Program Selected!"]
            elif program == "Computer Science":
                majors = ["Computer Science"]
            elif program == "English":
                majors = ["English Education", "Eng- Linguistics - Translation", "English Literature"]
            elif program == "Mass Communication":
                majors = ["Public relations & Advertising", "Digital Media Production", "Visual Communication"]
        elif college == "CEA":
            if program == "Please Choose the required program!":
                st.warning("Please Choose the required program!")
                majors = ["No Program Selected!"]
            elif program == "Computer Engineering":
                majors = ["Computer Engineering"]
            elif program == "Electrical Engineering":
                majors = ["Electrical Engineering"]
            elif program == "Engineering Management":
                majors = ["Engineering Management"]

        major = st.selectbox(f"Major (Semester {i + 1}):", majors, key=f"major_{i}")
        passed_credits = st.number_input(f"Passed Credits (Semester {i + 1}):", value=0, min_value=0)
        student_level = st.selectbox(f"Student Level (Semester {i + 1}):", ["Freshman", "Sophomore", "Junior", "Senior"])


        try:
            course_list_df = pd.read_excel("Course_ID.xlsx")
            course_list = course_list_df['Course_ID'].tolist()
        except Exception as e:
            st.error(f"Error loading course list: {e}")

        # Ensure that course IDs are selected
        course_id = st.multiselect(f"Course ID (Semester {i + 1}):", course_list, key=f"course_id_{i}")
        grades_list = ["P"]
        grades = st.multiselect(f"Grade (Semester {i + 1}):", grades_list)
        incoming_pcr = st.number_input(f"Incoming PCR (Semester {i + 1}):", value=0, min_value=0)

        
        if not course_id:
            st.warning("Please select at least one Course ID.")

        student_info = {
            'Student_ID': student_id,
            'Semester': semester,
            'College': college,
            'Passed Credits': passed_credits,
            'Student_Level': student_level,
            'Program': program,
            'Major': major,
            'Course_ID': course_id,
            'GRADE':grades,
            "Incoming_PCR": incoming_pcr
        }
        student_info_list.append(student_info)

    if st.checkbox("Process Manual Input Data"):
        try:
            major_data = pd.read_excel("Updated_MajorSheet_.xlsx", sheet_name=None)
        except Exception as e:
            st.error(f"Error loading Major Sheet: {e}")
            
        valid_input = True
        # Validate input
        for info in student_info_list:
            if info['College'] == "Please Select The Required College!" or info['Program'] == "Please Choose the required program!" or not info['Course_ID']:
                valid_input = False

        if valid_input:
            requirement_df_list = []
            student_progrsss_list = []
            summary_taken_list = []
            remaining_courses_list = []
            latest_eligible_courses_list = []
            comprehensive_eligible_list = []
            recommended_courses_list = []
            summary_eligible_list = []

            # Combine all student info into a single DataFrame
            combined_data = pd.DataFrame(student_info_list)
            combined_data = combined_data.explode("Course_ID")
            combined_data["College"] = combined_data["College"].replace("CEA","COE")
            combined_data["Student_Level"] = combined_data["Student_Level"].replace({"Freshman": 1, "Sophomore": 2, "Junior": 3, "Senior": 4})

            st.success("Manual Data entered successfully!")
            st.table(combined_data)

            for major in combined_data['Major'].unique():
                process_function = major_processing_functions.get(major)
                if process_function:
                    st_hist_data = combined_data[combined_data['Major'] == major]
                    requirements_df,student_progress,summary_area_of_study_taken,remaining_courses_df,latest_eligible_courses,eligible_courses_comprehensive_data,recommended_courses,summary_area_of_study_eligible = process_function(
                            st_hist_data, major_data, "Requierments_Weights.xlsx"
                        )
                    
                    requirement_df_list.append(requirements_df)
                    student_progrsss_list.append(student_progress)
                    summary_taken_list.append(summary_area_of_study_taken)
                    remaining_courses_list.append(remaining_courses_df)
                    latest_eligible_courses_list.append(latest_eligible_courses)
                    comprehensive_eligible_list.append(eligible_courses_comprehensive_data)
                    recommended_courses_list.append(recommended_courses)
                    summary_eligible_list.append(summary_area_of_study_eligible) 

                    requirements_df = pd.concat(requirement_df_list, ignore_index=True)
                    student_progress = pd.concat(student_progrsss_list, ignore_index=True)
                    summary_area_of_study_taken = pd.concat(summary_taken_list, ignore_index=True)
                    remaining_courses_df = pd.concat(remaining_courses_list, ignore_index=True)
                    latest_eligible_courses = pd.concat(latest_eligible_courses_list, ignore_index=True)
                    eligible_courses_comprehensive_data = pd.concat(comprehensive_eligible_list, ignore_index=True)
                    recommended_courses = pd.concat(recommended_courses_list, ignore_index=True)
                    summary_area_of_study_eligible = pd.concat(summary_eligible_list, ignore_index=True)
                else:
                    st.error(f"No processing function found for major: {major}")

            st.success("Data processed successfully!")
            
            section = st.selectbox("Select Data to Display", [
            "None", "Major Sheet Requirements Data","Student Progress Report","Summary of Taken Courses by AREA_OF_STUDY","Remaining Courses by AREA_OF_STUDY","Latest Eligible Courses",
            "Comprehensive Eligible Courses Data", "Recommended Courses Report", "Summary of Eligible Courses by AREA_OF_STUDY"])

            if section == "Major Sheet Requirements Data":
                    st.header("Major Sheet Requirements Data")
                    st.dataframe(requirements_df)

                    # Download the DataFrame as CSV
                    st.header("Download Major Sheet Requirements Data")
                    csv = requirements_df.to_csv(index=False)
                    st.download_button(
                        label="Download data as CSV",
                        data=csv,
                        file_name='requirements_df.csv',
                        mime='text/csv',
                    )
            elif section == "Student Progress Report":
                st.header("Student Progress Report")
                st.dataframe(student_progress)

                # Download the DataFrame as CSV
                st.header("Download Student Progress Report")
                csv = student_progress.to_csv(index=False)
                st.download_button(
                    label="Download data as CSV",
                    data=csv,
                    file_name='student_progress.csv',
                    mime='text/csv',
                )
            elif section == "Summary of Taken Courses by AREA_OF_STUDY":
                st.header("Summary of Taken Courses by AREA_OF_STUDY")
                st.dataframe(summary_area_of_study_taken)

                # Download the DataFrame as CSV
                st.header("Download Recommended Courses Data")
                csv = summary_area_of_study_taken.to_csv(index=False)
                st.download_button(
                    label="Download data as CSV",
                    data=csv,
                    file_name='summary_area_of_study_taken.csv',
                    mime='text/csv',
                )
            elif section == "Remaining Courses by AREA_OF_STUDY":
                st.header("Remaining Courses by AREA_OF_STUDY")
                st.dataframe(remaining_courses_df)

                # Download the DataFrame as CSV
                st.header("Download Remaining Courses by AREA_OF_STUDY")
                csv = remaining_courses_df.to_csv(index=False)
                st.download_button(
                    label="Download data as CSV",
                    data=csv,
                    file_name='remaining_courses_df.csv',
                    mime='text/csv',
                )
            elif section == "Latest Eligible Courses":
                st.header("Latest Eligible Courses Report")
                st.dataframe(latest_eligible_courses)
                csv = latest_eligible_courses.to_csv(index=False)
                st.download_button("Download data as CSV", data=csv, file_name='latest_eligible_courses.csv', mime='text/csv'
                )
            elif section == "Comprehensive Eligible Courses Data":
                st.header("Comprehensive Eligible Courses Data")
                st.dataframe(eligible_courses_comprehensive_data)
                csv = eligible_courses_comprehensive_data.to_csv(index=False)
                st.download_button("Download data as CSV", data=csv, file_name='eligible_courses_comprehensive_data.csv', mime='text/csv'
                )
            elif section == "Recommended Courses Report":   
                st.header("Recommended Courses Report")
                st.dataframe(recommended_courses)
                csv = recommended_courses.to_csv(index=False)
                st.download_button("Download data as CSV", data=csv, file_name='recommended_courses.csv', mime='text/csv'
                )
            elif section == "Summary of Eligible Courses by AREA_OF_STUDY":
                st.header("Summary of Eligible Courses by AREA_OF_STUDY")
                st.dataframe(summary_area_of_study_eligible)
                csv = summary_area_of_study_eligible.to_csv(index=False)
                st.download_button("Download data as CSV", data=csv, file_name='summary_area_of_study_eligible.csv', mime='text/csv')
        else:
            st.error("Please fill in all required fields correctly before processing.")