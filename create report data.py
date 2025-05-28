import pandas as pd
import random
import datetime
import sqlalchemy
import json # Added
import sys # Added

# --- Configuration Loading ---
CONFIG_PATH = 'config.json'
try:
    with open(CONFIG_PATH, 'r') as f:
        config = json.load(f)
    
    db_config = config.get('database_config', {})
    db_host = db_config.get('host', 'mariadb')
    db_user = db_config.get('user', 'trainee')
    db_password = db_config.get('password', 'trainpass')
    db_name = db_config.get('database', 'warehouse')
    db_port = db_config.get('port', 3306)
    DB_URI = f"mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"

    # Load report_data_options
    report_data_opts = config.get('report_data_options', {})
    if not report_data_opts:
        print(f"Warning: 'report_data_options' section not found in {CONFIG_PATH}. Using default report data parameters.")
    
    teacher_tags = report_data_opts.get('teacher_tags', ['A', 'B', 'C'])
    if 'teacher_tags' not in report_data_opts:
        print(f"Warning: 'teacher_tags' not in report_data_options. Using default: {teacher_tags}")


except FileNotFoundError:
    print(f"Error: Configuration file '{CONFIG_PATH}' not found. Exiting.")
    sys.exit(1)
except json.JSONDecodeError:
    print(f"Error: Could not decode JSON from '{CONFIG_PATH}'. Check its format. Exiting.")
    sys.exit(1)
except KeyError as e: # Should not happen with .get and defaults, but good practice
    print(f"Error: Missing essential key '{e}' in configuration file. Exiting.")
    sys.exit(1)
except Exception as e:
    print(f"An unexpected error occurred while loading configuration: {e}. Exiting.")
    sys.exit(1)

engine = sqlalchemy.create_engine(DB_URI) # Use loaded DB_URI
random.seed(42)

# Get the current academic year
today = pd.to_datetime("today")
# Ensure academic_year is fetched correctly and handle potential empty result
academic_year_df = pd.read_sql(f"""
    SELECT DISTINCT `academic_year`
    FROM dim_dates
    WHERE `date` = '{today.date()}'
    LIMIT 1
""", engine)
if academic_year_df.empty:
    print(f"Error: Could not determine academic year for date {today.date()}. Exiting.")
    sys.exit(1)
academic_year = academic_year_df.iloc[0]["academic_year"]


# Load enrolments only for current academic year
enrolments_df = pd.read_sql(f"""
    SELECT 
        e.student_warehouse_bk AS person_bk, 
        e.teaching_group_id, 
        e.academic_year,
        g.subject_name
    FROM fact_student_class_enrolment e
    JOIN dim_teaching_groups g ON e.teaching_group_id = g.pk
    WHERE e.academic_year = '{academic_year}'
""", engine)

# Load dates from current academic year
dates_df = pd.read_sql(f"""
    SELECT `date`, `academic_year`, `term_name`
    FROM dim_dates
    WHERE `academic_year` = '{academic_year}'
""", engine)
dates_df["date"] = pd.to_datetime(dates_df["date"], errors="coerce") # Changed column name
dates_df = dates_df.dropna(subset=["date"]) # Changed column name
term_groups = dates_df.groupby(["academic_year", "term_name"]).first().reset_index() # Changed column names

# Load grades
grades_df = pd.read_sql("SELECT id, result, numerical_result, data_type, category FROM dim_report_grade", engine) # Changed column names
def get_grade_options(data_type_filter, category_filter=None): # Renamed params to avoid conflict
    df = grades_df[grades_df["data_type"].str.lower() == data_type_filter.lower()] # Changed column name
    if category_filter:
        df = df[df["category"].str.lower() == category_filter.lower()] # Changed column name
    return df.reset_index(drop=True)

attainment_grades = get_grade_options("Attainment", "Single grades")
org_grades = get_grade_options("OB") # Assuming "OB" is a data_type
att_grades = get_grade_options("AB") # Assuming "AB" is a data_type

# Report generation
report_rows = []
added_target = set()

for _, row in enrolments_df.iterrows():
    person_bk = row["person_bk"] # Changed column name
    tg_id = row["teaching_group_id"] # Changed column name
    subject = row["subject_name"] # Changed column name

    for _, term_row in term_groups.iterrows():
        entry_date = term_row["date"] # Changed column name
        term = term_row["term_name"] # Changed column name
        teacher_tag = random.choice(teacher_tags) # Use teacher_tags from config

        for label, grade_pool in [
            ("AB", att_grades),
            ("OB", org_grades),
            ("Attainment", attainment_grades)
        ]:
            if grade_pool.empty: # Add check for empty grade_pool
                print(f"Warning: Grade pool for {label} is empty. Skipping.")
                continue
            grade = grade_pool.sample(1).iloc[0]
            report_rows.append({
                "student": person_bk,
                "entry_date": int(entry_date.strftime("%Y%m%d")),
                "result": grade["result"], # This key is from grades_df, already correct
                "academic_year": academic_year,
                "term": term,
                "subject": subject,
                "data_type": grade["data_type"], # This key is from grades_df, already correct
                "result_pk": grade["id"], # This key is from grades_df, already correct
                "teacher_a_b_or_c": teacher_tag,
                "teaching_group_pk": tg_id,
                "numeric_result": grade["numerical_result"], # This key is from grades_df, already correct
                "sims_aspect_name": None,
                "sims_result_set_name": None
            })

        if (person_bk, subject, academic_year) not in added_target:
            if attainment_grades.empty: # Add check for empty attainment_grades
                print(f"Warning: Attainment grade pool is empty. Skipping target grade for {person_bk}, {subject}.")
            else:
                grade = attainment_grades.sample(1).iloc[0]
                report_rows.append({
                    "student": person_bk,
                    "entry_date": int(entry_date.strftime("%Y%m%d")),
                    "result": grade["result"],
                    "academic_year": academic_year,
                    "term": term,
                    "subject": subject,
                    "data_type": "Target grade", # This is a literal value, not from grades_df for this specific target row
                    "result_pk": grade["id"],
                    "teacher_a_b_or_c": teacher_tag,
                    "teaching_group_pk": tg_id,
                    "numeric_result": grade["numerical_result"],
                    "sims_aspect_name": None,
                    "sims_result_set_name": None
                })
                added_target.add((person_bk, subject, academic_year))

# Save to DB
df_reports = pd.DataFrame(report_rows)
df_reports.to_sql("fact_report", con=engine, if_exists="append", index=False)
