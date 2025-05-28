import pandas as pd
import random
import datetime
import sqlalchemy
from collections import defaultdict
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

    attendance_rules = config.get('attendance_rules', {})
    # Use default week_structure from original script if not in config
    week_structure = attendance_rules.get('week_structure', {
        "Monday": ["AM Reg", "P1", "P2", "P3", "PM Reg", "P4"],
        "Tuesday": ["AM Reg", "P1", "P2", "P3", "PM Reg", "P4"],
        "Wednesday": ["AM Reg", "P1", "P2", "P3"],
        "Thursday": ["AM Reg", "P1", "P2", "P3", "PM Reg", "P4"],
        "Friday": ["P1", "P2", "P3", "P4"]
    })
    # Use default attendance_codes_probabilities from original script if not in config
    attendance_codes_probabilities = attendance_rules.get('attendance_codes_probabilities', {
        "/": 0.90, "N": 0.03, "I": 0.03, "M": 0.01, "L": 0.02, "H": 0.005, "O": 0.005
    })
    if not week_structure or not attendance_codes_probabilities:
        print("Warning: Attendance rules (week_structure or attendance_codes_probabilities) missing or empty in config. Using defaults.")
        # Defaults already set above, so this is just a warning.

except FileNotFoundError:
    print(f"Error: Configuration file '{CONFIG_PATH}' not found. Exiting.")
    sys.exit(1)
except json.JSONDecodeError:
    print(f"Error: Could not decode JSON from '{CONFIG_PATH}'. Check its format. Exiting.")
    sys.exit(1)
except KeyError as e:
    print(f"Error: Missing essential key '{e}' in configuration file. Exiting.")
    sys.exit(1)
except Exception as e:
    print(f"An unexpected error occurred while loading configuration: {e}. Exiting.")
    sys.exit(1)

engine = sqlalchemy.create_engine(DB_URI) # Use loaded DB_URI
random.seed(42)

# Generate attendance_mark_choices from config probabilities
attendance_mark_choices = [
    mark for mark, prob in attendance_codes_probabilities.items() for _ in range(int(prob * 1000))
]

# Hardcoded mapping for mark descriptions as per requirements
mark_to_description = {
    "/": "Present",
    "N": "Absent - no reason",
    "I": "Illness",
    "M": "Medical / Dental",
    "L": "Late",
    "H": "Authorised holiday",
    "O": "Unauthorised holiday",
    "Y": "Authorised Late", # Adding a few more common ones just in case
    "U": "Unauthorised absence (not holiday)",
    "C": "Authorised absence - other",
    "E": "Excluded",
    "S": "Study Leave",
    "P": "Present (AM/PM)",
    "B": "Approved Educational Activity"
}


# Get current academic year
today = pd.to_datetime("today").date()
academic_year_result = pd.read_sql(f"""
    SELECT DISTINCT `academic_year` 
    FROM dim_dates 
    WHERE `date` = '{today}' 
    LIMIT 1
""", engine)
# Ensure academic_year_result is not empty before accessing
academic_year = academic_year_result.iloc[0]["academic_year"] if not academic_year_result.empty else None
if academic_year is None:
    print(f"Error: Could not determine current academic year for date {today}. Exiting.")
    sys.exit(1)


# Load valid school days for current academic year
dates_df = pd.read_sql(f"""
    SELECT id AS date_pk, `date`, `day_name` 
    FROM dim_dates 
    WHERE `academic_year` = '{academic_year}' 
      AND `is_weekend` = 'Weekday' 
      AND (`holiday_type` = 'Not a holiday' OR `holiday_type` = 'Unknown' ) 
""", engine)
dates_df["date"] = pd.to_datetime(dates_df["date"], errors="coerce").dropna() # Changed column name to lowercase 'date'

# Load students and class info for the current year only
students_df = pd.read_sql("SELECT id, `person_bk`, `year_group` FROM dim_students_isams", engine) # Changed column names
classes_df = pd.read_sql("SELECT * FROM `dim_teaching_groups`", engine) # Changed table name
enrolments_df = pd.read_sql(f"""
    SELECT * FROM `fact_student_class_enrolment`  # Changed table name
    WHERE `academic_year` = '{academic_year}'
""", engine)

# Join data
# Assuming column names in classes_df and enrolments_df are now lowercase snake_case from previous script changes
merged = enrolments_df.merge(classes_df, left_on="teaching_group_id", right_on="pk") # Changed left_on
merged = merged.merge(students_df, left_on="student_warehouse_bk", right_on="person_bk") # Changed left_on and right_on

# Prepare output
attendance_records = []
student_timetable = defaultdict(set)

# Use lowercase snake_case for DataFrame column access
for year_group_val in merged["year_group"].unique(): # Changed column name
    year_classes = merged[merged["year_group"] == year_group_val] # Changed column name
    class_ids = year_classes["teaching_group_id"].unique() # Changed column name

    for class_id in class_ids:
        class_info = classes_df[classes_df["pk"] == class_id].iloc[0]
        subject = class_info["subject_name"] # Changed column name
        class_code = class_info["class_code"] # Changed column name
        teacher_bk_val = class_info["teacher"] # Changed column name, avoid conflict

        # Pick 2 random valid time slots from school days
        available_slots = [
            (r["date_pk"], r["date"], r["day_name"], period) # Changed column names
            for _, r in dates_df.iterrows()
            if r["day_name"] in week_structure # Changed column name
            for period in week_structure[r["day_name"]] # Changed column name
        ]
        random.shuffle(available_slots)
        selected_slots = []
        while available_slots and len(selected_slots) < 2:
            selected_slots.append(available_slots.pop())

        if len(selected_slots) < 2:
            continue

        students_in_class = year_classes[year_classes["teaching_group_id"] == class_id] # Changed column name

        for _, student in students_in_class.iterrows():
            sid = student["id"] # This 'id' comes from dim_students_isams
            student_bk_val = student["person_bk"] # Changed column name, avoid conflict

            if any((student_bk_val, day, period) in student_timetable for _, _, day, period in selected_slots):
                continue

            for date_pk_val, date_val_dt, day_val, period_val in selected_slots: # Renamed to avoid conflict
                mark = random.choice(attendance_mark_choices)
                attendance_records.append({
                    "date_pk": date_pk_val, # Changed
                    "period": period_val, # Changed
                    "date_recorded": int(date_val_dt.strftime("%Y%m%d")), # Changed
                    "time_recorded": datetime.time(hour=random.randint(8, 15), minute=random.choice([0, 10, 30, 45])), # Changed
                    "teaching_group_pk": class_id, # Changed
                    "student_pk": sid, # Changed
                    "student_warehouse_bk": student_bk_val, # Changed
                    "recording_teacher_pk": teacher_bk_val, # Changed
                    "recording_teacher_warehouse_bk": teacher_bk_val, # Changed
                    "mark": mark, # Changed
                    "mark_description": mark_to_description.get(mark, "Unknown code"), # Changed
                    "minutes_late": random.randint(1, 10) if mark == "L" else None, # Changed
                    "comment": None, # Changed
                    "class_code": class_code, # Changed
                    "subject": subject, # Changed
                    "room": f"R{random.randint(1, 99)}", # Changed
                    "class_teacher": teacher_bk_val, # Changed
                    "class_teacher_warehouse_bk": teacher_bk_val, # Changed
                    "sims_class_pk": random.randint(10000, 99999), # Changed
                    "sims_subject_pk": random.randint(100, 999) # Changed
                })
                student_timetable[(student_bk_val, day_val, period_val)] = True

# Save to DB
df_attendance = pd.DataFrame(attendance_records)
df_attendance.to_sql("fact Attendance", con=engine, if_exists="append", index=False)
print(f"Inserted {len(df_attendance)} attendance records for academic year {academic_year}.")
