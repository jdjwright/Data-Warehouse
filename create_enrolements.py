import pandas as pd
import random
import re
from datetime import date
import sqlalchemy

# DB connection
engine = sqlalchemy.create_engine("mysql+pymysql://trainee:trainpass@mariadb/warehouse")
random.seed(42)

# Parameters
min_students = 8
max_students = 24

# Load students
students_df = pd.read_sql("SELECT id, `Year Group`, `Person BK` FROM dim_students_isams", engine)
students_df["Year Group"] = students_df["Year Group"].astype(str).str.strip()

# Load teaching groups
classes_df = pd.read_sql("SELECT pk, `Class code`, `Subject name`, `Academic year` FROM `dim Teaching Groups`", engine)

# Extract year from class code (e.g., 9A/Ph1 → 9)
def extract_year(code):
    match = re.match(r"(\d{1,2})[A-Z]/", code)
    return match.group(1) if match else None

classes_df["Year Group"] = classes_df["Class code"].apply(extract_year)

# Track student enrollment to avoid duplicate subjects
student_subjects = {sid: set() for sid in students_df["id"]}
enrolments = []

# Enroll students
for _, cls in classes_df.iterrows():
    year = cls["Year Group"]
    subject = cls["Subject name"]
    class_pk = cls["pk"]
    academic_year = cls["Academic year"]

    eligible = students_df[
        (students_df["Year Group"] == year) &
        (~students_df["id"].isin(
            [sid for sid, subs in student_subjects.items() if subject in subs]
        ))
    ]

    selected = eligible.sample(min(len(eligible), random.randint(min_students, max_students)))

    for _, student in selected.iterrows():
        student_subjects[student["id"]].add(subject)
        enrolments.append({
            "Student ID": student["id"],
            "Student warehouse BK": student["Person BK"],
            "Teaching group ID": class_pk,
            "Row effective date": date.today(),
            "Row expiry date": None,
            "Academic year": academic_year
        })

# Insert to MariaDB
df_enrolments = pd.DataFrame(enrolments)
df_enrolments.to_sql("fact student class enrolement", con=engine, if_exists="append", index=False)
