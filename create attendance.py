import pandas as pd
import random
import datetime
import sqlalchemy
from collections import defaultdict

engine = sqlalchemy.create_engine("mysql+pymysql://trainee:trainpass@mariadb/warehouse")
random.seed(42)

# Week structure (user-defined)
week_structure = {
    "Monday": ["AM Reg", "P1", "P2", "P3", "PM Reg", "P4"],
    "Tuesday": ["AM Reg", "P1", "P2", "P3", "PM Reg", "P4"],
    "Wednesday": ["AM Reg", "P1", "P2", "P3"],
    "Thursday": ["AM Reg", "P1", "P2", "P3", "PM Reg", "P4"],
    "Friday": ["P1", "P2", "P3", "P4"]
}

# Attendance mark codes and probabilities
attendance_codes = {
    "/":  {"description": "Present", "prob": 0.90},
    "N":  {"description": "Absent - no reason", "prob": 0.03},
    "I":  {"description": "Illness", "prob": 0.03},
    "M":  {"description": "Medical / Dental", "prob": 0.01},
    "L":  {"description": "Late", "prob": 0.02},
    "H":  {"description": "Authorised holiday", "prob": 0.005},
    "O":  {"description": "Unauthorised holiday", "prob": 0.005}
}
attendance_mark_choices = [k for k, v in attendance_codes.items() for _ in range(int(v["prob"] * 1000))]

# Get current academic year
today = pd.to_datetime("today").date()
academic_year = pd.read_sql(f"""
    SELECT DISTINCT `Academic Year` 
    FROM dim_Dates 
    WHERE Date = '{today}' 
    LIMIT 1
""", engine).iloc[0]["Academic Year"]

# Load valid school days for current academic year
dates_df = pd.read_sql(f"""
    SELECT id AS `Date pk`, Date, `Day name` 
    FROM dim_Dates 
    WHERE `Academic Year` = '{academic_year}' 
      AND `Is weekend` = 'Weekday' 
      AND (`Holiday type` = 'Not a holiday' OR `Holiday type` = 'Unknown' ) 
""", engine)
dates_df["Date"] = pd.to_datetime(dates_df["Date"], errors="coerce").dropna()

# Load students and class info for the current year only
students_df = pd.read_sql("SELECT id, `Person BK`, `Year Group` FROM dim_students_isams", engine)
classes_df = pd.read_sql("SELECT * FROM `dim Teaching Groups`", engine)
enrolments_df = pd.read_sql(f"""
    SELECT * FROM `fact student class enrolement` 
    WHERE `Academic year` = '{academic_year}'
""", engine)

# Join data
merged = enrolments_df.merge(classes_df, left_on="Teaching group ID", right_on="pk")
merged = merged.merge(students_df, left_on="Student warehouse BK", right_on="Person BK")

# Prepare output
attendance_records = []
student_timetable = defaultdict(set)

for year_group in merged["Year Group"].unique():
    year_classes = merged[merged["Year Group"] == year_group]
    class_ids = year_classes["Teaching group ID"].unique()

    for class_id in class_ids:
        class_info = classes_df[classes_df["pk"] == class_id].iloc[0]
        subject = class_info["Subject name"]
        class_code = class_info["Class code"]
        teacher_bk = class_info["Teacher"]

        # Pick 2 random valid time slots from school days
        available_slots = [
            (r["Date pk"], r["Date"], r["Day name"], period)
            for _, r in dates_df.iterrows()
            if r["Day name"] in week_structure
            for period in week_structure[r["Day name"]]
        ]
        random.shuffle(available_slots)
        selected_slots = []
        while available_slots and len(selected_slots) < 2:
            selected_slots.append(available_slots.pop())

        if len(selected_slots) < 2:
            continue

        students_in_class = year_classes[year_classes["Teaching group ID"] == class_id]

        for _, student in students_in_class.iterrows():
            sid = student["id"]
            student_bk = student["Person BK"]

            if any((student_bk, day, period) in student_timetable for _, _, day, period in selected_slots):
                continue

            for date_pk, date_val, day, period in selected_slots:
                mark = random.choice(attendance_mark_choices)
                attendance_records.append({
                    "Date pk": date_pk,
                    "Period": period,
                    "Date recorded": int(date_val.strftime("%Y%m%d")),
                    "Time recorded": datetime.time(hour=random.randint(8, 15), minute=random.choice([0, 10, 30, 45])),
                    "Teaching Group pk": class_id,
                    "Student pk": sid,
                    "Student warehouse bk": student_bk,
                    "Recording teacher pk": teacher_bk,
                    "Recording teacher warehouse bk": teacher_bk,
                    "Mark": mark,
                    "Mark description": attendance_codes[mark]["description"],
                    "Minutes late": random.randint(1, 10) if mark == "L" else None,
                    "Comment": None,
                    "Class code": class_code,
                    "Subject": subject,
                    "Room": f"R{random.randint(1, 99)}",
                    "Class teacher": teacher_bk,
                    "Class teacher warehouse BK": teacher_bk,
                    "SIMS class pk": random.randint(10000, 99999),
                    "SIMS subject PK": random.randint(100, 999)
                })
                student_timetable[(student_bk, day, period)] = True

# Save to DB
df_attendance = pd.DataFrame(attendance_records)
df_attendance.to_sql("fact Attendance", con=engine, if_exists="append", index=False)
print(f"Inserted {len(df_attendance)} attendance records for academic year {academic_year}.")
