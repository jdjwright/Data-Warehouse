import pandas as pd
import random
import datetime
import sqlalchemy

engine = sqlalchemy.create_engine("mysql+pymysql://trainee:trainpass@mariadb/warehouse")
random.seed(42)

# Get the current academic year
today = pd.to_datetime("today")
academic_year = pd.read_sql(f"""
    SELECT DISTINCT `Academic Year`
    FROM dim_Dates
    WHERE Date = '{today.date()}'
    LIMIT 1
""", engine).iloc[0]["Academic Year"]

# Load enrolments only for current academic year
enrolments_df = pd.read_sql(f"""
    SELECT 
        e.`Student warehouse BK` AS `Person BK`, 
        e.`Teaching group ID`, 
        e.`Academic year`,
        g.`Subject name`
    FROM `fact student class enrolement` e
    JOIN `dim Teaching Groups` g ON e.`Teaching group ID` = g.pk
    WHERE e.`Academic year` = '{academic_year}'
""", engine)

# Load dates from current academic year
dates_df = pd.read_sql(f"""
    SELECT Date, `Academic Year`, `Term name`
    FROM dim_Dates
    WHERE `Academic Year` = '{academic_year}'
""", engine)
dates_df["Date"] = pd.to_datetime(dates_df["Date"], errors="coerce")
dates_df = dates_df.dropna(subset=["Date"])
term_groups = dates_df.groupby(["Academic Year", "Term name"]).first().reset_index()

# Load grades
grades_df = pd.read_sql("SELECT id, result, `Numerical result`, `Data type`, Category FROM dim_report_grade", engine)
def get_grade_options(data_type, category=None):
    df = grades_df[grades_df["Data type"].str.lower() == data_type.lower()]
    if category:
        df = df[df["Category"].str.lower() == category.lower()]
    return df.reset_index(drop=True)

attainment_grades = get_grade_options("Attainment", "Single grades")
org_grades = get_grade_options("OB")
att_grades = get_grade_options("AB")

# Report generation
report_rows = []
added_target = set()

for _, row in enrolments_df.iterrows():
    person_bk = row["Person BK"]
    tg_id = row["Teaching group ID"]
    subject = row["Subject name"]

    for _, term_row in term_groups.iterrows():
        entry_date = term_row["Date"]
        term = term_row["Term name"]
        teacher_tag = random.choice(["A", "B", "C"])

        for label, grade_pool in [
            ("AB", att_grades),
            ("OB", org_grades),
            ("Attainment", attainment_grades)
        ]:
            grade = grade_pool.sample(1).iloc[0]
            report_rows.append({
                "Student": person_bk,
                "Entry Date": int(entry_date.strftime("%Y%m%d")),
                "Result": grade["result"],
                "Academic year": academic_year,
                "Term": term,
                "Subject": subject,
                "Data type": grade["Data type"],
                "Result pk": grade["id"],
                "Teacher a b or c": teacher_tag,
                "Teaching group pk": tg_id,
                "Numeric result": grade["Numerical result"],
                "SIMS aspect name": None,
                "SIMS result set name": None
            })

        if (person_bk, subject, academic_year) not in added_target:
            grade = attainment_grades.sample(1).iloc[0]
            report_rows.append({
                "Student": person_bk,
                "Entry Date": int(entry_date.strftime("%Y%m%d")),
                "Result": grade["result"],
                "Academic year": academic_year,
                "Term": term,
                "Subject": subject,
                "Data type": "Target grade",
                "Result pk": grade["id"],
                "Teacher a b or c": teacher_tag,
                "Teaching group pk": tg_id,
                "Numeric result": grade["Numerical result"],
                "SIMS aspect name": None,
                "SIMS result set name": None
            })
            added_target.add((person_bk, subject, academic_year))

# Save to DB
df_reports = pd.DataFrame(report_rows)
df_reports.to_sql("fact_report", con=engine, if_exists="append", index=False)
