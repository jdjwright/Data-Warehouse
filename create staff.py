import pandas as pd
import random
import sqlalchemy
from faker import Faker
import re
from datetime import date

effective_date = date.today()
effective_date_str = effective_date.strftime("%Y-%m-%d")

# Database connection
engine = sqlalchemy.create_engine("mysql+pymysql://trainee:trainpass@mariadb/warehouse")
fake = Faker()
random.seed(42)

# Load staff warehouse PKs
people_df = pd.read_sql(
    "SELECT `person_bk` AS `Warehouse PK` FROM dim_people WHERE account_type = 'Staff'", engine
)

# Track uniqueness
used_emails = set()
used_staff_codes = set()

staff_rows = []

for _, row in people_df.iterrows():
    first_name = fake.first_name()
    last_name = fake.last_name()
    first_initial = first_name[0].lower()

    # === Email (surname.first_initial) ===
    base_email = f"{last_name.lower()}.{first_initial}"
    email = base_email
    suffix = 1
    while f"{email}@example.edu" in used_emails:
        email = f"{base_email}{suffix}"
        suffix += 1
    full_email = f"{email}@example.edu"
    used_emails.add(full_email)

    # === Staff code (first 3 letters of surname, uppercase) ===
    base_code = last_name[:3].upper()
    code = base_code
    suffix = 1
    while code in used_staff_codes:
        code = f"{base_code}{suffix}"
        suffix += 1
        if len(code) > 4:
            code = f"{base_code[:2]}{suffix}"
    used_staff_codes.add(code)

    staff_rows.append({
        "Warehouse PK": row["Warehouse PK"],
        "SIMS pk": random.randint(10000, 99999),
        "Title": random.choice(["Mr", "Ms", "Mrs", "Dr"]),
        "First name": first_name,
        "Last name": last_name,
        "Staff code": code[:4],  # truncate to fit 4-char limit
        "Full name": f"{first_name} {last_name}",
        "Email address": full_email,
        "Row effective date": fake.date_between(start_date='-10y', end_date='-1y'),
        "Row expiry date": None if random.random() > 0.1 else fake.date_between(start_date='today', end_date='+1y'),
        "FAM email address": fake.email()
    })
    print(f"Added staff: {full_email} / code: {code}")

print("Finished adding all staff")
# Insert into MariaDB
df_staff = pd.DataFrame(staff_rows)
print("Created dataframe")
df_staff.to_sql("dim Staff", con=engine, if_exists="append", index=False)



# Load departments
departments_df = pd.read_sql("SELECT * FROM `dim Departments`", engine)

# Load academic year from dim_Dates for the effective date
academic_year_row = pd.read_sql(f"""
    SELECT DISTINCT `Academic Year` FROM dim_Dates
    WHERE Date = '{effective_date_str}'
    LIMIT 1;
""", engine)
academic_year = academic_year_row.iloc[0]["Academic Year"]

# Load staff PKs to assign teachers
teachers_df = pd.read_sql("SELECT `Warehouse PK` FROM `dim Staff`", engine)
teacher_pks = teachers_df["Warehouse PK"].tolist()

# Optional: custom distribution
custom_distribution = [
    {'department': 'Physics', 'numbers': {'9': 6, '10': 5, '11': 5, '12': 3, '13': 3}},
    {'department': 'English', 'numbers': {'9': 8, '10': 7, '11': 7, '12': 5, '13': 4}},
]
custom_map = {d['department'].lower(): d['numbers'] for d in custom_distribution}
default_year_groups = ['3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13']
default_classes_per_group = 3

# Generate classes
teaching_groups = []
used_codes = set()

for _, dept in departments_df.iterrows():
    subject_name = dept["Subject name"]
    isams_subject_code = dept["iSAMS subject code"]
    dept_name = subject_name.lower()
    class_numbers = custom_map.get(dept_name, {yr: default_classes_per_group for yr in default_year_groups})

    for year in default_year_groups:
        num_classes = class_numbers.get(year)
        if not num_classes:
            continue

        for _ in range(num_classes):
            # Unique class code generation
            for _ in range(100):
                letter = random.choice(['A', 'B', 'C', 'D'])
                suffix = random.randint(1, 9)
                class_code = f"{year}{letter}/{isams_subject_code}{suffix}"
                code_and_year = f"{class_code} {academic_year}"
                if code_and_year not in used_codes:
                    used_codes.add(code_and_year)
                    break

            teacher = random.choice(teacher_pks)
            teacher_b = random.choice(teacher_pks) if random.random() < 0.3 else None
            teacher_c = random.choice(teacher_pks) if random.random() < 0.05 else None

            teaching_groups.append({
                "SIMS PK": random.randint(100000, 999999),
                "Academic year": academic_year,
                "Teacher": teacher,
                "Teacher B": teacher_b,
                "Teacher C": teacher_c,
                "Teacher Name": None,
                "Teacher B Name": None,
                "Teacher C Name": None,
                "Class code": class_code,
                "Code and year": code_and_year,
                "Row effective date": effective_date,
                "Row expiry date": None,
                "Current group": "Yes",
                "Subject name": subject_name,
                "Teacher a b or c": None,
                "iSAMS id": random.randint(10000, 99999)
            })

# Save to MariaDB
df_classes = pd.DataFrame(teaching_groups)
df_classes.to_sql("dim Teaching Groups", con=engine, if_exists="append", index=False)

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
