import pandas as pd
import random
import string
from datetime import date
import sqlalchemy

# Setup
engine = sqlalchemy.create_engine("mysql+pymysql://trainee:trainpass@mariadb/warehouse")
random.seed(42)

# Config
effective_date = date.today()
effective_date_str = effective_date.strftime("%Y-%m-%d")

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
default_year_groups = ['9', '10', '11', '12', '13']
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
