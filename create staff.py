import pandas as pd
import random
import sqlalchemy
from faker import Faker
import re
from datetime import date
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

    school_params = config.get('school_parameters', {})
    email_domain = school_params.get('email_domain', "example.edu") # Default from original script

    class_gen_config = config.get('class_generation', {})
    # Default custom_distribution to empty list if not found, script handles this
    department_custom_distribution = class_gen_config.get('department_custom_distribution', []) 
    # Default year groups from original script
    default_year_groups_for_classes = class_gen_config.get('default_year_groups_for_classes', ['3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13'])
    # Default classes per group from original script
    default_classes_per_group = class_gen_config.get('default_classes_per_group', 3)
    # Default min/max students from original script
    min_students_per_class = class_gen_config.get('min_students_per_class', 8)
    max_students_per_class = class_gen_config.get('max_students_per_class', 24)
    
    # New class_generation parameters
    teacher_b_prob = class_gen_config.get('teacher_b_probability', 0.3)
    teacher_c_prob = class_gen_config.get('teacher_c_probability', 0.05)
    tg_isams_id_min = class_gen_config.get('isams_id_teaching_group_min', 10000)
    tg_isams_id_max = class_gen_config.get('isams_id_teaching_group_max', 99999)
    if 'teacher_b_probability' not in class_gen_config : print(f"Warning: 'teacher_b_probability' not in class_generation. Using default {teacher_b_prob}.")
    if 'teacher_c_probability' not in class_gen_config : print(f"Warning: 'teacher_c_probability' not in class_generation. Using default {teacher_c_prob}.")
    if 'isams_id_teaching_group_min' not in class_gen_config : print(f"Warning: 'isams_id_teaching_group_min' not in class_generation. Using default {tg_isams_id_min}.")
    if 'isams_id_teaching_group_max' not in class_gen_config : print(f"Warning: 'isams_id_teaching_group_max' not in class_generation. Using default {tg_isams_id_max}.")

    # Load staff_generation_options with defaults
    staff_gen_opts = config.get('staff_generation_options', {})
    if not staff_gen_opts:
        print(f"Warning: 'staff_generation_options' section not found in {CONFIG_PATH}. Using default staff generation parameters.")

    staff_code_len = staff_gen_opts.get('staff_code_length_from_surname', 3)
    staff_sims_pk_min = staff_gen_opts.get('sims_pk_min', 10000)
    staff_sims_pk_max = staff_gen_opts.get('sims_pk_max', 99999)
    staff_row_expiry_prob = staff_gen_opts.get('row_expiry_probability', 0.1) # Probability of having an expiry date
    if 'staff_code_length_from_surname' not in staff_gen_opts : print(f"Warning: 'staff_code_length_from_surname' not in staff_generation_options. Using default {staff_code_len}.")
    if 'sims_pk_min' not in staff_gen_opts : print(f"Warning: 'sims_pk_min' not in staff_generation_options. Using default {staff_sims_pk_min}.")
    if 'sims_pk_max' not in staff_gen_opts : print(f"Warning: 'sims_pk_max' not in staff_generation_options. Using default {staff_sims_pk_max}.")
    if 'row_expiry_probability' not in staff_gen_opts : print(f"Warning: 'row_expiry_probability' not in staff_generation_options. Using default {staff_row_expiry_prob}.")


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

effective_date = date.today()
effective_date_str = effective_date.strftime("%Y-%m-%d")

# Database connection
engine = sqlalchemy.create_engine(DB_URI) # Use loaded DB_URI
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
    while f"{email}@{email_domain}" in used_emails: # Use config email_domain
        email = f"{base_email}{suffix}"
        suffix += 1
    full_email = f"{email}@{email_domain}" # Use config email_domain
    used_emails.add(full_email)

    # === Staff code generation using config ===
    base_code = last_name[:staff_code_len].upper()
    code = base_code
    suffix = 1
    while code in used_staff_codes:
        code = f"{base_code}{suffix}" # Max length of code can exceed 4 if suffix is large
        suffix += 1
        if len(code) > 4 and staff_code_len > 1 : # If code gets too long, shorten base part
             # Ensure base_code_shortened is not empty if staff_code_len was 1
            base_code_shortened_len = max(1, staff_code_len -1)
            base_code_shortened = last_name[:base_code_shortened_len].upper()
            code = f"{base_code_shortened}{suffix}"
        elif len(code) > 4 and staff_code_len ==1:
            code = f"{last_name[0].upper()}{suffix}"


    used_staff_codes.add(code[:4]) # Ensure final code is max 4 chars

    staff_rows.append({
        "warehouse_pk": row["Warehouse PK"], 
        "sims_pk": random.randint(staff_sims_pk_min, staff_sims_pk_max), # Use config sims_pk
        "title": random.choice(["Mr", "Ms", "Mrs", "Dr"]), 
        "first_name": first_name, 
        "last_name": last_name, 
        "staff_code": code[:4],  # Use generated code, truncated to 4 chars
        "full_name": f"{first_name} {last_name}", 
        "email_address": full_email, 
        "row_effective_date": fake.date_between(start_date='-10y', end_date='-1y'), 
        "row_expiry_date": fake.date_between(start_date='today', end_date='+1y') if random.random() < staff_row_expiry_prob else None, # Use config probability
        "fam_email_address": fake.email() 
    })
    print(f"Added staff: {full_email} / code: {code[:4]}")

print("Finished adding all staff")
# Insert into MariaDB
df_staff = pd.DataFrame(staff_rows)
print("Created dataframe")
df_staff.to_sql("dim_staff", con=engine, if_exists="append", index=False) # Changed table name



# Load departments
departments_df = pd.read_sql("SELECT * FROM `dim_departments`", engine) # Changed table name

# Load academic year from dim_Dates for the effective date
academic_year_row = pd.read_sql(f"""
    SELECT DISTINCT `academic_year` FROM dim_dates # Changed column and table names
    WHERE `date` = '{effective_date_str}' # Changed column name
    LIMIT 1;
""", engine)
academic_year = academic_year_row.iloc[0]["academic_year"] # Changed column name

# Load staff PKs to assign teachers
teachers_df = pd.read_sql("SELECT `warehouse_pk` FROM `dim_staff`", engine) # Changed column and table names
teacher_pks = teachers_df["warehouse_pk"].tolist() # Changed column name

# Use configured custom distribution and defaults
custom_map = {d['department'].lower(): d['numbers'] for d in department_custom_distribution} # Use config department_custom_distribution
# default_year_groups is now default_year_groups_for_classes from config
# default_classes_per_group is from config

# Generate classes
teaching_groups = []
used_codes = set()

for _, dept in departments_df.iterrows():
    subject_name = dept["subject_name"] # Changed
    isams_subject_code = dept["isams_subject_code"] # Changed
    dept_name = subject_name.lower()
    # Use default_year_groups_for_classes and default_classes_per_group from config
    class_numbers = custom_map.get(dept_name, {yr: default_classes_per_group for yr in default_year_groups_for_classes})

    for year in default_year_groups_for_classes: # Use config default_year_groups_for_classes
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
            # Use config probabilities for teacher_b and teacher_c
            teacher_b = random.choice(teacher_pks) if random.random() < teacher_b_prob else None
            teacher_c = random.choice(teacher_pks) if random.random() < teacher_c_prob else None

            teaching_groups.append({
                "sims_pk": random.randint(100000, 999999), 
                "academic_year": academic_year, 
                "teacher": teacher, 
                "teacher_b": teacher_b, 
                "teacher_c": teacher_c, 
                "teacher_name": None, 
                "teacher_b_name": None, 
                "teacher_c_name": None, 
                "class_code": class_code, 
                "code_and_year": code_and_year, 
                "row_effective_date": effective_date, 
                "row_expiry_date": None, 
                "current_group": "Yes", 
                "subject_name": subject_name, 
                "teacher_a_b_or_c": None, 
                "isams_id": random.randint(tg_isams_id_min, tg_isams_id_max) # Use config isams_id for TGs
            })

# Save to MariaDB
df_classes = pd.DataFrame(teaching_groups)
df_classes.to_sql("dim_teaching_groups", con=engine, if_exists="append", index=False) # Changed table name

# Parameters from config
# min_students is min_students_per_class from config
# max_students is max_students_per_class from config

# Load students
students_df = pd.read_sql("SELECT id, `year_group`, `person_bk` FROM dim_students_isams", engine) # Changed column names
students_df["year_group"] = students_df["year_group"].astype(str).str.strip() # Changed column name

# Load teaching groups
classes_df = pd.read_sql("SELECT pk, `class_code`, `subject_name`, `academic_year` FROM `dim_teaching_groups`", engine) # Changed column names & table name

# Extract year from class code (e.g., 9A/Ph1 → 9)
def extract_year(code):
    match = re.match(r"(\d{1,2})[A-Z]/", code) # This logic assumes class_code format
    return match.group(1) if match else None

classes_df["year_group"] = classes_df["class_code"].apply(extract_year) # Changed column name

# Track student enrollment to avoid duplicate subjects
student_subjects = {sid: set() for sid in students_df["id"]} # Assumes 'id' is the PK from dim_students_isams
enrolments = []

# Enroll students
for _, cls in classes_df.iterrows():
    year = cls["year_group"] # Changed
    subject = cls["subject_name"] # Changed
    class_pk = cls["pk"] # Assumes 'pk' is the PK from dim_teaching_groups
    academic_year_val = cls["academic_year"] # Changed, avoid conflict with outer scope 'academic_year' variable

    eligible = students_df[
        (students_df["year_group"] == year) & # Changed
        (~students_df["id"].isin( # Assumes 'id' is the PK from dim_students_isams
            [sid for sid, subs in student_subjects.items() if subject in subs]
        ))
    ]

    # Use min_students_per_class and max_students_per_class from config
    selected = eligible.sample(min(len(eligible), random.randint(min_students_per_class, max_students_per_class)))

    for _, student in selected.iterrows():
        student_subjects[student["id"]].add(subject) # Assumes 'id' is the PK from dim_students_isams
        enrolments.append({
            "student_id": student["id"], # Changed, Assumes 'id' is the PK from dim_students_isams
            "student_warehouse_bk": student["person_bk"], # Changed
            "teaching_group_id": class_pk, # Changed
            "row_effective_date": date.today(), # Changed
            "row_expiry_date": None, # Changed
            "academic_year": academic_year_val # Changed
        })

# Insert to MariaDB
df_enrolments = pd.DataFrame(enrolments)
df_enrolments.to_sql("fact_student_class_enrolment", con=engine, if_exists="append", index=False) # Changed table name
