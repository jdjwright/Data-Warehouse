from faker import Faker
import random
import pandas as pd
import sqlalchemy
import json
import sys

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

    school_params = config.get('school_parameters', {})
    students_to_create = school_params.get('students_to_create', 1500) 
    staff_to_create = school_params.get('staff_to_create', 150)     

    # Load id_generation parameters with defaults from original script
    id_gen_config = config.get('id_generation', {})
    if not id_gen_config:
        print(f"Warning: 'id_generation' section not found in {CONFIG_PATH}. Using default ID generation parameters.")

    gis_id_min = id_gen_config.get('gis_id_min', 1_000_000)
    gis_id_max = id_gen_config.get('gis_id_max', 9_999_999)
    sims_pk_min = id_gen_config.get('sims_pk_min', 2_000_000)
    sims_pk_max = id_gen_config.get('sims_pk_max', 9_999_999)
    isams_school_id_min = id_gen_config.get('isams_school_id_min', 3_000_000)
    isams_school_id_max = id_gen_config.get('isams_school_id_max', 9_999_999)
    
    student_user_code_prefix = id_gen_config.get('student_user_code_prefix', "stu")
    student_user_code_range_max = id_gen_config.get('student_user_code_range_max', 10000)
    
    staff_user_code_prefix = id_gen_config.get('staff_user_code_prefix', "stf")
    staff_user_code_range_min = id_gen_config.get('staff_user_code_range_min', 10000)
    staff_user_code_range_max = id_gen_config.get('staff_user_code_range_max', 99999)

    person_bk_min_global = id_gen_config.get('person_bk_min', 1000) # Store globally for generate_people
    person_bk_max_global = id_gen_config.get('person_bk_max', 9999) # Store globally for generate_people

    # Warnings for missing specific keys (optional, as defaults are handled by .get)
    # Example: if 'gis_id_min' not in id_gen_config: print(f"Warning: 'gis_id_min' not in id_generation. Using default.")


except FileNotFoundError:
    print(f"Error: Configuration file '{CONFIG_PATH}' not found. Exiting.")
    sys.exit(1)
except json.JSONDecodeError:
    print(f"Error: Could not decode JSON from '{CONFIG_PATH}'. Check its format. Exiting.")
    sys.exit(1)
except Exception as e:
    print(f"An unexpected error occurred while loading configuration: {e}. Exiting.")
    sys.exit(1)

total = students_to_create + staff_to_create
DB_URI = f"mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"

fake = Faker()
Faker.seed(42)
random.seed(42)

# Pre-generate globally unique IDs using parameters from config (with defaults)
gis_ids = random.sample(range(gis_id_min, gis_id_max), total)
sims_pks = random.sample(range(sims_pk_min, sims_pk_max), total)
isams_school_ids = random.sample(range(isams_school_id_min, isams_school_id_max), total)

# Format and sample from guaranteed-unique pools using parameters from config
isams_user_codes_students = random.sample(
    [f"{student_user_code_prefix}{str(i).zfill(4)}" for i in range(student_user_code_range_max)], 
    students_to_create
)
isams_user_codes_staff = random.sample(
    [f"{staff_user_code_prefix}{str(i).zfill(4)}" for i in range(staff_user_code_range_min, staff_user_code_range_max)], 
    staff_to_create
)

# Generate people from pre-allocated pools
# Pass person_bk_min and person_bk_max to the function if they are needed there,
# or use the global variables directly as they are now defined.
def generate_people(n, ids, user_codes, type="Student"):
    people = []
    for i in range(n):
        person = {
            "gis_id": ids["gis"][i],
            "sims_pk": ids["sims"][i],
            "isams_school_id": str(ids["isams"][i]),
            "isams_user_code": user_codes[i],
            "account_type": type,
            "person_bk": random.randint(person_bk_min_global, person_bk_max_global), # Use global config values
            "google_classroom_student_id": fake.lexify(text="gclass???????") if type == "Student" else None
        }
        people.append(person)
    return people

# Save to MariaDB
def save_people_to_mariadb(people, db_uri=DB_URI): # Use loaded DB_URI
    df = pd.DataFrame(people)
    engine = sqlalchemy.create_engine(db_uri)
    df.to_sql("dim_people", con=engine, if_exists="append", index=False)
    return df["person_bk"].tolist()

# Prepare ID pools
student_ids = {
    "gis": gis_ids[:students_to_create],
    "sims": sims_pks[:students_to_create],
    "isams": isams_school_ids[:students_to_create]
}
staff_ids = {
    "gis": gis_ids[students_to_create:],
    "sims": sims_pks[students_to_create:],
    "isams": isams_school_ids[students_to_create:]
}

# Run the full pipeline
if __name__ == "__main__":
    students = generate_people(students_to_create, student_ids, isams_user_codes_students, "Student")
    student_bks = save_people_to_mariadb(students)
    print(f"✅ Saved {len(students)} students. Sample BKs: {student_bks[:5]}")

    staff = generate_people(staff_to_create, staff_ids, isams_user_codes_staff, "Staff")
    staff_bks = save_people_to_mariadb(staff)
    print(f"✅ Saved {len(staff)} staff. Sample BKs: {staff_bks[:5]}")
