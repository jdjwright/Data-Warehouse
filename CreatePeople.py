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
    students_to_create = school_params.get('students_to_create', 1500) # Default if not in config
    staff_to_create = school_params.get('staff_to_create', 150)     # Default if not in config

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

# Pre-generate globally unique IDs
gis_ids = random.sample(range(1_000_000, 9_999_999), total)
sims_pks = random.sample(range(2_000_000, 9_999_999), total)
isams_school_ids = random.sample(range(3_000_000, 9_999_999), total)
# Format and sample from guaranteed-unique pools
isams_user_codes_students = random.sample([f"stu{str(i).zfill(4)}" for i in range(10000)], students_to_create)
isams_user_codes_staff = random.sample([f"stf{str(i).zfill(4)}" for i in range(10000, 99999)], staff_to_create)


# Generate people from pre-allocated pools
def generate_people(n, ids, user_codes, type="Student"):
    people = []
    for i in range(n):
        person = {
            "gis_id": ids["gis"][i],
            "sims_pk": ids["sims"][i],
            "isams_school_id": str(ids["isams"][i]),
            "isams_user_code": user_codes[i],
            "account_type": type,
            "person_bk": random.randint(1000, 9999),
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
