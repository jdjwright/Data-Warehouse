import pandas as pd
import random
import datetime
import uuid
import sqlalchemy
from faker import Faker
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
    year_groups = school_params.get('year_groups', ["N", "R", "1", '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13'])
    tutor_groups_suffix = school_params.get('tutor_groups_suffix', ["A", "B", "C", "D", "E", "F"]) # Renamed from tutor_groups
    houses = school_params.get('houses', ["Red", "Blue", "Green", "Yellow"])
    nationalities = school_params.get('nationalities', ["British", "Malaysian", "Indian", "Chinese", "Australian", "American"])
    ethnicities = school_params.get('ethnicities', ["White", "Asian", "Black", "Mixed", "Other"])
    eal_statuses = school_params.get('eal_statuses', ["EAL", "Non-EAL"])
    sen_statuses = school_params.get('sen_statuses', ["None", "SEN Support", "EHCP"])
    email_domain = school_params.get('email_domain', "example.edu")

    # Load student_generation_options with defaults
    student_gen_opts = config.get('student_generation_options', {})
    if not student_gen_opts:
        print(f"Warning: 'student_generation_options' section not found in {CONFIG_PATH}. Using default student generation parameters.")

    # Specific options with their defaults from original script
    preferred_name_prob = student_gen_opts.get('preferred_name_difference_probability', 0.2)
    sen_profile_url_prob = student_gen_opts.get('sen_profile_url_probability', 0.2) # Original was > 0.8 for None, so < 0.2 for URL
    exam_candidate_min = student_gen_opts.get('exam_candidate_number_min', 100000)
    exam_candidate_max = student_gen_opts.get('exam_candidate_number_max', 999999)
    ucas_id_prob = student_gen_opts.get('ucas_personal_id_probability', 0.5) # Original was > 0.5 for None, so < 0.5 for ID
    ucas_id_min = student_gen_opts.get('ucas_personal_id_min', 100000000)
    ucas_id_max = student_gen_opts.get('ucas_personal_id_max', 999999999)


except FileNotFoundError:
    print(f"Error: Configuration file '{CONFIG_PATH}' not found. Exiting.")
    sys.exit(1)
except json.JSONDecodeError:
    print(f"Error: Could not decode JSON from '{CONFIG_PATH}'. Check its format. Exiting.")
    sys.exit(1)
except Exception as e:
    print(f"An unexpected error occurred while loading configuration: {e}. Exiting.")
    sys.exit(1)

# Setup
fake = Faker()
random.seed(42)

# Load student BKs and GIS IDs from dim_people
engine = sqlalchemy.create_engine(DB_URI) # Use loaded DB_URI
df_people = pd.read_sql("SELECT person_bk, gis_id FROM dim_people WHERE account_type = 'Student'", engine)

# Generate a single student record using info from dim_people
def generate_fake_student(row):
    first_name = fake.first_name()
    last_name = fake.last_name()
    # Use preferred_name_prob from config
    preferred_first_name = first_name if random.random() > preferred_name_prob else fake.first_name()

    year_group = random.choice(year_groups) # Uses config year_groups
    age_position = year_groups.index(year_group) # Uses config year_groups
    max_age = 18 - (len(year_groups) - 1 - age_position) # Uses config year_groups
    min_age = max_age - 1
    dob = fake.date_of_birth(minimum_age=min_age, maximum_age=max_age)

    # Determine realistic join date range
    max_years_back = 15 - (len(year_groups) - 1 - age_position)
    max_join_days_ago = max_years_back * 365
    join_date = datetime.date.today() - datetime.timedelta(days=random.randint(0, max_join_days_ago))
    row_effective_date = join_date
    leave_date = None
    row_expiration_date = leave_date or None

    return {
        "first_name": first_name,
        "last_name": last_name,
        "student_email": f"{str(row['gis_id'])}@{email_domain}",
        "preferred_first_name": preferred_first_name,
        "fam_email": fake.email(),
        "gis_id_number": str(row["gis_id"]),
        "gender": random.choice(["Male", "Female"]),
        "date_of_birth": int(dob.strftime('%Y%m%d')),
        "parent_salutation": f"Mr. and Mrs. {last_name}",
        "house": random.choice(houses), # Uses config houses
        "year_group": year_group,
        "tutor_group": year_group + random.choice(tutor_groups_suffix), # Uses config tutor_groups_suffix
        "eal_status": random.choice(eal_statuses), # Uses config eal_statuses
        "sen_status": random.choice(sen_statuses), # Uses config sen_statuses
        "sen_profile_url": fake.url() if random.random() < sen_profile_url_prob else None, # Use config probability
        "exam_candidate_number": str(random.randint(exam_candidate_min, exam_candidate_max)), # Use config min/max
        "nationality": random.choice(nationalities), # Uses config nationalities
        "ethnicity": random.choice(ethnicities), # Uses config ethnicities
        "gis_join_date": int(join_date.strftime('%Y%m%d')),
        "gis_leave_date": int(leave_date.strftime('%Y%m%d')) if leave_date else None,
        "row_effective_date": row_effective_date,
        "row_expiration_date": row_expiration_date,
        "isams_pk": str(uuid.uuid4()),
        "on_roll": "Yes" if leave_date is None else "No",
        # Use config probability and min/max for ucas_personal_id
        "ucas_personal_id": str(random.randint(ucas_id_min, ucas_id_max)) if random.random() < ucas_id_prob else None,
        "reason_for_leaving": fake.sentence(nb_words=6) if leave_date else None,
        "destination_after_leaving": fake.job() if leave_date else None,
        "destination_institution": fake.company() if leave_date else None,
        "graduation_academic_year": f"{join_date.year + 5}/{(join_date.year + 6)%100:02d}" if leave_date else None,
        "person_bk": row["person_bk"],
    }

# Generate and insert students
students = [generate_fake_student(row) for _, row in df_people.iterrows()]
df_students = pd.DataFrame(students)
df_students.to_sql("dim_students_isams", con=engine, if_exists="append", index=False)
