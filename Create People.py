import pandas as pd
import random
import datetime
import uuid
import sqlalchemy
from faker import Faker

# Setup
fake = Faker()
random.seed(42)

# Load student BKs and GIS IDs from dim_people
engine = sqlalchemy.create_engine("mysql+pymysql://trainee:trainpass@mariadb/warehouse")
df_people = pd.read_sql("SELECT person_bk, gis_id FROM dim_people WHERE account_type = 'Student'", engine)

# Sample lists
year_groups = ["N", "R", "1", '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13']
tutor_groups = ["A", "B", "C", "D", "E", "F"]
houses = ["Red", "Blue", "Green", "Yellow"]
nationalities = ["British", "Malaysian", "Indian", "Chinese", "Australian", "American"]
ethnicities = ["White", "Asian", "Black", "Mixed", "Other"]
eal_statuses = ["EAL", "Non-EAL"]
sen_statuses = ["None", "SEN Support", "EHCP"]
email_domain = "example.edu"

# Generate a single student record using info from dim_people
def generate_fake_student(row):
    first_name = fake.first_name()
    last_name = fake.last_name()
    preferred_first_name = first_name if random.random() > 0.2 else fake.first_name()

    year_group = random.choice(year_groups)
    age_position = year_groups.index(year_group)
    max_age = 18 - (len(year_groups) - 1 - age_position)
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
        "First Name": first_name,
        "Last name": last_name,
        "Student Email": f"{str(row['gis_id'])}@{email_domain}",
        "Preferred first name": preferred_first_name,
        "FAM email": fake.email(),
        "GIS ID Number": str(row["gis_id"]),
        "Gender": random.choice(["Male", "Female"]),
        "Date of Birth": int(dob.strftime('%Y%m%d')),
        "Parent Salutation": f"Mr. and Mrs. {last_name}",
        "House": random.choice(houses),
        "Year Group": year_group,
        "Tutor Group": year_group + random.choice(tutor_groups),
        "EAL Status": random.choice(eal_statuses),
        "SEN Status": random.choice(sen_statuses),
        "SEN Profile URL": fake.url() if random.random() > 0.8 else None,
        "Exam candidate number": str(random.randint(100000, 999999)),
        "Nationality": random.choice(nationalities),
        "Ethnicity": random.choice(ethnicities),
        "GIS Join Date": int(join_date.strftime('%Y%m%d')),
        "GIS Leave Date": int(leave_date.strftime('%Y%m%d')) if leave_date else None,
        "Row Effective Date": row_effective_date,
        "Row Expiration Date": row_expiration_date,
        "ISAMS PK": str(uuid.uuid4()),
        "On Roll": "Yes" if leave_date is None else "No",
        "UCAS Personal id": random.randint(100000000, 999999999) if random.random() > 0.5 else None,
        "Reason for leaving": fake.sentence(nb_words=6) if leave_date else None,
        "Destination after leaving": fake.job() if leave_date else None,
        "Destination institution": fake.company() if leave_date else None,
        "Graduation academic year": f"{join_date.year + 5}/{(join_date.year + 6)%100:02d}" if leave_date else None,
        "Person BK": row["person_bk"],
    }

# Generate and insert students
students = [generate_fake_student(row) for _, row in df_people.iterrows()]
df_students = pd.DataFrame(students)
df_students.to_sql("dim_students_isams", con=engine, if_exists="append", index=False)
