from faker import Faker
import random
import pandas as pd
import sqlalchemy

# Configuration
students_to_create = 3000
staff_to_create = 500
total = students_to_create + staff_to_create

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

# Generate people using correct column names
def generate_people(n, ids, user_codes, type="Student"):
    people = []
    for i in range(n):
        person = {
            "GIS ID": ids["gis"][i],
            "SIMS PK": ids["sims"][i],
            "iSAMS School ID": str(ids["isams"][i]),
            "iSAMS User Code": user_codes[i],
            "Account type": type,
            "Person BK": random.randint(1000, 9999),
            "Google classroom student_id": fake.lexify(text="gclass???????") if type == "Student" else None
        }
        people.append(person)
    return people

# Save using correct column casing
def save_people_to_mariadb(people, db_uri="mysql+pymysql://trainee:trainpass@mariadb/warehouse"):
    df = pd.DataFrame(people)
    engine = sqlalchemy.create_engine(db_uri)
    df.to_sql("dim_People", con=engine, if_exists="append", index=False)
    return df["Person BK"].tolist()

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

# Run everything
if __name__ == "__main__":
    students = generate_people(students_to_create, student_ids, isams_user_codes_students, "Student")
    student_bks = save_people_to_mariadb(students)
    print(f"✅ Saved {len(students)} students. Sample BKs: {student_bks[:5]}")

    staff = generate_people(staff_to_create, staff_ids, isams_user_codes_staff, "Staff")
    staff_bks = save_people_to_mariadb(staff)
    print(f"✅ Saved {len(staff)} staff. Sample BKs: {staff_bks[:5]}")
