import pandas as pd
import random
import datetime
from faker import Faker
from sqlalchemy import create_engine

# Connect to MariaDB
engine = create_engine("mysql+pymysql://trainee:trainpass@mariadb/warehouse")
fake = Faker()
random.seed(42)

# Parameters
incident_distribution = {"mean": 5, "std": 4, "min": 0, "max": 50}
incident_categories = {
    "Negative: Homework": 0.5,
    "Negative: Disruption": 0.1,
    "Negative: Uniform": 0.1,
    "Positive: Participation": 0.2,
    "Positive: Homework": 0.1,
}
resolution_statuses = ["Resolved", "Pending", "Escalated"]
locations = ["Classroom", "Corridor", "Library", "Playground", "Online"]
times = ["AM", "PM", "Lunch", "After School"]
departments = ["Math", "Science", "English", "Humanities", "PE", "Languages"]

# Load necessary tables
students = pd.read_sql("SELECT `Person BK`, `GIS Join Date`, `GIS Leave Date` FROM dim_students_isams", engine)
students["GIS Join Date"] = pd.to_datetime(students["GIS Join Date"].astype(str).str[:8], format="%Y%m%d", errors="coerce")
students["GIS Leave Date"] = pd.to_datetime(students["GIS Leave Date"].astype(str).str[:8], format="%Y%m%d", errors="coerce")

teachers = pd.read_sql("SELECT `Person BK` FROM dim_People WHERE `Account type` = 'Staff'", engine)["Person BK"].tolist()

dates_df = pd.read_sql("SELECT id, Date FROM dim_Dates WHERE `Holiday type` = 'Not a holiday' AND `Is weekend` = 'Weekday'", engine)
dates_df["Date"] = pd.to_datetime(dates_df["Date"])
dates_df = dates_df.sort_values("Date")

# Build behavior records
behaviour_rows = []

for _, student in students.iterrows():
    person_bk = student["Person BK"]
    join_date = student["GIS Join Date"]
    leave_date = student["GIS Leave Date"] if pd.notnull(student["GIS Leave Date"]) else datetime.datetime.today()
    eligible_dates = dates_df[(dates_df["Date"] >= join_date) & (dates_df["Date"] <= leave_date)]

    if eligible_dates.empty:
        continue

    # Generate incident count from normal distribution
    n_incidents = int(min(max(random.gauss(incident_distribution["mean"], incident_distribution["std"]), incident_distribution["min"]), incident_distribution["max"]))

    for _ in range(n_incidents):
        date_row = eligible_dates.sample(1).iloc[0]
        date_id = int(date_row["id"])
        date_recorded = date_id
        incident_date = date_id

        category = random.choices(list(incident_categories.keys()), weights=list(incident_categories.values()), k=1)[0]
        status = random.choice(resolution_statuses)
        teacher = random.choice(teachers)

        behaviour_rows.append({
            "Student": person_bk,
            "Date recorded": date_recorded,
            "Recording teacher": teacher,
            "Recording teacher Warehouse BK": teacher,
            "Incident type": category.split(":")[0].strip(),
            "Incident subtype": category.split(":")[1].strip(),
            "Department": random.choice(departments),
            "Comments": fake.sentence(nb_words=10),
            "Incident date": incident_date,
            "Resolution status": status,
            "Date resolved": incident_date if status == "Resolved" else None,
            "Action taken": fake.sentence(nb_words=6),
            "Action taken by": teacher,
            "Location": random.choice(locations),
            "Time": random.choice(times),
            "Points": -1 if "Negative" in category else 1,
            "iSAMS ID": fake.uuid4(),
            "Action Location": random.choice(locations),
            "Action time": random.choice(["08:00", "12:15", "15:30"]),
            "Action date": incident_date
        })

# Save to database
df_behaviour = pd.DataFrame(behaviour_rows)
df_behaviour.to_sql("fact Behaviour", con=engine, if_exists="append", index=False)

print(f"Inserted {len(df_behaviour)} behaviour records.")
