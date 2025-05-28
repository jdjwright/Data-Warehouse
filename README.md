# School Data Warehouse Generator

## Introduction

This project generates a dummy school data warehouse populated with synthetic data. It's designed to simulate a realistic school environment, including students, staff, classes, attendance, and assessment data. The warehouse is built on a MySQL-compatible database (MariaDB by default).

## Environment Setup

### Docker-based Setup (Recommended)

For a consistent and isolated environment, it's recommended to use Docker. Please refer to `Enviroment_setup.md` for detailed instructions on setting up the Docker containers for the database and the Python environment.

### Manual Setup (Running Scripts Locally)

If you prefer to run the Python scripts directly on your local machine, you will need Python 3.x and the following libraries:

*   pandas
*   sqlalchemy
*   pymysql
*   Faker

You can install these using pip:
```bash
pip install pandas sqlalchemy pymysql Faker
```
A `requirements.txt` file is also provided for convenience:
```bash
pip install -r requirements.txt
```
Ensure you have a MySQL-compatible database server running and accessible.

## Configuration (`config.json`)

The `config.json` file is crucial for customizing the data generation process and database connections.

### `database_config`
This section contains the connection details for your target database.
*   `host`: The hostname or IP address of your database server (e.g., "mariadb" if using the provided Docker setup, or "localhost").
*   `user`: Database username.
*   `password`: Database password.
*   `database`: The name of the database to use/create.
*   `port`: The port number for the database connection (default for MySQL is 3306).

### `school_parameters`
Defines various parameters related to the school's structure and demographics.
*   `students_to_create`: Number of student records to generate.
*   `staff_to_create`: Number of staff records to generate.
*   `email_domain`: The email domain to use for generated email addresses (e.g., "example.edu").
*   `year_groups`: A list of year group names (e.g., `["N", "R", "1", ..., "13"]`).
*   `tutor_groups_suffix`: Suffixes for tutor groups (e.g., `["A", "B", "C"]`).
*   `houses`: List of school houses (e.g., `["Red", "Blue"]`).
*   `nationalities`: List of nationalities for students/staff.
*   `ethnicities`: List of ethnicities.
*   `eal_statuses`: English as an Additional Language statuses.
*   `sen_statuses`: Special Education Needs statuses.
*   `default_department_list`: A list of department objects used if `dim_departments_csv_path` is not provided or the file is empty/not found. Each object should contain:
    *   `isams_subject_code`: (e.g., "PHY")
    *   `subject_name`: (e.g., "Physics")
    *   `report_name`: (e.g., "Physics")
*   `default_report_grades_list`: A list of report grade objects used if `dim_report_grade_csv_path` is not provided or the file is empty/not found. Each object defines a grade and its properties. Example structure:
    *   `{"result": "A*", "numerical_result": 9.5, "data_type": "Attainment", "category": "Single grades", "ucas_points": null, ...}` (see `config.json` for full example structure).

### `id_generation`
Configures ranges and prefixes for various generated IDs. This helps in ensuring uniqueness and providing a structure to the synthetic IDs.
*   `gis_id_min`, `gis_id_max`: Range for generating GIS IDs.
*   `sims_pk_min`, `sims_pk_max`: Range for generating SIMS primary keys.
*   `isams_school_id_min`, `isams_school_id_max`: Range for generating iSAMS school IDs.
*   `student_user_code_prefix`: Prefix for student user codes (e.g., "stu").
*   `student_user_code_range_max`: Maximum number for generating student user codes (e.g., 9999 will generate codes like "stu0001" to "stu9999").
*   `staff_user_code_prefix`: Prefix for staff user codes (e.g., "stf").
*   `staff_user_code_range_min`, `staff_user_code_range_max`: Range for generating staff user codes.
*   `person_bk_min`, `person_bk_max`: Range for generating `person_bk` values.

### `student_generation_options`
Customizes various aspects of fake student data generation.
*   `preferred_name_difference_probability`: Probability (0.0 to 1.0) that a student will have a preferred name different from their first name.
*   `sen_profile_url_probability`: Probability that a student will have a SEN profile URL.
*   `exam_candidate_number_min`, `exam_candidate_number_max`: Range for generating exam candidate numbers.
*   `ucas_personal_id_probability`: Probability that a student will have a UCAS personal ID.
*   `ucas_personal_id_min`, `ucas_personal_id_max`: Range for generating UCAS personal IDs.

### `staff_generation_options`
Customizes various aspects of fake staff data generation.
*   `staff_code_length_from_surname`: Number of initial letters from the surname to use for the base staff code.
*   `sims_pk_min`, `sims_pk_max`: Range for generating SIMS primary keys specifically for staff records.
*   `row_expiry_probability`: Probability (0.0 to 1.0) that a staff record will have an expiry date.

### `class_generation`
Parameters for generating classes and class structures.
*   `department_custom_distribution`: Allows custom class count distribution per department and year group.
    *   Example: `{"department": "Physics", "numbers": {"9": 3, "10": 2}}` means 3 Physics classes for year 9, 2 for year 10.
*   `default_year_groups_for_classes`: Year groups for which classes will be generated by default if not specified in custom distribution.
*   `default_classes_per_group`: Default number of classes per year group if not specified in custom distribution.
*   `min_students_per_class`: Minimum number of students to enroll in a class.
*   `max_students_per_class`: Maximum number of students to enroll in a class.
*   `teacher_b_probability`: Probability (0.0 to 1.0) of a teaching group having a second teacher (`teacher_b`).
*   `teacher_c_probability`: Probability (0.0 to 1.0) of a teaching group having a third teacher (`teacher_c`).
*   `isams_id_teaching_group_min`, `isams_id_teaching_group_max`: Range for generating iSAMS IDs for teaching groups.

### `attendance_rules`
Defines the structure of the school week and attendance code parameters.
*   `week_structure`: A dictionary defining teaching periods for each day of the week.
    *   Example: `"Monday": ["AM Reg", "P1", "P2", "P3", "PM Reg", "P4"]`
*   `attendance_codes`: A dictionary where each key is an attendance mark (e.g., "/", "N") and the value is an object containing:
    *   `description`: Textual description of the mark (e.g., "Present", "Illness").
    *   `probability`: Probability (0.0 to 1.0) of this mark occurring.
    *   Example: `"/": {"description": "Present", "probability": 0.90}`
*   `time_recorded_hour_min`, `time_recorded_hour_max`: Min and max hour for generating `time_recorded`.
*   `time_recorded_minute_choices`: A list of possible minutes for `time_recorded`.
*   `minutes_late_min`, `minutes_late_max`: Range for generating `minutes_late` if the mark is "L" (Late).
*   `room_prefix`: Prefix for generated room names (e.g., "R").
*   `room_number_min`, `room_number_max`: Range for generating room numbers.
*   `sims_class_pk_min`, `sims_class_pk_max`: Range for generating SIMS class primary keys for attendance records.
*   `sims_subject_pk_min`, `sims_subject_pk_max`: Range for generating SIMS subject primary keys for attendance records.


### `report_data_options`
Parameters related to the generation of student report data.
*   `teacher_tags`: A list of possible teacher tags (e.g., "A", "B", "C") to be randomly assigned to report entries.

### `pivoted_report_card_options`
Options for customizing the generation of the pivoted termly report card.
*   `fill_down_limit_year_groups`: A list of year groups for which the special fill-down logic (with a limit) is applied.
*   `fill_down_limit_value`: The `limit` parameter for the `ffill()` operation in the special fill-down logic for the specified year groups.

### `data_file_paths`
Specifies paths to CSV files for dimension data. This allows you to provide your own specific data for key dimensions.
*   **`dim_dates_csv_path`**:
    *   **Required**: This path *must* point to a CSV file containing your date dimension data. The project relies on this file for academic year structure, holidays, etc.
    *   **Expected Columns**: `id` (int, PK), `date` (YYYY-MM-DD or parsable by pandas), `academic_year` (e.g., "2023/24"), `term_name` (e.g., "Autumn Term"), `term_order_number` (int), `day_of_week_number` (int, Monday=0 or 1), `week_of_year_number` (int), `day_name` (e.g., "Monday"), `month_name` (e.g., "January"), `year_number` (int), `term_and_year` (e.g., "Autumn 2023"), `academic_year_order` (int), `term_order` (int), `week_order_number` (int), `week_of_academic_year_number` (int), `day_of_year_number` (int), `day_of_term_number` (int), `day_of_academic_year_number` (int), `month_number` (int), `day_number` (int), `is_weekend` (e.g., "Weekend", "Weekday"), `week_commencing_date` (YYYY-MM-DD), `holiday_type` (e.g., "Christmas Holiday", "Not a holiday"), `half_termly_report_day` (boolean/int), `first_day_of_academic_year` (YYYY-MM-DD), `last_day_of_academic_year` (YYYY-MM-DD), `first_day_of_half_term` (YYYY-MM-DD), `last_day_of_half_term` (YYYY-MM-DD), `previous_thursday` (YYYY-MM-DD).
    *   A placeholder `data/dim_dates.csv` is created with these headers. You **must** populate this file.
*   **`dim_departments_csv_path`**:
    *   **Optional**: If a path to a CSV file is provided here, the data from this file will be loaded into the `dim_Departments` table.
    *   If the path is empty, `null`, or the file is not found, the script `create_dim_departments.py` will use the `default_department_list` from `school_parameters` in `config.json`.
    *   **Expected Columns**: `isams_subject_code` (e.g., "MAT"), `subject_name` (e.g., "Mathematics"), `report_name` (e.g., "Maths"). Other columns like `isams_id`, `isams_department_id` can be included if available.
    *   A placeholder `data/dim_departments.csv` is created with sample headers.
*   **`dim_report_grade_csv_path`**:
    *   **Optional**: If a path to a CSV file is provided, it will be used to populate the `dim_report_grade` table.
    *   If the path is empty, `null`, or the file is not found, the script `create_dim_report_grade.py` will use the `default_report_grades_list` from `school_parameters` in `config.json`.
    *   **Expected Columns**: `result` (e.g., "A*", "9", "Pass"), `numerical_result` (float), `ucas_points` (int, optional), `gis_points` (int, optional), `data_type` (e.g., "Attainment", "Effort", "Target grade"), `printable_result` (string, optional), `hex_colour` (string, optional), `hex_colour_text` (string, optional), `description` (string, optional), `total_grades` (int, optional), `igcse_pass` (boolean/int, optional), `category` (e.g., "GCSE 9-1", "A-Level", "Generic").
    *   A placeholder `data/dim_report_grade.csv` is created with sample headers.

## Running the Warehouse Creation

1.  **Configure `config.json`**: Before running, ensure `config.json` is correctly set up, especially the `database_config` and `data_file_paths` (specifically `dim_dates_csv_path` which needs to be populated).
2.  **Execute the main script**:
    ```bash
    python run_warehouse_creation.py
    ```
    This script will:
    *   Execute the `warehouse_schema.sql` to set up database tables.
    *   Run a series of Python scripts to generate and load data into the tables.

## Script Overview

The `run_warehouse_creation.py` script executes the following key Python scripts in order:

*   `load_dim_dates.py`: Loads date dimension data from the CSV specified in `config.json`.
*   `create_dim_departments.py`: Creates department dimension data, either from CSV or defaults from `config.json`.
*   `create_dim_report_grade.py`: Creates report grade dimension data, either from CSV or defaults from `config.json`.
*   `CreatePeople.py`: Generates basic records for people (students and staff) including unique IDs, configured by `id_generation`.
*   `create students.py`: Generates detailed student records based on `CreatePeople.py` output and configuration (`school_parameters`, `student_generation_options`).
*   `create staff.py`: Generates detailed staff records, teaching groups, and class enrolments based on configuration (`school_parameters`, `staff_generation_options`, `class_generation`).
*   `create attendance.py`: Generates attendance data for students based on configuration (`attendance_rules`).
*   `create report data.py`: Generates report card data (grades, comments, etc.) for students, configured by `report_data_options`.
*   `Create pivoted report card`: Transforms the report data into a pivoted format suitable for certain types of analysis and reporting, configured by `pivoted_report_card_options`.

## Redundant Scripts

*   **`Create People.py` (with a space in the name)**: This script is redundant. The correct script used by the orchestrator is `CreatePeople.py` (no space). The one with the space can be ignored and may be removed in future updates.

---
*This README provides a general overview. For specific implementation details, refer to the individual scripts and `config.json`.*
