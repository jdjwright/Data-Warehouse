
-- Warehouse schema creation for MariaDB

CREATE TABLE dim_departments (
    id INT AUTO_INCREMENT PRIMARY KEY,
    isams_subject_code VARCHAR(5) NOT NULL,
    subject_name VARCHAR(50) NOT NULL,
    report_name VARCHAR(50) NOT NULL,
    isams_id INT,
    isams_department_id INT
);

CREATE TABLE dim_pastoral_structure (
    tutor_group VARCHAR(6) NOT NULL,
    year_group INT NOT NULL,
    tutor_bk INT,
    head_of_year_bk INT,
    aht_bk INT,
    tutor_name VARCHAR(50),
    head_of_year_name VARCHAR(50),
    aht_name VARCHAR(50),
    tutor_email VARCHAR(50),
    head_of_year_email VARCHAR(50),
    aht_email VARCHAR(50),
    row_valid_from DATETIME,
    row_expiry_date DATETIME
);

CREATE TABLE dim_staff (
    pk INT AUTO_INCREMENT PRIMARY KEY,
    warehouse_pk INT,
    sims_pk INT,
    title VARCHAR(100),
    first_name VARCHAR(60),
    last_name VARCHAR(60),
    staff_code VARCHAR(4),
    full_name VARCHAR(121),
    email_address VARCHAR(60),
    row_effective_date DATE NOT NULL,
    row_expiry_date DATE,
    fam_email_address VARCHAR(60)
);

CREATE TABLE dim_teaching_groups (
    pk INT AUTO_INCREMENT PRIMARY KEY,
    sims_pk INT,
    academic_year VARCHAR(50),
    teacher INT,
    teacher_b INT,
    teacher_c INT,
    teacher_name VARCHAR(50),
    teacher_b_name VARCHAR(50),
    teacher_c_name VARCHAR(50),
    class_code VARCHAR(50),
    code_and_year VARCHAR(101),
    row_effective_date DATE NOT NULL,
    row_expiry_date DATE,
    current_group VARCHAR(50),
    subject_name VARCHAR(60) NOT NULL,
    teacher_a_b_or_c VARCHAR(50),
    isams_id INT,
    INDEX idx_class_code (class_code)
);

CREATE TABLE dim_dates (
    id BIGINT PRIMARY KEY,
    date DATE NOT NULL,
    academic_year VARCHAR(50) NOT NULL,
    term_name CHAR(3) NOT NULL,
    term_order_number INT NOT NULL,
    day_of_week_number INT NOT NULL,
    week_of_year_number INT NOT NULL,
    day_name VARCHAR(10) NOT NULL,
    month_name VARCHAR(10) NOT NULL,
    year_number SMALLINT NOT NULL,
    term_and_year VARCHAR(20) NOT NULL,
    academic_year_order INT NOT NULL,
    term_order INT NOT NULL,
    week_order_number INT NOT NULL,
    week_of_academic_year_number INT NOT NULL,
    day_of_year_number INT NOT NULL,
    day_of_term_number INT NOT NULL,
    day_of_academic_year_number INT NOT NULL,
    month_number INT NOT NULL,
    day_number INT NOT NULL,
    is_weekend VARCHAR(30) NOT NULL,
    week_commencing_date DATE NOT NULL,
    holiday_type VARCHAR(30) NOT NULL,
    half_termly_report_day VARCHAR(50),
    first_day_of_academic_year DATE,
    last_day_of_academic_year DATE,
    first_day_of_half_term DATE,
    last_day_of_half_term DATE,
    previous_thursday DATE NOT NULL,
    INDEX idx_date (date)
);

CREATE TABLE dim_people (
    id INT AUTO_INCREMENT PRIMARY KEY,
    gis_id INT,
    sims_pk INT,
    isams_school_id VARCHAR(50),
    isams_user_code VARCHAR(50),
    account_type VARCHAR(50) NOT NULL,
    person_bk INT NOT NULL,
    google_classroom_student_id VARCHAR(30),
    INDEX idx_gis_id (gis_id),
    INDEX idx_isams_school_id (isams_school_id),
    INDEX idx_isams_user_code (isams_user_code),
    INDEX idx_sims_pk (sims_pk)
);

CREATE TABLE dim_report_grade (
    id INT AUTO_INCREMENT PRIMARY KEY,
    result VARCHAR(1000) NOT NULL,
    numerical_result FLOAT,
    ucas_points INT,
    gis_points FLOAT,
    data_type VARCHAR(50) NOT NULL,
    printable_result VARCHAR(1000),
    hex_colour VARCHAR(7),
    hex_colour_text VARCHAR(7),
    description VARCHAR(100),
    total_grades INT DEFAULT 1,
    igcse_pass INT,
    category VARCHAR(50)
);

CREATE TABLE dim_students_isams (
    id INT AUTO_INCREMENT PRIMARY KEY,
    first_name VARCHAR(60) NOT NULL,
    last_name VARCHAR(60) NOT NULL,
    student_email VARCHAR(255),
    preferred_first_name VARCHAR(60),
    fam_email VARCHAR(255),
    gis_id_number VARCHAR(20),
    gender VARCHAR(20),
    date_of_birth BIGINT NOT NULL,
    parent_salutation VARCHAR(200),
    house VARCHAR(60),
    year_group VARCHAR(60),
    tutor_group VARCHAR(50),
    eal_status VARCHAR(60),
    sen_status VARCHAR(60),
    sen_profile_url VARCHAR(2000),
    exam_candidate_number VARCHAR(50),
    nationality VARCHAR(100),
    ethnicity VARCHAR(100),
    gis_join_date BIGINT,
    gis_leave_date BIGINT,
    row_effective_date DATE,
    row_expiration_date DATE,
    isams_pk VARCHAR(50),
    on_roll VARCHAR(50),
    ucas_personal_id INT,
    reason_for_leaving VARCHAR(1000),
    destination_after_leaving VARCHAR(255),
    destination_institution VARCHAR(255),
    graduation_academic_year VARCHAR(9),
    person_bk INT
);

CREATE TABLE fact_attendance (
    pk INT AUTO_INCREMENT PRIMARY KEY,
    date_pk BIGINT NOT NULL,
    period VARCHAR(50),
    date_recorded BIGINT NOT NULL,
    time_recorded TIME,
    teaching_group_pk INT,
    student_pk INT,
    student_warehouse_bk INT NOT NULL,
    recording_teacher_pk INT,
    recording_teacher_warehouse_bk INT,
    mark VARCHAR(10) NOT NULL,
    mark_description VARCHAR(50),
    minutes_late TINYINT,
    comment VARCHAR(500),
    class_code VARCHAR(50),
    subject VARCHAR(50),
    room VARCHAR(50),
    class_teacher INT,
    class_teacher_warehouse_bk INT,
    sims_class_pk INT,
    sims_subject_pk INT,
    INDEX idx_fact_attendance (pk, date_pk)
);

CREATE TABLE fact_behaviour (
    pk INT AUTO_INCREMENT PRIMARY KEY,
    student INT NOT NULL,
    date_recorded BIGINT NOT NULL,
    recording_teacher INT,
    recording_teacher_warehouse_bk INT,
    incident_type VARCHAR(50) NOT NULL,
    incident_subtype VARCHAR(50),
    department VARCHAR(50),
    comments VARCHAR(5000),
    incident_date INT NOT NULL,
    resolution_status VARCHAR(50) NOT NULL,
    date_resolved INT,
    action_taken VARCHAR(1000),
    action_taken_by INT,
    location VARCHAR(50),
    time VARCHAR(50),
    points INT NOT NULL,
    isams_id CHAR(36),
    action_location VARCHAR(50),
    action_time VARCHAR(10),
    action_date INT
);

CREATE TABLE fact_student_class_enrolment (
    id INT AUTO_INCREMENT PRIMARY KEY,
    student_id INT,
    student_warehouse_bk INT NOT NULL,
    teaching_group_id INT NOT NULL,
    row_effective_date DATE NOT NULL,
    row_expiry_date DATE,
    academic_year VARCHAR(50) NOT NULL
);

CREATE TABLE fact_update_history (
    id INT AUTO_INCREMENT PRIMARY KEY,
    date DATETIME,
    date_fk INT NOT NULL,
    time TIME NOT NULL,
    dim_staff VARCHAR(3) DEFAULT 'No' NOT NULL,
    dim_related_teaching_groups VARCHAR(3) DEFAULT 'No' NOT NULL,
    dim_dates VARCHAR(3) DEFAULT 'No' NOT NULL,
    dim_report_grade VARCHAR(3) DEFAULT 'No' NOT NULL,
    dim_students VARCHAR(3) DEFAULT 'No' NOT NULL,
    fact_attendance VARCHAR(3) DEFAULT 'No' NOT NULL,
    fact_student_class_enrolment VARCHAR(3) DEFAULT 'No' NOT NULL,
    fact_student_standardised_data VARCHAR(3) DEFAULT 'No' NOT NULL,
    fact_ucas_application_status VARCHAR(3) DEFAULT 'No' NOT NULL,
    fact_report VARCHAR(3) DEFAULT 'No' NOT NULL,
    fact_student_termly_report_card VARCHAR(3) DEFAULT 'No' NOT NULL,
    fact_achievement VARCHAR(3) DEFAULT 'No' NOT NULL,
    fact_behaviour VARCHAR(3) DEFAULT 'No' NOT NULL
);

CREATE TABLE fact_report (
    pk INT AUTO_INCREMENT PRIMARY KEY,
    student INT NOT NULL,
    entry_date BIGINT NOT NULL,
    result TEXT,
    academic_year VARCHAR(30) NOT NULL,
    term VARCHAR(3) NOT NULL,
    subject VARCHAR(30) NOT NULL,
    data_type VARCHAR(50) NOT NULL,
    result_pk INT,
    teacher_a_b_or_c VARCHAR(3) NOT NULL,
    teaching_group_pk INT,
    numeric_result FLOAT,
    sims_aspect_name VARCHAR(50),
    sims_result_set_name VARCHAR(55)
);

CREATE TABLE fact_termly_report_card (
    student BIGINT NOT NULL,
    academic_year TEXT NOT NULL,
    term TEXT NOT NULL,
    subject TEXT NOT NULL,
    teaching_group_pk INT,
    teacher_a_b_or_c TEXT NOT NULL,
    a2_grade TEXT,
    attitudinal_behaviours TEXT,
    as_grade TEXT,
    current_attainment TEXT,
    commendation_or_general_comment TEXT,
    igcse_grade TEXT,
    cat4_target_grade TEXT,
    mock_grade_a2 TEXT,
    mock_grade_as TEXT,
    mock_grade_igcse TEXT,
    mock_grade_y10 TEXT,
    organisational_behaviours TEXT,
    teacher_predicted_grade TEXT,
    progress TEXT,
    target_or_recommendation_comment TEXT,
    target_grade TEXT,
    result_target_or_recommendation TEXT,
    a2_grade_pk FLOAT,
    attitudinal_behaviours_pk FLOAT,
    as_grade_pk FLOAT,
    current_attainment_pk FLOAT,
    igcse_grade_pk FLOAT,
    cat4_target_grade_pk FLOAT,
    mock_grade_a2_pk FLOAT,
    mock_grade_as_pk FLOAT,
    mock_grade_igcse_pk FLOAT,
    mock_grade_y10_pk FLOAT,
    organisational_behaviours_pk FLOAT,
    teacher_predicted_grade_pk FLOAT,
    progress_pk FLOAT,
    result_pk_recommendation FLOAT,
    target_grade_pk FLOAT,
    year_group TEXT,
    current_attainment_minus_target FLOAT,
    current_attainment_minus_cat4_target FLOAT,
    target_minus_cat4_target FLOAT,
    igcse_minus_target FLOAT,
    igcse_minus_cat4 FLOAT,
    igcse_minus_attainment FLOAT,
    igcse_minus_prediction FLOAT,
    as_minus_target FLOAT,
    as_minus_cat4 FLOAT,
    as_minus_attainment FLOAT,
    as_minus_prediction FLOAT,
    a2_minus_target FLOAT,
    a2_minus_cat4 FLOAT,
    a2_minus_attainment FLOAT,
    a2_minus_prediction FLOAT,
    as_minus_igcse FLOAT,
    a2_minus_igcse FLOAT,
    a2_minus_as FLOAT,
    teacher_name VARCHAR(255),
    mock_igcse_minus_target INT,
    mock_a2_minus_target INT,
    igcse_minus_mock_igcse INT,
    as_minus_mock_as INT,
    current_attainment_minus_mock_igcse INT,
    current_attainment_minus_mock_as INT,
    current_attainment_minus_mock_a2 INT,
    mock_as_minus_target INT
);
