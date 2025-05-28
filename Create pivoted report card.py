import pandas as pd
import random
from sqlalchemy import create_engine, text

# Setup
engine = create_engine("mysql+pymysql://trainee:trainpass@localhost/warehouse")
random.seed(42)

# Get current academic year
today = pd.to_datetime("today")
academic_year = pd.read_sql(f"""
    SELECT DISTINCT `Academic Year`
    FROM dim_Dates
    WHERE Date = '{today.date()}'
    LIMIT 1
""", engine).iloc[0]["Academic Year"]

# Load current year report data
report_df = pd.read_sql(f"""
    SELECT *
    FROM `fact_report`
    WHERE `Academic year` = '{academic_year}' AND `Data type` IN (
        'A2 Grade', 'AB', 'AS Grade', 'Attainment', 'Commendation', 'If Challenged',
        'IGCSE Grade', 'Mock', 'Mock - A2', 'Mock - AS', 'Mock - IGCSE', 'Mock - Y10 EOY',
        'Mock score', 'OB', 'Predicted', 'Progress', 'PTE', 'PTM', 'Quantitative',
        'Mean', 'Recommendation', 'Spatial', 'Target', 'Target or recommendation', 'Verbal'
    )
""", engine)

# Compute average entry date for AB/OB
avg_entry = report_df[report_df['Data type'].isin(['AB', 'OB'])].groupby(
    ['Student', 'Academic year', 'Term', 'Subject', 'Teacher a b or c']
)['Entry Date'].mean().reset_index()

# Keep latest record per grouping
latest = report_df.drop_duplicates(
    subset=['Student', 'Academic year', 'Term', 'Subject', 'Teacher a b or c', 'Data type'],
    keep='first'
).merge(
    avg_entry,
    on=['Student', 'Academic year', 'Term', 'Subject', 'Teacher a b or c'],
    how='left',
    suffixes=('', '_avg')
)

# Pivot into wide format
pivoted = latest.pivot_table(
    index=['Student', 'Academic year', 'Term', 'Subject', 'Teacher a b or c', 'Entry Date_avg'],
    columns='Data type',
    values=['Result', 'Numeric result', 'Result pk'],
    aggfunc='first'
).reset_index()

pivoted.columns = ['_'.join(col).strip() if col[1] else col[0] for col in pivoted.columns.values]

# Bring in Year Group from dim_students_isams
students = pd.read_sql("""
    SELECT `Year Group`, `Row Effective Date`, `Row Expiration Date`, `Person BK`
    FROM `dim_students_isams`
""", engine)

# Ensure dates are clean
students['Row Expiration Date'] = students['Row Expiration Date'].fillna('9999-01-01')
students['Row Effective Date'] = pd.to_datetime(students['Row Effective Date'], errors='coerce')
students['Row Expiration Date'] = pd.to_datetime(students['Row Expiration Date'], errors='coerce')
pivoted['Entry Date_avg'] = pd.to_datetime(pivoted['Entry Date_avg'], format='%Y%m%d', errors='coerce')

# Join to get year group
merged = pivoted.merge(students, left_on='Student', right_on='Person BK', how='left')

merged['Row Expiration Date'] = merged['Row Expiration Date'].fillna(pd.Timestamp("2099-12-31"))

merged = merged[
    (merged['Entry Date_avg'] >= merged['Row Effective Date']) &
    (merged['Entry Date_avg'] <= merged['Row Expiration Date'])
].drop(columns=['Row Effective Date', 'Row Expiration Date', 'Person BK'])


# Define fill-down columns
fill_cols = [c for c in merged.columns if any(prefix in c for prefix in ["Result_", "Numeric result_", "Result pk_"])]

# Fill-down logic per student/subject
def fill_down(group):
    group = group.copy()
    for col in fill_cols:
        group[col] = group.groupby('Academic year')[col].ffill()
        if group['Year Group'].isin(['10', '12']).any():
            group[col] = group.groupby('Student')[col].ffill(limit=2)
    return group

df = merged.sort_values(['Student', 'Subject', 'Teacher a b or c', 'Academic year', 'Term'])
df = df.groupby(['Student', 'Subject']).apply(fill_down).reset_index(drop=True)

# Calculated columns
# Safe calculation of derived columns
def safe_calc(col1, col2, label):
    if col1 in df.columns and col2 in df.columns:
        df[label] = df[col1] - df[col2]

safe_calc('Numeric result_Attainment', 'Numeric result_Target', 'Current attainment minus target')
safe_calc('Numeric result_Attainment', 'Numeric result_If Challenged', 'Current attainment minus CAT4 target')
safe_calc('Numeric result_Target', 'Numeric result_If Challenged', 'Target minus CAT4 target')

safe_calc('Numeric result_IGCSE Grade', 'Numeric result_Target', 'IGCSE minus target')
safe_calc('Numeric result_IGCSE Grade', 'Numeric result_If Challenged', 'IGCSE minus CAT4')
safe_calc('Numeric result_IGCSE Grade', 'Numeric result_Attainment', 'IGCSE minus attainment')
safe_calc('Numeric result_IGCSE Grade', 'Numeric result_Predicted', 'IGCSE minus prediction')

safe_calc('Numeric result_AS Grade', 'Numeric result_Target', 'AS minus target')
safe_calc('Numeric result_AS Grade', 'Numeric result_If Challenged', 'AS minus CAT4')
safe_calc('Numeric result_AS Grade', 'Numeric result_Attainment', 'AS minus attainment')
safe_calc('Numeric result_AS Grade', 'Numeric result_Predicted', 'AS minus prediction')

safe_calc('Numeric result_A2 Grade', 'Numeric result_Target', 'A2 minus target')
safe_calc('Numeric result_A2 Grade', 'Numeric result_If Challenged', 'A2 minus CAT4')
safe_calc('Numeric result_A2 Grade', 'Numeric result_Attainment', 'A2 minus attainment')
safe_calc('Numeric result_A2 Grade', 'Numeric result_Predicted', 'A2 minus prediction')

safe_calc('Numeric result_AS Grade', 'Numeric result_IGCSE Grade', 'AS minus IGCSE')
safe_calc('Numeric result_A2 Grade', 'Numeric result_IGCSE Grade', 'A2 minus IGCSE')
safe_calc('Numeric result_A2 Grade', 'Numeric result_AS Grade', 'A2 minus AS')


# Rename for clarity
column_mapping = {
    'Result_Attainment': 'Current Attainment',
    'Result_Target': 'Target grade',
    'Result_If Challenged': 'CAT4 Target grade',
    'Result_AB': 'Attitudinal behaviours',
    'Result_OB': 'Organisational behaviours',
}
df_renamed = df.rename(columns=column_mapping)

# Clean up columns
drop_cols = [c for c in df_renamed.columns if 'Mock score' in c or 'Commendation' in c]
df_final = df_renamed.drop(columns=drop_cols)
df_final

# Save to staging and run post-processing
with engine.begin() as conn:
    df_final.to_sql("fact Termly Report Card", con=conn, schema="warehouse", if_exists="replace", index=False)
