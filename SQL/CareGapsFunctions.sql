-- =====================================================
-- CARE GAPS SQL FUNCTIONS WITH PARAMETER COMMENTS
-- This version includes COMMENT clauses for all parameters
-- to eliminate LLM warnings and improve function calling accuracy
-- =====================================================

USE CATALOG dev_kiddo;
USE SCHEMA silver;

-- =====================================================
-- FUNCTION 1: GET PATIENT CARE GAPS
-- =====================================================

CREATE OR REPLACE FUNCTION silver.get_patient_gaps(
  patient_id STRING COMMENT 'Patient identifier - can be either PAT_ID or PAT_MRN_ID (Medical Record Number). Example: "12345" or "MRN1234567". Use this after searching for a patient to get their detailed gap list.'
)
RETURNS TABLE(
  patient_name STRING,
  patient_mrn STRING,
  gap_type STRING,
  gap_category STRING,
  gap_detail STRING,
  priority STRING,
  days_open INT,
  gap_date DATE,
  next_appt_date DATE,
  days_until_appt INT,
  pcp_name STRING,
  outreach_priority STRING
)
COMMENT 'Returns all care gaps for a specific patient by PAT_ID or MRN. Use when user asks "show gaps for patient X" or "what gaps does this patient have". Results sorted by priority and days open.'
RETURN
  SELECT 
    PAT_NAME as patient_name,
    PAT_MRN_ID as patient_mrn,
    GAP_TYPE as gap_type,
    GAP_CATEGORY as gap_category,
    GAP_DETAIL as gap_detail,
    PRIORITY_NAME as priority,
    DAYS_OPEN as days_open,
    CAST(GAP_DATE AS DATE) as gap_date,
    CAST(NEXT_APPT_DATE AS DATE) as next_appt_date,
    DAYS_UNTIL_APPT as days_until_appt,
    PCP_NAME as pcp_name,
    OUTREACH_PRIORITY as outreach_priority
  FROM dev_kiddo.silver.care_gaps_cleaned
  WHERE PAT_ID = patient_id 
     OR PAT_MRN_ID = patient_id
  ORDER BY PRIORITY_LEVEL, DAYS_OPEN DESC;


-- =====================================================
-- FUNCTION 2: GET GAPS BY TYPE
-- =====================================================

CREATE OR REPLACE FUNCTION silver.get_gaps_by_type(
  gap_type_filter STRING COMMENT 'Type of care gap to filter by. Examples: "Immunization", "Well Child Visit", "BMI Screening", "Developmental Assessment", "Lead Screening", "Vision Screening", "Hearing Screening", "Dental", "Asthma", "Diabetes". Case-insensitive partial matching supported (e.g., "immun" matches "Immunization"). Use when user asks about specific gap types like "show me immunization gaps" or "which patients need well child visits".'
)
RETURNS TABLE(
  gap_type STRING,
  total_gaps BIGINT,
  critical_gaps BIGINT,
  important_gaps BIGINT,
  routine_gaps BIGINT,
  patients_affected BIGINT,
  avg_days_open DOUBLE
)
COMMENT 'Returns summary statistics for a specific gap type. Shows total count, breakdown by priority level, number of patients affected, and average days gaps have been open. Use for gap-type-specific analysis.'
RETURN
  SELECT 
    GAP_TYPE as gap_type,
    COUNT(*) as total_gaps,
    SUM(CASE WHEN PRIORITY_NAME = 'Critical' THEN 1 ELSE 0 END) as critical_gaps,
    SUM(CASE WHEN PRIORITY_NAME = 'Important' THEN 1 ELSE 0 END) as important_gaps,
    SUM(CASE WHEN PRIORITY_NAME = 'Routine' THEN 1 ELSE 0 END) as routine_gaps,
    COUNT(DISTINCT PAT_ID) as patients_affected,
    AVG(DAYS_OPEN) as avg_days_open
  FROM dev_kiddo.silver.care_gaps_cleaned
  WHERE LOWER(GAP_TYPE) LIKE LOWER(CONCAT('%', gap_type_filter, '%'))
  GROUP BY GAP_TYPE;


-- =====================================================
-- FUNCTION 3: GET CRITICAL GAPS (Already has comments)
-- =====================================================

CREATE OR REPLACE FUNCTION silver.get_critical_gaps(
  limit_rows INT COMMENT 'Maximum number of critical gaps to return. Default is 100. Use smaller values (5-20) for quick reviews, larger values (100+) for comprehensive analysis. Critical gaps are sorted by days_open (oldest first).'
)
RETURNS TABLE(
  patient_name STRING COMMENT 'Patient full name (will be masked in agent responses)',
  patient_mrn STRING COMMENT 'Patient Medical Record Number (will be masked to show last 4 digits only)',
  age_years INT COMMENT 'Patient age in years',
  gap_type STRING COMMENT 'Type of care gap (e.g., Immunization, Well Child Visit, BMI Screening)',
  gap_detail STRING COMMENT 'Detailed description of the specific gap',
  days_open INT COMMENT 'Number of days the gap has been open (older gaps appear first)',
  pcp_name STRING COMMENT 'Primary Care Provider name',
  pcp_department STRING COMMENT 'PCP department (e.g., Pediatrics, Family Medicine)',
  home_phone STRING COMMENT 'Patient home phone number (will be masked)',
  email STRING COMMENT 'Patient email address (will be masked)',
  next_appt_date DATE COMMENT 'Next scheduled appointment date (NULL if no appointment)',
  days_until_appt INT COMMENT 'Days until next appointment (negative if overdue, NULL if no appointment)',
  outreach_priority STRING COMMENT 'Outreach priority level: High (no appointment), Medium (appointment >30 days), Low (appointment <30 days)'
)
COMMENT 'Returns critical priority care gaps requiring immediate attention. Results sorted by days_open (oldest first). Use for urgent outreach planning when user asks "show me critical gaps" or "urgent gaps".'
RETURN
  SELECT 
    patient_name,
    patient_mrn,
    age_years,
    gap_type,
    gap_detail,
    days_open,
    pcp_name,
    pcp_department,
    home_phone,
    email,
    next_appt_date,
    days_until_appt,
    outreach_priority
  FROM (
    SELECT 
      PAT_NAME as patient_name,
      PAT_MRN_ID as patient_mrn,
      AGE_YEARS as age_years,
      GAP_TYPE as gap_type,
      GAP_DETAIL as gap_detail,
      DAYS_OPEN as days_open,
      PCP_NAME as pcp_name,
      PCP_DEPARTMENT as pcp_department,
      HOME_PHONE as home_phone,
      EMAIL_ADDRESS as email,
      CAST(NEXT_APPT_DATE AS DATE) as next_appt_date,
      DAYS_UNTIL_APPT as days_until_appt,
      OUTREACH_PRIORITY as outreach_priority,
      ROW_NUMBER() OVER (ORDER BY DAYS_OPEN DESC) as rn
    FROM dev_kiddo.silver.care_gaps_cleaned
    WHERE PRIORITY_NAME = 'Critical'
  )
  WHERE rn <= limit_rows;


-- =====================================================
-- FUNCTION 4: GET PROVIDER GAPS
-- =====================================================

CREATE OR REPLACE FUNCTION silver.get_provider_gaps(
  provider_filter STRING COMMENT 'Provider name or department to filter by. Can be: full provider name ("Dr. John Smith"), partial name ("Smith"), or department name ("Pediatrics", "Family Medicine"). Case-insensitive partial matching supported. Use when user asks "show gaps for Dr. X" or "gaps in pediatrics department".'
)
RETURNS TABLE(
  patient_name STRING,
  patient_mrn STRING,
  age_years INT,
  gap_type STRING,
  gap_category STRING,
  priority STRING,
  days_open INT,
  pcp_name STRING,
  pcp_department STRING,
  next_appt_date DATE,
  home_phone STRING,
  email STRING
)
COMMENT 'Returns all care gaps for patients assigned to a specific provider or department. Useful for provider-specific gap reviews and workload analysis. Results sorted by priority and days open.'
RETURN
  SELECT 
    PAT_NAME as patient_name,
    PAT_MRN_ID as patient_mrn,
    AGE_YEARS as age_years,
    GAP_TYPE as gap_type,
    GAP_CATEGORY as gap_category,
    PRIORITY_NAME as priority,
    DAYS_OPEN as days_open,
    PCP_NAME as pcp_name,
    PCP_DEPARTMENT as pcp_department,
    CAST(NEXT_APPT_DATE AS DATE) as next_appt_date,
    HOME_PHONE as home_phone,
    EMAIL_ADDRESS as email
  FROM dev_kiddo.silver.care_gaps_cleaned
  WHERE LOWER(PCP_NAME) LIKE LOWER(CONCAT('%', provider_filter, '%'))
     OR LOWER(PCP_DEPARTMENT) LIKE LOWER(CONCAT('%', provider_filter, '%'))
  ORDER BY PRIORITY_LEVEL, DAYS_OPEN DESC;


-- =====================================================
-- FUNCTION 5: GET OUTREACH NEEDED
-- =====================================================

CREATE OR REPLACE FUNCTION silver.get_outreach_needed(
  gap_type_filter STRING COMMENT 'Optional filter for specific gap type (e.g., "Immunization", "Well Child Visit"). Use NULL or empty string to include all gap types. Example: Pass "Immunization" to get only patients with immunization gaps needing outreach.',
  limit_rows INT COMMENT 'Maximum number of patients to return. Default is 50. Use 20-30 for daily call lists, 50-100 for weekly planning. Patients are prioritized by outreach urgency (High priority first).'
)
RETURNS TABLE(
  patient_name STRING,
  patient_mrn STRING,
  age_years INT,
  sex STRING,
  total_gaps BIGINT,
  critical_gaps BIGINT,
  gap_types STRING,
  pcp_name STRING,
  pcp_department STRING,
  home_phone STRING,
  email STRING,
  next_appt_date DATE,
  days_until_appt INT,
  outreach_priority STRING
)
COMMENT 'Returns consolidated list of patients needing outreach calls, grouped by patient with their total gaps, contact information, and outreach priority. Use when user asks "who should we call" or "patients needing outreach". Results prioritized by urgency: patients with no appointments appear first.'
RETURN
  SELECT 
    patient_name,
    patient_mrn,
    age_years,
    sex,
    total_gaps,
    critical_gaps,
    gap_types,
    pcp_name,
    pcp_department,
    home_phone,
    email,
    next_appt_date,
    days_until_appt,
    outreach_priority
  FROM (
    SELECT 
      PAT_NAME as patient_name,
      PAT_MRN_ID as patient_mrn,
      MAX(AGE_YEARS) as age_years,
      MAX(SEX) as sex,
      COUNT(*) as total_gaps,
      SUM(CASE WHEN PRIORITY_NAME = 'Critical' THEN 1 ELSE 0 END) as critical_gaps,
      LISTAGG(DISTINCT GAP_TYPE, ', ') WITHIN GROUP (ORDER BY GAP_TYPE) as gap_types,
      MAX(PCP_NAME) as pcp_name,
      MAX(PCP_DEPARTMENT) as pcp_department,
      MAX(HOME_PHONE) as home_phone,
      MAX(EMAIL_ADDRESS) as email,
      CAST(MAX(NEXT_APPT_DATE) AS DATE) as next_appt_date,
      MAX(DAYS_UNTIL_APPT) as days_until_appt,
      MAX(OUTREACH_PRIORITY) as outreach_priority,
      ROW_NUMBER() OVER (
        ORDER BY 
          CASE MAX(OUTREACH_PRIORITY) 
            WHEN 'High' THEN 1 
            WHEN 'Medium' THEN 2 
            ELSE 3 
          END,
          COUNT(*) DESC
      ) as rn
    FROM dev_kiddo.silver.care_gaps_cleaned
    WHERE (gap_type_filter IS NULL 
           OR gap_type_filter = ''
           OR LOWER(GAP_TYPE) LIKE LOWER(CONCAT('%', gap_type_filter, '%')))
    GROUP BY PAT_NAME, PAT_MRN_ID
  )
  WHERE rn <= limit_rows;


-- =====================================================
-- FUNCTION 6: GET GAP STATISTICS
-- =====================================================

CREATE OR REPLACE FUNCTION silver.get_gap_statistics()
RETURNS TABLE(
  metric_name STRING,
  metric_value STRING
)
COMMENT 'Returns high-level summary statistics about all care gaps including total count, breakdown by priority (Critical/Important/Routine), average days open, and longest open gap. No parameters needed. Use when user asks "how many gaps", "statistics", "summary", or "overview".'
RETURN
  SELECT * FROM (
    SELECT 'Total Gaps' as metric_name, CAST(COUNT(*) AS STRING) as metric_value
    FROM dev_kiddo.silver.care_gaps_cleaned
    UNION ALL
    SELECT 'Critical Gaps', CAST(SUM(CASE WHEN PRIORITY_NAME = 'Critical' THEN 1 ELSE 0 END) AS STRING)
    FROM dev_kiddo.silver.care_gaps_cleaned
    UNION ALL
    SELECT 'Important Gaps', CAST(SUM(CASE WHEN PRIORITY_NAME = 'Important' THEN 1 ELSE 0 END) AS STRING)
    FROM dev_kiddo.silver.care_gaps_cleaned
    UNION ALL
    SELECT 'Routine Gaps', CAST(SUM(CASE WHEN PRIORITY_NAME = 'Routine' THEN 1 ELSE 0 END) AS STRING)
    FROM dev_kiddo.silver.care_gaps_cleaned
    UNION ALL
    SELECT 'Patients Affected', CAST(COUNT(DISTINCT PAT_ID) AS STRING)
    FROM dev_kiddo.silver.care_gaps_cleaned
    UNION ALL
    SELECT 'Avg Days Open', CAST(ROUND(AVG(DAYS_OPEN), 1) AS STRING)
    FROM dev_kiddo.silver.care_gaps_cleaned
    UNION ALL
    SELECT 'Longest Open Gap (Days)', CAST(MAX(DAYS_OPEN) AS STRING)
    FROM dev_kiddo.silver.care_gaps_cleaned
  )
  ORDER BY 
    CASE metric_name
      WHEN 'Total Gaps' THEN 1
      WHEN 'Critical Gaps' THEN 2
      WHEN 'Important Gaps' THEN 3
      WHEN 'Routine Gaps' THEN 4
      WHEN 'Patients Affected' THEN 5
      WHEN 'Avg Days Open' THEN 6
      WHEN 'Longest Open Gap (Days)' THEN 7
    END;


-- =====================================================
-- FUNCTION 7: GET GAPS BY AGE
-- =====================================================

CREATE OR REPLACE FUNCTION silver.get_gaps_by_age(
  min_age INT COMMENT 'Minimum age in years (inclusive). Use 0 for infants. Example age ranges: 0-2 (infants/toddlers), 3-5 (preschool), 6-12 (school age), 13-18 (adolescents). Default is 0.',
  max_age INT COMMENT 'Maximum age in years (inclusive). Use 18 for all pediatric patients. Must be greater than min_age. Example: Use min_age=13, max_age=18 for teenagers.'
)
RETURNS TABLE(
  age_range STRING,
  total_gaps BIGINT,
  critical_gaps BIGINT,
  patients_affected BIGINT,
  most_common_gap_type STRING,
  avg_days_open DOUBLE
)
COMMENT 'Returns care gap statistics for patients within a specific age range. Useful for age-specific outreach campaigns or analyzing gaps by developmental stage. Use when user asks about specific age groups like "toddlers", "school age", or "teenagers".'
RETURN
  WITH base AS (
    SELECT *
    FROM dev_kiddo.silver.care_gaps_cleaned
    WHERE AGE_YEARS BETWEEN min_age AND max_age
  ),
  gap_type_counts AS (
    SELECT
      GAP_TYPE,
      COUNT(*) AS gap_type_count
    FROM base
    GROUP BY GAP_TYPE
  ),
  ranked_gap_types AS (
    SELECT
      GAP_TYPE,
      gap_type_count,
      ROW_NUMBER() OVER (ORDER BY gap_type_count DESC) AS rn
    FROM gap_type_counts
  )
  SELECT
    CONCAT(CAST(min_age AS STRING), '-', CAST(max_age AS STRING), ' years') AS age_range,
    COUNT(*) AS total_gaps,
    SUM(CASE WHEN PRIORITY_NAME = 'Critical' THEN 1 ELSE 0 END) AS critical_gaps,
    COUNT(DISTINCT PAT_ID) AS patients_affected,
    rgt.GAP_TYPE AS most_common_gap_type,
    AVG(DAYS_OPEN) AS avg_days_open
  FROM base
  LEFT JOIN ranked_gap_types rgt
    ON rgt.rn = 1
  GROUP BY rgt.GAP_TYPE, min_age, max_age;


-- =====================================================
-- FUNCTION 8: GET APPOINTMENTS WITH GAPS
-- =====================================================

CREATE OR REPLACE FUNCTION silver.get_appointments_with_gaps(
  days_ahead INT COMMENT 'Number of days in the future to look for scheduled appointments. Examples: 7 (next week), 14 (next 2 weeks), 30 (next month), 90 (next quarter). Use this to identify patients with upcoming appointments who also have open care gaps, enabling gap closure during the scheduled visit.'
)
RETURNS TABLE(
  patient_name STRING,
  patient_mrn STRING,
  age_years INT,
  appointment_date DATE,
  days_until_appointment INT,
  provider_name STRING,
  appointment_location STRING,
  total_gaps BIGINT,
  critical_gaps BIGINT,
  gap_types_list STRING,
  home_phone STRING,
  email STRING
)
COMMENT 'Returns patients who have scheduled appointments within the specified timeframe AND have open care gaps. Enables proactive gap closure by addressing gaps during already-scheduled visits. Use when user asks "upcoming appointments with gaps" or "patients visiting soon who need followup".'
RETURN
  SELECT 
    PAT_NAME as patient_name,
    PAT_MRN_ID as patient_mrn,
    MAX(AGE_YEARS) as age_years,
    CAST(MAX(NEXT_APPT_DATE) AS DATE) as appointment_date,
    MAX(DAYS_UNTIL_APPT) as days_until_appointment,
    MAX(NEXT_APPT_PROVIDER) as provider_name,
    MAX(NEXT_APPT_LOCATION) as appointment_location,
    COUNT(*) as total_gaps,
    SUM(CASE WHEN PRIORITY_NAME = 'Critical' THEN 1 ELSE 0 END) as critical_gaps,
    LISTAGG(DISTINCT GAP_TYPE, ', ') WITHIN GROUP (ORDER BY GAP_TYPE) as gap_types_list,
    MAX(HOME_PHONE) as home_phone,
    MAX(EMAIL_ADDRESS) as email
  FROM dev_kiddo.silver.care_gaps_cleaned
  WHERE NEXT_APPT_DATE IS NOT NULL
    AND DAYS_UNTIL_APPT BETWEEN 0 AND days_ahead
  GROUP BY PAT_NAME, PAT_MRN_ID
  ORDER BY MAX(NEXT_APPT_DATE);


-- =====================================================
-- FUNCTION 9: GET LONG OPEN GAPS
-- =====================================================

CREATE OR REPLACE FUNCTION silver.get_long_open_gaps(
  days_threshold INT COMMENT 'Minimum number of days gap must be open to be included in results. Common thresholds: 30 (1 month), 60 (2 months), 90 (3 months), 180 (6 months), 365 (1 year). Use to identify long-neglected gaps requiring escalated outreach. Example: Use 90 to find gaps open more than 3 months.'
)
RETURNS TABLE(
  patient_name STRING,
  patient_mrn STRING,
  age_years INT,
  gap_type STRING,
  gap_category STRING,
  priority STRING,
  days_open INT,
  gap_date DATE,
  pcp_name STRING,
  pcp_department STRING,
  home_phone STRING,
  email STRING,
  next_appt_date DATE
)
COMMENT 'Returns care gaps that have been open for longer than the specified number of days. Identifies long-neglected gaps that may need escalated outreach or special attention. Results sorted by days_open (longest first). Use when user asks "old gaps", "long overdue", "longest open gaps", or "gaps open more than X days".'
RETURN
  SELECT 
    PAT_NAME as patient_name,
    PAT_MRN_ID as patient_mrn,
    AGE_YEARS as age_years,
    GAP_TYPE as gap_type,
    GAP_CATEGORY as gap_category,
    PRIORITY_NAME as priority,
    DAYS_OPEN as days_open,
    CAST(GAP_DATE AS DATE) as gap_date,
    PCP_NAME as pcp_name,
    PCP_DEPARTMENT as pcp_department,
    HOME_PHONE as home_phone,
    EMAIL_ADDRESS as email,
    CAST(NEXT_APPT_DATE AS DATE) as next_appt_date
  FROM dev_kiddo.silver.care_gaps_cleaned
  WHERE DAYS_OPEN >= days_threshold
  ORDER BY DAYS_OPEN DESC;


-- =====================================================
-- FUNCTION 10: SEARCH PATIENTS
-- =====================================================

CREATE OR REPLACE FUNCTION silver.search_patients(
  search_term STRING COMMENT 'Search term to find patients. Can be: patient name (full or partial, e.g., "Smith", "John"), Medical Record Number (full or partial, e.g., "12345", "MRN9876"), phone number, or email address. Case-insensitive partial matching supported. Use when user asks "find patient", "search for", "look up", or mentions a name/MRN.'
)
RETURNS TABLE(
  patient_name STRING,
  patient_mrn STRING,
  age_years INT,
  sex STRING,
  pcp_name STRING,
  total_gaps BIGINT,
  critical_gaps BIGINT,
  next_appointment DATE,
  home_phone STRING,
  email STRING
)
COMMENT 'Search for patients by name, MRN, phone, or email and return their care gap summary. Returns list of matching patients sorted by total gaps (highest first). Limited to top 50 matches. Use this as first step before getting detailed patient information with get_patient_gaps() or get_patient_360().'
RETURN
  SELECT 
    PAT_NAME as patient_name,
    PAT_MRN_ID as patient_mrn,
    MAX(AGE_YEARS) as age_years,
    MAX(SEX) as sex,
    MAX(PCP_NAME) as pcp_name,
    COUNT(*) as total_gaps,
    SUM(CASE WHEN PRIORITY_NAME = 'Critical' THEN 1 ELSE 0 END) as critical_gaps,
    CAST(MAX(NEXT_APPT_DATE) AS DATE) as next_appointment,
    MAX(HOME_PHONE) as home_phone,
    MAX(EMAIL_ADDRESS) as email
  FROM dev_kiddo.silver.care_gaps_cleaned
  WHERE LOWER(PAT_NAME) LIKE LOWER(CONCAT('%', search_term, '%'))
     OR PAT_MRN_ID LIKE CONCAT('%', search_term, '%')
     OR HOME_PHONE LIKE CONCAT('%', search_term, '%')
     OR LOWER(EMAIL_ADDRESS) LIKE LOWER(CONCAT('%', search_term, '%'))
  GROUP BY PAT_NAME, PAT_MRN_ID
  ORDER BY total_gaps DESC
  LIMIT 50;


-- =====================================================
-- FUNCTION 11: GET DEPARTMENT SUMMARY
-- =====================================================

CREATE OR REPLACE FUNCTION silver.get_department_summary()
RETURNS TABLE(
  department STRING,
  total_gaps BIGINT,
  critical_gaps BIGINT,
  important_gaps BIGINT,
  unique_providers BIGINT,
  patients_affected BIGINT,
  avg_days_open DOUBLE
)
COMMENT 'Returns care gap statistics aggregated by PCP department/clinic. Shows total gaps, breakdown by priority, number of providers, and patients affected for each department. No parameters needed. Use when user asks "gaps by department", "which clinic has most gaps", or "department comparison". Results sorted by total gaps (highest first).'
RETURN
  SELECT 
    PCP_DEPARTMENT as department,
    COUNT(*) as total_gaps,
    SUM(CASE WHEN PRIORITY_NAME = 'Critical' THEN 1 ELSE 0 END) as critical_gaps,
    SUM(CASE WHEN PRIORITY_NAME = 'Important' THEN 1 ELSE 0 END) as important_gaps,
    COUNT(DISTINCT PCP_NAME) as unique_providers,
    COUNT(DISTINCT PAT_ID) as patients_affected,
    AVG(DAYS_OPEN) as avg_days_open
  FROM dev_kiddo.silver.care_gaps_cleaned
  WHERE PCP_DEPARTMENT IS NOT NULL
  GROUP BY PCP_DEPARTMENT
  ORDER BY total_gaps DESC;


-- =====================================================
-- FUNCTION 12: GET GAP CATEGORIES
-- =====================================================

CREATE OR REPLACE FUNCTION silver.get_gap_categories()
RETURNS TABLE(
  gap_category STRING,
  gap_type STRING,
  total_occurrences BIGINT,
  patients_affected BIGINT
)
COMMENT 'Returns all unique gap categories and types with their occurrence counts. Shows the taxonomy/classification of all care gaps tracked in the system. No parameters needed. Use when user asks "what types of gaps", "list gap categories", "what do we track", or needs to understand available gap classifications. Results sorted by occurrence count (highest first).'
RETURN
  SELECT 
    GAP_CATEGORY as gap_category,
    GAP_TYPE as gap_type,
    COUNT(*) as total_occurrences,
    COUNT(DISTINCT PAT_ID) as patients_affected
  FROM dev_kiddo.silver.care_gaps_cleaned
  GROUP BY GAP_CATEGORY, GAP_TYPE
  ORDER BY total_occurrences DESC;


-- =====================================================
-- FUNCTION 13: GET PATIENT 360 VIEW
-- =====================================================

CREATE OR REPLACE FUNCTION silver.get_patient_360(
  patient_id STRING COMMENT 'Patient identifier - can be either PAT_ID or PAT_MRN_ID (Medical Record Number). Example: "12345" or "MRN1234567". Use this when user asks for "complete patient information", "patient 360", "everything about patient X", or "full patient view".'
)
RETURNS TABLE(
  section STRING,
  attribute STRING,
  value STRING
)
COMMENT 'Returns comprehensive 360-degree view of a single patient including: demographics (name, MRN, age, sex), care team (PCP, department, care team), contact information (phone, email), gap summary (total, by priority, days open), and next appointment details. Data returned in hierarchical sections for easy reading. Use when user needs complete patient profile.'
RETURN
  WITH patient_info AS (
    SELECT 
      PAT_NAME,
      PAT_MRN_ID,
      AGE_YEARS,
      AGE_MONTHS,
      SEX,
      PCP_NAME,
      PCP_DEPARTMENT,
      PCP_CARE_TEAM_NAME,
      HOME_PHONE,
      EMAIL_ADDRESS
    FROM dev_kiddo.silver.care_gaps_cleaned
    WHERE PAT_ID = patient_id OR PAT_MRN_ID = patient_id
    LIMIT 1
  ),
  gap_summary AS (
    SELECT 
      COUNT(*) as total_gaps,
      SUM(CASE WHEN PRIORITY_NAME = 'Critical' THEN 1 ELSE 0 END) as critical_gaps,
      SUM(CASE WHEN PRIORITY_NAME = 'Important' THEN 1 ELSE 0 END) as important_gaps,
      SUM(CASE WHEN PRIORITY_NAME = 'Routine' THEN 1 ELSE 0 END) as routine_gaps,
      AVG(DAYS_OPEN) as avg_days_open,
      MAX(DAYS_OPEN) as max_days_open
    FROM dev_kiddo.silver.care_gaps_cleaned
    WHERE PAT_ID = patient_id OR PAT_MRN_ID = patient_id
  ),
  next_appt AS (
    SELECT 
      CAST(NEXT_APPT_DATE AS STRING) as appt_date,
      CAST(DAYS_UNTIL_APPT AS STRING) as days_until,
      NEXT_APPT_PROVIDER as provider,
      NEXT_APPT_LOCATION as location
    FROM dev_kiddo.silver.care_gaps_cleaned
    WHERE (PAT_ID = patient_id OR PAT_MRN_ID = patient_id)
      AND NEXT_APPT_DATE IS NOT NULL
    LIMIT 1
  )
  SELECT * FROM (
    SELECT 'Demographics' as section, 'Name' as attribute, PAT_NAME as value FROM patient_info
    UNION ALL SELECT 'Demographics', 'MRN', PAT_MRN_ID FROM patient_info
    UNION ALL SELECT 'Demographics', 'Age (Years)', CAST(AGE_YEARS AS STRING) FROM patient_info
    UNION ALL SELECT 'Demographics', 'Age (Months)', CAST(AGE_MONTHS AS STRING) FROM patient_info
    UNION ALL SELECT 'Demographics', 'Sex', SEX FROM patient_info
    UNION ALL SELECT 'Care Team', 'PCP Name', PCP_NAME FROM patient_info
    UNION ALL SELECT 'Care Team', 'PCP Department', PCP_DEPARTMENT FROM patient_info
    UNION ALL SELECT 'Care Team', 'Care Team', PCP_CARE_TEAM_NAME FROM patient_info
    UNION ALL SELECT 'Contact', 'Phone', HOME_PHONE FROM patient_info
    UNION ALL SELECT 'Contact', 'Email', EMAIL_ADDRESS FROM patient_info
    UNION ALL SELECT 'Gap Summary', 'Total Gaps', CAST(total_gaps AS STRING) FROM gap_summary
    UNION ALL SELECT 'Gap Summary', 'Critical Gaps', CAST(critical_gaps AS STRING) FROM gap_summary
    UNION ALL SELECT 'Gap Summary', 'Important Gaps', CAST(important_gaps AS STRING) FROM gap_summary
    UNION ALL SELECT 'Gap Summary', 'Routine Gaps', CAST(routine_gaps AS STRING) FROM gap_summary
    UNION ALL SELECT 'Gap Summary', 'Avg Days Open', CAST(ROUND(avg_days_open, 1) AS STRING) FROM gap_summary
    UNION ALL SELECT 'Gap Summary', 'Longest Open Gap (Days)', CAST(max_days_open AS STRING) FROM gap_summary
    UNION ALL SELECT 'Next Appointment', 'Date', appt_date FROM next_appt
    UNION ALL SELECT 'Next Appointment', 'Days Until', days_until FROM next_appt
    UNION ALL SELECT 'Next Appointment', 'Provider', provider FROM next_appt
    UNION ALL SELECT 'Next Appointment', 'Location', location FROM next_appt
  )
  ORDER BY 
    CASE section
      WHEN 'Demographics' THEN 1
      WHEN 'Care Team' THEN 2
      WHEN 'Contact' THEN 3
      WHEN 'Gap Summary' THEN 4
      WHEN 'Next Appointment' THEN 5
      ELSE 6
    END,
    attribute;


-- =====================================================
-- FUNCTION 14: GET TOP PROVIDERS BY GAPS
-- =====================================================

CREATE OR REPLACE FUNCTION silver.get_top_providers(
  limit_rows INT COMMENT 'Number of top providers to return, ranked by total care gaps. Default is 10. Use 5 for quick overview, 10-20 for standard reporting, 50+ for comprehensive analysis. Results show providers with highest gap counts first.'
)
RETURNS TABLE(
  provider_name STRING COMMENT 'Provider full name',
  department STRING COMMENT 'Provider department/clinic',
  total_gaps BIGINT COMMENT 'Total number of open care gaps for this provider''s patients',
  critical_gaps BIGINT COMMENT 'Number of critical priority gaps',
  patients_affected BIGINT COMMENT 'Number of unique patients with gaps assigned to this provider',
  avg_days_open DOUBLE COMMENT 'Average number of days gaps have been open for this provider''s patients'
)
COMMENT 'Returns providers ranked by total number of care gaps across their patient panel (highest first). Use to identify which providers need most support with gap closure, have highest workload, or require quality improvement focus. Use when user asks "which providers have most gaps", "top providers by gaps", or "provider rankings".'
RETURN
  SELECT 
    provider_name,
    department,
    total_gaps,
    critical_gaps,
    patients_affected,
    avg_days_open
  FROM (
    SELECT 
      PCP_NAME as provider_name,
      PCP_DEPARTMENT as department,
      COUNT(*) as total_gaps,
      SUM(CASE WHEN PRIORITY_NAME = 'Critical' THEN 1 ELSE 0 END) as critical_gaps,
      COUNT(DISTINCT PAT_ID) as patients_affected,
      AVG(DAYS_OPEN) as avg_days_open,
      ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) as rn
    FROM dev_kiddo.silver.care_gaps_cleaned
    WHERE PCP_NAME IS NOT NULL
    GROUP BY PCP_NAME, PCP_DEPARTMENT
  )
  WHERE rn <= limit_rows;