-- =====================================================
-- CAMPAIGN OPPORTUNITIES UC FUNCTIONS
-- Unified functions for all campaign types (Flu Vaccine, Lab Piggybacking, etc.)
-- Table: dev_kiddo.silver.campaign_opportunities
-- =====================================================

USE CATALOG dev_kiddo;
USE SCHEMA silver;

-- =====================================================
-- FUNCTION 1: GET CAMPAIGN STATISTICS
-- =====================================================

CREATE OR REPLACE FUNCTION silver.get_campaign_statistics(
  campaign_type_filter STRING COMMENT 'Campaign type to get statistics for. Examples: "FLU_VACCINE", "LAB_PIGGYBACKING". Use NULL or empty string for all campaigns. Case-insensitive.'
)
RETURNS TABLE(
  metric_name STRING,
  metric_value STRING
)
COMMENT 'Returns high-level summary statistics for a campaign including total opportunities, breakdown by status (pending/approved/sent/completed), confidence levels, MyChart activation rate, asthma patients, and average age. Use when user asks "how is the flu campaign going", "campaign statistics", "flu numbers", or "campaign overview".'
RETURN
  WITH base AS (
    SELECT *
    FROM dev_kiddo.silver.campaign_opportunities
    WHERE (campaign_type_filter IS NULL
           OR campaign_type_filter = ''
           OR UPPER(campaign_type) = UPPER(campaign_type_filter))
  )
  SELECT * FROM (
    SELECT 'Total Opportunities' AS metric_name, CAST(COUNT(*) AS STRING) AS metric_value FROM base
    UNION ALL
    SELECT 'Pending Review', CAST(SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) AS STRING) FROM base
    UNION ALL
    SELECT 'Approved', CAST(SUM(CASE WHEN status = 'approved' THEN 1 ELSE 0 END) AS STRING) FROM base
    UNION ALL
    SELECT 'Sent', CAST(SUM(CASE WHEN status = 'sent' THEN 1 ELSE 0 END) AS STRING) FROM base
    UNION ALL
    SELECT 'Completed', CAST(SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) AS STRING) FROM base
    UNION ALL
    SELECT 'Unique Target Patients', CAST(COUNT(DISTINCT patient_mrn) AS STRING) FROM base
    UNION ALL
    SELECT 'Unique Subject Patients (with appt)', CAST(COUNT(DISTINCT subject_mrn) AS STRING) FROM base
    UNION ALL
    SELECT 'Shared Address (Sibling) Opportunities', CAST(SUM(CASE WHEN relationship_type = 'Shared Address' THEN 1 ELSE 0 END) AS STRING) FROM base
    UNION ALL
    SELECT 'Patient (Self) Opportunities', CAST(SUM(CASE WHEN relationship_type = 'Patient' THEN 1 ELSE 0 END) AS STRING) FROM base
    UNION ALL
    SELECT 'HIGH Confidence Matches', CAST(SUM(CASE WHEN confidence_level = 'HIGH' THEN 1 ELSE 0 END) AS STRING) FROM base
    UNION ALL
    SELECT 'MEDIUM Confidence Matches', CAST(SUM(CASE WHEN confidence_level = 'MEDIUM' THEN 1 ELSE 0 END) AS STRING) FROM base
    UNION ALL
    SELECT 'MyChart Active', CAST(SUM(CASE WHEN mychart_active = 'Activated' THEN 1 ELSE 0 END) AS STRING) FROM base
    UNION ALL
    SELECT 'Has Mobile Number', CAST(SUM(CASE WHEN mobile_number_on_file = 'Y' THEN 1 ELSE 0 END) AS STRING) FROM base
    UNION ALL
    SELECT 'Asthma Patients (J45)', CAST(SUM(CASE WHEN has_asthma = 'Y' THEN 1 ELSE 0 END) AS STRING) FROM base
    UNION ALL
    SELECT 'Avg Age (Years)', CAST(ROUND(AVG(CAST(age_years AS DOUBLE)), 1) AS STRING) FROM base
  )
  ORDER BY
    CASE metric_name
      WHEN 'Total Opportunities' THEN 1
      WHEN 'Pending Review' THEN 2
      WHEN 'Approved' THEN 3
      WHEN 'Sent' THEN 4
      WHEN 'Completed' THEN 5
      WHEN 'Unique Target Patients' THEN 6
      WHEN 'Unique Subject Patients (with appt)' THEN 7
      WHEN 'Shared Address (Sibling) Opportunities' THEN 8
      WHEN 'Patient (Self) Opportunities' THEN 9
      WHEN 'HIGH Confidence Matches' THEN 10
      WHEN 'MEDIUM Confidence Matches' THEN 11
      WHEN 'MyChart Active' THEN 12
      WHEN 'Has Mobile Number' THEN 13
      WHEN 'Asthma Patients (J45)' THEN 14
      WHEN 'Avg Age (Years)' THEN 15
    END;


-- =====================================================
-- FUNCTION 2: SEARCH CAMPAIGN OPPORTUNITIES
-- =====================================================

CREATE OR REPLACE FUNCTION silver.search_campaign_opportunities(
  search_term STRING COMMENT 'Search term to find campaign opportunities. Can be: patient MRN (e.g., "12345"), patient name (e.g., "Smith"), appointment location (e.g., "Beachwood"), or subject MRN. Case-insensitive partial matching supported.',
  campaign_type_filter STRING COMMENT 'Campaign type to search within. Examples: "FLU_VACCINE", "LAB_PIGGYBACKING". Use NULL or empty string to search all campaigns.'
)
RETURNS TABLE(
  campaign_type STRING,
  patient_mrn STRING,
  patient_name STRING,
  age_years INT,
  sex STRING,
  relationship_type STRING,
  subject_mrn STRING,
  subject_name STRING,
  appointment_date TIMESTAMP,
  appointment_location STRING,
  mychart_active STRING,
  mobile_number_on_file STRING,
  confidence_level STRING,
  has_asthma STRING,
  last_flu_vaccine_date DATE,
  status STRING,
  llm_message STRING
)
COMMENT 'Search for specific campaign opportunities by patient MRN, name, location, or subject MRN. Returns matching opportunities with full details. Use when user asks "show flu opportunities for MRN 12345", "find patient Smith in flu campaign", or "opportunities at Beachwood". Limited to 50 results.'
RETURN
  SELECT
    campaign_type,
    patient_mrn,
    patient_name,
    age_years,
    sex,
    relationship_type,
    subject_mrn,
    subject_name,
    appointment_date,
    appointment_location,
    mychart_active,
    mobile_number_on_file,
    confidence_level,
    has_asthma,
    CAST(last_flu_vaccine_date AS DATE) AS last_flu_vaccine_date,
    status,
    llm_message
  FROM dev_kiddo.silver.campaign_opportunities
  WHERE (campaign_type_filter IS NULL
         OR campaign_type_filter = ''
         OR UPPER(campaign_type) = UPPER(campaign_type_filter))
    AND (LOWER(patient_mrn) LIKE LOWER(CONCAT('%', search_term, '%'))
         OR LOWER(patient_name) LIKE LOWER(CONCAT('%', search_term, '%'))
         OR LOWER(subject_mrn) LIKE LOWER(CONCAT('%', search_term, '%'))
         OR LOWER(subject_name) LIKE LOWER(CONCAT('%', search_term, '%'))
         OR LOWER(appointment_location) LIKE LOWER(CONCAT('%', search_term, '%')))
  ORDER BY appointment_date
  LIMIT 50;


-- =====================================================
-- FUNCTION 3: GET CAMPAIGN OPPORTUNITIES
-- =====================================================

CREATE OR REPLACE FUNCTION silver.get_campaign_opportunities(
  campaign_type_filter STRING COMMENT 'Campaign type to list opportunities for. Examples: "FLU_VACCINE", "LAB_PIGGYBACKING". Required — specify which campaign.',
  status_filter STRING COMMENT 'Filter by opportunity status. Options: "pending", "approved", "sent", "completed". Use NULL or empty string for all statuses.',
  location_filter STRING COMMENT 'Filter by appointment location. Example: "Beachwood", "Akron". Use NULL or empty string for all locations. Case-insensitive partial match.',
  limit_rows INT COMMENT 'Maximum number of opportunities to return. Default 50. Use 10-20 for quick review, 50-100 for comprehensive lists.'
)
RETURNS TABLE(
  patient_mrn STRING,
  patient_name STRING,
  age_years INT,
  relationship_type STRING,
  subject_mrn STRING,
  subject_name STRING,
  appointment_date TIMESTAMP,
  appointment_location STRING,
  mychart_active STRING,
  mobile_number_on_file STRING,
  confidence_level STRING,
  has_asthma STRING,
  last_flu_vaccine_date DATE,
  status STRING,
  llm_message STRING
)
COMMENT 'Returns a list of campaign opportunities with optional filters for status and location. Use when user asks "show pending flu opportunities", "flu opportunities at Beachwood", "list approved campaigns", or "upcoming flu piggybacking opportunities". Results sorted by appointment date (soonest first).'
RETURN
  SELECT
    patient_mrn,
    patient_name,
    age_years,
    relationship_type,
    subject_mrn,
    subject_name,
    appointment_date,
    appointment_location,
    mychart_active,
    mobile_number_on_file,
    confidence_level,
    has_asthma,
    CAST(last_flu_vaccine_date AS DATE) AS last_flu_vaccine_date,
    status,
    llm_message
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER (ORDER BY appointment_date ASC) AS rn
    FROM dev_kiddo.silver.campaign_opportunities
    WHERE UPPER(campaign_type) = UPPER(campaign_type_filter)
      AND (status_filter IS NULL OR status_filter = '' OR LOWER(status) = LOWER(status_filter))
      AND (location_filter IS NULL OR location_filter = '' OR LOWER(appointment_location) LIKE LOWER(CONCAT('%', location_filter, '%')))
  )
  WHERE rn <= limit_rows;


-- =====================================================
-- FUNCTION 4: GET PATIENT CAMPAIGN HISTORY
-- =====================================================

CREATE OR REPLACE FUNCTION silver.get_patient_campaign_history(
  patient_mrn_filter STRING COMMENT 'Patient MRN to look up campaign history for. Example: "12345". Returns all campaign opportunities where this patient appears as either the target patient or the subject patient (the one with the appointment).'
)
RETURNS TABLE(
  campaign_type STRING,
  role STRING,
  patient_mrn STRING,
  patient_name STRING,
  subject_mrn STRING,
  subject_name STRING,
  relationship_type STRING,
  appointment_date TIMESTAMP,
  appointment_location STRING,
  has_asthma STRING,
  last_flu_vaccine_date DATE,
  status STRING,
  llm_message STRING,
  created_date TIMESTAMP
)
COMMENT 'Returns all campaign opportunities for a specific patient — both as a target (needing the service) and as a subject (the one with the appointment whose siblings need the service). Use when user asks "campaign history for MRN 12345", "has this patient been contacted", or "what campaigns is this patient in".'
RETURN
  SELECT
    campaign_type,
    CASE
      WHEN patient_mrn = patient_mrn_filter THEN 'Target Patient'
      WHEN subject_mrn = patient_mrn_filter THEN 'Subject Patient (has appt)'
    END AS role,
    patient_mrn,
    patient_name,
    subject_mrn,
    subject_name,
    relationship_type,
    appointment_date,
    appointment_location,
    has_asthma,
    CAST(last_flu_vaccine_date AS DATE) AS last_flu_vaccine_date,
    status,
    llm_message,
    created_date
  FROM dev_kiddo.silver.campaign_opportunities
  WHERE patient_mrn = patient_mrn_filter
     OR subject_mrn = patient_mrn_filter
  ORDER BY created_date DESC;
