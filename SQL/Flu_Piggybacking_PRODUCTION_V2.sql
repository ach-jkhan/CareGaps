/*******************************************************************************
 * FLU VACCINE PIGGYBACKING QUERY - PRODUCTION VERSION V2 (AGENTIC)
 * 
 * PROJECT REQUIREMENTS (from PowerPoint slides 7-9):
 * - Date Range: September 1, 2025 to March 31, 2026 (flu season)
 * - Focus: FLU VACCINE ONLY (HM_TOPIC_ID 20, 24)
 * - First Appointment Only: Each subject patient limited to earliest appointment
 * - Exclusion: Age > 24 months with NO lifetime flu vaccines
 * - Output: 160-character SMS messages
 * 
 * KEY AGENTIC LOGIC (What Makes This AI, Not a Dashboard):
 * 1. Subject patients: Limited to FIRST appointment in date range
 * 2. Siblings: Can have multiple per subject patient (if multiple siblings need flu vaccine)
 * 3. CRITICAL: Siblings with their own appointments are EXCLUDED
 *    - Prevents duplicate/confusing messages
 *    - Only suggests true "piggybacking" opportunities
 *    - Example: If Tommy AND Sarah both have appointments, NO message sent
 *    - Example: If Tommy has appointment but Sarah doesn't, message sent for Sarah
 * 
 * EXPECTED RESULTS:
 * - Before exclusion: ~14,388 potential sibling opportunities
 * - After exclusion: ~8,000-10,000 TRUE piggybacking opportunities
 * - Siblings excluded: ~4,000-6,000 (already have appointments)
 * 
 * TABLE STRUCTURES CONFIRMED:
 * - PATIENT_HMT_STATUS: Current HMT status (PAT_ID, QUALIFIED_HMT_ID, HMT_DUE_STATUS_C, IDEAL_RETURN_DT)
 * - HM_HISTORICAL_STATUS: Historical HMT data (includes LAST_COMPLETED_DATE)
 * 
 * Author: AI Assistant
 * Date: January 31, 2026
 ******************************************************************************/

IF OBJECT_ID('tempdb..#AllAppointments') IS NOT NULL DROP TABLE #AllAppointments;
IF OBJECT_ID('tempdb..#UpcomingAppointments') IS NOT NULL DROP TABLE #UpcomingAppointments;
IF OBJECT_ID('tempdb..#SubjectPatients') IS NOT NULL DROP TABLE #SubjectPatients;
IF OBJECT_ID('tempdb..#AllMatches') IS NOT NULL DROP TABLE #AllMatches;
IF OBJECT_ID('tempdb..#HouseholdMembers') IS NOT NULL DROP TABLE #HouseholdMembers;
IF OBJECT_ID('tempdb..#SiblingFluStatus') IS NOT NULL DROP TABLE #SiblingFluStatus;
IF OBJECT_ID('tempdb..#FluOpportunities') IS NOT NULL DROP TABLE #FluOpportunities;

-- ============================================================================
-- STEP 1: Get Upcoming Appointments (Sept 1 - March 31)
-- ============================================================================
-- First get all appointments, then filter to FIRST appointment per patient
SELECT 
    appt.PAT_ID AS SUBJECT_PATIENT_ID,
    appt.PAT_ENC_CSN_ID AS CSN,
    appt.APPT_DTTM,
    appt.ACCOUNT_ID,
    dep.DEPARTMENT_ID,
    dep.DEPARTMENT_NAME AS VACCINE_LOCATION
INTO #AllAppointments
FROM F_SCHED_APPT appt
INNER JOIN ZC_APPT_STATUS apptStatus 
    ON appt.APPT_STATUS_C = apptStatus.APPT_STATUS_C 
    AND apptStatus.NAME <> 'Canceled'
INNER JOIN CLARITY_DEP dep
    ON appt.DEPARTMENT_ID = dep.DEPARTMENT_ID
    AND dep.INPATIENT_DEPT_YN = 'N'
    AND dep.DEPARTMENT_NAME NOT LIKE 'ZZZ%'
WHERE 
    -- Date range: Sept 1, 2025 to March 31, 2026 (flu season)
    --appt.APPT_DTTM BETWEEN '2025-09-01' AND '2026-03-31'
    appt.appt_dttm BETWEEN DATEADD(DAY, 1, GETDATE()) AND DATEADD(DAY, 30, GETDATE())
    AND appt.ACCOUNT_ID IS NOT NULL;

--select * from #AllAppointments

-- Filter to FIRST appointment per subject patient
SELECT *
INTO #UpcomingAppointments
FROM (
    SELECT 
        SUBJECT_PATIENT_ID,
        CSN,
        APPT_DTTM,
        ACCOUNT_ID,
        DEPARTMENT_ID,
        VACCINE_LOCATION,
        ROW_NUMBER() OVER (
            PARTITION BY SUBJECT_PATIENT_ID 
            ORDER BY APPT_DTTM ASC  -- Earliest appointment first
        ) AS APPT_RANK
    FROM #AllAppointments
) ranked
WHERE APPT_RANK = 1;  -- Only first appointment per patient

DROP TABLE #AllAppointments;

CREATE INDEX IDX_SUBJECT ON #UpcomingAppointments(SUBJECT_PATIENT_ID);
CREATE INDEX IDX_ACCT ON #UpcomingAppointments(ACCOUNT_ID);

-- ============================================================================
-- STEP 2: Get Subject Patient Details (Guarantor + Address)
-- ============================================================================
SELECT 
    ua.SUBJECT_PATIENT_ID,
    ua.CSN,
    ua.APPT_DTTM,
    ua.VACCINE_LOCATION,
    ua.ACCOUNT_ID,
    acct.ACCOUNT_NAME AS GUARANTOR_NAME,
    acctAddr.ADDRESS AS GUARANTOR_ADDRESS,
    acct.ZIP AS GUARANTOR_ZIP,
    subjectPat.ADD_LINE_1 AS SUBJECT_PATIENT_ADDRESS,
    subjectPat.ZIP AS SUBJECT_PATIENT_ZIP
INTO #SubjectPatients
FROM #UpcomingAppointments ua
INNER JOIN ACCOUNT acct
    ON ua.ACCOUNT_ID = acct.ACCOUNT_ID
INNER JOIN ACCT_ADDR acctAddr
    ON acct.ACCOUNT_ID = acctAddr.ACCOUNT_ID
    AND acctAddr.ADDRESS_LINE = 1
INNER JOIN PATIENT subjectPat
    ON ua.SUBJECT_PATIENT_ID = subjectPat.PAT_ID
WHERE UPPER(acct.ACCOUNT_NAME) NOT LIKE '%COUNTY%'
  AND UPPER(acct.ACCOUNT_NAME) NOT LIKE 'CSB,%'
  AND UPPER(acct.ACCOUNT_NAME) NOT LIKE '%,CSB'
  AND UPPER(acct.ACCOUNT_NAME) NOT LIKE '%, CSB'
  AND UPPER(acct.ACCOUNT_NAME) NOT LIKE '%CARING,FOR KIDS%'
  AND acctAddr.ADDRESS IS NOT NULL
  AND subjectPat.ADD_LINE_1 IS NOT NULL;

CREATE INDEX IDX_SUBJECT ON #SubjectPatients(SUBJECT_PATIENT_ID);
CREATE INDEX IDX_ACCT ON #SubjectPatients(ACCOUNT_ID);

-- ============================================================================
-- STEP 3: Find Household Members (3-Tier Matching)
-- ============================================================================

-- 3A: HIGH Confidence - PAT_RELATIONSHIPS
SELECT DISTINCT
    sp.SUBJECT_PATIENT_ID,
    sp.CSN,
    sp.APPT_DTTM,
    sp.VACCINE_LOCATION,
    sp.GUARANTOR_NAME,
    pr_sibling.PAT_ID AS SIBLING_ID,
    'HIGH  ' AS CONFIDENCE_LEVEL,
    1 AS CONFIDENCE_RANK
INTO #AllMatches
FROM #SubjectPatients sp
INNER JOIN PAT_RELATIONSHIPS pr_subject
    ON sp.SUBJECT_PATIENT_ID = pr_subject.PAT_ID
    AND pr_subject.LINE = 1
    AND pr_subject.PAT_REL_LGL_GUAR_YN = 'Y'
    AND pr_subject.PAT_REL_NAME IS NOT NULL
    AND UPPER(pr_subject.PAT_REL_NAME) NOT LIKE '%COUNTY%'
    AND UPPER(pr_subject.PAT_REL_NAME) NOT LIKE 'CSB,%'
    AND UPPER(pr_subject.PAT_REL_NAME) NOT LIKE '%,CSB'
    AND UPPER(pr_subject.PAT_REL_NAME) NOT LIKE '%, CSB'
    AND UPPER(pr_subject.PAT_REL_NAME) NOT LIKE '%CARING, FOR KIDS%'
INNER JOIN PAT_RELATIONSHIPS pr_sibling
    ON pr_sibling.LINE = 1
    AND CONCAT_WS('|', 
        pr_sibling.PAT_REL_NAME,
        pr_sibling.PAT_REL_HOME_PHONE,
        pr_sibling.PAT_REL_MOBILE_PHNE
    ) = CONCAT_WS('|',
        pr_subject.PAT_REL_NAME,
        pr_subject.PAT_REL_HOME_PHONE,
        pr_subject.PAT_REL_MOBILE_PHNE
    )
    AND pr_sibling.PAT_ID != sp.SUBJECT_PATIENT_ID
    AND pr_sibling.PAT_REL_LGL_GUAR_YN = 'Y'
    AND UPPER(pr_sibling.PAT_REL_NAME) NOT LIKE '%COUNTY%'
    AND UPPER(pr_sibling.PAT_REL_NAME) NOT LIKE 'CSB,%'
    AND UPPER(pr_sibling.PAT_REL_NAME) NOT LIKE '%,CSB'
    AND UPPER(pr_sibling.PAT_REL_NAME) NOT LIKE '%, CSB'
    AND UPPER(pr_sibling.PAT_REL_NAME) NOT LIKE '%CARING, FOR KIDS%'
WHERE 
    -- CRITICAL: Exclude siblings who already have their own appointments
    -- This is true agentic AI - don't suggest piggybacking for siblings already scheduled
    NOT EXISTS (
        SELECT 1 
        FROM #UpcomingAppointments sibling_appt
        WHERE sibling_appt.SUBJECT_PATIENT_ID = pr_sibling.PAT_ID
    );

    select count(*) as ALL_HIGH_CONFIDENCE_MATCHES, count(distinct SUBJECT_PATIENT_ID) as DISTINCT_SUJECT_PATIENTS, 
    count(distinct SIBLING_ID) as DISTINCT_SIBLINGS from #AllMatches

-- 3B: MEDIUM Confidence - Same Guarantor + Same Address
INSERT INTO #AllMatches
SELECT DISTINCT
    sp.SUBJECT_PATIENT_ID,
    sp.CSN,
    sp.APPT_DTTM,
    sp.VACCINE_LOCATION,
    sp.GUARANTOR_NAME,
    siblingPat.PAT_ID AS SIBLING_ID,
    'MEDIUM' AS CONFIDENCE_LEVEL,
    2 AS CONFIDENCE_RANK
FROM #SubjectPatients sp
INNER JOIN (
    SELECT DISTINCT PAT_ID, ACCOUNT_ID
    FROM F_SCHED_APPT
    WHERE ACCOUNT_ID IS NOT NULL
) siblingAppts
    ON siblingAppts.ACCOUNT_ID = sp.ACCOUNT_ID
    AND siblingAppts.PAT_ID != sp.SUBJECT_PATIENT_ID
INNER JOIN PATIENT siblingPat
    ON siblingPat.PAT_ID = siblingAppts.PAT_ID
    AND siblingPat.BIRTH_DATE > DATEADD(YEAR, -22, GETDATE())
    AND siblingPat.ADD_LINE_1 = sp.SUBJECT_PATIENT_ADDRESS
    AND siblingPat.ZIP = sp.SUBJECT_PATIENT_ZIP
WHERE 
    -- CRITICAL: Exclude siblings who already have their own appointments
    NOT EXISTS (
        SELECT 1 
        FROM #UpcomingAppointments sibling_appt
        WHERE sibling_appt.SUBJECT_PATIENT_ID = siblingPat.PAT_ID
    );

-- 3C: LOW Confidence - Same Guarantor, Different Address
INSERT INTO #AllMatches
SELECT DISTINCT
    sp.SUBJECT_PATIENT_ID,
    sp.CSN,
    sp.APPT_DTTM,
    sp.VACCINE_LOCATION,
    sp.GUARANTOR_NAME,
    siblingPat.PAT_ID AS SIBLING_ID,
    'LOW   ' AS CONFIDENCE_LEVEL,
    3 AS CONFIDENCE_RANK
FROM #SubjectPatients sp
INNER JOIN (
    SELECT DISTINCT PAT_ID, ACCOUNT_ID
    FROM F_SCHED_APPT
    WHERE ACCOUNT_ID IS NOT NULL
) siblingAppts
    ON siblingAppts.ACCOUNT_ID = sp.ACCOUNT_ID
    AND siblingAppts.PAT_ID != sp.SUBJECT_PATIENT_ID
INNER JOIN PATIENT siblingPat
    ON siblingPat.PAT_ID = siblingAppts.PAT_ID
    AND siblingPat.BIRTH_DATE > DATEADD(YEAR, -22, GETDATE())
    AND (siblingPat.ADD_LINE_1 != sp.SUBJECT_PATIENT_ADDRESS 
         OR siblingPat.ZIP != sp.SUBJECT_PATIENT_ZIP
         OR siblingPat.ADD_LINE_1 IS NULL
         OR siblingPat.ZIP IS NULL)
WHERE 
    -- CRITICAL: Exclude siblings who already have their own appointments
    NOT EXISTS (
        SELECT 1 
        FROM #UpcomingAppointments sibling_appt
        WHERE sibling_appt.SUBJECT_PATIENT_ID = siblingPat.PAT_ID
    );

-- Deduplicate to highest confidence only
SELECT *
INTO #HouseholdMembers
FROM (
    SELECT 
        SUBJECT_PATIENT_ID,
        SIBLING_ID,
        CSN,
        APPT_DTTM,
        VACCINE_LOCATION,
        GUARANTOR_NAME,
        CONFIDENCE_LEVEL,
        CONFIDENCE_RANK,
        ROW_NUMBER() OVER (
            PARTITION BY SUBJECT_PATIENT_ID, SIBLING_ID 
            ORDER BY CONFIDENCE_RANK
        ) AS RN
    FROM #AllMatches
) deduped
WHERE RN = 1;

CREATE INDEX IDX_SIBLING ON #HouseholdMembers(SIBLING_ID);

-- ============================================================================
-- STEP 4: Check Flu Vaccine Status (HMT + Historical)
-- ============================================================================
SELECT 
    hm.SUBJECT_PATIENT_ID,
    hm.SIBLING_ID,
    hm.CSN,
    hm.APPT_DTTM,
    hm.VACCINE_LOCATION,
    hm.CONFIDENCE_LEVEL,
    
    -- Sibling demographics
    siblingPat.BIRTH_DATE AS SIBLING_BIRTH_DATE,
    DATEDIFF(YEAR, siblingPat.BIRTH_DATE, GETDATE()) - 
        CASE WHEN DATEADD(YEAR, DATEDIFF(YEAR, siblingPat.BIRTH_DATE, GETDATE()), siblingPat.BIRTH_DATE) > GETDATE() 
        THEN 1 ELSE 0 END AS SIBLING_AGE_YEARS,
    DATEDIFF(MONTH, siblingPat.BIRTH_DATE, GETDATE()) AS SIBLING_AGE_MONTHS,
    
    -- Current HMT Status (PATIENT_HMT_STATUS - current snapshot)
    hmt_current.HMT_DUE_STATUS_C AS CURRENT_STATUS_C,
    status_current.NAME AS CURRENT_STATUS_NAME,
    hmt_current.IDEAL_RETURN_DT,
    
    -- Historical Flu Vaccine Data (HM_HISTORICAL_STATUS)
    hmt_hist.LAST_COMPLETED_DATE AS LAST_FLU_VACCINE_DATE,
    hmt_hist.NEXT_DUE_DATE,
    hmt_hist.HAS_OUTSIDE_COMPLETION_YN,
    
    -- Check if patient has EVER had flu vaccine (for exclusion rule)
    CASE 
        WHEN hmt_hist.LAST_COMPLETED_DATE IS NOT NULL THEN 1
        ELSE 0
    END AS HAS_LIFETIME_FLU_VACCINE,
    
    -- Check for asthma diagnosis (J45* ICD codes)
    CASE 
        WHEN EXISTS (
            SELECT 1 
            FROM PAT_ENC_DX dx
            INNER JOIN CLARITY_EDG edg ON dx.DX_ID = edg.DX_ID
            WHERE dx.PAT_ID = hm.SIBLING_ID
              AND edg.CURRENT_ICD10_LIST LIKE 'J45%'
        ) THEN 1
        ELSE 0
    END AS HAS_ASTHMA

INTO #SiblingFluStatus
FROM #HouseholdMembers hm
INNER JOIN PATIENT siblingPat
    ON hm.SIBLING_ID = siblingPat.PAT_ID
-- Current HMT Status
LEFT JOIN PATIENT_HMT_STATUS hmt_current
    ON hm.SIBLING_ID = hmt_current.PAT_ID
    AND hmt_current.QUALIFIED_HMT_ID IN (20, 24)  -- Flu vaccine topics
LEFT JOIN ZC_HMT_DUE_STATUS status_current
    ON hmt_current.HMT_DUE_STATUS_C = status_current.HMT_DUE_STATUS_C
-- Historical HMT Data (most recent)
LEFT JOIN (
    SELECT 
        PAT_ID,
        HM_TOPIC_ID,
        LAST_COMPLETED_DATE,
        NEXT_DUE_DATE,
        HAS_OUTSIDE_COMPLETION_YN,
        ROW_NUMBER() OVER (
            PARTITION BY PAT_ID, HM_TOPIC_ID 
            ORDER BY SNAPSHOT_DATE DESC
        ) AS RN
    FROM HM_HISTORICAL_STATUS
    WHERE HM_TOPIC_ID IN (20, 24)  -- Flu vaccine
) hmt_hist
    ON hm.SIBLING_ID = hmt_hist.PAT_ID
    AND hmt_hist.RN = 1  -- Most recent historical record
WHERE 
    -- Filter to overdue flu vaccines only
    hmt_current.HMT_DUE_STATUS_C IN (2, 3, 4)  -- Due Soon, Due On, Overdue
    -- Critical exclusion from slide 7: Age > 24 months with NO lifetime flu vaccines
    AND NOT (
        DATEDIFF(MONTH, siblingPat.BIRTH_DATE, GETDATE()) > 24
        AND hmt_hist.LAST_COMPLETED_DATE IS NULL
    );

-- ============================================================================
-- STEP 5: Create Final Output (Excel/CSV format from slide 8)
-- ============================================================================
SELECT 
    -- Subject patient (has appointment)
    subjectPat_idd.IDENTITY_ID AS SUBJECT_MRN,
    subjectPat.PAT_NAME AS SUBJECT_NAME,
    sfs.CSN,
    sfs.APPT_DTTM AS UPCOMING_APPOINTMENT_DATE,
    sfs.VACCINE_LOCATION AS UPCOMING_APPOINTMENT_LOCATION,
    
    -- Sibling (needs flu vaccine)
    siblingPat_idd.IDENTITY_ID AS MRN,
    siblingPat.PAT_NAME AS SIBLING_NAME,
    sfs.SIBLING_AGE_YEARS AS AGE_YEARS,
    sfs.SIBLING_AGE_MONTHS AS AGE_MONTHS,
    sex.NAME AS SEX,
    
    -- Patient or shared address?
    CASE 
        WHEN sfs.SIBLING_ID = sfs.SUBJECT_PATIENT_ID THEN 'Patient'
        ELSE 'Shared Address'
    END AS PATIENT_OR_SHARED_ADDRESS,
    
    -- MyChart status
    mychartStatus.NAME AS MYCHART_ACTIVE,
    
    -- Mobile number on file
    CASE 
        WHEN siblingPat.HOME_PHONE IS NOT NULL OR mobilePhone.OTHER_COMMUNIC_NUM IS NOT NULL THEN 'Y'
        ELSE 'N'
    END AS MOBILE_NUMBER_ON_FILE,
    
    -- Last flu vaccine date
    sfs.LAST_FLU_VACCINE_DATE AS LAST_ADMINISTERED_INFLUENZA_VACCINE_DATE,
    
    -- Asthma flag (for J45* ICD codes)
    CASE 
        WHEN sfs.HAS_ASTHMA = 1 THEN 'Y'
        ELSE 'N'
    END AS ANY_ICD_CODE_J45,
    
    -- Match confidence
    RTRIM(sfs.CONFIDENCE_LEVEL) AS CONFIDENCE_LEVEL,
    
    -- LLM Prompt Context (for message generation)
    CONCAT(
        'PATIENT_NAME:', siblingPat.PAT_NAME, ';',
        'LAST_VACCINE_DATE:', ISNULL(CONVERT(VARCHAR, sfs.LAST_FLU_VACCINE_DATE, 107), 'Never'), ';',
        'HAS_ASTHMA:', CASE WHEN sfs.HAS_ASTHMA = 1 THEN 'Yes' ELSE 'No' END, ';',
        'APPT_LOCATION:', sfs.VACCINE_LOCATION, ';',
        'APPT_DATE:', CONVERT(VARCHAR, sfs.APPT_DTTM, 107), ';',
        'PATIENT_OR_SHARED:', CASE WHEN sfs.SIBLING_ID = sfs.SUBJECT_PATIENT_ID THEN 'Patient' ELSE 'Shared' END
    ) AS LLM_PROMPT_CONTEXT

INTO #FluOpportunities
FROM #SiblingFluStatus sfs
INNER JOIN PATIENT subjectPat
    ON sfs.SUBJECT_PATIENT_ID = subjectPat.PAT_ID
INNER JOIN PATIENT siblingPat
    inner join OTHER_COMMUNCTN mobilePhone 
        on siblingPat.PAT_ID = mobilePhone.PAT_ID and mobilePhone.OTHER_COMMUNIC_C = '1' --mobile phone
    ON sfs.SIBLING_ID = siblingPat.PAT_ID
INNER JOIN IDENTITY_ID subjectPat_idd
    ON subjectPat.PAT_ID = subjectPat_idd.PAT_ID
    AND subjectPat_idd.IDENTITY_TYPE_ID = 40
INNER JOIN IDENTITY_ID siblingPat_idd
    ON siblingPat.PAT_ID = siblingPat_idd.PAT_ID
    AND siblingPat_idd.IDENTITY_TYPE_ID = 40
LEFT JOIN PATIENT_MYC mychart 
    INNER JOIN ZC_MYCHART_STATUS mychartStatus 
        ON mychart.MYCHART_STATUS_C = mychartStatus.MYCHART_STATUS_C
    ON siblingPat.PAT_ID = mychart.PAT_ID
LEFT JOIN ZC_SEX sex
    ON siblingPat.SEX_C = sex.RCPT_MEM_SEX_C;

-- ============================================================================
-- FINAL OUTPUT (Excel/CSV format from slide 8)
-- ============================================================================
SELECT 
    MRN,
    PATIENT_OR_SHARED_ADDRESS,
    MYCHART_ACTIVE,
    MOBILE_NUMBER_ON_FILE,
    UPCOMING_APPOINTMENT_DATE,
    UPCOMING_APPOINTMENT_LOCATION,
    LAST_ADMINISTERED_INFLUENZA_VACCINE_DATE,
    ANY_ICD_CODE_J45,
    
    -- LLM will generate message using this context
    LLM_PROMPT_CONTEXT,
    
    -- Additional context for analytics
    CONFIDENCE_LEVEL,
    SUBJECT_MRN,
    AGE_YEARS,
    SEX

FROM #FluOpportunities
ORDER BY 
    UPCOMING_APPOINTMENT_DATE,
    CASE PATIENT_OR_SHARED_ADDRESS 
        WHEN 'Patient' THEN 1 
        ELSE 2 
    END,
    MRN;

-- ============================================================================
-- VALIDATION METRICS (for demo)
-- ============================================================================

-- Core metrics
SELECT 
    'Total Opportunities (Rows)' AS METRIC,
    COUNT(*) AS VALUE
FROM #FluOpportunities

UNION ALL

SELECT 
    'Unique Subject Patients',
    COUNT(DISTINCT SUBJECT_MRN)
FROM #FluOpportunities

UNION ALL

SELECT 
    'Unique Siblings',
    COUNT(DISTINCT MRN)
FROM #FluOpportunities

UNION ALL

SELECT 
    'Avg Siblings per Subject',
    CAST(COUNT(*) * 1.0 / NULLIF(COUNT(DISTINCT SUBJECT_MRN), 0) AS DECIMAL(5,2))
FROM #FluOpportunities

UNION ALL

SELECT 
    'Patient Appointments (Self)',
    COUNT(*)
FROM #FluOpportunities
WHERE PATIENT_OR_SHARED_ADDRESS = 'Patient'

UNION ALL

SELECT 
    'Shared Address (Siblings)',
    COUNT(*)
FROM #FluOpportunities
WHERE PATIENT_OR_SHARED_ADDRESS = 'Shared Address'

UNION ALL

SELECT 
    'MyChart Active',
    COUNT(*)
FROM #FluOpportunities
WHERE MYCHART_ACTIVE = 'Activated'

UNION ALL

SELECT 
    'Has Asthma (J45*)',
    COUNT(*)
FROM #FluOpportunities
WHERE ANY_ICD_CODE_J45 = 'Y'

UNION ALL

SELECT 
    'HIGH Confidence Matches',
    COUNT(*)
FROM #FluOpportunities
WHERE CONFIDENCE_LEVEL = 'HIGH';

-- AGENTIC AI VALUE: Show how many siblings were excluded
-- These are siblings who have their own appointments (not true piggybacking opportunities)
SELECT 
    'AGENTIC EXCLUSIONS' AS METRIC_TYPE,
    'Siblings with Own Appointments (Excluded)' AS METRIC,
    COUNT(DISTINCT sibling_with_appt.SUBJECT_PATIENT_ID) AS VALUE
FROM #HouseholdMembers hm
INNER JOIN #UpcomingAppointments sibling_with_appt
    ON hm.SIBLING_ID = sibling_with_appt.SUBJECT_PATIENT_ID
-- These siblings were in household matches but excluded because they have appointments

UNION ALL

SELECT 
    'AGENTIC EXCLUSIONS',
    'Would-Be Duplicate Messages Prevented',
    COUNT(DISTINCT hm.SIBLING_ID)
FROM #HouseholdMembers hm
INNER JOIN #UpcomingAppointments sibling_with_appt
    ON hm.SIBLING_ID = sibling_with_appt.SUBJECT_PATIENT_ID;

-- Verify no duplicate subject-sibling pairs
SELECT 
    'DATA QUALITY' AS METRIC_TYPE,
    'Duplicate Subject-Sibling Pairs' AS CHECK_TYPE,
    COUNT(*) AS DUPLICATE_COUNT
FROM (
    SELECT SUBJECT_MRN, MRN, COUNT(*) AS CNT
    FROM #FluOpportunities
    GROUP BY SUBJECT_MRN, MRN
    HAVING COUNT(*) > 1
) dupes;

-- Clean up
DROP TABLE IF EXISTS #AllAppointments;
DROP TABLE IF EXISTS #UpcomingAppointments;
DROP TABLE IF EXISTS #SubjectPatients;
DROP TABLE IF EXISTS #AllMatches;
DROP TABLE IF EXISTS #HouseholdMembers;
DROP TABLE IF EXISTS #SiblingFluStatus;
DROP TABLE IF EXISTS #FluOpportunities;

/*******************************************************************************
 * LLM MESSAGE GENERATION PROMPT (from slide 8):
 * 
 * Use cheerful and positive but professional language. Remind patients that 
 * they did a great job getting their flu vaccine for the previous season on 
 * <LAST_ADMINISTERED_INFLUENZA_VACCINE_DATE>, but that each vaccine only 
 * provides one year of protection. 
 * 
 * If <ANY_ICD_CODE_J45> == Y, remind that asthma puts them at higher risk for 
 * severe influenza. 
 * 
 * Remind them that they have an appointment coming up at 
 * <UPCOMING_APPOINTMENT_LOCATION> on <UPCOMING_APPOINTMENT_DATE> and that they 
 * can get their flu shot while there. 
 * 
 * Limit to 160 characters whenever possible.
 * 
 * If <PATIENT_OR_SHARED_ADDRESS> == "Shared Address" frame the message instead 
 * as "someone in your household has an appointment" rather than "you have an 
 * appointment."
 * 
 * EXAMPLE LLM PROMPT:
 * Generate a 160-character SMS message for flu vaccine outreach.
 * Context: {LLM_PROMPT_CONTEXT}
 * Requirements: Cheerful, professional, mention last vaccine date, asthma risk if applicable, appointment details.
 * 
 ******************************************************************************/