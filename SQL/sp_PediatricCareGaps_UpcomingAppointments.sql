-- =====================================================
-- PEDIATRIC CARE GAPS - APPOINTMENT-BASED (v3.1 CORRECTED)
-- Using Correct Epic Clarity Patterns + @KeepTempTables
-- Database: CHMCA_CUSTOM
-- =====================================================

USE [CHMCA_CUSTOM]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE OR ALTER PROCEDURE [dbo].[sp_PediatricCareGaps_UpcomingAppointments]
    @MonthsAhead INT = 12,
    @ProviderID VARCHAR(18) = NULL,
    @DepartmentID INT = NULL,
    @PriorityLevel INT = NULL,
    @KeepTempTables BIT = 0  -- NEW: Preserve temp tables for staging reuse
AS

/*
**************************************************************************************************
TITLE:  Pediatric Care Gaps - Appointment-Based Gap Closure System (v3.1 CORRECTED)
PURPOSE: 
    Identify pediatric care gaps and link them to upcoming appointments for proactive closure.
    Uses CORRECT Epic Clarity table patterns (SmartData, Flowsheets, Health Maintenance).

NEW IN v3.1:
    • Fixed Epic Clarity table patterns (SMRTDTA_*, IP_FLWSHT_*, etc.)
    • @KeepTempTables parameter for staging procedure reuse
    • Appointment-based prioritization
    • All 28+ care gap categories

CARE GAP SECTIONS (28+ Gaps):
    1. Incomplete Orders
    2. Well-Child Visits (Age-based)
    3. Developmental Screening (M-CHAT, ASQ, PEDS)
    4. Adolescent Depression Screening (PHQ-9/PHQ-A)
    5. BMI Documentation & Counseling
    6. Lead Screening (12 & 24 months)
    7. Vision Screening
    8. Hearing Screening
    9. Blood Pressure Screening
   10. Diabetes Management (Retinopathy, HbA1c, Nephropathy)
   11. Graves Disease (TSH monitoring)
   12. Asthma Control & Action Plans
   13. ADHD Medication Follow-up
   14. Immunizations (Flu, HPV, Routine)
   15. Adolescent Health (Chlamydia, Substance Abuse)
   16. Lipid Screening
   17. Transition to Adult Care
   18. ED/Hospital Follow-up

USAGE:
    -- Normal use (drops temp tables)
    EXEC sp_PediatricCareGaps_UpcomingAppointments @MonthsAhead = 3
    
    -- For staging (preserves temp tables)
    EXEC sp_PediatricCareGaps_UpcomingAppointments @MonthsAhead = 12, @KeepTempTables = 1

**************************************************************************************************
*/

SET NOCOUNT ON;

PRINT '========================================';
PRINT 'PEDIATRIC CARE GAPS - CORRECTED v3.1';
PRINT '========================================';
PRINT 'Parameters:';
PRINT '  Months Ahead: ' + CAST(@MonthsAhead AS VARCHAR);
PRINT '  Provider Filter: ' + ISNULL(@ProviderID, 'All');
PRINT '  Department Filter: ' + ISNULL(CAST(@DepartmentID AS VARCHAR), 'All');
PRINT '  Priority Filter: ' + ISNULL(CAST(@PriorityLevel AS VARCHAR), 'All');
PRINT '  Keep Temp Tables: ' + CASE WHEN @KeepTempTables = 1 THEN 'YES' ELSE 'NO' END;
PRINT '========================================';
PRINT '';

-- =====================================================
-- STEP 1: BUILD PATIENT BASE (CORRECT PATTERN)
-- =====================================================

PRINT 'Building patient base...';

-- Check if #PatientBase already exists (called by staging procedure)
DECLARE @PatientBaseExists BIT = 0;
IF OBJECT_ID('tempdb..#PatientBase') IS NOT NULL 
    SET @PatientBaseExists = 1;

-- If temp table doesn't exist, create it
IF @PatientBaseExists = 0
BEGIN
    CREATE TABLE #PatientBase (
        PAT_ID VARCHAR(18),
        PAT_MRN_ID VARCHAR(50),
        PAT_NAME VARCHAR(200),
        BIRTH_DATE DATETIME,
        AGE_MONTHS INT,
        AGE_YEARS INT,
        SEX_C INT,
        SEX VARCHAR(50),
        PCP_NAME VARCHAR(200),
        PCP_ID VARCHAR(18),
        PCP_DEPARTMENT VARCHAR(200),
        PCP_DEPARTMENT_ID INT,
        PCP_CARE_TEAM_NAME VARCHAR(200),
        ADD_LINE_1 VARCHAR(200),
        CITY VARCHAR(100),
        STATE_C VARCHAR(10),
        ZIP VARCHAR(20),
        HOME_PHONE VARCHAR(50),
        EMAIL_ADDRESS VARCHAR(100)
    );
END

-- Populate (whether created now or already exists)
INSERT INTO #PatientBase (
    PAT_ID, PAT_MRN_ID, PAT_NAME, BIRTH_DATE, AGE_MONTHS, AGE_YEARS,
    SEX_C, SEX, PCP_NAME, PCP_ID, PCP_DEPARTMENT, PCP_DEPARTMENT_ID,
    PCP_CARE_TEAM_NAME, ADD_LINE_1, CITY, STATE_C, ZIP, HOME_PHONE, EMAIL_ADDRESS
)
SELECT 
    pat.PAT_ID,
    pat.PAT_MRN_ID,
    pat.PAT_NAME,
    pat.BIRTH_DATE,
    CHMCA_CUSTOM.dbo.FN_ACH_AGE_IN_MONTHS(pat.BIRTH_DATE, GETDATE()) AS AGE_MONTHS,
    FLOOR(DATEDIFF(DAY, pat.BIRTH_DATE, GETDATE()) / 365.25) AS AGE_YEARS,
    pat.SEX_C,
    zs.NAME AS SEX,
    ser.PROV_NAME AS PCP_NAME,
    ser.PROV_ID AS PCP_ID,
    dep.DEPARTMENT_NAME AS PCP_DEPARTMENT,
    dep.DEPARTMENT_ID AS PCP_DEPARTMENT_ID,
    SUBGRP.SUBGROUP_NAME AS PCP_CARE_TEAM_NAME,
    pat.ADD_LINE_1,
    pat.CITY,
    pat.STATE_C,
    pat.ZIP,
    pat.HOME_PHONE,
    pat.EMAIL_ADDRESS
--INTO #PatientBase
FROM CLARITY..PATIENT pat
INNER JOIN CLARITY..CLARITY_SER ser ON pat.CUR_PCP_PROV_ID = ser.PROV_ID 
    AND ser.REFERRAL_SOURCE_TYPE_C = '1'
    AND ser.ACTIVE_STATUS_C = '1'
    AND ser.PROVIDER_TYPE_C IN ('1','6','9','113','3005')
INNER JOIN CLARITY..CLARITY_SER_DEPT serdep ON ser.PROV_ID = serdep.PROV_ID AND serdep.LINE = '1'
LEFT OUTER JOIN CLARITY..SUBGROUP SUBGRP ON serdep.TEAM_SUBGROUP_ID = SUBGRP.SUBGROUP_ID
INNER JOIN CLARITY..CLARITY_DEP dep ON serdep.DEPARTMENT_ID = dep.DEPARTMENT_ID
    AND dep.RPT_GRP_TEN = 1
LEFT JOIN CLARITY..ZC_SEX zs ON pat.SEX_C = zs.RCPT_MEM_SEX_C
WHERE pat.DEATH_DATE IS NULL
    AND FLOOR(DATEDIFF(DAY, pat.BIRTH_DATE, GETDATE()) / 365.25) < 21
    --AND (@ProviderID IS NULL OR ser.PROV_ID = @ProviderID);

IF @PatientBaseExists = 0
BEGIN
CREATE INDEX IX_PatientBase_PAT_ID ON #PatientBase(PAT_ID);
CREATE INDEX IX_PatientBase_PCP ON #PatientBase(PCP_ID);
CREATE INDEX IX_PatientBase_AGE ON #PatientBase(AGE_YEARS, AGE_MONTHS);
END

DECLARE @PatientCount INT;
SELECT @PatientCount = COUNT(*) FROM #PatientBase;
PRINT 'Patient Base: ' + CAST(@PatientCount AS VARCHAR) + ' patients';

-- =====================================================
-- STEP 2: GET UPCOMING APPOINTMENTS
-- =====================================================

PRINT 'Getting upcoming appointments...';

DECLARE @ApptsExist BIT = 0;
IF OBJECT_ID('tempdb..#UpcomingAppointments') IS NOT NULL 
    SET @ApptsExist = 1;

IF @ApptsExist = 0
BEGIN
    CREATE TABLE #UpcomingAppointments (
        PAT_ID VARCHAR(18),
        PAT_ENC_CSN_ID NUMERIC(18,0),
        APPT_DATE DATETIME,
        APPT_LENGTH_MINUTES INT,
        DEPARTMENT_ID INT,
        DEPARTMENT_NAME VARCHAR(200),
        APPT_PROVIDER_ID VARCHAR(18),
        APPT_PROVIDER_NAME VARCHAR(200),
        APPT_CARE_TEAM VARCHAR(200),
        ENC_TYPE_C INT,
        ENCOUNTER_TYPE VARCHAR(100),
        APPT_STATUS_C INT,
        APPT_STATUS VARCHAR(100),
        DAYS_UNTIL_APPT INT,
        APPT_TIMEFRAME VARCHAR(50),
        APPT_RANK INT
    );
END

INSERT INTO #UpcomingAppointments
SELECT 
    pe.PAT_ID,
    pe.PAT_ENC_CSN_ID,
    pe.CONTACT_DATE AS APPT_DATE,
    pe.APPT_LENGTH AS APPT_LENGTH_MINUTES,
    pe.DEPARTMENT_ID,
    dep.DEPARTMENT_NAME,
    pe.VISIT_PROV_ID AS APPT_PROVIDER_ID,
    prov.PROV_NAME AS APPT_PROVIDER_NAME,
    SUBGRP.SUBGROUP_NAME AS APPT_CARE_TEAM,
    pe.ENC_TYPE_C,
    enctype.NAME AS ENCOUNTER_TYPE,
    pe.APPT_STATUS_C,
    apptstatus.NAME AS APPT_STATUS,
    DATEDIFF(DAY, GETDATE(), pe.CONTACT_DATE) AS DAYS_UNTIL_APPT,
    CASE 
        WHEN DATEDIFF(DAY, GETDATE(), pe.CONTACT_DATE) <= 7 THEN '1. Next 7 Days'
        WHEN DATEDIFF(DAY, GETDATE(), pe.CONTACT_DATE) <= 30 THEN '2. Next 30 Days'
        WHEN DATEDIFF(DAY, GETDATE(), pe.CONTACT_DATE) <= 90 THEN '3. Next 3 Months'
        WHEN DATEDIFF(DAY, GETDATE(), pe.CONTACT_DATE) <= 180 THEN '4. Next 6 Months'
        ELSE '5. Beyond 6 Months'
    END AS APPT_TIMEFRAME,
    ROW_NUMBER() OVER (PARTITION BY pe.PAT_ID ORDER BY pe.CONTACT_DATE ASC) AS APPT_RANK
--INTO #UpcomingAppointments
FROM CLARITY..PAT_ENC pe
INNER JOIN #PatientBase pb ON pe.PAT_ID = pb.PAT_ID
INNER JOIN CLARITY..CLARITY_DEP dep ON pe.DEPARTMENT_ID = dep.DEPARTMENT_ID AND dep.RPT_GRP_TEN = 1
LEFT JOIN CLARITY..CLARITY_SER prov ON pe.VISIT_PROV_ID = prov.PROV_ID
LEFT JOIN CLARITY..CLARITY_SER_DEPT serdep ON prov.PROV_ID = serdep.PROV_ID AND serdep.LINE = '1'
LEFT OUTER JOIN CLARITY..SUBGROUP SUBGRP ON serdep.TEAM_SUBGROUP_ID = SUBGRP.SUBGROUP_ID
LEFT JOIN CLARITY..ZC_DISP_ENC_TYPE enctype ON pe.ENC_TYPE_C = enctype.DISP_ENC_TYPE_C
LEFT JOIN CLARITY..ZC_APPT_STATUS apptstatus ON pe.APPT_STATUS_C = apptstatus.APPT_STATUS_C
WHERE pe.CONTACT_DATE > GETDATE()
    AND pe.CONTACT_DATE <= DATEADD(MONTH, @MonthsAhead, GETDATE())
    AND pe.ENC_TYPE_C IN ('101','50')
    AND pe.APPT_STATUS_C NOT IN ('3','4','5')
    AND (@DepartmentID IS NULL OR pe.DEPARTMENT_ID = @DepartmentID);

IF @ApptsExist = 0
BEGIN
CREATE INDEX IX_UpcomingAppts_PAT_ID ON #UpcomingAppointments(PAT_ID);
CREATE INDEX IX_UpcomingAppts_DATE ON #UpcomingAppointments(APPT_DATE);
END

DECLARE @ApptCount INT;
SELECT @ApptCount = COUNT(*) FROM #UpcomingAppointments;
PRINT 'Upcoming Appointments: ' + CAST(@ApptCount AS VARCHAR);
PRINT '';

-- =====================================================
-- STEP 3: BUILD ALL CARE GAPS (CORRECTED PATTERNS)
-- =====================================================

PRINT 'Calculating care gaps...';

DECLARE @GapsExist BIT = 0;
IF OBJECT_ID('tempdb..#AllCareGaps') IS NOT NULL 
    SET @GapsExist = 1;

IF @GapsExist = 0
BEGIN
    CREATE TABLE #AllCareGaps (
        PAT_ID VARCHAR(18),
        GAP_TYPE VARCHAR(100),
        GAP_CATEGORY VARCHAR(50),
        GAP_DETAIL VARCHAR(500),
        DAYS_OPEN INT,
        GAP_DATE DATETIME,
        PRIORITY_LEVEL INT
    );
END

-- =====================================================
-- SECTION 1: INCOMPLETE ORDERS
-- =====================================================

PRINT 'Building Incomplete Orders...';

--IF OBJECT_ID('tempdb..#IncompleteOrders') IS NOT NULL DROP TABLE #IncompleteOrders;


-- =====================================================
-- GAP 1: INCOMPLETE ORDERS
-- =====================================================
INSERT INTO #AllCareGaps (PAT_ID, GAP_TYPE, GAP_CATEGORY, GAP_DETAIL, DAYS_OPEN, GAP_DATE, PRIORITY_LEVEL)

SELECT DISTINCT
    pb.PAT_ID,
    'Incomplete Order' AS GAP_TYPE,
    'Orders & Referrals' AS GAP_CATEGORY,
    op.DESCRIPTION AS GAP_DETAIL,
    DATEDIFF(DAY, op.ORDERING_DATE, GETDATE()) AS DAYS_OPEN,
    op.ORDERING_DATE AS GAP_DATE,
    1 AS PRIORITY_LEVEL
--INTO #IncompleteOrders
FROM #PatientBase pb
INNER JOIN CLARITY..ORDER_PROC op 
	left join CLARITY..PAT_ENC enc 
		left join CLARITY..ZC_DISP_ENC_TYPE encType on enc.ENC_TYPE_C = encType.DISP_ENC_TYPE_C
		and encType.NAME NOT in ('Hospital Encounter','Hospital', 'External Hospital Admission',
	'Inpatient Consult/Documentation', 'Deleted','Erroneous Encounter','Erroneous Telephone Encounter')
	on op.PAT_ENC_CSN_ID = enc.PAT_ENC_CSN_ID
ON (pb.PAT_ID = op.PAT_ID
    AND op.ORDERING_DATE >= DATEADD(YEAR, -1, GETDATE())
    AND op.ORDER_STATUS_C IN ('1', '2')); -- Sent/Pending


-- =====================================================
-- SECTION 2: WELL-CHILD VISITS
-- =====================================================

PRINT 'Building Well-Child Visits...';

--IF OBJECT_ID('tempdb..#WellChildVisits') IS NOT NULL DROP TABLE #WellChildVisits;

INSERT INTO #AllCareGaps (PAT_ID, GAP_TYPE, GAP_CATEGORY, GAP_DETAIL, DAYS_OPEN, GAP_DATE, PRIORITY_LEVEL)

SELECT 
    pb.PAT_ID,
    'Overdue Well-Child Visit' AS GAP_TYPE,
    'Preventive Care' AS GAP_CATEGORY,
    CASE 
        WHEN pb.AGE_MONTHS < 1 THEN '3-6 Day Visit'
        WHEN pb.AGE_MONTHS = 1 THEN '1 Month Visit'
        WHEN pb.AGE_MONTHS = 2 THEN '2 Month Visit'
        WHEN pb.AGE_MONTHS BETWEEN 3 AND 5 THEN '4 Month Visit'
        WHEN pb.AGE_MONTHS BETWEEN 5 AND 7 THEN '6 Month Visit'
        WHEN pb.AGE_MONTHS BETWEEN 8 AND 10 THEN '9 Month Visit'
        WHEN pb.AGE_MONTHS BETWEEN 11 AND 13 THEN '12 Month Visit'
        WHEN pb.AGE_MONTHS BETWEEN 14 AND 16 THEN '15 Month Visit'
        WHEN pb.AGE_MONTHS BETWEEN 17 AND 20 THEN '18 Month Visit'
        WHEN pb.AGE_MONTHS BETWEEN 21 AND 27 THEN '24 Month Visit'
        WHEN pb.AGE_MONTHS BETWEEN 28 AND 41 THEN '30-36 Month Visit'
        WHEN pb.AGE_YEARS >= 3 THEN 'Annual Well Visit (Age ' + CAST(pb.AGE_YEARS AS VARCHAR) + ')'
    END AS GAP_DETAIL,
    DATEDIFF(DAY, last_wcv.LAST_WCV_DATE, GETDATE()) AS DAYS_OPEN,
    last_wcv.LAST_WCV_DATE AS GAP_DATE,
    2 AS PRIORITY_LEVEL
--INTO #WellChildVisits
FROM #PatientBase pb
LEFT JOIN (
    SELECT 
        pe.PAT_ID,
        MAX(pe.CONTACT_DATE) AS LAST_WCV_DATE
    FROM CLARITY..PAT_ENC pe
    INNER JOIN CLARITY..CLARITY_DEP dep ON pe.DEPARTMENT_ID = dep.DEPARTMENT_ID
        AND dep.RPT_GRP_TEN = 1
    LEFT JOIN CLARITY..PAT_ENC_DX pdd ON pe.PAT_ENC_CSN_ID = pdd.PAT_ENC_CSN_ID
    WHERE (pdd.DX_ID LIKE 'Z00.1%' OR pdd.DX_ID LIKE 'Z00.0%' OR pdd.DX_ID LIKE 'V20%')
        OR pe.ENC_TYPE_C IN ('101','50')
        AND pe.APPT_STATUS_C = '2' -- Completed
    GROUP BY pe.PAT_ID
) last_wcv ON pb.PAT_ID = last_wcv.PAT_ID
WHERE (
    (pb.AGE_MONTHS < 6 AND (last_wcv.LAST_WCV_DATE IS NULL OR DATEDIFF(MONTH, last_wcv.LAST_WCV_DATE, GETDATE()) > 2))
    OR (pb.AGE_MONTHS BETWEEN 6 AND 18 AND (last_wcv.LAST_WCV_DATE IS NULL OR DATEDIFF(MONTH, last_wcv.LAST_WCV_DATE, GETDATE()) > 3))
    OR (pb.AGE_MONTHS BETWEEN 19 AND 36 AND (last_wcv.LAST_WCV_DATE IS NULL OR DATEDIFF(MONTH, last_wcv.LAST_WCV_DATE, GETDATE()) > 6))
    OR (pb.AGE_YEARS >= 3 AND (last_wcv.LAST_WCV_DATE IS NULL OR DATEDIFF(MONTH, last_wcv.LAST_WCV_DATE, GETDATE()) > 15))
);

-- =====================================================
-- SECTION 3: DEVELOPMENTAL SCREENING (SMARTDATA + FLOWSHEETS)
-- =====================================================

PRINT 'Building Developmental Screening...';

--IF OBJECT_ID('tempdb..#DevelopmentalScreening') IS NOT NULL DROP TABLE #DevelopmentalScreening;
INSERT INTO #AllCareGaps (PAT_ID, GAP_TYPE, GAP_CATEGORY, GAP_DETAIL, DAYS_OPEN, GAP_DATE, PRIORITY_LEVEL)

SELECT 
    pb.PAT_ID,
    'Missing Developmental Screening' AS GAP_TYPE,
    'Developmental/Behavioral' AS GAP_CATEGORY,
    CASE 
        WHEN pb.AGE_MONTHS BETWEEN 9 AND 10 THEN '9 Month Screen Due'
        WHEN pb.AGE_MONTHS BETWEEN 18 AND 19 THEN '18 Month Screen Due (M-CHAT)'
        WHEN pb.AGE_MONTHS BETWEEN 24 AND 30 THEN '24-30 Month Screen Due (M-CHAT)'
    END AS GAP_DETAIL,
    NULL AS DAYS_OPEN,
    NULL AS GAP_DATE,
    2 AS PRIORITY_LEVEL
--INTO #DevelopmentalScreening
FROM #PatientBase pb
WHERE pb.AGE_MONTHS IN (9, 10, 18, 19, 24, 25, 26, 27, 28, 29, 30)
    AND NOT EXISTS (
        -- SmartData check
        SELECT 1
        FROM CLARITY..SMRTDTA_ELEM_DATA sed
        INNER JOIN CLARITY..SMRTDTA_ELEM_VALUE sev ON sed.HLV_ID = sev.HLV_ID
        INNER JOIN CLARITY..CLARITY_CONCEPT cc ON sed.ELEMENT_ID = cc.CONCEPT_ID
            AND cc.DATA_TYPE_C IN (3,4,11)
        WHERE sed.PAT_LINK_ID = pb.PAT_ID
            AND sed.CONTEXT_NAME = 'PATIENT'
            AND (cc.NAME LIKE '%M-CHAT%' OR cc.NAME LIKE '%ASQ%' OR cc.NAME LIKE '%PEDS%' OR cc.NAME LIKE '%DEVELOPMENTAL%')
            AND sed.CUR_VALUE_DATETIME >= DATEADD(MONTH, -6, GETDATE())
        
        UNION
        
        -- Flowsheet check (with IP_DATA_STORE for CSN)
        SELECT 1
        FROM CLARITY..IP_FLWSHT_MEAS ifm
        INNER JOIN CLARITY..IP_FLWSHT_REC ifr ON ifm.FSD_ID = ifr.FSD_ID
        INNER JOIN CLARITY..IP_FLO_GP_DATA ifg ON ifm.FLO_MEAS_ID = ifg.FLO_MEAS_ID
        WHERE ifr.PAT_ID = pb.PAT_ID
            AND ifg.VAL_TYPE_C IS NOT NULL
            AND (ifg.FLO_MEAS_NAME LIKE '%M-CHAT%' OR ifg.FLO_MEAS_NAME LIKE '%ASQ%' OR ifg.FLO_MEAS_NAME LIKE '%PEDS%')
            AND ifm.RECORDED_TIME >= DATEADD(MONTH, -6, GETDATE())
    );

-- =====================================================
-- SECTION 4: ADOLESCENT DEPRESSION SCREENING
-- =====================================================

PRINT 'Building Adolescent Depression Screening...';

--IF OBJECT_ID('tempdb..#AdolescentDepressionScreening') IS NOT NULL DROP TABLE #AdolescentDepressionScreening;
INSERT INTO #AllCareGaps (PAT_ID, GAP_TYPE, GAP_CATEGORY, GAP_DETAIL, DAYS_OPEN, GAP_DATE, PRIORITY_LEVEL)

SELECT 
    pb.PAT_ID,
    'Missing Depression Screening' AS GAP_TYPE,
    'Behavioral Health' AS GAP_CATEGORY,
    'Annual PHQ-9/PHQ-A Due (Age ' + CAST(pb.AGE_YEARS AS VARCHAR) + ')' AS GAP_DETAIL,
    DATEDIFF(DAY, last_screen.SCREEN_DATE, GETDATE()) AS DAYS_OPEN,
    last_screen.SCREEN_DATE AS GAP_DATE,
    2 AS PRIORITY_LEVEL
--INTO #AdolescentDepressionScreening
FROM #PatientBase pb
LEFT JOIN (
    SELECT 
        sed.PAT_LINK_ID AS PAT_ID,
        MAX(sed.CUR_VALUE_DATETIME) AS SCREEN_DATE
    FROM CLARITY..SMRTDTA_ELEM_DATA sed
    INNER JOIN CLARITY..SMRTDTA_ELEM_VALUE sev ON sed.HLV_ID = sev.HLV_ID
    INNER JOIN CLARITY..CLARITY_CONCEPT cc ON sed.ELEMENT_ID = cc.CONCEPT_ID
        AND cc.DATA_TYPE_C IN (3,4,11)
    WHERE sed.CONTEXT_NAME = 'PATIENT'
        AND (cc.NAME LIKE '%PHQ%' OR cc.NAME LIKE '%DEPRESSION%SCREEN%')
    GROUP BY sed.PAT_LINK_ID
    
    UNION ALL
    
    SELECT 
        ifr.PAT_ID,
        MAX(ifm.RECORDED_TIME) AS SCREEN_DATE
    FROM CLARITY..IP_FLWSHT_MEAS ifm
    INNER JOIN CLARITY..IP_FLWSHT_REC ifr ON ifm.FSD_ID = ifr.FSD_ID
    INNER JOIN CLARITY..IP_FLO_GP_DATA ifg ON ifm.FLO_MEAS_ID = ifg.FLO_MEAS_ID
    WHERE ifg.VAL_TYPE_C IS NOT NULL
        AND (ifg.FLO_MEAS_NAME LIKE '%PHQ%' OR ifg.FLO_MEAS_NAME LIKE '%DEPRESSION%')
    GROUP BY ifr.PAT_ID
) last_screen ON pb.PAT_ID = last_screen.PAT_ID
WHERE pb.AGE_YEARS BETWEEN 12 AND 21
    AND (last_screen.SCREEN_DATE IS NULL OR DATEDIFF(YEAR, last_screen.SCREEN_DATE, GETDATE()) >= 1);

-- =====================================================
-- SECTION 5: BMI DOCUMENTATION (FLOWSHEETS)
-- =====================================================

PRINT 'Building BMI Documentation...';

--IF OBJECT_ID('tempdb..#BMIDocumentation') IS NOT NULL DROP TABLE #BMIDocumentation;
INSERT INTO #AllCareGaps (PAT_ID, GAP_TYPE, GAP_CATEGORY, GAP_DETAIL, DAYS_OPEN, GAP_DATE, PRIORITY_LEVEL)

SELECT 
    pb.PAT_ID,
    CASE 
        WHEN last_bmi.BMI_PERCENTILE IS NULL THEN 'Missing BMI Documentation'
        WHEN last_bmi.BMI_PERCENTILE >= 95 THEN 'Obesity - Counseling Needed'
        WHEN last_bmi.BMI_PERCENTILE >= 85 THEN 'Overweight - Counseling Needed'
    END AS GAP_TYPE,
    'Preventive Care' AS GAP_CATEGORY,
    'BMI Percentile: ' + ISNULL(CAST(last_bmi.BMI_PERCENTILE AS VARCHAR), 'Not Documented') AS GAP_DETAIL,
    DATEDIFF(DAY, last_bmi.MEASURE_DATE, GETDATE()) AS DAYS_OPEN,
    last_bmi.MEASURE_DATE AS GAP_DATE,
    3 AS PRIORITY_LEVEL
--INTO #BMIDocumentation
FROM #PatientBase pb
LEFT JOIN (
    SELECT 
        ifr.PAT_ID,
        CAST(ifm.meas_value AS DECIMAL(5,2)) AS BMI_PERCENTILE,
        ifm.RECORDED_TIME AS MEASURE_DATE,
        ROW_NUMBER() OVER (PARTITION BY ifr.PAT_ID ORDER BY ifm.RECORDED_TIME DESC) AS RN
    FROM CLARITY..IP_FLWSHT_MEAS ifm
    INNER JOIN CLARITY..IP_FLWSHT_REC ifr ON ifm.FSD_ID = ifr.FSD_ID
    INNER JOIN CLARITY..IP_FLO_GP_DATA ifg ON ifm.FLO_MEAS_ID = ifg.FLO_MEAS_ID
    WHERE ifg.VAL_TYPE_C IS NOT NULL
        AND (ifg.FLO_MEAS_NAME LIKE '%BMI%PERCENTILE%' OR ifg.FLO_MEAS_NAME LIKE '%BMI PERCENTILE%')
        AND ifm.RECORDED_TIME >= DATEADD(YEAR, -2, GETDATE())
        AND ISNUMERIC(ifm.MEAS_VALUE) = 1
) last_bmi ON pb.PAT_ID = last_bmi.PAT_ID AND last_bmi.RN = 1
WHERE pb.AGE_YEARS >= 2
    AND (
        last_bmi.BMI_PERCENTILE IS NULL 
        OR last_bmi.BMI_PERCENTILE >= 85
        OR DATEDIFF(YEAR, last_bmi.MEASURE_DATE, GETDATE()) > 1
    );

-- =====================================================
-- SECTION 6: LEAD SCREENING
-- =====================================================

PRINT 'Building Lead Screening...';

--IF OBJECT_ID('tempdb..#LeadScreening') IS NOT NULL DROP TABLE #LeadScreening;
INSERT INTO #AllCareGaps (PAT_ID, GAP_TYPE, GAP_CATEGORY, GAP_DETAIL, DAYS_OPEN, GAP_DATE, PRIORITY_LEVEL)

SELECT 
    pb.PAT_ID,
    'Missing Lead Screening' AS GAP_TYPE,
    'Preventive Care' AS GAP_CATEGORY,
    CASE 
        WHEN pb.AGE_MONTHS BETWEEN 12 AND 14 THEN '12 Month Lead Screen'
        WHEN pb.AGE_MONTHS BETWEEN 24 AND 26 THEN '24 Month Lead Screen'
    END AS GAP_DETAIL,
    NULL AS DAYS_OPEN,
    NULL AS GAP_DATE,
    2 AS PRIORITY_LEVEL
--INTO #LeadScreening
FROM #PatientBase pb
WHERE pb.AGE_MONTHS IN (12, 13, 14, 24, 25, 26)
    AND NOT EXISTS (
        SELECT 1
        FROM CLARITY..ORDER_RESULTS lr
        INNER JOIN CLARITY..CLARITY_COMPONENT cc ON lr.COMPONENT_ID = cc.COMPONENT_ID
        WHERE lr.PAT_ID = pb.PAT_ID
            AND (cc.NAME LIKE '%LEAD%' OR cc.BASE_NAME LIKE '%LEAD%')
            AND lr.RESULT_DATE >= DATEADD(MONTH, -3, GETDATE())
    );

-- =====================================================
-- SECTION 7: VISION & HEARING SCREENING (FLOWSHEETS)
-- =====================================================

PRINT 'Building Vision & Hearing Screening...';

--IF OBJECT_ID('tempdb..#VisionScreening') IS NOT NULL DROP TABLE #VisionScreening;
INSERT INTO #AllCareGaps (PAT_ID, GAP_TYPE, GAP_CATEGORY, GAP_DETAIL, DAYS_OPEN, GAP_DATE, PRIORITY_LEVEL)

SELECT 
    pb.PAT_ID,
    'Overdue Vision Screening' AS GAP_TYPE,
    'Preventive Care' AS GAP_CATEGORY,
    'Vision Screen Age ' + CAST(pb.AGE_YEARS AS VARCHAR) AS GAP_DETAIL,
    DATEDIFF(DAY, last_vision.SCREEN_DATE, GETDATE()) AS DAYS_OPEN,
    last_vision.SCREEN_DATE AS GAP_DATE,
    3 AS PRIORITY_LEVEL
--INTO #VisionScreening
FROM #PatientBase pb
LEFT JOIN (
    SELECT 
        ifr.PAT_ID,
        MAX(ifm.RECORDED_TIME) AS SCREEN_DATE
    FROM CLARITY..IP_FLWSHT_MEAS ifm
    INNER JOIN CLARITY..IP_FLWSHT_REC ifr ON ifm.FSD_ID = ifr.FSD_ID
    INNER JOIN CLARITY..IP_FLO_GP_DATA ifg ON ifm.FLO_MEAS_ID = ifg.FLO_MEAS_ID
    WHERE ifg.VAL_TYPE_C IS NOT NULL
        AND (ifg.FLO_MEAS_NAME LIKE '%VISION%' OR ifg.FLO_MEAS_NAME LIKE '%VISUAL ACUITY%')
    GROUP BY ifr.PAT_ID
) last_vision ON pb.PAT_ID = last_vision.PAT_ID
WHERE pb.AGE_YEARS IN (3, 4, 5, 6, 8, 10, 12, 15)
    AND (last_vision.SCREEN_DATE IS NULL OR DATEDIFF(YEAR, last_vision.SCREEN_DATE, GETDATE()) >= 2);

--IF OBJECT_ID('tempdb..#HearingScreening') IS NOT NULL DROP TABLE #HearingScreening;
INSERT INTO #AllCareGaps (PAT_ID, GAP_TYPE, GAP_CATEGORY, GAP_DETAIL, DAYS_OPEN, GAP_DATE, PRIORITY_LEVEL)

SELECT 
    pb.PAT_ID,
    'Overdue Hearing Screening' AS GAP_TYPE,
    'Preventive Care' AS GAP_CATEGORY,
    'Hearing Screen Age ' + CAST(pb.AGE_YEARS AS VARCHAR) AS GAP_DETAIL,
    DATEDIFF(DAY, last_hearing.SCREEN_DATE, GETDATE()) AS DAYS_OPEN,
    last_hearing.SCREEN_DATE AS GAP_DATE,
    3 AS PRIORITY_LEVEL
--INTO #HearingScreening
FROM #PatientBase pb
LEFT JOIN (
    SELECT 
        ifr.PAT_ID,
        MAX(ifm.RECORDED_TIME) AS SCREEN_DATE
    FROM CLARITY..IP_FLWSHT_MEAS ifm
    INNER JOIN CLARITY..IP_FLWSHT_REC ifr ON ifm.FSD_ID = ifr.FSD_ID
    INNER JOIN CLARITY..IP_FLO_GP_DATA ifg ON ifm.FLO_MEAS_ID = ifg.FLO_MEAS_ID
    WHERE ifg.VAL_TYPE_C IS NOT NULL
        AND (ifg.FLO_MEAS_NAME LIKE '%HEARING%' OR ifg.FLO_MEAS_NAME LIKE '%AUDIOLOG%')
    GROUP BY ifr.PAT_ID
) last_hearing ON pb.PAT_ID = last_hearing.PAT_ID
WHERE pb.AGE_YEARS IN (4, 5, 6, 8, 10)
    AND (last_hearing.SCREEN_DATE IS NULL OR DATEDIFF(YEAR, last_hearing.SCREEN_DATE, GETDATE()) >= 2);

-- =====================================================
-- SECTION 8: BLOOD PRESSURE SCREENING
-- =====================================================

PRINT 'Building Blood Pressure Screening...';

--IF OBJECT_ID('tempdb..#BloodPressureScreening') IS NOT NULL DROP TABLE #BloodPressureScreening;
INSERT INTO #AllCareGaps (PAT_ID, GAP_TYPE, GAP_CATEGORY, GAP_DETAIL, DAYS_OPEN, GAP_DATE, PRIORITY_LEVEL)

SELECT 
    pb.PAT_ID,
    'Missing Blood Pressure' AS GAP_TYPE,
    'Preventive Care' AS GAP_CATEGORY,
    'Annual BP Check Due (Age ' + CAST(pb.AGE_YEARS AS VARCHAR) + ')' AS GAP_DETAIL,
    DATEDIFF(DAY, last_bp.BP_DATE, GETDATE()) AS DAYS_OPEN,
    last_bp.BP_DATE AS GAP_DATE,
    3 AS PRIORITY_LEVEL
--INTO #BloodPressureScreening
FROM #PatientBase pb
LEFT JOIN (
    SELECT 
        ifr.PAT_ID,
        MAX(ifm.RECORDED_TIME) AS BP_DATE
    FROM CLARITY..IP_FLWSHT_MEAS ifm
    INNER JOIN CLARITY..IP_FLWSHT_REC ifr ON ifm.FSD_ID = ifr.FSD_ID
    INNER JOIN CLARITY..IP_FLO_GP_DATA ifg ON ifm.FLO_MEAS_ID = ifg.FLO_MEAS_ID
    WHERE ifg.VAL_TYPE_C IS NOT NULL
        AND (ifg.FLO_MEAS_NAME LIKE '%BP%' OR ifg.FLO_MEAS_NAME LIKE '%BLOOD PRESSURE%' OR ifg.FLO_MEAS_NAME LIKE '%SYSTOLIC%')
    GROUP BY ifr.PAT_ID
) last_bp ON pb.PAT_ID = last_bp.PAT_ID
WHERE pb.AGE_YEARS >= 3
    AND (last_bp.BP_DATE IS NULL OR DATEDIFF(YEAR, last_bp.BP_DATE, GETDATE()) >= 1);

-- =====================================================
-- SECTION 9: CHRONIC DISEASE - DIABETES
-- =====================================================

PRINT 'Building Diabetes Care Gaps...';

--IF OBJECT_ID('tempdb..#DiabeticVisionScreening') IS NOT NULL DROP TABLE #DiabeticVisionScreening;
INSERT INTO #AllCareGaps (PAT_ID, GAP_TYPE, GAP_CATEGORY, GAP_DETAIL, DAYS_OPEN, GAP_DATE, PRIORITY_LEVEL)

SELECT DISTINCT
    pl.PAT_ID,
    'Diabetes - Vision Screening Needed' AS GAP_TYPE,
    'Chronic Disease Management' AS GAP_CATEGORY,
    pl.DESCRIPTION AS GAP_DETAIL,
    DATEDIFF(DAY, last_eye.LAST_VISIT, GETDATE()) AS DAYS_OPEN,
    last_eye.LAST_VISIT AS GAP_DATE,
    1 AS PRIORITY_LEVEL
--INTO #DiabeticVisionScreening
FROM CLARITY..PROBLEM_LIST pl
INNER JOIN CLARITY..ZC_PROBLEM_STATUS pStatus on pl.PROBLEM_STATUS_C = pStatus.PROBLEM_STATUS_C
inner join CLARITY..CLARITY_EDG dx on pl.DX_ID = dx.DX_ID
INNER JOIN #PatientBase pb ON pl.PAT_ID = pb.PAT_ID
LEFT JOIN (
    SELECT 
        pe.PAT_ID,
        MAX(pe.CONTACT_DATE) AS LAST_VISIT
    FROM CLARITY..PAT_ENC pe
    INNER JOIN CLARITY..CLARITY_DEP dep ON pe.DEPARTMENT_ID = dep.DEPARTMENT_ID
    WHERE pe.ENC_TYPE_C IN ('101','50')
        AND pe.CONTACT_DATE >= DATEADD(YEAR, -1, GETDATE())
        AND dep.SPECIALTY LIKE '%OPHTH%' -- Ophthalmology visits
    GROUP BY pe.PAT_ID
) last_eye ON pl.PAT_ID = last_eye.PAT_ID
WHERE (dx.CURRENT_ICD10_LIST LIKE 'E10%' OR dx.CURRENT_ICD10_LIST LIKE 'E11%')
    AND pStatus.NAME = 'Active'
    AND (last_eye.LAST_VISIT IS NULL OR DATEDIFF(MONTH, last_eye.LAST_VISIT, GETDATE()) > 12);

--IF OBJECT_ID('tempdb..#DiabetesHbA1c') IS NOT NULL DROP TABLE #DiabetesHbA1c;
INSERT INTO #AllCareGaps (PAT_ID, GAP_TYPE, GAP_CATEGORY, GAP_DETAIL, DAYS_OPEN, GAP_DATE, PRIORITY_LEVEL)

SELECT 
    pl.PAT_ID,
    'Diabetes - HbA1c Overdue' AS GAP_TYPE,
    'Chronic Disease Management' AS GAP_CATEGORY,
    'Last HbA1c: ' + ISNULL(CAST(last_a1c.A1C_VALUE AS VARCHAR), 'Never') + 
    ' (' + ISNULL(CAST(DATEDIFF(MONTH, last_a1c.RESULT_DATE, GETDATE()) AS VARCHAR), '>12') + ' months ago)' AS GAP_DETAIL,
    DATEDIFF(DAY, last_a1c.RESULT_DATE, GETDATE()) AS DAYS_OPEN,
    last_a1c.RESULT_DATE AS GAP_DATE,
    1 AS PRIORITY_LEVEL
--INTO #DiabetesHbA1c
FROM CLARITY..PROBLEM_LIST pl
INNER JOIN CLARITY..ZC_PROBLEM_STATUS pStatus on pl.PROBLEM_STATUS_C = pStatus.PROBLEM_STATUS_C
inner join CLARITY..CLARITY_EDG dx on pl.DX_ID = dx.DX_ID
INNER JOIN #PatientBase pb ON pl.PAT_ID = pb.PAT_ID
LEFT JOIN (
    SELECT 
        lr.PAT_ID,
        lr.ORD_NUM_VALUE AS A1C_VALUE,
        lr.RESULT_DATE,
        ROW_NUMBER() OVER (PARTITION BY lr.PAT_ID ORDER BY lr.RESULT_DATE DESC) AS RN
    FROM CLARITY..ORDER_RESULTS lr
    INNER JOIN CLARITY..CLARITY_COMPONENT cc ON lr.COMPONENT_ID = cc.COMPONENT_ID
    WHERE cc.NAME LIKE '%HBA1C%' OR cc.NAME LIKE '%A1C%' OR cc.NAME LIKE '%HEMOGLOBIN A1C%'
) last_a1c ON pl.PAT_ID = last_a1c.PAT_ID AND last_a1c.RN = 1
WHERE (dx.CURRENT_ICD10_LIST LIKE 'E10%' OR dx.CURRENT_ICD10_LIST LIKE 'E11%')
    AND pStatus.NAME = 'Active'
    AND (last_a1c.RESULT_DATE IS NULL OR DATEDIFF(MONTH, last_a1c.RESULT_DATE, GETDATE()) > 3);

-- =====================================================
-- SECTION 10: CHRONIC DISEASE - GRAVES DISEASE
-- =====================================================

PRINT 'Building Graves Disease Care Gaps...';

--IF OBJECT_ID('tempdb..#GravesDiseaseAbnormalTSH') IS NOT NULL DROP TABLE #GravesDiseaseAbnormalTSH;
INSERT INTO #AllCareGaps (PAT_ID, GAP_TYPE, GAP_CATEGORY, GAP_DETAIL, DAYS_OPEN, GAP_DATE, PRIORITY_LEVEL)

SELECT DISTINCT
    pl.PAT_ID,
    'Graves Disease - TSH Check Needed' AS GAP_TYPE,
    'Chronic Disease Management' AS GAP_CATEGORY,
    'TSH: ' + ISNULL(CAST(last_tsh.TSH_VALUE AS VARCHAR), 'No Recent') + ' - ' +
    CASE 
        WHEN last_tsh.TSH_VALUE IS NULL THEN 'Check Needed'
        WHEN last_tsh.TSH_VALUE < last_tsh.REF_LOW THEN 'Low'
        WHEN last_tsh.TSH_VALUE > last_tsh.REF_HIGH THEN 'High'
        ELSE 'Recheck Overdue'
    END AS GAP_DETAIL,
    DATEDIFF(DAY, last_tsh.RESULT_DATE, GETDATE()) AS DAYS_OPEN,
    last_tsh.RESULT_DATE AS GAP_DATE,
    1 AS PRIORITY_LEVEL
--INTO #GravesDiseaseAbnormalTSH
FROM CLARITY..PROBLEM_LIST pl
INNER JOIN CLARITY..ZC_PROBLEM_STATUS pStatus on pl.PROBLEM_STATUS_C = pStatus.PROBLEM_STATUS_C
inner join CLARITY..CLARITY_EDG dx on pl.DX_ID = dx.DX_ID
INNER JOIN #PatientBase pb ON pl.PAT_ID = pb.PAT_ID
LEFT JOIN (
    SELECT 
        lr.PAT_ID,
        lr.ORD_NUM_VALUE AS TSH_VALUE,
        lr.RESULT_DATE,
        cc.DEFAULT_LOW AS REF_LOW,
        cc.DEFAULT_HIGH AS REF_HIGH,
        ROW_NUMBER() OVER (PARTITION BY lr.PAT_ID ORDER BY lr.RESULT_DATE DESC) AS RN
    FROM CLARITY..ORDER_RESULTS lr
    INNER JOIN CLARITY..CLARITY_COMPONENT cc ON lr.COMPONENT_ID = cc.COMPONENT_ID
    --LEFT JOIN CLARITY..REFERENCE_RANGE rc ON cc.COMPONENT_ID = rc.COMPONENT_ID
    WHERE cc.NAME LIKE '%TSH%'
) last_tsh ON pl.PAT_ID = last_tsh.PAT_ID AND last_tsh.RN = 1
WHERE (dx.CURRENT_ICD10_LIST LIKE 'E05%' OR dx.CURRENT_ICD10_LIST LIKE '242%')
    AND pStatus.NAME = 'Active'
    AND (
        last_tsh.RESULT_DATE IS NULL 
        OR last_tsh.TSH_VALUE < last_tsh.REF_LOW 
        OR last_tsh.TSH_VALUE > last_tsh.REF_HIGH
        OR DATEDIFF(MONTH, last_tsh.RESULT_DATE, GETDATE()) > 6
    );

-- =====================================================
-- SECTION 11: CHRONIC DISEASE - ASTHMA (WITH ACTUAL FLO_MEAS_IDs)
-- =====================================================

PRINT 'Building Asthma Care Gaps...';

--IF OBJECT_ID('tempdb..#AsthmaControl') IS NOT NULL DROP TABLE #AsthmaControl;
INSERT INTO #AllCareGaps (PAT_ID, GAP_TYPE, GAP_CATEGORY, GAP_DETAIL, DAYS_OPEN, GAP_DATE, PRIORITY_LEVEL)

SELECT 
    pl.PAT_ID,
    'Asthma - Control Assessment Due' AS GAP_TYPE,
    'Chronic Disease Management' AS GAP_CATEGORY,
    'No ACT score in past 12 months' AS GAP_DETAIL,
    DATEDIFF(DAY, last_act.ASSESS_DATE, GETDATE()) AS DAYS_OPEN,
    last_act.ASSESS_DATE AS GAP_DATE,
    2 AS PRIORITY_LEVEL
--INTO #AsthmaControl
FROM CLARITY..PROBLEM_LIST pl
INNER JOIN CLARITY..ZC_PROBLEM_STATUS pStatus on pl.PROBLEM_STATUS_C = pStatus.PROBLEM_STATUS_C
inner join CLARITY..CLARITY_EDG dx on pl.DX_ID = dx.DX_ID
INNER JOIN #PatientBase pb ON pl.PAT_ID = pb.PAT_ID
LEFT JOIN (
    SELECT 
        sed.PAT_LINK_ID AS PAT_ID,
        MAX(sed.CUR_VALUE_DATETIME) AS ASSESS_DATE
    FROM CLARITY..SMRTDTA_ELEM_DATA sed
    INNER JOIN CLARITY..SMRTDTA_ELEM_VALUE sev ON sed.HLV_ID = sev.HLV_ID
    INNER JOIN CLARITY..CLARITY_CONCEPT cc ON sed.ELEMENT_ID = cc.CONCEPT_ID
        AND cc.DATA_TYPE_C IN (3,4,11)
    WHERE sed.CONTEXT_NAME = 'PATIENT'
        AND (cc.NAME LIKE '%ASTHMA CONTROL%' OR cc.NAME LIKE '%ACT%SCORE%' OR cc.NAME LIKE '%ASTHMA%TEST%')
    GROUP BY sed.PAT_LINK_ID
    
    UNION ALL
    
    SELECT 
        ifr.PAT_ID,
        MAX(ifm.RECORDED_TIME) AS ASSESS_DATE
    FROM CLARITY..IP_FLWSHT_MEAS ifm
    INNER JOIN CLARITY..IP_FLWSHT_REC ifr ON ifm.FSD_ID = ifr.FSD_ID
    WHERE ifm.FLO_MEAS_ID IN ('17327','17313','1570048684') -- Actual ACT FLO_MEAS_IDs
    GROUP BY ifr.PAT_ID
) last_act ON pl.PAT_ID = last_act.PAT_ID
WHERE (dx.CURRENT_ICD10_LIST LIKE 'J45%' OR dx.CURRENT_ICD10_LIST LIKE '493%')
    AND pStatus.NAME = 'Active'
    AND (last_act.ASSESS_DATE IS NULL OR DATEDIFF(YEAR, last_act.ASSESS_DATE, GETDATE()) >= 1);

--IF OBJECT_ID('tempdb..#AsthmaActionPlan') IS NOT NULL DROP TABLE #AsthmaActionPlan;
INSERT INTO #AllCareGaps (PAT_ID, GAP_TYPE, GAP_CATEGORY, GAP_DETAIL, DAYS_OPEN, GAP_DATE, PRIORITY_LEVEL)

SELECT 
    pl.PAT_ID,
    'Asthma - Action Plan Missing' AS GAP_TYPE,
    'Chronic Disease Management' AS GAP_CATEGORY,
    'No documented action plan' AS GAP_DETAIL,
    NULL AS DAYS_OPEN,
    NULL AS GAP_DATE,
    2 AS PRIORITY_LEVEL
--INTO #AsthmaActionPlan
FROM CLARITY..PROBLEM_LIST pl
INNER JOIN CLARITY..ZC_PROBLEM_STATUS pStatus on pl.PROBLEM_STATUS_C = pStatus.PROBLEM_STATUS_C
inner join CLARITY..CLARITY_EDG dx on pl.DX_ID = dx.DX_ID
INNER JOIN #PatientBase pb ON pl.PAT_ID = pb.PAT_ID
WHERE (dx.CURRENT_ICD10_LIST LIKE 'J45%' OR dx.CURRENT_ICD10_LIST LIKE '493%')
    AND pStatus.NAME = 'Active'
    AND NOT EXISTS (
        SELECT 1
        FROM CLARITY..HNO_INFO doc
        WHERE doc.PAT_ID = pl.PAT_ID
            AND (doc.NOTE_DESC LIKE '%asthma%action%plan%' OR doc.NOTE_DESC LIKE '%AAP%')
    );

-- =====================================================
-- SECTION 12: CHRONIC DISEASE - ADHD
-- =====================================================

PRINT 'Building ADHD Care Gaps...';

--IF OBJECT_ID('tempdb..#ADHDMedicationFollowup') IS NOT NULL DROP TABLE #ADHDMedicationFollowup;
INSERT INTO #AllCareGaps (PAT_ID, GAP_TYPE, GAP_CATEGORY, GAP_DETAIL, DAYS_OPEN, GAP_DATE, PRIORITY_LEVEL)

SELECT 
    om.PAT_ID,
    'ADHD - Medication Follow-up Due' AS GAP_TYPE,
    'Chronic Disease Management' AS GAP_CATEGORY,
    'Stimulant started, 30-day follow-up needed' AS GAP_DETAIL,
    DATEDIFF(DAY, om.START_DATE, GETDATE()) AS DAYS_OPEN,
    om.START_DATE AS GAP_DATE,
    1 AS PRIORITY_LEVEL
--INTO #ADHDMedicationFollowup
FROM CLARITY..ORDER_MED om
INNER JOIN #PatientBase pb ON om.PAT_ID = pb.PAT_ID
INNER JOIN CLARITY..CLARITY_MEDICATION cm ON om.MEDICATION_ID = cm.MEDICATION_ID
WHERE cm.PHARM_CLASS_C IN ('72', '198') -- Stimulants
    AND om.START_DATE >= DATEADD(DAY, -45, GETDATE())
    AND om.START_DATE <= DATEADD(DAY, -15, GETDATE())
    AND NOT EXISTS (
        SELECT 1
        FROM CLARITY..PAT_ENC pe
        INNER JOIN CLARITY..CLARITY_DEP dep ON pe.DEPARTMENT_ID = dep.DEPARTMENT_ID
            AND dep.RPT_GRP_TEN = 1
        WHERE pe.PAT_ID = om.PAT_ID
            AND pe.CONTACT_DATE > om.START_DATE
            AND pe.CONTACT_DATE <= DATEADD(DAY, 60, om.START_DATE)
    );

--IF OBJECT_ID('tempdb..#ADHDHeightWeight') IS NOT NULL DROP TABLE #ADHDHeightWeight;
INSERT INTO #AllCareGaps (PAT_ID, GAP_TYPE, GAP_CATEGORY, GAP_DETAIL, DAYS_OPEN, GAP_DATE, PRIORITY_LEVEL)

SELECT 
    om.PAT_ID,
    'ADHD - Growth Monitoring Overdue' AS GAP_TYPE,
    'Chronic Disease Management' AS GAP_CATEGORY,
    'Height/Weight check needed (on stimulant)' AS GAP_DETAIL,
    DATEDIFF(DAY, last_measure.MEASURE_DATE, GETDATE()) AS DAYS_OPEN,
    last_measure.MEASURE_DATE AS GAP_DATE,
    2 AS PRIORITY_LEVEL
--INTO #ADHDHeightWeight
FROM CLARITY..ORDER_MED om
INNER JOIN #PatientBase pb ON om.PAT_ID = pb.PAT_ID
INNER JOIN CLARITY..CLARITY_MEDICATION cm ON om.MEDICATION_ID = cm.MEDICATION_ID
LEFT JOIN (
    SELECT 
        ifr.PAT_ID,
        MAX(ifm.RECORDED_TIME) AS MEASURE_DATE
    FROM CLARITY..IP_FLWSHT_MEAS ifm
    INNER JOIN CLARITY..IP_FLWSHT_REC ifr ON ifm.FSD_ID = ifr.FSD_ID
    INNER JOIN CLARITY..IP_FLO_GP_DATA ifg ON ifm.FLO_MEAS_ID = ifg.FLO_MEAS_ID
    WHERE ifg.VAL_TYPE_C IS NOT NULL
        AND (ifg.FLO_MEAS_NAME LIKE '%HEIGHT%' OR ifg.FLO_MEAS_NAME LIKE '%WEIGHT%')
    GROUP BY ifr.PAT_ID
) last_measure ON om.PAT_ID = last_measure.PAT_ID
WHERE cm.PHARM_CLASS_C IN ('72', '198')
    AND om.ORDER_STATUS_C = 2 -- Active
    AND (last_measure.MEASURE_DATE IS NULL OR DATEDIFF(MONTH, last_measure.MEASURE_DATE, GETDATE()) > 6);

-- =====================================================
-- SECTION 13: IMMUNIZATIONS (WITH HEALTH MAINTENANCE TOPICS)
-- =====================================================

PRINT 'Building Immunization Gaps...';

--IF OBJECT_ID('tempdb..#MissingFluShots') IS NOT NULL DROP TABLE #MissingFluShots;
INSERT INTO #AllCareGaps (PAT_ID, GAP_TYPE, GAP_CATEGORY, GAP_DETAIL, DAYS_OPEN, GAP_DATE, PRIORITY_LEVEL)

SELECT DISTINCT
    pb.PAT_ID,
    'Missing Flu Shot' AS GAP_TYPE,
    'Immunizations' AS GAP_CATEGORY,
    'Flu Season ' + CAST(
        CASE WHEN MONTH(GETDATE()) >= 7 THEN YEAR(GETDATE()) ELSE YEAR(GETDATE()) - 1 END 
    AS VARCHAR) AS GAP_DETAIL,
    NULL AS DAYS_OPEN,
    NULL AS GAP_DATE,
    3 AS PRIORITY_LEVEL
--INTO #MissingFluShots
FROM #PatientBase pb
WHERE pb.AGE_MONTHS >= 6
    AND NOT EXISTS (
        SELECT 1
        FROM CLARITY..IMMUNE imm
        INNER JOIN CLARITY..CLARITY_IMMUNZATN ci ON imm.IMMUNZATN_ID = ci.IMMUNZATN_ID
        WHERE imm.PAT_ID = pb.PAT_ID
            AND imm.IMMNZTN_STATUS_C = 1 -- Given
            AND (ci.NAME LIKE '%INFLUENZA%' OR ci.NAME LIKE '%FLU%')
            AND imm.IMMUNE_DATE >= CASE 
                WHEN MONTH(GETDATE()) >= 7 THEN DATEFROMPARTS(YEAR(GETDATE()), 7, 1)
                ELSE DATEFROMPARTS(YEAR(GETDATE()) - 1, 7, 1)
            END
    );

--IF OBJECT_ID('tempdb..#OverdueVaccines') IS NOT NULL DROP TABLE #OverdueVaccines;
INSERT INTO #AllCareGaps (PAT_ID, GAP_TYPE, GAP_CATEGORY, GAP_DETAIL, DAYS_OPEN, GAP_DATE, PRIORITY_LEVEL)

-- Using Health Maintenance Topics pattern
SELECT DISTINCT
    pb.PAT_ID,
    'Overdue Vaccine' AS GAP_TYPE,
    'Immunizations' AS GAP_CATEGORY,
    hmt.NAME + ' - Overdue' AS GAP_DETAIL,
    NULL AS DAYS_OPEN,
    NULL AS GAP_DATE,
    2 AS PRIORITY_LEVEL
--INTO #OverdueVaccines
FROM #PatientBase pb
CROSS JOIN (
    SELECT DISTINCT lim.HM_TOPICS_ID, hmt.NAME
    FROM CLARITY..LIM_HM_TOPICS lim
    INNER JOIN CLARITY..CLARITY_HM_TOPIC hmt ON lim.HM_TOPICS_ID = hmt.HM_TOPIC_ID
    WHERE lim.HM_TOPICS_ID IN (6 /*DTAP*/, 16 /*IPV*/, 14 /*MMR*/, 12 /*HIB*/, 10 /*HEPB*/, 18 /*Varicella*/, 19 /*Pneumococcal*/)
) hmt
WHERE NOT EXISTS (
    SELECT 1
    FROM CLARITY..LIM_HM_TOPICS lim
    INNER JOIN CLARITY..IMMUNE imm ON lim.IMMUN_ID = imm.IMMUNZATN_ID
    WHERE imm.PAT_ID = pb.PAT_ID
        AND lim.HM_TOPICS_ID = hmt.HM_TOPICS_ID
        AND imm.IMMNZTN_STATUS_C = 1 -- Given
        AND imm.IMMUNZATN_ID <> 18 -- Exclude Hepatitis B Immune Globulin
);

--IF OBJECT_ID('tempdb..#HPVVaccination') IS NOT NULL DROP TABLE #HPVVaccination;
INSERT INTO #AllCareGaps (PAT_ID, GAP_TYPE, GAP_CATEGORY, GAP_DETAIL, DAYS_OPEN, GAP_DATE, PRIORITY_LEVEL)

SELECT 
    pb.PAT_ID,
    'HPV Vaccination Incomplete' AS GAP_TYPE,
    'Immunizations' AS GAP_CATEGORY,
    'HPV series incomplete (Age ' + CAST(pb.AGE_YEARS AS VARCHAR) + ')' AS GAP_DETAIL,
    NULL AS DAYS_OPEN,
    NULL AS GAP_DATE,
    2 AS PRIORITY_LEVEL
--INTO #HPVVaccination
FROM #PatientBase pb
LEFT JOIN (
    SELECT 
        imm.PAT_ID,
        COUNT(*) AS DOSES_GIVEN
    FROM CLARITY..IMMUNE imm
    INNER JOIN CLARITY..CLARITY_IMMUNZATN ci ON imm.IMMUNZATN_ID = ci.IMMUNZATN_ID
    WHERE ci.NAME LIKE '%HPV%'
        AND imm.IMMNZTN_STATUS_C = 1 -- Given
    GROUP BY imm.PAT_ID
) hpv ON pb.PAT_ID = hpv.PAT_ID
WHERE pb.AGE_YEARS BETWEEN 11 AND 26
    AND (hpv.DOSES_GIVEN IS NULL OR hpv.DOSES_GIVEN < 2);

-- =====================================================
-- SECTION 14: ADOLESCENT HEALTH
-- =====================================================

PRINT 'Building Adolescent Health Gaps...';

--IF OBJECT_ID('tempdb..#ChlamydiaScreening') IS NOT NULL DROP TABLE #ChlamydiaScreening;
INSERT INTO #AllCareGaps (PAT_ID, GAP_TYPE, GAP_CATEGORY, GAP_DETAIL, DAYS_OPEN, GAP_DATE, PRIORITY_LEVEL)

SELECT 
    pb.PAT_ID,
    'Chlamydia Screening Due' AS GAP_TYPE,
    'Adolescent Health' AS GAP_CATEGORY,
    'Sexually active female, annual screening' AS GAP_DETAIL,
    DATEDIFF(DAY, last_test.TEST_DATE, GETDATE()) AS DAYS_OPEN,
    last_test.TEST_DATE AS GAP_DATE,
    2 AS PRIORITY_LEVEL
--INTO #ChlamydiaScreening
FROM #PatientBase pb
LEFT JOIN (
    SELECT 
        lr.PAT_ID,
        MAX(lr.RESULT_DATE) AS TEST_DATE
    FROM CLARITY..ORDER_RESULTS lr
    INNER JOIN CLARITY..CLARITY_COMPONENT cc ON lr.COMPONENT_ID = cc.COMPONENT_ID
    WHERE cc.NAME LIKE '%CHLAMYDIA%'
    GROUP BY lr.PAT_ID
) last_test ON pb.PAT_ID = last_test.PAT_ID
WHERE pb.SEX = 'Female'
    AND pb.AGE_YEARS BETWEEN 16 AND 24
    AND (last_test.TEST_DATE IS NULL OR DATEDIFF(YEAR, last_test.TEST_DATE, GETDATE()) >= 1);

--IF OBJECT_ID('tempdb..#SubstanceAbuseScreening') IS NOT NULL DROP TABLE #SubstanceAbuseScreening;
INSERT INTO #AllCareGaps (PAT_ID, GAP_TYPE, GAP_CATEGORY, GAP_DETAIL, DAYS_OPEN, GAP_DATE, PRIORITY_LEVEL)

SELECT 
    pb.PAT_ID,
    'Substance Abuse Screening Due' AS GAP_TYPE,
    'Adolescent Health' AS GAP_CATEGORY,
    'Annual CRAFFT screening (Age ' + CAST(pb.AGE_YEARS AS VARCHAR) + ')' AS GAP_DETAIL,
    DATEDIFF(DAY, last_screen.SCREEN_DATE, GETDATE()) AS DAYS_OPEN,
    last_screen.SCREEN_DATE AS GAP_DATE,
    2 AS PRIORITY_LEVEL
--INTO #SubstanceAbuseScreening
FROM #PatientBase pb
LEFT JOIN (
    SELECT 
        sed.PAT_LINK_ID AS PAT_ID,
        MAX(sed.CUR_VALUE_DATETIME) AS SCREEN_DATE
    FROM CLARITY..SMRTDTA_ELEM_DATA sed
    INNER JOIN CLARITY..SMRTDTA_ELEM_VALUE sev ON sed.HLV_ID = sev.HLV_ID
    INNER JOIN CLARITY..CLARITY_CONCEPT cc ON sed.ELEMENT_ID = cc.CONCEPT_ID
        AND cc.DATA_TYPE_C IN (3,4,11)
    WHERE sed.CONTEXT_NAME = 'PATIENT'
        AND (cc.NAME LIKE '%CRAFFT%' OR cc.NAME LIKE '%SUBSTANCE%' OR cc.NAME LIKE '%DRUG%SCREEN%')
    GROUP BY sed.PAT_LINK_ID
    
    UNION ALL
    
    SELECT 
        ifr.PAT_ID,
        MAX(ifm.RECORDED_TIME) AS SCREEN_DATE
    FROM CLARITY..IP_FLWSHT_MEAS ifm
    INNER JOIN CLARITY..IP_FLWSHT_REC ifr ON ifm.FSD_ID = ifr.FSD_ID
    INNER JOIN CLARITY..IP_FLO_GP_DATA ifg ON ifm.FLO_MEAS_ID = ifg.FLO_MEAS_ID
    WHERE ifg.VAL_TYPE_C IS NOT NULL
        AND (ifg.FLO_MEAS_NAME LIKE '%CRAFFT%' OR ifg.FLO_MEAS_NAME LIKE '%SUBSTANCE%')
    GROUP BY ifr.PAT_ID
) last_screen ON pb.PAT_ID = last_screen.PAT_ID
WHERE pb.AGE_YEARS BETWEEN 12 AND 21
    AND (last_screen.SCREEN_DATE IS NULL OR DATEDIFF(YEAR, last_screen.SCREEN_DATE, GETDATE()) >= 1);

-- =====================================================
-- SECTION 15: LIPID SCREENING
-- =====================================================

PRINT 'Building Lipid Screening...';

--IF OBJECT_ID('tempdb..#LipidScreening') IS NOT NULL DROP TABLE #LipidScreening;
INSERT INTO #AllCareGaps (PAT_ID, GAP_TYPE, GAP_CATEGORY, GAP_DETAIL, DAYS_OPEN, GAP_DATE, PRIORITY_LEVEL)

SELECT 
    pb.PAT_ID,
    'Lipid Screening Due' AS GAP_TYPE,
    'Preventive Care' AS GAP_CATEGORY,
    CASE 
        WHEN pb.AGE_YEARS BETWEEN 9 AND 11 THEN 'Universal screening (Age 9-11)'
        WHEN pb.AGE_YEARS BETWEEN 17 AND 21 THEN 'Universal screening (Age 17-21)'
        ELSE 'High-risk screening'
    END AS GAP_DETAIL,
    DATEDIFF(DAY, last_lipid.TEST_DATE, GETDATE()) AS DAYS_OPEN,
    last_lipid.TEST_DATE AS GAP_DATE,
    3 AS PRIORITY_LEVEL
--INTO #LipidScreening
FROM #PatientBase pb
LEFT JOIN (
    SELECT 
        lr.PAT_ID,
        MAX(lr.RESULT_DATE) AS TEST_DATE
    FROM CLARITY..ORDER_RESULTS lr
    INNER JOIN CLARITY..CLARITY_COMPONENT cc ON lr.COMPONENT_ID = cc.COMPONENT_ID
    WHERE cc.NAME LIKE '%CHOLESTEROL%' OR cc.NAME LIKE '%LIPID%'
    GROUP BY lr.PAT_ID
) last_lipid ON pb.PAT_ID = last_lipid.PAT_ID
WHERE (
    (pb.AGE_YEARS BETWEEN 9 AND 11)
    OR (pb.AGE_YEARS BETWEEN 17 AND 21)
)
AND (last_lipid.TEST_DATE IS NULL OR DATEDIFF(YEAR, last_lipid.TEST_DATE, GETDATE()) >= 5);

-- =====================================================
-- SECTION 16: TRANSITION OF CARE
-- =====================================================

PRINT 'Building Transition of Care...';

--IF OBJECT_ID('tempdb..#TransitionToAdultCare') IS NOT NULL DROP TABLE #TransitionToAdultCare;
INSERT INTO #AllCareGaps (PAT_ID, GAP_TYPE, GAP_CATEGORY, GAP_DETAIL, DAYS_OPEN, GAP_DATE, PRIORITY_LEVEL)

SELECT 
    pb.PAT_ID,
    'Transition Planning Needed' AS GAP_TYPE,
    'Care Coordination' AS GAP_CATEGORY,
    'Age ' + CAST(pb.AGE_YEARS AS VARCHAR) + ' - Adult care transition planning' AS GAP_DETAIL,
    NULL AS DAYS_OPEN,
    NULL AS GAP_DATE,
    2 AS PRIORITY_LEVEL
--INTO #TransitionToAdultCare
FROM #PatientBase pb
WHERE pb.AGE_YEARS BETWEEN 16 AND 20
    AND NOT EXISTS (
        SELECT 1
        FROM CLARITY..HNO_INFO doc
        WHERE doc.PAT_ID = pb.PAT_ID
            AND (doc.NOTE_DESC LIKE '%TRANSITION%ADULT%' OR doc.NOTE_DESC LIKE '%TRANSITION PLAN%')
            AND doc.CREATE_INSTANT_DTTM >= DATEADD(YEAR, -1, GETDATE())
    );

--IF OBJECT_ID('tempdb..#EmergencyFollowUp') IS NOT NULL DROP TABLE #EmergencyFollowUp;
INSERT INTO #AllCareGaps (PAT_ID, GAP_TYPE, GAP_CATEGORY, GAP_DETAIL, DAYS_OPEN, GAP_DATE, PRIORITY_LEVEL)

SELECT 
    pe.PAT_ID,
    'ED Visit - Follow-up Needed' AS GAP_TYPE,
    'Care Coordination' AS GAP_CATEGORY,
    'ED visit ' + CAST(DATEDIFF(DAY, pe.CONTACT_DATE, GETDATE()) AS VARCHAR) + ' days ago, no follow-up' AS GAP_DETAIL,
    DATEDIFF(DAY, pe.CONTACT_DATE, GETDATE()) AS DAYS_OPEN,
    pe.CONTACT_DATE AS GAP_DATE,
    1 AS PRIORITY_LEVEL
--INTO #EmergencyFollowUp
FROM CLARITY..PAT_ENC pe
INNER JOIN #PatientBase pb ON pe.PAT_ID = pb.PAT_ID
WHERE pe.ENC_TYPE_C IN ('2', '3', '110') -- ED encounters
    AND pe.CONTACT_DATE >= DATEADD(DAY, -14, GETDATE())
    AND pe.CONTACT_DATE <= DATEADD(DAY, -7, GETDATE())
    AND NOT EXISTS (
        SELECT 1
        FROM CLARITY..PAT_ENC follow
        INNER JOIN CLARITY..CLARITY_DEP dep ON follow.DEPARTMENT_ID = dep.DEPARTMENT_ID
            AND dep.RPT_GRP_TEN = 1
        WHERE follow.PAT_ID = pe.PAT_ID
            AND follow.CONTACT_DATE > pe.CONTACT_DATE
            AND follow.CONTACT_DATE <= DATEADD(DAY, 7, pe.CONTACT_DATE)
            AND follow.ENC_TYPE_C IN ('101', '50')
    );

--IF OBJECT_ID('tempdb..#HospitalDischargeFollowUp') IS NOT NULL DROP TABLE #HospitalDischargeFollowUp;
INSERT INTO #AllCareGaps (PAT_ID, GAP_TYPE, GAP_CATEGORY, GAP_DETAIL, DAYS_OPEN, GAP_DATE, PRIORITY_LEVEL)

SELECT 
    hsp.PAT_ID,
    'Hospital Discharge - Follow-up Needed' AS GAP_TYPE,
    'Care Coordination' AS GAP_CATEGORY,
    'Discharged ' + CAST(DATEDIFF(DAY, hsp.DISCH_DATE_TIME, GETDATE()) AS VARCHAR) + ' days ago' AS GAP_DETAIL,
    DATEDIFF(DAY, hsp.DISCH_DATE_TIME, GETDATE()) AS DAYS_OPEN,
    hsp.DISCH_DATE_TIME AS GAP_DATE,
    1 AS PRIORITY_LEVEL
--INTO #HospitalDischargeFollowUp
FROM CLARITY..HSP_ACCOUNT hsp
INNER JOIN #PatientBase pb ON hsp.PAT_ID = pb.PAT_ID
WHERE hsp.DISCH_DATE_TIME >= DATEADD(DAY, -21, GETDATE())
    AND hsp.DISCH_DATE_TIME <= DATEADD(DAY, -7, GETDATE())
    AND NOT EXISTS (
        SELECT 1
        FROM CLARITY..PAT_ENC follow
        INNER JOIN CLARITY..CLARITY_DEP dep ON follow.DEPARTMENT_ID = dep.DEPARTMENT_ID
            AND dep.RPT_GRP_TEN = 1
        WHERE follow.PAT_ID = hsp.PAT_ID
            AND follow.CONTACT_DATE > hsp.DISCH_DATE_TIME
            AND follow.CONTACT_DATE <= DATEADD(DAY, 14, hsp.DISCH_DATE_TIME)
            AND follow.ENC_TYPE_C IN ('101', '50')
    );

	/*
-- =====================================================
-- STEP 4: COMBINE ALL GAPS
-- =====================================================

PRINT 'Combining all gaps...';

IF OBJECT_ID('tempdb..#AllCareGaps') IS NOT NULL DROP TABLE #AllCareGaps;

SELECT * INTO #AllCareGaps FROM #IncompleteOrders
UNION ALL SELECT * FROM #WellChildVisits
UNION ALL SELECT * FROM #DevelopmentalScreening
UNION ALL SELECT * FROM #AdolescentDepressionScreening
UNION ALL SELECT * FROM #BMIDocumentation
UNION ALL SELECT * FROM #LeadScreening
UNION ALL SELECT * FROM #VisionScreening
UNION ALL SELECT * FROM #HearingScreening
UNION ALL SELECT * FROM #BloodPressureScreening
UNION ALL SELECT * FROM #MissingFluShots
UNION ALL SELECT * FROM #OverdueVaccines
UNION ALL SELECT * FROM #HPVVaccination
UNION ALL SELECT * FROM #DiabetesHbA1c
UNION ALL SELECT * FROM #AsthmaControl
UNION ALL SELECT * FROM #ADHDMedicationFollowup
UNION ALL SELECT * FROM #ChlamydiaScreening
UNION ALL SELECT * FROM #SubstanceAbuseScreening
UNION ALL SELECT * FROM #LipidScreening
UNION ALL SELECT * FROM #EmergencyFollowUp
UNION ALL SELECT * FROM #HospitalDischargeFollowUp;
*/

IF @GapsExist = 0
BEGIN
CREATE INDEX IX_AllCareGaps_PAT_ID ON #AllCareGaps(PAT_ID);
CREATE INDEX IX_AllCareGaps_PRIORITY ON #AllCareGaps(PRIORITY_LEVEL);
END

-- Apply priority filter if specified
IF @PriorityLevel IS NOT NULL
    DELETE FROM #AllCareGaps WHERE PRIORITY_LEVEL <> @PriorityLevel;

DECLARE @TotalGaps INT;
SELECT @TotalGaps = COUNT(*) FROM #AllCareGaps;
PRINT 'Total Gaps: ' + CAST(@TotalGaps AS VARCHAR);
PRINT '';

-- =====================================================
-- OUTPUT 1: PATIENT-LEVEL VIEW
-- =====================================================

PRINT '========================================';
PRINT 'OUTPUT 1: PATIENT-LEVEL VIEW';
PRINT '========================================';

SELECT 
    pb.PAT_MRN_ID,
    pb.PAT_NAME,
    pb.BIRTH_DATE,
    pb.AGE_YEARS,
    pb.SEX,
    pb.PCP_NAME,
    pb.PCP_CARE_TEAM_NAME,
    pb.HOME_PHONE,
    pb.EMAIL_ADDRESS,
    next_appt.APPT_DATE AS NEXT_APPT_DATE,
    next_appt.DAYS_UNTIL_APPT,
    next_appt.APPT_TIMEFRAME,
    next_appt.APPT_PROVIDER_NAME AS NEXT_APPT_PROVIDER,
    next_appt.DEPARTMENT_NAME AS NEXT_APPT_LOCATION,
    gap_counts.TOTAL_GAPS,
    gap_counts.CRITICAL_GAPS,
    gap_counts.IMPORTANT_GAPS,
    gap_counts.ROUTINE_GAPS,
    CASE 
        WHEN next_appt.DAYS_UNTIL_APPT <= 3 AND gap_counts.CRITICAL_GAPS > 0 THEN 'URGENT'
        WHEN next_appt.DAYS_UNTIL_APPT <= 7 THEN 'HIGH'
        WHEN next_appt.DAYS_UNTIL_APPT <= 30 THEN 'MEDIUM'
        ELSE 'STANDARD'
    END AS OUTREACH_PRIORITY
FROM #PatientBase pb
INNER JOIN #UpcomingAppointments next_appt ON pb.PAT_ID = next_appt.PAT_ID AND next_appt.APPT_RANK = 1
LEFT JOIN (
    SELECT 
        PAT_ID,
        COUNT(*) AS TOTAL_GAPS,
        SUM(CASE WHEN PRIORITY_LEVEL = 1 THEN 1 ELSE 0 END) AS CRITICAL_GAPS,
        SUM(CASE WHEN PRIORITY_LEVEL = 2 THEN 1 ELSE 0 END) AS IMPORTANT_GAPS,
        SUM(CASE WHEN PRIORITY_LEVEL = 3 THEN 1 ELSE 0 END) AS ROUTINE_GAPS
    FROM #AllCareGaps
    GROUP BY PAT_ID
) gap_counts ON pb.PAT_ID = gap_counts.PAT_ID
WHERE gap_counts.TOTAL_GAPS > 0
ORDER BY 
    CASE 
        WHEN next_appt.DAYS_UNTIL_APPT <= 3 AND gap_counts.CRITICAL_GAPS > 0 THEN 1
        WHEN next_appt.DAYS_UNTIL_APPT <= 7 THEN 2
        WHEN next_appt.DAYS_UNTIL_APPT <= 30 THEN 3
        ELSE 4 
    END,
    gap_counts.CRITICAL_GAPS DESC;

-- =====================================================
-- OUTPUT 2: PROVIDER-LEVEL VIEW
-- =====================================================

PRINT '';
PRINT '========================================';
PRINT 'OUTPUT 2: PROVIDER-LEVEL VIEW';
PRINT '========================================';

-- Get top gap per provider
IF OBJECT_ID('tempdb..#TopGapsByProvider') IS NOT NULL DROP TABLE #TopGapsByProvider;

SELECT 
    ua.APPT_PROVIDER_ID,
    ag.GAP_TYPE,
    COUNT(*) AS GAP_COUNT,
    ROW_NUMBER() OVER (PARTITION BY ua.APPT_PROVIDER_ID ORDER BY COUNT(*) DESC) AS GAP_RANK
INTO #TopGapsByProvider
FROM #UpcomingAppointments ua
INNER JOIN #AllCareGaps ag ON ua.PAT_ID = ag.PAT_ID
WHERE ua.APPT_PROVIDER_ID IS NOT NULL
GROUP BY ua.APPT_PROVIDER_ID, ag.GAP_TYPE;

SELECT 
    ua.APPT_PROVIDER_ID,
    ua.APPT_PROVIDER_NAME,
    ua.APPT_CARE_TEAM,
    ua.APPT_TIMEFRAME,
    COUNT(DISTINCT ua.PAT_ID) AS PATIENTS_WITH_GAPS,
    COUNT(DISTINCT ua.PAT_ENC_CSN_ID) AS APPOINTMENTS_WITH_GAPS,
    SUM(CASE WHEN ag.PRIORITY_LEVEL = 1 THEN 1 ELSE 0 END) AS CRITICAL_GAPS,
    SUM(CASE WHEN ag.PRIORITY_LEVEL = 2 THEN 1 ELSE 0 END) AS IMPORTANT_GAPS,
    SUM(CASE WHEN ag.PRIORITY_LEVEL = 3 THEN 1 ELSE 0 END) AS ROUTINE_GAPS,
    topgap.GAP_TYPE AS TOP_GAP_TYPE
FROM #UpcomingAppointments ua
INNER JOIN #AllCareGaps ag ON ua.PAT_ID = ag.PAT_ID
LEFT JOIN #TopGapsByProvider topgap 
    ON ua.APPT_PROVIDER_ID = topgap.APPT_PROVIDER_ID 
    AND topgap.GAP_RANK = 1
WHERE ua.APPT_PROVIDER_ID IS NOT NULL
GROUP BY 
    ua.APPT_PROVIDER_ID,
    ua.APPT_PROVIDER_NAME,
    ua.APPT_CARE_TEAM,
    ua.APPT_TIMEFRAME,
    topgap.GAP_TYPE
ORDER BY ua.APPT_PROVIDER_NAME, ua.APPT_TIMEFRAME;

-- =====================================================
-- OUTPUT 3: SUMMARY STATISTICS
-- =====================================================

PRINT '';
PRINT '========================================';
PRINT 'OUTPUT 3: SUMMARY STATISTICS';
PRINT '========================================';

SELECT 
    'By Priority' AS METRIC_TYPE,
    CASE PRIORITY_LEVEL
        WHEN 1 THEN 'Critical'
        WHEN 2 THEN 'Important'
        WHEN 3 THEN 'Routine'
    END AS PRIORITY_NAME,
    COUNT(DISTINCT PAT_ID) AS PATIENTS_AFFECTED,
    COUNT(*) AS TOTAL_GAPS
FROM #AllCareGaps
GROUP BY PRIORITY_LEVEL
ORDER BY PRIORITY_LEVEL;

SELECT 
    'By Category' AS METRIC_TYPE,
    GAP_CATEGORY,
    COUNT(DISTINCT PAT_ID) AS PATIENTS_AFFECTED,
    COUNT(*) AS TOTAL_GAPS
FROM #AllCareGaps
GROUP BY GAP_CATEGORY
ORDER BY PATIENTS_AFFECTED DESC;

PRINT '';
PRINT '========================================';
PRINT 'PROCEDURE COMPLETE';
PRINT '========================================';

-- =====================================================
-- CLEANUP: Conditionally drop temp tables
-- =====================================================

IF @KeepTempTables = 0
BEGIN
    -- Default: Clean up everything
    IF OBJECT_ID('tempdb..#PatientBase') IS NOT NULL DROP TABLE #PatientBase;
    IF OBJECT_ID('tempdb..#UpcomingAppointments') IS NOT NULL DROP TABLE #UpcomingAppointments;
    IF OBJECT_ID('tempdb..#AllCareGaps') IS NOT NULL DROP TABLE #AllCareGaps;
    IF OBJECT_ID('tempdb..#TopGapsByProvider') IS NOT NULL DROP TABLE #TopGapsByProvider;
    
    PRINT 'Temp tables cleaned up.';
END
ELSE
BEGIN
    -- Preserve for staging procedure reuse
    PRINT 'Temp tables preserved for reuse:';
    PRINT '  - #PatientBase';
    PRINT '  - #UpcomingAppointments';
    PRINT '  - #AllCareGaps';
    PRINT 'NOTE: Calling procedure is responsible for cleanup.';
END

GO