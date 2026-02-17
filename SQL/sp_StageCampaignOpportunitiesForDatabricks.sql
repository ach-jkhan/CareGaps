-- =====================================================
-- STAGING PROCEDURE FOR DATABRICKS ETL - CAMPAIGN OPPORTUNITIES
-- Wraps Flu_Piggybacking_PRODUCTION_V2 into unified staging table
-- Follows same pattern as sp_StageCareGapsForDatabricks
-- Database: CHMCA_CUSTOM
-- =====================================================

USE [CHMCA_CUSTOM]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE OR ALTER PROCEDURE [dbo].[sp_StageCampaignOpportunitiesForDatabricks]
AS

/*
**************************************************************************************************
TITLE:  Stage Campaign Opportunities for Databricks ETL
PURPOSE:
    Prepare campaign opportunity data for daily Databricks ETL transfer.
    Wraps flu piggybacking logic into a unified staging table that supports
    multiple campaign types (FLU_VACCINE, LAB_PIGGYBACKING, DEPRESSION_SCREENING).

ARCHITECTURE:
    1. Run flu piggybacking logic (from Flu_Piggybacking_PRODUCTION_V2)
    2. Map output columns to unified campaign_opportunities schema
    3. Write to STG_CampaignOpportunities_Daily staging table
    4. ADF extracts staging table → ADLS parquet → Databricks Delta table

STAGING TABLE CREATED:
    - STG_CampaignOpportunities_Daily  (unified for all campaign types)

TARGET DATABRICKS TABLE:
    - dev_kiddo.silver.campaign_opportunities

CALLED BY:
    - SQL Server Agent Job (Daily at 6:00 AM, after sp_StageCareGapsForDatabricks)
    - ADF Pipeline: PL_CareGaps_Simple (new Copy activity)
    - Manual execution for testing

USAGE:
    EXEC sp_StageCampaignOpportunitiesForDatabricks

**************************************************************************************************
*/

SET NOCOUNT ON;

DECLARE @StartTime DATETIME = GETDATE();
DECLARE @RowCount INT;
DECLARE @ErrorMessage NVARCHAR(4000);

PRINT '========================================';
PRINT 'DATABRICKS STAGING - CAMPAIGN OPPORTUNITIES';
PRINT 'Version 1.0';
PRINT 'Started: ' + CONVERT(VARCHAR, @StartTime, 120);
PRINT '========================================';
PRINT '';

-- =====================================================
-- STEP 1: CLEANUP TEMP TABLES
-- =====================================================

PRINT 'Step 1: Cleaning up any existing temp tables...';

IF OBJECT_ID('tempdb..#AllAppointments') IS NOT NULL DROP TABLE #AllAppointments;
IF OBJECT_ID('tempdb..#UpcomingAppointments') IS NOT NULL DROP TABLE #UpcomingAppointments;
IF OBJECT_ID('tempdb..#SubjectPatients') IS NOT NULL DROP TABLE #SubjectPatients;
IF OBJECT_ID('tempdb..#AllMatches') IS NOT NULL DROP TABLE #AllMatches;
IF OBJECT_ID('tempdb..#HouseholdMembers') IS NOT NULL DROP TABLE #HouseholdMembers;
IF OBJECT_ID('tempdb..#SiblingFluStatus') IS NOT NULL DROP TABLE #SiblingFluStatus;
IF OBJECT_ID('tempdb..#FluOpportunities') IS NOT NULL DROP TABLE #FluOpportunities;

PRINT '  Done.';
PRINT '';

-- =====================================================
-- STEP 2: FLU PIGGYBACKING LOGIC
-- (From Flu_Piggybacking_PRODUCTION_V2.sql)
-- =====================================================

BEGIN TRY

    -- ============================================================================
    -- 2A: Get Upcoming Appointments (next 30 days)
    -- ============================================================================
    PRINT 'Step 2A: Getting upcoming appointments...';

    SELECT
        appt.PAT_ID AS SUBJECT_PATIENT_ID,
        appt.PAT_ENC_CSN_ID AS CSN,
        appt.APPT_DTTM,
        appt.ACCOUNT_ID,
        dep.DEPARTMENT_ID,
        dep.DEPARTMENT_NAME AS VACCINE_LOCATION
    INTO #AllAppointments
    FROM CLARITY..F_SCHED_APPT appt
    INNER JOIN CLARITY..ZC_APPT_STATUS apptStatus
        ON appt.APPT_STATUS_C = apptStatus.APPT_STATUS_C
        AND apptStatus.NAME <> 'Canceled'
    INNER JOIN CLARITY..CLARITY_DEP dep
        ON appt.DEPARTMENT_ID = dep.DEPARTMENT_ID
        AND dep.INPATIENT_DEPT_YN = 'N'
        AND dep.DEPARTMENT_NAME NOT LIKE 'ZZZ%'
    WHERE
        appt.APPT_DTTM BETWEEN DATEADD(DAY, 1, GETDATE()) AND DATEADD(DAY, 30, GETDATE())
        AND appt.ACCOUNT_ID IS NOT NULL;

    SET @RowCount = @@ROWCOUNT;
    PRINT '  All appointments: ' + CAST(@RowCount AS VARCHAR) + ' rows';

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
                ORDER BY APPT_DTTM ASC
            ) AS APPT_RANK
        FROM #AllAppointments
    ) ranked
    WHERE APPT_RANK = 1;

    DROP TABLE #AllAppointments;

    CREATE INDEX IDX_SUBJECT ON #UpcomingAppointments(SUBJECT_PATIENT_ID);
    CREATE INDEX IDX_ACCT ON #UpcomingAppointments(ACCOUNT_ID);

    SET @RowCount = (SELECT COUNT(*) FROM #UpcomingAppointments);
    PRINT '  First appointments (unique patients): ' + CAST(@RowCount AS VARCHAR);
    PRINT '';

    -- ============================================================================
    -- 2B: Get Subject Patient Details (Guarantor + Address)
    -- ============================================================================
    PRINT 'Step 2B: Getting subject patient details...';

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
    INNER JOIN CLARITY..ACCOUNT acct
        ON ua.ACCOUNT_ID = acct.ACCOUNT_ID
    INNER JOIN CLARITY..ACCT_ADDR acctAddr
        ON acct.ACCOUNT_ID = acctAddr.ACCOUNT_ID
        AND acctAddr.ADDRESS_LINE = 1
    INNER JOIN CLARITY..PATIENT subjectPat
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

    SET @RowCount = (SELECT COUNT(*) FROM #SubjectPatients);
    PRINT '  Subject patients: ' + CAST(@RowCount AS VARCHAR);
    PRINT '';

    -- ============================================================================
    -- 2C: Find Household Members (3-Tier Matching)
    -- ============================================================================
    PRINT 'Step 2C: Finding household members...';

    -- HIGH Confidence - PAT_RELATIONSHIPS
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
    INNER JOIN CLARITY..PAT_RELATIONSHIPS pr_subject
        ON sp.SUBJECT_PATIENT_ID = pr_subject.PAT_ID
        AND pr_subject.LINE = 1
        AND pr_subject.PAT_REL_LGL_GUAR_YN = 'Y'
        AND pr_subject.PAT_REL_NAME IS NOT NULL
        AND UPPER(pr_subject.PAT_REL_NAME) NOT LIKE '%COUNTY%'
        AND UPPER(pr_subject.PAT_REL_NAME) NOT LIKE 'CSB,%'
        AND UPPER(pr_subject.PAT_REL_NAME) NOT LIKE '%,CSB'
        AND UPPER(pr_subject.PAT_REL_NAME) NOT LIKE '%, CSB'
        AND UPPER(pr_subject.PAT_REL_NAME) NOT LIKE '%CARING, FOR KIDS%'
    INNER JOIN CLARITY..PAT_RELATIONSHIPS pr_sibling
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
        NOT EXISTS (
            SELECT 1
            FROM #UpcomingAppointments sibling_appt
            WHERE sibling_appt.SUBJECT_PATIENT_ID = pr_sibling.PAT_ID
        );

    SET @RowCount = (SELECT COUNT(*) FROM #AllMatches);
    PRINT '  HIGH confidence matches: ' + CAST(@RowCount AS VARCHAR);

    -- MEDIUM Confidence - Same Guarantor + Same Address
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
        FROM CLARITY..F_SCHED_APPT
        WHERE ACCOUNT_ID IS NOT NULL
    ) siblingAppts
        ON siblingAppts.ACCOUNT_ID = sp.ACCOUNT_ID
        AND siblingAppts.PAT_ID != sp.SUBJECT_PATIENT_ID
    INNER JOIN CLARITY..PATIENT siblingPat
        ON siblingPat.PAT_ID = siblingAppts.PAT_ID
        AND siblingPat.BIRTH_DATE > DATEADD(YEAR, -22, GETDATE())
        AND siblingPat.ADD_LINE_1 = sp.SUBJECT_PATIENT_ADDRESS
        AND siblingPat.ZIP = sp.SUBJECT_PATIENT_ZIP
    WHERE
        NOT EXISTS (
            SELECT 1
            FROM #UpcomingAppointments sibling_appt
            WHERE sibling_appt.SUBJECT_PATIENT_ID = siblingPat.PAT_ID
        );

    PRINT '  + MEDIUM confidence added';

    -- LOW Confidence - Same Guarantor, Different Address
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
        FROM CLARITY..F_SCHED_APPT
        WHERE ACCOUNT_ID IS NOT NULL
    ) siblingAppts
        ON siblingAppts.ACCOUNT_ID = sp.ACCOUNT_ID
        AND siblingAppts.PAT_ID != sp.SUBJECT_PATIENT_ID
    INNER JOIN CLARITY..PATIENT siblingPat
        ON siblingPat.PAT_ID = siblingAppts.PAT_ID
        AND siblingPat.BIRTH_DATE > DATEADD(YEAR, -22, GETDATE())
        AND (siblingPat.ADD_LINE_1 != sp.SUBJECT_PATIENT_ADDRESS
             OR siblingPat.ZIP != sp.SUBJECT_PATIENT_ZIP
             OR siblingPat.ADD_LINE_1 IS NULL
             OR siblingPat.ZIP IS NULL)
    WHERE
        NOT EXISTS (
            SELECT 1
            FROM #UpcomingAppointments sibling_appt
            WHERE sibling_appt.SUBJECT_PATIENT_ID = siblingPat.PAT_ID
        );

    PRINT '  + LOW confidence added';

    SET @RowCount = (SELECT COUNT(*) FROM #AllMatches);
    PRINT '  Total matches (all tiers): ' + CAST(@RowCount AS VARCHAR);

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

    SET @RowCount = (SELECT COUNT(*) FROM #HouseholdMembers);
    PRINT '  Deduplicated household members: ' + CAST(@RowCount AS VARCHAR);
    PRINT '';

    -- ============================================================================
    -- 2D: Check Flu Vaccine Status (HMT + Historical)
    -- ============================================================================
    PRINT 'Step 2D: Checking flu vaccine status...';

    SELECT
        hm.SUBJECT_PATIENT_ID,
        hm.SIBLING_ID,
        hm.CSN,
        hm.APPT_DTTM,
        hm.VACCINE_LOCATION,
        hm.CONFIDENCE_LEVEL,

        siblingPat.BIRTH_DATE AS SIBLING_BIRTH_DATE,
        DATEDIFF(YEAR, siblingPat.BIRTH_DATE, GETDATE()) -
            CASE WHEN DATEADD(YEAR, DATEDIFF(YEAR, siblingPat.BIRTH_DATE, GETDATE()), siblingPat.BIRTH_DATE) > GETDATE()
            THEN 1 ELSE 0 END AS SIBLING_AGE_YEARS,
        DATEDIFF(MONTH, siblingPat.BIRTH_DATE, GETDATE()) AS SIBLING_AGE_MONTHS,

        hmt_current.HMT_DUE_STATUS_C AS CURRENT_STATUS_C,
        status_current.NAME AS CURRENT_STATUS_NAME,
        hmt_current.IDEAL_RETURN_DT,

        hmt_hist.LAST_COMPLETED_DATE AS LAST_FLU_VACCINE_DATE,
        hmt_hist.NEXT_DUE_DATE,
        hmt_hist.HAS_OUTSIDE_COMPLETION_YN,

        CASE
            WHEN hmt_hist.LAST_COMPLETED_DATE IS NOT NULL THEN 1
            ELSE 0
        END AS HAS_LIFETIME_FLU_VACCINE,

        CASE
            WHEN EXISTS (
                SELECT 1
                FROM CLARITY..PAT_ENC_DX dx
                INNER JOIN CLARITY..CLARITY_EDG edg ON dx.DX_ID = edg.DX_ID
                WHERE dx.PAT_ID = hm.SIBLING_ID
                  AND edg.CURRENT_ICD10_LIST LIKE 'J45%'
            ) THEN 1
            ELSE 0
        END AS HAS_ASTHMA

    INTO #SiblingFluStatus
    FROM #HouseholdMembers hm
    INNER JOIN CLARITY..PATIENT siblingPat
        ON hm.SIBLING_ID = siblingPat.PAT_ID
    LEFT JOIN CLARITY..PATIENT_HMT_STATUS hmt_current
        ON hm.SIBLING_ID = hmt_current.PAT_ID
        AND hmt_current.QUALIFIED_HMT_ID IN (20, 24)
    LEFT JOIN CLARITY..ZC_HMT_DUE_STATUS status_current
        ON hmt_current.HMT_DUE_STATUS_C = status_current.HMT_DUE_STATUS_C
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
        FROM CLARITY..HM_HISTORICAL_STATUS
        WHERE HM_TOPIC_ID IN (20, 24)
    ) hmt_hist
        ON hm.SIBLING_ID = hmt_hist.PAT_ID
        AND hmt_hist.RN = 1
    WHERE
        hmt_current.HMT_DUE_STATUS_C IN (2, 3, 4)  -- Due Soon, Due On, Overdue
        AND NOT (
            DATEDIFF(MONTH, siblingPat.BIRTH_DATE, GETDATE()) > 24
            AND hmt_hist.LAST_COMPLETED_DATE IS NULL
        );

    SET @RowCount = (SELECT COUNT(*) FROM #SiblingFluStatus);
    PRINT '  Siblings with overdue flu vaccine: ' + CAST(@RowCount AS VARCHAR);
    PRINT '';

    -- ============================================================================
    -- 2E: Create Final Flu Opportunities
    -- ============================================================================
    PRINT 'Step 2E: Building final flu opportunities...';

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

        CASE
            WHEN sfs.SIBLING_ID = sfs.SUBJECT_PATIENT_ID THEN 'Patient'
            ELSE 'Shared Address'
        END AS PATIENT_OR_SHARED_ADDRESS,

        mychartStatus.NAME AS MYCHART_ACTIVE,

        CASE
            WHEN siblingPat.HOME_PHONE IS NOT NULL OR mobilePhone.OTHER_COMMUNIC_NUM IS NOT NULL THEN 'Y'
            ELSE 'N'
        END AS MOBILE_NUMBER_ON_FILE,

        sfs.LAST_FLU_VACCINE_DATE AS LAST_ADMINISTERED_INFLUENZA_VACCINE_DATE,

        CASE
            WHEN sfs.HAS_ASTHMA = 1 THEN 'Y'
            ELSE 'N'
        END AS ANY_ICD_CODE_J45,

        RTRIM(sfs.CONFIDENCE_LEVEL) AS CONFIDENCE_LEVEL,

        -- LLM Prompt Context (for message generation in Databricks ETL)
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
    INNER JOIN CLARITY..PATIENT subjectPat
        ON sfs.SUBJECT_PATIENT_ID = subjectPat.PAT_ID
    INNER JOIN CLARITY..PATIENT siblingPat
        ON sfs.SIBLING_ID = siblingPat.PAT_ID
    LEFT JOIN CLARITY..OTHER_COMMUNCTN mobilePhone
        ON siblingPat.PAT_ID = mobilePhone.PAT_ID
        AND mobilePhone.OTHER_COMMUNIC_C = '1'
    INNER JOIN CLARITY..IDENTITY_ID subjectPat_idd
        ON subjectPat.PAT_ID = subjectPat_idd.PAT_ID
        AND subjectPat_idd.IDENTITY_TYPE_ID = 40
    INNER JOIN CLARITY..IDENTITY_ID siblingPat_idd
        ON siblingPat.PAT_ID = siblingPat_idd.PAT_ID
        AND siblingPat_idd.IDENTITY_TYPE_ID = 40
    LEFT JOIN CLARITY..PATIENT_MYC mychart
        ON siblingPat.PAT_ID = mychart.PAT_ID
    LEFT JOIN CLARITY..ZC_MYCHART_STATUS mychartStatus
        ON mychart.MYCHART_STATUS_C = mychartStatus.MYCHART_STATUS_C
    LEFT JOIN CLARITY..ZC_SEX sex
        ON siblingPat.SEX_C = sex.RCPT_MEM_SEX_C;

    SET @RowCount = (SELECT COUNT(*) FROM #FluOpportunities);
    PRINT '  Flu opportunities: ' + CAST(@RowCount AS VARCHAR);
    PRINT '';

END TRY
BEGIN CATCH
    SET @ErrorMessage = ERROR_MESSAGE();
    PRINT 'ERROR in flu piggybacking logic: ' + @ErrorMessage;
    THROW;
END CATCH

-- =====================================================
-- STEP 3: CREATE UNIFIED STAGING TABLE
-- Maps flu output columns to campaign_opportunities schema
-- =====================================================

PRINT 'Step 3: Creating unified staging table...';

IF OBJECT_ID('dbo.STG_CampaignOpportunities_Daily') IS NOT NULL
    DROP TABLE dbo.STG_CampaignOpportunities_Daily;

SELECT
    GETDATE() AS LOAD_TIMESTAMP,

    -- Campaign metadata
    'FLU_VACCINE' AS campaign_type,
    'pending' AS status,

    -- Target patient (sibling who needs vaccine)
    fo.MRN AS patient_mrn,
    fo.SIBLING_NAME AS patient_name,
    fo.AGE_YEARS AS age_years,
    fo.SEX AS sex,

    -- Relationship
    fo.PATIENT_OR_SHARED_ADDRESS AS relationship_type,

    -- Subject patient (has the appointment)
    fo.SUBJECT_MRN AS subject_mrn,
    fo.SUBJECT_NAME AS subject_name,

    -- Appointment details
    fo.UPCOMING_APPOINTMENT_DATE AS appointment_date,
    fo.UPCOMING_APPOINTMENT_LOCATION AS appointment_location,

    -- Contact info
    fo.MYCHART_ACTIVE AS mychart_active,
    fo.MOBILE_NUMBER_ON_FILE AS mobile_number_on_file,

    -- Clinical context
    fo.CONFIDENCE_LEVEL AS confidence_level,
    fo.ANY_ICD_CODE_J45 AS has_asthma,
    fo.LAST_ADMINISTERED_INFLUENZA_VACCINE_DATE AS last_flu_vaccine_date,

    -- LLM message (NULL here, generated in Databricks ETL notebook)
    CAST(NULL AS VARCHAR(500)) AS llm_message,

    -- LLM prompt context (for Databricks ETL to generate messages)
    fo.LLM_PROMPT_CONTEXT AS llm_prompt_context,

    -- Timestamps
    GETDATE() AS created_date

INTO dbo.STG_CampaignOpportunities_Daily
FROM #FluOpportunities fo;

SELECT @RowCount = COUNT(*) FROM dbo.STG_CampaignOpportunities_Daily;
PRINT '  STG_CampaignOpportunities_Daily: ' + CAST(@RowCount AS VARCHAR) + ' records';
PRINT '';

-- =====================================================
-- STEP 4: CLEANUP TEMP TABLES
-- =====================================================

PRINT 'Step 4: Cleaning up temp tables...';

IF OBJECT_ID('tempdb..#AllAppointments') IS NOT NULL DROP TABLE #AllAppointments;
IF OBJECT_ID('tempdb..#UpcomingAppointments') IS NOT NULL DROP TABLE #UpcomingAppointments;
IF OBJECT_ID('tempdb..#SubjectPatients') IS NOT NULL DROP TABLE #SubjectPatients;
IF OBJECT_ID('tempdb..#AllMatches') IS NOT NULL DROP TABLE #AllMatches;
IF OBJECT_ID('tempdb..#HouseholdMembers') IS NOT NULL DROP TABLE #HouseholdMembers;
IF OBJECT_ID('tempdb..#SiblingFluStatus') IS NOT NULL DROP TABLE #SiblingFluStatus;
IF OBJECT_ID('tempdb..#FluOpportunities') IS NOT NULL DROP TABLE #FluOpportunities;

PRINT '  Temp tables dropped.';
PRINT '';

PRINT '';
PRINT 'STAGING COMPLETE: STG_CampaignOpportunities_Daily ready for ADF extraction.';

GO
