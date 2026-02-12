-- =====================================================
-- STAGING PROCEDURE FOR DATABRICKS ETL (v3.2 CORRECTED)
-- Fixed: Temp Table Scope Issue
-- Database: CHMCA_CUSTOM
-- =====================================================

USE [CHMCA_CUSTOM]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE OR ALTER PROCEDURE [dbo].[sp_StageCareGapsForDatabricks]
AS

/*
**************************************************************************************************
TITLE:  Stage Pediatric Care Gaps for Databricks ETL (v3.2 CORRECTED)
PURPOSE: 
    Prepare pediatric care gap data for daily Databricks ETL transfer.
    Creates 4 staging tables from care gap calculations.

FIXED IN v3.2:
    • Temp table scope issue - creates temp tables FIRST before calling main procedure
    • Main procedure now populates existing temp tables instead of creating them

ARCHITECTURE:
    1. Create empty temp table structures
    2. Call main procedure to populate them
    3. Use populated temp tables to create staging tables
    4. Staging tables ready for Databricks JDBC extraction

STAGING TABLES CREATED:
    • STG_CareGaps_Daily           - Detailed gap records
    • STG_Appointments_Daily       - Appointments with gap counts
    • STG_PatientGapSummary_Daily  - Patient-level aggregation
    • STG_ProviderMetrics_Daily    - Provider-level metrics

CALLED BY:
    • SQL Server Agent Job (Daily at 6:00 AM)
    • Manual execution for testing

CALLS:
    • sp_PediatricCareGaps_UpcomingAppointments (@KeepTempTables = 1)

USAGE:
    EXEC sp_StageCareGapsForDatabricks

**************************************************************************************************
*/

SET NOCOUNT ON;

DECLARE @StartTime DATETIME = GETDATE();
DECLARE @RowCount INT;
DECLARE @ErrorMessage NVARCHAR(4000);

PRINT '========================================';
PRINT 'DATABRICKS STAGING - CARE GAPS';
PRINT 'Version 3.2 (Temp Table Scope Fixed)';
PRINT 'Started: ' + CONVERT(VARCHAR, @StartTime, 120);
PRINT '========================================';
PRINT '';

-- =====================================================
-- STEP 1: CREATE TEMP TABLE STRUCTURES FIRST
-- =====================================================

PRINT 'Step 1: Creating temp table structures...';

-- Create #PatientBase structure
IF OBJECT_ID('tempdb..#PatientBase') IS NOT NULL DROP TABLE #PatientBase;

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

CREATE INDEX IX_PatientBase_PAT_ID ON #PatientBase(PAT_ID);
CREATE INDEX IX_PatientBase_PCP ON #PatientBase(PCP_ID);
CREATE INDEX IX_PatientBase_AGE ON #PatientBase(AGE_YEARS, AGE_MONTHS);

PRINT '  ✓ #PatientBase structure created';

-- Create #UpcomingAppointments structure
IF OBJECT_ID('tempdb..#UpcomingAppointments') IS NOT NULL DROP TABLE #UpcomingAppointments;

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

CREATE INDEX IX_UpcomingAppts_PAT_ID ON #UpcomingAppointments(PAT_ID);
CREATE INDEX IX_UpcomingAppts_DATE ON #UpcomingAppointments(APPT_DATE);

PRINT '  ✓ #UpcomingAppointments structure created';

-- Create #AllCareGaps structure
IF OBJECT_ID('tempdb..#AllCareGaps') IS NOT NULL DROP TABLE #AllCareGaps;

CREATE TABLE #AllCareGaps (
    PAT_ID VARCHAR(18),
    GAP_TYPE VARCHAR(100),
    GAP_CATEGORY VARCHAR(50),
    GAP_DETAIL VARCHAR(500),
    DAYS_OPEN INT,
    GAP_DATE DATETIME,
    PRIORITY_LEVEL INT
);

CREATE INDEX IX_AllCareGaps_PAT_ID ON #AllCareGaps(PAT_ID);
CREATE INDEX IX_AllCareGaps_PRIORITY ON #AllCareGaps(PRIORITY_LEVEL);

PRINT '  ✓ #AllCareGaps structure created';
PRINT '';

-- =====================================================
-- STEP 2: CALL MAIN PROCEDURE TO POPULATE TEMP TABLES
-- =====================================================

PRINT 'Step 2: Calling main procedure to populate temp tables...';
PRINT '';

BEGIN TRY
    -- Call main procedure with @KeepTempTables = 1
    -- Main procedure will INSERT INTO existing temp tables
    EXEC sp_PediatricCareGaps_UpcomingAppointments 
        @MonthsAhead = 12,
        @KeepTempTables = 1;
    
    PRINT '';
    PRINT 'Main procedure complete.';
END TRY
BEGIN CATCH
    SET @ErrorMessage = ERROR_MESSAGE();
    PRINT 'ERROR calling main procedure: ' + @ErrorMessage;
    THROW;
END CATCH

-- =====================================================
-- STEP 3: VALIDATE TEMP TABLES
-- =====================================================

PRINT '';
PRINT 'Step 3: Validating temp tables...';

-- Validate #PatientBase
IF OBJECT_ID('tempdb..#PatientBase') IS NULL
BEGIN
    RAISERROR('ERROR: #PatientBase temp table not found. Main procedure may have failed.', 16, 1);
    RETURN;
END

SELECT @RowCount = COUNT(*) FROM #PatientBase;
PRINT '  ✓ #PatientBase: ' + CAST(@RowCount AS VARCHAR) + ' patients';

IF @RowCount = 0
BEGIN
    RAISERROR('ERROR: #PatientBase is empty. No patients found.', 16, 1);
    RETURN;
END

-- Validate #UpcomingAppointments
IF OBJECT_ID('tempdb..#UpcomingAppointments') IS NULL
BEGIN
    RAISERROR('ERROR: #UpcomingAppointments temp table not found.', 16, 1);
    RETURN;
END

SELECT @RowCount = COUNT(*) FROM #UpcomingAppointments;
PRINT '  ✓ #UpcomingAppointments: ' + CAST(@RowCount AS VARCHAR) + ' appointments';

-- Validate #AllCareGaps
IF OBJECT_ID('tempdb..#AllCareGaps') IS NULL
BEGIN
    RAISERROR('ERROR: #AllCareGaps temp table not found.', 16, 1);
    RETURN;
END

SELECT @RowCount = COUNT(*) FROM #AllCareGaps;
PRINT '  ✓ #AllCareGaps: ' + CAST(@RowCount AS VARCHAR) + ' gaps';

PRINT '';

-- =====================================================
-- STEP 4: CREATE STAGING TABLES
-- =====================================================

PRINT 'Step 4: Creating staging tables...';

-- Drop existing staging tables
IF OBJECT_ID('dbo.STG_CareGaps_Daily') IS NOT NULL DROP TABLE dbo.STG_CareGaps_Daily;
IF OBJECT_ID('dbo.STG_Appointments_Daily') IS NOT NULL DROP TABLE dbo.STG_Appointments_Daily;
IF OBJECT_ID('dbo.STG_PatientGapSummary_Daily') IS NOT NULL DROP TABLE dbo.STG_PatientGapSummary_Daily;
IF OBJECT_ID('dbo.STG_ProviderMetrics_Daily') IS NOT NULL DROP TABLE dbo.STG_ProviderMetrics_Daily;

-- =====================================================
-- STAGING TABLE 1: STG_CareGaps_Daily
-- Detailed gap records with patient and appointment context
-- =====================================================

SELECT 
    GETDATE() AS LOAD_TIMESTAMP,
    pb.PAT_ID,
    pb.PAT_MRN_ID,
    pb.PAT_NAME,
    pb.BIRTH_DATE,
    pb.AGE_YEARS,
    pb.AGE_MONTHS,
    pb.SEX,
    pb.PCP_NAME,
    pb.PCP_ID,
    pb.PCP_DEPARTMENT,
    pb.PCP_CARE_TEAM_NAME,
    pb.HOME_PHONE,
    pb.EMAIL_ADDRESS,
    ag.GAP_TYPE,
    ag.GAP_CATEGORY,
    ag.GAP_DETAIL,
    ag.DAYS_OPEN,
    ag.GAP_DATE,
    ag.PRIORITY_LEVEL,
    CASE ag.PRIORITY_LEVEL
        WHEN 1 THEN 'Critical'
        WHEN 2 THEN 'Important'
        WHEN 3 THEN 'Routine'
    END AS PRIORITY_NAME,
    ua.APPT_DATE AS NEXT_APPT_DATE,
    ua.DAYS_UNTIL_APPT,
    ua.APPT_TIMEFRAME,
    ua.APPT_PROVIDER_NAME AS NEXT_APPT_PROVIDER,
    ua.DEPARTMENT_NAME AS NEXT_APPT_LOCATION,
    CASE 
        WHEN ua.DAYS_UNTIL_APPT <= 3 AND ag.PRIORITY_LEVEL = 1 THEN 'URGENT'
        WHEN ua.DAYS_UNTIL_APPT <= 7 THEN 'HIGH'
        WHEN ua.DAYS_UNTIL_APPT <= 30 THEN 'MEDIUM'
        ELSE 'STANDARD'
    END AS OUTREACH_PRIORITY
INTO dbo.STG_CareGaps_Daily
FROM #AllCareGaps ag
INNER JOIN #PatientBase pb ON ag.PAT_ID = pb.PAT_ID
LEFT JOIN (
    SELECT PAT_ID, APPT_DATE, DAYS_UNTIL_APPT, APPT_TIMEFRAME, 
           APPT_PROVIDER_NAME, DEPARTMENT_NAME
    FROM #UpcomingAppointments
    WHERE APPT_RANK = 1
) ua ON pb.PAT_ID = ua.PAT_ID;

SELECT @RowCount = COUNT(*) FROM dbo.STG_CareGaps_Daily;
PRINT '  ✓ STG_CareGaps_Daily: ' + CAST(@RowCount AS VARCHAR) + ' records';

-- =====================================================
-- STAGING TABLE 2: STG_Appointments_Daily
-- Appointments with associated gap counts
-- =====================================================

SELECT 
    GETDATE() AS LOAD_TIMESTAMP,
    ua.PAT_ENC_CSN_ID,
    ua.PAT_ID,
    pb.PAT_MRN_ID,
    pb.PAT_NAME,
    ua.APPT_DATE,
    ua.DAYS_UNTIL_APPT,
    ua.APPT_TIMEFRAME,
    ua.APPT_LENGTH_MINUTES,
    ua.DEPARTMENT_NAME,
    ua.APPT_PROVIDER_ID,
    ua.APPT_PROVIDER_NAME,
    ua.APPT_CARE_TEAM,
    ua.ENCOUNTER_TYPE,
    ua.APPT_STATUS,
    gap_counts.TOTAL_GAPS,
    gap_counts.CRITICAL_GAPS,
    gap_counts.IMPORTANT_GAPS,
    gap_counts.ROUTINE_GAPS,
    CASE 
        WHEN ua.DAYS_UNTIL_APPT <= 3 AND gap_counts.CRITICAL_GAPS > 0 THEN 'URGENT'
        WHEN ua.DAYS_UNTIL_APPT <= 7 THEN 'HIGH'
        WHEN ua.DAYS_UNTIL_APPT <= 30 THEN 'MEDIUM'
        ELSE 'STANDARD'
    END AS OUTREACH_PRIORITY
INTO dbo.STG_Appointments_Daily
FROM #UpcomingAppointments ua
INNER JOIN #PatientBase pb ON ua.PAT_ID = pb.PAT_ID
LEFT JOIN (
    SELECT 
        PAT_ID,
        COUNT(*) AS TOTAL_GAPS,
        SUM(CASE WHEN PRIORITY_LEVEL = 1 THEN 1 ELSE 0 END) AS CRITICAL_GAPS,
        SUM(CASE WHEN PRIORITY_LEVEL = 2 THEN 1 ELSE 0 END) AS IMPORTANT_GAPS,
        SUM(CASE WHEN PRIORITY_LEVEL = 3 THEN 1 ELSE 0 END) AS ROUTINE_GAPS
    FROM #AllCareGaps
    GROUP BY PAT_ID
) gap_counts ON ua.PAT_ID = gap_counts.PAT_ID
WHERE gap_counts.TOTAL_GAPS > 0;

SELECT @RowCount = COUNT(*) FROM dbo.STG_Appointments_Daily;
PRINT '  ✓ STG_Appointments_Daily: ' + CAST(@RowCount AS VARCHAR) + ' records';

-- =====================================================
-- STAGING TABLE 3: STG_PatientGapSummary_Daily
-- Patient-level aggregation
-- =====================================================

SELECT 
    GETDATE() AS LOAD_TIMESTAMP,
    pb.PAT_ID,
    pb.PAT_MRN_ID,
    pb.PAT_NAME,
    pb.AGE_YEARS,
    pb.SEX,
    pb.PCP_NAME,
    pb.PCP_ID,
    pb.PCP_DEPARTMENT,
    pb.PCP_CARE_TEAM_NAME,
    COUNT(DISTINCT ag.GAP_TYPE) AS UNIQUE_GAP_TYPES,
    COUNT(*) AS TOTAL_GAPS,
    SUM(CASE WHEN ag.PRIORITY_LEVEL = 1 THEN 1 ELSE 0 END) AS CRITICAL_GAPS,
    SUM(CASE WHEN ag.PRIORITY_LEVEL = 2 THEN 1 ELSE 0 END) AS IMPORTANT_GAPS,
    SUM(CASE WHEN ag.PRIORITY_LEVEL = 3 THEN 1 ELSE 0 END) AS ROUTINE_GAPS,
    MAX(CASE WHEN ag.PRIORITY_LEVEL = 1 THEN 1 ELSE 0 END) AS HAS_CRITICAL_GAP,
    AVG(CAST(ag.DAYS_OPEN AS FLOAT)) AS AVG_DAYS_OPEN,
    MAX(ag.DAYS_OPEN) AS MAX_DAYS_OPEN,
    ua.APPT_DATE AS NEXT_APPT_DATE,
    ua.DAYS_UNTIL_APPT,
    ua.APPT_TIMEFRAME
INTO dbo.STG_PatientGapSummary_Daily
FROM #PatientBase pb
INNER JOIN #AllCareGaps ag ON pb.PAT_ID = ag.PAT_ID
LEFT JOIN (
    SELECT PAT_ID, APPT_DATE, DAYS_UNTIL_APPT, APPT_TIMEFRAME
    FROM #UpcomingAppointments
    WHERE APPT_RANK = 1
) ua ON pb.PAT_ID = ua.PAT_ID
GROUP BY 
    pb.PAT_ID,
    pb.PAT_MRN_ID,
    pb.PAT_NAME,
    pb.AGE_YEARS,
    pb.SEX,
    pb.PCP_NAME,
    pb.PCP_ID,
    pb.PCP_DEPARTMENT,
    pb.PCP_CARE_TEAM_NAME,
    ua.APPT_DATE,
    ua.DAYS_UNTIL_APPT,
    ua.APPT_TIMEFRAME;

SELECT @RowCount = COUNT(*) FROM dbo.STG_PatientGapSummary_Daily;
PRINT '  ✓ STG_PatientGapSummary_Daily: ' + CAST(@RowCount AS VARCHAR) + ' records';

-- =====================================================
-- STAGING TABLE 4: STG_ProviderMetrics_Daily
-- Provider-level metrics and opportunity scores
-- =====================================================

SELECT 
    GETDATE() AS LOAD_TIMESTAMP,
    pb.PCP_ID,
    pb.PCP_NAME,
    pb.PCP_CARE_TEAM_NAME,
    pb.PCP_DEPARTMENT,
    COUNT(DISTINCT pb.PAT_ID) AS TOTAL_PATIENTS_WITH_GAPS,
    COUNT(DISTINCT ag.GAP_TYPE) AS UNIQUE_GAP_TYPES,
    COUNT(*) AS TOTAL_GAPS,
    SUM(CASE WHEN ag.PRIORITY_LEVEL = 1 THEN 1 ELSE 0 END) AS CRITICAL_GAPS,
    SUM(CASE WHEN ag.PRIORITY_LEVEL = 2 THEN 1 ELSE 0 END) AS IMPORTANT_GAPS,
    SUM(CASE WHEN ag.PRIORITY_LEVEL = 3 THEN 1 ELSE 0 END) AS ROUTINE_GAPS,
    -- Opportunity score (weighted by priority and proximity)
    SUM(
        CASE ag.PRIORITY_LEVEL
            WHEN 1 THEN 10
            WHEN 2 THEN 5
            ELSE 1 
        END *
        CASE 
            WHEN ua.DAYS_UNTIL_APPT <= 7 THEN 10
            WHEN ua.DAYS_UNTIL_APPT <= 30 THEN 5
            WHEN ua.DAYS_UNTIL_APPT <= 90 THEN 3
            ELSE 1
        END
    ) AS OPPORTUNITY_SCORE,
    COUNT(DISTINCT CASE WHEN ua.DAYS_UNTIL_APPT <= 7 THEN pb.PAT_ID END) AS PATIENTS_NEXT_7_DAYS,
    COUNT(DISTINCT CASE WHEN ua.DAYS_UNTIL_APPT <= 30 THEN pb.PAT_ID END) AS PATIENTS_NEXT_30_DAYS
INTO dbo.STG_ProviderMetrics_Daily
FROM #PatientBase pb
INNER JOIN #AllCareGaps ag ON pb.PAT_ID = ag.PAT_ID
LEFT JOIN (
    SELECT PAT_ID, DAYS_UNTIL_APPT
    FROM #UpcomingAppointments
    WHERE APPT_RANK = 1
) ua ON pb.PAT_ID = ua.PAT_ID
GROUP BY 
    pb.PCP_ID,
    pb.PCP_NAME,
    pb.PCP_CARE_TEAM_NAME,
    pb.PCP_DEPARTMENT;

SELECT @RowCount = COUNT(*) FROM dbo.STG_ProviderMetrics_Daily;
PRINT '  ✓ STG_ProviderMetrics_Daily: ' + CAST(@RowCount AS VARCHAR) + ' records';

PRINT '';

-- =====================================================
-- STEP 5: CLEANUP
-- =====================================================

PRINT 'Step 5: Cleaning up temp tables...';

IF OBJECT_ID('tempdb..#PatientBase') IS NOT NULL DROP TABLE #PatientBase;
IF OBJECT_ID('tempdb..#UpcomingAppointments') IS NOT NULL DROP TABLE #UpcomingAppointments;
IF OBJECT_ID('tempdb..#AllCareGaps') IS NOT NULL DROP TABLE #AllCareGaps;
IF OBJECT_ID('tempdb..#TopGapsByProvider') IS NOT NULL DROP TABLE #TopGapsByProvider;

PRINT '  ✓ Temp tables dropped';
PRINT '';

-- =====================================================
-- STEP 6: SUMMARY
-- =====================================================

DECLARE @EndTime DATETIME = GETDATE();
DECLARE @DurationSeconds INT = DATEDIFF(SECOND, @StartTime, @EndTime);

PRINT '========================================';
PRINT 'STAGING COMPLETE';
PRINT '========================================';
PRINT 'Duration: ' + CAST(@DurationSeconds AS VARCHAR) + ' seconds';
PRINT 'End Time: ' + CONVERT(VARCHAR, @EndTime, 120);
PRINT '';
PRINT 'Staging tables ready for Databricks ETL:';
PRINT '  • STG_CareGaps_Daily';
PRINT '  • STG_Appointments_Daily';
PRINT '  • STG_PatientGapSummary_Daily';
PRINT '  • STG_ProviderMetrics_Daily';
PRINT '========================================';

GO