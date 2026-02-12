/*******************************************************************************
 * QUERY 1: LAB PIGGYBACKING OPPORTUNITIES (FIXED - NO DUPLICATES)
 * 
 * Purpose: Identify patients with upcoming appointments who have outstanding 
 *          lab orders that could be completed at the same location
 *
 * FIXES:
 * - Added outpatient filter (user requested)
 * - Deduplicates: Only shows MOST RECENT order per patient per lab test
 * - Prevents same patient/lab showing multiple times
 *
 * Author: Jawad Khan
 * Date: January 20, 2026 (Updated)
 ******************************************************************************/

WITH UpcomingAppointments AS (
    -- Get appointments in next 14-30 days with patient and guarantor info
    SELECT 
        appt.PAT_ENC_CSN_ID AS CSN,
        appt.PAT_ID,
        appt.APPT_DTTM,
        appt.DEPARTMENT_ID,
        appt.PROV_ID,
        apptStatus.NAME AS APPT_STATUS,
        prc.PRC_NAME AS APPT_TYPE,
        dep.DEPARTMENT_NAME,
        guarantor.ACCOUNT_ID,
        guarantor.ACCOUNT_NAME AS GUARANTOR_NAME,
        guarantorAddress.ADDRESS AS GUARANTOR_ADDRESS,
        guarantor.CITY AS GUARANTOR_CITY,
        guarantorState.NAME AS GUARANTOR_STATE,
        guarantor.ZIP AS GUARANTOR_ZIP
    FROM F_SCHED_APPT appt
    INNER JOIN ZC_APPT_STATUS apptStatus 
        ON appt.APPT_STATUS_C = apptStatus.APPT_STATUS_C 
        AND apptStatus.NAME <> 'Canceled'
    INNER JOIN CLARITY_PRC prc 
        ON appt.PRC_ID = prc.PRC_ID
    INNER JOIN CLARITY_DEP dep
        ON appt.DEPARTMENT_ID = dep.DEPARTMENT_ID
    LEFT JOIN ACCOUNT guarantor 
        INNER JOIN ZC_ACCOUNT_TYPE guarantorAcctType 
            ON guarantor.ACCOUNT_TYPE_C = guarantorAcctType.ACCOUNT_TYPE_C
        INNER JOIN ACCT_ADDR guarantorAddress 
            ON guarantor.ACCOUNT_ID = guarantorAddress.ACCOUNT_ID 
            AND guarantorAddress.ADDRESS_LINE = 1
        INNER JOIN ZC_STATE guarantorState 
            ON guarantor.STATE_C = guarantorState.STATE_C
        ON appt.ACCOUNT_ID = guarantor.ACCOUNT_ID
    WHERE appt.APPT_DTTM BETWEEN DATEADD(DAY, 1, GETDATE()) AND DATEADD(DAY, 30, GETDATE())
),

OutstandingLabsRaw AS (
    -- Get ALL outstanding lab orders (not canceled, not finalized)
    -- BEFORE deduplication
    SELECT 
        flab.ORDER_ID,
        flab.PAT_ID,
        flab.ORDER_DTTM,
        flab.AUTHRZING_PROV_ID,
        flab.PROC_ID,
        COALESCE(flab.SUBMITTER_ID, flab.LOGIN_DEP_ID) AS ORDERING_DEPT_ID,
        CLARITY_EAP.PROC_NAME AS LAB_NAME,
        CLARITY_EAP.ORDER_DISPLAY_NAME AS LAB_DISPLAY_NAME,
        flab.ORDER_STATUS_C,
        flab.LAB_STATUS_C,
        patClass.NAME AS PATIENT_CLASS,
        DATEDIFF(DAY, flab.ORDER_DTTM, GETDATE()) AS DAYS_PENDING,
        
        -- Add row number to identify most recent order per patient per lab
        ROW_NUMBER() OVER (
            PARTITION BY flab.PAT_ID, CLARITY_EAP.PROC_NAME 
            ORDER BY flab.ORDER_DTTM DESC, flab.ORDER_ID DESC
        ) AS RN
        
    FROM F_LAB_ORDERS flab
    LEFT JOIN CLARITY_EAP 
        ON flab.PROC_ID = CLARITY_EAP.PROC_ID
    LEFT JOIN PAT_ENC_2 enc 
        INNER JOIN ZC_PAT_CLASS patClass 
            ON enc.ADT_PAT_CLASS_C = patClass.ADT_PAT_CLASS_C
        ON flab.PAT_ENC_CSN_ID = enc.PAT_ENC_CSN_ID
    WHERE flab.ORDER_STATUS_C NOT IN (4, 9)  -- Not canceled (4) or discontinued (9)
      AND (flab.LAB_STATUS_C NOT IN (3, 5) OR flab.LAB_STATUS_C IS NULL)  -- Not finalized (3) or complete (5)
      AND (flab.ACTV_EXCLUDE_FROM_CDS_REASON_C NOT IN (1, 2) OR flab.ACTV_EXCLUDE_FROM_CDS_REASON_C IS NULL)  -- Not wrong patient or unsuccessful
      AND flab.ORDER_DTTM > DATEADD(DAY, -90, GETDATE())  -- Not too old (< 90 days)
      AND flab.PAT_ID IS NOT NULL
      AND patClass.NAME = 'Outpatient'  -- OUTPATIENT ONLY (user requested)
),

OutstandingLabs AS (
    -- Keep only MOST RECENT order per patient per lab
    -- This eliminates duplicates like patient having same lab ordered twice
    SELECT 
        ORDER_ID,
        PAT_ID,
        ORDER_DTTM,
        AUTHRZING_PROV_ID,
        PROC_ID,
        ORDERING_DEPT_ID,
        LAB_NAME,
        LAB_DISPLAY_NAME,
        ORDER_STATUS_C,
        LAB_STATUS_C,
        PATIENT_CLASS,
        DAYS_PENDING
    FROM OutstandingLabsRaw
    WHERE RN = 1  -- Only keep most recent order per patient per lab
),

LabCapableDepartments AS (
    -- Departments that can draw labs (from LAB_PROFILE)
    SELECT DISTINCT
        lp.LAB_LINK_DEP_ID AS DEPARTMENT_ID,
        lp.LAB_NAME
    FROM LAB_PROFILE lp 
    WHERE lp.LAB_STATUS_C = 1  -- Active
      AND lp.EXTERNAL_LAB_YN = 'N'  -- Internal lab, not send-out
),

PatientInfo AS (
    -- Get patient demographics and MyChart status
    SELECT 
        pat.PAT_ID,
        idd.IDENTITY_ID AS MRN,
        pat.PAT_NAME,
        pat.ADD_LINE_1,
        pat.CITY,
        pat.STATE_C,
        pat.ZIP,
        pat.HOME_PHONE,
        mychartStatus.NAME AS MYCHART_STATUS
    FROM PATIENT pat
    INNER JOIN IDENTITY_ID idd 
        ON pat.PAT_ID = idd.PAT_ID 
        AND idd.IDENTITY_TYPE_ID = 40  -- Enterprise MRN
    LEFT JOIN PATIENT_MYC mychart 
        INNER JOIN ZC_MYCHART_STATUS mychartStatus 
            ON mychart.MYCHART_STATUS_C = mychartStatus.MYCHART_STATUS_C
        ON pat.PAT_ID = mychart.PAT_ID
)

-- Final query: Match appointments with outstanding labs
SELECT 
    -- Patient Info
    pi.MRN,
    pi.PAT_NAME,
    pi.HOME_PHONE,
    pi.MYCHART_STATUS,

    -- Lab Order Info
    labs.ORDER_ID AS LAB_ORDER_ID,
    labs.ORDER_DTTM AS LAB_ORDER_DATE,
    labs.DAYS_PENDING,
    COALESCE(labs.LAB_DISPLAY_NAME, labs.LAB_NAME) AS LAB_TEST_NAME,
    orderingDep.DEPARTMENT_NAME AS ORDERING_DEPARTMENT,
    orderingProv.PROV_NAME AS ORDERING_PROVIDER,
    
    -- Appointment Info
    appt.CSN,
    appt.APPT_DTTM,
    appt.DEPARTMENT_NAME AS APPT_DEPARTMENT,
    appt.APPT_TYPE,
    
    -- Guarantor Info (for household context)
    appt.GUARANTOR_NAME,
    appt.GUARANTOR_ADDRESS,
    appt.GUARANTOR_CITY,
    appt.GUARANTOR_STATE,
    appt.GUARANTOR_ZIP,
    
    -- Location Capability
    CASE 
        WHEN labDepts.DEPARTMENT_ID IS NOT NULL THEN 'YES'
        ELSE 'NO'
    END AS LOCATION_CAN_DRAW_LAB,
    
    -- Opportunity Score (higher = better opportunity)
    CASE 
        WHEN labDepts.DEPARTMENT_ID IS NOT NULL THEN  -- Can draw at this location
            CASE 
                WHEN labs.DAYS_PENDING > 60 THEN 100  -- Very overdue
                WHEN labs.DAYS_PENDING > 30 THEN 75   -- Overdue
                WHEN labs.DAYS_PENDING > 14 THEN 50   -- Pending
                ELSE 25                                -- Recent
            END
        ELSE 0  -- Can't draw at this location
    END AS OPPORTUNITY_SCORE,
    
    -- Routing Recommendation
    CASE 
        WHEN pi.MYCHART_STATUS = 'Active' THEN 'MyChart Message'
        WHEN pi.HOME_PHONE IS NOT NULL THEN 'Phone Call'
        ELSE 'In-Visit Alert'
    END AS CONTACT_METHOD

FROM UpcomingAppointments appt
INNER JOIN OutstandingLabs labs
    ON appt.PAT_ID = labs.PAT_ID
INNER JOIN PatientInfo pi
    ON appt.PAT_ID = pi.PAT_ID
LEFT JOIN LabCapableDepartments labDepts
    ON appt.DEPARTMENT_ID = labDepts.DEPARTMENT_ID
LEFT JOIN CLARITY_DEP orderingDep
    ON labs.ORDERING_DEPT_ID = orderingDep.DEPARTMENT_ID
LEFT JOIN CLARITY_SER orderingProv
    ON labs.AUTHRZING_PROV_ID = orderingProv.PROV_ID

WHERE 
    -- Only include if lab was ordered by DIFFERENT department (avoid duplicates)
    labs.ORDERING_DEPT_ID != appt.DEPARTMENT_ID
    -- Only show opportunities where location CAN draw the lab
    AND labDepts.DEPARTMENT_ID IS NOT NULL

ORDER BY 
    OPPORTUNITY_SCORE DESC,
    appt.APPT_DTTM ASC,
    labs.DAYS_PENDING DESC;
