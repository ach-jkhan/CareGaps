# CLAUDE.md — Pediatric Care Gaps & Flu Vaccine Piggybacking Project

This file provides Claude Code with full project context. Read this before making any changes.

---

## 🏥 PROJECT OVERVIEW

**Organization:** Akron Children's Hospital (CHMCA)
**Goal:** LLM-powered agentic solution to close pediatric care gaps and drive opportunistic flu vaccination
**Demo Date:** Mid-February 2026
**Tech Stack:** Epic Clarity (SQL Server) → ADF Pipeline → ADLS Parquet → Databricks (Delta/Unity Catalog) → Llama 3.3 70B → Python Agent (MLflow ResponsesAgent)

---

## 📁 PROJECT STRUCTURE

```
CareGaps/
├── agent.py                          # Main agent (current: agent_v9_piggybacking.py)
├── Flu_Piggybacking_PRODUCTION_V2.sql # Core SQL query for flu opportunities
├── HouseholdMembers_PRODUCTION_FINAL.sql  # Validated household matching query
├── ADF_Pipeline_Simple.json          # Azure Data Factory pipeline
├── CLAUDE.md                         # This file
└── sql/
    ├── stored_procs/                 # TODO: wrap SQL into stored procedures
    └── uc_functions/                 # TODO: Unity Catalog function definitions
```

---

## 🗄️ DATA PIPELINE

### Flow
```
Epic Clarity (SQL Server)
    → SQL Stored Procedures (run nightly)
    → ADF Pipeline (Copy activities → Parquet files)
    → ADLS: dev/landing/chmca_custom/caregaps/
    → Databricks ETL Notebook
    → Unity Catalog Delta Tables (dev_kiddo.silver.*)
    → UC Functions (dev_kiddo.silver.function_name)
    → Python Agent (MLflow)
```

### Existing ADF Pipeline: `PL_CareGaps_Simple`
Copies these staging tables from Epic Clarity to ADLS:
- `dbo.STG_CareGaps_Daily` → `care_gaps.parquet`
- `dbo.STG_Appointments_Daily` → `appointments.parquet`
- `dbo.STG_PatientGapSummary_Daily` → `patient_summary.parquet`
- `dbo.STG_ProviderMetrics_Daily` → `provider_metrics.parquet`
- Then runs Databricks notebook: `CareGapsETLSimple`

**TODO:** Add new Copy activity for flu piggybacking:
- Source: `dbo.STG_FluPiggybacking_Daily` (stored proc wrapping V2 SQL)
- Destination: `dev/landing/chmca_custom/caregaps/flu_piggybacking.parquet`

---

## 🦠 FLU VACCINE PIGGYBACKING

### Concept
When a patient (Tommy) has an upcoming appointment, identify household siblings (Sarah, Jake) who are overdue for flu vaccine but have NO upcoming appointment of their own. Send a message to the parent: "Bring Sarah for her flu shot while you're here for Tommy's appointment."

### Key Business Rules
1. **Date range:** September 1, 2025 – March 31, 2026 (flu season)
2. **First appointment only:** Each subject patient uses their EARLIEST appointment in the date range
3. **Sibling exclusion (CRITICAL - agentic logic):** If a sibling already has their own appointment, EXCLUDE them. Do not suggest piggybacking for siblings who are already scheduled. This is what makes it AI vs a dashboard.
4. **Vaccine history check:** Exclude siblings age > 24 months with NO lifetime flu vaccines (vaccine-hesitant, low success rate)
5. **HMT status filter:** Use `PATIENT_HMT_STATUS` where `HMT_DUE_STATUS_C IN (2,3,4)` (Due Soon, Due On, Overdue)
6. **Department filter:** NOT YET APPLIED — will be added later with clinical operations team
7. **Message length:** 160 characters max (SMS)
8. **Asthma flag:** Check J45* ICD codes — mention higher risk in message

### ZC_HMT_DUE_STATUS Codes (Confirmed)
| Code | Name | Use? |
|------|------|------|
| 1 | Not Due | No |
| 2 | Due Soon | ✅ Yes |
| 3 | Due On | ✅ Yes |
| 4 | Overdue | ✅ Yes |
| 5 | Postponed | No |
| 6 | Completed | No |
| 7 | Addressed | No |
| 8 | Aged Out | No |
| 9 | Discontinued | No |
| 10 | Hidden | No |

### Flu HMT Topic IDs (Confirmed from hmt_clarity.xlsx)
- `20` = IMM INFLUENZA
- `24` = FLU

### Epic Table Structures (CONFIRMED — never guess column names)

**PATIENT_HMT_STATUS** (current snapshot, flat table)
- PK: PAT_ID + LINE
- Columns: PAT_ID, QUALIFIED_HMT_ID, HMT_DUE_STATUS_C, IDEAL_RETURN_DT
- Note: QUALIFIED_HMT_ID links to CLARITY_HM_TOPIC.HM_TOPIC_ID
- Note: No NEXT_DUE_DATE column here — use HM_HISTORICAL_STATUS for that

**HM_HISTORICAL_STATUS** (historical records)
- Columns: PAT_ID, LINE, HM_TOPIC_ID, HM_STATUS_C, LAST_COMPLETED_DATE, NEXT_DUE_DATE, SNAPSHOT_DATE, HAS_OUTSIDE_COMPLETION_YN
- Use: Check if patient EVER had flu vaccine via LAST_COMPLETED_DATE
- Get most recent: `ROW_NUMBER() OVER (PARTITION BY PAT_ID, HM_TOPIC_ID ORDER BY SNAPSHOT_DATE DESC)`

**OTHER_COMMUNCTN** (mobile phone — not in PATIENT table)
- Mobile phone: `OTHER_COMMUNCTN.OTHER_COMMUNIC_NUM` where `OTHER_COMMUNIC_C = '1'`
- Join: `LEFT JOIN OTHER_COMMUNCTN ON PAT_ID = PAT_ID AND OTHER_COMMUNIC_C = '1'`
- Note: Use LEFT JOIN (don't exclude patients without mobile phones)
- Home phone: `PATIENT.HOME_PHONE` (confirmed column exists)

**PAT_RELATIONSHIPS** (household matching — HIGH confidence)
- LINE = 1 filter for primary guardian
- PAT_REL_LGL_GUAR_YN = 'Y' for legal guardian
- Match siblings by: PAT_REL_NAME + PAT_REL_HOME_PHONE + PAT_REL_MOBILE_PHNE

### Household Matching (3-Tier)
- **HIGH (99%+ of matches):** PAT_RELATIONSHIPS — shared parent name + phone
- **MEDIUM:** Same ACCOUNT_ID + same patient ADD_LINE_1 + ZIP
- **LOW:** Same ACCOUNT_ID + different addresses

### Guarantor Exclusion Filters
Always apply (case-insensitive):
```sql
AND UPPER(name) NOT LIKE '%COUNTY%'
AND UPPER(name) NOT LIKE 'CSB,%'
AND UPPER(name) NOT LIKE '%,CSB'
AND UPPER(name) NOT LIKE '%, CSB'
AND UPPER(name) NOT LIKE '%CARING FOR KIDS%'
```

### Validation Results (from testing)
- `#AllMatches` (HIGH confidence only): 20,555 rows, 12,344 unique subjects, 14,388 unique siblings
- Multiple rows per subject patient = multiple siblings needing flu vaccine (expected and correct)
- After sibling-already-has-appointment exclusion: expect ~8,000–10,000 TRUE opportunities

---

## 🤖 AGENT ARCHITECTURE

### Current Agent: `agent_v9_piggybacking.py`
- **LLM:** `databricks-meta-llama-3-3-70b-instruct`
- **Framework:** MLflow ResponsesAgent + UCFunctionToolkit
- **UC Catalog:** `dev_kiddo.silver.*`
- **Functions:** 19 total (15 care gaps + 4 flu piggybacking)

### UC Functions (Care Gaps — existing, confirmed working)
```
dev_kiddo.silver.get_top_providers
dev_kiddo.silver.get_patient_360
dev_kiddo.silver.get_gap_categories
dev_kiddo.silver.get_provider_gaps
dev_kiddo.silver.get_long_open_gaps
dev_kiddo.silver.get_outreach_needed
dev_kiddo.silver.get_appointments_with_gaps
dev_kiddo.silver.get_critical_gaps
dev_kiddo.silver.search_patients
dev_kiddo.silver.get_gaps_by_type
dev_kiddo.silver.get_gap_statistics
dev_kiddo.silver.get_department_summary
dev_kiddo.silver.get_gaps_by_age
dev_kiddo.silver.get_gaps_no_appointments
dev_kiddo.silver.get_patient_gaps
```

### UC Functions (Flu Piggybacking — TODO: create these)
```
dev_kiddo.silver.get_flu_statistics        # Campaign metrics
dev_kiddo.silver.search_flu_opportunities  # Look up by MRN
dev_kiddo.silver.get_flu_opportunities     # List opportunities
dev_kiddo.silver.get_patient_flu_history   # Flu vaccine history
```

### Agent Design Principle
The chat agent handles ANALYTICAL queries only (read-only).
Campaign operations (review, approve, send messages) belong in the DASHBOARD.
Agent boundary rule in system prompt: redirect "send message" requests to the flu campaign dashboard.

---

## 📊 WORKFLOW: CHAT vs DASHBOARD

| Task | Tool |
|------|------|
| Review today's sibling opportunities | **Dashboard** |
| Approve / send MyChart messages | **Dashboard** |
| Track vaccines administered | **Dashboard** |
| Generate personalized message TEXT | **LLM (background, silent)** |
| "How is the flu campaign going?" | **Chat agent** |
| "Show flu opportunities for MRN 12345" | **Chat agent** |
| "Any asthma siblings at Beachwood this week?" | **Chat agent** |

### LLM Message Generation (Background, Not Chat)
Runs nightly in Databricks ETL notebook after ADF pipeline.
Calls Llama 3.3 to generate 160-char personalized message per opportunity.
Stores in `llm_message` column of Delta table.
Dashboard displays pre-generated message for admin to review → approve → send.

---

## 📋 DESIRED OUTPUT FORMAT (from PowerPoint slides 7-9)

### Flu Piggybacking Output Columns
| Column | Source |
|--------|--------|
| MRN | IDENTITY_ID (type 40) |
| Patient or shared address? | "Patient" if self, "Shared Address" if sibling |
| MyChart Active Y/N | PATIENT_MYC |
| Mobile number on file | OTHER_COMMUNCTN + PATIENT.HOME_PHONE |
| Upcoming appointment date | F_SCHED_APPT |
| Upcoming appointment location | CLARITY_DEP |
| Last administered influenza vaccine date | HM_HISTORICAL_STATUS.LAST_COMPLETED_DATE |
| Any ICD code matching J45* Y/N | PAT_ENC_DX + CLARITY_EDG |
| LLM-generated suggested message | Generated by Llama 3.3 70B |

### LLM Message Prompt (from slide 8)
> Use cheerful and positive but professional language. Remind patients that they did a great job getting their flu vaccine for the previous season on <last_vaccine_date>, but that each vaccine only provides one year of protection. If has asthma, remind that asthma puts them at higher risk for severe influenza. Remind them they have an appointment coming up at <location> on <date> and they can get their flu shot while there. Limit to 160 characters. If "Shared Address", frame as "someone in your household has an appointment."

---

## ⚠️ STRICT RULES — NEVER VIOLATE

1. **NEVER guess Epic column names.** Always ask if unsure. Confirmed columns listed above.
2. **NEVER use INNER JOIN for phone/contact tables** — always LEFT JOIN (don't exclude patients without phones).
3. **Always apply guarantor exclusion filters** (COUNTY, CSB, CARING FOR KIDS).
4. **Always use LINE = 1** when querying PAT_RELATIONSHIPS.
5. **Always use first appointment per subject patient** (ROW_NUMBER PARTITION BY SUBJECT_PATIENT_ID ORDER BY APPT_DTTM ASC).
6. **Always exclude siblings who have their own appointments** (NOT EXISTS subquery against #UpcomingAppointments).

---

## 🚧 TODO LIST

### Immediate (Before Demo)
- [ ] Wrap `Flu_Piggybacking_PRODUCTION_V2.sql` in stored procedure `dbo.STG_FluPiggybacking_Daily`
- [ ] Add Copy activity to ADF pipeline for flu piggybacking
- [ ] Update Databricks ETL notebook to load flu parquet → Delta table
- [ ] Create 4 flu UC functions in `dev_kiddo.silver`
- [ ] Add `llm_message` column generation to ETL notebook
- [ ] Validate department names (CLARITY_DEP) match slide 7 location list
- [ ] Confirm QUALIFIED_HMT_ID values 20, 24 are correct for flu

### Post-Demo
- [ ] Add department filter once confirmed with clinical operations
- [ ] Build flu campaign dashboard (Databricks SQL or Power BI)
- [ ] Add outcomes tracking (vaccine administered on appointment day)
- [ ] Expand to lab piggybacking use case
- [ ] Expand to depression screening (PHQ-9, HM_TOPIC_ID 126, 127)

---

## 📞 KEY DECISIONS MADE

1. **HMT over CVX codes:** Use Epic's PATIENT_HMT_STATUS instead of manually checking CVX codes. Epic handles age/dose/timing logic automatically.
2. **First appointment only:** Each subject patient uses earliest appointment to avoid duplicate messages.
3. **Agentic exclusion:** Siblings with their own appointments are excluded. This is the core "AI vs dashboard" differentiator.
4. **Likely acceptors only:** Exclude age >24 months with no lifetime flu vaccines (vaccine-hesitant, low ROI).
5. **Dashboard for ops, chat for analytics:** Campaign send/approve workflow = dashboard. Ad-hoc questions = chat agent.

---

## 🔗 RELATED FILES IN THIS SESSION

- `Flu_Piggybacking_PRODUCTION_V2.sql` — Core production SQL query
- `HouseholdMembers_PRODUCTION_FINAL.sql` — Standalone household matching
- `agent_v9_piggybacking.py` — Latest agent with flu functions added
- `ADF_Pipeline_Simple.json` — Existing ADF pipeline to extend
- `WORKFLOW_RECOMMENDATION.md` — Chat vs dashboard decision rationale
- `AGENTIC_EXCLUSION_LOGIC.md` — Why sibling exclusion makes this AI not a report
