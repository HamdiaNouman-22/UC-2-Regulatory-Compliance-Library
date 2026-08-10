# Banking Control Law (reg 103296) — actual output, before vs after

Everything here is copied verbatim from the two runs. Raw files:

- `benchmarks/runs/baseline/` — before any changes
- `benchmarks/runs/optimized/` — after the changes

Each folder holds `stage4.md` (the executive report), `rows.json` (exactly what would be written
to the DB), `input_clean_text.txt` (the text fed in), and `calls/` with the raw prompt and raw
model reply for every LLM call.

---

## 1. Requirement groups extracted

| # | Before | After |
|---|---|---|
| 1 | Licensing and Authorization | Licensing and Authorization |
| 2 | Capital, Reserves, and Deposit Limits | Capital, Reserves, and Liquidity |
| 3 | Credit and Exposure Limits | Credit and Exposure Limits |
| 4 | Prohibited Activities and Investments | Prohibited Activities and Investments |
| 5 | Approval-Based Operations and Changes | Governance and Personnel |
| 6 | Audit and Reporting | Reporting and Disclosure |
| 7 | Information Provision and Inspection | Prior Approvals and Notifications |

**Before: 7 groups, 38 obligations. After: 7 groups, 39 obligations.**

Same seven topic groups, near-identical titles. This is the part that had to not regress, and it
didn't.

---

## 2. Controls designed — the headline difference

**Before: 0 controls. After: 22 controls.**

The before run produced *none*. Stage 3 hit its token limit, the JSON came back cut in half,
the parse failed, and the error handler returned an empty result. All 7 rows have
`stage3_json = {}`. You can see the truncation for yourself — the last line of
`benchmarks/runs/baseline/calls/03_stage3_controls_completion.txt` stops mid-sentence:

```
..."control": {
                        "control_title": "SAMA Information Request Management Process",
                        "control_objective": "To ensure all ad-hoc information requests from SAMA are identified, responded to accurately, and submitted
```

### The 22 controls the new run produced

| # | Req | Control title | Owner | Type | Execution | Frequency | Residual risk |
|---:|---|---|---|---|---|---|---|
| 1 | REQ-001 | Automated Screening for Unlicensed Banking Activity | Compliance Department | Preventive | Hybrid | Daily | High |
| 2 | REQ-001 | Marketing and Branding Material Review for Unauthorized Bank Terminology | Marketing & Communications Department | Detective | Manual | Per-Transaction | Medium |
| 3 | REQ-002 | Deposit-to-Capital Ratio Monitoring | Treasury Department | Detective | Hybrid | Monthly | High |
| 4 | REQ-002 | Capital Increase or SAMA Deposit Remediation Plan | Chief Financial Officer (CFO) Office | Preventive | Manual | Event-Driven | High |
| 5 | REQ-002 | Statutory Deposit Balance Reconciliation | Treasury Department | Detective | Hybrid | Daily | High |
| 6 | REQ-002 | Liquidity Reserve Composition and Sufficiency Check | Asset-Liability Management (ALM) Committee | Detective | Hybrid | Weekly | High |
| 7 | REQ-002 | Liquidity Reserve Asset Eligibility Validation | Treasury Department | Preventive | Hybrid | Per-Transaction | Medium |
| 8 | REQ-002 | Pre-Dividend Statutory Reserve Transfer Verification | Financial Control Department | Preventive | Manual | Annually | Medium |
| 9 | REQ-003 | Large Exposure Limit Monitoring | Credit Risk Management | Preventive | Automated | Per-Transaction | High |
| 10 | REQ-003 | Prohibition on Lending Against Own Shares | Collateral Management | Preventive | Hybrid | Per-Transaction | Medium |
| 11 | REQ-003 | Unsecured Lending to Directors and Auditors Control | Compliance | Preventive | Hybrid | Per-Transaction | High |
| 12 | REQ-003 | Unsecured Lending to Director/Auditor-Related Entities Control | Legal & Compliance | Detective | Hybrid | Event-Driven | High |
| 13 | REQ-003 | Unsecured Lending with Director/Auditor as Guarantor Control | Credit Administration | Preventive | Manual | Per-Transaction | Medium |
| 14 | REQ-003 | Employee Unsecured Lending Limit Control | Retail Banking | Preventive | Automated | Per-Transaction | Low |
| 15 | REQ-004 | Trade Activity Prohibition Review | Compliance Department | Preventive | Hybrid | Event-Driven | High |
| 16 | REQ-004 | Non-Banking Investment Holding Review | Treasury Department | Detective | Manual | Quarterly | Medium |
| 17 | REQ-004 | Equity Investment Concentration Limit Monitoring | Investment Department | Detective | Automated | Quarterly | High |
| 18 | REQ-004 | Real Estate Acquisition Authorization | Real Estate Committee | Preventive | Manual | Event-Driven | Medium |
| 19 | REQ-004 | Non-Essential Real Estate Liquidation Tracking | Real Estate Department | Detective | Hybrid | Semi-Annually | Medium |
| 20 | REQ-005 | Board Membership Conflict of Interest Control | Corporate Governance Department | Preventive | Hybrid | Event-Driven | High |
| 21 | REQ-006 | SAMA Information Request Management | Compliance Department | Preventive | Hybrid | Event-Driven | High |
| 22 | REQ-006 | Staff Cooperation with Regulatory Examinations | Internal Audit Department | Preventive | Manual | Event-Driven | High |

### One control in full, so you can judge the depth

```json
{
  "control_title": "Automated Screening for Unlicensed Banking Activity",
  "control_objective": "To prevent unlicensed entities from conducting banking business through the bank's channels.",
  "control_description": "An automated system screens all new and existing customer relationships and transaction patterns against a database of licensed financial institutions and known red flags for unauthorized banking. Alerts are generated for investigation by the compliance team to confirm no business is being conducted with unlicensed entities.",
  "control_owner": "Compliance Department",
  "control_type": "Preventive",
  "execution_type": "Hybrid",
  "frequency": "Daily",
  "control_level": "System",
  "evidence_generated": "System screening logs and investigation reports for generated alerts.",
  "key_steps": [
    "System scans customer database and transaction feeds daily.",
    "Matches are flagged against the internal 'Licensed Entities' list.",
    "Compliance officer investigates and documents all alerts.",
    "Confirmed unlicensed relationships are escalated for termination.",
    "The licensed entities list is updated quarterly from SAMA.",
    ""
  ],
  "residual_risk_if_failed": "High",
  "req": "REQ-001"
}
```

---

## 3. The fabrication problem, side by side

Both runs produced a section 5 'Control Engineering Summary'. The before run had **zero** controls
in its data — yet its report listed ten, with owners, frequencies and risk ratings. None of them
existed anywhere in the pipeline.

**BEFORE — invented from an empty input:**

```
| Control Title | Owner | Type | Execution | Frequency | Residual Risk |
| :--- | :--- | :--- | :--- | :--- | :--- |
| Banking License Verification | Legal & Compliance | Preventive | Automated Check | Continuous | Low |
| License Application Procedure | Legal & Compliance | Governance | Manual Procedure | On-demand | Low |
| Restricted Term Usage Control | Marketing / Compliance | Preventive | Automated & Manual Review | Ongoing / Periodic | Medium |
| Capital & Deposit Ratio Monitor | Finance / Risk | Preventive | Automated Calculation & Alert | Daily | Medium |
| Capital Breach Remediation Procedure | Finance / Treasury | Corrective | Manual Procedure | Event-driven | High |
| Statutory Deposit Compliance | Treasury | Preventive | Automated Reconciliation | Daily | Low |
| Liquidity Reserve Compliance | Treasury | Preventive | Automated Reconciliation | Daily | Low |
| Statutory Reserve Allocation | Finance | Governance | Automated Allocation | Quarterly / Annually | Low |
| Profit Distribution Authorization | Finance / Board | Preventive | Manual Governance | Annually | Low |
| Single Borrower Exposure Limit | Credit Risk | Preventive | Automated Limit Check | Transactional | M
```

**AFTER — rendered from the 22 real controls:**

```
| Control Title | Owner | Type | Execution | Frequency | Residual Risk |
| --- | --- | --- | --- | --- | --- |
| Automated Screening for Unlicensed Banking Activity | Compliance Department | Preventive | Hybrid | Daily | High |
| Marketing and Branding Material Review for Unauthorized Bank Terminology | Marketing & Communications Department | Detective | Manual | Per-Transaction | Medium |
| Deposit-to-Capital Ratio Monitoring | Treasury Department | Detective | Hybrid | Monthly | High |
| Capital Increase or SAMA Deposit Remediation Plan | Chief Financial Officer (CFO) Office | Preventive | Manual | Event-Driven | High |
| Statutory Deposit Balance Reconciliation | Treasury Department | Detective | Hybrid | Daily | High |
| Liquidity Reserve Composition and Sufficiency Check | Asset-Liability Management (ALM) Committee | Detective | Hybrid | Weekly | High |
| Liquidity Reserve Asset Eligibility Validation | Treasury Department | Preventive | Hybrid | Per-Transaction | Medium |
| Pre-Dividend Statutory Reserve Transfer Verification | Financial Control Department | Preventive | Manual | Annually | Medium |
| Large Exposure Limit Monitoring | Credit Risk Management | Preventive | Aut
```

The after version is built in Python from the stored rows, so a title can only appear if the
control exists. Verified: 22 controls in data, 22 rows in the table, zero unbacked entries.

---

## 4. Obligation inventory completeness

- Before: stage 4 table had **29** table rows total
- After: **71** table rows total

The before run fed stage 4 its data truncated to 3,000 characters, so most obligations never
reached the report. The after run renders every one.

---

## 5. Taxonomy violations

The stage 2 prompt permits exactly five `obligation_type` values: Detective, Documentation, Governance, Preventive, Reporting.

**Before — 4 violation(s):**

- `REQ-002-OB-002-A` → `Corrective`
- `REQ-002-OB-002-B` → `Corrective`
- `REQ-004-OB-002-DISPOSAL` → `Corrective`
- `REQ-004-OB-006` → `Corrective`

**After — 0 violation(s).** Out-of-taxonomy values are now coerced and the obligation flagged for review.

---

## 6. Same obligation, both runs

To show the classification work is unchanged in character, here is the first obligation from each:

**BEFORE**

```json
{
  "obligation_id": "REQ-001-OB-001",
  "obligation_text": "No person, natural or juristic, unlicensed in accordance with the provisions of this Law, shall carry on basically any of the banking business.",
  "obligation_type": "Preventive",
  "criticality": "High",
  "evidence_expected": [
    "License",
    "Record"
  ],
  "test_method": "Auditor verifies that the bank holds a valid banking license issued by SAMA.",
  "clarity_score": 5,
  "needs_manual_review": false,
  "source_reference": "Article 2",
  "execution_category": "One_Time_Implementation",
  "req": "REQ-001",
  "req_title": "Licensing and Authorization"
}
```

**AFTER**

```json
{
  "obligation_id": "REQ-001-OB-001",
  "obligation_text": "No person, natural or juristic, unlicensed in accordance with the provisions of this Law, shall carry on basically any of the banking business.",
  "obligation_type": "Preventive",
  "criticality": "High",
  "evidence_expected": [
    "Policy",
    "Procedure",
    "Record"
  ],
  "test_method": "An auditor reviews the bank's licensing documentation and customer onboarding procedures to ensure no unlicensed banking business is conducted.",
  "clarity_score": 4,
  "needs_manual_review": false,
  "source_reference": "Article 2",
  "execution_category": "Ongoing_Control",
  "req": "REQ-001",
  "req_title": "Licensing and Authorization"
}
```

---

## 7. Full executive reports

Read them directly — they are the clearest single comparison:

- Before: `benchmarks/runs/baseline/stage4.md`
- After: `benchmarks/runs/optimized/stage4.md`

### Before — sections present and their sizes

**Before**

| Section | Chars | Table rows |
|---|---:|---:|
| 1. Executive Summary | 666 | - |
| 2. Requirement Overview | 620 | - |
| 3. Obligation Inventory | 2,830 | 11 |
| 4. Execution Classification Summary | 185 | 2 |
| 5. Control Engineering Summary | 1,207 | 10 |
| 6. Architectural & Operational Implications | 1,452 | - |

**After**

| Section | Chars | Table rows |
|---|---:|---:|
| 1. Executive Summary | 341 | - |
| 2. Requirement Overview | 445 | - |
| 3. Obligation Inventory | 8,181 | 39 |
| 4. Execution Classification Summary | 281 | 4 |
| 5. Control Engineering Summary | 2,773 | 22 |
| 6. Architectural & Operational Implications | 401 | - |
