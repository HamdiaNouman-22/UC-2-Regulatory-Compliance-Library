# Analyzer run: baseline

- Regulation : 103296 - Banking Control Law
- Regulator  : SAMA / Laws and Implementing Regulations
- Model      : deepseek/deepseek-v3.2
- Input      : 19,274 chars
- Run at     : 2026-08-03T06:15:39+00:00

## Cost and latency by stage

Seconds are summed across calls; concurrent stages therefore total to more
than the wall clock. Cost is the token-based estimate so nothing is missing.

| Stage | Calls | Sec (sum) | Prompt tok | Completion tok | Total tok | Cost USD | Truncated |
|---|---:|---:|---:|---:|---:|---:|---:|
| stage1_extract | 1 | 50.8 | 4,447 | 2,868 | 7,315 | 0.0022 | - |
| stage2_normalize | 1 | 229.0 | 3,024 | 7,177 | 10,201 | 0.0035 | - |
| stage3_controls | 1 | 278.6 | 5,731 | 8,000 | 13,731 | 0.0045 | 1 |
| stage4_report | 1 | 55.5 | 1,931 | 1,582 | 3,513 | 0.0011 | - |
| **TOTAL** | **4** | | **15,133** | **19,627** | **34,760** | **0.0113** | **1** |

Wall clock: **614.5s**
OpenRouter-reported cost for the calls it had indexed: $0.0096

<details><summary>Individual calls</summary>

| Call | Stage | Sec | Prompt | Completion | finish_reason | max_tokens |
|---:|---|---:|---:|---:|---|---:|
| 1 | stage1_extract | 50.8 | 4,447 | 2,868 | stop | 16,000 |
| 2 | stage2_normalize | 229.0 | 3,024 | 7,177 | stop | 16,000 |
| 3 | stage3_controls | 278.6 | 5,731 | 8,000 | length **TRUNCATED** | 8,000 |
| 4 | stage4_report | 55.5 | 1,931 | 1,582 | stop | 4,000 |

</details>

## Output shape (quality baseline)

- Requirements        : 7
- Obligations         : 38 (5.43 per requirement)
- Controls designed   : 0
- Mean clarity score  : 4.68
- needs_manual_review : 2
- Empty obligation text: 0
- Criticality         : {"High": 31, "Medium": 7}
- Execution category  : {"Ongoing_Control": 20, "Governance_Approval": 8, "One_Time_Implementation": 6, "One_Off_Reporting": 4}
- Obligation type     : {"Preventive": 18, "Governance": 10, "Reporting": 6, "Corrective": 4}
- Stage 4 report      : 7,342 chars, 29 table rows, all sections present: True
