# Analyzer run: optimized

- Regulation : 103296 - Banking Control Law
- Regulator  : SAMA / Laws and Implementing Regulations
- Model      : deepseek/deepseek-v3.2
- Input      : 19,274 chars
- Run at     : 2026-08-03T07:09:26+00:00

## Cost and latency by stage

Seconds are summed across calls; concurrent stages therefore total to more
than the wall clock. Cost is the token-based estimate so nothing is missing.

| Stage | Calls | Sec (sum) | Prompt tok | Completion tok | Total tok | Cost USD | Truncated |
|---|---:|---:|---:|---:|---:|---:|---:|
| stage1_extract | 1 | 95.0 | 4,448 | 2,902 | 7,350 | 0.0022 | - |
| stage2_normalize | 1 | 104.1 | 2,609 | 3,207 | 5,816 | 0.0019 | - |
| stage4_report | 1 | 12.6 | 455 | 221 | 676 | 0.0002 | - |
| stage3_controls | 6 | 188.4 | 3,632 | 4,542 | 8,174 | 0.0026 | - |
| **TOTAL** | **9** | | **11,144** | **10,872** | **22,016** | **0.0069** | **-** |

Wall clock: **253.3s**
OpenRouter-reported cost for the calls it had indexed: $0.0039

<details><summary>Individual calls</summary>

| Call | Stage | Sec | Prompt | Completion | finish_reason | max_tokens |
|---:|---|---:|---:|---:|---|---:|
| 1 | stage1_extract | 95.0 | 4,448 | 2,902 | stop | 16,000 |
| 2 | stage2_normalize | 104.1 | 2,609 | 3,207 | stop | 16,000 |
| 3 | stage4_report | 12.6 | 455 | 221 | stop | 2,500 |
| 4 | stage3_controls | 18.4 | 505 | 416 | stop | 12,000 |
| 5 | stage3_controls#2 | 11.2 | 422 | 195 | stop | 12,000 |
| 6 | stage3_controls#3 | 37.2 | 757 | 915 | stop | 12,000 |
| 7 | stage3_controls#4 | 48.4 | 683 | 1,227 | stop | 12,000 |
| 8 | stage3_controls#5 | 19.6 | 505 | 422 | stop | 12,000 |
| 9 | stage3_controls#6 | 53.6 | 760 | 1,367 | stop | 12,000 |

</details>

## Output shape (quality baseline)

- Requirements        : 7
- Obligations         : 39 (5.57 per requirement)
- Controls designed   : 22
- Mean clarity score  : 4.87
- needs_manual_review : 1
- Empty obligation text: 0
- Criticality         : {"High": 34, "Medium": 5}
- Execution category  : {"Ongoing_Control": 22, "Governance_Approval": 10, "One_Off_Reporting": 4, "One_Time_Implementation": 3}
- Obligation type     : {"Preventive": 19, "Governance": 14, "Reporting": 6}
- Stage 4 report      : 12,689 chars, 71 table rows, all sections present: True
