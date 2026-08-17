# Comparison: baseline -> optimized

Regulation 103296 - Banking Control Law

## Cost and latency

| Metric | baseline | optimized | Change |
|---|---:|---:|---:|
| Wall clock (s) | 614.5500 | 253.3000 | -58.8% |
| LLM calls | 4 | 9 | +125.0% |
| Prompt tokens | 15,133 | 11,144 | -26.4% |
| Completion tokens | 19,627 | 10,872 | -44.6% |
| Total tokens | 34,760 | 22,016 | -36.7% |
| Cost (USD, token-based) | 0.0113 | 0.0069 | -38.6% |
| Truncated calls | 1 | 0 | -100.0% |

## Output shape -- these should stay materially the same

| Metric | baseline | optimized | Change |
|---|---:|---:|---:|
| Requirements | 7 | 7 | +0.0% |
| Obligations | 38 | 39 | +2.6% |
| Controls | 0 | 22 | - |
| Mean clarity | 4.6800 | 4.8700 | +4.1% |
| needs_manual_review | 2 | 1 | -50.0% |
| Stage 4 table rows | 29 | 71 | +144.8% |

**Criticality**

- baseline: {"High": 31, "Medium": 7}
- optimized: {"High": 34, "Medium": 5}

**Execution category**

- baseline: {"Ongoing_Control": 20, "Governance_Approval": 8, "One_Time_Implementation": 6, "One_Off_Reporting": 4}
- optimized: {"Ongoing_Control": 22, "Governance_Approval": 10, "One_Off_Reporting": 4, "One_Time_Implementation": 3}
