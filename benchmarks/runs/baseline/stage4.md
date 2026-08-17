# Regulatory Impact Analysis Report

**Regulatory Source:** Banking Control Law (SAMA)
**Reference:** M/5
**Publication Date:** 11/6/1966
**Analysis Date:** [Current Date]

## 1. Executive Summary
This document provides a formal impact analysis of key requirements from the Banking Control Law (SAMA M/5). The analysis covers three primary regulatory domains: Licensing and Authorization, Capital and Reserve Management, and Credit Exposure Limits. The obligations identified mandate strict governance, preventive controls, and corrective actions to ensure financial stability and regulatory compliance. High-criticality obligations dominate, primarily requiring ongoing operational controls managed by Finance, Treasury, and Risk Management functions. Implementation will necessitate enhancements to policy frameworks, monitoring procedures, and control systems.

## 2. Requirement Overview
*   **REQ-001: Licensing and Authorization:** Governs the fundamental legality of conducting banking business, including licensing applications and restrictions on the use of banking terminology.
*   **REQ-002: Capital, Reserves, and Deposit Limits:** Establishes prudential limits for leverage (deposit-to-capital ratio), mandates statutory deposits and liquidity reserves with SAMA, and sets rules for profit distribution and reserve allocation.
*   **REQ-003: Credit and Exposure Limits:** Imposes restrictions on the concentration of credit risk by limiting loans and exposures to single parties or connected groups.

## 3. Obligation Inventory
| Obligation ID | Text | Type | Criticality | Execution Category |
| :--- | :--- | :--- | :--- | :--- |
| REQ-001-OB-001 | No person, natural or juristic, unlicensed in accordance with the provisions of this Law, shall carry on basically any of the banking business. | Preventive | High | One_Time_Implementation |
| REQ-001-OB-002 | All applications, for the grant of licenses to carry on banking business in the Kingdom, shall be addressed to SAMA. | Governance | High | One_Time_Implementation |
| REQ-001-OB-003 | Any person not authorized basically to carry on banking business in the Kingdom is not allowed to use the word "Bank", or its synonyms, or any similar term in any language on his papers or printed matter, or in his commercial address, or his name or in his advertisements. | Preventive | Medium | Ongoing_Control |
| REQ-002-OB-001 | The deposit liabilities of a bank shall not exceed fifteen times its reserves and paid-up or invested capital. | Preventive | High | Ongoing_Control |
| REQ-002-OB-002-A | If the deposit liabilities exceed the prescribed limit, the bank must within one month of the date of submission of the statement referred to in paragraph 1 of Article 15, increase its capital and reserves to the prescribed limit. | Corrective | High | One_Time_Implementation |
| REQ-002-OB-002-B | If the deposit liabilities exceed the prescribed limit, the bank must within one month of the date of submission of the statement referred to in paragraph 1 of Article 15, deposit fifty percent of the excess with SAMA. | Corrective | High | One_Time_Implementation |
| REQ-002-OB-003 | Every bank shall maintain with SAMA at all times a statutory deposit of a sum not less than fifteen percent of its deposit liabilities. | Preventive | High | Ongoing_Control |
| REQ-002-OB-004 | In addition to the statutory deposit provided for in the previous paragraph, every bank shall maintain a liquidity reserve of not less than 15 percent of its deposit liabilities. | Preventive | High | Ongoing_Control |
| REQ-002-OB-005 | Every bank shall, before declaring distribution of any profits, transfer a sum equal to not less than 25 percent of its net profits, to the statutory reserve, until the amount of that reserve equals as a minimum of the paid-up capital. | Governance | High | Ongoing_Control |
| REQ-002-OB-006 | No bank shall pay dividends or remit any part of its profits abroad, until its capital expenditures including aggregate foundation expenditures and losses incurred have been completely written off. | Preventive | High | Ongoing_Control |
| REQ-003-OB-001 | No bank shall grant a loan or extend a credit to any one person, natural or juristic, or a group of connected persons, exceeding 25 percent of the bank's reserves and paid-up or invested capital. | Preventive | High | Ongoing_Control |

## 4. Execution Classification Summary
| Category | Count | Primary Owner |
| :--- | :--- | :--- |
| Ongoing_Control | 7 | Finance / Treasury / Risk Management |
| One_Time_Implementation | 5 | Legal & Compliance / Finance |

## 5. Control Engineering Summary
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
| Single Borrower Exposure Limit | Credit Risk | Preventive | Automated Limit Check | Transactional | Medium |

## 6. Architectural & Operational Implications
*   **Data & System Architecture:** Requires integrated data feeds from core banking, treasury, and financial reporting systems to calculate ratios (e.g., deposit-to-capital, large exposures) in real-time or near-real-time. A centralized regulatory reporting data mart is recommended.
*   **Policy & Procedure Framework:** Must establish and document clear policies for license management, capital planning, reserve allocation, profit distribution, and credit approval that explicitly incorporate these legal limits.
*   **Governance & Oversight:** Board and senior management committees (ALCO, Risk Committee) require enhanced reporting dashboards tracking compliance with these prudential limits. Clear escalation paths for breaches are mandatory.
*   **Finance & Treasury Operations:** Treasury functions must automate the monitoring and reporting of statutory deposits and liquidity reserves with SAMA. Finance must embed checks for reserve transfers and profit distribution conditions into the financial closing process.
*   **Credit Risk Management:** Credit approval systems and processes must be configured with hard stops or mandatory overrides for exposures exceeding 25% of capital, requiring robust party identification and grouping logic.
*   **Compliance Monitoring:** The compliance function must implement ongoing surveillance of marketing materials and public-facing communications for unauthorized use of restricted terms like "Bank".
