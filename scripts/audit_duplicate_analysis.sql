-- ===================================================================== --
--  AUDIT: damage already done by the archive-without-retire bug          --
--                                                                        --
--  Written 2026-08-10 alongside the fix in                               --
--  MSSQLRepository.archive_current_analysis (storage/mssql_repo.py).      --
--                                                                        --
--  WHAT THE BUG WAS                                                      --
--    archive_current_analysis COPIED the current compliance_analysis rows --
--    into compliance_analysis_versions and never retired the originals.   --
--    Every reader filters `is_current = 1`, so after a document update a  --
--    regulation returned BOTH its old and its new requirement set as      --
--    live. The next update archived both again.                          --
--                                                                        --
--  The code fix stops this happening again. It does NOT clean up rows     --
--  already in this state -- that is what this file is for.                --
--                                                                        --
--  EVERY STATEMENT HERE IS READ-ONLY. A read-only login is sufficient.    --
--  Nothing below modifies a single row. The cleanup is deliberately NOT   --
--  written yet: which set to keep is a compliance judgement, and it       --
--  should be a separate reviewed change once the scale is known.          --
-- ===================================================================== --


-- --------------------------------------------------------------------- --
-- 1. THE HEADLINE NUMBER. How many regulations are affected?            --
--    Expect 0 if the bug never fired. Anything above 0 is real damage.  --
-- --------------------------------------------------------------------- --
SELECT COUNT(*) AS affected_regulations
FROM (
    SELECT regulation_id
    FROM   compliance_analysis
    WHERE  is_current = 1
      AND  schema_version = 'v2'
    GROUP  BY regulation_id
    HAVING COUNT(DISTINCT ISNULL(version_id, -1)) > 1
) x;


-- --------------------------------------------------------------------- --
-- 2. THE DETAIL. One row per affected regulation, worst first.          --
--                                                                      --
--    live_sets  = how many distinct content versions are simultaneously --
--                 marked current. Should always be 1.                   --
--    live_rows  = total requirement rows showing as live. If live_sets   --
--                 is 3, roughly two thirds of these are stale.          --
--                                                                      --
--    NOTE on ISNULL(version_id, -1): version_id is NULL for every       --
--    regulator except CBB, and COUNT(DISTINCT) ignores NULLs -- so      --
--    without this, a non-CBB regulation with three stacked NULL-version --
--    sets counts as 0 distinct versions and hides from this report.     --
-- --------------------------------------------------------------------- --
SELECT
    ca.regulation_id,
    r.title,
    r.source_system,
    COUNT(DISTINCT ISNULL(ca.version_id, -1)) AS live_sets,
    COUNT(*)                                  AS live_rows,
    MIN(ca.created_at)                        AS oldest_live_row,
    MAX(ca.created_at)                        AS newest_live_row
FROM       compliance_analysis ca
LEFT JOIN  regulations r ON r.id = ca.regulation_id
WHERE      ca.is_current = 1
  AND      ca.schema_version = 'v2'
GROUP BY   ca.regulation_id, r.title, r.source_system
HAVING     COUNT(DISTINCT ISNULL(ca.version_id, -1)) > 1
ORDER BY   live_rows DESC, ca.regulation_id;


-- --------------------------------------------------------------------- --
-- 3. THE SECOND-ORDER DAMAGE. The archive table was growing             --
--    quadratically, because each update re-archived everything that was --
--    still (wrongly) marked current.                                    --
--                                                                      --
--    A regulation appearing here with copies >> its real requirement    --
--    count is the quadratic growth, visible.                            --
-- --------------------------------------------------------------------- --
SELECT TOP 50
    regulation_id,
    COUNT(*)                       AS archived_rows,
    COUNT(DISTINCT version_id)     AS archived_versions,
    COUNT(*) / NULLIF(COUNT(DISTINCT version_id), 0) AS rows_per_version
FROM     compliance_analysis_versions
GROUP BY regulation_id
ORDER BY archived_rows DESC;


-- --------------------------------------------------------------------- --
-- 4. ARCHIVED ROWS WRONGLY FLAGGED CURRENT.                            --
--    The archive INSERT copied `is_current` verbatim from the source     --
--    row, so archived rows landed flagged current. Harmless today (no    --
--    reader of the versions table filters on it) but wrong, and fixed    --
--    going forward. This counts the historical rows.                    --
-- --------------------------------------------------------------------- --
SELECT
    COUNT(*)                                          AS archived_rows_total,
    SUM(CASE WHEN is_current = 1 THEN 1 ELSE 0 END)    AS wrongly_flagged_current
FROM compliance_analysis_versions;


-- --------------------------------------------------------------------- --
-- 5. DUPLICATE SUGGESTED REQUIREMENTS (the A2 bug).                    --
--    ref_key used to be AUTO-<regulation_id>-<loop index>, re-inserted   --
--    unconditionally on every re-analysis.                              --
--                                                                      --
--    The fix changes the key to AUTO-<regulation_id>-<md5 of the text>,  --
--    so these old index-keyed rows will NOT be matched by the new        --
--    upsert -- they are orphans. This is the count that decides whether  --
--    a cleanup is worth writing.                                        --
-- --------------------------------------------------------------------- --
SELECT
    COUNT(*)                                                AS auto_requirements_total,
    SUM(CASE WHEN REF_KEY LIKE 'AUTO-%-[0-9]%'   THEN 1 ELSE 0 END) AS old_index_keyed,
    COUNT(DISTINCT REF_KEY)                                 AS distinct_ref_keys
FROM COMPLIANCE_REQUIREMENT
WHERE REF_KEY LIKE 'AUTO-%';

-- Exact-duplicate DESCRIPTIONs -- the same obligation text inserted more than
-- once under different index keys. These are what the content hash prevents.
SELECT TOP 50
    REF_NO,
    LEFT(DESCRIPTION, 120) AS description_start,
    COUNT(*)               AS copies
FROM     COMPLIANCE_REQUIREMENT
WHERE    REF_KEY LIKE 'AUTO-%'
GROUP BY REF_NO, LEFT(DESCRIPTION, 120)
HAVING   COUNT(*) > 1
ORDER BY copies DESC;


-- --------------------------------------------------------------------- --
-- 6. DUPLICATE REQUIREMENT MAPPINGS (the other half of A2).            --
--    store_requirement_mappings was insert-only, so re-analysis         --
--    appended a whole second set. version_id is NULL for every          --
--    regulator except CBB, so for those the sets are indistinguishable  --
--    -- which is exactly why the fix scopes its cleanup by version_id.   --
-- --------------------------------------------------------------------- --
SELECT TOP 50
    regulation_id,
    COUNT(*)                                     AS mapping_rows,
    COUNT(DISTINCT extracted_requirement_text)   AS distinct_texts,
    COUNT(*) - COUNT(DISTINCT extracted_requirement_text) AS surplus_rows
FROM     sama_requirement_mapping
GROUP BY regulation_id
HAVING   COUNT(*) > COUNT(DISTINCT extracted_requirement_text)
ORDER BY surplus_rows DESC;
