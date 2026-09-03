-- Add match_confidence to sama_requirement_mapping.
--
-- WHY
--   Requirement matching disagrees with itself on 2-3 of every 39 obligations.
--   Measured: temperature 0 + fixed seed + pinned provider made agreement
--   marginally WORSE (95% -> 92%), because the disagreements are not sampling
--   noise -- they are genuine ties. Two similar reporting obligations compete
--   for the same internal requirement and the prompt has no rule for which
--   wins. See docs/determinism.md.
--
--   Caching the verdict (done, in processor/analysis_cache.py) makes the answer
--   STABLE. It does not make it RIGHT. This column is what lets a person see
--   which verdicts were close instead of reading a stable number as a confident
--   one.
--
-- SAFETY
--   Additive and nullable. Existing rows get NULL, which every reader treats as
--   "high" -- so nothing changes for data already stored, and nothing needs
--   backfilling. `store_requirement_mappings` probes for this column and writes
--   the six-column insert when it is absent, so the code works before and after
--   this runs.
--
--   Reversible:  ALTER TABLE sama_requirement_mapping DROP COLUMN match_confidence;
--
-- AFTER RUNNING
--   RESTART the API. The column check is cached for the life of the process, so
--   a running instance keeps using the six-column insert until it restarts.
--   Rows written in between simply carry no confidence; nothing fails.

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = 'sama_requirement_mapping'
      AND COLUMN_NAME = 'match_confidence'
)
BEGIN
    ALTER TABLE sama_requirement_mapping
        ADD match_confidence varchar(10) NULL;

    PRINT 'match_confidence added.';
END
ELSE
    PRINT 'match_confidence already present - nothing to do.';
GO

-- The review queue this exists to serve:
--
--   SELECT regulation_id, extracted_requirement_text, match_status,
--          matched_requirement_id, match_explanation
--   FROM   sama_requirement_mapping
--   WHERE  match_confidence = 'low'
--   ORDER  BY regulation_id;
