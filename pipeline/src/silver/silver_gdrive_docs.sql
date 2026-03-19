-- Databricks notebook source
-- Silver Layer — Google Drive Account Docs
-- Deduplicates by file_id (latest version wins) and keeps only the most
-- recently modified document per account for action-item extraction.

CREATE OR REPLACE LIVE TABLE silver_gdrive_docs
COMMENT 'Latest Google Docs text per account folder, ready for AI action-item extraction'
AS
WITH deduped AS (
  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY file_id ORDER BY _extracted_at DESC) AS rn
  FROM   LIVE.raw_gdrive_docs
  WHERE  content IS NOT NULL AND LENGTH(TRIM(content)) > 20
)
SELECT
  file_id,
  file_name,
  account_name,
  modified_time,
  content,
  _extracted_at
FROM deduped
WHERE rn = 1
