-- Databricks notebook source
-- Silver Layer — Google Tasks
-- Cleans and normalises raw_gtasks: deduplicated by task_id,
-- handles due-date parsing, and surfaces account linkage.

CREATE OR REPLACE LIVE TABLE silver_gtasks
COMMENT 'Cleaned pending Google Tasks with account linkage'
AS
WITH deduped AS (
  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY task_id ORDER BY _extracted_at DESC) AS rn
  FROM   LIVE.raw_gtasks
  WHERE  status != 'completed'
)
SELECT
  task_id,
  tasklist_id,
  tasklist_name,
  title                                    AS task_title,
  NULLIF(TRIM(notes), '')                  AS task_notes,
  status,
  CASE
    WHEN due_date != '' THEN TO_DATE(due_date, 'yyyy-MM-dd')
    ELSE NULL
  END                                      AS due_date,
  DATEDIFF(current_date(),
    CASE WHEN due_date != '' THEN TO_DATE(due_date, 'yyyy-MM-dd') ELSE NULL END
  )                                        AS days_overdue,
  account_name,
  updated,
  _extracted_at
FROM deduped
WHERE rn = 1
