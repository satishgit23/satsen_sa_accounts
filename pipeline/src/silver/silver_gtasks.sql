-- Databricks notebook source
-- Silver Layer — Google Tasks
-- Cleans and normalises raw_gtasks. Self-joins to resolve the parent task title
-- so that subtasks can be displayed with full context (e.g. "Schedule Meeting
-- (re: Serving Endpoint - My models selection)").

CREATE OR REPLACE LIVE TABLE silver_gtasks
COMMENT 'Cleaned pending Google Tasks with parent-child relationship resolved'
AS
WITH deduped AS (
  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY task_id ORDER BY _extracted_at DESC) AS rn
  FROM   LIVE.raw_gtasks
  WHERE  status != 'completed'
),
parent_titles AS (
  SELECT task_id, title AS parent_title, account_name AS parent_account
  FROM   LIVE.raw_gtasks
  WHERE  (parent_task_id IS NULL OR parent_task_id = '')
    AND  status != 'completed'
)
SELECT
  t.task_id,
  t.parent_task_id,
  p.parent_title,
  t.tasklist_id,
  t.tasklist_name,
  t.title                                     AS task_title,
  CASE
    WHEN p.parent_title IS NOT NULL
    THEN CONCAT(t.title, ' (re: ', p.parent_title, ')')
    ELSE t.title
  END                                         AS task_display,
  NULLIF(TRIM(t.notes), '')                   AS task_notes,
  t.status,
  CASE
    WHEN t.due_date != '' THEN TO_DATE(t.due_date, 'yyyy-MM-dd')
    ELSE NULL
  END                                         AS due_date,
  DATEDIFF(current_date(),
    CASE WHEN t.due_date != '' THEN TO_DATE(t.due_date, 'yyyy-MM-dd') ELSE NULL END
  )                                           AS days_overdue,
  COALESCE(t.account_name, p.parent_account) AS account_name,
  t.updated,
  t._extracted_at
FROM  deduped  t
LEFT  JOIN parent_titles p ON t.parent_task_id = p.task_id
WHERE t.rn = 1
