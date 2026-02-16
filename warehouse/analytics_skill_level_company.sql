-- Analytics SQL: Skill demand by seniority level and company count
-- Returns: SKILL, LEVEL, COMPANY_COUNT

SELECT
  js.SKILL,
  j.LEVEL,
  COUNT(DISTINCT j.COMPANY) AS COMPANY_COUNT
FROM
  JOB_SKILLS js
JOIN
  JOBS j
    ON js.JOB_ID = j.JOB_ID
GROUP BY
  js.SKILL,
  j.LEVEL
ORDER BY
  COMPANY_COUNT DESC, js.SKILL, j.LEVEL;
