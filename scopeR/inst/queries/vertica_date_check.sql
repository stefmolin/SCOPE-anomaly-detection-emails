SELECT /*+label(scope_date_check)*/
	table_name
	, update_time
FROM
	monitoring_schema.import_reports
WHERE
  update_time >= todaysDate
  AND table_schema IN (schemaList)
  AND table_name IN (tableList)
