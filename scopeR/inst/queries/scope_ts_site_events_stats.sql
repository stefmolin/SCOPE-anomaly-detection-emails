SELECT /*+label(scope_ts_site_events_for_anomaly_detection)*/
	partner_name
	, stats.partner_id AS partner_id
	, day
	, TS_name
	, ts_engineer_email
	, manager_name
	, manager_email
	, ranking
	, cost_center AS ts_cost_center
	, country AS ts_country
	, CASE WHEN cost_center = 'LM' THEN 'LATAM' ELSE subregion END AS ts_subregion
	, region AS ts_region
	, vertical_level_2_name AS RTC_vertical
	, vertical_name
	, global_account_name
	, site_type
	, event_name
	, SUM(events) AS events
FROM
	(SELECT
    	TS_name
    	, ts_engineer_email
    	, manager_name
    	, manager_email
    	, partner_name
      , a.partner_id AS partner_id
      , ranking
      , co.country AS country
      , subregion
      , region
      , cost_center
			, vertical_level_2_name
			, vertical_name
			, global_account_name
      , COUNT(DISTINCT day) AS days_live
    FROM
    	(SELECT
    		full_name AS TS_name
    		, email AS ts_engineer_email
    		, cost_center_country AS cost_center
    		, employee_id
    		, country
    		, direct_reports_id
    	FROM
    		schema_1.employee_lookup_table em
    	WHERE
    		countryConditions
    	GROUP BY
    		full_name
    		, email
    		, cost_center_country
    		, employee_id
    		, country
    		, direct_reports_id) em
    JOIN
      (SELECT
    		employee_id
    		, full_name AS manager_name
    		, email AS manager_email
    	FROM
    		schema_1.employee_lookup_table
    	GROUP BY
    		employee_id
    		, full_name
    		, email) managers
    ON
      em.direct_reports_id = managers.employee_id
    JOIN
    	(SELECT
    		country_code
    		, country_level_1_name AS country
    		, country_level_2_name AS subregion
    		, country_level_3_name AS region
    	FROM
    		schema_1.country_lookup_table co
    	GROUP BY
    		country_code
    		, country_level_1_name
    		, country_level_2_name
    		, country_level_3_name) co
    ON
    	co.country_code = em.country
    JOIN
    	(SELECT
    		partner_id
    		, ts_engineer_employee_id
    		, ranking
    		, partner_name
    		, campaign_id
  			, vertical_level_2_name
  			, vertical_name
  			, global_account_name
    	FROM
    		schema_1.client_lookup_table a
    	WHERE
    		tiering
    	GROUP BY
    		partner_id
    		, ts_engineer_employee_id
    		, ranking
    		, partner_name
    		, campaign_id
  			, vertical_level_2_name
  			, vertical_name
  			, global_account_name) a
    ON
    	em.employee_id = a.ts_engineer_employee_id
    JOIN
    	(SELECT
    		day
    		, campaign_id
    		, tac_local AS tac
    	FROM
    		schema_1.stats_table_2 f
    	WHERE
    		day >= CURRENT_DATE - 30
			AND day <= CURRENT_DATE - 1
		GROUP BY
			campaign_id
			, day
			, tac_local) f
    ON
    	f.campaign_id = a.campaign_id
    GROUP BY
    	TS_name
    	, ts_engineer_email
    	, manager_name
    	, manager_email
    	, partner_name
      , a.partner_id
      , ranking
      , co.country
      , subregion
      , region
      , cost_center
			, vertical_level_2_name
			, vertical_name
			, global_account_name
    HAVING
  		COUNT(DISTINCT day) >= 25
  		AND SUM(CASE WHEN day IN (CURRENT_DATE - 1, CURRENT_DATE - 2) THEN tac ELSE 0 END) > 0) live
JOIN
	(SELECT
		partner_id
		, day
		, site_type
		, CASE WHEN event_name = 'page' THEN 'homepage'
		WHEN event_name = 'itempageview' THEN 'product'
		ELSE event_name END AS event_name
		, SUM(events) AS events
	FROM
		schema_2.events_table
	WHERE
		day >= CURRENT_DATE - 60
	GROUP BY
		partner_id
		, day
		, site_type
		, CASE WHEN event_name = 'page' THEN 'homepage'
		WHEN event_name = 'itempageview' THEN 'product'
		ELSE event_name END) stats
ON
	live.partner_id = stats.partner_id
GROUP BY
	partner_name
	, stats.partner_id
	, TS_name
	, ts_engineer_email
	, manager_name
	, manager_email
	, day
	, site_type
	, event_name
	, ranking
	, cost_center
	, country
	, CASE WHEN cost_center = 'LM' THEN 'LATAM' ELSE subregion END
	, region
	, vertical_level_2_name
	, vertical_name
	, global_account_name
ORDER BY
  TS_name
  , partner_name
  , day
