SELECT /*+label(scope_campaign_level_for_anomaly_detection)*/
  client_name
  , client_id
	, global_account_name
	, vertical_level_2_name AS RTC_vertical
	, vertical_name
  , campaign_name
  , campaign_id
	, AS_name
	, AS_email
	, manager_name
	, manager_email
	, cost_center
  , currency_code
  , ranking
  , country
	, subregion
	, region
  , campaign_scenario
	, campaign_type_name
	, campaign_revenue_type
	, campaign_funnel_id
  , stats.day AS day
  , COALESCE(SUM(clicks), 0) AS clicks
  , COALESCE(SUM(displays), 0) AS displays
  , COALESCE(SUM(post_click_conversions), 0) AS conversions
  , COALESCE(SUM(revenue_local), 0) AS spend
  , COALESCE(SUM(tac_local*pub.rate), 0) AS tac
  , COALESCE(SUM(revenue_local - tac_local*pub.rate), 0) AS rext
  , COALESCE(SUM(revenue_euro - tac_euro), 0) AS rext_euro
  , COALESCE(SUM(ov), 0) AS order_value
FROM
  (SELECT
  		client_name
      , client_id
      , campaign_name
      , f.campaign_id AS campaign_id
  		, AS_name
  		, AS_email
  		, manager_name
  		, manager_email
  		, cost_center
      , currency_code
      , client_currency_id
      , ranking
      , co.country_level_1_name AS country
  		, co.country_level_2_name AS subregion
  		, co.country_level_3_name AS region
      , day
      , displays
      , clicks
      , post_click_conversions
      , ov
      , revenue_local
      , tac_local
      , revenue_euro
      , tac_euro
      , affiliate_currency_id
  		, campaign_scenario
  		, vertical_level_2_name
  		, vertical_name
  		, global_account_name
  		, campaign_type_name
  		, campaign_revenue_type
  		, campaign_funnel_id
    FROM
    	(SELECT
    		employee_id
    		, country
  			, full_name AS AS_name
  			, email AS AS_email
  			, cost_center_country AS cost_center
  			, direct_reports_id AS manager_id
    	FROM
    		schema_1.employee_lookup_table em
    	WHERE
    		countryConditions
	    GROUP BY
	    	employee_id
	    	, country
  			, full_name
  			, email
  			, cost_center_country
  			, direct_reports_id) em
    JOIN
  		(SELECT
  			employee_id AS manager_id
  			, full_name AS manager_name
  			, email AS manager_email
  		FROM
  			schema_1.employee_lookup_table em
  		GROUP BY
  			employee_id
  			, full_name
  			, email) manager
  	ON
  		em.manager_id = manager.manager_id
    JOIN
  		(SELECT
  			campaign_id
  			, account_strategist_employee_id
  			, client_currency_id
  			, client_name
  			, client_id
  			, campaign_name
  			, ranking
  			, campaign_scenario_product_level_1_name AS campaign_scenario
  			, vertical_level_2_name
  			, vertical_name
  			, global_account_name
  			, campaign_type_name
  			, campaign_revenue_type
  			, campaign_funnel_id
  		FROM
  			schema_1.client_lookup_table a
  		WHERE
  			tiering
  		GROUP BY
  			campaign_id
  			, account_strategist_employee_id
  			, client_currency_id
  			, client_name
  			, client_id
  			, campaign_name
  			, ranking
  			, campaign_scenario_product_level_1_name
  			, vertical_level_2_name
  			, vertical_name
  			, global_account_name
  			, campaign_type_name
  			, campaign_revenue_type
  			, campaign_funnel_id) a
	 ON
		em.employee_id = a.account_strategist_employee_id
	JOIN
		(SELECT
			currency_id
			, currency_code
		FROM
			schema_1.currency_lookup_table c
		GROUP BY
			currency_id
			, currency_code) c
	ON
		a.client_currency_id = c.currency_id
	JOIN
		(SELECT
			country_code
			, country_level_1_name
			, country_level_2_name
			, country_level_3_name
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
    		day
    		, displays
    		, clicks
    		, sales_all_post_click AS post_click_conversions
    		, order_value_all_post_click_local AS ov
    		, revenue_local
    		, tac_local
    		, revenue_euro
    		, tac_euro
    		, campaign_id
    		, affiliate_currency_id
    	FROM
    		schema_1.stats_table f
    	WHERE
    		day >= CURRENT_DATE - 60
    	GROUP BY
    		day
    		, displays
    		, clicks
    		, sales_all_post_click
    		, order_value_all_post_click_local
    		, revenue_local
    		, tac_local
    		, revenue_euro
    		, tac_euro
    		, campaign_id
    		, affiliate_currency_id) f
    ON
    	f.campaign_id = a.campaign_id
    GROUP BY
		  client_name
      , client_id
      , campaign_name
      , f.campaign_id
  		, AS_name
  		, AS_email
  		, manager_name
  		, manager_email
  		, cost_center
      , currency_code
	  	, client_currency_id
      , ranking
      , co.country_level_1_name
  		, co.country_level_2_name
  		, co.country_level_3_name
      , day
      , displays
      , clicks
      , post_click_conversions
      , ov
      , revenue_local
      , tac_local
      , revenue_euro
      , tac_euro
      , affiliate_currency_id
  		, campaign_scenario
  		, vertical_level_2_name
  		, vertical_name
  		, global_account_name
  		, campaign_type_name
  		, campaign_revenue_type
  		, campaign_funnel_id) stats
JOIN
	(SELECT
		source_currency_id
		, destination_currency_id
		, day
		, rate
	FROM
		schema_1.exchange_rates_table
	WHERE
		day >= CURRENT_DATE - 60
	GROUP BY
		source_currency_id
		, destination_currency_id
		, day
		, rate) pub
ON
	stats.affiliate_currency_id = pub.source_currency_id
	AND stats.day = pub.day
	AND stats.client_currency_id = pub.destination_currency_id
GROUP BY
  client_name
  , client_id
	, global_account_name
	, vertical_level_2_name
	, vertical_name
  , campaign_name
  , campaign_id
  , campaign_scenario
	, campaign_type_name
	, campaign_revenue_type
	, campaign_funnel_id
	, AS_name
	, AS_email
	, manager_name
	, manager_email
	, cost_center
  , currency_code
  , ranking
  , country
	, subregion
	, region
  , stats.day
ORDER BY
    client_name
    , campaign_name
    , day
