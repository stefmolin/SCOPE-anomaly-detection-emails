SELECT /*+label(scope_exec_account_level_for_anomaly_detection)*/
	day
	, area
   	, region
   	, country
	, client_name
	, SUM(RexT_USD_Constant) AS RexT_USD_Constant
FROM
	(SELECT
		f.day AS day
		, client_name
		, client_country_level_3_name AS area
	   	, client_country_level_2_name AS region
	   	, CASE WHEN (client_country_level_1_name = 'US') THEN 'US'
            WHEN (client_country_level_1_name = 'CANADA') THEN 'CANADA'
            WHEN (client_country_level_1_name = 'BRAZIL') THEN 'BRAZIL'
            WHEN (client_country_level_1_name = 'SOUTH AMERICA' AND client_country_name IN ('ARGENTINA', 'MEXICO', 'CHILE', 'COLOMBIA')) THEN client_country_name
            WHEN (client_country_level_1_name = 'SOUTH AMERICA') THEN 'SAM OTHER'
        END AS country
        , SUM(COALESCE(revenue*ex_adv_usd_constant.rate,revenue*ex_adv_usd.rate,0) - COALESCE(tac*ex_pub_usd_constant.rate,tac*ex_pub.rate,0)*COALESCE(ex_adv_usd_constant.rate,ex_adv_usd.rate,1)) AS RexT_USD_Constant
	FROM
		(SELECT
			client_country_level_1_name
			, client_country_level_2_name
			, client_country_level_3_name
			, client_country_name
			, campaign_id
			, account_strategist_employee_id
			, client_currency_id
            , client_id
            , client_name
		FROM
			schema_1.client_lookup_table
		WHERE
			client_country_level_3_name = 'AMERICAS'
			AND ranking = 'TIER 1'
			AND vertical_level_3_name NOT IN ('FILLER')
		GROUP BY
			client_country_level_1_name
			, client_country_level_2_name
			, client_country_level_3_name
			, campaign_id
			, account_strategist_employee_id
			, client_currency_id
			, client_country_name
            , client_id
            , client_name) a
    JOIN
        (SELECT
            client_id AS cid
            , COUNT(DISTINCT day) AS days_live
        FROM
            schema_1.stats_table
        WHERE
            day >= CURRENT_DATE - 25
            AND day <= CURRENT_DATE - 1
        GROUP BY
            client_id
        HAVING
            COUNT(DISTINCT day) = 25) live
    ON
        live.cid = a.client_id
	JOIN
		(SELECT
			employee_id
		FROM
			schema_1.employee_lookup_table
		GROUP BY
			employee_id) em
	ON
		em.employee_id = a.account_strategist_employee_id
	JOIN
		(SELECT
			day
			, campaign_id
			, tac_local AS tac
			, revenue_local AS revenue
			, affiliate_currency_id
		FROM
			schema_1.stats_table_2
		WHERE
			day >= CURRENT_DATE - 30
		GROUP BY
			day
			, campaign_id
			, tac_local
			, revenue_local
			, affiliate_currency_id) f
	ON
		f.campaign_id = a.campaign_id
	LEFT JOIN
		(SELECT
			source_currency_id
			, day
			, rate
			, destination_currency_id
		FROM
			schema_1.exchange_rates_table
		WHERE
			day >= CURRENT_DATE - 30
		GROUP BY
			source_currency_id
			, day
			, rate
			, destination_currency_id) ex_pub
	ON
		f.affiliate_currency_id = ex_pub.source_currency_id
		AND f.day = ex_pub.day
		AND a.client_currency_id = ex_pub.destination_currency_id
	LEFT JOIN
		(SELECT
			source_currency_id
			, day
			, rate
		FROM
			schema_1.exchange_rates_table
		WHERE
			day >= CURRENT_DATE - 30
			AND destination_currency_id = 2
		GROUP BY
			source_currency_id
			, day
			, rate) ex_adv_usd
	ON
		ex_adv_usd.source_currency_id = a.client_currency_id
		AND ex_adv_usd.day = f.day
	LEFT JOIN
		(SELECT
			source_currency_id
			, rate
			, year_code
		FROM
			schema_2.exchange_rates_2_table
		WHERE
			year_code IN (YEAR(CURRENT_DATE - 1), YEAR(CURRENT_DATE - 30))
			AND forecast_type_id = 0
			AND destination_currency_id = 2
		GROUP BY
			source_currency_id
			, rate
			, year_code) ex_adv_usd_constant
	ON
		a.client_currency_id = ex_adv_usd_constant.source_currency_id
		AND YEAR(f.day) = ex_adv_usd_constant.year_code
	LEFT JOIN
		(SELECT
			source_currency_id
			, destination_currency_id
			, rate
			, year_code
		FROM
			schema_2.exchange_rates_2_table
		WHERE
			year_code IN (YEAR(CURRENT_DATE - 1), YEAR(CURRENT_DATE - 30))
			AND forecast_type_id = 0
		GROUP BY
			source_currency_id
			, destination_currency_id
			, rate
			, year_code) ex_pub_usd_constant
	ON
		f.affiliate_currency_id = ex_pub_usd_constant.source_currency_id
		AND a.client_currency_id = ex_pub_usd_constant.destination_currency_id
		AND YEAR(f.day) = ex_pub_usd_constant.year_code
	GROUP BY
		f.day
		, client_name
		, client_country_level_3_name
	   	, client_country_level_2_name
	   	, client_country_level_1_name
	   	, client_country_name
	   	, CASE WHEN (client_country_level_1_name = 'US') THEN 'US'
	        WHEN (client_country_level_1_name = 'CANADA') THEN 'CANADA'
	        WHEN (client_country_level_1_name = 'SOUTH AMERICA' AND client_country_name IN ('ARGENTINA', 'MEXICO', 'CHILE', 'COLOMBIA')) THEN client_country_name
	        WHEN (client_country_level_1_name = 'SOUTH AMERICA') THEN 'SAM OTHER'
	    END) stats
GROUP BY
	day
	, area
	, region
	, country
    , client_name
ORDER BY
	day
	, area
   	, region
   	, country
    , client_name
