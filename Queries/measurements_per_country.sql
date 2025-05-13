WITH 
	cf_measurements_per_country AS (
		SELECT country_iso, COUNT(DISTINCT uuid) AS cloudflare_count
		FROM cloudflare_flat
		GROUP BY country_iso
		ORDER BY country_iso
	),
	ndt7_measurements_per_country AS (
		SELECT country_iso, COUNT(DISTINCT uuid) AS ndt7_count
		FROM ndt7_flat
		GROUP BY country_iso
		ORDER BY country_iso
	)
SELECT 
	COALESCE(cf.country_iso, ndt.country_iso) AS country_iso,
	cf.cloudflare_count,
	ndt.ndt7_count
FROM cf_measurements_per_country cf 
	FULL OUTER JOIN ndt7_measurements_per_country ndt ON cf.country_iso = ndt.country_iso
