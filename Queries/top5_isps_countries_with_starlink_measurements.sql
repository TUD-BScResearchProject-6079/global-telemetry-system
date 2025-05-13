WITH rank_within_country_view AS (
	SELECT asn, RANK() OVER (PARTITION BY country_name ORDER BY rank ASC) AS rank_within_country
	FROM AS_Statistics
)
SELECT a.asn, a.asn_name, a.rank, b.rank_within_country, a.country_name, a.country_iso
FROM as_statistics a
	JOIN rank_within_country_view b ON a.asn = b.asn
	JOIN countries_with_starlink_measurements c ON a.country_iso = c.country_iso
WHERE b.rank_within_country <= 5 OR a.asn = 14593
ORDER BY a.country_name, b.rank_within_country


