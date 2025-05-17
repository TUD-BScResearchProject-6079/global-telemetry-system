SELECT
	city,
	country_iso,
	test_time,
	asn,
	packet_loss_rate,
	download_throughput_mbps,
	download_latency_ms,
	download_jitter,
	download_bytes,
	upload_throughput_mbps,
	upload_latency_ms,
	upload_jitter,
	upload_bytes,
	CASE 
        	WHEN country_iso IN ('US', 'CA') THEN 'North America'
        	WHEN country_iso IN ('BR', 'AR') THEN 'South America'
        	WHEN country_iso IN ('DE', 'FR') THEN 'Europe'
        	WHEN country_iso IN ('GH', 'KE') THEN 'Africa'
        	WHEN country_iso IN ('AU', 'NZ') THEN 'Oceania'
        	WHEN country_iso IN ('PH', 'JP') THEN 'Asia'
        	ELSE 'Unknown'
    	END AS continent
FROM cloudflare_flat
WHERE country_iso IN ('US', 'CA', 'BR', 'AR', 'DE', 'FR', 'GH', 'KE', 'AU', 'NZ', 'PH', 'JP')
	AND packet_loss_rate IS NOT NULL
	AND download_throughput_mbps IS NOT NULL
	AND download_jitter IS NOT NULL
	AND download_bytes IS NOT NULL
	AND upload_throughput_mbps IS NOT NULL
	AND upload_latency_ms IS NOT NULL
	AND upload_jitter IS NOT NULL
	AND upload_bytes IS NOT NULL
ORDER BY country_iso