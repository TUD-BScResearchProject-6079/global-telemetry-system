SELECT
	country_iso,
	city,
	test_time,
	asn,
	packet_loss_rate,
	throughput_mbps AS download_throughput_mbps,
	download_latency_ms,
	download_jitter,
	download_bytes
FROM ndt7_flat
WHERE packet_loss_rate IS NOT NULL 
	AND throughput_mbps IS NOT NULL
	AND download_latency_ms IS NOT NULL
	AND download_jitter IS NOT NULL
	AND download_bytes IS NOT NULL
	AND country_iso IN ('GT', 'PL', 'GH', 'BR', 'FJ')
ORDER BY country_iso

SELECT
	country_iso,
	city,
	test_time,
	asn,
	packet_loss_rate,
	throughput_mbps AS upload_throughput_mbps,
	upload_latency_ms,
	upload_jitter,
	upload_bytes
FROM ndt7_flat
WHERE packet_loss_rate IS NOT NULL 
	AND throughput_mbps IS NOT NULL
	AND upload_latency_ms IS NOT NULL
	AND upload_jitter IS NOT NULL
	AND upload_bytes IS NOT NULL
	AND country_iso IN ('GT', 'PL', 'GH', 'BR', 'FJ')
ORDER BY country_iso