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
	upload_bytes
FROM cloudflare_flat
WHERE country_iso IN ('GT', 'PL', 'GH', 'BR', 'FJ')
	AND packet_loss_rate IS NOT NULL
	AND download_throughput_mbps IS NOT NULL
	AND download_jitter IS NOT NULL
	AND download_bytes IS NOT NULL
	AND upload_throughput_mbps IS NOT NULL
	AND upload_latency_ms IS NOT NULL
	AND upload_jitter IS NOT NULL
	AND upload_bytes IS NOT NULL
ORDER BY country_iso