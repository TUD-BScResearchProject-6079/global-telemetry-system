SELECT
        client_country_code,
        test_time,
        asn,
        packet_loss_rate,
        download_throughput_mbps,
        download_latency_ms,
        download_jitter,
        upload_throughput_mbps,
        upload_latency_ms,
        upload_jitter
    FROM {0}
    WHERE client_country_code IN ('US', 'CA', 'BR', 'AR', 'DE', 'FR', 'GH', 'KE', 'AU', 'NZ', 'PH', 'JP')
        AND packet_loss_rate IS NOT NULL
        AND download_throughput_mbps IS NOT NULL
        AND download_jitter IS NOT NULL
        AND upload_throughput_mbps IS NOT NULL
        AND upload_latency_ms IS NOT NULL
        AND upload_jitter IS NOT NULL
    ORDER BY client_country_code