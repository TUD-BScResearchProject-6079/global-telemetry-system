SELECT
        client_country_code,
        test_time,
        asn,
        packet_loss_rate,
        download_throughput_mbps,
        download_latency_ms,
        download_jitter
    FROM ndt7
    WHERE packet_loss_rate IS NOT NULL 
        AND download_throughput_mbps IS NOT NULL
        AND download_latency_ms IS NOT NULL
        AND download_jitter IS NOT NULL
        AND client_country_code IN ('US', 'CA', 'BR', 'AR', 'DE', 'FR', 'GH', 'KE', 'AU', 'NZ', 'PH', 'JP')
    ORDER BY client_country_code

SELECT
        client_country_code,
        test_time,
        asn,
        packet_loss_rate,
        upload_throughput_mbps,
        upload_latency_ms,
        upload_jitter
    FROM ndt7
    WHERE packet_loss_rate IS NOT NULL 
        AND upload_throughput_mbps IS NOT NULL
        AND upload_latency_ms IS NOT NULL
        AND upload_jitter IS NOT NULL
        AND client_country_code IN ('US', 'CA', 'BR', 'AR', 'DE', 'FR', 'GH', 'KE', 'AU', 'NZ', 'PH', 'JP')
    ORDER BY client_country_code