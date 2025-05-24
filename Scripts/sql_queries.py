from psycopg2 import sql


def get_cf_create_table_sql(table_name: str) -> str:
    create_cf_table_sql = f"""
        CREATE TABLE IF NOT EXISTS cf_{table_name} (
            uuid VARCHAR(255) COLLATE pg_catalog."default" PRIMARY KEY,
            test_time TIMESTAMP WITH TIME ZONE NOT NULL,
            client_city VARCHAR(255) COLLATE pg_catalog."default",
            client_region VARCHAR(255) COLLATE pg_catalog."default",
            client_country_code CHAR(2) COLLATE pg_catalog."default" NOT NULL,
            server_airport_code CHAR(3) COLLATE pg_catalog."default" NOT NULL,
            asn INTEGER NOT NULL,
            packet_loss_rate NUMERIC(10, 5),
            download_throughput_mbps NUMERIC(10, 5),
            download_latency_ms INTEGER,
            download_jitter NUMERIC(10, 5),
            upload_throughput_mbps NUMERIC(10, 5),
            upload_latency_ms INTEGER,
            upload_jitter NUMERIC(10, 5)
        );

        CREATE UNIQUE INDEX IF NOT EXISTS pk_cf_{table_name}
            ON public.cf_{table_name} USING btree
            (uuid ASC NULLS LAST)
            TABLESPACE pg_default;

        CREATE INDEX IF NOT EXISTS time_btree_cf_{table_name}
            ON public.cf_{table_name} USING btree
            (test_time ASC NULLS LAST)
            TABLESPACE pg_default;

        ALTER TABLE IF EXISTS public.cf_{table_name}
            CLUSTER ON time_btree_cf_{table_name};

        CREATE INDEX IF NOT EXISTS asn_btree_cf_{table_name}
            ON public.cf_{table_name} USING btree
            (asn ASC NULLS LAST)
            TABLESPACE pg_default;

        CREATE INDEX IF NOT EXISTS city_hash_cf_{table_name}
            ON public.cf_{table_name} USING hash
            (client_city COLLATE pg_catalog."default")
            TABLESPACE pg_default;

        CREATE INDEX IF NOT EXISTS country_hash_cf_{table_name}
            ON public.cf_{table_name} USING hash
            (client_country_code COLLATE pg_catalog."default")
            TABLESPACE pg_default;
    """
    return create_cf_table_sql


ndt_create_table_sql = """
    CREATE TABLE IF NOT EXISTS public.ndt7
    (
        uuid character varying(255) COLLATE pg_catalog."default" NOT NULL,
        test_time TIMESTAMP WITH TIME ZONE NOT NULL,
        client_region character varying(255) COLLATE pg_catalog."default",
        client_city character varying(255) COLLATE pg_catalog."default",
        client_country_code character(2) COLLATE pg_catalog."default" NOT NULL,
        server_city character varying(255) COLLATE pg_catalog."default",
        server_country_code character(2) COLLATE pg_catalog."default" NOT NULL,
        asn integer NOT NULL,
        packet_loss_rate numeric(10,5) NOT NULL,
        download_throughput_mbps numeric(10,5),
        download_latency_ms integer,
        download_jitter numeric(10,5),
        upload_throughput_mbps numeric(10,5),
        upload_latency_ms integer,
        upload_jitter numeric(10,5),
        CONSTRAINT ndt7_pkey PRIMARY KEY (uuid)
    );

    CREATE UNIQUE INDEX IF NOT EXISTS pk_ndt
        ON public.ndt7 USING btree
        (uuid ASC NULLS LAST)
        TABLESPACE pg_default;

    CREATE INDEX IF NOT EXISTS time_btree_ndt7
        ON public.ndt7 USING btree
        (test_time ASC NULLS LAST)
        TABLESPACE pg_default;

    ALTER TABLE IF EXISTS public.ndt7
        CLUSTER ON time_btree_ndt7;

    CREATE INDEX IF NOT EXISTS asn_btree_ndt
        ON public.ndt7 USING btree
        (asn ASC NULLS LAST)
        TABLESPACE pg_default;

    CREATE INDEX IF NOT EXISTS country_btree_ndt
        ON public.ndt7 USING btree
        (client_country_code COLLATE pg_catalog."default" ASC NULLS LAST)
        TABLESPACE pg_default;

    CREATE INDEX IF NOT EXISTS country_hash_ndt
        ON public.ndt7 USING hash
        (client_country_code COLLATE pg_catalog."default")
        TABLESPACE pg_default;
"""

cities_create_query = sql.SQL(
    """
    CREATE TABLE IF NOT EXISTS public.cities
    (
        name character varying(200) COLLATE pg_catalog."default" NOT NULL,
        asciiname character varying(200) COLLATE pg_catalog."default" NOT NULL,
        name1 character varying(200) COLLATE pg_catalog."default",
        name2 character varying(200) COLLATE pg_catalog."default",
        name3 character varying(200) COLLATE pg_catalog."default",
        name4 character varying(200) COLLATE pg_catalog."default",
        region character varying(200) COLLATE pg_catalog."default" NOT NULL,
        country_code character(2) COLLATE pg_catalog."default"
    )
"""
)

cities_insert_query = sql.SQL(
    """
    INSERT INTO cities (name, asciiname, name1, name2, name3, name4, region, country_code)
    VALUES %s
"""
)

ndt_insert_query = sql.SQL(
    """
            INSERT INTO ndt7 (
                uuid,
                test_time,
                client_city,
                client_region,
                client_country_code,
                server_city,
                server_country_code,
                asn,
                packet_loss_rate,
                download_throughput_mbps,
                download_latency_ms,
                download_jitter,
                upload_throughput_mbps,
                upload_latency_ms,
                upload_jitter
            ) VALUES %s
        """
)

cf_insert_query = sql.SQL(
    """
    INSERT INTO {} (
        uuid,
        test_time,
        client_city,
        client_region,
        client_country_code,
        server_airport_code,
        asn,
        packet_loss_rate,
        download_throughput_mbps,
        download_latency_ms,
        download_jitter,
        upload_throughput_mbps,
        upload_latency_ms,
        upload_jitter
    ) VALUES %s
"""
)

airports_create_query = """
    CREATE TABLE IF NOT EXISTS airport_country (
        airport_code CHAR(3) PRIMARY KEY,
        country_code CHAR(2) NOT NULL
    );
"""

airport_insert_query = sql.SQL(
    """
    INSERT INTO airport_country (country_code, airport_code)
    VALUES %s
"""
)

ndt_best_servers_create_query = """
    CREATE TABLE IF NOT EXISTS ndt_server_for_country (
        client_country CHAR(2) NOT NULL,
        server_city VARCHAR(100) NOT NULL,
        server_country CHAR(2) NOT NULL
    );
"""

ndt_best_server_insert_query = sql.SQL(
    """
    INSERT INTO ndt_server_for_country (client_country, server_city, server_country)
    VALUES %s
"""
)

cf_best_servers_create_query = """
    CREATE TABLE IF NOT EXISTS cf_server_for_country (
        client_country CHAR(2) NOT NULL,
        server_airport_code CHAR(3) NOT NULL
    );
"""

cf_best_server_insert_query = sql.SQL(
    """
    INSERT INTO cf_server_for_country (client_country, server_airport_code)
    VALUES %s
"""
)

cf_delete_abnormal_servers_query = sql.SQL(
    """
    DELETE FROM {} cf
    USING
        airport_country ac
    WHERE
        cf.server_airport_code = ac.airport_code
        AND cf.client_country_code <> ac.country_code
        AND cf.server_airport_code NOT IN (
            SELECT DISTINCT sv.server_airport_code
            FROM cf_server_for_country sv
            WHERE cf.client_country_code = sv.client_country
        );
"""
)

ndt_delete_abnormal_servers_query = sql.SQL(
    """
    DELETE
    FROM ndt7 n
    WHERE n.client_country_code <> n.server_country_code
        AND (n.server_city, n.server_country_code) NOT IN (
            SELECT DISTINCT server_city, server_country
            FROM ndt_server_for_country sv
            WHERE sv.client_country = n.client_country_code
        );
"""
)

ndt_standardize_cities_query = sql.SQL(
    """
    UPDATE ndt7 n
    SET
        client_city = c.asciiname,
        client_region = c.region
    FROM cities c
    WHERE
        n.client_city IS NOT NULL
        AND n.client_city <> ''
        AND n.client_city IN (c.name, c.asciiname, c.name1, c.name2, c.name3, c.name4)
        AND n.client_country_code = c.country_code;
"""
)

cf_standardize_cities_query = sql.SQL(
    """
    UPDATE {} cf
    SET
        client_city = c.asciiname,
        client_region = c.region
    FROM cities c
    WHERE
        cf.client_city IS NOT NULL
        AND cf.client_city <> ''
        AND cf.client_city IN (c.name, c.asciiname, c.name1, c.name2, c.name3, c.name4)
        AND cf.client_country_code = c.country_code;
"""
)

cf_case_study_query = sql.SQL(
    """
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
    WHERE client_country_code IN ({1})
        AND packet_loss_rate IS NOT NULL
        AND download_throughput_mbps IS NOT NULL
        AND download_jitter IS NOT NULL
        AND upload_throughput_mbps IS NOT NULL
        AND upload_latency_ms IS NOT NULL
        AND upload_jitter IS NOT NULL
    ORDER BY client_country_code
"""
)

ndt_download_case_study = sql.SQL(
    """
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
        AND client_country_code IN ({})
    ORDER BY client_country_code
"""
)

ndt_upload_case_study = sql.SQL(
    """
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
        AND client_country_code IN ({})
    ORDER BY client_country_code
"""
)

caida_asn_create_table_query = sql.SQL(
    """
    CREATE TABLE IF NOT EXISTS public.as_statistics
    (
        asn bigint NOT NULL,
        asn_name character varying(512) COLLATE pg_catalog."default",
        rank bigint,
        country_code character varying(512) COLLATE pg_catalog."default",
        country_name character varying(512) COLLATE pg_catalog."default",
        CONSTRAINT as_statistics_pkey PRIMARY KEY (asn)
    );
"""
)

caida_asn_insert_query = sql.SQL(
    "INSERT INTO as_statistics (asn, asn_name, rank, country_code, country_name) VALUES %s"
)

top_five_isps_countries_with_starlink_measurements = sql.SQL(
    """
    WITH rank_within_country_view AS (
        SELECT asn, RANK() OVER (PARTITION BY country_name ORDER BY rank ASC) AS rank_within_country
        FROM AS_Statistics
    )
    SELECT a.asn
    FROM as_statistics a
        JOIN rank_within_country_view b ON a.asn = b.asn
        JOIN countries_with_starlink_measurements c ON a.country_code = c.country_code
    WHERE b.rank_within_country <= 5 OR a.asn = 14593
    ORDER BY a.country_name, b.rank_within_country
"""
)

countries_with_starlink_measurements_create_query = sql.SQL(
    """
    CREATE TABLE IF NOT EXISTS public.countries_with_starlink_measurements
    (
        country_code CHAR(2) COLLATE pg_catalog."default" NOT NULL,
        CONSTRAINT countries_with_starlink_measurements_pkey PRIMARY KEY (country_code)
    )
"""
)

countries_with_starlink_measurements_insert_query = sql.SQL(
    """
    INSERT INTO countries_with_starlink_measurements (country_code) VALUES %s
"""
)
