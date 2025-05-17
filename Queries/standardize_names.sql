UPDATE cloudflare_flat_90th_percentile cf
SET 
    city = c.asciiname,
    region = c.region
FROM cities c
WHERE cf.city IS NOT NULL
	AND cf.country_iso = c.country_iso
    AND LOWER(cf.city) IN (
        LOWER(c.name),
        LOWER(c.asciiname),
        LOWER(c.name1),
        LOWER(c.name2),
        LOWER(c.name3),
        LOWER(c.name4)
    );
	
select distinct a.city, a.region, a.country_iso, b.city, b.region, b.country_iso
from cloudflare_flat_90th_percentile a JOIN cloudflare_flat_mean b ON a.uuid = b.uuid
where a.city = 'Floresti'
order by a.country_iso

select * from cities where region = 'Cluj County'