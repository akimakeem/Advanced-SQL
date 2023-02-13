/*
Query to identify the impacted customers and their attributes in order to compose an offer to these customers to make things right. 
*/
with active_customer_preference as 
(
 select 
        customer_id,
        count(*) as food_pref_count
    from vk_data.customers.customer_survey
    where is_active = true
    group by 1
),
---get chicago geo location
chicago_store_geo as 
(
 select 
        geo_location
    from vk_data.resources.us_cities 
    where city_name = 'CHICAGO' and state_abbr = 'IL'
),
-- get gary geo location
gary_store_geo as 
( 
select 
        geo_location
    from vk_data.resources.us_cities 
    where city_name = 'GARY' and state_abbr = 'IN'
)
-- get customers affected based on state and city locations 
,affected_customers as (
select 
    first_name || ' ' || last_name as customer_name,
    ca.customer_city ,
    ca.customer_state,
    ca.customer_id,
    s.food_pref_count,
    us.geo_location
  from vk_data.customers.customer_address as ca
  inner join vk_data.customers.customer_data c on ca.customer_id = c.customer_id
  left join vk_data.resources.us_cities us 
  on UPPER(rtrim(ltrim(ca.customer_state))) = upper(TRIM(us.state_abbr))
  and trim(lower(ca.customer_city)) = trim(lower(us.city_name))   
  inner join active_customer_preference s on c.customer_id = s.customer_id  
where 
    ((trim(city_name) ilike '%concord%' or trim(city_name) ilike '%georgetown%' or trim(city_name) ilike '%ashland%')
    and customer_state = 'KY')
    or
    (customer_state = 'CA' and (trim(city_name) ilike '%oakland%' or trim(city_name) ilike '%pleasant hill%'))
    or
    (customer_state = 'TX' and (trim(city_name) ilike '%arlington%' or trim(city_name) ilike '%brownsville%'))
)
select 
  customer_name,
  customer_city,
  customer_state,
  food_pref_count,
  (st_distance(us.geo_location, chic.geo_location) / 1609)::int as chicago_distance_miles,
  (st_distance(us.geo_location, gary.geo_location) / 1609)::int as gary_distance_miles
from affected_customers us 
cross join chicago_store_geo chic 
cross join gary_store_geo gary
