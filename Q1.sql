--- Filter out duplicate records in city table using qualify and row_number windows function
with city as 
(
SELECT 
    city_name as city
   ,state_abbr as state 
   ,lat
   ,long
FROM 
    VK_DATA.RESOURCES.US_CITIES 
qualify row_number() over (partition by city_name,state_abbr order by 1) = 1  
)
--- get supplier info
,supplier_city as 
(
 select 
     supplier_id,
     supplier_name,
     supplier_city,
     supplier_state,
     lat as supplier_lat, 
     long as supplier_long
 from VK_DATA.SUPPLIERS.SUPPLIER_INFO a join city b
    on (upper(a.supplier_city) = upper(b.city) and upper(a.supplier_state) = upper(b.state))

)
----get customer data connected with city details 
,customer_info as 
(
select 
    a.customer_id, 
    first_name,
    last_name,
    email,
    customer_city,
    customer_state,
    lat as customer_lat,
    long as customer_long
from VK_DATA.CUSTOMERS.CUSTOMER_DATA a 
    inner join VK_DATA.CUSTOMERS.CUSTOMER_ADDRESS b on a.customer_id = b.customer_id
    inner join city c 
    on (upper(c.city) = upper(b.customer_city) and upper(c.state) = upper(b.customer_state))
)
--select * from customer_info
--calculate the distance between supplier and customer 
--cross join was implemented to get all possible combination of customers with all the cities
select 
    customer_id, 
    first_name,
    last_name,
    email,
    supplier_id,
    supplier_name,
    st_distance(st_makepoint(customer_long,customer_lat),st_makepoint(supplier_long, supplier_lat))/1000 as distance ,
    customer_city,
    customer_state
from customer_info ci 
cross join supplier_city s 
qualify row_number() over  (partition by customer_id order by distance) = 1


 