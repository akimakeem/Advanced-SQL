/*  
The Virtual Kitchen developers are making some changes to the search functionality on the website. After gathering customer feedback, they want to change the recipe suggestion algorithm in order to improve the customer experience.

We have a beta version of the website available and have opened it for use by a small number of customers. Next week we plan to increase this number from 200 customers to 5,000. To ensure everything is ready for the test, we have implemented logging and are saving results to a table in Snowflake called vk_data.events.website_activity.

The table contains: 

event_id: A unique identifier for the user action on the website

session_id: The identifier for the user session

user_id: The identifier for the logged-in user

event_timestamp: Time of the event

event_details: Details about the event in JSON — what action was performed by the user?

Once we expand the beta version, we expect the website_activity table to grow very quickly. While it is still fairly small, we need to develop a query to measure the impact of the changes to our search algorithm. Please create a query and review the query profile to ensure that the query will be efficient once the activity increases.

We want to create a daily report to track:

Total unique sessions

The average length of sessions in seconds

The average number of searches completed before displaying a recipe 

The ID of the recipe that was most viewed 

In addition to your query, please submit a short description of what you determined from the query profile and how you structured your query to plan for a higher volume of events once the website traffic increases.
*/
WITH events AS (

    SELECT
        event_id,
        session_id,
        event_timestamp,
        TRIM(PARSE_JSON(event_details):"recipe_id", '"') AS recipe_id,
        TRIM(PARSE_JSON(event_details):"event", '"') AS event_type
    FROM VK_DATA.EVENTS.WEBSITE_ACTIVITY
    GROUP BY 1, 2, 3, 4, 5

),
grouped_sessions AS (

    SELECT
        session_id,
        DATE(min(event_timestamp)) AS min_event_day,
        datediff(second,min(event_timestamp), max(event_timestamp)) as session_length_sec,
        count_if(event_type='search') as search_cnt
    FROM events
    GROUP BY session_id

),
--select * from grouped_sessions
recipe_view as (
    select 
    	date(event_timestamp) as event_day_date,
        recipe_id,
        count(*) as total_views
    from events
    where recipe_id is not null
    group by 1,2
    qualify row_number() over(partition by event_day_date order by total_views desc)=1
)
,
result AS (

    SELECT
        min_event_day AS event_day,
        COUNT(session_id) AS unique_sessions,
        round(AVG(session_length_sec)) AS avg_session_length_sec,
        AVG(search_cnt) as avg_search,
        MAX(recipe_id) as top_recipe_id
    FROM grouped_sessions
    INNER JOIN recipe_view on grouped_sessions.min_event_day = recipe_view.event_day_date
GROUP by 1
)
SELECT * FROM result order by 1
