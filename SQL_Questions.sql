--1.
SELECT candidate_id
FROM candidates
WHERE skill IN ('Python', 'Tableau', 'PostgreSQL')
GROUP BY candidate_id
HAVING COUNT(DISTINCT skill) = 3
ORDER BY candidate_id;


--2.
SELECT 
  app_id,
  ROUND(100.0 * 
    SUM(CASE WHEN event_type = 'click' THEN 1 ELSE 0 END) /
    NULLIF(SUM(CASE WHEN event_type = 'impression' THEN 1 ELSE 0 END), 0), 
    2) AS ctr
FROM events
WHERE YEAR(timestamp) = 2022
GROUP BY app_id;


WITH cte AS (
  SELECT 
    app_id,
    SUM(CASE WHEN event_type = 'impression' THEN 1 ELSE 0 END) AS count_imp,
    SUM(CASE WHEN event_type = 'click' THEN 1 ELSE 0 END) AS count_click
  FROM events
  WHERE YEAR(timestamp) = 2022
  GROUP BY app_id
)
SELECT 
  app_id,
  ROUND(
    CASE 
      WHEN count_imp = 0 THEN 0 
      ELSE 100.0 * count_click / count_imp 
    END, 
    2
  ) AS ctr
FROM cte;


--3.
WITH duplicate_listings AS (
  SELECT company_id
  FROM job_listings
  GROUP BY company_id, LOWER(title), LOWER(description)
  HAVING COUNT(*) > 1
)
SELECT COUNT(DISTINCT company_id) AS duplicate_companies
FROM duplicate_listings;


SELECT COUNT(DISTINCT company_id) AS duplicate_companies
FROM (
  SELECT company_id
  FROM job_listings
  GROUP BY company_id, LOWER(title), LOWER(description)
  HAVING COUNT(*) > 1
) AS duplicates;


--4.
SELECT 
sender_id,count(*) as message_count
FROM messages
where extract(year from sent_date)='2022' and 
extract(month from sent_date)='08'
group by sender_id
order by message_count desc
limit 2;


--5.
SELECT 
  card_name,
  MAX(issued_amount) - MIN(issued_amount) AS difference
FROM monthly_cards_issued
GROUP BY card_name
ORDER BY difference DESC;


--6.
SELECT 
sum(case when device_type='laptop' then 1 else 0 end) as laptop_views,
sum(case when device_type='tablet' or device_type='phone' then 1 else 0 end) as mobile_views
FROM viewership;


SELECT 
sum(case when device_type='laptop' then 1 else 0 end) as laptop_views,
sum(case when device_type in('tablet','phone') then 1 else 0 end) as mobile_views
FROM viewership;


--7.
SELECT u.city,count(*) as total_orders
FROM trades t 
left join users u on t.user_id=u.user_id
where t.status='Completed'
group by u.city
order by total_orders desc
limit 3;


--8.
SELECT 
  ROUND(
    SUM(item_count * order_occurrences)/ SUM(order_occurrences), 
    1
  ) AS mean
FROM items_per_order;


--9.
SELECT u.user_id 
FROM texts t
left join emails e on e.email_id=t.email_id
where t.action_date=e.signup_date+interval '1' day and t.signup_action='Confirmed';


--10.
SELECT 
  manufacturer,
  COUNT(*) AS drug_count,
  ROUND(SUM(ABS(total_sales - cogs)), 2) AS total_loss
FROM pharmacy_sales
WHERE total_sales < cogs
GROUP BY manufacturer
ORDER BY total_loss DESC;


--11.
SELECT 
user_id,
datediff(max(post_date),min(post_date)) as days_between
FROM posts
where year(post_date)=2021
group by user_id
having count(distinct post_id) >=2;


--12.
ith cte as(SELECT 
user_id,count(tweet_id) as tweet_count
FROM tweets
where year(tweet_date)=2022
group by user_id)

select tweet_count as tweet_bucket,count(user_id)
from cte
group by tweet_count


--13.
SELECT p.page_id
FROM pages p
LEFT JOIN page_likes pl ON p.page_id = pl.page_id
WHERE pl.page_id IS NULL
ORDER BY p.page_id;


--14.
SELECT e.employee_id, e.name AS employee_name
FROM employee e
JOIN employee m ON e.manager_id = m.employee_id
WHERE e.salary > m.salary;


--15.
WITH q3_queries AS (
  SELECT DISTINCT employee_id, query_id
  FROM queries
  WHERE query_starttime >= '2023-07-01' AND query_starttime < '2023-10-01'
),
employee_query_counts AS (
  SELECT e.employee_id, COUNT(qq.query_id) AS unique_queries
  FROM employees e
  LEFT JOIN q3_queries qq ON e.employee_id = qq.employee_id
  GROUP BY e.employee_id
)
SELECT unique_queries, COUNT(*) AS employee_count
FROM employee_query_counts
GROUP BY unique_queries
ORDER BY unique_queries;


--16.
WITH cte AS (
  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY transaction_date) AS tn_rank
  FROM transactions
)
SELECT user_id, spend, transaction_date
FROM cte
WHERE tn_rank = 3;


--17.
with cte as
(SELECT *,dense_rank() over(order by salary desc) as sal_rank
FROM employee)

select distinct salary from cte 
where sal_rank=2;


--18.
WITH total_spend_per_product AS (
  SELECT 
    category,
    product,
    SUM(spend) AS total_spend
  FROM product_spend
  WHERE EXTRACT(YEAR FROM transaction_date) = 2022
  GROUP BY category, product
),
ranked_products AS (
  SELECT 
    category,
    product,
    total_spend,
    DENSE_RANK() OVER (PARTITION BY category ORDER BY total_spend DESC) AS rank
  FROM total_spend_per_product
)
SELECT 
  category,
  product,
  total_spend
FROM ranked_products
WHERE rank <= 2;


--19.
with cte as(SELECT 
d.department_name,e.name,e.salary,
dense_rank() over(partition by department_name order by salary desc) as sal_rank
FROM employee e
join department d on e.department_id=d.department_id)

select department_name,name,salary
from cte 
where sal_rank<=3
order by department_name asc,salary desc,name asc;


--20.
WITH confirmed_users AS (
  SELECT DISTINCT e.user_id
  FROM emails e
  JOIN texts t ON e.email_id = t.email_id
  WHERE t.signup_action = 'Confirmed'
),
total_users AS (
  SELECT COUNT(DISTINCT user_id) AS total FROM emails
),
confirmed_count AS (
  SELECT COUNT(*) AS confirmed FROM confirmed_users
)
SELECT 
  ROUND(1.0 * c.confirmed / t.total, 2) AS confirm_rate
FROM confirmed_count c, total_users t;


--21.
SELECT c.customer_id
FROM customer_contracts c
JOIN products p ON c.product_id = p.product_id
GROUP BY c.customer_id
HAVING COUNT(DISTINCT p.product_category) = (
    SELECT COUNT(DISTINCT product_category) FROM products
);


--22.
WITH ranked_measurements AS (
  SELECT 
    measurement_id,
    measurement_value,
    CAST(measurement_time AS DATE) AS measurement_day,
    ROW_NUMBER() OVER (
      PARTITION BY CAST(measurement_time AS DATE) 
      ORDER BY measurement_time
    ) AS rn
  FROM measurements
),
classified AS (
  SELECT 
    measurement_day,
    CASE 
      WHEN rn % 2 = 1 THEN measurement_value 
      ELSE 0 
    END AS odd_value,
    CASE 
      WHEN rn % 2 = 0 THEN measurement_value 
      ELSE 0 
    END AS even_value
  FROM ranked_measurements
)
SELECT 
  measurement_day,
  SUM(odd_value) AS odd_sum,
  SUM(even_value) AS even_sum
FROM classified
GROUP BY measurement_day
ORDER BY measurement_day;


--23.
with cte as(
select 
case
when order_id%2!=0 and order_id!=(select max(order_id) from orders) then order_id+1
when order_id%2=0 then order_id-1
else order_id end as correct_order_id,
item
from orders
)
select correct_order_id,item
from cte 
order by correct_order_id


--24.
WITH cte AS (
  SELECT ticker,
         MIN(open) AS lowest_price,
         MAX(open) AS high_price
  FROM stock_prices
  GROUP BY ticker
)
SELECT 
  c.ticker,
  c.lowest_price,
  MIN(CASE WHEN sp.open = c.lowest_price THEN TO_CHAR(sp.date, 'Mon-YYYY') END) AS lowest_mth,
  c.high_price,
  MIN(CASE WHEN sp.open = c.high_price THEN TO_CHAR(sp.date, 'Mon-YYYY') END) AS highest_mth
FROM cte c
JOIN stock_prices sp 
  ON c.ticker = sp.ticker
GROUP BY c.ticker, c.lowest_price, c.high_price
ORDER BY c.ticker;


--25.
with cte as(SELECT *,
dense_rank() over(partition by user_id order by transaction_date desc) as rn
FROM user_transactions)
select transaction_date,user_id,count(distinct product_id) as purchase_count
from cte
where rn=1
group by transaction_date,user_id
order by transaction_date;


--26.
with cte as(SELECT *,
dense_rank() over(order by order_occurrences desc) as rnk
FROM items_per_order)
select item_count as mode 
from cte 
where rnk=1;


--27.
with cte as(SELECT count(p.caller_id) as nri_call,(select count(*) from phone_calls) as total_calls
FROM phone_calls p 
JOIN phone_info pi1 ON p.caller_id = pi1.caller_id
JOIN phone_info pi2 ON p.receiver_id = pi2.caller_id
where pi1.country_id!=pi2.country_id)

select round(100.0*nri_call/total_calls,1) as international_calls_pct
from cte 


--28.
with cte as(SELECT distinct user_id FROM user_actions
where EXTRACT(YEAR FROM event_date)=2022
AND
EXTRACT(MONTH FROM event_date)=7
INTERSECT
select DISTINCT user_id from user_actions
where EXTRACT(YEAR FROM event_date)=2022
AND
EXTRACT(MONTH FROM event_date)=6)

select '7' as month,count(user_id) as monthly_active_users
from cte;


--29.
with cte as(select 
EXTRACT(year from transaction_date) as year,
product_id,
sum(spend) as curr_year_spend
from user_transactions
group by EXTRACT(YEAR FROM transaction_date),product_id
),
cte2 as(
SELECT *,lag(curr_year_spend) over(partition by product_id order by year) as prev_year_spend
from cte
)
select *,
round((curr_year_spend-prev_year_spend)/prev_year_spend*100.0,2) as yoy_rate
from cte2;


--30.
SELECT 
  CONCAT(p1.topping_name, ',', p2.topping_name, ',', p3.topping_name) AS pizza,
  (p1.ingredient_cost + p2.ingredient_cost + p3.ingredient_cost) AS total_cost
FROM 
  pizza_toppings p1
CROSS JOIN 
  pizza_toppings p2
CROSS JOIN 
  pizza_toppings p3
WHERE 
  p1.topping_name < p2.topping_name 
  AND p2.topping_name < p3.topping_name
ORDER BY 
  total_cost DESC, pizza;




