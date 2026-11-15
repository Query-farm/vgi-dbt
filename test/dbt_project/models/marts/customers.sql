with orders as (select * from {{ ref('stg_orders') }}),
payments as (select * from {{ ref('stg_payments') }}),
customer_orders as (
    select customer_id, min(order_date) as first_order, count(order_id) as number_of_orders
    from orders group by 1
),
customer_payments as (
    select o.customer_id, sum(p.amount) as total_amount
    from payments p join orders o using (order_id) group by 1
)
select c.customer_id, c.first_name, c.last_name, co.first_order,
       coalesce(co.number_of_orders, 0) as number_of_orders,
       cp.total_amount as customer_lifetime_value
from {{ ref('stg_customers') }} c
left join customer_orders co using (customer_id)
left join customer_payments cp using (customer_id)
