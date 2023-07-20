
with orders as (

	select * from {{ ref('orders_88') }}

),

customers as (

	select * from {{ ref('stg_customers_88') }}

),

final as (

	select
		customers.customer_id,
		customers.first_name,
		customers.last_name,

		count(orders.order_id) as orders_count,

		sum(orders.amount) as total_amount

	from customers

	left join orders
		on customers.customer_id = orders.customer_id

	group by 1, 2, 3

)

select * from final
