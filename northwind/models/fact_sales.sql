with stg_orders as (
    select
        OrderID,
        {{ dbt_utils.generate_surrogate_key(['employeeid']) }} as employeekey,
        {{ dbt_utils.generate_surrogate_key(['customerid']) }} as customerkey,
        replace(to_date(orderdate)::varchar, '-', '')::int as orderdatekey,
        replace(to_date(shippeddate)::varchar, '-', '')::int as shippeddatekey,
        replace(to_date(requireddate)::varchar, '-', '')::int as requireddatekey,
        freight,
        shipvia
    from {{ source('northwind', 'Orders') }}
),

stg_order_details as (
    select
        orderid,
        sum(Quantity) as totalquantity,
        sum(Quantity * UnitPrice * (1 - Discount)) as totalamount
    from {{ source('northwind', 'Order_Details') }}
    group by orderid
)

select
    o.OrderID as orderid,
    o.employeekey,
    o.customerkey,
    o.orderdatekey,
    o.shippeddatekey,
    o.requireddatekey,
    od.totalquantity as quantity,
    od.totalamount as totalorderamount
from stg_orders o
join stg_order_details od on o.OrderID = od.orderid