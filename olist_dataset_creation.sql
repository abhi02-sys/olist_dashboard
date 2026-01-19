use olist;
#Creating staging 
select * from olist_orders;

create table orders_staging like olist_orders;
insert orders_staging select * from olist_orders;
select * from orders_staging;

select* from olist_order_items;
create table order_items_staging like olist_order_items;
insert order_items_staging select* from olist_order_items;

select * from order_items_staging;

select * from olist_customers;
create table customers_staging like olist_customers;

insert into customers_staging select * from olist_customers;
select * from customers_staging;

create table payments_staging like order_payments;
insert into payments_staging select * from order_payments;
select * from payments_staging;

create table products_staging like olist_products;
insert into products_staging select * from olist_products;
select* from products_staging;

/* -- Table 1: orders_staging --*/

select* from orders_staging;

select count(order_id) from orders_staging where length(order_id) = 32; 
select count(order_id) from orders_staging;

select customer_id, length(customer_id) from orders_staging group by customer_id;
select count(customer_id) from orders_staging where length(customer_id) = 32; 

select order_id, count(*) from orders_staging group by order_id having count(*) > 1;
select customer_id, count(*) from orders_staging group by customer_id having count(*) > 1;

#Checking are there any rows where datetime format is incorrect n we need to preprocess before changing the col type to date time
select order_purchase_timestamp from orders_staging where str_to_date(order_purchase_timestamp, '%Y-%m-%d %H:%i:%s') is null; 

alter table orders_staging modify column order_purchase_timestamp datetime;

select order_id,order_approved_at from orders_staging where str_to_date(order_approved_at,'%Y-%m-%d %H:%i:%s') is null;
SET SQL_SAFE_UPDATES = 0;
update orders_staging set order_approved_at = null where order_approved_at = '';
alter table orders_staging modify column order_approved_at datetime;
select order_id,order_approved_at from orders_staging where order_approved_at is null;

update orders_staging set order_delivered_carrier_date = null where order_delivered_carrier_date = '';
alter table orders_staging modify column order_delivered_carrier_date datetime;

select order_id, order_delivered_customer_date from orders_staging where str_to_date(order_delivered_customer_date, '%Y-%m-%d %H:%i:%s') is null;
update orders_staging set order_delivered_customer_date = null where order_delivered_customer_date = '';
alter table orders_staging modify column order_delivered_customer_date datetime;

select order_id, order_estimated_delivery_date from orders_staging where str_to_date(order_estimated_delivery_date, '%Y-%m-%d %H:%i:%s') is null;
alter table orders_staging modify column order_estimated_delivery_date datetime;

/*----------Table 2: order_items_staging ------*/
select * from order_items_staging;
select count(order_id) from order_items_staging;
select count(order_id) from order_items_staging where length(order_id) = 32;
select count(product_id) from order_items_staging where length(product_id) = 32;
select count(seller_id) from order_items_staging where length(seller_id) = 32;

select shipping_limit_date from order_items_staging where str_to_date(shipping_limit_date,'%Y-%m-%d %H:%i:%s') is null;
alter table order_items_staging modify column shipping_limit_date datetime;

SELECT 
  COUNT(*) AS total_rows,
  COUNT(DISTINCT CONCAT(order_id, '-', order_item_id)) AS distinct_rows
FROM order_items_staging;

SELECT
  SUM(price IS NULL) AS null_price,
  SUM(freight_value IS NULL) AS null_freight,
  MIN(price) AS min_price,
  MIN(freight_value) AS min_freight
FROM order_items_staging;

/*------ Table 3: customers_staging ------ */
select * from customers_staging;

select count(customer_id) from customers_staging where length(customer_id) <> 32;
select count(customer_unique_id) from customers_staging where length(customer_unique_id) <> 32;
select customer_state, length(customer_zip_code_prefix) from customers_staging group by customer_state, customer_zip_code_prefix;
select count(customer_zip_code_prefix) from customers_staging where length(customer_zip_code_prefix) <> 5; 
SELECT 
  COUNT(*) AS total_rows,
  COUNT(DISTINCT customer_id) AS distinct_customer_id
FROM customers_staging;

SELECT customer_id
FROM customers_staging
GROUP BY customer_id
HAVING COUNT(DISTINCT customer_unique_id) > 1;

/* ----- Table 4: Payments_staging ---------- */
select * from payments_staging;
select order_id,length(order_id) from payments_staging where length(order_id) <>32;
SELECT COUNT(*) AS total_rows, COUNT(DISTINCT CONCAT(order_id, '-', payment_sequential)) AS distinct_rows FROM payments_staging;

SELECT COUNT(*) FROM payments_staging WHERE payment_value < 0;

/*---- Table 5: products_staging ----- */
select* from products_staging;
select product_id,length(product_id) from products_staging;
select product_id from products_staging where length(product_id) <> 32;

SELECT COUNT(*) FROM products_staging WHERE product_id IS NULL;

/* ----- Joins ------ */
select * from orders_staging;
select * from order_items_staging;
#To check if there are duplicate values in order_id 
select count(*) as total_rows, count(distinct order_id) as total_distinct from orders_staging;
#There are duplicate oreder_ids in order_items_staging
select count(*) as total_rows, count(distinct order_id) as total_distinct from order_items_staging;

#Creating new agg table for oder_items_staging
select order_id, count(order_item_id) as items_count, sum(price) as order_price, sum(freight_value) as order_freight_value from order_items_staging group by order_id; 

create table agg_order_items as 
select order_id, count(order_item_id) as items_count, sum(price) as order_price, sum(freight_value) as order_freight_value from order_items_staging group by order_id; 

select * from agg_order_items;

# payments_staging
select * from payments_staging;
select count(*), count(distinct order_id) from payments_staging;

select order_id, max(payment_installments) as total_payment_installments, sum(payment_value) as total_payment_value from payments_staging group by order_id; 

create table payments_agg as
select order_id, max(payment_installments) as total_payment_installments, sum(payment_value) as total_payment_value from payments_staging group by order_id; 

select * from payments_agg;

#customers_staging
select * from customers_staging;
select * from orders_staging;

select * from products_staging;
select count(*) , count(distinct product_id) from products_staging group by product_id; 

/*---- Creating final dataset ---- */
select * from orders_staging;
select* from agg_order_items;
select * from payments_agg;
select * from products_staging;
select * from customers_staging;

select o.*, oi.items_count, oi.order_price, oi.order_freight_value, p.total_payment_installments, p.total_payment_value,
c.customer_unique_id,c.customer_zip_code_prefix,c.customer_city,c.customer_state
from orders_staging as o 
left join agg_order_items as oi on o.order_id = oi.order_id
left join payments_agg as p on o.order_id = p.order_id
left join customers_staging as c on c.customer_id = o.customer_id;

create table final_olist_orders as 
select o.*, oi.items_count, oi.order_price, oi.order_freight_value, p.total_payment_installments, p.total_payment_value,
c.customer_unique_id,c.customer_zip_code_prefix,c.customer_city,c.customer_state
from orders_staging as o 
left join agg_order_items as oi on o.order_id = oi.order_id
left join payments_agg as p on o.order_id = p.order_id
left join customers_staging as c on c.customer_id = o.customer_id;

select * from final_olist_orders;
select count(*) from final_olist_orders;

select * from order_items_staging;
select * from products_staging;

SELECT
  oi.order_id,
  oi.product_id,
  p.product_category_name,
  oi.price,
  oi.freight_value,
  p.product_weight_g,
  p.product_length_cm,
  p.product_height_cm,
  p.product_width_cm,
  p.product_photos_qty,
  p.product_description_lenght
FROM order_items_staging AS oi
LEFT JOIN products_staging AS p
  ON oi.product_id = p.product_id;
  
  create table final_olist_product as 
  SELECT
  oi.order_id,
  oi.product_id,
  p.product_category_name,
  oi.price,
  oi.freight_value,
  p.product_weight_g,
  p.product_length_cm,
  p.product_height_cm,
  p.product_width_cm,
  p.product_photos_qty,
  p.product_description_lenght
FROM order_items_staging AS oi
LEFT JOIN products_staging AS p
  ON oi.product_id = p.product_id;
  
  select * from final_olist_product;







