WITH t1 AS(
SELECT order_id, order_date_ymd AS order_date, ship_date_ymd AS ship_date
, julianday(ship_date_ymd)-julianday(order_date_ymd) AS day_to_ship
,region, country, customer_id, product_id, quantity, sales AS sales_usd
FROM Trans_table_compact
WHERE order_date_ymd>='2023-03-15' AND order_date_ymd<='2024-05-03' --15 MAR 2023, 3 MAY 2024
)

, ex_master AS (SELECT * FROM Exchange_master WHERE import_date is NOT NULL)
, cust_master AS (
SELECT * from
(SELECT *, row_number() over (partition By customer_id ORDER BY import_date DESC) AS rn_import_date
from Customer_master) WHERE rn_import_date=1)
, prod_master AS (
SELECT * FROM
(SELECT *, row_number() over (partition by product_id order BY length(product_name)ASC) AS rn
FROM Product_master
WHERE import_date=CURRENT_DATE
ORDER BY product_id) WHERE rn=1)

-- แยก 2 condition sum(sales) เท่ากับ 0 และ sales ค่าติดลบ
, trans_if_sum_per_order_is_zero AS
(select DISTINCT order_id FROM
(SELECT order_id, sum(sales) FROM Trans_table_compact
GROUP by 1
HAVING sum(sales)=0))
, trans_if_has_negative_sale AS
(select DISTINCT order_id FROM t1
WHERE sales_usd < 0)
-- เอาทั้ง 2 ค่ามาหาค่ามา join
, has_negative_value_and_sum_per_order_is_zero as (
SELECT DISTINCT It.order_id
FROM trans_if_sum_per_order_is_zero It INNER JOIN trans_if_has_negative_sale rt
on It.order_id = rt.order_id
)

-- main process: join table
SELECT t1., c.customer_name, c.segment, pd.category, pd.subcategory
, coalesce(round(t1.sales_usdex.exchange_rate, 2),0) AS sales_THB
FROM t1
LEFT JOIN prod_master pd ON pd.product_id=t1.product_id
left JOIN ex_master ex on ex.date=t1.order_date
LEFT join cust_master c on c.customer_id=t1.customer_id
WHERE order_id not in (SELECT order_id FROM has_negative_value_and_sum_per_order_is_zero) -- เลือก order ที่ไม่ใช้ cancle
;