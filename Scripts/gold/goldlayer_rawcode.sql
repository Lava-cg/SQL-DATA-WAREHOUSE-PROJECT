---- Gold layer ---



----------------------------
---------Customers----------
----------------------------

SELECT * FROM silver.crm_cust_info;
SELECT * FROM silver.erp_cust_az12;
SELECT * FROM silver.erp_loc_a101;



-- Joining customer data from three tables(key) silver.crm_cust_info(cst_key) silver.erp_cust_az12(cid), silver.erp_loc_a101(cid). 
SELECT 
	ci.cst_id, 
	ci.cst_key,
	ci.cst_firstname,
	ci.cst_lastname,
	ci.cst_marital_status,
	ci.cst_gndr,
	ci.cst_create_date,
	ca.bdate,
	ca.gen,
	la.cntry
FROM silver.crm_cust_info CI
INNER JOIN silver.erp_cust_az12 CA
ON CI.cst_key = CA.cid
INNER JOIN silver.erp_loc_a101 LA
ON CI.cst_key = la.cid;

-- test  check if above customerinfo join table has duplicates cid's

SELECT cst_id, count(*) from 
(
SELECT 
	ci.cst_id, 
	ci.cst_key,
	ci.cst_firstname,
	ci.cst_lastname,
	ci.cst_marital_status,
	ci.cst_gndr,
	ci.cst_create_date,
	ca.bdate,
	ca.gen,
	la.cntry
FROM silver.crm_cust_info CI
INNER JOIN silver.erp_cust_az12 CA
ON CI.cst_key = CA.cid
INNER JOIN silver.erp_loc_a101 LA
ON CI.cst_key = la.cid
)t 
GROUP BY cst_id
HAVING COUNT(*)>1

--- DATA Integration issue as two tables as gender columns, cst_gndr, gen

SELECT DISTINCT 
	ci.cst_gndr,
	ca.gen
FROM  silver.crm_cust_info CI
INNER JOIN silver.erp_cust_az12 CA
ON CI.cst_key = CA.cid
 ORDER BY 1,2

 -- Above result columns shows different gender value, not matching. we consider crm file value as master
 -- and replace gen values with cst_gndr and if  cst_gndr is null or N/A and gnr has values, then use it

 SELECT DISTINCT 
	ci.cst_gndr,
	ca.gen,
	CASE WHEN ci.cst_gndr!= 'N/A' THEN ci.cst_gndr
	ELSE COALESCE(ca.gen, 'n/a')
	END AS NEW_GEN
FROM  silver.crm_cust_info CI
INNER JOIN silver.erp_cust_az12 CA
ON CI.cst_key = CA.cid
 ORDER BY 1,2


 --------   CREATING CUSTOMER DIMENSIONS view TABLE ---
 Drop view gold_dim_customers;

 CREATE VIEW gold.dim_customers AS
 SELECT 
	ROW_NUMBER() OVER(ORDER BY ci.cst_id) AS customer_key, -- since its a dimension table, we create a product key
	ci.cst_id AS customer_ID, 
	ci.cst_key AS customer_number,
	ci.cst_firstname AS first_name,
	ci.cst_lastname AS last_name,
	la.cntry As country,
	ci.cst_marital_status AS martial_status,	
	CASE WHEN ci.cst_gndr!= 'N/A' THEN ci.cst_gndr
	ELSE COALESCE(ca.gen, 'N/A')
	END AS Gender,
	ca.bdate AS birth_day,
	ci.cst_create_date AS create_date	
FROM silver.crm_cust_info CI
LEFT JOIN silver.erp_cust_az12 CA
ON CI.cst_key = CA.cid
LEFT JOIN silver.erp_loc_a101 LA
ON CI.cst_key = la.cid

select * from gold.dim_customers


--------------------------
-------- products --------
--------------------------

select * from silver.crm_sales_details;
select * from silver.crm_prd_info;
select * from silver.erp_px_cat_g1v2;


--- JOINING silver.crm_prd_info(prd_id) and silver.erp_px_cat_g1v2(id)
--------- checking for any duplicates


SELECT prd_key, COUNT(*) FROM (
SELECT 
	pn.prd_id ,
	pn.prd_key,
	pn.prd_nm,
	pn.cat_id ,
	pc.cat ,
	pc.subcat ,
	pc.maintenance ,
	pn.prd_cost,
	pn.prd_line,
	pn.prd_start_dt 
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
ON pn.cat_id = pc.id
WHERE pn.prd_end_dt IS NULL-- Filter out all historical data
)t
GROUP BY prd_key
HAVING COUNT(*) > 1


--------- Sort the columns into logical groups to improve readability
--------- Renaming columns to friendly, meaningful names
--------- creating view for products dimensions table

drop view gold_dim_products

CREATE VIEW gold.dim_products AS


SELECT 
	ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key  ) AS product_key, -- since its a dimension table, we create a product key
	pn.prd_id       AS product_id,
	pn.prd_key      AS product_number,
	pn.prd_nm       AS product_name,
	pn.cat_id       AS category_id,
	pc.cat          AS category,
	pc.subcat       AS subcategory,
	pc.maintenance  AS maintenance,
	pn.prd_cost     AS cost,
	pn.prd_line     AS product_line,
	pn.prd_start_dt AS start_date
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
ON pn.cat_id = pc.id
WHERE pn.prd_end_dt IS NULL;-- Filter out all historical data


SELECT * FROM gold.dim_products;


---------------- sales ----------------
---------------------------------------

-- SALES table is a fact, It has multiple dimenions connecting.

-- Building FACT

-- Use the dimension's surrogate keys instead of id's to easily connect facts with dimensions


select * from silver.crm_sales_details;
select * from gold_dim_products;
select * from gold_dim_customers;



CREATE VIEW gold.fact_sales AS
SELECT 
		sd.sls_ord_num AS order_number,
		pr.product_key, 
		cu.customer_key,
		sd.sls_order_dt AS order_date,
		sd.sls_ship_dt AS shipping_date,
		sd.sls_due_dt AS due_date,
		sd.sls_sales AS sales_amount,
		sd.sls_quantity AS quantity,
		sd.sls_price AS price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr
ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers cu
ON sd.sls_cust_id = cu.customer_id

select * from gold.fact_sales;
select * from gold.dim_customers;
select * from gold.dim_products;

---------------

-- Testing foreighkey integrity ( Dimensions), expected no result

SELECT * FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON F.customer_key = c.customer_key
LEFT JOIN gold.dim_products p
ON f.product_key = p.product_id
where c.customer_key = null;
