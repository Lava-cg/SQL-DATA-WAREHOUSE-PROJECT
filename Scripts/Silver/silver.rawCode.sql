--BUILD SILVER LAYER - CLEAN&LOAD crm_cust_info


/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs various quality checks for data consistency, accuracy, 
    and standardization across the 'silver' layer. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

===============================================================================
*/

---------------------------------
------ Silver.crm_cust_info------
----------------------------------

-- quality check
-- 1- A primary key must be unique and not null --
-- TO FIND DUPLICATES

select cst_id,
count(*)
from bronze.crm_cust_info
group by cst_id
having COUNT(*) > 1	OR cst_id IS NULL;

-- RANK THE DUPLICATES AND SELECT FIRST ENTRY BASED ON CREATION DATE, THIS WILL GIVE NON DUPLICATE VALUES

SELECT * FROM
(
SELECT *, 
ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date) flag_last
FROM bronze.crm_cust_info
)T WHERE flag_last = 1; 


-- QUALITY CHECK 2 - Check for unwanted spaces in string values

SELECT cst_lastname FROM bronze.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname)

-- Trim spaces from cst_key, expectation: no results

SELECT cst_key
FROM bronze.crm_cust_info
WHERE cst_key != TRIM(cst_key);

-- Data Standardization & Consistence, check gender, martial status values are standardise

SELECT DISTINCT cst_gndr
FROM bronze.crm_cust_info;


SELECT 
CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
	WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
	else 'n/a'
	end cst_marital_status,

CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female' 
	-- UPPER will conver all small letter to caps and trim removes spaces so that standarization does not miss any value
	WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
	ELSE 'N/A'
	END cst_gndr

FROM bronze.crm_cust_info



-- INSERT cleaned, standardized and normalized bronze.crm_cust_info INTO silver.crm_cust_info


INSERT INTO silver.crm_cust_info (
	 cst_id, 
	 cst_key, 
	 cst_firstname, 
	 cst_lastname, 
	 cst_marital_status, 
	 cst_gndr, 
	 cst_create_date
)

SELECT 
cst_id,
cst_key,
TRIM(cst_firstname) AS cst_firstname,
TRIM(cst_lastname) AS cst_lastname,
CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
	WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
	else 'n/a'
	end cst_marital_status,

CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female' 
	WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
	ELSE 'N/A'
	END cst_gndr,
cst_create_date
FROM
(
SELECT *, 
ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date) flag_last
FROM bronze.crm_cust_info
WHERE cst_id IS NOT NULL
)T 
WHERE flag_last = 1; 


SELECT * FROM silver.crm_cust_info;
SELECT * FROM bronze.crm_prd_info

------------------------------------------
---------silver.crm_prd_info--------------
------------------------------------------


----- CLEANING bronze.crm_prd_info table and insterting it into silver.crm_prd_info table

--  prd_key is combination of category code and product key, first five char are category key
 -- however the erp_pr_cat_g1v2 has cat_id with '_' so, we must replace cat_id '-' to '_';


 
SELECT
REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') as cat_id,
SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key
from bronze.crm_prd_info

-- prd_id checking if any null or duplicate values in prd_id coloumn, expected no result


select prd_id,count(*)
from bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

-- prd_nm column, CHECK FOR UNWANTED SPACES -- expectation: no results

SELECT prd_nm
FROM bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)

-- prd_cost, check for null values, and convert it to 0

SELECT prd_cost,
COALESCE (prd_cost, 0) AS prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost IS NULL

-- prd_line, 

SELECT 
CASE UPPER(TRIM(prd_line))
	WHEN 'M' THEN 'Mountain'
	WHEN 'R' THEN 'Road'
	WHEN 'S' THEN 'Other sales'
	WHEN 'T' THEN 'Touring'
	ELSE 'N/A'
	END AS prd_line
FROM bronze.crm_prd_info

-- prd_start_dt and prd_end_dt check overlapping, and if overlapping, prd_end_dt as next start_day - 1 day

SELECT prd_start_dt, 

LEAD(prd_start_dt) OVER(PARTITION BY prd_key order by prd_start_dt ) -1  AS  prd_end_dt 

FROM bronze.crm_prd_info;

-- creating and inserting final code to silver crm_prd_info

INSERT INTO silver.crm_prd_info ( 

		prd_id,
		cat_id,
		prd_key,
		prd_nm,
		prd_cost,
		prd_line,
		prd_start_dt,
		prd_end_dt
)

SELECT
prd_id,
REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') as cat_id,-- Extract category ID
SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key, -- Extract product key
prd_nm,
COALESCE (prd_cost, 0) AS prd_cost,
CASE UPPER(TRIM(prd_line))
	WHEN 'M' THEN 'Mountain'
	WHEN 'R' THEN 'Road'
	WHEN 'S' THEN 'Other sales'
	WHEN 'T' THEN 'Touring'
	ELSE 'N/A'
	END AS prd_line,
CAST(prd_start_dt AS date) AS prd_start_dt,
CAST(LEAD(prd_start_dt) OVER(PARTITION BY prd_key order by prd_start_dt ) -1 AS date)  AS  prd_end_dt 
FROM bronze.crm_prd_info;


-- ----------------------------
-- silver crm_sales_details ---
-------------------------------

SELECT * FROM bronze.crm_sales_details;

-- sls_order_dt, sls_ship_dt,sls_due_dt , check if  date is 0, or more than 8 digit, if yes change it to null, 
-- and update date from INT to DATE format USING CAST 
-- Explicit conversion from data type int to date is not allowed, SO CAST to varchar and then to date

SELECT 
CASE WHEN sls_order_dt = 0 or LEN(sls_order_dt) != 8 THEN NULL
	ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS date)
	END AS sls_order_dt,
CASE WHEN sls_ship_dt = 0 or LEN(sls_ship_dt) != 8 THEN NULL
	ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS date)
	END AS sls_ship_dt,
CASE WHEN sls_ship_dt = 0 or LEN(sls_ship_dt) != 8 THEN NULL
	ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS date)
	END AS sls_order_dt
FROM bronze.crm_sales_details

--- sls_sales, sls_quantity,sls_price

SELECT 
sls_sales, sls_quantity,sls_price,

CASE 
				WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) 
					THEN sls_quantity * ABS(sls_price)
				ELSE sls_sales
			END AS sls_sales, -- Recalculate sales if original value is missing or incorrect
			sls_quantity,
			CASE 
				WHEN sls_price IS NULL OR sls_price <= 0 
					THEN sls_sales / NULLIF(sls_quantity, 0)
				ELSE sls_price  -- Derive price if original value is invalid
			END AS sls_price
from bronze.crm_sales_details;

--- Inserting bronze.crm_sales_details into silver.crm_sales_details ---

select sls_price, sls_quantity, sls_sales from bronze.crm_sales_details where sls_quantity > 1;
select sls_price, sls_quantity, sls_sales from silver.crm_sales_details where sls_quantity > 1;


select * from silver.crm_sales_details;


INSERT INTO silver.crm_sales_details (
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price 
	)

SELECT 
sls_ord_num,
sls_prd_key,
sls_cust_id,
CASE 
	WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
	ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
END AS sls_order_dt,
CASE 
	WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
	ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
END AS sls_ship_dt,
	CASE 
	WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
	ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
END AS sls_due_dt,
CASE 
	WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) 
	THEN sls_quantity * ABS(sls_price)
	ELSE sls_sales
END AS sls_sales, -- Recalculate sales if original value is missing or incorrect
sls_quantity,
CASE 
	WHEN sls_price IS NULL OR sls_price <= 0 
	THEN sls_sales / NULLIF(sls_quantity, 0)
	ELSE sls_price  -- Derive price if original value is invalid
END AS sls_price
FROM bronze.crm_sales_details;

-------------------------------------------
--------- Silver.erp_cust_az12-------------
-------------------------------------------



SELECT * FROM bronze.erp_cust_az12;

SELECT * FROM SILVER.erp_cust_az12;

-- As per integration model to connect tables, the erp_cust_az12(CID) connects to crm_cust_info(cst_id)
-- the bronze.erp_cust_az12(cid) values have incorrect data, starts with '%NAS', we must remove and update CID so its ready to connect with crm_cust_info

SELECT 
CID,
CASE WHEN cid LIKE 'NAS%'THEN SUBSTRING(cid, 4, LEN(cid))
	ELSE cid
END cid
FROM bronze.erp_cust_az12;


-- BDATE, Identify out-of-range dates

SELECT
BDATE
from bronze.erp_cust_az12
WHERE BDATE < '1991-01-01' OR BDATE > GETDATE();

-- GEN, Data Standardization & Consistency check

SELECT DISTINCT GEN,
CASE WHEN UPPER(TRIM(GEN)) IN ('F', 'FEMALE') THEN 'Female'
	WHEN UPPER(TRIM(GEN)) IN ('M', 'MALE') THEN 'Male'
	ELSE 'N/A'
	END AS GEN
from bronze.erp_cust_az12

----------- INSERTING INTO Silver.erp_cust_az12 -----




INSERT INTO Silver.erp_cust_az12 (
	CID,
	BDATE,
	GEN
)
SELECT 
CASE WHEN cid LIKE 'NAS%'THEN SUBSTRING(cid, 4, LEN(cid))
	ELSE cid
END cid,
CASE 
	WHEN BDATE > GETDATE() THEN NULL
	ELSE BDATE
END AS BDATE, -- Set futuer birthdates to NULL
CASE WHEN UPPER(TRIM(GEN)) IN ('F', 'FEMALE') THEN 'Female'
	WHEN UPPER(TRIM(GEN)) IN ('M', 'MALE') THEN 'Male'
	ELSE 'N/A'
END AS GEN -- Normalize gender values and handle unknown cases
from bronze.erp_cust_az12;

select * from silver.erp_cust_az12;

-------------------------------------------
--------- Silver.erp_loc_a101-------------
-------------------------------------------


select * from bronze.erp_loc_a101;

-- cid, crm_cust_info(cst_key) -> erp_loc_a101(cid) removing '-'

SELECT 
REPLACE(CID, '-', '') AS CID
FROM bronze.erp_loc_a101;


-- cntry, Normalize 

SELECT
DISTINCT cntry,
CASE
	WHEN UPPER(TRIM(cntry)) in ( 'DE', 'Germany') THEN 'Germany'
	WHEN UPPER(TRIM(cntry)) in ('US', 'USA') THEN 'United States'
	ELSE cntry
END AS cntry 
FROM bronze.erp_loc_a101


-- INSERTING INTO Silver.erp_loc_a101------

INSERT INTO silver.erp_loc_a101 (
cid,
cntry
)

select 
REPLACE(cid, '-', '') AS cid,
CASE
	WHEN UPPER(TRIM(cntry)) in ( 'DE', 'Germany') THEN 'Germany'
	WHEN UPPER(TRIM(cntry)) in ('US', 'USA') THEN 'United States'
	ELSE cntry
END AS cntry
FROM bronze.erp_loc_a101;

select * from silver.erp_loc_a101;


-------------------------------------------
--------- Silver.erp_px_cat_g1v2-------------
-------------------------------------------

select * from bronze.erp_px_cat_g1v2;

-- cat, checking for unwanted spaces, EXPECTED OUTPUT EMPTY

SELECT * FROM bronze.erp_px_cat_g1v2
where cat != Trim(cat)  OR subcat!= Trim(subcat) OR maintenance!= TRIM(maintenance);

--- maintenance, standarization check

SELECT distinct maintenance FROM bronze.erp_px_cat_g1v2


---- INSERTING INTO Silver.erp_px_cat_g1v2

INSERT INTO Silver.erp_px_cat_g1v2 (
	id,
	cat,
	subcat,
	maintenance
)
SELECT 
	id,
	cat,
	subcat,
	maintenance
FROM bronze.erp_px_cat_g1v2

SELECT * from silver.erp_px_cat_g1v2;



-------------------------------------------------------
------- COMPLETED SILVER LAYER ---------------
-------------------------------------------------------

PRINT '>> Truncating and Inserting date into Table >> silver.crm_cust_info';
TRUNCATE TABLE silver.crm_cust_info;
INSERT INTO silver.crm_cust_info (
	 cst_id, 
	 cst_key, 
	 cst_firstname, 
	 cst_lastname, 
	 cst_marital_status, 
	 cst_gndr, 
	 cst_create_date
)

SELECT 
cst_id,
cst_key,
TRIM(cst_firstname) AS cst_firstname,
TRIM(cst_lastname) AS cst_lastname,
CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
	WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
	else 'n/a'
	end cst_marital_status,

CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female' 
	WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
	ELSE 'N/A'
	END cst_gndr,
cst_create_date
FROM
(
SELECT *, 
ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date) flag_last
FROM bronze.crm_cust_info
WHERE cst_id IS NOT NULL
)T 
WHERE flag_last = 1; 

PRINT '>> Truncating and Inserting date into Table >> silver.crm_prd_info';
TRUNCATE TABLE silver.crm_prd_info;

INSERT INTO silver.crm_prd_info ( 

		prd_id,
		cat_id,
		prd_key,
		prd_nm,
		prd_cost,
		prd_line,
		prd_start_dt,
		prd_end_dt
)

SELECT
prd_id,
REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') as cat_id,-- Extract category ID
SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key, -- Extract product key
prd_nm,
COALESCE (prd_cost, 0) AS prd_cost,
CASE UPPER(TRIM(prd_line))
	WHEN 'M' THEN 'Mountain'
	WHEN 'R' THEN 'Road'
	WHEN 'S' THEN 'Other sales'
	WHEN 'T' THEN 'Touring'
	ELSE 'N/A'
	END AS prd_line,
CAST(prd_start_dt AS date) AS prd_start_dt,
CAST(LEAD(prd_start_dt) OVER(PARTITION BY prd_key order by prd_start_dt ) -1 AS date)  AS  prd_end_dt 
FROM bronze.crm_prd_info;

PRINT '>> Truncating and Inserting date into Table >> silver.crm_sales_details';
TRUNCATE TABLE silver.crm_sales_details;

INSERT INTO silver.crm_sales_details (
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price 
	)

SELECT 
sls_ord_num,
sls_prd_key,
sls_cust_id,
CASE 
	WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
	ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
END AS sls_order_dt,
CASE 
	WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
	ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
END AS sls_ship_dt,
	CASE 
	WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
	ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
END AS sls_due_dt,
CASE 
	WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) 
	THEN sls_quantity * ABS(sls_price)
	ELSE sls_sales
END AS sls_sales, -- Recalculate sales if original value is missing or incorrect
sls_quantity,
CASE 
	WHEN sls_price IS NULL OR sls_price <= 0 
	THEN sls_sales / NULLIF(sls_quantity, 0)
	ELSE sls_price  -- Derive price if original value is invalid
END AS sls_price
FROM bronze.crm_sales_details;

PRINT '>> Truncating and Inserting date into Table >> silver.erp_cust_az12';
TRUNCATE TABLE silver.erp_cust_az12;

INSERT INTO silver.erp_cust_az12 (
	CID,
	BDATE,
	GEN
)
SELECT 
CASE WHEN cid LIKE 'NAS%'THEN SUBSTRING(cid, 4, LEN(cid))
	ELSE cid
END cid,
CASE 
	WHEN BDATE > GETDATE() THEN NULL
	ELSE BDATE
END AS BDATE, -- Set futuer birthdates to NULL
CASE WHEN UPPER(TRIM(GEN)) IN ('F', 'FEMALE') THEN 'Female'
	WHEN UPPER(TRIM(GEN)) IN ('M', 'MALE') THEN 'Male'
	ELSE 'N/A'
END AS GEN -- Normalize gender values and handle unknown cases
from bronze.erp_cust_az12;

PRINT '>> Truncating and Inserting date into Table >> silver.erp_loc_a101';
TRUNCATE TABLE silver.erp_loc_a101;

INSERT INTO silver.erp_loc_a101 (
cid,
cntry
)

select 
REPLACE(cid, '-', '') AS cid,
CASE
	WHEN UPPER(TRIM(cntry)) in ( 'DE', 'Germany') THEN 'Germany'
	WHEN UPPER(TRIM(cntry)) in ('US', 'USA') THEN 'United States'
	ELSE cntry
END AS cntry
FROM bronze.erp_loc_a101;

PRINT '>> Truncating and Inserting date into Table >> silver.erp_px_cat_g1v2';
TRUNCATE TABLE silver.erp_px_cat_g1v2;

INSERT INTO silver.erp_px_cat_g1v2 (
	id,
	cat,
	subcat,
	maintenance
)
SELECT 
	id,
	cat,
	subcat,
	maintenance
FROM bronze.erp_px_cat_g1v2;
