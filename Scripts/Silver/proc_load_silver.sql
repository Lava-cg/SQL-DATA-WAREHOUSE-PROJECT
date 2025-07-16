-------------------------------------------------------
-------STORED PROCEDURE FOR SILVER LAYER ---------------
-------------------------------------------------------

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN

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
END

EXEC silver.load_silver;
