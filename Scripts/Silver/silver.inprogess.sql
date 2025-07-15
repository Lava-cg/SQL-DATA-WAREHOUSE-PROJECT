--BUILD SILVER LAYER - CLEAN&LOAD crm_cust_info

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


----- CLEANING bronze.crm_prd_info table and insterting it into silver.crm_prd_info table

--  prd_key is combination of category code and product key, first five char are category key
 -- however the erp_pr_cat_g1v2 has cat_id with '_' so, we must replace cat_id '-' to '_';

SELECT
REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') as cat_id
from bronze.crm_prd_info

-- 

SELECT
prd_id,
prd_key,
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt
FROM bronze.crm_prd_info
