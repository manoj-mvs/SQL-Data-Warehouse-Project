/*
===============================================================================
Data Quality Checks and Insertion: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This data quality check performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Data check Bronze & Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.

===============================================================================
*/

USE DataWarehouse;

PRINT '-------------------The CRM Tables-------------------'

PRINT 'TABLE 1 : bronze.crm_cust_info'
PRINT '==============================='
PRINT 'BRONZE LAYER DATA QUALITY CHECK'
PRINT '==============================='

-- Check for Nulls or Duplicates in Primary Key
-- Expectation: No Result
-- ACTUAL: There were Duplicates and NULL entries

SELECT cst_id,COUNT(*) AS tot_count
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*)>1 OR cst_id IS NULL;

-- Since there were duplicates and NULL's we used below queries
SELECT *, ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
FROM bronze.crm_cust_info

SELECT * FROM(SELECT *, ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
FROM bronze.crm_cust_info)t WHERE flag_last = 1;

-- Check for unwanted Spaces
SELECT cst_firstname
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

SELECT cst_lastname
FROM bronze.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname);

-- Data Standardization & Consistency
SELECT DISTINCT cst_gndr
FROM bronze.crm_cust_info;

SELECT DISTINCT cst_material_status
FROM bronze.crm_cust_info;

PRINT '===================================='
PRINT 'INSERTING DATA FROM BRONZE TO SILVER'
PRINT '===================================='

-- Insert Data to Silver Layer
INSERT INTO silver.crm_cust_info(
cst_id,
cst_key,
cst_firstname,
cst_lastname,
cst_marital_status,
cst_gndr,
cst_create_date)

SELECT 
cst_id,
cst_key,
TRIM(cst_firstname) AS cst_firstname,
TRIM(cst_lastname) AS cst_lastname,
CASE WHEN UPPER(TRIM(cst_material_status)) = 'S' THEN 'Single'
	 WHEN UPPER(TRIM(cst_material_status)) = 'M' THEN 'Married'
	 ELSE 'n/a'
END cst_material_status,
CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
	 WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
	 ELSE 'n/a'
END cst_gndr,
cst_create_date
FROM (
		SELECT *, ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
		FROM bronze.crm_cust_info
		WHERE cst_id IS NOT NULL
)t WHERE flag_last = 1;


PRINT '==============================='
PRINT 'SILVER LAYER DATA QUALITY CHECK'
PRINT '==============================='

-- Check silver Layer
SELECT * 
FROM silver.crm_cust_info;

-- Check for Nulls or Duplicates in Primary Key
-- Expectation: No Result

SELECT cst_id,COUNT(*) AS tot_count
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*)>1 OR cst_id IS NULL;

-- Check for unwanted Spaces

SELECT cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

SELECT cst_lastname
FROM silver.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname);

-- Data Standardization & Consistency

SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info;

SELECT DISTINCT cst_marital_status
FROM silver.crm_cust_info;


PRINT 'TABLE 2 : silver.crm_prd_info'
PRINT '==============================='
PRINT 'BRONZE LAYER DATA QUALITY CHECK'
PRINT '==============================='

-- Check for Nulls or Duplicates in Primary Key
-- Expectation: No Result
-- Actual: No duplicates or NULL's

SELECT prd_id,COUNT(*) AS tot_count
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*)>1 OR prd_id IS NULL;

-- Check for unwanted spaces
-- Check for NULL's or Negative Numbers
SELECT prd_nm
FROM bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)

-- Check for NULL's or Negative Numbers
SELECT prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost < 0  OR prd_cost IS NULL;

-- Check Standardization & Consistency
SELECT DISTINCT prd_line
FROM bronze.crm_prd_info;

-- Check for Invalid Date Orders
SELECT *
FROM bronze.crm_prd_info
WHERE prd_end_dt < prd_start_dt;

PRINT '===================================='
PRINT 'INSERTING DATA FROM BRONZE TO SILVER'
PRINT '===================================='

--Since modifications were done to bronze layer table, the silver layer meta data and cast types need to be updated
IF OBJECT_ID('silver.crm_prd_info','U') IS NOT NULL
	DROP TABLE silver.crm_prd_info;
CREATE TABLE silver.crm_prd_info (
	prd_id INT,
	cat_id NVARCHAR(50),
	prd_key NVARCHAR(50),
	prd_nm NVARCHAR(50),
	prd_cost INT,
	prd_line VARCHAR(50),
	prd_start_dt DATE,
	prd_end_dt DATE,
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);

-- Insert Data to Silver Layer
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
REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,
SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,
prd_nm,
ISNULL(prd_cost,0) AS prd_cost,
CASE UPPER(TRIM(prd_line))
	 WHEN 'M' THEN 'Mountain'
	 WHEN 'R' THEN 'Road'
	 WHEN 'S' THEN 'Other Sales'
	 WHEN 'T' THEN 'Touring'
	 ELSE 'n/a'
END AS prd_line,
CAST(prd_start_dt AS DATE) AS prd_start_dt,--Records show 00:00:00.0000 for all records
CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_dt
FROM bronze.crm_prd_info

PRINT '==============================='
PRINT 'SILVER LAYER DATA QUALITY CHECK'
PRINT '==============================='

-- Check silver Layer
SELECT * 
FROM silver.crm_prd_info;

PRINT 'TABLE 3 : crm_sales_details'
PRINT '==============================='
PRINT 'BRONZE LAYER DATA QUALITY CHECK'
PRINT '==============================='

SELECT 
sls_ord_num,
sls_prd_key,
sls_cust_id,
sls_order_dt,
sls_ship_dt,
sls_due_dt,
sls_sales,
sls_quantity,
sls_price
FROM bronze.crm_sales_details;

-- Check for Invalid Dates sls_order_dt
SELECT sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0;
-- Replace the 0 with NULL's sls_order_dt
SELECT 
NULLIF(sls_order_dt,0) sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0 
OR LEN(sls_order_dt) != 8
OR sls_order_dt > 20500101
OR sls_order_dt < 19000101;

-- Check for Inavlid Dates sls_ship_dt
SELECT 
NULLIF(sls_ship_dt,0) sls_ship_dt
FROM bronze.crm_sales_details
WHERE sls_ship_dt <= 0 
OR LEN(sls_ship_dt) != 8
OR sls_ship_dt > 20500101
OR sls_ship_dt < 19000101;

-- Check for Inavlid Dates sls_due_dt
SELECT 
NULLIF(sls_due_dt,0) sls_due_dt
FROM bronze.crm_sales_details
WHERE sls_due_dt <= 0 
OR LEN(sls_due_dt) != 8
OR sls_due_dt > 20500101
OR sls_due_dt < 19000101;

SELECT *
FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt;

-- Check sales data
SELECT DISTINCT
sls_sales AS old_sls_sales,
sls_quantity,
sls_price AS old_sls_price,
CASE WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != sls_quantity * ABS(sls_price)
	 THEN sls_quantity * ABS(sls_price)
	 ELSE sls_sales
END AS sls_sales,
CASE WHEN sls_price IS NULL OR sls_price <=0
	 THEN sls_sales/NULLIF(sls_quantity,0)
	 ELSE sls_price
END AS sls_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <=0
ORDER BY sls_sales,sls_quantity,sls_price;

PRINT '===================================='
PRINT 'INSERTING DATA FROM BRONZE TO SILVER'
PRINT '===================================='

--Since modifications were done to bronze layer table, the silver layer meta data and cast types need to be updated
IF OBJECT_ID ('silver.crm_sales_details','U') IS NOT NULL
	DROP TABLE silver.crm_sales_details;
CREATE TABLE silver.crm_sales_details(
sls_ord_num NVARCHAR(50),
sls_prd_key NVARCHAR(50),
sls_cust_id INT,
sls_order_dt DATE,
sls_ship_dt DATE,
sls_due_dt DATE,
sls_sales INT,
sls_quantity INT,
sls_price INT,
dwh_create_date DATETIME2 DEFAULT GETDATE()
);

INSERT INTO silver.crm_sales_details(
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
CASE WHEN sls_order_dt = 0 or LEN(sls_order_dt)!=8 THEN NULL
	 ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
END AS sls_order_dt,
CASE WHEN sls_ship_dt = 0 or LEN(sls_ship_dt)!=8 THEN NULL
	 ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
END AS sls_ship_dt,
CASE WHEN sls_due_dt = 0 or LEN(sls_due_dt)!=8 THEN NULL
	 ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
END AS sls_due_dt,
CASE WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != sls_quantity * ABS(sls_price)
	 THEN sls_quantity * ABS(sls_price)
	 ELSE sls_sales
END AS sls_sales,
sls_quantity,
CASE WHEN sls_price IS NULL OR sls_price <=0
	 THEN sls_sales/NULLIF(sls_quantity,0)
	 ELSE sls_price
END AS sls_price
FROM bronze.crm_sales_details;

PRINT '==============================='
PRINT 'SILVER LAYER DATA QUALITY CHECK'
PRINT '==============================='

-- Check silver Layer
SELECT * 
FROM silver.crm_sales_details;

SELECT *
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt;

SELECT DISTINCT
sls_sales AS old_sls_sales,
sls_quantity,
sls_price AS old_sls_price,
CASE WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != sls_quantity * ABS(sls_price)
	 THEN sls_quantity * ABS(sls_price)
	 ELSE sls_sales
END AS sls_sales,
CASE WHEN sls_price IS NULL OR sls_price <=0
	 THEN sls_sales/NULLIF(sls_quantity,0)
	 ELSE sls_price
END AS sls_price
FROM silver.crm_sales_details




PRINT '-------------------The ERP Tables-------------------'


PRINT 'TABLE 4 : erp_cust_az12'
PRINT '==============================='
PRINT 'BRONZE LAYER DATA QUALITY CHECK'
PRINT '==============================='

SELECT * FROM [silver].[crm_cust_info];

-- Identify Out-of-range Dates
SELECT DISTINCT
bdate
FROM bronze.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > GETDATE();

-- Data Standardization & Consistency
SELECT DISTINCT gen
FROM bronze.erp_cust_az12;

PRINT '===================================='
PRINT 'INSERTING DATA FROM BRONZE TO SILVER'
PRINT '===================================='


INSERT INTO silver.erp_cust_az12(cid,bdate,gen)
SELECT 
CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))
	 ELSE cid
END AS cid,
CASE WHEN bdate> GETDATE() THEN NULL
	 ELSE bdate
END AS bdate,
CASE WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
	 WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
	 ELSE 'n/a'
END AS gen
FROM bronze.erp_cust_az12;


PRINT '==============================='
PRINT 'SILVER LAYER DATA QUALITY CHECK'
PRINT '==============================='

SELECT * FROM [silver].[erp_cust_az12];

-- Identify Out-of-range Dates
SELECT DISTINCT
bdate
FROM [silver].[erp_cust_az12]
WHERE bdate < '1924-01-01' OR bdate > GETDATE();

-- Data Standardization & Consistency
SELECT DISTINCT gen
FROM [silver].[erp_cust_az12];



PRINT 'TABLE 5 : erp_loc_a101'
PRINT '==============================='
PRINT 'BRONZE LAYER DATA QUALITY CHECK'
PRINT '==============================='

SELECT cid,cntry
FROM [bronze].[erp_loc_a101];
-- cid and cst_key are a bit diiferent
SELECT [cst_key] FROM [silver].[crm_cust_info];

-- Fixing cid in erp_loc_a101
SELECT 
REPLACE(cid,'-','') cid,
CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
	 WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
	 WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
	 ELSE TRIM(cntry)
END AS cntry
FROM [bronze].[erp_loc_a101]

-- Checking for unmatched data after fixing cid
SELECT 
REPLACE(cid,'-','') cid,
CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
	 WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
	 WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
	 ELSE TRIM(cntry)
END AS cntry
FROM [bronze].[erp_loc_a101] WHERE REPLACE(cid,'-','') NOT IN
(SELECT cst_key FROM [silver].[crm_cust_info]);

-- Data Standardization & Consistency
SELECT DISTINCT cntry
FROM bronze.erp_loc_a101
ORDER BY cntry;


PRINT '===================================='
PRINT 'INSERTING DATA FROM BRONZE TO SILVER'
PRINT '===================================='

INSERT INTO silver.erp_loc_a101 (cid,cntry)
SELECT 
REPLACE(cid,'-','') cid,
CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
	 WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
	 WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
	 ELSE TRIM(cntry)
END AS cntry
FROM [bronze].[erp_loc_a101]


PRINT '==============================='
PRINT 'SILVER LAYER DATA QUALITY CHECK'
PRINT '==============================='

SELECT * FROM [silver].[erp_loc_a101];

-- Data Standardization & Consistency
SELECT DISTINCT cntry
FROM silver.erp_loc_a101
ORDER BY cntry;



PRINT 'TABLE 6 : erp_px_cat_g1v2'
PRINT '==============================='
PRINT 'BRONZE LAYER DATA QUALITY CHECK'
PRINT '==============================='

SELECT 
id,
cat,
subcat,
maintenance
FROM [bronze].[erp_px_cat_g1v2];

-- Check for unwanted spaces
SELECT * FROM [bronze].[erp_px_cat_g1v2]
WHERE cat != TRIM(cat) OR subcat != TRIM(subcat) OR maintenance != TRIM(maintenance);

-- Data Standardization & Consistency
SELECT DISTINCT cat
FROM [bronze].[erp_px_cat_g1v2];

SELECT DISTINCT subcat
FROM [bronze].[erp_px_cat_g1v2];

SELECT DISTINCT maintenance
FROM [bronze].[erp_px_cat_g1v2];

PRINT '===================================='
PRINT 'INSERTING DATA FROM BRONZE TO SILVER'
PRINT '===================================='

INSERT INTO [silver].[erp_px_cat_g1v2](id,cat,subcat,maintenance)
SELECT id,cat,subcat,maintenance FROM [bronze].[erp_px_cat_g1v2];

PRINT '==============================='
PRINT 'SILVER LAYER DATA QUALITY CHECK'
PRINT '==============================='

SELECT * FROM [silver].[erp_px_cat_g1v2];


PRINT '----------END OF QUALITY CHECK & INSERTION----------'
