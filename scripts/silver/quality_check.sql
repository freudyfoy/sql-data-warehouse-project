/*
===============================================================================
Quality check
===============================================================================
Script Purpose:
    This quality check performs various quality checkes which includes:
	- Null or duplicate primary keys
	- Unwanted spaces in string values
	- Data standardization and consistency
	- Invalid date range and orders
	- Data consistency between related fields
		
Note:
	Run these checks after loading silver layer
===============================================================================
*/

-- ## Cleaning data
-- ## crm_cust_info
-- ##################################################################################################
-- ##################################################################################################
TRUNCATE TABLE silver.crm_cust_info
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
		 ELSE 'n/a' -- Normalize marital status values to readable format
	END AS cst_marital_status,
	CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
		 WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
		 ELSE 'n/a'
	END AS cst_gndr, -- Normalize gender values to readable format
	cst_create_date
FROM (
	SELECT
	*,
	ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
	FROM bronze.crm_cust_info
	WHERE cst_id IS NOT NULL
) t 
WHERE flag_last = 1; -- Select the most recent record per customer
-- ##################################################################################################
-- ##################################################################################################

-- ### Start investigate ###

-- Check for NULLs or Duplicatse in Primary Key
-- Expectation: No result
select top(1000) *
from silver.crm_cust_info;

select
cst_id,
COUNT(*)
from bronze.crm_cust_info
group by cst_id
having count(*) > 1 or cst_id is null; -- Duplicate/Null


-- Check for unwanted spaces
-- Expectation: No results
select
*
from bronze.crm_cust_info
where cst_firstname != TRIM(cst_firstname);


-- Check data standardization & consistency
-- Since there is NULL values so we'll use the dafualt value 'n/a' for missing values
select DISTINCT cst_gndr 
from bronze.crm_cust_info

select
DISTINCT cst_marital_status
from bronze.crm_cust_info

-- ##################################################################################################
-- ##################################################################################################
-- ## crm_prd_info
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
	REPLACE(SUBSTRING(prd_key,1,5), '-', '_') AS cat_id,
	SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key, 
	prd_nm,
	ISNULL(prd_cost,0) AS prd_cost,
	CASE UPPER(TRIM(prd_line))
		 WHEN 'M' THEN 'Mountain'
		 WHEN 'R' THEN 'Road'
		 WHEN 'S' THEN 'other Sales'
		 WHEN 'T' THEN 'Touring'
		 ELSE 'n/a'
	END AS prd_line,
	CAST(prd_start_dt AS DATE) AS prd_start_dt, -- Data transformation
	CAST(
		  LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 -- Data Enrinchment: Add new,relevant data to enhance dataset
		  AS DATE 
	) AS prd_end_dt
FROM bronze.crm_prd_info;
-- ##################################################################################################
-- ##################################################################################################

-- ### Start investigate ###

-- Check for NULLs or Duplicatse in Primary Key
select
prd_id,
COUNT(*)
from bronze.crm_prd_info
group by prd_id
having count(*) > 1 or prd_id is null;
-- Result: No result

-- Check prd_key
-- Since the prd_key is long string which contains 5 first characters of product categories ids
select 
prd_key
from bronze.crm_prd_info;
-- Re-check with bronze.erp_px_cat_g1v2 table (product categories)  
select distinct id from bronze.erp_px_cat_g1v2; 
-- and
select sls_prd_key from bronze.crm_sales_details;

-- Check prd_nm for unwanted spaces
select prd_nm from bronze.crm_prd_info
where prd_nm != trim(prd_nm);
-- Result: No result

-- Check prd_cost for NULLs or Negative numbers
select prd_cost from bronze.crm_prd_info
where prd_cost < 0 or prd_cost is null;
-- Result: 2 NULLs

-- Check prd_line for data standardization
-- Since there is NULL values so we'll use the dafualt value 'n/a' for missing values
select distinct prd_line
from bronze.crm_prd_info;

-- Check prd_start_dt and prd_end_dt for invalid date orders
-- Expectation: End date must not be earlier that the start date
select *
from bronze.crm_prd_info
where prd_end_dt < prd_start_dt or prd_start_dt is null or prd_end_dt is null 
-- Result: There are data issues
-- Solution #1: Switch start date and end date (Caution: Overlapping date data)
-- Solution #2: End date = Start date of the NEXT record - 1

select
prd_id,
prd_key,
prd_nm,
prd_start_dt,
prd_end_dt,
LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS prd_end_dt_test
from bronze.crm_prd_info
where prd_key in ('AC-HE-HL-U509-R', 'AC-HE-HL-U509')


-- ##################################################################################################
-- ##################################################################################################
-- crm_sales_details

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
	CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
		 ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
	END AS sls_order_dt,
	CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
		 ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
	END AS sls_ship_dt,
	CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
		 ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
	END AS sls_due_dt,
	CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
			THEN sls_quantity * ABS(sls_price)
		 ELSE sls_sales
	END AS sls_sales,
	sls_quantity,
	CASE WHEN sls_price IS NULL OR sls_price <= 0
			THEN sls_sales / NULLIF(sls_quantity, 0)
		 ELSE sls_price
	END AS sls_price
FROM bronze.crm_sales_details

-- ##################################################################################################
-- ##################################################################################################

-- ### Start investigate ###

-- Check sls_ord_num  for unwanted spaces or null values
select *
from bronze.crm_sales_details
where sls_ord_num != trim(sls_ord_num) or sls_ord_num is null;
-- Result: No result

-- Check sls_prd_key and sls_cst_id
-- Since sls_prd_key relates to prd_key from crm_prd_info
-- and sls_cst_id relates to cst_id from crm_cust_info
select * 
from bronze.crm_sales_details
where --sls_cust_id not in (select cst_id from silver.crm_cust_info)  
sls_prd_key not in (select prd_key from silver.crm_prd_info)
-- Result: No result

-- Check sls_order_dt, sls_ship_dt, sls_due_dt
-- Since sls_order_dt is the integer so need to transform to DATE
select sls_order_dt
from bronze.crm_sales_details
where sls_order_dt <= 0 
or len(sls_order_dt) != 8
or sls_order_dt > 20500101
or sls_order_dt < 19000101

-- Check for Invalid Date Orders
select * 
from bronze.crm_sales_details
where sls_order_dt > sls_ship_dt or sls_order_dt > sls_due_dt 
or sls_ship_dt > sls_due_dt
-- Result: No result

-- Check Data Consistency: Between Sales, Quantity and Price
-- >> Sales = Quantity * Price
-- >> Values must not be NULL, zero or negative

select distinct
sls_sales as old_sls_sales,
sls_quantity,
sls_price as old_sls_price,
CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
		THEN sls_quantity * ABS(sls_price)
	 ELSE sls_sales
END AS sls_sales,
CASE WHEN sls_price IS NULL OR sls_price <= 0
		THEN sls_sales / NULLIF(sls_quantity, 0)
	 ELSE sls_price
END AS sls_price
from bronze.crm_sales_details
where sls_sales != sls_quantity * sls_price
or sls_sales is null or sls_quantity is null or sls_price is null
or sls_sales <= 0 or sls_quantity <= 0 or sls_price <= 0
order by sls_sales, sls_quantity

-- Result: bad qualities: wrong calculated sales, zero data, null data and negative data
-- Solution #1: Data issues will be fixed direct in source system/ talk to expert or business team
-- Solution #2: Data issues has to be fixed in data warehouse
-- Rules: If Sales is negative, zero, or null, derive it using Quantity and Price
--		  If Price is zero or null, calculate it using Sales and Quantity
--		  If Price is negative, convert it to a positive value

-- ##################################################################################################
-- ##################################################################################################
-- erp_cust_az12
TRUNCATE TABLE silver.erp_cust_az12;
INSERT INTO silver.erp_cust_az12 (
	CID,
	BDATE,
	GEN
)
SELECT
	--cid,
	CASE WHEN CID LIKE 'NAS%' THEN SUBSTRING(CID,4,LEN(CID))
		 ELSE CID
	END AS CID,
	CASE WHEN BDATE > GETDATE() THEN NULL
		 ELSE BDATE
	END AS BDATE,
	CASE WHEN UPPER(TRIM(GEN)) IN ('F', 'FEMALE') THEN 'Female'
		 WHEN UPPER(TRIM(GEN)) IN ('M', 'MALE') THEN 'Male'
		 ELSE 'n/a'
	END AS GEN
FROM bronze.erp_cust_az12


-- ##################################################################################################
-- ##################################################################################################

-- ### Start investigate ###

-- Check CID
-- Since CID relates to cust_key in crm_cust_info
select *
from bronze.crm_cust_info

select *
from bronze.erp_cust_az12
where cid like '%AW00011000%'

-- But the three characters is no specification, to clean up we'll remove 3 chars

-- Check BDATE if it out-of-range
select distinct bdate
from bronze.erp_cust_az12
where bdate < '1924-01-01' or bdate > GETDATE() -- 100 years old and bdates in the future
-- Result: invalid dates ==> clean up by using NULL

-- Check gen for data standardization
select distinct gen
from bronze.erp_cust_az12

-- ##################################################################################################
-- ##################################################################################################
-- erp_loc_a101

TRUNCATE TABLE silver.erp_loc_a101;
INSERT INTO silver.erp_loc_a101 (
	CID,
	CNTRY
)
SELECT
	REPLACE(CID,'-','') AS CID,
	CASE WHEN TRIM(CNTRY) = 'DE' THEN 'Germany'
		 WHEN TRIM(CNTRY) IN ('US','USA') THEN 'United States'
		 WHEN TRIM(CNTRY) = '' or CNTRY IS NULL THEN 'n/a'
		 ELSE TRIM(CNTRY)
	END AS CNTRY
FROM bronze.erp_loc_a101


-- ##################################################################################################
-- ##################################################################################################

-- ### Start investigate ###

-- Check CID
-- Since CID relates to cust_key in crm_cust_info
select *
from bronze.erp_loc_a101

-- Check country
select distinct cntry
from bronze.erp_loc_a101

-- ##################################################################################################
-- ##################################################################################################
-- erp_px_cat_g1v2
TRUNCATE TABLE silver.erp_px_cat_g1v2;
INSERT INTO silver.erp_px_cat_g1v2 (
	ID,
	CAT,
	SUBCAT,
	MAINTENANCE
)
SELECT
	ID,
	CAT,
	SUBCAT,
	MAINTENANCE
FROM bronze.erp_px_cat_g1v2

-- ##################################################################################################
-- ##################################################################################################

-- ### Start investigate ###

-- Check unwanted spaces
select *
from bronze.erp_px_cat_g1v2
where CAT != TRIM(CAT) or SUBCAT != TRIM(SUBCAT) or MAINTENANCE != TRIM(MAINTENANCE)
-- Result: No result

-- Check data standardization
select distinct CAT
from bronze.erp_px_cat_g1v2

select distinct SUBCAT
from bronze.erp_px_cat_g1v2

select distinct MAINTENANCE
from bronze.erp_px_cat_g1v2

-- ##################################################################################################
-- ##################################################################################################

