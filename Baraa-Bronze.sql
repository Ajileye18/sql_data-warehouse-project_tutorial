-- Create Database 'DataWarehouse'
CREATE DATABASE DataWarehouse;
USE datawarehouse;

-- CREATE SCHEMA
CREATE SCHEMA bronze;
CREATE SCHEMA silver;
CREATE SCHEMA gold;

-- building the bronze layer
-- Create ddl for  Tables

CREATE TABLE BRONZE.crm_cust_info(
cst_id INT,
cst_key VARCHAR(50),
cst_firstname VARCHAR(50),
cst_lastname VARCHAR(50),
cst_marital_status VARCHAR(50),
cst_gndr VARCHAR(50),
cst_create_date DATE
);

-- Create prd.info Table
DROP TABLE bronze.crm_prd_info;
CREATE TABLE bronze.crm_prd_info (
prd_id INT,
prd_key VARCHAR(50),
prd_nm VARCHAR(50),
prd_cost INT,
prd_line VARCHAR(50),
prd_start_dt DATE,
prd_end_dt DATE
);

-- create sales_details Table
DROP TABLE  bronze.crm_sales_details;
CREATE TABLE bronze.crm_sales_details (
sls_ord_num VARCHAR(50),
sls_prd_key VARCHAR(50),
sls_cust_id INT,
sls_order_dt DATE,
sls_ship_dt DATE,
sls_due_dt DATE,
sls_sales INT,
sls_quantity INT,
sls_price INT
);

-- CREATE ERP TABLE
DROP TABLE bronze.erp_cust_az12;
CREATE TABLE bronze.erp_cust_az12 (
cid VARCHAR(50),
bdate DATE,
gen VARCHAR(50)
);

DROP TABLE bronze.erp_loc_a101;
CREATE TABLE bronze.erp_loc_a101 (
cid VARCHAR(50),
cntry VARCHAR(50)
);


DROP TABLE bronze.erp_px_cat_giv2;
CREATE TABLE bronze.erp_px_cat_giv2 (
id VARCHAR(50),
cat VARCHAR(50),
subcat VARCHAR(50),
maintenance VARCHAR(50)
);

-- develop sql load script using LOAD DATA INFILE
/* LOAD DATA INFILE "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/sql-data-warehouse-project/datasets/source_crm/cust_info.csv"
INTO TABLE bronze.crm_cust_info
FIELDS TERMINATED BY ','
IGNORE 1 LINES
(@cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr, cst_create_date)
SET cst_id = NULLIF(@cst_id, '');

LOAD DATA INFILE "C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\sql-data-warehouse-project\\datasets\\source_crm\\cust_info.csv"
INTO TABLE bronze.crm_cust_info
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"' 
IGNORE 1 LINES
(@cst_id, @cst_key, @cst_firstname, @cst_lastname, @cst_marital_status, @cst_gndr, @cst_create_date)
SET
  cst_id = NULLIF(NULLIF(TRIM(@cst_id), ''), ' '),
  cst_key = NULLIF(NULLIF(TRIM(@cst_key), ''), ' '),
  cst_firstname = NULLIF(NULLIF(TRIM(@cst_firstname), ''), ' '),
  cst_lastname = NULLIF(NULLIF(TRIM(@cst_lastname), ''), ' '),
  cst_marital_status = NULLIF(NULLIF(TRIM(@cst_marital_status), ''), ' '),
  cst_gndr = NULLIF(NULLIF(TRIM(@cst_gndr), ''), ' '),
  cst_create_date = NULLIF(NULLIF(TRIM(@cst_create_date), ''), ' '); */
  
  LOAD DATA INFILE "C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\sql-data-warehouse-project\\datasets\\source_crm\\cust_info.csv"
INTO TABLE bronze.crm_cust_info
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"' 
IGNORE 1 LINES
(@cst_id, @cst_key, @cst_firstname, @cst_lastname, @cst_marital_status, @cst_gndr, @cst_create_date)
SET  
  cst_id = NULLIF(NULLIF(TRIM(@cst_id), ''), ' '),
  cst_key = NULLIF(NULLIF(TRIM(@cst_key), ''), ' '),
  cst_firstname = NULLIF(NULLIF(TRIM(@cst_firstname), ''), ' '),
  cst_lastname = NULLIF(NULLIF(TRIM(@cst_lastname), ''), ' '),
  cst_marital_status = NULLIF(NULLIF(TRIM(@cst_marital_status), ''), ' '),
  cst_gndr = NULLIF(NULLIF(TRIM(@cst_gndr), ''), ' '),
  cst_create_date = NULLIF(NULLIF(TRIM(@cst_create_date), ''), ' ');

/* SHOW FULL COLUMNS FROM bronze.crm_cust_info;
SHOW WARNINGS;
-- SHOW VARIABLES LIKE 'local_infile';
SET GLOBAL local_infile = 1;
SHOW GLOBAL VARIABLES LIKE 'local_infile';
SHOW VARIABLES LIKE 'secure_file_priv'; */

SELECT *
FROM bronze.crm_prd_info;

LOAD DATA INFILE "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/prd_info_cleaned.csv"
INTO TABLE bronze.crm_prd_info
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"' 
IGNORE 1 LINES
(@v_prd_id, @v_prd_key, @v_prd_nm, @v_prd_cost, @v_prd_line, @v_prd_start_dt, @v_prd_end_dt)
SET
prd_id = NULLIF(TRIM(@v_prd_id), ''),
prd_key = NULLIF(TRIM(@v_prd_key), ''),
prd_nm = NULLIF(TRIM(@v_prd_nm), ''),
prd_cost = NULLIF(TRIM(@v_prd_cost), ''),
prd_line = NULLIF(TRIM(@v_prd_line), ''),
prd_start_dt = NULLIF(TRIM(@v_prd_start_dt), ''),
prd_end_dt = NULLIF(TRIM(@v_prd_end_dt), '');


SELECT *
FROM bronze.crm_sales_details
LIMIT 100;

LOAD DATA INFILE "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/sql-data-warehouse-project/datasets/sales_details_cleaned.csv"
INTO TABLE bronze.crm_sales_details
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"' 
IGNORE 1 LINES
(@sls_ord_num, @sls_prd_key, @sls_cust_id, @sls_order_dt, @sls_ship_dt, @sls_due_dt, @sls_sales, @sls_quantity, @sls_price)
SET
sls_ord_num = NULLIF(TRIM(@sls_ord_num), ''),
sls_prd_key = NULLIF(TRIM(@sls_prd_key), ''),
sls_cust_id = NULLIF(TRIM(@sls_cust_id), ''),
sls_order_dt = NULLIF(TRIM(@sls_order_dt), ''),
sls_ship_dt = NULLIF(TRIM(@sls_ship_dt), ''),
sls_due_dt =  NULLIF(TRIM(@sls_due_dt), ''),
sls_sales = NULLIF(TRIM(@sls_sales), ''),
sls_quantity = NULLIF(TRIM(@sls_quantity), ''),
sls_price = NULLIF(TRIM(@sls_price), '');

-- ERP
LOAD DATA INFILE "C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\source_erp\\CUST_AZ12.csv"
INTO TABLE bronze.erp_cust_az12
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
IGNORE 1 LINES
(@CID, @BDATE, @GEN)
SET
CID = NULLIF(TRIM(@CID), ''),
BDATE = NULLIF(TRIM(@BDATE), ''),
GEN = NULLIF(TRIM(@GEN), '');

SELECT *
FROM bronze.erp_cust_az12;

LOAD DATA INFILE "C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\source_erp\\LOC_A101.csv"
INTO TABLE  bronze.erp_loc_a101
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
IGNORE 1 LINES
(@CID, @CNTRY)
SET
CID = NULLIF(TRIM(@CID), ''),
CNTRY = NULLIF(TRIM(@CNTRY), '');
SELECT *
FROM   bronze.erp_loc_a101;

LOAD DATA INFILE "C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\source_erp\\PX_CAT_G1V2.csv"
INTO TABLE bronze.erp_px_cat_giv2
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
IGNORE 1 LINES
(@ID, @CAT, @SUBCAT, @MAINTENANCE)
SET
ID = NULLIF(TRIM(@ID), ''),
CAT = NULLIF(TRIM(@CAT), ''),
SUBCAT = NULLIF(TRIM(@SUBCAT), ''),
MAINTENANCE = NULLIF(TRIM(@MAINTENANCE), '');

SELECT *
FROM  bronze.crm_cust_info;
SELECT *
FROM bronze.crm_prd_info;
SELECT *
FROM  bronze.crm_sales_details;
SELECT *
FROM  bronze.erp_cust_az12;
SELECT *
FROM  bronze.erp_loc_a101;
SELECT *
FROM bronze.erp_px_cat_giv2;

-- CREATE DDL FOR TABLES
DROP TABLE silver.crm_cust_info;
CREATE TABLE silver.crm_cust_info(
cst_id INT,
cst_key VARCHAR(50),
cst_firstname VARCHAR(50),
cst_lastname VARCHAR(50),
cst_marital_status VARCHAR(50),
cst_gndr VARCHAR(50),
cst_create_date DATE,
dwh_create_date DATETIME DEFAULT NOW()
);

-- Create prd.info Table
DROP TABLE silver.crm_prd_info;
CREATE TABLE silver.crm_prd_info (
prd_id INT,
cat_id VARCHAR (50),
prd_key VARCHAR(50),
prd_nm VARCHAR(50),
prd_cost DECIMAL(8,0),
prd_line VARCHAR(50),
prd_start_dt DATE,
prd_end_dt DATE,
dwh_create_date DATETIME DEFAULT NOW()
);

-- create sales_details Table
DROP TABLE  silver.crm_sales_details;
CREATE TABLE silver.crm_sales_details (
sls_ord_num VARCHAR(50),
sls_prd_key VARCHAR(50),
sls_cust_id INT,
sls_order_dt DATE,
sls_ship_dt DATE,
sls_due_dt DATE,
sls_sales INT,
sls_quantity INT,
sls_price INT,
dwh_create_date DATETIME DEFAULT NOW()
);

-- CREATE ERP TABLE
DROP TABLE silver.erp_cust_az12;
CREATE TABLE silver.erp_cust_az12 (
cid VARCHAR(50),
bdate DATE,
gen VARCHAR(50),
dwh_create_date DATETIME DEFAULT NOW()
);

DROP TABLE silver.erp_loc_a101;
CREATE TABLE silver.erp_loc_a101 (
cid VARCHAR(50),
cntry VARCHAR(50),
dwh_create_date DATETIME DEFAULT NOW()
);

DROP TABLE silver.erp_px_cat_giv2;
CREATE TABLE silver.erp_px_cat_giv2 (
id VARCHAR(50),
cat VARCHAR(50),
subcat VARCHAR(50),
maintenance VARCHAR(50),
dwh_create_date DATETIME DEFAULT NOW()
);

-- check for nulls or dupicate in Primary Key
SELECT *
FROM (
SELECT *,
ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
FROM bronze.crm_cust_info) T
WHERE flag_last = 1;

-- Quality Check
-- chec for unwanted spaces
-- expectation: No Results
SELECT cst_lastname
FROM bronze.crm_cust_info
WHERE cst_lastname!= TRIM(cst_lastname);

SELECT cst_marital_status
FROM bronze.crm_cust_info
WHERE cst_marital_status != TRIM(cst_marital_status);

SELECT cst_firstname
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

-- Data standardization & consistency
SELECT * FROM(
SELECT *,
ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) flag
FROM bronze.crm_cust_info) T
WHERE flag = 1;
SELECT DISTINCT(cst_gndr)
FROM bronze.crm_cust_info;

SELECT DISTINCT(cst_marital_status)
FROM bronze.crm_cust_info;

-- Data Cleaning and silver layer creation for silver.crm_prd_info from bronze
SELECT *
FROM bronze.crm_prd_info;

-- Check for data quality issue.
-- check for nulls or duplicate in the primary key
SELECT prd_id,
COUNT(*)
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;   -- NO ISSUE

-- SPLITTING THE prd_key into two information
SELECT prd_id,
prd_key,
REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') cat_id,
SUBSTRING(prd_key, 7, LENGTH(prd_key)) prd_key,
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt
FROM bronze.crm_prd_info;	 -- SELECT DISTINCT id FROM bronze.erp_px_cat_giv2;

-- check for unwanted spaces
-- Expectation: No result
SELECT prd_nm
FROM bronze.crm_prd_info
WHERE prd_nm <> trim(prd_nm);

-- Check for NULLS or Negative numbers
-- Expectation: No results
SELECT prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;

-- Standadrdizing the Data in prd_line column
-- using CASE function
SELECT CASE 
	WHEN prd_line = 'M' THEN 'Mountain'
    WHEN prd_line = 'R' THEN 'Road'
    WHEN prd_line = 'S' THEN 'Other Sales'
    WHEN prd_line = 'T' THEN 'Touring'
    ELSE 'N/A'
END prd_line
FROM bronze.crm_prd_info;

-- check for invalid date
SELECT prd_id,
prd_key,
prd_nm,
prd_start_dt,
prd_end_dt,
LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt) prd_end_dt_test
FROM bronze.crm_prd_info
WHERE prd_key IN ('AC-HE-HL-U509-R', 'AC-HE-HL-U509');



-- Data Cleaning and silver layer creation for silver.crm_sales_details from bronze
SELECT *
FROM  bronze.crm_sales_details;
-- CHECK for unwanted spaces in the columns
SELECT sls_cust_id
FROM  bronze.crm_sales_details
WHERE sls_cust_id <> TRIM(sls_cust_id); -- No Issue with all the columns

-- Check if the primary keys are in other table
SELECT sls_prd_key
FROM bronze.crm_sales_details
WHERE sls_prd_key NOT IN (SELECT sls_prd_key FROM silver.crm_prd_info); -- No issue

SELECT sls_cust_id
FROM bronze.crm_sales_details
WHERE sls_cust_id NOT IN (SELECT sls_cust_id FROM silver.crm_cust_info); -- No issue

-- check for invalid date
SELECT * FROM bronze.crm_sales_details
WHERE  sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt; -- no issue

-- Check for invalid, negative, zero or nulls in the sales, quantity and price column
SELECT DISTINCT
		sls_sales AS old_sls_sales,
		sls_quantity,
        sls_price AS old_sls_price,
        
CASE 
	WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != sls_quantity * ABS(sls_price)
	THEN sls_quantity * ABS(sls_price)
    ELSE sls_sales
END sls_sales,	-- recalculate price sales if original value is missing or incorrect

CASE 
	WHEN sls_price IS NULL OR sls_price <=0 
    THEN sls_sales / COALESCE(sls_quantity, 0)
    ELSE sls_price
END sls_price		--  recalculate price if original value is invalid
/* CASE 
	WHEN sls_quantity IS NULL OR sls_quantity <=0 OR sls_quantity * ABS(sls_price)
	THEN sls_quantity * ABS(sls_price)
    ELSE sls_sales
END sls_sales */
FROM bronze.crm_sales_details
WHERE sls_sales = sls_quantity * sls_price OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
	OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price;



-- Data cleaning for bronze.erp_cust_az12 and creation of silver layer
SELECT * 
FROM  bronze.erp_cust_az12;
-- extract key from the cid column to enable joining to other tables
SELECT
    CASE 
			WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid))
            ELSE cid
	END cid
FROM  bronze.erp_cust_az12 order by cid;

-- bdate column
SELECT bdate
FROM bronze.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > current_date(); 
-- SELECT * FROM silver.crm_cust_info;

-- Data Standardization & Consistency
-- gen column
SELECT 
CASE
	WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
	WHEN  UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
    ELSE 'n/a'
END gen
FROM bronze.erp_cust_az12;

-- data cleaning for bronze.erp_loc_a101 and creating of silver layer
SELECT cid, cntry
FROM  bronze.erp_loc_a101;


-- The cid column differs from the cid column of silver.erp_cust_az12 so it is impossible to join the tow tables
SELECT REPLACE(cid, '-', '') cid,
cntry
FROM bronze.erp_loc_a101;

-- CLEANING the country
SELECT cntry, 
CASE 
    WHEN UPPER(TRIM(cntry)) IN ('DE\r', 'Germany\r') THEN 'Germany'
    WHEN UPPER(TRIM(cntry)) IN ('US\r', 'USA\r', 'United States\r') THEN 'United States'
    WHEN UPPER(TRIM(cntry)) = 'Australia\r' THEN 'Australia'
	WHEN UPPER(TRIM(cntry)) = 'Canada\r' THEN 'Canada'
    WHEN UPPER(TRIM(cntry)) = 'France\r' THEN 'France'
    WHEN cntry IS NULL OR TRIM(cntry) = '\r' THEN 'n/a'
    ELSE cntry
END AS cntry
FROM  bronze.erp_loc_a101;

SELECT *
FROM bronze.erp_px_cat_giv2;

-- checking for unwanted spaces in the cat and subcat columns
SELECT cat
FROM bronze.erp_px_cat_giv2
WHERE cat <> TRIM(cat);

SELECT subcat
FROM bronze.erp_px_cat_giv2
WHERE subcat <> TRIM(subcat);

SELECT subcat
FROM bronze.erp_px_cat_giv2
WHERE subcat <> TRIM(subcat);

-- Data Standardization and consistency
SELECT DISTINCT cat
FROM bronze.erp_px_cat_giv2;

SELECT DISTINCT maintenance
FROM bronze.erp_px_cat_giv2;
