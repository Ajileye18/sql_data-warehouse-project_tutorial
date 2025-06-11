-- Transformation Query
TRUNCATE TABLE silver.crm_cust_info;
INSERT INTO silver.crm_cust_info (cst_id, cst_key, cst_firstname, cst_lastname, 
cst_marital_status, cst_gndr, cst_create_date)
SELECT cst_id,
cst_key,
TRIM(cst_firstname) cst_firstname,
TRIM(cst_lastname) cst_lastname,
CASE
	WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
    WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
    ELSE 'N/A'
END cst_marital_status,
CASE
	WHEN UPPER(cst_gndr) = 'F' THEN 'Female'
    WHEN UPPER(cst_gndr) = 'M' THEN 'Male'
    ELSE 'N/A'
END cst_gndr,
cst_create_date
FROM (
SELECT *,
ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
FROM bronze.crm_cust_info) T
WHERE flag_last = 1;

-- SELECT cst_id FROM silver.crm_cust_info order by cst_id;

-- Data Cleaning and silver layer creation for silver.crm_prd_info from bronze
TRUNCATE TABLE silver.crm_prd_info;
INSERT INTO silver.crm_prd_info (prd_id, cat_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt)
SELECT prd_id,
REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') cat_id,
SUBSTRING(prd_key, 7, LENGTH(prd_key)) prd_key,
prd_nm,
COALESCE(prd_cost, 0) AS prd_cost,
CASE 
	WHEN prd_line = 'M' THEN 'Mountain'
    WHEN prd_line = 'R' THEN 'Road'
    WHEN prd_line = 'S' THEN 'Other Sales'
    WHEN prd_line = 'T' THEN 'Touring'
    ELSE 'N/A'
END prd_line,
prd_start_dt,
DATE_SUB(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt), INTERVAL 1 DAY) prd_end_dt_test
FROM bronze.crm_prd_info;

-- SELECT prd_id FROM SILVER.crm_prd_info order by prd_id;

-- Data Cleaning and silver layer creation for silver.crm_sales_details from bronze
TRUNCATE TABLE silver.crm_sales_details;
INSERT INTO silver.crm_sales_details (sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt,
			sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price)
SELECT 
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		sls_order_dt,
		sls_ship_dt,
		sls_due_dt,
		CASE 
			WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != sls_quantity * ABS(sls_price)
			THEN sls_quantity * ABS(sls_price)
			ELSE sls_sales
		END sls_sales,
		sls_quantity,
		CASE 
			WHEN sls_price IS NULL OR sls_price <=0 
			THEN sls_sales / COALESCE(sls_quantity, 0)
			ELSE sls_price
		END sls_price
FROM bronze.crm_sales_details;

-- SELECT sls_ord_num FROM silver.crm_sales_details order by sls_ord_num;



-- Data cleaning for bronze.erp_cust_az12 and creation of silver layer
TRUNCATE TABLE silver.erp_cust_az12;
INSERT INTO silver.erp_cust_az12 (cid, bdate, gen)
SELECT
    CASE 
			WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid))
            ELSE cid
	END cid,
CASE 
			WHEN bdate > curdate() THEN NULL
            ELSE bdate
END bdate, 
CASE
			WHEN UPPER(gen) IN ('F', 'Female') THEN 'Female'
			WHEN  UPPER(gen) IN ('M', 'Male') THEN 'Male'
			ELSE gen
			END gen
FROM bronze.erp_cust_az12;

-- SELECT cid FROM silver.erp_cust_az12 order by cid;



-- Data cleaning for bronze.erp_loc_a101 and creation of silver layer
TRUNCATE TABLE silver.erp_loc_a101;
INSERT INTO silver.erp_loc_a101 (cid, cntry)
SELECT REPLACE(cid, '-', '') cid,
CASE 
    WHEN UPPER(TRIM(cntry)) IN ('DE\r', 'Germany\r') THEN 'Germany'
    WHEN UPPER(TRIM(cntry)) IN ('US\r', 'USA\r', 'United States\r') THEN 'United States'
    WHEN UPPER(TRIM(cntry)) = 'france\r' THEN 'France'
    WHEN UPPER(TRIM(cntry)) = 'Australia\r' THEN 'Australia'
     WHEN UPPER(TRIM(cntry)) = 'Canada\r' THEN 'Canada'
    WHEN cntry IS NULL OR TRIM(cntry) = '\r' THEN 'n/a'
    ELSE cntry
END AS cntry
FROM bronze.erp_loc_a101;

-- SELECT DISTINCT CID FROM silver.erp_loc_a101 ORDER BY CID;

-- Data cleaning for bronze.erp_px_cat_giv2; and creation of silver layer
TRUNCATE TABLE silver.erp_px_cat_giv2;
INSERT INTO silver.erp_px_cat_giv2 (id, cat, subcat, maintenance)
SELECT *
FROM bronze.erp_px_cat_giv2;

-- SELECT  ID FROM silver.erp_px_cat_giv2 ORDER BY ID;