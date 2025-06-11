-- Star Schema 
-- Gold Layer

CREATE VIEW gold.dim_customers AS
SELECT 
ROW_NUMBER() OVER (ORDER BY cst_id) customer_key,
ci.cst_id customer_id,
ci.cst_key customer_number, 
ci.cst_firstname first_name,
ci.cst_lastname last_name,
1a.cntry country,
ci.cst_marital_status marital_status,
CASE WHEN ci.cst_gndr <> 'N/A' THEN ci.cst_gndr
ELSE COALESCE(ca.gen, 'n/a')
END gender,
ca.bdate birthdate,
ci.cst_create_date create_date
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca ON
ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 1a
ON ci.cst_key = 1a.cid;

SELECT * FROM gold.dim_customers;

-- CREATE VIEW for dimension Table
CREATE VIEW gold.dim_products AS
SELECT
ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) product_key,
pn.prd_id product_id,
pn.prd_key product_number,
pn.prd_nm product_name,
pn.cat_id category_id,
pc.cat category,
pc.subcat subcategory,
pc.maintenance,
pn.prd_cost cost,
pn.prd_line product_line,
pn.prd_start_dt start_date
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_giv2 pc
ON pn.cat_id = pc.id
WHERE prd_end_dt IS NULL;  -- filter out all hidtorical data
SELECT * FROM gold.dim_products;

-- CREATE fact table
CREATE VIEW gold.fact_sales AS
SELECT sls_ord_num order_number,
pr.product_key,
cu.customer_key,
sls_order_dt order_date,
sls_ship_dt shipping_date,
sls_due_dt due_date,
sls_sales sales_amount,
sls_quantity quantity,
sls_price price
FROM silver.crm_sales_details sd
LEFT JOIN GOLD.DIM_PRODUCTs pr
ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers cu
ON sd.sls_cust_id = cu.customer_id;

-- foreign key check
-- Expected reusult: none
SELECT * 
FROM gold.fact_sales s
LEFT JOIN gold.dim_customers CU
ON s.customer_key = cu.customer_key
WHERE cu.customer_key IS NULL;  -- return no value. joins are valid

SELECT * 
FROM gold.fact_sales s
LEFT JOIN gold.dim_products p
ON s.product_key = p.product_key
WHERE p.product_key IS NULL;  -- return no value. joins are valid

