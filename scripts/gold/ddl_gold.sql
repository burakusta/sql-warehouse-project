/* 
========================================================
DDL Script: Create Gold Views 
========================================================
Script Purpose: 
This 
I 
pt creates views for the Gold layer in the data warehouse. The Go layer represents the final dimension and fact tables (Star Schema) 
Each view performs transformations and combines data from the Silver layer to produce a clean, enriched, and business-ready dataset. 
Usage: 
These views can be queried directly for analytics and reporting. */ 

-------- gold.dim_customers------------
CREATE or alter VIEW gold.dim_customers as
select
row_number() over(order by ci.cust_id) customer_key,
ci.cust_id as customer_id,
ci.cst_key as customer_number ,
ci.cst_firstname as first_name ,
ci.cst_lastname as last_name,
la.cntry as country,
case 
when ci.cst_gndr!= 'n/a' then ci.cst_gndr -- crm is the master for gender info
else coalesce(ca.gen, 'n/a')
end as gender,
ci.cst_marital_status as marital_status,
ca.bdate birth_date,
ci.cst_create_date as create_date
from silver.crm_cust_info as ci 
left join silver.erp_cust_az12 ca
	on ci.cst_key=ca.cid
left join silver.erp_loc_a101 la
	on ci.cst_key=la.cid;


-------- gold.dim_products------------
create or alter view gold.dim_products as
select 
	row_number() over(order by pn.prd_key) as product_key,
    pn.prd_id as product_id,
	pn.prd_key as  product_number,
	pn.prd_nm  as product_name,	
	pn.cat_id category_id ,   
	pc.cat category,
	pc.subcat subcategory,
	pc.maintaince ,
	pn.prd_cost cost,
	pn.prd_line product_line ,
	pn.prd_start_dt  as start_date
from silver.crm_prd_info pn
left join silver.erp_px_cat_g1v2 pc
 on pn.cat_id=pc.id
where prd_end_dt is null
;



-------- gold.fact_sales-------------
create or alter view gold.fact_sales as
select  
	sd.sls_ord_num as order_number,
	pr.product_key,
	cu.customer_key,
	sd.sls_order_dt order_date,
	sd.sls_ship_dt ship_date,
	sd.sls_due_dt due_date ,
	sd.sls_sales sales_amount,
	sd.sls_quantity quantity,
	sd.sls_price price 


from silver.crm_sales_details sd
left join gold.dim_products pr
	on sd.sls_prd_key=pr.product_number
left join gold.dim_customers cu
 on sd.sls_cust_id= cu.customer_id;


