/*
===============================================================================
Quality Checks
===============================================================================

Script Purpose:
    This script performs various quality checks for data consistency, accuracy,
    and standardization across the 'silver' schemas. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage Notes:
    - Run these checks after data loading Silver Layer.
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/



--=====================================================
-- check for nulls or duplicates crm.cust_info
--=====================================================
-- exceptation: no result

select * from bronze.crm_cust_info;


select cust_id ,count(*)
from bronze.crm_cust_info
group by cust_id
having count(*)>1;


select * from bronze.crm_cust_info
where cust_id=29433;

======================================================
select * from bronze.crm_cust_info
where   in
(select *,row_number() over (partition by cust_id order by cst_create_date asc) flag_last
from bronze.crm_cust_info)
where cust_id=29433 ;


SELECT * 
FROM bronze.crm_cust_info
WHERE flag_last > 1
AND flag_last IN (
    SELECT ROW_NUMBER() OVER (PARTITION BY cust_id ORDER BY cst_create_date) flag_last
    FROM bronze.crm_cust_info
);





SELECT * 
from (select *,row_number() over (partition by cust_id order by cst_create_date desc ) flag_last
from bronze.crm_cust_info) t
where flag_last =1 and cust_id is not null ;




Select cst_firstname
from bronze.crm_cust_info
where cst_firstname like ' %';


select cst_firstname
from bronze.crm_cust_info
where cst_firstname!= trim(cst_firstname);


select cst_lastname
from bronze.crm_cust_info
where cst_lastname!= trim(cst_lastname);


select cst_gndr
from bronze.crm_cust_info
where cst_gndr!= trim(cst_gndr);



select gndr
from bronze.crm_cust_info;



SELECT *
INTO #crm_cust_info_1
FROM bronze.crm_cust_info;






select cust_id, count(*) t
from silver.crm_cust_info
group by cust_id
having t>1 or cust_id is null;


select *
from silver.crm_cust_info;






--=====================================================
-- check for nulls or duplicates crm.sales_details
--=====================================================


select prd_id, count(*)
from bronze.crm_prd_info
group by prd_id
having count(*)>1 or prd_id is null;



select prd_id,prd_key, replace(SUBSTRING(prd_key,1,5), '-','_') cat_id, 
	   SUBSTRING(prd_key,7,len(prd_key)) prd_key1,
	   prd_nm,
	   prd_cost,prd_line,
	   prd_start_dt,prd_end_dt
from bronze.crm_prd_info
where SUBSTRING(prd_key,7,len(prd_key)) not in (
select sls_prd_key from bronze.crm_sales_details);

select prd_key, left(prd_key,5)
from bronze.crm_prd_info;



select sls_prd_key from bronze.crm_sales_details;



select prd_nm
from  bronze.crm_prd_info
where prd_nm!=trim(prd_nm);



select prd_cost
from bronze.crm_prd_info
where prd_cost is null or prd_cost<0;



select distinct prd_line 
from bronze.crm_prd_info;




select *, row_number() over (partition by SUBSTRING(prd_key,7,len(prd_key)) order by prd_start_dt asc)
from bronze.crm_prd_info










select *

(select *, row_number() over (partition by SUBSTRING(prd_key,7,len(prd_key)) order by prd_start_dt asc) a,
COUNT(*) OVER (
            PARTITION BY SUBSTRING(prd_key, 7, LEN(prd_key))
        ) AS group_size
from bronze.crm_prd_info) ab
order by group_size desc;








select *, 
lead(prd_start_dt-1) over(partition by
						SUBSTRING(prd_key,7,len(prd_key))
						order by prd_start_dt) as adjusted_end_dt  ,
row_number() over
			(partition by SUBSTRING(prd_key,7,len(prd_key))
			order by prd_start_dt asc) ab
from bronze.crm_prd_info







select count(*)
from
(
(select *
from bronze.crm_prd_info
where prd_end_dt is null)) t
where prd_start_dt is null;


--- quality checks
select prd_id,count(*)
from  silver.crm_prd_info
group by prd_id
having count(*)>1;

select *
from  silver.crm_cust_info




--=====================================================
-- check for nulls or duplicates crm.prd_info
--=====================================================


select * from bronze.crm_sales_details;

(select *, row_number() over(partition by sls_ord_num order by sls_price) a
,count(*) over(partition by sls_ord_num) group_size
from bronze.crm_sales_details);


select * 
from bronze.crm_sales_details
where sls_ord_num!=trim(sls_ord_num);


select * 
from bronze.crm_sales_details
where sls_prd_key != trim(sls_prd_key);


select * 
from bronze.crm_sales_details
where sls_order_dt<=0 



select sls_order_dt
from bronze.crm_sales_details
where sls_order_dt<=0 
or len(sls_order_dt) !=8
or  sls_order_dt>20500101 
or  sls_order_dt<19000101;


select sls_ship_dt
from bronze.crm_sales_details
where sls_ship_dt<=0 
or len(sls_ship_dt) !=8
or  sls_ship_dt>20500101 
or  sls_ship_dt<19000101;



select sls_ship_dt
from bronze.crm_sales_details
where sls_ship_dt<=0 
or len(sls_ship_dt) !=8
or  sls_ship_dt>20500101 
or  sls_ship_dt<19000101;


select sls_due_dt
from bronze.crm_sales_details
where sls_due_dt<=0 
or len(sls_due_dt) !=8
or  sls_due_dt>20500101 
or  sls_due_dt<19000101;





select * 
from bronze.crm_sales_details
where sls_due_dt<sls_ship_dt or 
	  sls_due_dt<sls_order_dt or
	  sls_ship_dt<sls_order_dt;



select *,sls_price,sls_sales,sls_quantity from bronze.crm_sales_details
where sls_quantity*sls_price!=sls_sales 
or sls_price<0 or sls_price is null
or sls_sales<0 or sls_sales is null
or sls_quantity<0 or sls_quantity is null
order by  bronze.crm_sales_details.sls_price, bronze.crm_sales_details.sls_sales, bronze.crm_sales_details.sls_quantity;



--=====================================================
-- check for nulls or duplicates erp.cust_AZ12
--=====================================================

select * 
from bronze.erp_cust_az12;

select * from bronze.crm_cust_info;



select * 
from bronze.erp_cust_az12
where cid not  in ( select cst_key from silver.crm_cust_info)
;


select *
from silver.crm_cust_info
where cust_id not  in ( select right(cid,5) from bronze.erp_cust_az12);



select * 
from bronze.erp_cust_az12
where bdate>getdate()  or  bdate<'1900-01-01';




select case 
when gen='M' or gen='Male'  then 'Male' 
when gen= 'F' or gen='Female'  then 'Female' -- upper(trim((gen)) in ('F','FEMALE') THEN 'Female'
else 'n/a'

end gen, count(*)
from bronze.erp_cust_az12
group by case 
when gen='M' or gen='Male'  then 'Male' 
when gen= 'F' or gen='Female'  then 'Female' -- upper(trim((gen)) in ('F','FEMALE') THEN 'Female'
else 'n/a'

end;


--=====================================================
-- check for nulls or duplicates erp.cust_A101
--=====================================================

select *
from
(select 
case 
when cid like '%-%' then replace (cid,'-','')
else cid end cid1
from bronze.erp_loc_a101) as a
where cid1 not in (select cst_key from bronze.crm_cust_info);




select distinct cntry,case 
when trim(cntry) = 'DE' then 'Germany'
when trim(cntry) in ('US', 'USA') then 'United States'
when trim(cntry)= '' or cntry is null then 'n/a'
else trim(cntry)
end contry
from bronze.erp_loc_a101
order by 1;


--=====================================================
-- check for nulls or duplicates erp.pc_cat_g1v2
--=====================================================



select * 
from bronze.erp_px_cat_g1v2
where id not in (select cat_id from silver.crm_prd_info);



select * from bronze.erp_px_cat_g1v2
where trim(cat)!=cat
	  or trim(subcat)!=subcat
	  or trim(maintaince)!=maintaince ;

  


select distinct subcat
from bronze.erp_px_cat_g1v2;

select distinct maintaince
from bronze.erp_px_cat_g1v2;







