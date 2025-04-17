/*
========================================================================
Stored Procedure: Load Silver Layer (Bronze => Silver
========================================================================

Script Purpose:

      this stored procedure loads data info the bronze schema from external CSV files
      it performs the following actions:
      - Truncaes the silver tables before loading data
      - Inserts transformed and cleansed data from Bronze into Silver tables.



Parameters:
    None.
  This stored procedure does not accetp any parameters or return any values.

Usage Example:

    EXEC silver.load_bronze;
========================================================================
*/


exec silver.load_silver;

go 
Create or ALTER procedure silver.load_silver AS 
BEGIN

declare @start_time datetime, @end_time datetime
	
	begin try
	set @start_time= getdate();
		print '==============================================';
		print 'loading silver layer';
		print '==============================================';




		print '==============================================';
		print 'loading crm tables';
		print '==============================================';




	---CUST INFO
	set @start_time= getdate();
	print '==============================================';
	PRINT '>> Truncating Table: silver.crm_cust_info'
	print '==============================================';
	TRUNCATE TABLE silver.crm_cust_info
	print '==============================================';
	PRINT '>> Inserting Data Into: silver.crm_cust_info'
	print '==============================================';
	insert into silver.crm_cust_info(
	cust_id, 
	cst_key,
	cst_firstname,
	cst_lastname,
	cst_marital_status,
	cst_gndr,
	cst_create_date) 
	select cust_id,cst_key,trim(cst_firstname) cst_firstname,trim(cst_lastname) cst_lastname ,
	case
	when upper(TRIM(cst_marital_status))= 'S' then 'Single'
	when upper(trim(cst_marital_status))= 'M' then 'Married' 
	else 'n/a'
	end cst_marital_status,
	case
	when upper(TRIM(cst_gndr))= 'M' then 'Male'
	when upper(TRIM(cst_gndr))= 'F' then 'Female' 
	else 'n/a'
	end cst_gndr
	,cst_create_date
	from
	(SELECT *
	from (select *,row_number() over (partition by cust_id order by cst_create_date desc ) flag_last
	from bronze.crm_cust_info) t
	where flag_last =1 and cust_id is not null) t;
	set @end_time= getdate();
	print 'load duration' +  ' ' + cast(datediff(second, @start_time,@end_time) as nvarchar) +  ' ' + 'seconds';
	print '================'






	---PRODUCT INFO 
	set @start_time= getdate();
	print '==============================================';
	PRINT '>> Truncating Table: silver.crm_prd_info'
	print '==============================================';
	TRUNCATE TABLE silver.crm_prd_info
	print '==============================================';
	PRINT '>> Inserting Data Into: silver.crm_prd_info'
	print '==============================================';
	insert into silver.crm_prd_info(
			prd_id ,
			cat_id ,
			prd_key ,
			prd_nm ,
			prd_cost ,
			prd_line,
			prd_start_dt,
			prd_end_dt)


	select prd_id,
		   replace(SUBSTRING(prd_key,1,5), '-','_') cat_id,
		   SUBSTRING(prd_key,7,len(prd_key)) prd_key,
		   prd_nm,
		   isnull(prd_cost,0) prd_cost,
		   case upper(trim(prd_line))
		   when   'M' then 'Mountain'
		   when   'R' then 'Road'
		   when   'S' then 'Other Sales'
		   when   'T' then 'Touring'
		   else 'n/a'
		   end prd_line,
		   cast(prd_start_dt as date) prd_start_dt,
		   cast(lead(prd_start_dt) over(partition by
							SUBSTRING(prd_key,7,len(prd_key))
							order by prd_start_dt)-1 as  date)  as adjusted_end_dt 
		   from bronze.crm_prd_info;
		   set @end_time= getdate();
			print 'load duration' +  ' ' + cast(datediff(second, @start_time,@end_time) as nvarchar) + ' ' +  'seconds';
			print '================'





	---SALES DETAILS
	set @start_time= getdate();
	print '==============================================';
	PRINT '>> Truncating Table:silver.crm_sales_details'
	print '==============================================';
	TRUNCATE TABLE silver.crm_sales_details
	print '==============================================';
	PRINT '>> Inserting Data Into: silver.crm_sales_details'
	print '==============================================';
	insert into silver.crm_sales_details(
			sls_ord_num,
			sls_prd_key ,
			sls_cust_id ,
			sls_order_dt ,
			sls_ship_dt ,
			sls_due_dt ,
			sls_sales,
			sls_quantity,
			sls_price )
	select 
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	case 
	when sls_order_dt=0 or len(sls_order_dt) !=8 then null
	else cast(cast(sls_order_dt as varchar) as date) 
	end as sls_order_dt,

	case 
	when sls_ship_dt=0 or len(sls_ship_dt) !=8 then null
	else cast(cast(sls_ship_dt as varchar) as date) 
	end as sls_ship_dt,

	case 
	when sls_due_dt=0 or len(sls_due_dt) !=8 then null
	else cast(cast(sls_due_dt as varchar) as date) 
	end as sls_due_dt,


	case 
	when sls_sales is null	
		 or sls_sales<=0 
		 or sls_sales!= sls_quantity * abs(sls_price) 
				then sls_quantity * sls_price
	else sls_sales
	end as sls_sales,

	sls_quantity,
	case
	when sls_price is null or sls_price=0  then sls_sales/nullif(sls_quantity,0)
	when sls_price<0 then -sls_price
	else sls_price
	end as sls_price

	from bronze.crm_sales_details;
	set @end_time= getdate();		
	print 'load duration' +  ' ' + cast(datediff(second, @start_time,@end_time) as nvarchar) + ' ' + 'seconds';
	print '================'


	print '==============================================';
	print 'loading erp tables';
	print '==============================================';




	---AZ12
	set @start_time= getdate();
	print '==============================================';
	PRINT '>> Truncating Table: silver.erp_cust_az12'
	print '==============================================';
	TRUNCATE TABLE silver.erp_cust_az12
	print '==============================================';
	PRINT '>> Inserting Data Into: silver.erp_cust_az12'
	print '==============================================';
	insert into silver.erp_cust_az12(
			cid ,
			bdate,
			gen )
	select 
	case 
	when cid like'NAS%'  then replace(cid,'NAS','')
	ELSE cid 
	end as cid,

	case
	when bdate>getdate()
	then null
	else bdate
	end bdate,

	case 
	when gen='M' or gen='Male'  then 'Male' 
	when gen= 'F' or gen='Female'  then 'Female' -- upper(trim((gen)) in ('F','FEMALE') THEN 'Female'
	else 'n/a'

	end gen

	from bronze.erp_cust_az12;
	set @end_time= getdate();
	print 'load duration' +  ' ' + cast(datediff(second, @start_time,@end_time) as nvarchar) + ' ' + 'seconds';
	print '================'




	---LOC_A101
	set @start_time= getdate();
	print '==============================================';
	PRINT '>> Truncating Table: silver.erp_loc_a101'
	print '==============================================';
	TRUNCATE TABLE silver.erp_loc_a101
	print '==============================================';
	PRINT '>> Inserting Data Into: silver.erp_loc_a101'
	print '==============================================';
	insert into silver.erp_loc_a101(
			cid,
			cntry )
	select  
	case 
	when cid like '%-%' then replace (cid,'-','')
	else cid
	end cid,

	case 
	when trim(cntry) = 'DE' then 'Germany'
	when trim(cntry) in ('US', 'USA') then 'United States'
	when trim(cntry)= '' or cntry is null then 'n/a'
	else trim(cntry)
	end contry
	from bronze.erp_loc_a101;
	set @end_time= getdate();
	print 'load duration' +  ' ' + cast(datediff(second, @start_time,@end_time) as nvarchar) + ' ' + 'seconds';
	print '================';



	---  cat_g1v2
	set @start_time= getdate();
	print '==============================================';
	PRINT '>> Truncating Table: silver.erp_px_cat_g1v2'
	print '==============================================';
	TRUNCATE TABLE silver.erp_px_cat_g1v2
	print '==============================================';
	PRINT '>> Inserting Data Into: silver.erp_px_cat_g1v2'
	print '==============================================';
	insert into silver.erp_px_cat_g1v2(
			id ,
			cat ,
			subcat ,
			maintaince )
	select id ,
	cat ,
	subcat ,
	maintaince
	from bronze.erp_px_cat_g1v2;
	set @end_time= getdate();
		print 'load duration' +  ' ' + cast(datediff(second, @start_time,@end_time) as nvarchar) + ' ' + 'seconds';
		print '================'
END try
begin catch
		print ' =======================================';
		print 'error occured during loading bronze layer'
		print 'error message' + ERROR_MESSAGE();
		print 'error message' + cast(ERROR_number() as nvarchar);
		print 'error message' + cast(ERROR_state() as nvarchar);
		print ' =======================================';
	end catch
	set @end_time= getdate();
	print '================'
	print 'fullaaaaaaaaa duration' +  ' ' + cast(datediff(second, @start_time,@end_time) as nvarchar) +  ' ' + 'seconds';
	print '================'
end;
