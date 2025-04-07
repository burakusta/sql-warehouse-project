/*
========================================================================
Stored Procedure: Load Bronze Layer (Source => Bronze
========================================================================

Script Purpose:

      this stored procedure loads data info the bronze schema from external CSV files
      it performs the following actions:
      - Truncaes the bronze tables before loading data
      - Uses the 'bulk ınsert' command to load data from CSV files to bronze tables



Parameters:
    None.
  This stored procedure does not accetp any parameters or return any values.

Usage Example:

    EXEC bronze.load_bronze;
========================================================================
*/


create or alter procedure bronze.load_bronze as
begin
declare @start_time datetime, @end_time datetime
	set @start_time= getdate();
	begin try
		print '==============================================';
		print 'loading bronze layer';
		print '==============================================';




		print '==============================================';
		print 'loading crm tables';
		print '==============================================';




		--insert to crm_cust_info
		set @start_time= getdate();
		print '==============================================';
		print 'truncating crm_cust_info';
		print '==============================================';
		truncate table bronze.crm_cust_info;
		print '==============================================';
		print 'inserting crm_cust_info';
		print '==============================================';
		bulk insert  bronze.crm_cust_info
		from 'C:\Users\burak\OneDrive\Masaüstü\sql\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		with (
				firstrow=2,
				fieldterminator = ',',
				tablock
		);
		set @end_time= getdate();
		print 'load duration' + cast(datediff(second, @start_time,@end_time) as nvarchar) +  ' ' + 'seconds';
		print '================'





		--insert to crm_prd_info
		set @start_time= getdate();
		print '==============================================';
		print 'truncating crm_prd_info';
		print '==============================================';
		truncate table bronze.crm_prd_info;
		print '==============================================';
		print 'inserting crm_prd_info';
		print '==============================================';
		bulk insert  bronze.crm_prd_info
		from 'C:\Users\burak\OneDrive\Masaüstü\sql\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		with (
				firstrow=2,
				fieldterminator = ',',
				tablock
		);
		set @end_time= getdate();
		print 'load duration' + cast(datediff(second, @start_time,@end_time) as nvarchar) + ' ' +  'seconds';
		print '================'



		--insert to crm_sales_details
		set @start_time= getdate();
		print '==============================================';
		print 'truncating crm_sales_details';
		print '==============================================';
		truncate table bronze.crm_sales_details;
		print '==============================================';
		print 'inserting crm_sales_details';
		print '==============================================';
		bulk insert  bronze.crm_sales_details
		from 'C:\Users\burak\OneDrive\Masaüstü\sql\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		with (
				firstrow=2,
				fieldterminator = ',',
				tablock
		);
		set @end_time= getdate();
		print 'load duration' + cast(datediff(second, @start_time,@end_time) as nvarchar) + ' ' + 'seconds';
		print '================'




		print '==============================================';
		print 'loading erp tables';
		print '==============================================';

		--insert to erp_cust_az12

		set @start_time= getdate();
		print '==============================================';
		print 'truncating erp_cust_az12';
		print '==============================================';
		truncate table bronze.erp_cust_az12;
		print '==============================================';
		print 'inserting erp_cust_az12';
		print '==============================================';
		bulk insert  bronze.erp_cust_az12
		from 'C:\Users\burak\OneDrive\Masaüstü\sql\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		with (
				firstrow=2,
				fieldterminator = ',',
				tablock
		);
		set @end_time= getdate();
		print 'load duration' + cast(datediff(second, @start_time,@end_time) as nvarchar) + ' ' + 'seconds';
		print '================'



		--insert to erp_loc_a101

		set @start_time= getdate();
		print '==============================================';
		print 'truncating erp_loc_a101';
		print '==============================================';
		truncate table bronze.erp_loc_a101;
		print '==============================================';
		print 'inserting erp_loc_a101';
		print '==============================================';
		bulk insert  bronze.erp_loc_a101
		from 'C:\Users\burak\OneDrive\Masaüstü\sql\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		with (
				firstrow=2,
				fieldterminator = ',',
				tablock
		);
		set @end_time= getdate();
		print 'load duration' + cast(datediff(second, @start_time,@end_time) as nvarchar) + ' ' + 'seconds';
		print '================'





		--insert to erp_px_cat_g1v2
		set @start_time= getdate();
		print '==============================================';
		print 'truncating erp_px_cat_g1v2';
		print '==============================================';
		truncate table bronze.erp_px_cat_g1v2;
		print '==============================================';
		print 'inserting erp_px_cat_g1v2';
		print '==============================================';

		bulk insert  bronze.erp_px_cat_g1v2
		from 'C:\Users\burak\OneDrive\Masaüstü\sql\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		with (
				firstrow=2,
				fieldterminator = ',',
				tablock
		);
		set @end_time= getdate();
		print 'load duration' + cast(datediff(second, @start_time,@end_time) as nvarchar) + ' ' + 'seconds';
		print '================'

	end try

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
	print 'fullaaaaaaaaa duration' + cast(datediff(second, @start_time,@end_time) as nvarchar) +  ' ' + 'seconds';
	print '================'
end;
