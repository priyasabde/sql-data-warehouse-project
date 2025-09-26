/* 
*****************************************************************************
Store Procedure: Load Silver Layer(Bronze -> Sliver)
*****************************************************************************
Script Purpose:
    This stored procedure performs the ETL(Ectract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
Actions Performed:
    - Truncates Silver tables.
    - Inserts transformed and cleansed data from Bronze into silver tables.

Parameters:
None.
This stored procedure does not accept any parameters or return any values.

Usage Example:
EXEC Silver.load_silver;
*******************************************************************************
*/


create or alter procedure silver.load_silver as
begin
	
	declare @start_time datetime, @end_time datetime, @batch_start_time datetime, @batch_end_time datetime;


	begin try
		set @batch_start_time = GETDATE()
		print '============================================='
		print'loading silver layer';
		print '========================================='

		print'--------------------------------------------'
		print 'loading the crm table';
		print'---------------------------------------------'

		set @start_time = getdate();
		print'>> tuuncating table: silver.crm_cust_info';
		truncate table silver.crm_cust_info;
		print'>> inserting data into: silver.crm_cust_info';
		insert into silver.crm_cust_info(
		cst_id ,
		cst_key ,
		cst_firstname,
		cst_lastname ,
		cst_material_status,
		cst_gndr,
		cst_create_date 
		)
		select 
		cst_id,
		cst_key,
		TRIM(cst_firstname) as cst_firstname,
		TRIM(cst_lastname) as cst_lastname,

		case when upper(Trim(cst_material_status)) = 's' then 'single'
			 when upper(trim(cst_material_status)) = 'm' then 'Married'
			 else 'n/a'
		end cst_material_status,

		case when upper(Trim(cst_gndr)) = 'F' then 'Female'
			 when upper(trim(cst_gndr)) = 'M ' then 'Male'
			 else 'n/a'
		end cst_gndr,
		cst_create_date from
		( 
		select *,
		row_number() over(partition by cst_id order by cst_create_date desc) as flag_last
		from bronze.crm_cust_info)t
		WHERE flag_last = 1;
		set @end_time = getdate();
		print '>> load duration :' + cast(datediff(second , @start_time, @end_time) as nvarchar)+ 'seconds';
		print '-------------------'



		
		set @start_time = getdate()
		print'>> tuuncating table: silver.crm_prd_info';
		truncate table silver.crm_prd_info;
		print'>> inserting data into: silver.crm_prd_info';

		INSERT INTO silver.crm_prd_info(
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
			REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
			SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
			prd_nm,
			ISNULL(prd_cost, 0) AS prd_cost,
			CASE 
				WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'mountain'
				WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'road'
				WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'other sales'
				WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'touring'
				ELSE 'n/a'
			END AS prd_line,
			CAST(prd_start_dt AS date) AS prd_start_dt,
			LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS prd_end_dt
		FROM bronze.crm_prd_info;
				set @end_time = getdate();
		print '>> load duration :'+ cast(datediff(second , @start_time, @end_time) as nvarchar) +'seconds';
		print'-------------------'


		set @start_time = getdate()
		print'>> tuuncating table: silver.crm_sales_details';
		truncate table silver.crm_sales_details;
		print'>> inserting data into: silver.crm_sales_details';
		insert into silver.crm_sales_details(
		sls_ord_num ,
		sls_prd_key ,
		sls_cust_id,
		sls_order_dt,
		sls_ship_dt ,
		sls_due_dt ,
		sls_sales,
		sls_quantity,
		sls_price
		)

		select
		sls_ord_num,
		sls_prd_key,
		sls_cust_id ,
		case when sls_order_dt = 0 or len(sls_order_dt) != 8 then null
		else cast(cast(sls_order_dt as varchar)as date)
		end as sls_order_dt,
		case when sls_ship_dt = 0 or len(sls_ship_dt) != 8 then null
		else cast(cast(sls_ship_dt as varchar)as date)
		end as sls_ship_dt ,
		case when sls_due_dt = 0 or len(sls_due_dt) != 8 then null
		else cast(cast(sls_due_dt as varchar)as date)
		end as sls_due_dt ,
		case when sls_sales is null or sls_sales <=0 or sls_sales != sls_quantity * ABS(sls_price)
		then sls_quantity * Abs(sls_price)
		else sls_sales
		end as sls_sales,
		sls_quantity , 
		case when sls_price is null or sls_price <=0
		then sls_sales / nullif(sls_quantity,0)
		else sls_price
		end as sls_price
		from bronze.crm_sales_details
		set @end_time = getdate();
		print'load duration:' + cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';
		print '---------------------'

		print '----------------------------------------------'
		print 'loading the erp table';
		print '----------------------------------------------'


		set @start_time = getdate()
		print'>> tuuncating table: silver.erp_px_cat_g1v2';
		truncate table silver.erp_px_cat_g1v2;
		print'>> inserting data into: silver.erp_px_cat_g1v2';
		insert into silver.erp_px_cat_g1v2(
		id,
		cat ,
		subcat,
		maintenance 
		)
		select 
		id,
		cat ,
		subcat,
		maintenance 
		from bronze.erp_px_cat_g1v2
		set @end_time = getdate();
		print'load duration:' + cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';
		print '---------------------'
		
		
		set @start_time = getdate()
		print'>> tuuncating table: silver.erp_loc_a101';
		truncate table silver.erp_loc_a101;
		print'>> inserting data into: silver.erp_loc_a101';

		insert into silver.erp_loc_a101(
		cid,
		cntry
		)
		select
		replace (cid, '-', '')cid,
		case when trim(cntry) = 'de' then 'germany'
		when trim(cntry) in ('us','usa') then 'united states'
		when trim(cntry) = '' or cntry is null then 'n/a'
		else trim(cntry)
		end cntry
		from bronze.erp_loc_a101;
		set @end_time = getdate();
		print'load duration:' + cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';
		print '---------------------'


		set @start_time = getdate()
		print'>> tuuncating table: silver.erp_cust_az12';
		truncate table silver.erp_cust_az12;
		print'>> inserting data into: silver.erp_cust_az12';

		insert into silver.erp_cust_az12(cid,bdate,gen)
		select
		case when cid like'nas%' then substring(cid, 4,len(cid))
		else cid
		end cid,
		case when bdate > getdate() then null
		else bdate
		end as bdate,
		case when upper(trim(gen)) in ('f', 'FEMALE') then 'Female'
		when upper(trim(gen)) in ('m', 'male') then 'male'
		else 'n/a'
		end as gen
		from bronze.erp_cust_az12
		set @end_time = getdate();
		print'load duration:' + cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';
		print '---------------------';

		set @batch_end_time = GETDATE();
	end try

	begin catch
		print'========================================='
		print'error occured during loading silver layer'
		print'error message' + error_message();
		print'error message' + cast(error_message() as nvarchar);
		print'error message' + cast(error_message() as nvarchar);
		print'=========================================='
	end catch
	end

exec silver.load_silver
