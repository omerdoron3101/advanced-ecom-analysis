/* =============================================================================
   Stored Procedure: Load Bronze Layer (Source -> Bronze)
   ============================================================================= 

   Script Purpose:
       Loads raw CSV datasets into the Bronze layer of the ADVANCED_ECOM_ANALYSIS
       Data Warehouse. Bronze layer stores data exactly as received without 
       transformation and serves as the staging area for Silver processing.

   Key Notes:
       - Truncates each Bronze table before loading new data.
       - Uses BULK INSERT for high-performance loading of CSV files.
       - Measures and prints load duration for monitoring purposes.
       - Ensure file paths match your environment before execution.
       - Follows Medallion Architecture: Bronze = raw, Silver = cleaned, Gold = analytical.

   ⚠️ WARNING:
       Running this procedure will permanently delete existing Bronze data
       in all tables before loading new data. Ensure backups and environment
       safety prior to execution.

   Best Practices:
       - CSV headers must match table columns.
       - Use consistent file encoding (e.g., UTF-8).
       - Avoid data transformation at this layer; cleaning is done in Silver.
       - Maintain batch-level logging for auditing and troubleshooting.
   ============================================================================= */

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '=============================================================================';
		PRINT 'Loading Bronze Layer';
		PRINT '=============================================================================';

		-- 1. olist_orders_dataset
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.olist_orders_dataset';
		TRUNCATE TABLE bronze.olist_orders_dataset;

		PRINT '>> Inserting Data Into: bronze.olist_orders_dataset';
		BULK INSERT bronze.olist_orders_dataset
		FROM 'C:\Users\User\Downloads\archive\olist_orders_dataset.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK 
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '-----------------';
        
		-- 2. olist_products_dataset (CORRECTED TABLE NAMES)
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.olist_products_dataset';
		TRUNCATE TABLE bronze.olist_products_dataset; 

		PRINT '>> Inserting Data Into: bronze.olist_products_dataset';
		BULK INSERT bronze.olist_products_dataset
		FROM 'C:\Users\User\Downloads\archive\olist_products_dataset.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK 
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '-----------------';

		-- 3. olist_sellers_dataset (CORRECTED TABLE NAMES)
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.olist_sellers_dataset';
		TRUNCATE TABLE bronze.olist_sellers_dataset;

		PRINT '>> Inserting Data Into: bronze.olist_sellers_dataset';
		BULK INSERT bronze.olist_sellers_dataset
		FROM 'C:\Users\User\Downloads\archive\olist_sellers_dataset.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK 
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '-----------------';
		
		-- 4. olist_customers_dataset (MISSING TABLE ADDED)
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.olist_customers_dataset';
		TRUNCATE TABLE bronze.olist_customers_dataset;

		PRINT '>> Inserting Data Into: bronze.olist_customers_dataset';
		BULK INSERT bronze.olist_customers_dataset
		FROM 'C:\Users\User\Downloads\archive\olist_customers_dataset.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK 
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '-----------------';

		-- 5. product_category_name_translation (MISSING TABLE ADDED)
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.product_category_name_translation';
		TRUNCATE TABLE bronze.product_category_name_translation;

		PRINT '>> Inserting Data Into: bronze.product_category_name_translation';
		BULK INSERT bronze.product_category_name_translation
		FROM 'C:\Users\User\Downloads\archive\product_category_name_translation.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK 
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '-----------------';

		-- 6. olist_geolocation_dataset
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.olist_geolocation_dataset';
		TRUNCATE TABLE bronze.olist_geolocation_dataset;

		PRINT '>> Inserting Data Into: bronze.olist_geolocation_dataset';
		BULK INSERT bronze.olist_geolocation_dataset
		FROM 'C:\Users\User\Downloads\archive\olist_geolocation_dataset.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK 
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '-----------------';

		-- 7. olist_order_items_dataset
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.olist_order_items_dataset';
		TRUNCATE TABLE bronze.olist_order_items_dataset;

		PRINT '>> Inserting Data Into: bronze.olist_order_items_dataset';
		BULK INSERT bronze.olist_order_items_dataset
		FROM 'C:\Users\User\Downloads\archive\olist_order_items_dataset.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK 
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '-----------------';

		-- 8. olist_order_payments_dataset
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.olist_order_payments_dataset';
		TRUNCATE TABLE bronze.olist_order_payments_dataset;

		PRINT '>> Inserting Data Into: bronze.olist_order_payments_dataset';
		BULK INSERT bronze.olist_order_payments_dataset
		FROM 'C:\Users\User\Downloads\archive\olist_order_payments_dataset.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK 
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '-----------------';
		
		-- 9. olist_order_reviews_dataset
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.olist_order_reviews_dataset';
		TRUNCATE TABLE bronze.olist_order_reviews_dataset;

		PRINT '>> Inserting Data Into: bronze.olist_order_reviews_dataset';
		BULK INSERT bronze.olist_order_reviews_dataset
		FROM 'C:\Users\User\Downloads\archive\olist_order_reviews_dataset.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK 
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '-----------------';

        SET @batch_end_time = GETDATE();

		/* ----------------------------------------------------------------------
		    SECTION 3: Completion Summary
		    ---------------------------------------------------------------------- */

		PRINT '=============================================================================';
		PRINT 'Loading Bronze Layer Is Completed';
		PRINT '    - Total Load Duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '=============================================================================';

	END TRY
	BEGIN CATCH
		-- Rollback any open transactions (good practice in catch block)
		IF @@TRANCOUNT > 0 
			ROLLBACK TRANSACTION;

		PRINT '=============================================================================';
		PRINT 'ERROR OCCURRED DURING BRONZE LAYER LOAD';
		PRINT 'Message: ' + ERROR_MESSAGE();
		PRINT 'Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Severity: ' + CAST(ERROR_SEVERITY() AS NVARCHAR);
		PRINT 'State: ' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT 'Line: ' + CAST(ERROR_LINE() AS NVARCHAR);
		PRINT '=============================================================================';
	END CATCH
END
GO
