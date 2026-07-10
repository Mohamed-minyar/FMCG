-- DEPI Data Engineering Capstone Project
-- Medallion Architecture: Bronze > Silver > Gold
-- Parent Company + SportsBar Acquired Integration

-- Section 1: Schemas
CREATE SCHEMA bronze_parent;
CREATE SCHEMA bronze_acquired;
CREATE SCHEMA bronze_shared;
CREATE SCHEMA silver_parent;
CREATE SCHEMA silver_acquired;
CREATE SCHEMA gold;

SELECT name FROM sys.schemas
WHERE name IN ('bronze_parent','bronze_acquired','bronze_shared','silver_parent','silver_acquired','gold')
ORDER BY name;


-- Section 2: Bronze Layer
CREATE TABLE bronze_parent.customers (
    customer_id   INT,
    name          VARCHAR(100),
    gender        VARCHAR(20),
    age           INT,
    country       VARCHAR(50),
    city          VARCHAR(50),
    signup_date   DATE,
    loyalty_tier  VARCHAR(20)
);

CREATE TABLE bronze_parent.products (
    product_id    INT,
    product_name  VARCHAR(100),
    brand         VARCHAR(50),
    category      VARCHAR(50),
    subcategory   VARCHAR(50)
);

CREATE TABLE bronze_parent.fact_sales (
    transaction_id    BIGINT,
    transaction_date  DATE,
    customer_id       INT,
    product_id        INT,
    channel_id        INT,
    quantity          INT,
    unit_price        DECIMAL(10,2),
    discount          DECIMAL(5,2),
    cost              DECIMAL(10,2),
    currency          VARCHAR(10),
    revenue           DECIMAL(10,4),
    profit            DECIMAL(10,4)
);

CREATE TABLE bronze_acquired.customers (
    customer_id   INT,
    name          VARCHAR(100),
    gender        VARCHAR(20),
    age           INT,
    country       VARCHAR(50),
    city          VARCHAR(50),
    signup_date   DATE,
    loyalty_tier  VARCHAR(20)
);

CREATE TABLE bronze_acquired.products (
    product_id    INT,
    product_name  VARCHAR(100),
    brand         VARCHAR(50),
    category      VARCHAR(50),
    subcategory   VARCHAR(50)
);

CREATE TABLE bronze_acquired.fact_sales (
    transaction_id    BIGINT,
    transaction_date  DATE,
    customer_id       INT,
    product_id        INT,
    channel_id        INT,
    quantity          INT,
    unit_price        DECIMAL(10,2),
    discount          DECIMAL(5,2),
    cost              DECIMAL(10,2),
    currency          VARCHAR(10),
    revenue           DECIMAL(10,4),
    profit            DECIMAL(10,4)
);

CREATE TABLE bronze_shared.channels (
    channel_id    INT,
    channel_type  VARCHAR(50),
    region        VARCHAR(50),
    store_size    VARCHAR(20)
);

CREATE TABLE bronze_shared.currency_rates (
    rate_date             DATE,
    currency              VARCHAR(10),
    exchange_rate_to_usd  DECIMAL(10,4)
);

SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA IN ('bronze_parent','bronze_acquired','bronze_shared')
  AND TABLE_TYPE = 'BASE TABLE'
ORDER BY TABLE_SCHEMA, TABLE_NAME;


-- Section 3: Silver Layer
CREATE TABLE silver_parent.dim_customers (
    customer_key   INT IDENTITY(1,1) PRIMARY KEY,
    customer_id    INT,
    name           VARCHAR(250),
    gender         VARCHAR(10),
    age            INT,
    country        VARCHAR(100),
    city           VARCHAR(100),
    loyalty_tier   VARCHAR(50),
    valid_from     DATE,
    valid_to       DATE,
    is_current     BIT
);

CREATE TABLE silver_parent.dim_products (
    product_key   INT IDENTITY(1,1) PRIMARY KEY,
    product_id    INT,
    product_name  VARCHAR(250),
    brand         VARCHAR(100),
    category      VARCHAR(100),
    subcategory   VARCHAR(100)
);

CREATE TABLE silver_parent.fact_sales (
    sales_key            BIGINT IDENTITY(1,1) PRIMARY KEY,
    transaction_id       BIGINT,
    transaction_date     DATE,
    customer_key         INT,
    product_key          INT,
    channel_id           INT,
    quantity             INT,
    unit_price_usd       DECIMAL(10,2),
    discount_amount_usd  DECIMAL(10,2),
    cost_usd             DECIMAL(10,2),
    revenue_usd          DECIMAL(10,2),
    profit_usd           DECIMAL(10,2)
);

CREATE TABLE silver_acquired.dim_customers (
    customer_key   INT IDENTITY(1,1) PRIMARY KEY,
    customer_id    INT,
    name           VARCHAR(250),
    gender         VARCHAR(10),
    age            INT,
    country        VARCHAR(100),
    city           VARCHAR(100),
    loyalty_tier   VARCHAR(50),
    valid_from     DATE,
    valid_to       DATE,
    is_current     BIT
);

CREATE TABLE silver_acquired.dim_products (
    product_key   INT IDENTITY(1,1) PRIMARY KEY,
    product_id    INT,
    product_name  VARCHAR(250),
    brand         VARCHAR(100),
    category      VARCHAR(100),
    subcategory   VARCHAR(100)
);

CREATE TABLE silver_acquired.fact_sales (
    sales_key            BIGINT IDENTITY(1,1) PRIMARY KEY,
    transaction_id       BIGINT,
    transaction_date     DATE,
    customer_key         INT,
    product_key          INT,
    channel_id           INT,
    quantity             INT,
    unit_price_usd       DECIMAL(10,2),
    discount_amount_usd  DECIMAL(10,2),
    cost_usd             DECIMAL(10,2),
    revenue_usd          DECIMAL(10,2),
    profit_usd           DECIMAL(10,2)
);

SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA IN ('silver_parent','silver_acquired')
  AND TABLE_TYPE = 'BASE TABLE'
ORDER BY TABLE_SCHEMA, TABLE_NAME;


-- Section 4: ETL Procedures
CREATE PROCEDURE silver_parent.sp_load_dim_customers
    @p_debug BIT = 0
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @v_rows_inserted INT = 0;
    DECLARE @v_start_time DATETIME = GETDATE();
    
    BEGIN TRY
        TRUNCATE TABLE silver_parent.dim_customers;
        
        INSERT INTO silver_parent.dim_customers (
            customer_id, name, gender, age, country, city, loyalty_tier,
            valid_from, valid_to, is_current
        )
        SELECT
            customer_id,
            TRIM(name) AS name,
            CASE
                WHEN gender IS NULL THEN
                    CASE
                        WHEN LEFT(TRIM(name), CHARINDEX(' ', TRIM(name) + ' ') - 1)
                             IN ('James','Robert','Michael','William','David','Richard','Joseph','Thomas',
                                 'Charles','Christopher','Daniel','Matthew','Anthony','Mark','Donald',
                                 'Steven','Paul','Andrew','Joshua','Kenneth','Kevin','Brian','George',
                                 'Edward','Ronald','Timothy','Jason','Jeffrey','Ryan','Jacob','Gary',
                                 'Nicholas','Eric','Jonathan','Stephen','Larry','Justin','Scott','Brandon',
                                 'Benjamin','Samuel','Gregory','Alexander','Frank','Patrick','Raymond',
                                 'Jack','Dennis','Jerry','Tyler','Ahmed','Mohamed','Ali','Omar','Youssef') THEN 'M'
                        WHEN LEFT(TRIM(name), CHARINDEX(' ', TRIM(name) + ' ') - 1)
                             IN ('Mary','Patricia','Jennifer','Linda','Elizabeth','Barbara','Susan','Jessica',
                                 'Sarah','Karen','Nancy','Lisa','Betty','Margaret','Sandra','Ashley',
                                 'Kimberly','Emily','Donna','Michelle','Dorothy','Carol','Amanda','Melissa',
                                 'Deborah','Stephanie','Rebecca','Sharon','Laura','Cynthia','Kathleen','Amy',
                                 'Shirley','Angela','Helen','Anna','Brenda','Pamela','Nicole','Samantha',
                                 'Katherine','Emma','Elena','Mia','Sophie','Layla','Isabella','Fatima',
                                 'Noor','Amira','Mona','Chloe','Grace') THEN 'F'
                        ELSE 'U'
                    END
                ELSE gender
            END AS gender,
            ISNULL(age, 0) AS age,
            CASE
                WHEN UPPER(TRIM(city)) = 'NEW YORK' THEN 'USA'
                WHEN UPPER(TRIM(city)) = 'BERLIN'   THEN 'Germany'
                WHEN UPPER(TRIM(city)) = 'DUBAI'    THEN 'UAE'
                WHEN UPPER(TRIM(city)) = 'CAIRO'    THEN 'Egypt'
                WHEN UPPER(TRIM(city)) = 'PARIS'    THEN 'France'
                ELSE country
            END AS country,
            TRIM(city) AS city,
            ISNULL(TRIM(loyalty_tier), 'Bronze') AS loyalty_tier,
            signup_date AS valid_from,
            CAST('9999-12-31' AS DATE) AS valid_to,
            1 AS is_current
        FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY signup_date DESC) AS row_num
            FROM bronze_parent.customers
        ) AS raw_data
        WHERE row_num = 1;
        
        SET @v_rows_inserted = @@ROWCOUNT;
        IF @p_debug = 1
            PRINT CONCAT('Parent dim_customers: ', @v_rows_inserted, ' rows, ', DATEDIFF(MILLISECOND, @v_start_time, GETDATE()), 'ms');
    END TRY
    BEGIN CATCH
        PRINT CONCAT('ERROR: ', ERROR_MESSAGE());
        THROW;
    END CATCH
END;
GO

CREATE PROCEDURE silver_parent.sp_load_dim_products
    @p_debug BIT = 0
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @v_rows_inserted INT = 0;
    DECLARE @v_start_time DATETIME = GETDATE();
    
    BEGIN TRY
        TRUNCATE TABLE silver_parent.dim_products;
        
        INSERT INTO silver_parent.dim_products (
            product_id, product_name, brand, category, subcategory
        )
        SELECT
            product_id,
            CASE
                WHEN CHARINDEX(' - ', product_name) > 0
                THEN TRIM(LEFT(product_name, CHARINDEX(' - ', product_name) - 1))
                ELSE TRIM(product_name)
            END AS product_name,
            CASE
                WHEN product_name LIKE '%Soap%'     OR product_name LIKE '%Shampoo%'
                  OR product_name LIKE '%Lotion%'   OR product_name LIKE '%Toothpaste%'  THEN 'P&G'
                WHEN product_name LIKE '%Coffee%'   OR product_name LIKE '%Tea%'
                  OR product_name LIKE '%Water%'                                          THEN 'Nestle'
                WHEN product_name LIKE '%Juice%'    OR product_name LIKE '%Drink%'       THEN 'PepsiCo'
                WHEN product_name LIKE '%Capsules%' OR product_name LIKE '%Oil%'
                  OR product_name LIKE '%Complex%'                                        THEN 'Nature''s Best'
                WHEN product_name LIKE '%Powder%'   OR product_name LIKE '%Punch%'       THEN 'Optimum Nutrition'
                WHEN product_name LIKE '%Mix%'      OR product_name LIKE '%Almonds%'
                  OR product_name LIKE '%Chips%'    OR product_name LIKE '%Bar%'         THEN 'SportsBar Pro'
                ELSE brand
            END AS brand,
            CASE
                WHEN product_name LIKE '%Soap%'     OR product_name LIKE '%Shampoo%'
                  OR product_name LIKE '%Lotion%'   OR product_name LIKE '%Toothpaste%'  THEN 'Personal Care'
                WHEN product_name LIKE '%Coffee%'   OR product_name LIKE '%Tea%'
                  OR product_name LIKE '%Water%'    OR product_name LIKE '%Juice%'
                  OR product_name LIKE '%Drink%'                                          THEN 'Beverage'
                WHEN product_name LIKE '%Capsules%' OR product_name LIKE '%Oil%'
                  OR product_name LIKE '%Complex%'  OR product_name LIKE '%Powder%'
                  OR product_name LIKE '%Punch%'                                          THEN 'Health'
                WHEN product_name LIKE '%Mix%'      OR product_name LIKE '%Almonds%'
                  OR product_name LIKE '%Chips%'    OR product_name LIKE '%Bar%'         THEN 'Food'
                ELSE category
            END AS category,
            CASE
                WHEN product_name LIKE '%Soap%'     OR product_name LIKE '%Shampoo%'
                  OR product_name LIKE '%Lotion%'   OR product_name LIKE '%Toothpaste%'  THEN 'Hygiene'
                WHEN product_name LIKE '%Coffee%'   OR product_name LIKE '%Tea%'
                  OR product_name LIKE '%Water%'    OR product_name LIKE '%Juice%'       THEN 'Drinks'
                WHEN product_name LIKE '%Drink%'    OR product_name LIKE '%Powder%'
                  OR product_name LIKE '%Punch%'    OR product_name LIKE '%Capsules%'
                  OR product_name LIKE '%Oil%'      OR product_name LIKE '%Complex%'     THEN 'Supplements'
                WHEN product_name LIKE '%Mix%'      OR product_name LIKE '%Almonds%'
                  OR product_name LIKE '%Chips%'    OR product_name LIKE '%Bar%'         THEN 'Snacks'
                ELSE subcategory
            END AS subcategory
        FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY product_name) AS row_num
            FROM bronze_parent.products
        ) AS raw_products
        WHERE row_num = 1;
        
        SET @v_rows_inserted = @@ROWCOUNT;
        IF @p_debug = 1
            PRINT CONCAT('Parent dim_products: ', @v_rows_inserted, ' rows, ', DATEDIFF(MILLISECOND, @v_start_time, GETDATE()), 'ms');
    END TRY
    BEGIN CATCH
        PRINT CONCAT('ERROR: ', ERROR_MESSAGE());
        THROW;
    END CATCH
END;
GO

CREATE PROCEDURE silver_parent.sp_load_fact_sales
    @p_debug BIT = 0
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @v_rows_inserted INT = 0;
    DECLARE @v_start_time DATETIME = GETDATE();
    
    BEGIN TRY
        TRUNCATE TABLE silver_parent.fact_sales;
        
        INSERT INTO silver_parent.fact_sales (
            transaction_id, transaction_date, customer_key, product_key, channel_id,
            quantity, unit_price_usd, discount_amount_usd, cost_usd, revenue_usd, profit_usd
        )
        SELECT
            f.transaction_id,
            f.transaction_date,
            ISNULL(c.customer_key, -1) AS customer_key,
            ISNULL(p.product_key,  -1) AS product_key,
            f.channel_id,
            f.quantity,
            ROUND(f.unit_price / ISNULL(cr.exchange_rate_to_usd, 1), 2) AS unit_price_usd,
            ROUND((f.unit_price * f.quantity * (f.discount / 100.0)) / ISNULL(cr.exchange_rate_to_usd, 1), 2) AS discount_amount_usd,
            CASE
                WHEN (f.cost / ISNULL(cr.exchange_rate_to_usd, 1)) >
                     (f.unit_price * (1 - f.discount / 100.0) / ISNULL(cr.exchange_rate_to_usd, 1))
                THEN ROUND((f.unit_price * (1 - f.discount / 100.0) * 0.7) / ISNULL(cr.exchange_rate_to_usd, 1), 2)
                ELSE ROUND(f.cost / ISNULL(cr.exchange_rate_to_usd, 1), 2)
            END AS cost_usd,
            ROUND((f.unit_price * f.quantity * (1 - f.discount / 100.0)) / ISNULL(cr.exchange_rate_to_usd, 1), 2) AS revenue_usd,
            ROUND(((f.unit_price * f.quantity * (1 - f.discount / 100.0)) - (f.cost * f.quantity)) / ISNULL(cr.exchange_rate_to_usd, 1), 2) AS profit_usd
        FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY transaction_id ORDER BY transaction_date) AS rn
            FROM bronze_parent.fact_sales
        ) f
        LEFT JOIN silver_parent.dim_customers  c  ON f.customer_id = c.customer_id AND c.is_current = 1
        LEFT JOIN silver_parent.dim_products   p  ON f.product_id  = p.product_id
        LEFT JOIN bronze_shared.currency_rates cr ON f.currency = cr.currency AND f.transaction_date = cr.rate_date
        WHERE f.rn = 1;
        
        SET @v_rows_inserted = @@ROWCOUNT;
        IF @p_debug = 1
            PRINT CONCAT('Parent fact_sales: ', @v_rows_inserted, ' rows, ', DATEDIFF(MILLISECOND, @v_start_time, GETDATE()), 'ms');
    END TRY
    BEGIN CATCH
        PRINT CONCAT('ERROR: ', ERROR_MESSAGE());
        THROW;
    END CATCH
END;
GO

CREATE PROCEDURE silver_acquired.sp_load_dim_customers
    @p_debug BIT = 0
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @v_rows_inserted INT = 0;
    DECLARE @v_start_time DATETIME = GETDATE();
    
    BEGIN TRY
        TRUNCATE TABLE silver_acquired.dim_customers;
        
        INSERT INTO silver_acquired.dim_customers (
            customer_id, name, gender, age, country, city, loyalty_tier,
            valid_from, valid_to, is_current
        )
        SELECT
            customer_id,
            TRIM(name) AS name,
            CASE
                WHEN gender IS NULL THEN
                    CASE
                        WHEN LEFT(TRIM(name), CHARINDEX(' ', TRIM(name) + ' ') - 1)
                             IN ('James','Robert','Michael','William','David','Richard','Joseph','Thomas',
                                 'Charles','Christopher','Daniel','Matthew','Anthony','Mark','Donald',
                                 'Steven','Paul','Andrew','Joshua','Kenneth','Kevin','Brian','George',
                                 'Edward','Ronald','Timothy','Jason','Jeffrey','Ryan','Jacob','Gary',
                                 'Nicholas','Eric','Jonathan','Stephen','Larry','Justin','Scott','Brandon',
                                 'Benjamin','Samuel','Gregory','Alexander','Frank','Patrick','Raymond',
                                 'Jack','Dennis','Jerry','Tyler','Ahmed','Mohamed','Ali','Omar','Youssef') THEN 'M'
                        WHEN LEFT(TRIM(name), CHARINDEX(' ', TRIM(name) + ' ') - 1)
                             IN ('Mary','Patricia','Jennifer','Linda','Elizabeth','Barbara','Susan','Jessica',
                                 'Sarah','Karen','Nancy','Lisa','Betty','Margaret','Sandra','Ashley',
                                 'Kimberly','Emily','Donna','Michelle','Dorothy','Carol','Amanda','Melissa',
                                 'Deborah','Stephanie','Rebecca','Sharon','Laura','Cynthia','Kathleen','Amy',
                                 'Shirley','Angela','Helen','Anna','Brenda','Pamela','Nicole','Samantha',
                                 'Katherine','Emma','Elena','Mia','Sophie','Layla','Isabella','Fatima',
                                 'Noor','Amira','Mona','Chloe','Grace') THEN 'F'
                        ELSE 'U'
                    END
                ELSE gender
            END AS gender,
            ISNULL(age, 0) AS age,
            CASE
                WHEN UPPER(TRIM(city)) = 'NEW YORK' THEN 'USA'
                WHEN UPPER(TRIM(city)) = 'BERLIN'   THEN 'Germany'
                WHEN UPPER(TRIM(city)) = 'DUBAI'    THEN 'UAE'
                WHEN UPPER(TRIM(city)) = 'CAIRO'    THEN 'Egypt'
                WHEN UPPER(TRIM(city)) = 'PARIS'    THEN 'France'
                ELSE country
            END AS country,
            TRIM(city) AS city,
            ISNULL(TRIM(loyalty_tier), 'Bronze') AS loyalty_tier,
            signup_date AS valid_from,
            CAST('9999-12-31' AS DATE) AS valid_to,
            1 AS is_current
        FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY signup_date DESC) AS row_num
            FROM bronze_acquired.customers
        ) AS raw_data
        WHERE row_num = 1;
        
        SET @v_rows_inserted = @@ROWCOUNT;
        IF @p_debug = 1
            PRINT CONCAT('Acquired dim_customers: ', @v_rows_inserted, ' rows, ', DATEDIFF(MILLISECOND, @v_start_time, GETDATE()), 'ms');
    END TRY
    BEGIN CATCH
        PRINT CONCAT('ERROR: ', ERROR_MESSAGE());
        THROW;
    END CATCH
END;
GO

CREATE PROCEDURE silver_acquired.sp_load_dim_products
    @p_debug BIT = 0
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @v_rows_inserted INT = 0;
    DECLARE @v_start_time DATETIME = GETDATE();
    
    BEGIN TRY
        TRUNCATE TABLE silver_acquired.dim_products;
        
        INSERT INTO silver_acquired.dim_products (
            product_id, product_name, brand, category, subcategory
        )
        SELECT
            product_id,
            CASE
                WHEN CHARINDEX(' - ', product_name) > 0
                THEN TRIM(LEFT(product_name, CHARINDEX(' - ', product_name) - 1))
                ELSE TRIM(product_name)
            END AS product_name,
            CASE
                WHEN product_name LIKE '%Soap%'     OR product_name LIKE '%Shampoo%'
                  OR product_name LIKE '%Lotion%'   OR product_name LIKE '%Toothpaste%'  THEN 'P&G'
                WHEN product_name LIKE '%Coffee%'   OR product_name LIKE '%Tea%'
                  OR product_name LIKE '%Water%'                                          THEN 'Nestle'
                WHEN product_name LIKE '%Juice%'    OR product_name LIKE '%Drink%'       THEN 'PepsiCo'
                WHEN product_name LIKE '%Capsules%' OR product_name LIKE '%Oil%'
                  OR product_name LIKE '%Complex%'                                        THEN 'Nature''s Best'
                WHEN product_name LIKE '%Powder%'   OR product_name LIKE '%Punch%'       THEN 'Optimum Nutrition'
                WHEN product_name LIKE '%Mix%'      OR product_name LIKE '%Almonds%'
                  OR product_name LIKE '%Chips%'    OR product_name LIKE '%Bar%'         THEN 'SportsBar Pro'
                ELSE brand
            END AS brand,
            CASE
                WHEN product_name LIKE '%Soap%'     OR product_name LIKE '%Shampoo%'
                  OR product_name LIKE '%Lotion%'   OR product_name LIKE '%Toothpaste%'  THEN 'Personal Care'
                WHEN product_name LIKE '%Coffee%'   OR product_name LIKE '%Tea%'
                  OR product_name LIKE '%Water%'    OR product_name LIKE '%Juice%'
                  OR product_name LIKE '%Drink%'                                          THEN 'Beverage'
                WHEN product_name LIKE '%Capsules%' OR product_name LIKE '%Oil%'
                  OR product_name LIKE '%Complex%'  OR product_name LIKE '%Powder%'
                  OR product_name LIKE '%Punch%'                                          THEN 'Health'
                WHEN product_name LIKE '%Mix%'      OR product_name LIKE '%Almonds%'
                  OR product_name LIKE '%Chips%'    OR product_name LIKE '%Bar%'         THEN 'Food'
                ELSE category
            END AS category,
            CASE
                WHEN product_name LIKE '%Soap%'     OR product_name LIKE '%Shampoo%'
                  OR product_name LIKE '%Lotion%'   OR product_name LIKE '%Toothpaste%'  THEN 'Hygiene'
                WHEN product_name LIKE '%Coffee%'   OR product_name LIKE '%Tea%'
                  OR product_name LIKE '%Water%'    OR product_name LIKE '%Juice%'       THEN 'Drinks'
                WHEN product_name LIKE '%Drink%'    OR product_name LIKE '%Powder%'
                  OR product_name LIKE '%Punch%'    OR product_name LIKE '%Capsules%'
                  OR product_name LIKE '%Oil%'      OR product_name LIKE '%Complex%'     THEN 'Supplements'
                WHEN product_name LIKE '%Mix%'      OR product_name LIKE '%Almonds%'
                  OR product_name LIKE '%Chips%'    OR product_name LIKE '%Bar%'         THEN 'Snacks'
                ELSE subcategory
            END AS subcategory
        FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY product_name) AS row_num
            FROM bronze_acquired.products
        ) AS raw_products
        WHERE row_num = 1;
        
        SET @v_rows_inserted = @@ROWCOUNT;
        IF @p_debug = 1
            PRINT CONCAT('Acquired dim_products: ', @v_rows_inserted, ' rows, ', DATEDIFF(MILLISECOND, @v_start_time, GETDATE()), 'ms');
    END TRY
    BEGIN CATCH
        PRINT CONCAT('ERROR: ', ERROR_MESSAGE());
        THROW;
    END CATCH
END;
GO

CREATE PROCEDURE silver_acquired.sp_load_fact_sales
    @p_debug BIT = 0
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @v_rows_inserted INT = 0;
    DECLARE @v_start_time DATETIME = GETDATE();
    
    BEGIN TRY
        TRUNCATE TABLE silver_acquired.fact_sales;
        
        INSERT INTO silver_acquired.fact_sales (
            transaction_id, transaction_date, customer_key, product_key, channel_id,
            quantity, unit_price_usd, discount_amount_usd, cost_usd, revenue_usd, profit_usd
        )
        SELECT
            f.transaction_id,
            f.transaction_date,
            ISNULL(c.customer_key, -1) AS customer_key,
            ISNULL(p.product_key,  -1) AS product_key,
            f.channel_id,
            f.quantity,
            ROUND(f.unit_price / ISNULL(cr.exchange_rate_to_usd, 1), 2) AS unit_price_usd,
            ROUND((f.unit_price * f.quantity * (f.discount / 100.0)) / ISNULL(cr.exchange_rate_to_usd, 1), 2) AS discount_amount_usd,
            CASE
                WHEN (f.cost / ISNULL(cr.exchange_rate_to_usd, 1)) >
                     (f.unit_price * (1 - f.discount / 100.0) / ISNULL(cr.exchange_rate_to_usd, 1))
                THEN ROUND((f.unit_price * (1 - f.discount / 100.0) * 0.7) / ISNULL(cr.exchange_rate_to_usd, 1), 2)
                ELSE ROUND(f.cost / ISNULL(cr.exchange_rate_to_usd, 1), 2)
            END AS cost_usd,
            ROUND((f.unit_price * f.quantity * (1 - f.discount / 100.0)) / ISNULL(cr.exchange_rate_to_usd, 1), 2) AS revenue_usd,
            ROUND(((f.unit_price * f.quantity * (1 - f.discount / 100.0)) - (f.cost * f.quantity)) / ISNULL(cr.exchange_rate_to_usd, 1), 2) AS profit_usd
        FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY transaction_id ORDER BY transaction_date) AS rn
            FROM bronze_acquired.fact_sales
        ) f
        LEFT JOIN silver_acquired.dim_customers  c  ON f.customer_id = c.customer_id AND c.is_current = 1
        LEFT JOIN silver_acquired.dim_products   p  ON f.product_id  = p.product_id
        LEFT JOIN bronze_shared.currency_rates   cr ON f.currency = cr.currency AND f.transaction_date = cr.rate_date
        WHERE f.rn = 1;
        
        SET @v_rows_inserted = @@ROWCOUNT;
        IF @p_debug = 1
            PRINT CONCAT('Acquired fact_sales: ', @v_rows_inserted, ' rows, ', DATEDIFF(MILLISECOND, @v_start_time, GETDATE()), 'ms');
    END TRY
    BEGIN CATCH
        PRINT CONCAT('ERROR: ', ERROR_MESSAGE());
        THROW;
    END CATCH
END;
GO

CREATE PROCEDURE silver_parent.sp_refresh_all_silver_layers
    @p_debug BIT = 0,
    @p_log_execution BIT = 1
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @v_overall_start DATETIME = GETDATE();
    
    BEGIN TRY
        IF @p_log_execution = 1
            PRINT 'Starting silver layer refresh...';
        
        EXEC silver_parent.sp_load_dim_customers @p_debug = @p_debug;
        EXEC silver_parent.sp_load_dim_products @p_debug = @p_debug;
        EXEC silver_parent.sp_load_fact_sales @p_debug = @p_debug;
        
        EXEC silver_acquired.sp_load_dim_customers @p_debug = @p_debug;
        EXEC silver_acquired.sp_load_dim_products @p_debug = @p_debug;
        EXEC silver_acquired.sp_load_fact_sales @p_debug = @p_debug;
        
        IF @p_log_execution = 1
            PRINT CONCAT('Refresh completed in ', DATEDIFF(SECOND, @v_overall_start, GETDATE()), ' seconds');
    END TRY
    BEGIN CATCH
        PRINT CONCAT('FATAL ERROR: ', ERROR_MESSAGE());
        THROW;
    END CATCH
END;
GO


-- Section 5: QC Testing
CREATE PROCEDURE dbo.sp_qc_bronze_layer_validation
    @p_company_source VARCHAR(20) = 'Parent'
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @v_schema_prefix VARCHAR(20);
    
    IF @p_company_source = 'Parent'
        SET @v_schema_prefix = 'bronze_parent';
    ELSE IF @p_company_source = 'Acquired'
        SET @v_schema_prefix = 'bronze_acquired';
    ELSE
    BEGIN
        RAISERROR('Invalid company source. Use Parent or Acquired', 16, 1);
        RETURN;
    END
    
    PRINT CONCAT('BRONZE QC - ', @p_company_source);
    
    PRINT 'Test 1: NULL values in customers';
    EXEC(CONCAT(N'
    SELECT ''customer_id''  AS Col, COUNT(*) AS Null_Count FROM ', @v_schema_prefix, N'.customers WHERE customer_id IS NULL UNION ALL
    SELECT ''name'',                         COUNT(*)           FROM ', @v_schema_prefix, N'.customers WHERE name IS NULL        UNION ALL
    SELECT ''gender'',                       COUNT(*)           FROM ', @v_schema_prefix, N'.customers WHERE gender IS NULL      UNION ALL
    SELECT ''age'',                          COUNT(*)           FROM ', @v_schema_prefix, N'.customers WHERE age IS NULL         UNION ALL
    SELECT ''country'',                      COUNT(*)           FROM ', @v_schema_prefix, N'.customers WHERE country IS NULL     UNION ALL
    SELECT ''signup_date'',                  COUNT(*)           FROM ', @v_schema_prefix, N'.customers WHERE signup_date IS NULL;
    '));
    
    PRINT 'Test 2: Duplicate customer IDs';
    EXEC(CONCAT(N'
    SELECT customer_id, COUNT(*) AS Dup_Count
    FROM ', @v_schema_prefix, N'.customers
    GROUP BY customer_id
    HAVING COUNT(*) > 1;
    '));
    
    PRINT 'Test 3: Invalid gender values';
    EXEC(CONCAT(N'
    SELECT DISTINCT gender, COUNT(*) AS Count_Gender
    FROM ', @v_schema_prefix, N'.customers
    WHERE gender NOT IN (''Male'',''Female'',''M'',''F'') OR gender IS NULL
    GROUP BY gender;
    '));
    
    PRINT 'Test 4: Age range check';
    EXEC(CONCAT(N'
    SELECT 
        MIN(age) AS Min_Age, 
        MAX(age) AS Max_Age,
        COUNT(CASE WHEN age < 0 OR age > 150 THEN 1 END) AS Invalid_Age_Count
    FROM ', @v_schema_prefix, N'.customers;
    '));
    
    PRINT 'Test 5: Loyalty tier distribution';
    EXEC(CONCAT(N'
    SELECT DISTINCT loyalty_tier, COUNT(*) AS Count_Tier
    FROM ', @v_schema_prefix, N'.customers
    GROUP BY loyalty_tier
    ORDER BY Count_Tier DESC;
    '));
    
    PRINT 'Test 6: NULL values in products';
    EXEC(CONCAT(N'
    SELECT ''product_id''   AS Col, COUNT(*) AS Null_Count FROM ', @v_schema_prefix, N'.products WHERE product_id IS NULL   UNION ALL
    SELECT ''product_name'',                COUNT(*)           FROM ', @v_schema_prefix, N'.products WHERE product_name IS NULL  UNION ALL
    SELECT ''brand'',                       COUNT(*)           FROM ', @v_schema_prefix, N'.products WHERE brand IS NULL;
    '));
    
    PRINT 'Test 7: Duplicate product IDs';
    EXEC(CONCAT(N'
    SELECT product_id, COUNT(*) AS Dup_Count
    FROM ', @v_schema_prefix, N'.products
    GROUP BY product_id
    HAVING COUNT(*) > 1;
    '));
    
    PRINT 'Test 8: NULL and invalid sales values';
    EXEC(CONCAT(N'
    SELECT ''NULL IDs''              AS Issue, COUNT(*) AS Count FROM ', @v_schema_prefix, N'.fact_sales WHERE transaction_id IS NULL OR customer_id IS NULL UNION ALL
    SELECT ''Negative Qty/Price'',            COUNT(*)           FROM ', @v_schema_prefix, N'.fact_sales WHERE quantity <= 0 OR unit_price <= 0 UNION ALL
    SELECT ''Negative Profit'',               COUNT(*)           FROM ', @v_schema_prefix, N'.fact_sales WHERE profit < 0;
    '));
    
    PRINT 'Test 9: Duplicate transactions';
    EXEC(CONCAT(N'
    SELECT transaction_id, COUNT(*) AS Dup_Count
    FROM ', @v_schema_prefix, N'.fact_sales
    GROUP BY transaction_id
    HAVING COUNT(*) > 1;
    '));
    
    PRINT 'Test 10: Sales summary';
    EXEC(CONCAT(N'
    SELECT 
        COUNT(*) AS Total_Trans,
        COUNT(DISTINCT customer_id) AS Unique_Customers,
        COUNT(DISTINCT product_id) AS Unique_Products,
        CAST(SUM(quantity) AS BIGINT) AS Total_Qty,
        ROUND(SUM(revenue), 2) AS Total_Revenue,
        ROUND(SUM(profit), 2) AS Total_Profit
    FROM ', @v_schema_prefix, N'.fact_sales;
    '));
    
    PRINT 'BRONZE QC Complete';
END;
GO

CREATE PROCEDURE dbo.sp_qc_silver_layer_validation
    @p_company_source VARCHAR(20) = 'Parent'
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @v_schema_name VARCHAR(50);
    
    IF @p_company_source = 'Parent'
        SET @v_schema_name = 'silver_parent';
    ELSE IF @p_company_source = 'Acquired'
        SET @v_schema_name = 'silver_acquired';
    ELSE
    BEGIN
        RAISERROR('Invalid company source. Use Parent or Acquired', 16, 1);
        RETURN;
    END
    
    PRINT CONCAT('SILVER QC - ', @p_company_source);
    
    PRINT 'Test 1: Customers record count';
    EXEC(CONCAT(N'
    SELECT 
        COUNT(*) AS Total_Records,
        COUNT(CASE WHEN is_current = 1 THEN 1 END) AS Current_Records,
        COUNT(DISTINCT customer_id) AS Unique_IDs
    FROM ', @v_schema_name, N'.dim_customers;
    '));
    
    PRINT 'Test 2: Customers NULL check';
    EXEC(CONCAT(N'
    SELECT 
        ''customer_key'' AS Col, COUNT(*) AS Null_Count FROM ', @v_schema_name, N'.dim_customers WHERE customer_key IS NULL UNION ALL
    SELECT ''customer_id'',               COUNT(*)           FROM ', @v_schema_name, N'.dim_customers WHERE customer_id IS NULL UNION ALL
    SELECT ''name'',                      COUNT(*)           FROM ', @v_schema_name, N'.dim_customers WHERE name IS NULL UNION ALL
    SELECT ''gender'',                    COUNT(*)           FROM ', @v_schema_name, N'.dim_customers WHERE gender IS NULL;
    '));
    
    PRINT 'Test 3: SCD Type 2 violations';
    EXEC(CONCAT(N'
    SELECT customer_id, COUNT(*) AS Current_Count
    FROM ', @v_schema_name, N'.dim_customers
    WHERE is_current = 1
    GROUP BY customer_id
    HAVING COUNT(*) > 1;
    '));
    
    PRINT 'Test 4: Products record count';
    EXEC(CONCAT(N'
    SELECT 
        COUNT(*) AS Total_Records,
        COUNT(DISTINCT product_id) AS Unique_IDs,
        COUNT(DISTINCT brand) AS Unique_Brands,
        COUNT(DISTINCT category) AS Unique_Categories
    FROM ', @v_schema_name, N'.dim_products;
    '));
    
    PRINT 'Test 5: Products NULL check';
    EXEC(CONCAT(N'
    SELECT 
        ''product_key'' AS Col, COUNT(*) AS Null_Count FROM ', @v_schema_name, N'.dim_products WHERE product_key IS NULL UNION ALL
    SELECT ''product_id'',               COUNT(*)           FROM ', @v_schema_name, N'.dim_products WHERE product_id IS NULL UNION ALL
    SELECT ''product_name'',             COUNT(*)           FROM ', @v_schema_name, N'.dim_products WHERE product_name IS NULL;
    '));
    
    PRINT 'Test 6: Sales record count';
    EXEC(CONCAT(N'
    SELECT 
        COUNT(*) AS Total_Records,
        COUNT(DISTINCT transaction_id) AS Unique_Transactions,
        COUNT(DISTINCT customer_key) AS Unique_Customers,
        COUNT(DISTINCT product_key) AS Unique_Products
    FROM ', @v_schema_name, N'.fact_sales;
    '));
    
    PRINT 'Test 7: Sales NULL foreign keys';
    EXEC(CONCAT(N'
    SELECT 
        ''NULL customer_key'' AS Issue, COUNT(*) AS Count FROM ', @v_schema_name, N'.fact_sales WHERE customer_key = -1 UNION ALL
    SELECT ''NULL product_key'',                COUNT(*)           FROM ', @v_schema_name, N'.fact_sales WHERE product_key = -1;
    '));
    
    PRINT 'Test 8: Sales invalid values';
    EXEC(CONCAT(N'
    SELECT 
        ''Negative Qty'' AS Issue, COUNT(*) AS Count FROM ', @v_schema_name, N'.fact_sales WHERE quantity < 0 UNION ALL
    SELECT ''Negative Unit_Price'',                COUNT(*)           FROM ', @v_schema_name, N'.fact_sales WHERE unit_price_usd < 0 UNION ALL
    SELECT ''Negative Cost'',                       COUNT(*)           FROM ', @v_schema_name, N'.fact_sales WHERE cost_usd < 0 UNION ALL
    SELECT ''Negative Revenue'',                    COUNT(*)           FROM ', @v_schema_name, N'.fact_sales WHERE revenue_usd < 0 UNION ALL
    SELECT ''Negative Profit'',                     COUNT(*)           FROM ', @v_schema_name, N'.fact_sales WHERE profit_usd < 0;
    '));
    
    PRINT 'Test 9: Sales financial summary';
    EXEC(CONCAT(N'
    SELECT 
        CAST(SUM(quantity) AS BIGINT) AS Total_Qty,
        ROUND(SUM(revenue_usd), 2) AS Total_Revenue,
        ROUND(SUM(cost_usd), 2) AS Total_Cost,
        ROUND(SUM(profit_usd), 2) AS Total_Profit,
        CASE 
            WHEN SUM(revenue_usd) > 0 
            THEN ROUND((SUM(profit_usd) / SUM(revenue_usd)) * 100, 2)
            ELSE 0 
        END AS Profit_Margin_Pct
    FROM ', @v_schema_name, N'.fact_sales;
    '));
    
    PRINT 'Test 10: Referential integrity';
    EXEC(CONCAT(N'
    SELECT ''Orphaned customer_key'' AS Issue, COUNT(*) AS Count
    FROM ', @v_schema_name, N'.fact_sales f
    LEFT JOIN ', @v_schema_name, N'.dim_customers c ON f.customer_key = c.customer_key
    WHERE f.customer_key != -1 AND c.customer_key IS NULL
    UNION ALL
    SELECT ''Orphaned product_key'', COUNT(*)
    FROM ', @v_schema_name, N'.fact_sales f
    LEFT JOIN ', @v_schema_name, N'.dim_products p ON f.product_key = p.product_key
    WHERE f.product_key != -1 AND p.product_key IS NULL;
    '));
    
    PRINT 'SILVER QC Complete';
END;
GO

CREATE PROCEDURE dbo.sp_qc_run_all_tests
    @p_include_parent BIT = 1,
    @p_include_acquired BIT = 1
AS
BEGIN
    SET NOCOUNT ON;
    
    PRINT '============================================';
    PRINT 'DATA WAREHOUSE QUALITY CHECK';
    PRINT '============================================';
    
    IF @p_include_parent = 1
    BEGIN
        PRINT '';
        PRINT '-- PARENT COMPANY';
        EXEC dbo.sp_qc_bronze_layer_validation @p_company_source = 'Parent';
        PRINT '';
        EXEC dbo.sp_qc_silver_layer_validation @p_company_source = 'Parent';
    END
    
    IF @p_include_acquired = 1
    BEGIN
        PRINT '';
        PRINT '-- ACQUIRED COMPANY';
        EXEC dbo.sp_qc_bronze_layer_validation @p_company_source = 'Acquired';
        PRINT '';
        EXEC dbo.sp_qc_silver_layer_validation @p_company_source = 'Acquired';
    END
    
    PRINT '';
    PRINT '============================================';
    PRINT 'QUALITY CHECK COMPLETED';
    PRINT '============================================';
END;
GO


-- Section 6: Validation
SELECT 
    ROUTINE_SCHEMA,
    ROUTINE_NAME,
    ROUTINE_TYPE
FROM INFORMATION_SCHEMA.ROUTINES
WHERE ROUTINE_TYPE = 'PROCEDURE'
  AND ROUTINE_SCHEMA IN ('silver_parent', 'silver_acquired', 'dbo')
ORDER BY ROUTINE_SCHEMA, ROUTINE_NAME;

SELECT 
    TABLE_SCHEMA,
    TABLE_NAME
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA IN ('silver_parent', 'silver_acquired')
  AND TABLE_TYPE = 'BASE TABLE'
ORDER BY TABLE_SCHEMA, TABLE_NAME;
