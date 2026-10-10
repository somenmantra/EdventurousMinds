/* =====================================================================
   DYNAMIC TABLES MINI DEMO  —  Bronze ➜ Silver ➜ Gold (Medallion)
   ---------------------------------------------------------------------
   What you will see:
     1. Raw (BRONZE) data landing in a normal table
     2. A SILVER dynamic table that cleans / dedupes / types the data
     3. A GOLD dynamic table that aggregates silver
     4. Key properties: TARGET_LAG, WAREHOUSE, REFRESH_MODE, INITIALIZE,
        TARGET_LAG = DOWNSTREAM, manual REFRESH, SUSPEND / RESUME
     5. INCREMENTAL refreshes: insert / update / delete in bronze and
        watch only the changed rows flow through to silver & gold
   Run the script top to bottom, one block at a time.
   ===================================================================== */

-- ---------------------------------------------------------------------
-- STEP 0: Setup (isolated demo objects, safe to drop afterwards)
-- ---------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS DT_DEMO;
CREATE SCHEMA   IF NOT EXISTS DT_DEMO.MEDALLION;
CREATE WAREHOUSE IF NOT EXISTS DT_DEMO_WH
  WAREHOUSE_SIZE = XSMALL AUTO_SUSPEND = 60 AUTO_RESUME = TRUE;

USE DATABASE DT_DEMO;
USE SCHEMA   MEDALLION;
USE WAREHOUSE DT_DEMO_WH;

-- ---------------------------------------------------------------------
-- STEP 1: BRONZE layer = raw data "as it arrives"
--   Messy on purpose: strings for numbers, mixed case, spaces,
--   duplicates, NULLs, negative amounts.
-- ---------------------------------------------------------------------
CREATE OR REPLACE TABLE BRONZE_ORDERS (
    order_id     STRING,
    customer     STRING,
    country      STRING,
    amount       STRING,        -- raw text, not a number yet
    order_ts     STRING,        -- raw text, not a timestamp yet
    loaded_at    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

INSERT INTO BRONZE_ORDERS (order_id, customer, country, amount, order_ts) VALUES
  ('1', '  alice ', 'us',  '100.50', '2026-10-01 10:00:00'),
  ('2', 'BOB',      'UK',  '200',    '2026-10-01 11:00:00'),
  ('2', 'BOB',      'UK',  '200',    '2026-10-01 11:00:00'),   -- duplicate
  ('3', 'carol',    'In',  '-50',    '2026-10-02 09:30:00'),   -- invalid amount
  ('4', NULL,       'us',  '75',     '2026-10-02 12:00:00'),   -- missing customer
  ('5', 'dave',     'uk',  '300',    '2026-10-03 15:45:00');

SELECT * FROM BRONZE_ORDERS ORDER BY order_id;   -- 6 messy rows

-- ---------------------------------------------------------------------
-- STEP 2: SILVER layer = cleaned, typed, deduplicated
--   It is just a SELECT. Snowflake keeps it up to date automatically.
--
--   KEY PROPERTIES
--   TARGET_LAG   = DOWNSTREAM -> refresh only when a downstream DT needs it
--   WAREHOUSE    = compute used for refreshes
--   REFRESH_MODE = INCREMENTAL -> process only changed rows (AUTO/FULL also exist)
--   INITIALIZE   = ON_CREATE  -> populate immediately (or ON_SCHEDULE)
-- ---------------------------------------------------------------------
CREATE OR REPLACE DYNAMIC TABLE SILVER_ORDERS
  TARGET_LAG   = DOWNSTREAM
  WAREHOUSE    = DT_DEMO_WH
  REFRESH_MODE = INCREMENTAL
  INITIALIZE   = ON_CREATE
  COMMENT      = 'Silver: cleaned & deduplicated orders'
AS
SELECT DISTINCT                                   -- remove duplicates
       TRY_TO_NUMBER(order_id)              AS order_id,
       INITCAP(TRIM(customer))              AS customer,      -- '  alice ' -> 'Alice'
       UPPER(TRIM(country))                 AS country,       -- 'us' -> 'US'
       TRY_TO_DECIMAL(amount, 10, 2)        AS amount,        -- text -> number
       TRY_TO_TIMESTAMP_NTZ(order_ts)       AS order_ts,      -- text -> timestamp
       TO_DATE(TRY_TO_TIMESTAMP_NTZ(order_ts)) AS order_date
FROM   BRONZE_ORDERS
WHERE  customer IS NOT NULL                       -- data quality rules
  AND  TRY_TO_DECIMAL(amount, 10, 2) > 0;

SELECT * FROM SILVER_ORDERS ORDER BY order_id;
-- Expect 3 clean rows: 1 Alice, 2 Bob (deduped), 5 Dave
-- Order 3 (negative) and 4 (no customer) were filtered out

-- ---------------------------------------------------------------------
-- STEP 3: GOLD layer = business aggregate on top of silver
--   TARGET_LAG = '1 minute' drives the whole chain:
--   Snowflake refreshes SILVER first, then GOLD, in dependency order.
-- ---------------------------------------------------------------------
CREATE OR REPLACE DYNAMIC TABLE GOLD_SALES_BY_COUNTRY
  TARGET_LAG   = '10 minute'
  WAREHOUSE    = DT_DEMO_WH
  REFRESH_MODE = INCREMENTAL
AS
SELECT country,
       COUNT(*)     AS total_orders,
       SUM(amount)  AS total_sales
FROM   SILVER_ORDERS
GROUP BY country;

SELECT * FROM GOLD_SALES_BY_COUNTRY ORDER BY country;
-- UK: 2 orders / 500.00   US: 1 order / 100.50

-- Inspect the properties Snowflake recorded
SHOW DYNAMIC TABLES IN SCHEMA DT_DEMO.MEDALLION;
-- Look at: target_lag, refresh_mode, refresh_mode_reason, scheduling_state, warehouse

-- ---------------------------------------------------------------------
-- STEP 4: INCREMENTAL RUN #1  —  new data arrives in bronze
-- ---------------------------------------------------------------------
INSERT INTO BRONZE_ORDERS (order_id, customer, country, amount, order_ts) VALUES
  ('6', ' erin',  'in', '150',  '2026-10-04 08:00:00'),
  ('7', 'frank',  'US', '0',    '2026-10-04 09:00:00'),   -- invalid (0) -> filtered
  ('8', 'grace ', 'uk', '50.25','2026-10-04 10:00:00');


SELECT * FROM BRONZE_ORDERS;

-- Wait ~1 minute for the schedule, OR refresh on demand (refreshes SILVER too):
ALTER DYNAMIC TABLE GOLD_SALES_BY_COUNTRY REFRESH;

SELECT * FROM SILVER_ORDERS         ORDER BY order_id;   -- now 5 rows (+Erin, +Grace)
SELECT * FROM GOLD_SALES_BY_COUNTRY ORDER BY country;    -- IN appears, UK grows

-- ---------------------------------------------------------------------
-- STEP 5: INCREMENTAL RUN #2  —  updates and deletes also flow through
-- ---------------------------------------------------------------------
UPDATE BRONZE_ORDERS SET amount = '1234567' WHERE order_id = '1';  -- Alice 100.50 -> 500
UPDATE BRONZE_ORDERS SET customer = 'hank' WHERE order_id = '4'; -- fixes a bad row -> now valid
DELETE FROM BRONZE_ORDERS WHERE order_id = '5';                -- Dave cancelled

ALTER DYNAMIC TABLE GOLD_SALES_BY_COUNTRY REFRESH;

SELECT * FROM BRONZE_ORDERS WHERE order_id='1';
SELECT * FROM SILVER_ORDERS;

ALTER DYNAMIC TABLE GOLD_SALES_BY_COUNTRY REFRESH;



SELECT * FROM SILVER_ORDERS         ORDER BY order_id;   -- Alice=500, Hank added, Dave gone
SELECT * FROM GOLD_SALES_BY_COUNTRY ORDER BY country;    -- US and UK totals adjusted

-- ---------------------------------------------------------------------
-- STEP 6: PROOF it was incremental — refresh history
--   refresh_action = INCREMENTAL  (only changes processed)
--   statistics     = rows inserted / deleted per refresh
-- ---------------------------------------------------------------------
SELECT name,
       refresh_action,             -- INITIAL / INCREMENTAL / FULL / NO_DATA
       state,
       refresh_trigger,            -- SCHEDULED / MANUAL / CREATION
       statistics:numInsertedRows::INT AS rows_inserted,
       statistics:numDeletedRows::INT  AS rows_deleted,
       refresh_start_time,
       DATEDIFF('ms', refresh_start_time, refresh_end_time) AS duration_ms
FROM TABLE(DT_DEMO.INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY(
       NAME_PREFIX => 'DT_DEMO.MEDALLION'))
ORDER BY refresh_start_time DESC;

-- No changes in bronze? The next refresh is a cheap NO_DATA (no work done):
ALTER DYNAMIC TABLE GOLD_SALES_BY_COUNTRY REFRESH;

-- Dependency graph (BRONZE -> SILVER -> GOLD)
SELECT name, inputs, target_lag_type, target_lag_sec, scheduling_state
FROM TABLE(DT_DEMO.INFORMATION_SCHEMA.DYNAMIC_TABLE_GRAPH_HISTORY())
WHERE schema_name = 'MEDALLION';

-- ---------------------------------------------------------------------
-- STEP 7: Operational controls
-- ---------------------------------------------------------------------
ALTER DYNAMIC TABLE GOLD_SALES_BY_COUNTRY SET TARGET_LAG = '5 minutes'; -- change freshness
ALTER DYNAMIC TABLE GOLD_SALES_BY_COUNTRY SUSPEND;                      -- pause (no cost)
ALTER DYNAMIC TABLE GOLD_SALES_BY_COUNTRY RESUME;                       -- resume

-- Dynamic tables are read-only; DML against them fails (uncomment to see):
-- INSERT INTO SILVER_ORDERS VALUES (99,'X','US',1,CURRENT_TIMESTAMP(),CURRENT_DATE());

--
