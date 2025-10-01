-- ========================================
-- Step 1: Clean raw sales data
-- Replace 'ERROR' and 'UNKNOWN' with NULL, and cast numeric columns to float
-- ========================================
WITH cleaned_sales AS (
    SELECT
        Transaction_ID,
        CASE WHEN Item IN ('ERROR', 'UNKNOWN') THEN NULL ELSE Item END AS Item,
        CAST(CASE WHEN Quantity IN ('ERROR', 'UNKNOWN') THEN NULL ELSE Quantity END AS float) AS Quantity,
        CAST(CASE WHEN Price_Per_Unit IN ('ERROR', 'UNKNOWN') THEN NULL ELSE Price_Per_Unit END AS float) AS Price_Per_Unit,
        CAST(CASE WHEN Total_Spent IN ('ERROR', 'UNKNOWN') THEN NULL ELSE Total_Spent END AS float) AS Total_Spent,
        CASE WHEN Payment_Method IN ('ERROR', 'UNKNOWN') THEN NULL ELSE Payment_Method END AS Payment_Method,
        CASE WHEN Location IN ('ERROR', 'UNKNOWN') THEN NULL ELSE Location END AS Location,
        CASE WHEN Transaction_Date IN ('ERROR', 'UNKNOWN') THEN NULL ELSE Transaction_Date END AS Transaction_Date
    FROM Silver.cafe_sales
),

-- ========================================
-- Step 2: Infer or assign item prices
-- 1. If item is known → assign fixed price
-- 2. If price_per_unit is missing but Total_Spent / Quantity exists → calculate
-- 3. If everything is missing → fallback to 2.9 (average price of all known items)
-- ========================================
item_prices AS (
    SELECT
        Transaction_ID,
        Item,
        COALESCE(
            CASE 
                WHEN Item = 'Coffee'   THEN 2
                WHEN Item = 'Cake'     THEN 3
                WHEN Item = 'Cookie'   THEN 1
                WHEN Item = 'Salad'    THEN 5
                WHEN Item = 'Smoothie' THEN 4
                WHEN Item = 'Sandwich' THEN 4
                WHEN Item = 'Juice'    THEN 3
                WHEN Item = 'Tea'      THEN 1.5
                -- If item is unknown but total_spent & quantity exist → estimate price
                ELSE Total_Spent / NULLIF(Quantity,0)
            END,
            Price_Per_Unit,  -- If item still unknown but price_per_unit exists → use it
            2.9              -- Final fallback: 2.9 = global average price of menu items
        ) AS Item_Price,
        Quantity,
        Total_Spent,
        Payment_Method,
        Location,
        Transaction_Date
    FROM cleaned_sales
),

-- ========================================
-- Step 3: Normalize item names
-- Special inferred labels used when item is still ambiguous:
--   * 'AVGpro' → Items with fully unknown identity (no way to determine category)
--   * 'JuORCa' → Could be either Juice or Cake (same price range)
--   * 'SmORSa' → Could be Smoothie or Sandwich (same price range)
-- ========================================
final_sales AS (
    SELECT
        Transaction_ID,
        CASE COALESCE(Item, CAST(Item_Price AS varchar(10)))
            WHEN '1'   THEN 'Cookie'
            WHEN '1.5' THEN 'Tea'
            WHEN '2'   THEN 'Coffee'
            WHEN '2.9' THEN 'AVGpro' -- Undetermined → assigned average-based placeholder
            WHEN '3'   THEN 'JuORCa' -- Price 3 → could be Juice OR Cake
            WHEN '4'   THEN 'SmORSa' -- Price 4 → could be Smoothie OR Sandwich
            WHEN '5'   THEN 'Salad'
            ELSE Item
        END AS New_Item,
        Item_Price,
        -- If quantity missing → try Total_Spent / price, else default to 1
        COALESCE(Quantity, Total_Spent / NULLIF(Item_Price,0), 1) AS New_Quantity,
        -- Recalculate total if missing
        COALESCE(Total_Spent, Item_Price * COALESCE(Quantity,1), Item_Price) AS New_Total_Spent,
        COALESCE(Payment_Method,'N/A') AS New_Payment_Method,
        COALESCE(Location,'N/A') AS New_Location,
        COALESCE(Transaction_Date,'2025-01-01') AS New_Transaction_Date
    FROM item_prices
)

-- ========================================
-- Step 4: Final output with order index
-- ========================================
SELECT
    ROW_NUMBER() OVER (ORDER BY New_Transaction_Date) AS Order_ID,
    New_Item,
    Item_Price,
    New_Quantity,
    New_Total_Spent,
    New_Payment_Method,
    New_Location,
    New_Transaction_Date
FROM final_sales;
