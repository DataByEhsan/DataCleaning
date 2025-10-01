
/* =======================
   Monthly Sales Analysis
   - Calculates total monthly revenue
   - Measures change vs. previous month
   - Measures deviation vs. average
   ======================= */
WITH monthly_sales AS (
    SELECT 
        FORMAT(transaction_date, 'yyyy-MM') AS month, -- Extract month (YYYY-MM)
        SUM(total_spent) AS total_monthly_sales
    FROM Silver.cafe_sales_clean
    WHERE FORMAT(transaction_date, 'yyyy-MM') != '2025-01' -- Exclude incomplete month
    GROUP BY FORMAT(transaction_date, 'yyyy-MM')
)
SELECT
    month,
    total_monthly_sales,
    CONCAT(
        ROUND((total_monthly_sales - AVG(total_monthly_sales) OVER()) / 
              AVG(total_monthly_sales) OVER() * 100, 1), ' %'
    ) AS avg_difference, -- % difference from overall average revenue
    CONCAT(
        ROUND((total_monthly_sales - LAG(total_monthly_sales) OVER(ORDER BY month)) /
              LAG(total_monthly_sales) OVER(ORDER BY month) * 100, 1), ' %'
    ) AS monthly_change   -- % change vs previous month
FROM monthly_sales;


/* =======================
   Item Sales Contribution
   - Ranks items by total revenue and share of total
   ======================= */
SELECT 
    item,
    item_quantity,
    item_total_sales,
    CONCAT(ROUND(item_total_sales / SUM(item_total_sales) OVER() * 100, 1), ' %') AS percentage_of_total
FROM (
    SELECT 
        item,
        SUM(quantity) AS item_quantity, 
        SUM(total_spent) AS item_total_sales
    FROM Silver.cafe_sales_clean 
    GROUP BY item
) AS t
ORDER BY item_total_sales DESC;


/* =======================
   Location Distribution
   - Counts orders per location
   - Computes % distribution
   ======================= */
SELECT  
    location,
    COUNT(id) AS total_location,
    CONCAT(
        ROUND(COUNT(id) / CAST(SUM(COUNT(id)) OVER() AS float) * 100, 1), ' %'
    ) AS percent_of_total
FROM Silver.cafe_sales_clean
GROUP BY location;


/* =======================
   Payment Method Distribution
   - Shows preferred payment methods
   ======================= */
SELECT  
    payment_method,
    COUNT(id) AS total_method, 
    CONCAT(
        ROUND(COUNT(id) / CAST(SUM(COUNT(id)) OVER() AS float) * 100, 1), ' %'
    ) AS percent_of_total
FROM Silver.cafe_sales_clean
GROUP BY payment_method;
