-- This query cleans and transforms raw job data through a series of Common Table Expressions (CTEs).
-- Each CTE performs a specific step in the data preparation pipeline.

-- Step 1: Initial Parsing and Extraction from Raw Data
-- CTE Name: ParsedRawData (Original: datajob)
-- Purpose: To parse string fields like 'salary_estimate' and 'company_name' from the source table
-- to extract structured information like min/max salary, company name, and company rating.
WITH ParsedRawData AS (
    SELECT
        -- Raw columns that will be used or passed through
        [Job_Title],
        [Job_Description],
        [Location],
        [Headquarters],
        [Size],
        [Founded],
        [Type_of_ownership],
        [Industry],
        [Sector],
        [Revenue],
        [Competitors],

        -- Extract the minimum salary from the 'salary_estimate' string.
        -- Example: "$50K - $70K..." -> 50.0
        CAST(
            SUBSTRING(
                TRIM(salary_estimate),
                CHARINDEX('$', TRIM(Salary_Estimate)) + 1,
                CHARINDEX('k', TRIM(Salary_Estimate)) - 2
            ) AS FLOAT
        ) AS min_salary,

        -- Extract the maximum salary from the 'salary_estimate' string.
        -- Example: "... - $70K (..." -> 70.0
        CAST(
            REPLACE(
                SUBSTRING(
                    TRIM(salary_estimate),
                    CHARINDEX('-', TRIM(Salary_Estimate)) + 2,
                    CHARINDEX('(', TRIM(salary_estimate)) - CHARINDEX('-', TRIM(salary_estimate)) - 3
                ),
                'K',
                ''
            ) AS FLOAT
        ) AS max_salary,

        -- Extract the estimation method from within the parentheses.
        -- Example: "(Glassdoor est.)" -> "Glassdoor"
        SUBSTRING(
            TRIM(salary_estimate),
            CHARINDEX('(', TRIM(Salary_Estimate)) + 1,
            CHARINDEX(')', TRIM(salary_estimate)) - CHARINDEX('(', TRIM(salary_estimate)) - 6
        ) AS salary_estimation_method,

        -- Separate the company name from its rating if a rating is appended.
        -- Example: "Google 4.5" -> "Google"
        CASE
            -- Check if the last 4 characters of the company name can be cast to a number.
            WHEN TRY_CAST(SUBSTRING(company_name, LEN(TRIM(company_name)) - 3, LEN(TRIM(company_name))) AS FLOAT) IS NULL
            -- If not, it's just a name.
            THEN Company_Name
            -- If yes, it's a name with a rating, so trim the rating part.
            ELSE SUBSTRING(company_name, 0, LEN(TRIM(company_name)) - 3)
        END AS company_name,

        -- Extract the numeric rating from the company name if it exists.
        -- Example: "Google 4.5" -> 4.5
        TRY_CAST(
            SUBSTRING(company_name, LEN(TRIM(company_name)) - 3, LEN(TRIM(company_name))) AS FLOAT
        ) AS company_rating

    FROM [DataJobs].[Bronze].[Uncleaned_DS_jobs]
),

-- Step 2: De-duplication and Location Splitting
-- CTE Name: ProcessedLocations (Original: datajob2)
-- Purpose: To standardize salary ranges for duplicate job postings and to split location/headquarter strings into components.
ProcessedLocations AS (
    SELECT
        Job_Title,
        Job_Description,
        salary_estimation_method,
        company_name,
        company_rating,
        Size,
        Founded,
        Type_of_ownership,
        Industry,
        Sector,
        Revenue,
        Competitors,

        -- For identical job postings, find the absolute minimum salary across all duplicates.
        MIN(min_salary) OVER (
            PARTITION BY Job_Title, company_name, company_rating, Location, Headquarters
        ) AS min_salary_final,
        -- For identical job postings, find the absolute maximum salary across all duplicates.
        MAX(max_salary) OVER (
            PARTITION BY Job_Title, company_name, company_rating, Location, Headquarters
        ) AS max_salary_final,

        -- Split the location into city and state. This extracts the city.
        -- Example: "New York, NY" -> "New York"
        CASE
            WHEN LEN(TRIM(SUBSTRING(location, LEN(TRIM(location)) - 2, LEN(TRIM(location))))) != 2
            THEN Location
            ELSE SUBSTRING(location, 0, LEN(location) - 3)
        END AS location_city,

        -- This part extracts the state abbreviation.
        -- Example: "New York, NY" -> "NY"
        CASE
            WHEN LEN(TRIM(SUBSTRING(location, LEN(TRIM(location)) - 2, LEN(TRIM(location))))) = 2
            THEN TRIM(SUBSTRING(location, LEN(TRIM(location)) - 2, LEN(TRIM(location))))
            ELSE NULL
        END AS location_state,

        -- Split the headquarters string at the comma to get the city.
        -- Example: "Mountain View, CA" -> "Mountain View"
        NULLIF(SUBSTRING(Headquarters, 0, CHARINDEX(',', TRIM(Headquarters))), '') AS hq_city,

        -- Get the part after the comma for the state/country.
        -- Example: "Mountain View, CA" -> "CA"
        NULLIF(
            SUBSTRING(TRIM(Headquarters), CHARINDEX(',', TRIM(Headquarters)) + 2, LEN(TRIM(Headquarters))),
            '1' -- Handle a specific data anomaly where '1' is present.
        ) AS hq_state

    FROM ParsedRawData
),

-- Step 3: Data Cleaning and Standardization
-- CTE Name: CleanedData (Original: datajob3)
-- Purpose: To clean up placeholder values (like '-1' or 'Unknown'), standardize formats, and prepare text for categorization.
CleanedData AS (
    SELECT
        Job_Title,
        Job_Description,
        min_salary_final,
        max_salary_final,
        salary_estimation_method,
        company_name,
        company_rating,
        location_city,
        location_state,
        hq_city,

        -- Determine the headquarters country based on the state/country field's length.
        CASE
            WHEN LEN(hq_state) > 2 THEN hq_state -- If longer than 2 chars, assume it's a country.
            WHEN hq_state IS NULL THEN NULL
            ELSE 'United state' -- Otherwise, assume it's a US state.
        END AS hq_country,

        -- Standardize the headquarters state field.
        CASE
            WHEN LEN(hq_state) > 2 THEN NULL -- If it was a country, nullify the state.
            ELSE hq_state -- Otherwise, keep the state abbreviation.
        END AS hq_state_final,

        -- Clean and standardize the company 'Size' column.
        CASE
            WHEN Size IN ('-1', 'Unknown') THEN NULL
            WHEN CHARINDEX('+', TRIM(size)) != '0' THEN SUBSTRING(TRIM(Size), 0, CHARINDEX('employees', TRIM(size)) - 1)
            WHEN CHARINDEX('+', TRIM(size)) = '0' THEN REPLACE(SUBSTRING(TRIM(size), 0, CHARINDEX('employees', TRIM(size))), ' to ', '-')
            ELSE NULL
        END AS company_size,

        -- Clean 'Founded' year by replacing placeholder '-1' with NULL.
        CASE WHEN Founded = '-1' THEN NULL ELSE Founded END AS founded_year,

        -- Clean 'Type_of_ownership' by replacing placeholders with NULL.
        CASE WHEN type_of_ownership IN ('-1', 'Unknown') THEN NULL ELSE Type_of_ownership END AS type_of_ownership_cleaned,

        -- Clean 'Industry' by replacing placeholder '-1' with NULL.
        CASE WHEN Industry = '-1' THEN NULL ELSE Industry END AS industry_cleaned,

        -- Clean 'Sector' by replacing placeholder '-1' with NULL.
        CASE WHEN Sector = '-1' THEN NULL ELSE Sector END AS sector_cleaned,

        -- Extract and clean the 'Revenue' value.
        -- Example: "$10+ billion (USD)" -> "10+ billion "
        NULLIF(REPLACE(SUBSTRING(Revenue, 0, CHARINDEX('(USD)', revenue)), '$', ''), '') AS revenue_usd,

        -- Clean 'Competitors' by replacing placeholder '-1' with NULL.
        CASE WHEN Competitors = '-1' THEN NULL ELSE Competitors END AS competitors_cleaned,

        -- Create a standardized, lowercase job title for pattern matching.
        -- This involves removing punctuation and replacing seniority variations.
        LOWER(
            TRIM(
                REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                    Job_Title, '-', ' '), ',', ' '), '.', ' '), '/', ' '), '(', ' '), ')', ' '), 'Senior', 'sr'), '–', ' '),
                    ' iii', ' sr'), ' ii', ' mid'), ' i ', ' jr '), ' 1 ', ' jr '), '  ', ' '), '  ', ' ')
            )
        ) AS job_title_cleaned
    FROM ProcessedLocations
),

-- Step 4: Data Enrichment and Categorization
-- CTE Name: EnrichedData (Original: datajob4)
-- Purpose: To create new categorical columns like 'job_level' and 'role' based on keywords found in the cleaned data.
EnrichedData AS (
    SELECT
        *, -- Select all columns from the previous CTE

        -- Categorize the job's seniority level based on keywords in the title or description.
        CASE 
            WHEN job_title_cleaned LIKE '%chief%'   OR job_title_cleaned LIKE '%executive%'  OR job_title_cleaned LIKE '%officer%'  OR job_title_cleaned LIKE '%vp%'         OR job_title_cleaned LIKE '%head%'      OR job_title_cleaned LIKE '%director%'             OR job_title_cleaned LIKE '%manager %'  THEN 'Manager / Director'
            WHEN job_title_cleaned LIKE '%lead %'   OR job_title_cleaned LIKE '%principal%'  OR job_title_cleaned LIKE '%staff%'    OR job_title_cleaned LIKE '%architect%'  OR job_title_cleaned LIKE '%expert%'    OR job_title_cleaned LIKE '%technical specialist%'                                         THEN 'Lead / Principal'
            WHEN job_title_cleaned LIKE '%senior%'  OR job_title_cleaned LIKE '%sr%'         OR job_title_cleaned LIKE '%iii%'      OR job_title_cleaned LIKE '% iv%'                                                                                                                                           THEN 'Senior'
            WHEN job_title_cleaned LIKE '%intern %' OR job_title_cleaned LIKE '%entry%'      OR job_title_cleaned LIKE '%junior%'   OR job_title_cleaned LIKE '%jr%'         OR job_title_cleaned LIKE '%associate%' OR job_title_cleaned LIKE '%early career%'         OR job_title_cleaned LIKE '%assistant%' THEN 'Entry / junior'
            -- Fallback checks in the job description
            WHEN Job_Description   LIKE '%chief %'  OR Job_Description   LIKE '%executive %' OR Job_Description   LIKE '%officer %' OR Job_Description   LIKE '%vp%'         OR Job_Description   LIKE '%head %'     OR Job_Description   LIKE '%director %'            OR Job_Description   LIKE '%manager %'  THEN 'Manager / Director'
            WHEN Job_Description   LIKE '%lead %'   OR Job_Description   LIKE '%principal %' OR Job_Description   LIKE '%staff %'   OR Job_Description   LIKE '%architect %' OR Job_Description   LIKE '%expert %'   OR Job_Description   LIKE '%technical specialist%'                                         THEN 'Lead / Principal'
            WHEN Job_Description   LIKE '%senior%'  OR Job_Description   LIKE '%sr %'        OR Job_Description   LIKE '%iii%'      OR Job_Description   LIKE '% iv%'                                                                                                                                           THEN 'Senior'
            WHEN Job_Description   LIKE '%intern %' OR Job_Description   LIKE '%entry %'     OR Job_Description   LIKE '%junior%'   OR Job_Description   LIKE '% jr %'         OR Job_Description   LIKE '%associate %' OR Job_Description   LIKE '%early career%'         OR Job_Description   LIKE '%assistant%' THEN 'Entry / junior'
            ELSE 'Mid-Level'
        END AS job_level,

        -- Categorize the primary job role based on keywords in the cleaned title.
        CASE
            WHEN job_title_cleaned LIKE '%machine learning%' OR job_title_cleaned LIKE '%ml%' OR job_title_cleaned LIKE '%ai%' OR job_title_cleaned LIKE '%deep learning%' OR job_title_cleaned LIKE '%nlp%' OR job_title_cleaned LIKE '%computer vision%' OR job_title_cleaned LIKE '%ml engineer%' OR job_title_cleaned LIKE '%ai engineer%' OR job_title_cleaned LIKE '%ml scientist%' THEN 'ML / AI Engineer or Scientist / Director'
            WHEN job_title_cleaned LIKE '%applied data scientist%' OR job_title_cleaned LIKE '%applied scientist%' OR job_title_cleaned LIKE '%data scientist%' OR job_title_cleaned LIKE '%data scienc%' OR job_title_cleaned LIKE '%predictive%' OR job_title_cleaned LIKE '%analytics practitioner%' OR job_title_cleaned LIKE '%decision scientist%' THEN 'Data Scientist / Applied Scientist'
            WHEN job_title_cleaned LIKE '%data engineer%' OR job_title_cleaned LIKE '%data architect%' OR job_title_cleaned LIKE '%data integration%' OR job_title_cleaned LIKE '%data modeling%' OR job_title_cleaned LIKE '%data model%' OR job_title_cleaned LIKE '%data pipeline%' OR job_title_cleaned LIKE '%etl%' OR job_title_cleaned LIKE '%database%' OR job_title_cleaned LIKE '%engineer%' THEN 'Data Engineer'
            WHEN job_title_cleaned LIKE '%data analyst%' OR job_title_cleaned LIKE '%analytics%' OR job_title_cleaned LIKE '% bi%' OR job_title_cleaned LIKE '%business intelligence%' OR job_title_cleaned LIKE '%reporting%' OR job_title_cleaned LIKE '%insights%' THEN 'Data Analyst'
            WHEN job_title_cleaned LIKE '%research%' OR job_title_cleaned LIKE '%scientist%' OR job_title_cleaned LIKE '%r&d%' OR job_title_cleaned LIKE '%investigator%' OR job_title_cleaned LIKE '%biomedical%' OR job_title_cleaned LIKE '%clinical%' OR job_title_cleaned LIKE '%molecular%' OR job_title_cleaned LIKE '%statistical%' THEN 'Research / Scientific'
            WHEN job_title_cleaned LIKE '%software engineer%' OR job_title_cleaned LIKE '%software%' OR job_title_cleaned LIKE '%developer%' THEN 'Software / Engineering'
            WHEN job_title_cleaned LIKE '%manager%' OR job_title_cleaned LIKE '%director%' OR job_title_cleaned LIKE '%vp%' OR job_title_cleaned LIKE '%chief%' OR job_title_cleaned LIKE '%head%' OR job_title_cleaned LIKE '%officer%' OR job_title_cleaned LIKE '%executive%' THEN 'Executive / Management'
            ELSE NULL
        END AS job_role,

        -- Extract a list of required skills by searching for keywords in the job description.
        NULLIF(
            SUBSTRING(
                CONCAT(
                    CASE WHEN job_description LIKE '%python%' THEN 'python - ' ELSE NULL END,
                    CASE WHEN job_description LIKE '%excel%' THEN 'excel - ' ELSE NULL END,
                    CASE WHEN job_description LIKE '%aws%' THEN 'aws - ' ELSE NULL END,
                    CASE WHEN job_description LIKE '%spark%' THEN 'spark - ' ELSE NULL END,
                    CASE WHEN job_description LIKE '%hadoop%' THEN 'hadoop - ' ELSE NULL END,
                    CASE WHEN job_description LIKE '%big data%' THEN 'big-data - ' ELSE NULL END,
                    CASE WHEN job_description LIKE '%tableau%' THEN 'tableau  - ' ELSE NULL END
                ),
                0, -- Note: SUBSTRING is 1-based in SQL Server, so 0 might behave unexpectedly.
                LEN(
                    CONCAT(
                        CASE WHEN job_description LIKE '%python%' THEN 'python - ' ELSE NULL END,
                        CASE WHEN job_description LIKE '%excel%' THEN 'excel - ' ELSE NULL END,
                        CASE WHEN job_description LIKE '%aws%' THEN 'aws - ' ELSE NULL END,
                        CASE WHEN job_description LIKE '%spark%' THEN 'spark - ' ELSE NULL END,
                        CASE WHEN job_description LIKE '%hadoop%' THEN 'hadoop - ' ELSE NULL END,
                        CASE WHEN job_description LIKE '%big data%' THEN 'big data - ' ELSE NULL END,
                        CASE WHEN job_description LIKE '%tableau%' THEN 'tableau  - ' ELSE NULL END
                    )
                )  
            ),
            ''
        ) AS job_requirements

    FROM CleanedData
)

-- Final Step: Select and display the fully cleaned and enriched data.
-- Use DISTINCT to remove any fully duplicate rows that may have been created during processing.
SELECT DISTINCT
    -- ROW_NUMBER() OVER(ORDER BY min_salary_final , max_salary_final) AS job_index ,
    Job_Title,
    job_role,
    job_level,
    ISNULL(job_requirements, 'N/A') job_requirements ,
    min_salary_final AS min_salary,
    max_salary_final AS max_salary,
    salary_estimation_method,
    company_name,
    ISNULL(CAST(company_rating AS nvarchar), 'N/A') company_rating ,
    location_city,
    ISNULL(location_state,'N/A') location_state,
    ISNULL(hq_country,'N/A') hq_country,
    ISNULL(hq_state_final,'N/A') AS hq_state,
    ISNULL(hq_city,'N/A')hq_city,
    ISNULL(company_size,'N/A')company_size,
    ISNULL(CAST(founded_year AS nvarchar),'N/A')founded_year,
    ISNULL(type_of_ownership_cleaned ,'N/A')AS type_of_ownership,
    ISNULL(industry_cleaned, 'N/A')AS industry,
    ISNULL(sector_cleaned ,'N/A')AS sector,
    ISNULL(revenue_usd,'N/A')revenue_usd,
    ISNULL(competitors_cleaned ,'N/A')AS competitors,
    Job_Description
    INTO Silver.CLEANED_datajobs
FROM EnrichedData; 
