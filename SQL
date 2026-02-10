/* Create Schemas */

CREATE SCHEMA raw;
CREATE SCHEMA clean;
CREATE SCHEMA analytics;

/* Create raw Tables*/

--Create raw.accounts
CREATE TABLE raw.accounts (
    account_id VARCHAR(50), 
    account_name VARCHAR(255),
    industry VARCHAR(100),
    country VARCHAR(100),
    signup_date VARCHAR(100),
    referral_source VARCHAR(100),
    plan_tier VARCHAR(100),
    seats INT,
    is_trial BIT,
    churn_flag BIT
);

-- create raw.subscriptions
CREATE TABLE raw.subscriptions (
    subscription_id INT,
    account_id INT,
    plan_name VARCHAR(100),
    start_date VARCHAR(100),
    end_date DATE,
    monthly_recurring_revenue DECIMAL(10,2)
);

--create raw.feature_usage
CREATE TABLE raw.feature_usage (
    usage_id VARCHAR(50),
    usage_date VARCHAR(100),
    feature_name VARCHAR(100),
    usage_count INT
);

--create raw.support_tickets
CREATE TABLE raw.support_tickets (
    ticket_id VARCHAR(100),
    account_id INT,
    created_date VARCHAR(100),
    resolved_date VARCHAR(100),
    satisfaction_score INT,
    priority VARCHAR(100)
);

--create raw.churn_events
CREATE TABLE raw.churn_events (
    account_id VARCHAR(100),
    churn_date VARCHAR(100),
    reason_code VARCHAR(100),
    refund_amount_usd DECIMAL(10,2)
);

-- Table Creation Validation
SELECT COUNT(*) FROM raw.accounts;
SELECT COUNT(*) FROM raw.subscriptions;
SELECT COUNT(*) FROM raw.feature_usage;
SELECT COUNT(*) FROM raw.support_tickets;
SELECT COUNT(*) FROM raw.churn_events;





/* Create clean tables, Populate with date, Clean Date Columns*/


-- cleaned accounts table
SELECT
    account_id,
    account_name,
    industry,
    country,
    TRY_CAST(signup_date AS DATE) AS signup_date,
    referral_source,
    plan_tier,
    seats,
    is_trial,
    churn_flag
INTO clean.accounts
FROM raw.accounts;


-- cleaned subscriptions table
SELECT
    subscription_id,
    account_id,
    plan_name,
    TRY_CAST(start_date AS DATE) AS start_date,
    TRY_CAST(end_date AS DATE) AS end_date,
    monthly_recurring_revenue
INTO clean.subscriptions
FROM raw.subscriptions;
 

-- cleaned feature_usage table
SELECT
    usage_id,
    TRY_CAST(usage_date AS DATE) AS usage_date,
    feature_name,
    usage_count
INTO clean.feature_usage
FROM raw.feature_usage;


-- cleaned support_tickets table
SELECT
    ticket_id,
    account_id,
    TRY_CAST(created_date AS DATE) AS created_date,
    TRY_CAST(resolved_date AS DATE) AS resolved_date,
    satisfaction_score,
    priority
INTO clean.support_tickets
FROM raw.support_tickets;


-- cleaned churn_events table
SELECT
    account_id,
    TRY_CAST(churn_date AS DATE) AS churn_date,
    reason_code,
    refund_amount_usd
INTO clean.churn_events
FROM raw.churn_events;


/* Standardize Categorical Columns */

--standardize plan tiers
UPDATE clean.accounts
SET plan_tier = UPPER(plan_tier);

--standardize plan name
UPDATE clean.subscriptions
SET plan_name = UPPER(plan_name);

-- standardize priority
UPDATE clean.support_tickets
SET priority = UPPER(priority);


/* Convert Blank values to NULL values */

UPDATE clean.accounts
SET account_name = NULL
WHERE account_name = '';

UPDATE clean.accounts
SET referral_source = NULL
WHERE referral_source = '';

UPDATE clean.accounts
SET plan_tier = NULL
WHERE plan_tier = '';

UPDATE clean.support_tickets
SET priority = NULL
WHERE priority = '';

UPDATE clean.subscriptions
SET end_date = NULL
WHERE end_date = '';

UPDATE clean.support_tickets
SET satisfaction_score = NULL
WHERE satisfaction_score = '';


/* Add Potential useful columns for Analytics */

-- add subscription duration in days
ALTER TABLE clean.subscriptions
ADD subscription_days AS 
      CASE 
        WHEN end_date IS NOT NULL THEN DATEDIFF(DAY, start_date, end_date)
        ELSE NULL
    END;

-- calculate ticket resolution time
ALTER TABLE clean.support_tickets
ADD resolution_days AS  
      CASE 
        WHEN resolved_date IS NOT NULL THEN DATEDIFF(DAY, created_date, resolved_date)
        ELSE NULL
    END;

--churn flag
ALTER TABLE clean.accounts
ADD churn_flag_calc AS CASE WHEN account_id IN (SELECT account_id FROM clean.churn_events) THEN 1 ELSE 0 END;


/* 
====================================================================================================
Create Aggregated Fact Tables to use for Dashboard 
    fact_accounts - Total MRR, Subscription Duration, churn flag
    fact_support - Average Satisfaction, Ticket Counts by priority, Average Resolution Time
    fact_feature_usage - usage counts per day/week
====================================================================================================
*/

--create clean.fact_accounts
SELECT 
    a.account_id,
    a.account_name,
    a.industry,
    a.country,
    a.plan_tier,
    a.seats,
    a.is_trial,
    a.churn_flag_calc,
    COUNT(s.subscription_id) AS total_subscriptions,
    SUM(s.monthly_recurring_revenue) AS total_mrr,
    AVG(s.subscription_days) AS avg_subscription_days
INTO clean.fact_accounts
FROM clean.accounts a
LEFT JOIN clean.subscriptions s
    ON a.account_id = s.account_id
GROUP BY 
    a.account_id, a.account_name, a.industry, a.country, a.plan_tier, a.seats, a.is_trial, a.churn_flag_calc;

--create clean.fact_support
SELECT 
    st.account_id,
    COUNT(ticket_id) AS total_tickets,
    AVG(resolution_days) AS avg_resolution_days,
    AVG(satisfaction_score) AS avg_satisfaction_score,
    SUM(CASE WHEN priority='HIGH' THEN 1 ELSE 0 END) AS high_priority_tickets,
    SUM(CASE WHEN priority='MEDIUM' THEN 1 ELSE 0 END) AS medium_priority_tickets,
    SUM(CASE WHEN priority='LOW' THEN 1 ELSE 0 END) AS low_priority_tickets
INTO clean.fact_support
FROM clean.support_tickets st
GROUP BY st.account_id;

--create clean.fact_feature_usage
SELECT
    feature_name,
    usage_date,
    SUM(usage_count) AS total_usage
INTO clean.fact_feature_usage
FROM clean.feature_usage
GROUP BY feature_name, usage_date;


/*  Validate all Raw, Clean, and Fact tables have been successfully Created */

SELECT 
    TABLE_SCHEMA,
    TABLE_NAME
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA IN ('clean')
ORDER BY TABLE_NAME;

--Validate raw row counts match up with clean row counts
SELECT 'accounts' AS table_name, COUNT(*) AS row_count FROM clean.accounts
UNION ALL
SELECT 'subscriptions', COUNT(*) FROM clean.subscriptions
UNION ALL
SELECT 'support_tickets', COUNT(*) FROM clean.support_tickets
UNION ALL
SELECT 'feature_usage', COUNT(*) FROM clean.feature_usage
UNION ALL
SELECT 'churn_events', COUNT(*) FROM clean.churn_events;


/* Creating 5 Advanced Saas Metrics to use */

--Monthly churn counts
SELECT
    FORMAT(TRY_CAST(churn_date AS DATE), 'yyyy-MM') AS churn_month,
    COUNT(DISTINCT account_id) AS churned_accounts
INTO clean.fact_monthly_churn
FROM clean.churn_events
WHERE TRY_CAST(churn_date AS DATE) IS NOT NULL
GROUP BY FORMAT(TRY_CAST(churn_date AS DATE), 'yyyy-MM');

--MRR by Plan Tier
SELECT
    a.plan_tier,
    SUM(s.monthly_recurring_revenue) AS total_mrr
INTO clean.fact_mrr_by_plan
FROM clean.subscriptions s
JOIN clean.accounts a
    ON s.account_id = a.account_id
WHERE s.is_active = 1
GROUP BY a.plan_tier;

--Average Subscription Length
SELECT
    a.plan_tier,
    AVG(s.subscription_days) AS avg_days_before_churn
INTO clean.fact_churn_duration
FROM clean.subscriptions s
JOIN clean.accounts a
    ON s.account_id = a.account_id
WHERE a.churn_flag_calc = 1
  AND s.subscription_days IS NOT NULL
GROUP BY a.plan_tier;

--Support Load vs Churn
SELECT
    a.churn_flag_calc,
    COUNT(st.ticket_id) AS total_tickets,
    AVG(st.resolution_days) AS avg_resolution_days,
    AVG(st.satisfaction_score) AS avg_satisfaction
INTO clean.fact_support_vs_churn
FROM clean.accounts a
LEFT JOIN clean.support_tickets st
    ON a.account_id = st.account_id
GROUP BY a.churn_flag_calc;

--Feature Usage Volatility
SELECT
    feature_name,
    AVG(usage_count) AS avg_daily_usage,
    STDEV(usage_count) AS usage_volatility
INTO clean.fact_feature_volatility
FROM clean.feature_usage
GROUP BY feature_name;
