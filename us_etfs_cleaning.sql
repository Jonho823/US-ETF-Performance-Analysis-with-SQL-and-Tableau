-- Data Cleaning

-- Create a new table to work on, avoid editing raw data
DROP TABLE IF EXISTS us_etfs.us_etfs_staging;
CREATE TABLE us_etfs.us_efts_staging
LIKE us_etfs.us_efts_data;

INSERT us_etfs_staging
SELECT *
FROM us_efts_data;

-- check if data inserted
SELECT *
FROM us_etfs_staging;

-- 1. Remove Duplicates

# first, check for duplicates by creating a cte with row numbers for each unique row
WITH row_no_cte AS
(
SELECT *,
		ROW_NUMBER() OVER (
			PARTITION BY fund_symbol, fund_long_name, total_net_assets) AS row_num
	FROM 
		us_etfs_staging
)
SELECT *
FROM row_no_cte
WHERE row_num >1;
;
# results null means there is no duplciate row in the dataset

-- 2. Standardize the Data
UPDATE us_etfs_staging
SET inception_date = str_to_date(inception_date, '%Y/%m/%d');

-- 3. Remove any Columns
-- 3.1 Remove empty cells in fund_category columns
DELETE
FROM us_etfs_staging
WHERE fund_category IS NULL;

-- 3.2 Remove all unused columns
ALTER TABLE us_etfs_staging
DROP COLUMN quote_type,
DROP COLUMN region,
DROP COLUMN currency,
DROP COLUMN exchange_timezone,
DROP COLUMN exchange_code,
DROP COLUMN asset_stocks,
DROP COLUMN asset_bonds,
DROP COLUMN day50_moving_average,
DROP COLUMN day200_moving_average,
DROP COLUMN top10_holdings,
DROP COLUMN years_down,
DROP COLUMN years_up,
DROP COLUMN fund_short_name,
DROP COLUMN fund_bond_duration,
DROP COLUMN fund_bond_maturity,
DROP COLUMN fund_bonds_us_government,
DROP COLUMN category_annual_report_net_expense_ratio,
DROP COLUMN week52_high,
DROP COLUMN week52_high_change,
DROP COLUMN week52_high_change_perc,
DROP COLUMN week52_high_low_change,
DROP COLUMN week52_high_low_change_perc,
DROP COLUMN week52_low,
DROP COLUMN week52_low_change,
DROP COLUMN week52_low_change_perc,
DROP COLUMN category_return_2000,
DROP COLUMN category_return_2001,
DROP COLUMN category_return_2002,
DROP COLUMN category_return_2003,
DROP COLUMN category_return_2004,
DROP COLUMN category_return_2005,
DROP COLUMN category_return_2006,
DROP COLUMN category_return_2007,
DROP COLUMN category_return_2008,
DROP COLUMN category_return_2009,
DROP COLUMN category_return_2010,
DROP COLUMN category_return_2011,
DROP COLUMN category_return_2012,
DROP COLUMN category_return_2013,
DROP COLUMN category_return_2014,
DROP COLUMN category_return_2015,
DROP COLUMN category_return_2016,
DROP COLUMN category_return_2017,
DROP COLUMN category_return_2018,
DROP COLUMN category_return_2019,
DROP COLUMN category_return_2020,
DROP COLUMN fund_return_2000,
DROP COLUMN fund_return_2001,
DROP COLUMN fund_return_2002,
DROP COLUMN fund_return_2003,
DROP COLUMN fund_return_2004,
DROP COLUMN fund_return_2005,
DROP COLUMN fund_return_2006,
DROP COLUMN fund_return_2007,
DROP COLUMN fund_return_2008,
DROP COLUMN fund_return_2009,
DROP COLUMN fund_return_2010,
DROP COLUMN fund_return_2011,
DROP COLUMN fund_return_2012,
DROP COLUMN fund_return_2013,
DROP COLUMN fund_return_2014,
DROP COLUMN fund_return_2015,
DROP COLUMN fund_return_2016,
DROP COLUMN fund_return_2017,
DROP COLUMN fund_return_2018,
DROP COLUMN fund_return_2019,
DROP COLUMN fund_return_2020,
DROP COLUMN fund_bonds_a,
DROP COLUMN fund_bonds_aa,
DROP COLUMN fund_bonds_aaa,
DROP COLUMN fund_bonds_b,
DROP COLUMN fund_bonds_bb,
DROP COLUMN fund_bonds_bbb,
DROP COLUMN fund_bonds_below_b,
DROP COLUMN fund_bonds_others
;

-- 4. Data extraction
SELECT
    fund_symbol,
    fund_category,
    investment_type,
    size_type,
	date_format(inception_date, "%Y") AS incept_year,
    ROUND(avg_vol_3month) vol_3mth,
    ROUND(total_net_assets) net_assets,
    ROUND(fund_price_book_ratio,2) pb_ratio,
    ROUND(fund_price_earning_ratio, 2) pe_ratio,
    ROUND(fund_return_ytd, 2) fund_return,
    ROUND(fund_alpha_3years, 2) alpha_3yr,
    ROUND(fund_beta_3years, 2) beta_3yr,
    ROUND(fund_mean_annual_return_3years, 2) ann_ret_3yr,
    ROUND(fund_stdev_3years, 2) std_3yr,
    ROUND(fund_sharpe_ratio_3years, 2) sharpe_3yr,
    ROUND(fund_treynor_ratio_3years, 2) treynor_3yr
FROM us_etfs_staging
WHERE investment_type IS NOT NULL;
